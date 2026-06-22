"""Sincronización incremental recepciones Bsale → analytics.cost_reception_history."""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any

from backend.db import get_connection
from backend.repositories import cost_analytics_repo as repo
from backend.services.distribuidora.bsale_client import BsaleClient
from backend.utils.cost_analytics_calc import (
    cost_gross_from_net,
    make_unique_key,
    split_erp_cost,
    variation_pct,
)

logger = logging.getLogger(__name__)

LIMIT = 50
THROTTLE_SEC = float(os.getenv("COST_SYNC_THROTTLE_SEC", "0.05"))
INITIAL_LOOKBACK_DAYS = int(os.getenv("COST_SYNC_INITIAL_DAYS", "90"))
DIAG = os.getenv("COST_SYNC_DIAG", "").strip().lower() in ("1", "true", "yes")


def _diag(msg: str, *args) -> None:
    if DIAG:
        logger.warning("[COST_SYNC_DIAG] " + msg, *args)


def _load_companies(cur) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT company_id, name, bsale_token
        FROM bsale.companies
        WHERE active = TRUE
        ORDER BY company_id
        """
    )
    out: list[dict[str, Any]] = []
    for company_id, name, token_key in cur.fetchall():
        token = os.getenv(str(token_key or "").strip())
        if not token:
            logger.warning("Token no configurado para empresa %s (%s)", company_id, name)
            continue
        out.append({"company_id": int(company_id), "name": name, "token": token})
    return out


def _ts_to_dt(ts: int | float | None) -> datetime:
    return datetime.fromtimestamp(int(ts), tz=timezone.utc)


def _sync_company_receptions(
    cur,
    *,
    company_id: int,
    company_name: str,
    client: BsaleClient,
    since_ts: int,
) -> tuple[int, int, int | None, dict[str, int]]:
    """
    Returns (receptions_inserted, lines_inserted, max_admission_ts_inserted, stats).

    max_admission_ts_inserted solo se setea si hubo al menos una línea insertada.
    """
    receptions_n = 0
    lines_n = 0
    max_ts_inserted: int | None = None
    offset = 0
    stop_paging = False
    stats: dict[str, int] = {
        "pages": 0,
        "receptions_read": 0,
        "receptions_in_window": 0,
        "receptions_discarded_old": 0,
        "details_read": 0,
        "details_discarded_exists": 0,
        "details_discarded_no_variant": 0,
        "lines_inserted": 0,
    }
    _diag(
        "company=%s since_ts=%s (%s)",
        company_id,
        since_ts,
        _ts_to_dt(since_ts).isoformat(),
    )

    while not stop_paging:
        data = client.get(
            "/stocks/receptions.json",
            {"limit": LIMIT, "offset": offset, "expand": "[office]"},
        )
        items = data.get("items") or []
        stats["pages"] += 1
        if not items:
            _diag("company=%s offset=%s página vacía", company_id, offset)
            break

        if stats["pages"] == 1 and items:
            first_ts = int(items[0].get("admissionDate") or 0)
            last_ts = int(items[-1].get("admissionDate") or 0)
            order = "DESC" if first_ts > last_ts else ("ASC" if first_ts < last_ts else "FLAT")
            _diag(
                "company=%s página0 orden=%s first=%s last=%s",
                company_id,
                order,
                _ts_to_dt(first_ts).isoformat(),
                _ts_to_dt(last_ts).isoformat(),
            )

        for rec in items:
            adm_ts = int(rec.get("admissionDate") or 0)
            stats["receptions_read"] += 1
            if adm_ts < since_ts:
                stats["receptions_discarded_old"] += 1
                _diag(
                    "recepción descartada id=%s adm=%s motivo=below_since_ts",
                    rec.get("id"),
                    _ts_to_dt(adm_ts).isoformat(),
                )
                stop_paging = True
                continue
            stats["receptions_in_window"] += 1

            rec_id = int(rec["id"])
            admission = _ts_to_dt(adm_ts)
            document = (rec.get("document") or "").strip() or None
            doc_num = int(rec["documentNumber"]) if rec.get("documentNumber") is not None else None
            office = rec.get("office") or {}
            office_id = int(office["id"]) if office.get("id") is not None else None
            office_name = office.get("name")

            detail_offset = 0
            new_lines_in_rec = 0

            while True:
                det_data = client.get(
                    f"/stocks/receptions/{rec_id}/details.json",
                    {"limit": LIMIT, "offset": detail_offset, "expand": "[variant]"},
                )
                det_items = det_data.get("items") or []
                if not det_items:
                    break

                for line in det_items:
                    detail_id = int(line["id"])
                    stats["details_read"] += 1
                    if repo.line_exists(cur, company_id, detail_id):
                        stats["details_discarded_exists"] += 1
                        _diag(
                            "detalle descartado id=%s rec=%s motivo=already_in_db",
                            detail_id,
                            rec_id,
                        )
                        continue

                    variant_node = line.get("variant") or {}
                    variant_id = int(variant_node.get("id") or 0)
                    if not variant_id:
                        stats["details_discarded_no_variant"] += 1
                        _diag(
                            "detalle descartado id=%s rec=%s motivo=no_variant_id",
                            detail_id,
                            rec_id,
                        )
                        continue

                    ctx = repo.variant_tax_context(cur, company_id, variant_id)
                    cost_net = float(line.get("cost") or 0)
                    tf = float(ctx.get("tax_factor") or 1)
                    iva_rate = ctx.get("iva_rate")
                    iva_amt, other_tax, bruto = split_erp_cost(
                        cost_net, tax_factor=tf, iva_rate=iva_rate
                    )
                    qty = float(line.get("quantity") or 0)
                    prev = repo.previous_cost_for_variant(
                        cur,
                        company_id=company_id,
                        variant_id=variant_id,
                        office_id=office_id,
                        before=admission,
                    )
                    var_pct = variation_pct(cost_net, prev)
                    average_cost: float | None = None

                    try:
                        costs = client.get(f"/variants/{variant_id}/costs.json")
                        avg = costs.get("averageCost")
                        average_cost = float(avg) if avg is not None else None
                        avg_gross = (
                            float(cost_gross_from_net(average_cost, tf))
                            if average_cost is not None
                            else None
                        )
                        repo.upsert_variant_cost_snapshot(
                            cur,
                            company_id=company_id,
                            variant_id=variant_id,
                            average_cost_net=average_cost,
                            average_cost_gross=avg_gross,
                            tax_factor=tf,
                            iva_rate=iva_rate,
                            specific_taxes=ctx.get("specific_taxes"),
                        )
                    except Exception as exc:
                        logger.warning(
                            "No se pudo refrescar costs.json variant=%s: %s",
                            variant_id,
                            exc,
                        )

                    inserted = repo.insert_history_line(
                        cur,
                        unique_key=make_unique_key(company_id, detail_id),
                        company_id=company_id,
                        company_name=company_name,
                        office_id=office_id,
                        office_name=office_name,
                        variant_id=variant_id,
                        product_id=int(ctx["product_id"]) if ctx.get("product_id") else None,
                        barcode=ctx.get("barcode"),
                        product_name=ctx.get("product_name"),
                        variant_name=ctx.get("variant_name"),
                        reception_id=rec_id,
                        reception_detail_id=detail_id,
                        document=document,
                        document_number=doc_num,
                        admission_date=admission,
                        quantity=qty,
                        cost_net=cost_net,
                        iva_amount=float(iva_amt),
                        other_taxes=float(other_tax),
                        cost_bruto_erp=float(bruto),
                        average_cost=average_cost,
                        variation_pct=var_pct,
                    )
                    if inserted:
                        new_lines_in_rec += 1
                        lines_n += 1
                        stats["lines_inserted"] += 1
                        max_ts_inserted = (
                            adm_ts
                            if max_ts_inserted is None
                            else max(max_ts_inserted, adm_ts)
                        )

                detail_offset += LIMIT
                time.sleep(THROTTLE_SEC)
                if len(det_items) < LIMIT:
                    break

            if new_lines_in_rec > 0:
                receptions_n += 1

            time.sleep(THROTTLE_SEC)

        offset += LIMIT
        if len(items) < LIMIT:
            break
        time.sleep(THROTTLE_SEC)

    logger.info(
        "COST_SYNC company_id=%s receptions_read=%s receptions_in_window=%s "
        "receptions_discarded_old=%s details_read=%s details_discarded_exists=%s "
        "details_discarded_no_variant=%s lines_inserted=%s watermark_ts=%s",
        company_id,
        stats["receptions_read"],
        stats["receptions_in_window"],
        stats["receptions_discarded_old"],
        stats["details_read"],
        stats["details_discarded_exists"],
        stats["details_discarded_no_variant"],
        stats["lines_inserted"],
        _ts_to_dt(max_ts_inserted).isoformat() if max_ts_inserted else None,
    )
    _diag(
        "company=%s resumen pages=%s stats=%s watermark_inserted_only=%s",
        company_id,
        stats["pages"],
        stats,
        max_ts_inserted,
    )
    return receptions_n, lines_n, max_ts_inserted, stats


def sync_cost_receptions(
    *,
    company_id: int | None = None,
    lookback_days: int | None = None,
) -> dict[str, Any]:
    conn = get_connection()
    summary: dict[str, Any] = {
        "ok": True,
        "companies": [],
        "total_receptions": 0,
        "total_lines": 0,
    }
    try:
        cur = conn.cursor()
        companies = _load_companies(cur)
        if company_id is not None:
            companies = [c for c in companies if c["company_id"] == company_id]
        if not companies:
            raise ValueError("No hay empresas activas con token Bsale configurado.")

        for co in companies:
            cid = co["company_id"]
            state = repo.get_sync_state(cur, cid)
            if state and state.get("last_admission_ts"):
                since_ts = int(state["last_admission_ts"]) - 86400
                since_mode = "incremental"
            else:
                days = lookback_days if lookback_days is not None else INITIAL_LOOKBACK_DAYS
                since_ts = int(
                    (datetime.now(timezone.utc) - timedelta(days=days)).timestamp()
                )
                since_mode = f"lookback_{days}d"

            _diag(
                "empresa=%s mode=%s lookback_days_param=%s since_ts=%s state=%s",
                cid,
                since_mode,
                lookback_days,
                since_ts,
                state,
            )

            client = BsaleClient(co["token"])
            try:
                prev_watermark = (
                    int(state["last_admission_ts"])
                    if state and state.get("last_admission_ts")
                    else None
                )
                rec_n, line_n, max_ts_inserted, sync_stats = _sync_company_receptions(
                    cur,
                    company_id=cid,
                    company_name=co["name"],
                    client=client,
                    since_ts=since_ts,
                )
                # Protección: no avanzar watermark sin al menos una línea insertada.
                if line_n > 0 and max_ts_inserted is not None:
                    new_watermark = max_ts_inserted
                    watermark_advanced = True
                else:
                    new_watermark = prev_watermark
                    watermark_advanced = False
                    if line_n == 0 and sync_stats["receptions_in_window"] > 0:
                        logger.warning(
                            "COST_SYNC company_id=%s watermark NO avanzado: "
                            "0 líneas insertadas con %s recepciones en ventana "
                            "(details_read=%s exists=%s no_variant=%s)",
                            cid,
                            sync_stats["receptions_in_window"],
                            sync_stats["details_read"],
                            sync_stats["details_discarded_exists"],
                            sync_stats["details_discarded_no_variant"],
                        )

                conn.commit()
                repo.upsert_sync_state(
                    cur,
                    company_id=cid,
                    last_admission_ts=new_watermark,
                    status="ok",
                    message=f"+{rec_n} recepciones, +{line_n} líneas",
                    receptions_inserted=rec_n,
                    lines_inserted=line_n,
                )
                conn.commit()
                summary["companies"].append(
                    {
                        "company_id": cid,
                        "name": co["name"],
                        "receptions_inserted": rec_n,
                        "lines_inserted": line_n,
                        "last_admission_ts": new_watermark,
                        "watermark_advanced": watermark_advanced,
                        "sync_stats": sync_stats,
                    }
                )
                summary["total_receptions"] += rec_n
                summary["total_lines"] += line_n
            except Exception as exc:
                conn.rollback()
                logger.exception("Sync costos empresa %s", cid)
                repo.upsert_sync_state(
                    cur,
                    company_id=cid,
                    last_admission_ts=state.get("last_admission_ts") if state else None,
                    status="error",
                    message=str(exc)[:500],
                    receptions_inserted=0,
                    lines_inserted=0,
                )
                conn.commit()
                summary["ok"] = False
                summary["companies"].append(
                    {"company_id": cid, "name": co["name"], "error": str(exc)}
                )
        cur.close()
    finally:
        conn.close()
    return summary
