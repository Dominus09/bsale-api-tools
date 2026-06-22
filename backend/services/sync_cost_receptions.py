"""Sincronización incremental recepciones Bsale → analytics.cost_reception_history."""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any

from backend.db import get_connection
from backend.repositories import cost_analytics_repo as repo
from backend.services.cost_receptions_fetch import iter_receptions_for_sync
from backend.services.distribuidora.bsale_client import BsaleClient
from backend.utils.bsale_field_parse import (
    parse_float,
    parse_int,
    parse_optional_float,
    parse_optional_int,
)
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


def _ts_to_dt(ts: int | float) -> datetime:
    return datetime.fromtimestamp(int(ts), tz=timezone.utc)


def _fmt_ts(ts: int | float | None) -> str | None:
    if ts is None:
        return None
    return _ts_to_dt(ts).isoformat()


def _sync_company_receptions(
    cur,
    *,
    company_id: int,
    company_name: str,
    client: BsaleClient,
    since_ts: int,
    tax_context_log_emitted: list[bool] | None = None,
) -> tuple[int, int, int | None, dict[str, Any]]:
    """
    Returns (receptions_inserted, lines_inserted, max_admission_ts_inserted, stats).

    max_admission_ts_inserted solo se setea si hubo al menos una línea insertada.
    """
    receptions_n = 0
    lines_n = 0
    max_ts_inserted: int | None = None
    stats: dict[str, Any] = {
        "details_read": 0,
        "details_discarded_exists": 0,
        "details_discarded_no_variant": 0,
        "lines_inserted": 0,
    }

    def _log_tax_context_unavailable() -> None:
        if tax_context_log_emitted is not None and tax_context_log_emitted[0]:
            return
        logger.info(
            "[COST_SYNC] tax_context_not_available using_net_cost_only=true"
        )
        if tax_context_log_emitted is not None:
            tax_context_log_emitted[0] = True

    until_ts = int(datetime.now(timezone.utc).timestamp())
    _diag(
        "company=%s since_ts=%s (%s) until=%s",
        company_id,
        since_ts,
        _ts_to_dt(since_ts),
        _ts_to_dt(until_ts),
    )

    receptions, fetch_meta = iter_receptions_for_sync(
        client, since_ts=since_ts, until_ts=until_ts, limit=LIMIT
    )
    stats.update(fetch_meta)

    logger.info(
        "COST_SYNC_FETCH company_id=%s strategy=%s pages_read=%s api_count=%s "
        "order_hint=%s min_admission=%s max_admission=%s receptions_in_window=%s "
        "receptions_year_2026=%s discarded_old=%s",
        company_id,
        fetch_meta.get("strategy"),
        fetch_meta.get("pages_read"),
        fetch_meta.get("api_total_count"),
        fetch_meta.get("page_order_hint"),
        _fmt_ts(fetch_meta.get("min_admission_ts")),
        _fmt_ts(fetch_meta.get("max_admission_ts")),
        fetch_meta.get("receptions_in_window"),
        fetch_meta.get("receptions_year_2026"),
        fetch_meta.get("receptions_discarded_old"),
    )

    for rec in receptions:
        adm_ts = parse_int(rec.get("admissionDate"), default=0)
        if adm_ts < since_ts:
            continue

        rec_id = parse_int(rec.get("id"), default=0)
        if not rec_id:
            continue
        admission = _ts_to_dt(adm_ts)
        document = (rec.get("document") or "").strip() or None

        raw_doc_num = rec.get("documentNumber")
        doc_num = parse_optional_int(raw_doc_num)
        if doc_num is None:
            logger.info(
                "[COST_SYNC] recepcion_id=%s document_number_raw=%r",
                rec_id,
                raw_doc_num if raw_doc_num is not None else "",
            )

        office = rec.get("office") or {}
        office_id = parse_optional_int(office.get("id"))
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
                detail_id = parse_int(line.get("id"), default=0)
                if not detail_id:
                    continue
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
                variant_id = parse_int(variant_node.get("id"), default=0)
                if not variant_id:
                    stats["details_discarded_no_variant"] += 1
                    _diag(
                        "detalle descartado id=%s rec=%s motivo=no_variant_id",
                        detail_id,
                        rec_id,
                    )
                    continue

                ctx = repo.variant_tax_context(cur, company_id, variant_id)
                cost_net = parse_float(line.get("cost"), default=0.0)
                if not ctx.get("tax_context_available", True):
                    _log_tax_context_unavailable()
                    tf = 1.0
                    iva_rate = None
                    iva_amt = 0.0
                    other_tax = 0.0
                    bruto = cost_net
                else:
                    tf = parse_float(ctx.get("tax_factor"), default=1.0)
                    iva_rate = ctx.get("iva_rate")
                    iva_amt, other_tax, bruto = split_erp_cost(
                        cost_net, tax_factor=tf, iva_rate=iva_rate
                    )
                    iva_amt = float(iva_amt)
                    other_tax = float(other_tax)
                    bruto = float(bruto)
                qty = parse_float(line.get("quantity"), default=0.0)
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
                    average_cost = parse_optional_float(avg)
                    if ctx.get("tax_context_available", True):
                        avg_gross = (
                            float(cost_gross_from_net(average_cost, tf))
                            if average_cost is not None
                            else None
                        )
                    else:
                        avg_gross = average_cost
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
                    product_id=parse_optional_int(ctx.get("product_id")),
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
                    iva_amount=iva_amt,
                    other_taxes=other_tax,
                    cost_bruto_erp=bruto,
                    average_cost=average_cost,
                    variation_pct=var_pct,
                )
                if inserted:
                    new_lines_in_rec += 1
                    lines_n += 1
                    stats["lines_inserted"] += 1
                    max_ts_inserted = (
                        adm_ts if max_ts_inserted is None else max(max_ts_inserted, adm_ts)
                    )

            detail_offset += LIMIT
            time.sleep(THROTTLE_SEC)
            if len(det_items) < LIMIT:
                break

        if new_lines_in_rec > 0:
            receptions_n += 1

        time.sleep(THROTTLE_SEC)

    logger.info(
        "COST_SYNC company_id=%s receptions_processed=%s receptions_inserted=%s "
        "details_read=%s details_discarded_exists=%s details_discarded_no_variant=%s "
        "lines_inserted=%s watermark_ts=%s",
        company_id,
        len(receptions),
        receptions_n,
        stats["details_read"],
        stats["details_discarded_exists"],
        stats["details_discarded_no_variant"],
        stats["lines_inserted"],
        _fmt_ts(max_ts_inserted),
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

        tax_context_log_emitted = [False]

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
                    tax_context_log_emitted=tax_context_log_emitted,
                )
                if line_n > 0 and max_ts_inserted is not None:
                    new_watermark = max_ts_inserted
                    watermark_advanced = True
                else:
                    new_watermark = prev_watermark
                    watermark_advanced = False
                    in_window = int(sync_stats.get("receptions_in_window") or 0)
                    if line_n == 0 and in_window > 0:
                        logger.warning(
                            "COST_SYNC company_id=%s watermark NO avanzado: "
                            "0 líneas insertadas con %s recepciones en ventana "
                            "(details_read=%s exists=%s no_variant=%s)",
                            cid,
                            in_window,
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
