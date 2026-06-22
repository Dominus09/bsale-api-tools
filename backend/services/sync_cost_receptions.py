"""Sincronización incremental recepciones Bsale → analytics.cost_reception_history."""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import psycopg2

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


def _reception_id_bounds(receptions: list[dict[str, Any]]) -> tuple[int | None, int | None]:
    ids: list[int] = []
    for rec in receptions:
        rec_id = parse_int(rec.get("id"), default=0)
        if rec_id:
            ids.append(rec_id)
    if not ids:
        return None, None
    return ids[0], ids[-1]


def _is_sql_error(exc: BaseException) -> bool:
    return isinstance(exc, psycopg2.Error)


def _is_tax_related_error(exc: BaseException) -> bool:
    msg = str(exc).lower()
    return "tax" in msg or "p.taxes" in msg


def _new_company_stats() -> dict[str, Any]:
    return {
        "details_read": 0,
        "details_discarded_exists": 0,
        "details_discarded_no_variant": 0,
        "lines_inserted": 0,
        "recepciones_leidas": 0,
        "errores_tributarios": 0,
        "errores_sql": 0,
        "errores_generales": 0,
        "tax_audit": {
            "calls": 0,
            "fallback": 0,
            "p_taxes_attempted": 0,
            "errors": 0,
        },
    }


def _log_company_summary(
    company_id: int,
    company_name: str,
    stats: dict[str, Any],
    *,
    receptions_inserted: int,
) -> None:
    tax_audit = stats.get("tax_audit") or {}
    logger.info(
        "COMPANY %s SUMMARY company_id=%s name=%r recepciones_leidas=%s "
        "detalles_leidos=%s detalles_insertados=%s recepciones_insertadas=%s "
        "errores_tributarios=%s errores_sql=%s errores_generales=%s "
        "tax_calls=%s tax_fallback=%s tax_p_taxes_attempted=%s tax_errors=%s "
        "details_discarded_exists=%s details_discarded_no_variant=%s",
        company_id,
        company_id,
        company_name,
        stats.get("recepciones_leidas", 0),
        stats.get("details_read", 0),
        stats.get("lines_inserted", 0),
        receptions_inserted,
        stats.get("errores_tributarios", 0),
        stats.get("errores_sql", 0),
        stats.get("errores_generales", 0),
        tax_audit.get("calls", 0),
        tax_audit.get("fallback", 0),
        tax_audit.get("p_taxes_attempted", 0),
        tax_audit.get("errors", 0),
        stats.get("details_discarded_exists", 0),
        stats.get("details_discarded_no_variant", 0),
    )


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
    stats = _new_company_stats()
    first_reception_logged = False
    first_detail_logged = False
    first_insert_logged = False

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

    logger.info(
        "[SYNC_STEP_4] Llamada fetch_receptions company_id=%s since_ts=%s until_ts=%s",
        company_id,
        since_ts,
        until_ts,
    )
    receptions, fetch_meta = iter_receptions_for_sync(
        client, since_ts=since_ts, until_ts=until_ts, limit=LIMIT
    )
    stats.update(fetch_meta)
    first_rec_id, last_rec_id = _reception_id_bounds(receptions)
    logger.info(
        "[SYNC_STEP_5] Recepciones obtenidas company_id=%s len_receptions=%s "
        "first_reception_id=%s last_reception_id=%s receptions_in_window=%s "
        "strategy=%s pages_read=%s",
        company_id,
        len(receptions),
        first_rec_id,
        last_rec_id,
        fetch_meta.get("receptions_in_window"),
        fetch_meta.get("strategy"),
        fetch_meta.get("pages_read"),
    )

    logger.info(
        "[SYNC_STEP_6] Inicio procesamiento recepciones company_id=%s len_receptions=%s",
        company_id,
        len(receptions),
    )

    for rec in receptions:
        adm_ts = parse_int(rec.get("admissionDate"), default=0)
        if adm_ts < since_ts:
            continue

        rec_id = parse_int(rec.get("id"), default=0)
        if not rec_id:
            continue
        stats["recepciones_leidas"] += 1
        if not first_reception_logged:
            logger.info(
                "[SYNC_STEP_7] Primera recepción procesada company_id=%s "
                "reception_id=%s admission_ts=%s",
                company_id,
                rec_id,
                _fmt_ts(adm_ts),
            )
            first_reception_logged = True
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

                cur.execute("SAVEPOINT cost_sync_detail")
                try:
                    if not first_detail_logged:
                        logger.info(
                            "[SYNC_STEP_8] Primer detalle procesado company_id=%s "
                            "reception_id=%s reception_detail_id=%s variant_id=%s",
                            company_id,
                            rec_id,
                            detail_id,
                            variant_id,
                        )
                        first_detail_logged = True
                    ctx = repo.variant_tax_context(
                        cur,
                        company_id,
                        variant_id,
                        tax_audit=stats["tax_audit"],
                    )
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

                    if not first_insert_logged:
                        logger.info(
                            "[SYNC_STEP_9] Insert cost_reception_history company_id=%s "
                            "reception_id=%s reception_detail_id=%s variant_id=%s",
                            company_id,
                            rec_id,
                            detail_id,
                            variant_id,
                        )
                        first_insert_logged = True
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
                    cur.execute("RELEASE SAVEPOINT cost_sync_detail")
                except Exception as exc:
                    try:
                        cur.execute("ROLLBACK TO SAVEPOINT cost_sync_detail")
                    except Exception:
                        try:
                            cur.connection.rollback()
                        except Exception:
                            pass
                    stats["errores_generales"] += 1
                    if _is_sql_error(exc):
                        stats["errores_sql"] += 1
                    if _is_tax_related_error(exc):
                        stats["errores_tributarios"] += 1
                    logger.exception(
                        "[COST_SYNC_DETAIL_ERROR] company_id=%s reception_id=%s "
                        "reception_detail_id=%s variant_id=%s error_type=%s",
                        company_id,
                        rec_id,
                        detail_id,
                        variant_id,
                        type(exc).__name__,
                    )
                    continue

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

    tax_audit = stats.get("tax_audit") or {}
    logger.info(
        "COST_SYNC company_id=%s receptions_processed=%s receptions_inserted=%s "
        "details_read=%s details_discarded_exists=%s details_discarded_no_variant=%s "
        "lines_inserted=%s watermark_ts=%s tax_calls=%s tax_fallback=%s "
        "tax_p_taxes_attempted=%s tax_errors=%s errores_tributarios=%s "
        "errores_sql=%s errores_generales=%s",
        company_id,
        len(receptions),
        receptions_n,
        stats["details_read"],
        stats["details_discarded_exists"],
        stats["details_discarded_no_variant"],
        stats["lines_inserted"],
        _fmt_ts(max_ts_inserted),
        tax_audit.get("calls", 0),
        tax_audit.get("fallback", 0),
        tax_audit.get("p_taxes_attempted", 0),
        tax_audit.get("errors", 0),
        stats.get("errores_tributarios", 0),
        stats.get("errores_sql", 0),
        stats.get("errores_generales", 0),
    )
    _log_company_summary(
        company_id,
        company_name,
        stats,
        receptions_inserted=receptions_n,
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
        repo.reset_product_column_cache()
        repo.log_bsale_products_schema(cur)
        logger.info(
            "[COST_SYNC_AUDIT] p.taxes references in Python runtime: "
            "cost_analytics_repo.variant_tax_context (conditional on has_taxes column)"
        )
        companies = _load_companies(cur)
        if company_id is not None:
            companies = [c for c in companies if c["company_id"] == company_id]
        if not companies:
            raise ValueError("No hay empresas activas con token Bsale configurado.")

        tax_context_log_emitted = [False]

        for co in companies:
            cid = co["company_id"]
            state: dict[str, Any] | None = None
            try:
                logger.info(
                    "[SYNC_STEP_1] Inicio sync empresa company_id=%s name=%r",
                    cid,
                    co["name"],
                )
                state = repo.get_sync_state(cur, cid)
                logger.info(
                    "[SYNC_STEP_2] Lectura cost_sync_state company_id=%s state=%s",
                    cid,
                    state,
                )
                if state and state.get("last_admission_ts"):
                    since_ts = int(state["last_admission_ts"]) - 86400
                    since_mode = "incremental"
                else:
                    days = (
                        lookback_days
                        if lookback_days is not None
                        else INITIAL_LOOKBACK_DAYS
                    )
                    since_ts = int(
                        (
                            datetime.now(timezone.utc) - timedelta(days=days)
                        ).timestamp()
                    )
                    since_mode = f"lookback_{days}d"

                logger.info(
                    "[SYNC_STEP_3] Construcción ventana fechas company_id=%s "
                    "since_mode=%s since_ts=%s (%s) lookback_days=%s",
                    cid,
                    since_mode,
                    since_ts,
                    _fmt_ts(since_ts),
                    lookback_days,
                )

                _diag(
                    "empresa=%s mode=%s lookback_days_param=%s since_ts=%s state=%s",
                    cid,
                    since_mode,
                    lookback_days,
                    since_ts,
                    state,
                )

                client = BsaleClient(co["token"])
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
                logger.info(
                    "[SYNC_STEP_10] Fin empresa company_id=%s status=ok "
                    "receptions_inserted=%s lines_inserted=%s "
                    "recepciones_leidas=%s detalles_leidos=%s",
                    cid,
                    rec_n,
                    line_n,
                    sync_stats.get("recepciones_leidas", 0),
                    sync_stats.get("details_read", 0),
                )
            except Exception:
                conn.rollback()
                logger.exception(
                    "[SYNC_FATAL] company_id=%s name=%r",
                    cid,
                    co["name"],
                )
                repo.upsert_sync_state(
                    cur,
                    company_id=cid,
                    last_admission_ts=state.get("last_admission_ts") if state else None,
                    status="error",
                    message="sync fatal — ver logs [SYNC_FATAL]",
                    receptions_inserted=0,
                    lines_inserted=0,
                )
                conn.commit()
                summary["ok"] = False
                summary["companies"].append(
                    {
                        "company_id": cid,
                        "name": co["name"],
                        "error": "sync fatal — ver logs [SYNC_FATAL]",
                    }
                )
        cur.close()
    finally:
        conn.close()
    return summary
