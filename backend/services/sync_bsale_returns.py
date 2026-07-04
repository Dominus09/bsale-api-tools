"""Sincronización devoluciones Bsale — bootstrap histórico controlado + incremental."""

from __future__ import annotations

import logging
import os
import time
from datetime import date, datetime, time as dt_time, timedelta, timezone
from typing import Any, Iterator

from backend.config.returns_scope import (
    COMPANY_ID,
    HISTORY_DATE_FROM,
    HISTORY_DATE_TO,
    OFFICE_ID,
)
from backend.db import get_connection
from backend.repositories import returns_analytics_repo as repo
from backend.services.distribuidora.bsale_client import BsaleClient
from backend.services.returns_sync_debug import (
    fetch_returns_page_json,
    log_bootstrap_date_conversion,
    run_returns_api_diagnostic,
)
from backend.utils.bsale_field_parse import parse_float, parse_int, parse_optional_int

logger = logging.getLogger(__name__)

LIMIT = 50
THROTTLE_SEC = float(os.getenv("RETURNS_SYNC_THROTTLE_SEC", "0.08"))
INCREMENTAL_OVERLAP_DAYS = int(os.getenv("RETURNS_SYNC_OVERLAP_DAYS", "2"))
EXPAND = "[reference_document,credit_note,details]"


def _load_company_token(cur, company_id: int) -> tuple[str, str] | None:
    cur.execute(
        """
        SELECT company_id, name, bsale_token
        FROM bsale.companies
        WHERE company_id = %s AND active = TRUE
        """,
        (company_id,),
    )
    row = cur.fetchone()
    if not row:
        return None
    _cid, name, token_key = row
    token = os.getenv(str(token_key or "").strip())
    if not token:
        logger.warning("Token no configurado para empresa %s (%s)", company_id, name)
        return None
    return name, token


def _ts_to_dt(ts: int | None) -> datetime | None:
    if ts is None:
        return None
    return datetime.fromtimestamp(int(ts), tz=timezone.utc)


def _date_bounds_ts(d0: date, d1: date) -> tuple[int, int]:
    start = datetime.combine(d0, dt_time.min, tzinfo=timezone.utc)
    end = datetime.combine(d1, dt_time(23, 59, 59), tzinfo=timezone.utc)
    return int(start.timestamp()), int(end.timestamp())


def _ref_id(node: Any) -> int | None:
    if not isinstance(node, dict):
        return None
    return parse_optional_int(node.get("id"))


def _client_name(doc: dict[str, Any] | None) -> str | None:
    if not doc or not isinstance(doc, dict):
        return None
    client = doc.get("client")
    if isinstance(client, dict):
        parts = [
            str(client.get("company") or "").strip(),
            str(client.get("firstName") or client.get("first_name") or "").strip(),
            str(client.get("lastName") or client.get("last_name") or "").strip(),
        ]
        name = " ".join(p for p in parts if p)
        return name or None
    return None


def _seller_from_doc(doc: dict[str, Any] | None) -> tuple[int | None, str | None]:
    if not doc or not isinstance(doc, dict):
        return None, None
    seller = doc.get("seller")
    if isinstance(seller, dict):
        sid = parse_optional_int(seller.get("id"))
        name = str(seller.get("name") or seller.get("firstName") or "").strip() or None
        return sid, name
    sid = parse_optional_int(doc.get("sellerId") or doc.get("seller_id"))
    name = str(doc.get("sellerName") or doc.get("seller_name") or "").strip() or None
    return sid, name


def _parse_detail_items(details_node: Any) -> list[dict[str, Any]]:
    if isinstance(details_node, list):
        items = details_node
    elif isinstance(details_node, dict):
        items = details_node.get("items") or []
    else:
        return []
    out: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        variant = item.get("variant") if isinstance(item.get("variant"), dict) else {}
        product = variant.get("product") if isinstance(variant.get("product"), dict) else {}
        qty = parse_float(item.get("quantity"))
        unit = parse_float(item.get("unitValue") or item.get("unit_value"))
        total = parse_float(item.get("totalAmount") or item.get("total_amount"))
        if total == 0 and qty and unit:
            total = qty * unit
        out.append({
            "bsale_detail_id": parse_int(item.get("id")),
            "document_detail_id": parse_optional_int(item.get("documentDetailId")),
            "variant_id": parse_optional_int(variant.get("id")),
            "product_name": str(product.get("name") or "").strip() or None,
            "variant_description": str(variant.get("description") or "").strip() or None,
            "quantity": qty,
            "unit_value": unit,
            "total_amount": total,
            "raw_data": item,
        })
    return out


def _parse_return_row(
    raw: dict[str, Any],
    *,
    company_id: int,
    office_id: int,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    ref_doc = raw.get("reference_document")
    credit = raw.get("credit_note")
    ref_expanded = ref_doc if isinstance(ref_doc, dict) and ref_doc.get("number") is not None else None
    cn_expanded = credit if isinstance(credit, dict) and credit.get("number") is not None else None

    seller_id, seller_name = _seller_from_doc(cn_expanded) or _seller_from_doc(ref_expanded) or (None, None)
    client_id = None
    if cn_expanded and isinstance(cn_expanded.get("client"), dict):
        client_id = parse_optional_int(cn_expanded["client"].get("id"))
    elif ref_expanded and isinstance(ref_expanded.get("client"), dict):
        client_id = parse_optional_int(ref_expanded["client"].get("id"))

    client_name = _client_name(cn_expanded) or _client_name(ref_expanded)
    municipality = None
    if cn_expanded:
        municipality = cn_expanded.get("municipality") or cn_expanded.get("city")
    if not municipality and ref_expanded:
        municipality = ref_expanded.get("municipality") or ref_expanded.get("city")

    return_ts = parse_optional_int(raw.get("returnDate") or raw.get("return_date"))
    row = {
        "company_id": company_id,
        "office_id": office_id,
        "bsale_id": parse_int(raw.get("id")),
        "code": str(raw.get("code") or "").strip() or None,
        "return_date": _ts_to_dt(return_ts),
        "motive": str(raw.get("motive") or "").strip() or None,
        "return_type": parse_optional_int(raw.get("type")),
        "amount": parse_float(raw.get("amount")),
        "price_adjustment": parse_float(raw.get("priceAdjustment")),
        "edit_texts": parse_int(raw.get("editTexts")),
        "reference_document_id": _ref_id(ref_doc),
        "reference_document_number": parse_optional_int(ref_expanded.get("number")) if ref_expanded else None,
        "reference_document_type_id": parse_optional_int(
            (ref_expanded.get("document_type") or {}).get("id")
            if ref_expanded and isinstance(ref_expanded.get("document_type"), dict)
            else ref_expanded.get("documentTypeId") if ref_expanded else None
        ),
        "credit_note_id": _ref_id(credit),
        "credit_note_number": parse_optional_int(cn_expanded.get("number")) if cn_expanded else None,
        "client_id": client_id,
        "client_name": client_name,
        "seller_id": seller_id,
        "seller_name": seller_name,
        "municipality": str(municipality).strip() if municipality else None,
        "credit_note_emission": _ts_to_dt(parse_optional_int(cn_expanded.get("emissionDate"))) if cn_expanded else None,
        "reference_emission": _ts_to_dt(parse_optional_int(ref_expanded.get("emissionDate"))) if ref_expanded else None,
        "raw_data": raw,
    }
    details = _parse_detail_items(raw.get("details"))
    return row, details


def _in_window(return_ts: int | None, date_from_ts: int, date_to_ts: int) -> bool:
    if return_ts is None:
        return False
    return date_from_ts <= int(return_ts) <= date_to_ts


def iter_returns_pages(
    client: BsaleClient,
    *,
    company_id: int = COMPANY_ID,
    office_id: int,
    date_from_ts: int,
    date_to_ts: int,
    start_offset: int = 0,
) -> Iterator[list[dict[str, Any]]]:
    offset = start_offset
    while True:
        params: dict[str, Any] = {
            "limit": LIMIT,
            "offset": offset,
            "officeid": office_id,
            "returndate": f"[{date_from_ts},{date_to_ts}]",
            "expand": EXPAND,
        }
        payload = fetch_returns_page_json(
            client,
            "/returns.json",
            params,
            company_id=company_id,
            office_id=office_id,
            date_from_ts=date_from_ts,
            date_to_ts=date_to_ts,
        )
        items = payload.get("items") or []
        if not items:
            break
        yield items
        if len(items) < LIMIT:
            break
        offset += LIMIT
        time.sleep(THROTTLE_SEC)


def _process_page(
    cur,
    page: list[dict[str, Any]],
    *,
    company_id: int,
    office_id: int,
    date_from_ts: int | None = None,
    date_to_ts: int | None = None,
    enforce_window: bool = False,
) -> tuple[int, int, int, datetime | None, int | None]:
    """Procesa una página. Retorna returns, details, max_ts, last_date, last_id."""
    returns_upserted = 0
    details_upserted = 0
    max_return_ts = 0
    last_return_date: datetime | None = None
    last_return_id: int | None = None

    for raw in page:
        return_ts = parse_optional_int(raw.get("returnDate") or raw.get("return_date"))
        if enforce_window and date_from_ts is not None and date_to_ts is not None:
            if not _in_window(return_ts, date_from_ts, date_to_ts):
                continue

        row, details = _parse_return_row(raw, company_id=company_id, office_id=office_id)
        if not row["bsale_id"]:
            continue

        repo.upsert_return(cur, row)
        returns_upserted += 1
        if details:
            for d in details:
                d["company_id"] = company_id
                d["return_id"] = row["bsale_id"]
            repo.replace_return_details(
                cur,
                company_id=company_id,
                return_id=row["bsale_id"],
                details=details,
            )
            details_upserted += len(details)

        if return_ts and return_ts > max_return_ts:
            max_return_ts = return_ts
        if row.get("return_date"):
            last_return_date = row["return_date"]
            last_return_id = row["bsale_id"]

    return returns_upserted, details_upserted, max_return_ts, last_return_date, last_return_id


def diagnose_bsale_returns_api(
    *,
    company_id: int = COMPANY_ID,
    office_id: int = OFFICE_ID,
) -> dict[str, Any]:
    """Modo diagnóstico: pruebas A–E contra Bsale sin sincronizar."""
    date_from = HISTORY_DATE_FROM
    date_to = HISTORY_DATE_TO
    date_from_ts, date_to_ts = _date_bounds_ts(date_from, date_to)

    conn = get_connection()
    cur = conn.cursor()
    try:
        repo.ensure_returns_schema(cur)
        conn.commit()
        loaded = _load_company_token(cur, company_id)
    finally:
        cur.close()
        conn.close()

    if not loaded:
        return {"ok": False, "error": f"Sin token para company_id={company_id}"}
    company_name, token = loaded
    client = BsaleClient(token)

    log_bootstrap_date_conversion(date_from, date_to, company_id=company_id, office_id=office_id)

    results = run_returns_api_diagnostic(
        client,
        office_id=office_id,
        date_from_ts=date_from_ts,
        date_to_ts=date_to_ts,
        date_from_iso=date_from.isoformat(),
        date_to_iso=date_to.isoformat(),
    )

    return {
        "ok": True,
        "mode": "diagnostic",
        "company_id": company_id,
        "office_id": office_id,
        "company_name": company_name,
        "date_from": date_from.isoformat(),
        "date_to": date_to.isoformat(),
        "tests": results,
    }


def sync_bsale_returns_history(
    *,
    company_id: int = COMPANY_ID,
    office_id: int = OFFICE_ID,
    resume: bool = False,
    force: bool = False,
) -> dict[str, Any]:
    """
    Bootstrap histórico UNA VEZ: 2026-01-01 → 2026-06-30, Company 3 / Office 1.
    Reanudable por página si falla. Bootstrap vacío (0 páginas/registros) no bloquea reintentos.
    """
    date_from = HISTORY_DATE_FROM
    date_to = HISTORY_DATE_TO
    date_from_ts, date_to_ts = _date_bounds_ts(date_from, date_to)

    log_bootstrap_date_conversion(date_from, date_to, company_id=company_id, office_id=office_id)

    conn = get_connection()
    conn.autocommit = False
    cur = conn.cursor()
    t0 = time.monotonic()
    sync_id: int | None = None

    try:
        repo.ensure_returns_schema(cur)
        conn.commit()

        if not force and repo.get_completed_history_sync(
            cur,
            company_id=company_id,
            office_id=office_id,
            date_from=date_from,
            date_to=date_to,
        ):
            return {
                "ok": False,
                "error": "Bootstrap histórico ya completado para 2026-01-01..2026-06-30",
                "sync_type": "history",
            }

        loaded = _load_company_token(cur, company_id)
        if not loaded:
            return {"ok": False, "error": f"Sin token para company_id={company_id}"}
        company_name, token = loaded
        client = BsaleClient(token)

        existing = repo.get_resumable_history_sync(
            cur,
            company_id=company_id,
            office_id=office_id,
            date_from=date_from,
            date_to=date_to,
        )

        if existing and resume:
            sync_id = int(existing["id"])
            pages_done = int(existing.get("pages_processed") or 0)
            records_done = int(existing.get("records_processed") or 0)
            start_offset = pages_done * LIMIT
            cur.execute(
                f"""
                UPDATE {repo.SYNC_RUNS}
                SET status = 'running', error_message = NULL, finished_at = NULL, duration_ms = NULL
                WHERE id = %s
                """,
                (sync_id,),
            )
        elif existing and not resume:
            return {
                "ok": False,
                "error": "Hay una carga histórica incompleta. Use resume=true para reanudar.",
                "sync_id": existing["id"],
                "pages_processed": existing.get("pages_processed"),
            }
        else:
            sync_id = repo.create_sync_run(
                cur,
                company_id=company_id,
                office_id=office_id,
                sync_type="history",
                date_from=date_from,
                date_to=date_to,
            )
            pages_done = 0
            records_done = 0
            start_offset = 0

        conn.commit()

        total_returns = 0
        total_details = 0
        max_return_ts = 0
        last_return_date: datetime | None = None
        last_return_id: int | None = None
        pages_this_run = 0

        for page in iter_returns_pages(
            client,
            company_id=company_id,
            office_id=office_id,
            date_from_ts=date_from_ts,
            date_to_ts=date_to_ts,
            start_offset=start_offset,
        ):
            r_up, d_up, page_max_ts, page_last_date, page_last_id = _process_page(
                cur,
                page,
                company_id=company_id,
                office_id=office_id,
                date_from_ts=date_from_ts,
                date_to_ts=date_to_ts,
                enforce_window=True,
            )
            total_returns += r_up
            total_details += d_up
            if page_max_ts > max_return_ts:
                max_return_ts = page_max_ts
            if page_last_date:
                last_return_date = page_last_date
                last_return_id = page_last_id

            pages_done += 1
            pages_this_run += 1
            records_done += r_up
            repo.update_sync_run_progress(
                cur,
                sync_id,
                pages_processed=pages_done,
                records_processed=records_done,
                last_return_date=last_return_date,
                last_return_id=last_return_id,
            )
            conn.commit()

        duration_ms = int((time.monotonic() - t0) * 1000)
        is_empty_bootstrap = (
            total_returns == 0
            and total_details == 0
            and pages_this_run == 0
        )

        if is_empty_bootstrap:
            repo.finish_sync_run(
                cur,
                sync_id,
                status="no_data",
                duration_ms=duration_ms,
                error_message="No se encontraron devoluciones para los parámetros enviados",
            )
            conn.commit()
            logger.warning(
                "[RETURNS_HISTORY] bootstrap vacío — status=no_data (no bloquea reintentos) "
                "company=%s office=%s window=%s..%s",
                company_id,
                office_id,
                date_from.isoformat(),
                date_to.isoformat(),
            )
            return {
                "ok": True,
                "bootstrap_status": "no_data",
                "sync_type": "history",
                "sync_id": sync_id,
                "company_id": company_id,
                "office_id": office_id,
                "company_name": company_name,
                "returns_upserted": 0,
                "details_upserted": 0,
                "pages_processed": pages_done,
                "date_from": date_from.isoformat(),
                "date_to": date_to.isoformat(),
                "resumed": resume and bool(existing),
                "duration_ms": duration_ms,
                "message": "Bootstrap sin datos — puede reejecutarse",
            }

        repo.finish_sync_run(cur, sync_id, status="completed", duration_ms=duration_ms)
        repo.upsert_sync_state(
            cur,
            company_id=company_id,
            office_id=office_id,
            last_return_ts=max_return_ts or date_to_ts,
            records_delta=total_returns,
        )
        conn.commit()

        logger.info(
            "[RETURNS_HISTORY] company=%s office=%s returns=%s details=%s pages=%s window=%s..%s",
            company_id,
            office_id,
            total_returns,
            total_details,
            pages_this_run,
            date_from.isoformat(),
            date_to.isoformat(),
        )
        return {
            "ok": True,
            "bootstrap_status": "completed",
            "sync_type": "history",
            "sync_id": sync_id,
            "company_id": company_id,
            "office_id": office_id,
            "company_name": company_name,
            "returns_upserted": total_returns,
            "details_upserted": total_details,
            "pages_processed": pages_done,
            "date_from": date_from.isoformat(),
            "date_to": date_to.isoformat(),
            "resumed": resume and bool(existing),
            "duration_ms": duration_ms,
        }
    except Exception as exc:
        conn.rollback()
        if sync_id is not None:
            try:
                duration_ms = int((time.monotonic() - t0) * 1000)
                repo.finish_sync_run(
                    cur,
                    sync_id,
                    status="failed",
                    duration_ms=duration_ms,
                    error_message=str(exc)[:2000],
                )
                conn.commit()
            except Exception:
                conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def sync_bsale_returns_incremental(
    *,
    company_id: int = COMPANY_ID,
    office_id: int = OFFICE_ID,
) -> dict[str, Any]:
    """Sincroniza devoluciones nuevas desde la última marca registrada."""
    conn = get_connection()
    conn.autocommit = False
    cur = conn.cursor()
    t0 = time.monotonic()
    sync_id: int | None = None

    try:
        repo.ensure_returns_schema(cur)
        conn.commit()

        history_done = repo.get_completed_history_sync(
            cur,
            company_id=company_id,
            office_id=office_id,
            date_from=HISTORY_DATE_FROM,
            date_to=HISTORY_DATE_TO,
        )
        if not history_done:
            return {
                "ok": False,
                "error": "Bootstrap histórico pendiente. Ejecute sync_bsale_returns_history primero.",
                "sync_type": "incremental",
            }

        loaded = _load_company_token(cur, company_id)
        if not loaded:
            return {"ok": False, "error": f"Sin token para company_id={company_id}"}
        company_name, token = loaded
        client = BsaleClient(token)

        state = repo.get_sync_state(cur, company_id=company_id, office_id=office_id)
        now = datetime.now(timezone.utc)
        if state and state.get("last_return_ts"):
            from_dt = datetime.fromtimestamp(
                int(state["last_return_ts"]),
                tz=timezone.utc,
            ) - timedelta(days=INCREMENTAL_OVERLAP_DAYS)
        else:
            from_dt = datetime.combine(HISTORY_DATE_TO, dt_time(23, 59, 59), tzinfo=timezone.utc)

        date_from_ts = int(from_dt.timestamp())
        date_to_ts = int(now.timestamp())
        date_from = from_dt.date()
        date_to = now.date()

        sync_id = repo.create_sync_run(
            cur,
            company_id=company_id,
            office_id=office_id,
            sync_type="incremental",
            date_from=date_from,
            date_to=date_to,
        )
        conn.commit()

        total_returns = 0
        total_details = 0
        max_return_ts = parse_optional_int(state.get("last_return_ts") if state else None) or 0
        last_return_date: datetime | None = None
        last_return_id: int | None = None
        pages_done = 0

        for page in iter_returns_pages(
            client,
            company_id=company_id,
            office_id=office_id,
            date_from_ts=date_from_ts,
            date_to_ts=date_to_ts,
        ):
            r_up, d_up, page_max_ts, page_last_date, page_last_id = _process_page(
                cur,
                page,
                company_id=company_id,
                office_id=office_id,
                enforce_window=False,
            )
            total_returns += r_up
            total_details += d_up
            if page_max_ts > max_return_ts:
                max_return_ts = page_max_ts
            if page_last_date:
                last_return_date = page_last_date
                last_return_id = page_last_id

            pages_done += 1
            repo.update_sync_run_progress(
                cur,
                sync_id,
                pages_processed=pages_done,
                records_processed=total_returns,
                last_return_date=last_return_date,
                last_return_id=last_return_id,
            )
            conn.commit()

        duration_ms = int((time.monotonic() - t0) * 1000)
        repo.finish_sync_run(cur, sync_id, status="completed", duration_ms=duration_ms)
        if total_returns > 0:
            repo.upsert_sync_state(
                cur,
                company_id=company_id,
                office_id=office_id,
                last_return_ts=max_return_ts or date_to_ts,
                records_delta=total_returns,
            )
        conn.commit()

        logger.info(
            "[RETURNS_INCREMENTAL] company=%s office=%s returns=%s details=%s pages=%s",
            company_id,
            office_id,
            total_returns,
            total_details,
            pages_done,
        )
        return {
            "ok": True,
            "sync_type": "incremental",
            "sync_id": sync_id,
            "company_id": company_id,
            "office_id": office_id,
            "company_name": company_name,
            "returns_upserted": total_returns,
            "details_upserted": total_details,
            "pages_processed": pages_done,
            "date_from": from_dt.isoformat(),
            "date_to": now.isoformat(),
            "duration_ms": duration_ms,
        }
    except Exception as exc:
        conn.rollback()
        if sync_id is not None:
            try:
                duration_ms = int((time.monotonic() - t0) * 1000)
                repo.finish_sync_run(
                    cur,
                    sync_id,
                    status="failed",
                    duration_ms=duration_ms,
                    error_message=str(exc)[:2000],
                )
                conn.commit()
            except Exception:
                conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()
