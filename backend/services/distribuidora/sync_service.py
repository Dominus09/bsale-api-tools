"""
Orquestación sync Bsale → ``distribuidora.*`` (documentos, detalles, atributos, referencias).
"""

from __future__ import annotations

import logging
import os
import random
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

import requests

from backend.db import get_connection
from backend.repositories.distribuidora.attributes_repo import replace_document_attributes
from backend.repositories.distribuidora.details_repo import replace_document_details
from backend.repositories.distribuidora.documents_repo import (
    document_dict_from_bsale,
    parse_document_sellers_response,
    update_document_seller_if_empty,
    upsert_documents,
)
from backend.repositories.distribuidora.references_repo import replace_document_references
from backend.repositories.distribuidora.sync_repo import (
    ensure_distribuidora_schema,
    finish_sync_log,
    get_last_sync,
    insert_sync_status_row,
    set_sync_state,
    start_sync_log,
)
from backend.services.distribuidora.bsale_client import BASE_BSALE, BsaleClient

logger = logging.getLogger(__name__)

ADVISORY_LOCK_KEY = 5_927_184_003
COMPANY_ID = 3
OFFICE_ID = 1
LIMIT_BSALE = 50
RESYNC_BSALE_BACKOFFS_SEC = (3, 5, 10, 20, 30)
RESYNC_HTTP_MAX_ATTEMPTS = 5
PROCESS_INCREMENTAL = "documents_incremental"
PROCESS_RESYNC = "documents_resync"
_FIRST_SYNC_CUTOFF = datetime(2010, 1, 1, tzinfo=timezone.utc)
_token_missing_logged = False


def _bsale_token() -> str:
    return (os.getenv("BSALE_TOKEN") or "").strip() or (os.getenv("BSALE_TOKEN_SPA") or "").strip()


def bsale_token_distribuidora_configured() -> bool:
    return bool(_bsale_token())


def _resync_page_limit() -> int:
    raw = int(os.getenv("DISTRIBUIDORA_RESYNC_PAGE_LIMIT", str(LIMIT_BSALE)))
    return max(25, min(50, raw))


def _utc_day_timestamp_bounds(d: date) -> tuple[int, int]:
    """
    Límites UTC de un día calendario para ``emissiondaterange`` en Bsale.

    Misma idea que ``sync_documents.py``: inicio 00:00 UTC y fin
    ``(día+1).00:00 UTC - 1`` segundo (inclusive en la práctica para la API).
    """
    day_start = datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=timezone.utc)
    end_ts = int((day_start + timedelta(days=1)).timestamp()) - 1
    return int(day_start.timestamp()), end_ts


def _documents_get_resync(client: BsaleClient, params: dict[str, Any]) -> dict[str, Any]:
    """
    GET ``/documents.json`` con reintentos ante 502/503/504, 500 y errores de red.
    Hasta 5 intentos con backoff 3 → 5 → 10 → 20 → 30 s entre reintentos.

    Siempre envía ``officeId`` = sucursal Distribuidora (``OFFICE_ID``).
    """
    params = {**params, "officeId": OFFICE_ID}
    url = f"{BASE_BSALE}/documents.json"
    backoffs = RESYNC_BSALE_BACKOFFS_SEC
    http_retries = 0
    while True:
        try:
            r = client.session.get(
                url,
                headers={"access_token": client.access_token},
                params=params,
                timeout=90,
            )
        except requests.RequestException as e:
            if http_retries >= RESYNC_HTTP_MAX_ATTEMPTS - 1:
                raise RuntimeError(f"Bsale /documents.json red tras {RESYNC_HTTP_MAX_ATTEMPTS} intentos: {e}") from e
            wait = backoffs[min(http_retries, len(backoffs) - 1)]
            logger.warning(
                "Retry intento %s (red) — esperando %ss: %s",
                http_retries + 1,
                wait,
                e,
            )
            time.sleep(wait)
            http_retries += 1
            continue

        if r.status_code == 401:
            raise RuntimeError(
                "Bsale 401 Unauthorized — revisar BSALE_TOKEN o BSALE_TOKEN_SPA"
            )

        if r.status_code == 429:
            try:
                wait = int(r.json().get("retry_after", 60))
            except Exception:
                wait = 60
            logger.warning("Bsale 429 — esperando %s s", wait)
            time.sleep(wait)
            continue

        if r.status_code in (500, 502, 503, 504):
            if http_retries >= RESYNC_HTTP_MAX_ATTEMPTS - 1:
                raise RuntimeError(
                    f"Bsale HTTP {r.status_code} en /documents.json tras {RESYNC_HTTP_MAX_ATTEMPTS} intentos"
                )
            wait = backoffs[min(http_retries, len(backoffs) - 1)]
            logger.warning(
                "Retry intento %s por HTTP %s — esperando %ss",
                http_retries + 1,
                r.status_code,
                wait,
            )
            time.sleep(wait)
            http_retries += 1
            continue

        if not (200 <= r.status_code < 300):
            raise RuntimeError(f"Bsale HTTP {r.status_code}: {(r.text or '')[:500]}")

        return r.json()


def _append_items_from_bsale_response(
    items: list[dict[str, Any]],
    pending: list[dict[str, Any]],
    stats: dict[str, Any],
) -> None:
    for d in items:
        row = document_dict_from_bsale(
            d,
            company_id=COMPANY_ID,
            default_office_id=OFFICE_ID,
            sync_stats=stats,
        )
        if row is None:
            continue
        pending.append(row)


def _flush_pending_when_large(
    client: BsaleClient,
    cur,
    conn,
    pending: list[dict[str, Any]],
    stats: dict[str, Any],
    *,
    min_batch: int = 200,
) -> None:
    if len(pending) < min_batch:
        return
    to_save = list(pending)
    pending.clear()
    try:
        upsert_documents(cur, to_save)
        for r in to_save:
            _refresh_document_children(
                client,
                cur,
                int(r["document_id"]),
                r.get("document_type_id"),
                stats,
            )
        conn.commit()
        stats["documents_processed"] += len(to_save)
    except Exception as e:
        logger.exception(
            "distribuidora: lote documentos falló (%s filas), rollback y se continúa: %s",
            len(to_save),
            e,
        )
        try:
            conn.rollback()
        except Exception:
            logger.exception("distribuidora: rollback tras error de lote")
        stats["documents_batch_failures"] = int(stats.get("documents_batch_failures") or 0) + 1


def _flush_pending_tail(
    client: BsaleClient,
    cur,
    conn,
    pending: list[dict[str, Any]],
    stats: dict[str, Any],
) -> None:
    if not pending:
        return
    to_save = list(pending)
    pending.clear()
    try:
        upsert_documents(cur, to_save)
        for r in to_save:
            _refresh_document_children(
                client,
                cur,
                int(r["document_id"]),
                r.get("document_type_id"),
                stats,
            )
        conn.commit()
        stats["documents_processed"] += len(to_save)
    except Exception as e:
        logger.exception(
            "distribuidora: último lote documentos falló (%s filas), rollback: %s",
            len(to_save),
            e,
        )
        try:
            conn.rollback()
        except Exception:
            logger.exception("distribuidora: rollback tras error de último lote")
        stats["documents_batch_failures"] = int(stats.get("documents_batch_failures") or 0) + 1


def _fetch_documents_single_day_resync(
    client: BsaleClient,
    cur,
    conn,
    day: date,
    stats: dict[str, Any],
    *,
    page_limit: int | None = None,
) -> None:
    """Un día UTC completo: paginación con offset reiniciado y pausas entre llamadas a Bsale."""
    pl = page_limit if page_limit is not None else _resync_page_limit()
    desde_ts, hasta_ts = _utc_day_timestamp_bounds(day)
    day_start = datetime(day.year, day.month, day.day, 0, 0, 0, tzinfo=timezone.utc)
    day_end = datetime(day.year, day.month, day.day, 23, 59, 59, tzinfo=timezone.utc)
    logger.info(
        "➡️ Rango enviado a Bsale (UTC): %s → %s | emissiondaterange epoch [%s,%s]",
        day_start.isoformat(),
        day_end.isoformat(),
        desde_ts,
        hasta_ts,
    )
    logger.info("📅 Procesando día: %s", day.isoformat())
    pending: list[dict[str, Any]] = []
    offset = 0
    while True:
        logger.info("📄 Offset: %s (día %s)", offset, day.isoformat())
        params = {
            "limit": pl,
            "offset": offset,
            "emissiondaterange": f"[{desde_ts},{hasta_ts}]",
        }
        data = _documents_get_resync(client, params)
        items = data.get("items") or []
        logger.info(
            "📊 Docs en respuesta API (officeId=%s + validación backend): %s",
            OFFICE_ID,
            len(items),
        )
        if not items:
            break
        _append_items_from_bsale_response(items, pending, stats)
        _flush_pending_when_large(client, cur, conn, pending, stats)
        offset += len(items)
        time.sleep(random.uniform(0.2, 0.5))
    _flush_pending_tail(client, cur, conn, pending, stats)
    logger.info("✅ Día completado: %s", day.isoformat())


def _refresh_document_children(
    client: BsaleClient,
    cur,
    document_id: int,
    document_type_id: int | None,
    stats: dict[str, Any],
) -> None:
    try:
        det = client.get(f"/documents/{document_id}/details.json")
        items = det.get("items") or []
        n = replace_document_details(cur, document_id, items)
        stats["details_rows"] += n
    except Exception as e:
        logger.warning("details document_id=%s: %s", document_id, e)

    if document_type_id == 33:
        try:
            ad = client.get(f"/documents/{document_id}/attributes.json")
            if not isinstance(ad, dict):
                ad = {"_raw": ad}
            n = replace_document_attributes(cur, document_id, ad)
            stats["attributes_rows"] += n
        except Exception as e:
            logger.warning("attributes document_id=%s: %s", document_id, e)

    if document_type_id in (1, 6, 9, 33):
        try:
            rd = client.get(f"/documents/{document_id}/references.json")
            if not isinstance(rd, dict):
                rd = {"_raw": rd}
            n = replace_document_references(cur, document_id, rd)
            stats["references_rows"] += n
        except Exception as e:
            logger.warning("references document_id=%s: %s", document_id, e)

    _sync_document_sellers_if_empty(client, cur, document_id, document_type_id, stats)


def _sync_document_sellers_if_empty(
    client: BsaleClient,
    cur,
    document_id: int,
    document_type_id: int | None,
    stats: dict[str, Any],
) -> None:
    """GET ``/documents/{id}/sellers.json`` y guarda vendedor solo si ``seller_name`` está vacío."""
    if document_type_id != 33:
        return
    cur.execute(
        """
        SELECT seller_name
        FROM distribuidora.documents
        WHERE document_id = %s
        """,
        (document_id,),
    )
    row = cur.fetchone()
    if row is None:
        return
    if row[0] is not None and str(row[0]).strip() != "":
        return
    try:
        data = client.get(f"/documents/{document_id}/sellers.json")
    except Exception as e:
        logger.warning("sellers.json document_id=%s: %s", document_id, e)
        return
    sid, sname = parse_document_sellers_response(data)
    if not sname:
        return
    try:
        if update_document_seller_if_empty(cur, document_id, sid, sname):
            stats["sellers_filled"] = int(stats.get("sellers_filled") or 0) + 1
    except Exception as e:
        logger.warning("update seller document_id=%s: %s", document_id, e)


def _fetch_documents_window(
    client: BsaleClient,
    cur,
    conn,
    *,
    desde_ts: int,
    hasta_ts: int,
    stats: dict[str, Any],
    log_id: int | None,
) -> None:
    """Paginación por ``offset``; mismo cliente robusto que resync (429/5xx/red)."""
    offset = 0
    pending: list[dict[str, Any]] = []
    while True:
        params = {
            "limit": LIMIT_BSALE,
            "offset": offset,
            "emissiondaterange": f"[{desde_ts},{hasta_ts}]",
        }
        data = _documents_get_resync(client, params)
        items = data.get("items") or []
        if not items:
            break

        _append_items_from_bsale_response(items, pending, stats)
        _flush_pending_when_large(client, cur, conn, pending, stats)

        offset += len(items)
        time.sleep(random.uniform(0.15, 0.45))

    _flush_pending_tail(client, cur, conn, pending, stats)

    stats["documents_inserted"] = stats["documents_processed"]
    stats["documents_updated"] = stats["documents_processed"]
    stats["details_inserted"] = stats.get("details_rows", 0)
    stats["attributes_inserted"] = stats.get("attributes_rows", 0)
    stats["references_inserted"] = stats.get("references_rows", 0)
    if log_id is not None:
        finish_sync_log(
            cur,
            log_id,
            status="ok",
            stats=stats,
            message=None,
        )
        conn.commit()


def sync_bsale_distribuidora_incremental(*, strict_token: bool = False) -> dict[str, Any]:
    global _token_missing_logged
    t0 = time.perf_counter()
    token = _bsale_token()
    if not token:
        if strict_token:
            raise ValueError(
                "Ningún token Bsale: defina BSALE_TOKEN o BSALE_TOKEN_SPA (p. ej. en Coolify)."
            )
        if not _token_missing_logged:
            logger.warning("Sin BSALE_TOKEN ni BSALE_TOKEN_SPA: sync distribuidora omitido.")
            _token_missing_logged = True
        return {
            "mode": "incremental",
            "skipped": True,
            "skip_reason": "BSALE_TOKEN / BSALE_TOKEN_SPA no configuradas",
            "duration_seconds": round(time.perf_counter() - t0, 3),
            "omitido_concurrencia": False,
        }
    _token_missing_logged = False

    stats: dict[str, Any] = {
        "mode": "incremental",
        "documents_processed": 0,
        "documents_inserted": 0,
        "documents_updated": 0,
        "documents_batch_failures": 0,
        "skipped_other_office": 0,
        "skipped_other_company": 0,
        "details_inserted": 0,
        "attributes_inserted": 0,
        "references_inserted": 0,
        "details_rows": 0,
        "attributes_rows": 0,
        "references_rows": 0,
        "sellers_filled": 0,
        "duration_seconds": 0.0,
        "omitido_concurrencia": False,
        "skipped": False,
        "errors": None,
    }

    conn = get_connection()
    got_lock = False
    log_id: int | None = None
    try:
        cur = conn.cursor()
        cur.execute("SELECT pg_try_advisory_lock(%s)", (ADVISORY_LOCK_KEY,))
        got_lock = bool(cur.fetchone()[0])
        if not got_lock:
            stats["omitido_concurrencia"] = True
            cur.close()
            stats["duration_seconds"] = round(time.perf_counter() - t0, 3)
            return stats

        ensure_distribuidora_schema(cur)
        conn.commit()

        log_id = start_sync_log(cur, PROCESS_INCREMENTAL)
        conn.commit()

        last_sync = get_last_sync(cur, PROCESS_INCREMENTAL)
        if last_sync is None:
            last_sync = datetime(2000, 1, 1, tzinfo=timezone.utc)
        elif last_sync.tzinfo is None:
            last_sync = last_sync.replace(tzinfo=timezone.utc)

        now = datetime.now(timezone.utc)
        if last_sync < _FIRST_SYNC_CUTOFF:
            desde = now - timedelta(days=30)
            logger.info("sync incremental: primer ciclo amplio (30 días)")
        else:
            desde = last_sync - timedelta(hours=2)

        desde_ts = int(desde.timestamp())
        hasta_ts = int(now.timestamp())
        if desde_ts >= hasta_ts:
            desde_ts = hasta_ts - 3600

        client = BsaleClient(token)
        logger.info(
            "distribuidora sync incremental: procesando solo company_id=%s office_id=%s "
            "(Bsale GET /documents.json con officeId=%s)",
            COMPANY_ID,
            OFFICE_ID,
            OFFICE_ID,
        )
        _fetch_documents_window(
            client, cur, conn, desde_ts=desde_ts, hasta_ts=hasta_ts, stats=stats, log_id=log_id
        )

        insert_sync_status_row(
            cur,
            sync_type="documents",
            records_processed=int(stats.get("documents_processed", 0)),
            status="success",
        )
        insert_sync_status_row(
            cur,
            sync_type="details",
            records_processed=int(stats.get("details_rows", 0)),
            status="success",
        )
        insert_sync_status_row(cur, sync_type="orders", records_processed=0, status="success")
        insert_sync_status_row(cur, sync_type="sales", records_processed=0, status="success")

        set_sync_state(
            cur,
            process_name=PROCESS_INCREMENTAL,
            last_sync=now,
            last_status="ok",
            last_message=f"processed={stats['documents_processed']}",
        )
        conn.commit()
        cur.close()

        logger.info(
            "Total documentos guardados office %s: %s (omitidos otra office=%s otra company=%s)",
            OFFICE_ID,
            stats["documents_processed"],
            stats.get("skipped_other_office", 0),
            stats.get("skipped_other_company", 0),
        )
        logger.info(
            "sync distribuidora incremental OK: processed=%s details=%s attr=%s ref=%s s=%.2f",
            stats["documents_processed"],
            stats["details_inserted"],
            stats["attributes_inserted"],
            stats["references_inserted"],
            time.perf_counter() - t0,
        )
    except Exception as e:
        logger.exception("sync distribuidora incremental: %s", e)
        stats["errors"] = str(e)
        try:
            c2 = conn.cursor()
            if log_id is not None:
                finish_sync_log(c2, log_id, status="error", stats=stats, message=str(e))
            conn.commit()
            c2.close()
        except Exception:
            logger.exception("sync_log error")
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        try:
            if got_lock:
                c3 = conn.cursor()
                c3.execute("SELECT pg_advisory_unlock(%s)", (ADVISORY_LOCK_KEY,))
                c3.close()
        except Exception:
            logger.exception("advisory unlock incremental")
        try:
            conn.close()
        except Exception:
            pass

    stats["duration_seconds"] = round(time.perf_counter() - t0, 3)
    return stats


def resync_bsale_distribuidora_range(
    *,
    emission_from: datetime | None = None,
    emission_to: datetime | None = None,
    strict_token: bool = True,
) -> dict[str, Any]:
    token = _bsale_token()
    if not token:
        if strict_token:
            raise ValueError("Ningún token Bsale: defina BSALE_TOKEN o BSALE_TOKEN_SPA.")
        return {"skipped": True, "skip_reason": "sin token"}

    t0 = time.perf_counter()
    stats: dict[str, Any] = {
        "mode": "resync_range",
        "documents_processed": 0,
        "documents_inserted": 0,
        "documents_updated": 0,
        "documents_batch_failures": 0,
        "skipped_other_office": 0,
        "skipped_other_company": 0,
        "details_inserted": 0,
        "attributes_inserted": 0,
        "references_inserted": 0,
        "details_rows": 0,
        "attributes_rows": 0,
        "references_rows": 0,
        "sellers_filled": 0,
        "days_processed": 0,
        "duration_seconds": 0.0,
        "omitido_concurrencia": False,
        "errors": None,
    }

    conn = get_connection()
    got_lock = False
    log_id: int | None = None
    try:
        cur = conn.cursor()
        cur.execute("SELECT pg_try_advisory_lock(%s)", (ADVISORY_LOCK_KEY,))
        got_lock = bool(cur.fetchone()[0])
        if not got_lock:
            stats["omitido_concurrencia"] = True
            cur.close()
            stats["duration_seconds"] = round(time.perf_counter() - t0, 3)
            return stats

        ensure_distribuidora_schema(cur)
        conn.commit()

        log_id = start_sync_log(cur, PROCESS_RESYNC)
        conn.commit()

        now = datetime.now(timezone.utc)
        if emission_to is None:
            emission_to = now
        if emission_to.tzinfo is None:
            emission_to = emission_to.replace(tzinfo=timezone.utc)
        if emission_from is None:
            cur.execute(
                """
                SELECT MIN(emission_date)
                FROM distribuidora.documents
                WHERE company_id = %s AND office_id = %s
                """,
                (COMPANY_ID, OFFICE_ID),
            )
            row = cur.fetchone()
            min_em = row[0] if row and row[0] else None
            if min_em is None:
                fb_days = max(1, int(os.getenv("DISTRIBUIDORA_RESYNC_FALLBACK_DAYS", "90")))
                emission_from = now - timedelta(days=fb_days)
            else:
                emission_from = min_em if min_em.tzinfo else min_em.replace(tzinfo=timezone.utc)
        elif emission_from.tzinfo is None:
            emission_from = emission_from.replace(tzinfo=timezone.utc)

        if emission_from > emission_to:
            emission_from, emission_to = emission_to, emission_from

        logger.info(
            "Resync distribuidora desde %s hasta %s (UTC), modo día a día",
            emission_from.isoformat(),
            emission_to.isoformat(),
        )

        client = BsaleClient(token)
        start_d = emission_from.astimezone(timezone.utc).date()
        end_d = emission_to.astimezone(timezone.utc).date()
        page_lim = _resync_page_limit()
        num_calendar_days = (end_d - start_d).days + 1
        logger.info(
            "Resync: recorriendo %s día(s) calendario UTC (%s .. %s inclusive)",
            num_calendar_days,
            start_d.isoformat(),
            end_d.isoformat(),
        )
        logger.info(
            "distribuidora resync documentos: procesando solo company_id=%s office_id=%s "
            "(Bsale GET /documents.json con officeId=%s)",
            COMPANY_ID,
            OFFICE_ID,
            OFFICE_ID,
        )

        current = start_d
        while current <= end_d:
            while True:
                try:
                    _fetch_documents_single_day_resync(
                        client,
                        cur,
                        conn,
                        current,
                        stats,
                        page_limit=page_lim,
                    )
                    stats["days_processed"] += 1
                    break
                except Exception as e:
                    logger.exception(
                        "distribuidora resync: día %s falló; se reintenta el mismo día: %s",
                        current.isoformat(),
                        e,
                    )
                    time.sleep(5)

            current += timedelta(days=1)

        stats["documents_inserted"] = stats["documents_processed"]
        stats["documents_updated"] = stats["documents_processed"]
        stats["details_inserted"] = stats.get("details_rows", 0)
        stats["attributes_inserted"] = stats.get("attributes_rows", 0)
        stats["references_inserted"] = stats.get("references_rows", 0)

        insert_sync_status_row(
            cur,
            sync_type="documents",
            records_processed=int(stats.get("documents_processed", 0)),
            status="success",
        )
        insert_sync_status_row(
            cur,
            sync_type="details",
            records_processed=int(stats.get("details_rows", 0)),
            status="success",
        )
        insert_sync_status_row(cur, sync_type="orders", records_processed=0, status="success")
        insert_sync_status_row(cur, sync_type="sales", records_processed=0, status="success")

        if log_id is not None:
            finish_sync_log(cur, log_id, status="ok", stats=stats, message="resync_range completo")
        set_sync_state(
            cur,
            process_name=PROCESS_RESYNC,
            last_sync=emission_to,
            last_status="ok",
            last_message=(
                f"days={stats['days_processed']} processed={stats['documents_processed']}"
            ),
        )
        conn.commit()
        cur.close()
        logger.info(
            "Total documentos guardados office %s: %s (omitidos otra office=%s otra company=%s)",
            OFFICE_ID,
            stats["documents_processed"],
            stats.get("skipped_other_office", 0),
            stats.get("skipped_other_company", 0),
        )
        logger.info(
            "resync distribuidora OK: days=%s processed=%s s=%.2f",
            stats["days_processed"],
            stats["documents_processed"],
            time.perf_counter() - t0,
        )
    except Exception as e:
        logger.exception("resync distribuidora: %s", e)
        stats["errors"] = str(e)
        try:
            c2 = conn.cursor()
            if log_id is not None:
                finish_sync_log(c2, log_id, status="error", stats=stats, message=str(e))
            conn.commit()
            c2.close()
        except Exception:
            pass
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        try:
            if got_lock:
                c3 = conn.cursor()
                c3.execute("SELECT pg_advisory_unlock(%s)", (ADVISORY_LOCK_KEY,))
                c3.close()
        except Exception:
            logger.exception("advisory unlock resync")
        try:
            conn.close()
        except Exception:
            pass

    stats["duration_seconds"] = round(time.perf_counter() - t0, 3)
    return stats


def run_incremental_distribuidora_background() -> None:
    try:
        sync_bsale_distribuidora_incremental(strict_token=True)
    except Exception:
        logger.exception("incremental background")


def run_resync_distribuidora_background(
    emission_from_iso: str | None = None,
    emission_to_iso: str | None = None,
) -> None:
    _GARBAGE = frozenset(
        ("", "string", "null", "undefined", "none", "nan", "-"),
    )

    def _parse(s: str | None) -> datetime | None:
        if s is None:
            return None
        if not isinstance(s, str):
            t = str(s).strip()
        else:
            t = s.strip()
        if not t or t.lower() in _GARBAGE:
            return None
        try:
            if len(t) == 10 and t[4] == "-" and t[7] == "-":
                return datetime.strptime(t, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            return datetime.fromisoformat(t.replace("Z", "+00:00"))
        except ValueError:
            logger.warning(
                "fecha resync inválida %r — se usará rango por defecto (MIN en BD o fallback días)",
                s,
            )
            return None

    try:
        resync_bsale_distribuidora_range(
            emission_from=_parse(emission_from_iso),
            emission_to=_parse(emission_to_iso),
            strict_token=True,
        )
    except Exception:
        logger.exception("resync background")


class DistribuidoraSyncService:
    """Fachada opcional para tests o DI."""

    @staticmethod
    def run_incremental(*, strict_token: bool = False) -> dict[str, Any]:
        return sync_bsale_distribuidora_incremental(strict_token=strict_token)

    @staticmethod
    def run_resync(
        *,
        emission_from: datetime | None = None,
        emission_to: datetime | None = None,
        strict_token: bool = True,
    ) -> dict[str, Any]:
        return resync_bsale_distribuidora_range(
            emission_from=emission_from,
            emission_to=emission_to,
            strict_token=strict_token,
        )
