"""
Sync incremental de documentos relacionados a líneas OC (GET ``/documents.json?relateddetailid=``).

Desacoplado del sync principal; escribe ``distribuidora.document_related`` con deduplicación.

Incluye ``sync_related_documents_range`` para rellenar histórico por rango de emisión (día a día).
"""

from __future__ import annotations

import logging
import os
import random
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, TypeVar

import psycopg2.errors
from psycopg2.extensions import connection as PgConnection

from backend.db import get_connection
from backend.repositories.distribuidora.sync_repo import (
    ensure_distribuidora_schema,
    insert_sync_status_row,
)
from backend.services.distribuidora.bsale_client import BsaleClient

logger = logging.getLogger(__name__)

# Lock exclusivo global para **todo** trabajo sobre ``document_related`` (incremental + rango).
# Misma sesión: try_advisory_lock al abrir conexión, unlock en ``finally`` antes de ``close``.
ADVISORY_LOCK_RELATED = 5_927_184_005

DEADLOCK_MAX_ATTEMPTS = 5
DEADLOCK_SLEEP_SEC = 2.5

T = TypeVar("T")
COMPANY_ID = 3
OFFICE_ID = 1
DOC_TYPE_OC = 33
RELATED_PAGE_LIMIT = 50


def _utc_day_emission_bounds(d: date) -> tuple[datetime, datetime]:
    """Inicio UTC del día y fin exclusivo (``[start, end)``) para filtrar ``emission_date``."""
    start = datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=timezone.utc)
    end_excl = start + timedelta(days=1)
    return start, end_excl


def _safe_int(v: Any) -> int | None:
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _bsale_token() -> str:
    return (os.getenv("BSALE_TOKEN") or "").strip() or (os.getenv("BSALE_TOKEN_SPA") or "").strip()


def _with_deadlock_retry(conn: PgConnection, label: str, fn: Callable[[], T]) -> T:
    """Ejecuta ``fn`` (debe hacer ``commit`` o solo lecturas acotadas) y reintenta ante deadlock."""
    for attempt in range(1, DEADLOCK_MAX_ATTEMPTS + 1):
        try:
            return fn()
        except psycopg2.errors.DeadlockDetected:
            logger.warning(
                "DeadlockDetected (%s) intento %s/%s — rollback y reintento",
                label,
                attempt,
                DEADLOCK_MAX_ATTEMPTS,
            )
            try:
                conn.rollback()
            except Exception:
                logger.exception("rollback tras deadlock (%s)", label)
            if attempt >= DEADLOCK_MAX_ATTEMPTS:
                raise
            time.sleep(DEADLOCK_SLEEP_SEC + random.uniform(0, 0.5))
    raise RuntimeError("unreachable")


def _fetch_oc_detail_ids(
    cur,
    *,
    lookback_days: int,
    limit_details: int,
) -> list[int]:
    cur.execute(
        """
        SELECT DISTINCT dd.detail_id
        FROM distribuidora.document_details dd
        INNER JOIN distribuidora.documents d
            ON d.document_id = dd.document_id
        WHERE d.document_type_id = %s
          AND d.company_id = %s
          AND d.office_id = %s
          AND d.emission_date >= (NOW() AT TIME ZONE 'UTC' - (%s * interval '1 day'))
        ORDER BY dd.detail_id DESC
        LIMIT %s
        """,
        (DOC_TYPE_OC, COMPANY_ID, OFFICE_ID, max(1, lookback_days), limit_details),
    )
    return [int(r[0]) for r in cur.fetchall()]


def _fetch_oc_detail_ids_for_emission_day(cur, day: date) -> list[int]:
    """``detail_id`` distintos de líneas de OC cuyo documento emite ese día calendario (UTC)."""
    day_start, day_end_excl = _utc_day_emission_bounds(day)
    cur.execute(
        """
        SELECT DISTINCT dd.detail_id
        FROM distribuidora.document_details dd
        INNER JOIN distribuidora.documents d
            ON d.document_id = dd.document_id
        WHERE d.document_type_id = %s
          AND d.company_id = %s
          AND d.office_id = %s
          AND d.emission_date IS NOT NULL
          AND d.emission_date >= %s
          AND d.emission_date < %s
        ORDER BY dd.detail_id
        """,
        (DOC_TYPE_OC, COMPANY_ID, OFFICE_ID, day_start, day_end_excl),
    )
    return [int(r[0]) for r in cur.fetchall()]


def _fetch_and_persist_related_for_detail(
    client: BsaleClient,
    conn: PgConnection,
    cur,
    detail_id: int,
    *,
    throttle: float,
) -> tuple[int, int, int]:
    """
    GET ``/documents.json`` con ``relateddetailid`` y paginación; inserta con ``ON CONFLICT DO NOTHING``.

    Retorna ``(items_api_total, filas_insertadas, llamadas_http)``.
    """
    items_api_total = 0
    rows_inserted = 0
    api_calls = 0
    offset = 0
    while True:
        try:
            data = client.get(
                "/documents.json",
                {
                    "relateddetailid": detail_id,
                    "limit": RELATED_PAGE_LIMIT,
                    "offset": offset,
                },
            )
        except Exception as e:
            logger.warning("relateddetailid=%s offset=%s: %s", detail_id, offset, e)
            break
        api_calls += 1
        items = data.get("items") or []
        if not items:
            break
        items_api_total += len(items)
        rows_inserted += _insert_related_rows(conn, cur, detail_id, items)
        offset += RELATED_PAGE_LIMIT
        if throttle > 0:
            time.sleep(throttle)
    return items_api_total, rows_inserted, api_calls


def _insert_related_rows(
    conn: PgConnection,
    cur,
    detail_id: int,
    items: list[dict[str, Any]],
) -> int:
    """
    Inserta relaciones con ``ON CONFLICT DO NOTHING``.

    Una transacción corta por fila (execute + commit) con reintento ante deadlock.
    """
    n = 0
    for it in items:
        rid = _safe_int(it.get("id"))
        if rid is None:
            continue
        dt = it.get("documentType") or it.get("document_type") or {}
        tid = _safe_int(dt.get("id") if isinstance(dt, dict) else None)
        if tid is None:
            continue

        def _insert_one(
            _rid: int = rid,
            _tid: int = tid,
        ) -> int:
            cur.execute(
                """
                INSERT INTO distribuidora.document_related (
                    detail_id, related_document_id, related_document_type
                )
                VALUES (%s, %s, %s)
                ON CONFLICT (detail_id, related_document_id) DO NOTHING
                """,
                (detail_id, _rid, _tid),
            )
            rc = int(cur.rowcount or 0)
            conn.commit()
            return rc

        inserted = _with_deadlock_retry(
            conn,
            f"document_related detail_id={detail_id} related_document_id={rid}",
            _insert_one,
        )
        n += inserted
    return n


def sync_distribuidora_related_documents(
    *,
    strict_token: bool = False,
    lookback_days: int | None = None,
    limit_details: int | None = None,
) -> dict[str, Any]:
    """
    Para cada ``detail_id`` reciente de OC, consulta Bsale y persiste relaciones.

    Env:
      DISTRIBUIDORA_RELATED_LOOKBACK_DAYS (default 7)
      DISTRIBUIDORA_RELATED_DETAIL_LIMIT (default 250)
    """
    token = _bsale_token()
    if not token:
        if strict_token:
            raise ValueError("Ningún token Bsale: defina BSALE_TOKEN o BSALE_TOKEN_SPA.")
        return {"skipped": True, "skip_reason": "sin token", "inserted": 0}

    lb = lookback_days if lookback_days is not None else int(os.getenv("DISTRIBUIDORA_RELATED_LOOKBACK_DAYS", "7"))
    lim = limit_details if limit_details is not None else int(os.getenv("DISTRIBUIDORA_RELATED_DETAIL_LIMIT", "250"))

    t0 = time.perf_counter()
    stats: dict[str, Any] = {
        "details_considered": 0,
        "rows_inserted": 0,
        "api_calls": 0,
        "duration_seconds": 0.0,
        "skipped": False,
        "omitido_concurrencia": False,
        "errors": None,
    }

    conn = get_connection()
    got_lock = False
    try:
        cur = conn.cursor()
        cur.execute("SELECT pg_try_advisory_lock(%s)", (ADVISORY_LOCK_RELATED,))
        got_lock = bool(cur.fetchone()[0])
        if not got_lock:
            stats["omitido_concurrencia"] = True
            cur.close()
            stats["duration_seconds"] = round(time.perf_counter() - t0, 3)
            return stats

        ensure_distribuidora_schema(cur)
        conn.commit()

        detail_ids = _fetch_oc_detail_ids(cur, lookback_days=lb, limit_details=lim)
        stats["details_considered"] = len(detail_ids)

        client = BsaleClient(token)
        throttle = float(os.getenv("DISTRIBUIDORA_RELATED_API_DELAY_SEC", "0.12"))

        for did in detail_ids:
            try:
                _rels, ins, calls = _fetch_and_persist_related_for_detail(
                    client, conn, cur, did, throttle=throttle
                )
            except Exception as e:
                logger.warning("relateddetailid=%s: %s", did, e)
                continue
            stats["api_calls"] += calls
            stats["rows_inserted"] += ins
            if throttle > 0:
                time.sleep(throttle)

        def _finalize_incremental() -> None:
            insert_sync_status_row(
                cur,
                sync_type="related",
                records_processed=int(stats["rows_inserted"]),
                status="success",
            )
            conn.commit()

        _with_deadlock_retry(conn, "related incremental insert_sync_status", _finalize_incremental)
        cur.close()
        logger.info(
            "sync related OK: details=%s inserted=%s api=%s s=%.2f",
            stats["details_considered"],
            stats["rows_inserted"],
            stats["api_calls"],
            time.perf_counter() - t0,
        )
    except Exception as e:
        logger.exception("sync related: %s", e)
        stats["errors"] = str(e)
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        try:
            if got_lock:
                c2 = conn.cursor()
                c2.execute("SELECT pg_advisory_unlock(%s)", (ADVISORY_LOCK_RELATED,))
                c2.close()
        except Exception:
            logger.exception("advisory unlock related")
        try:
            conn.close()
        except Exception:
            pass

    stats["duration_seconds"] = round(time.perf_counter() - t0, 3)
    return stats


def sync_related_documents_range(
    *,
    start_date: date,
    end_date: date,
    strict_token: bool = True,
) -> dict[str, Any]:
    """
    Rellena ``distribuidora.document_related`` para OC (tipo 33) con emisión entre ``start_date`` y ``end_date``
    (inclusive, días calendario UTC), recorriendo **día a día**.

    Por cada ``detail_id`` de esas OC: GET ``/documents.json?relateddetailid=`` (paginado).
    """
    if start_date > end_date:
        start_date, end_date = end_date, start_date

    token = _bsale_token()
    if not token:
        if strict_token:
            raise ValueError("Ningún token Bsale: defina BSALE_TOKEN o BSALE_TOKEN_SPA.")
        return {"skipped": True, "skip_reason": "sin token"}

    t0 = time.perf_counter()
    stats: dict[str, Any] = {
        "mode": "related_range",
        "days_processed": 0,
        "details_processed": 0,
        "rows_inserted": 0,
        "api_calls": 0,
        "relations_found": 0,
        "duration_seconds": 0.0,
        "omitido_concurrencia": False,
        "errors": None,
    }

    conn = get_connection()
    got_lock = False
    try:
        cur = conn.cursor()
        cur.execute("SELECT pg_try_advisory_lock(%s)", (ADVISORY_LOCK_RELATED,))
        got_lock = bool(cur.fetchone()[0])
        if not got_lock:
            stats["omitido_concurrencia"] = True
            cur.close()
            stats["duration_seconds"] = round(time.perf_counter() - t0, 3)
            return stats

        ensure_distribuidora_schema(cur)
        conn.commit()

        client = BsaleClient(token)
        throttle = float(os.getenv("DISTRIBUIDORA_RELATED_API_DELAY_SEC", "0.12"))

        current = start_date
        while current <= end_date:
            logger.info("Procesando related día: %s", current.isoformat())
            detail_ids = _fetch_oc_detail_ids_for_emission_day(cur, current)
            for did in detail_ids:
                logger.info("Detail procesado: %s", did)
                items_total, ins, calls = _fetch_and_persist_related_for_detail(
                    client, conn, cur, did, throttle=throttle
                )
                stats["api_calls"] += calls
                stats["details_processed"] += 1
                stats["rows_inserted"] += ins
                stats["relations_found"] += items_total
                logger.info("Relaciones encontradas: %s", items_total)

            stats["days_processed"] += 1
            current += timedelta(days=1)

        def _finalize_range() -> None:
            insert_sync_status_row(
                cur,
                sync_type="related",
                records_processed=int(stats["rows_inserted"]),
                status="success",
            )
            conn.commit()

        _with_deadlock_retry(conn, "related range insert_sync_status", _finalize_range)
        cur.close()
        logger.info(
            "sync related range OK: days=%s details=%s inserted=%s relations=%s s=%.2f",
            stats["days_processed"],
            stats["details_processed"],
            stats["rows_inserted"],
            stats["relations_found"],
            time.perf_counter() - t0,
        )
    except Exception as e:
        logger.exception("sync related range: %s", e)
        stats["errors"] = str(e)
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        try:
            if got_lock:
                c2 = conn.cursor()
                c2.execute("SELECT pg_advisory_unlock(%s)", (ADVISORY_LOCK_RELATED,))
                c2.close()
        except Exception:
            logger.exception("advisory unlock related range")
        try:
            conn.close()
        except Exception:
            pass

    stats["duration_seconds"] = round(time.perf_counter() - t0, 3)
    return stats


def _parse_iso_to_date(s: str) -> date:
    t = s.strip()
    if len(t) == 10 and t[4] == "-" and t[7] == "-":
        return datetime.strptime(t, "%Y-%m-%d").date()
    dt = datetime.fromisoformat(t.replace("Z", "+00:00"))
    return dt.astimezone(timezone.utc).date()


def run_resync_related_range_background(start_date_iso: str, end_date_iso: str) -> None:
    try:
        sd = _parse_iso_to_date(start_date_iso)
        ed = _parse_iso_to_date(end_date_iso)
        sync_related_documents_range(start_date=sd, end_date=ed, strict_token=True)
    except Exception:
        logger.exception("resync related range background")


def run_sync_distribuidora_related_background() -> None:
    try:
        sync_distribuidora_related_documents(strict_token=True)
    except Exception:
        logger.exception("related sync background")
