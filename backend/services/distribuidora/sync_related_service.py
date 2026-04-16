"""
Sync incremental de documentos relacionados a líneas OC (GET ``/documents.json?relateddetailid=``).

Desacoplado del sync principal; escribe ``distribuidora.document_related`` con deduplicación.
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any

from backend.db import get_connection
from backend.repositories.distribuidora.sync_repo import (
    ensure_distribuidora_schema,
    insert_sync_status_row,
)
from backend.services.distribuidora.bsale_client import BsaleClient

logger = logging.getLogger(__name__)

ADVISORY_LOCK_RELATED = 5_927_184_005
COMPANY_ID = 3
OFFICE_ID = 1
DOC_TYPE_OC = 33


def _safe_int(v: Any) -> int | None:
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _bsale_token() -> str:
    return (os.getenv("BSALE_TOKEN") or "").strip() or (os.getenv("BSALE_TOKEN_SPA") or "").strip()


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


def _insert_related_rows(
    cur,
    detail_id: int,
    items: list[dict[str, Any]],
) -> int:
    n = 0
    for it in items:
        rid = _safe_int(it.get("id"))
        if rid is None:
            continue
        dt = it.get("documentType") or it.get("document_type") or {}
        tid = _safe_int(dt.get("id") if isinstance(dt, dict) else None)
        if tid is None:
            continue
        cur.execute(
            """
            INSERT INTO distribuidora.document_related (
                detail_id, related_document_id, related_document_type
            )
            VALUES (%s, %s, %s)
            ON CONFLICT (detail_id, related_document_id) DO NOTHING
            """,
            (detail_id, rid, tid),
        )
        if cur.rowcount:
            n += 1
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
        conn.commit()

        client = BsaleClient(token)
        throttle = float(os.getenv("DISTRIBUIDORA_RELATED_API_DELAY_SEC", "0.12"))

        for did in detail_ids:
            try:
                data = client.get(
                    "/documents.json",
                    {"relateddetailid": did, "limit": 50, "offset": 0},
                )
            except Exception as e:
                logger.warning("relateddetailid=%s: %s", did, e)
                continue
            stats["api_calls"] += 1
            items = data.get("items") or []
            if items:
                ins = _insert_related_rows(cur, did, items)
                stats["rows_inserted"] += ins
                conn.commit()
            if throttle > 0:
                time.sleep(throttle)

        insert_sync_status_row(
            cur,
            sync_type="related",
            records_processed=int(stats["rows_inserted"]),
            status="success",
        )
        conn.commit()
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


def run_sync_distribuidora_related_background() -> None:
    try:
        sync_distribuidora_related_documents(strict_token=True)
    except Exception:
        logger.exception("related sync background")
