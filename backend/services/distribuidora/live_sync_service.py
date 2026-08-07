"""
Sync incremental operacional (ventanas cortas + ``distribuidora.sync_state``).

Tipos: ``documents_live``, ``details_live``, ``related_live``, ``probable_live``.
"""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any

from backend.db import get_connection
from backend.services.distribuidora.bsale_client import BsaleClient
from backend.services.distribuidora.probable_invoice_service import (
    build_probable_invoice_matches_may_2026,
)
from backend.services.distribuidora.sync_related_service import (
    sync_distribuidora_related_documents,
)
from backend.services.distribuidora.sync_service import (
    COMPANY_ID,
    DOC_TYPES_OC,
    DOC_TYPES_SALES,
    OFFICE_ID,
    _bsale_token,
    _fetch_documents_window,
    _refresh_document_children,
    bsale_token_distribuidora_configured,
)
from backend.utils.db_tx import log_tx, pg_backend_pid, release_transaction, safe_rollback
from backend.utils.sync_state import (
    MODE_INCREMENTAL,
    get_sync_state,
    update_sync_state_error,
    update_sync_state_success,
)

logger = logging.getLogger(__name__)

SYNC_TYPE_DOCUMENTS_LIVE = "documents_live"
SYNC_TYPE_DETAILS_LIVE = "details_live"
SYNC_TYPE_RELATED_LIVE = "related_live"
SYNC_TYPE_PROBABLE_LIVE = "probable_live"

ADVISORY_LOCK_DOCUMENTS_LIVE = 5_927_184_010
ADVISORY_LOCK_DETAILS_LIVE = 5_927_184_011
ADVISORY_LOCK_PROBABLE_LIVE = 5_927_184_012
ADVISORY_LOCK_GLOBAL_LIVE_NOW = 5_927_184_019

DEFAULT_DOCUMENTS_WINDOW_HOURS = 2
DEFAULT_DETAILS_WINDOW_HOURS = 24
DEFAULT_RELATED_WINDOW_DAYS = 3
DEFAULT_PROBABLE_WINDOW_DAYS = 5

DEFAULT_OVERLAP_SECONDS_DOCUMENTS = 900
DEFAULT_OVERLAP_SECONDS_DETAILS = 3600
DEFAULT_OVERLAP_DAYS_RELATED = 1
DEFAULT_OVERLAP_DAYS_PROBABLE = 1


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _try_advisory_lock(cur, key: int) -> bool:
    cur.execute("SELECT pg_try_advisory_lock(%s)", (key,))
    return bool(cur.fetchone()[0])


def _advisory_unlock(cur, key: int) -> None:
    cur.execute("SELECT pg_advisory_unlock(%s)", (key,))


def _compute_window(
    *,
    now: datetime,
    window_hours: float | None = None,
    window_days: float | None = None,
    overlap_seconds: int = 0,
    overlap_days: int = 0,
    state: dict[str, Any] | None,
) -> tuple[datetime, datetime]:
    if window_hours is not None:
        window_from = now - timedelta(hours=window_hours)
    else:
        window_from = now - timedelta(days=float(window_days or 1))
    window_to = now
    if state and state.get("last_watermark"):
        wm = state["last_watermark"]
        if not isinstance(wm, datetime):
            wm = None
        elif wm.tzinfo is None:
            wm = wm.replace(tzinfo=timezone.utc)
    else:
        wm = None
    if wm is not None:
        if overlap_seconds > 0:
            wm_from = wm - timedelta(seconds=overlap_seconds)
        elif overlap_days > 0:
            wm_from = wm - timedelta(days=overlap_days)
        else:
            wm_from = wm
        if wm_from < window_from:
            window_from = wm_from
    return window_from, window_to


def _live_details_debug_enabled() -> bool:
    return os.getenv("LIVE_DETAILS_DEBUG", "").strip().lower() in ("1", "true", "yes")


def _child_sync_stats_template() -> dict[str, Any]:
    """Contadores que espera ``_refresh_document_children`` (sync_service)."""
    return {
        "details_rows": 0,
        "attributes_rows": 0,
        "references_rows": 0,
        "document_sellers_rows": 0,
        "sellers_filled": 0,
        "seller_sync_failures": 0,
    }


def _extract_bsale_detail_items(det: Any) -> tuple[list[dict[str, Any]], str]:
    """
    Normaliza respuesta de ``GET /documents/{id}/details.json``.

    Bsale estándar: ``{"items": [...], "count": N}``.
    """
    if isinstance(det, list):
        return det, "list_root"
    if not isinstance(det, dict):
        return [], f"unexpected_{type(det).__name__}"
    items = det.get("items")
    if isinstance(items, list):
        return items, "items"
    if items is None and "count" in det:
        return [], "dict_count_no_items"
    return [], "dict_no_items"


def _base_stats(sync_type: str, window_from: datetime, window_to: datetime, **extra: Any) -> dict[str, Any]:
    return {
        "sync_type": sync_type,
        "mode": MODE_INCREMENTAL,
        "window_from": window_from.isoformat(),
        "window_to": window_to.isoformat(),
        "skipped": False,
        "omitido_concurrencia": False,
        "errors": None,
        **extra,
    }


def _finalize_success(
    cur,
    *,
    sync_type: str,
    stats: dict[str, Any],
    window_from: datetime,
    window_to: datetime,
    overlap_seconds: int | None = None,
    overlap_days: int | None = None,
    items_processed: int = 0,
) -> None:
    update_sync_state_success(
        cur,
        sync_type=sync_type,
        mode=MODE_INCREMENTAL,
        office_id=OFFICE_ID,
        last_window_from=window_from,
        last_window_to=window_to,
        last_watermark=window_to,
        overlap_seconds=overlap_seconds,
        overlap_days=overlap_days,
        items_processed=items_processed,
    )


def _finalize_error(cur, *, sync_type: str, summary: str, items_processed: int = 0) -> None:
    update_sync_state_error(
        cur,
        sync_type=sync_type,
        mode=MODE_INCREMENTAL,
        office_id=OFFICE_ID,
        error_summary=summary,
        items_processed=items_processed,
    )


def _print_summary(title: str, stats: dict[str, Any]) -> None:
    print("", flush=True)
    print("=" * 60, flush=True)
    print(title, flush=True)
    print("=" * 60, flush=True)
    for key in sorted(stats.keys()):
        if key.startswith("_"):
            continue
        print(f"  {key}: {stats[key]}", flush=True)
    print("=" * 60, flush=True)


def live_sync_documents(*, strict_token: bool = True) -> dict[str, Any]:
    """OC (33) + ventas (1/6/9) en ventana ~2 h UTC vía API Bsale."""
    t0 = time.perf_counter()
    if not bsale_token_distribuidora_configured():
        if strict_token:
            raise ValueError("BSALE_TOKEN / BSALE_TOKEN_SPA no configuradas")
        return {"skipped": True, "skip_reason": "sin token", "duration_seconds": 0}

    now = _utc_now()
    win_h = float(os.getenv("LIVE_SYNC_DOCUMENTS_WINDOW_HOURS", str(DEFAULT_DOCUMENTS_WINDOW_HOURS)))
    overlap_sec = _env_int("LIVE_SYNC_DOCUMENTS_OVERLAP_SECONDS", DEFAULT_OVERLAP_SECONDS_DOCUMENTS)

    conn = get_connection()
    got_lock = False
    try:
        cur = conn.cursor()
        got_lock = _try_advisory_lock(cur, ADVISORY_LOCK_DOCUMENTS_LIVE)
        if not got_lock:
            cur.close()
            return {
                **_base_stats(SYNC_TYPE_DOCUMENTS_LIVE, now, now),
                "omitido_concurrencia": True,
                "duration_seconds": round(time.perf_counter() - t0, 3),
            }

        # Sin DDL aquí. Commit tras advisory lock (sesión) para no dejar TX abierta.
        conn.commit()
        log_tx(
            "COMMIT",
            job="live_sync_documents",
            conn=conn,
            step="after_advisory_lock",
            pg_pid=pg_backend_pid(conn),
        )

        state = get_sync_state(
            cur, sync_type=SYNC_TYPE_DOCUMENTS_LIVE, mode=MODE_INCREMENTAL, office_id=OFFICE_ID
        )
        window_from, window_to = _compute_window(
            now=now,
            window_hours=win_h,
            overlap_seconds=overlap_sec,
            state=state,
        )
        desde_ts = int(window_from.timestamp())
        hasta_ts = int(window_to.timestamp())
        if desde_ts >= hasta_ts:
            desde_ts = hasta_ts - 3600

        if os.getenv("LIVE_SYNC_DEBUG", "").strip().lower() in ("1", "true", "yes"):
            wm = (state or {}).get("last_watermark")
            logger.info(
                "[LIVE_SYNC_DEBUG] live_sync_documents state_exists=%s watermark=%s "
                "window_from=%s window_to=%s emissiondaterange=[%s,%s] overlap_sec=%s",
                state is not None,
                wm.isoformat() if isinstance(wm, datetime) else wm,
                window_from.isoformat(),
                window_to.isoformat(),
                desde_ts,
                hasta_ts,
                overlap_sec,
            )

        stats: dict[str, Any] = _base_stats(
            SYNC_TYPE_DOCUMENTS_LIVE,
            window_from,
            window_to,
            overlap_seconds=overlap_sec,
            documents_processed=0,
            documents_inserted=0,
            documents_updated=0,
            details_rows=0,
        )

        client = BsaleClient(_bsale_token())
        for allowed in (DOC_TYPES_OC, DOC_TYPES_SALES):
            stats["_allowed_document_type_ids"] = allowed
            _fetch_documents_window(
                client,
                cur,
                conn,
                desde_ts=desde_ts,
                hasta_ts=hasta_ts,
                stats=stats,
                log_id=None,
            )
            stats.pop("_allowed_document_type_ids", None)

        proc = int(stats.get("documents_processed") or 0)
        stats["documents_inserted"] = max(
            0, proc - int(stats.get("updated_documents") or 0)
        )
        stats["documents_updated"] = int(stats.get("updated_documents") or 0)
        stats["details_processed"] = int(stats.get("details_rows") or 0)

        _finalize_success(
            cur,
            sync_type=SYNC_TYPE_DOCUMENTS_LIVE,
            stats=stats,
            window_from=window_from,
            window_to=window_to,
            overlap_seconds=overlap_sec,
            items_processed=proc,
        )
        conn.commit()
        cur.close()
        stats["duration_seconds"] = round(time.perf_counter() - t0, 3)
        return stats
    except Exception as e:
        logger.exception("live_sync_documents")
        try:
            conn.rollback()
            c2 = conn.cursor()
            _finalize_error(c2, sync_type=SYNC_TYPE_DOCUMENTS_LIVE, summary=str(e))
            conn.commit()
            c2.close()
        except Exception:
            pass
        raise
    finally:
        if got_lock:
            try:
                c3 = conn.cursor()
                _advisory_unlock(c3, ADVISORY_LOCK_DOCUMENTS_LIVE)
                c3.close()
            except Exception:
                pass
        try:
            conn.close()
        except Exception:
            pass


def live_sync_details(*, strict_token: bool = True) -> dict[str, Any]:
    """Refresca ``document_details`` (y hijos OC) para documentos con emisión en ~24 h."""
    t0 = time.perf_counter()
    token = _bsale_token()
    if not token:
        if strict_token:
            raise ValueError("BSALE_TOKEN / BSALE_TOKEN_SPA no configuradas")
        return {"skipped": True, "skip_reason": "sin token", "duration_seconds": 0}

    now = _utc_now()
    win_h = float(os.getenv("LIVE_SYNC_DETAILS_WINDOW_HOURS", str(DEFAULT_DETAILS_WINDOW_HOURS)))
    overlap_sec = _env_int("LIVE_SYNC_DETAILS_OVERLAP_SECONDS", DEFAULT_OVERLAP_SECONDS_DETAILS)

    conn = get_connection()
    got_lock = False
    try:
        cur = conn.cursor()
        got_lock = _try_advisory_lock(cur, ADVISORY_LOCK_DETAILS_LIVE)
        if not got_lock:
            cur.close()
            return {
                **_base_stats(SYNC_TYPE_DETAILS_LIVE, now, now),
                "omitido_concurrencia": True,
                "duration_seconds": round(time.perf_counter() - t0, 3),
            }

        conn.commit()
        log_tx(
            "COMMIT",
            job="live_sync_details",
            conn=conn,
            step="after_advisory_lock",
            pg_pid=pg_backend_pid(conn),
        )
        state = get_sync_state(
            cur, sync_type=SYNC_TYPE_DETAILS_LIVE, mode=MODE_INCREMENTAL, office_id=OFFICE_ID
        )
        window_from, window_to = _compute_window(
            now=now,
            window_hours=win_h,
            overlap_seconds=overlap_sec,
            state=state,
        )

        stats: dict[str, Any] = _base_stats(
            SYNC_TYPE_DETAILS_LIVE,
            window_from,
            window_to,
            overlap_seconds=overlap_sec,
            documents_reviewed=0,
            details_rows_written=0,
            details_replace_calls=0,
            details_api_items_total=0,
            document_errors=0,
        )

        if _live_details_debug_enabled():
            logger.info(
                "[LIVE_DETAILS_DEBUG] window_from=%s window_to=%s docs_query=emission in range",
                window_from.isoformat(),
                window_to.isoformat(),
            )

        cur.execute(
            """
            SELECT document_id, document_type_id, number,
                   COALESCE(
                       NULLIF(to_jsonb(d)->>'source_document_id', '')::bigint,
                       NULLIF(raw_data->>'id', '')::bigint
                   ) AS bsale_source_id,
                   COALESCE(total_amount, 0)
            FROM distribuidora.v_documents_latest d
            WHERE company_id = %s AND office_id = %s
              AND emission_date >= %s
              AND emission_date <= %s
            ORDER BY document_id
            """,
            (COMPANY_ID, OFFICE_ID, window_from, window_to),
        )
        rows = cur.fetchall()
        stats["documents_reviewed"] = len(rows)
        # Liberar AccessShareLock del SELECT de listado antes del loop HTTP.
        conn.commit()
        log_tx(
            "COMMIT",
            job="live_sync_details",
            conn=conn,
            step="after_list_documents",
            pg_pid=pg_backend_pid(conn),
            rows=len(rows),
        )

        client = BsaleClient(token)
        for document_id, document_type_id, number, bsale_source_id, total_amount in rows:
            doc_id = int(document_id)
            doc_type = int(document_type_id) if document_type_id is not None else None
            try:
                folio_int = int(number) if number is not None else None
            except (TypeError, ValueError):
                folio_int = None
            child_stats = _child_sync_stats_template()
            try:
                release_transaction(conn, job=f"live_sync_details:{doc_id}")
                cur.execute(
                    "SELECT COUNT(*)::int FROM distribuidora.document_details WHERE document_id = %s",
                    (doc_id,),
                )
                br = cur.fetchone()
                rows_before = int(br[0] or 0) if br else 0
                conn.commit()

                det_payload: Any = None
                parser_key = "not_fetched"
                details_api_count = 0
                if _live_details_debug_enabled():
                    from backend.utils.bsale_document_ids import resolve_bsale_source_document_id

                    source_dbg = resolve_bsale_source_document_id(
                        local_document_id=doc_id,
                        raw_data_id=bsale_source_id,
                    )
                    det_payload = client.get(f"/documents/{source_dbg}/details.json")
                    items_dbg, parser_key = _extract_bsale_detail_items(det_payload)
                    details_api_count = len(items_dbg)
                    logger.info(
                        "[LIVE_DETAILS_DEBUG] local=%s bsale_source=%s type=%s "
                        "pre_fetch api_count=%s parser=%s",
                        doc_id,
                        source_dbg,
                        doc_type,
                        details_api_count,
                        parser_key,
                    )

                raw_for_children = {
                    "id": bsale_source_id,
                    "number": folio_int,
                    "totalAmount": float(total_amount or 0),
                }
                _refresh_document_children(
                    client,
                    cur,
                    conn,
                    doc_id,
                    doc_type,
                    child_stats,
                    raw_document=raw_for_children,
                    folio=folio_int,
                    bsale_source_document_id=(
                        int(bsale_source_id) if bsale_source_id is not None else None
                    ),
                    raw_data_id=bsale_source_id,
                )

                cur.execute(
                    "SELECT COUNT(*)::int FROM distribuidora.document_details WHERE document_id = %s",
                    (doc_id,),
                )
                ar = cur.fetchone()
                rows_after = int(ar[0] or 0) if ar else 0
                conn.commit()
                rows_written = int(child_stats.get("details_rows") or 0)
                details_pending = bool(child_stats.get("last_children_details_pending"))

                stats["details_replace_calls"] = int(stats.get("details_replace_calls") or 0) + 1
                stats["details_rows_written"] = int(stats.get("details_rows_written") or 0) + rows_written
                stats["details_api_items_total"] = int(stats.get("details_api_items_total") or 0) + (
                    details_api_count if _live_details_debug_enabled() else rows_written
                )
                if details_pending:
                    stats["header_ok_details_pending"] = (
                        int(stats.get("header_ok_details_pending") or 0) + 1
                    )

                if _live_details_debug_enabled():
                    logger.info(
                        "[LIVE_DETAILS_DEBUG] doc=%s replace_called=yes rows_deleted_then_inserted=%s "
                        "rows_before=%s rows_after=%s attributes_rows=%s references_rows=%s "
                        "details_pending=%s",
                        doc_id,
                        rows_written,
                        rows_before,
                        rows_after,
                        child_stats.get("attributes_rows"),
                        child_stats.get("references_rows"),
                        details_pending,
                    )
                elif details_pending or (rows_written == 0 and rows_after == 0 and doc_type == 33):
                    logger.warning(
                        "live_sync_details document_id=%s: status=header_ok_details_pending "
                        "(type=%s rows_after=%s); reintento en próximo ciclo",
                        doc_id,
                        doc_type,
                        rows_after,
                    )
            except Exception as e:
                safe_rollback(conn, job=f"live_sync_details:{doc_id}")
                stats["document_errors"] = int(stats.get("document_errors") or 0) + 1
                logger.warning(
                    "live_sync_details document_id=%s: %s",
                    doc_id,
                    e,
                    exc_info=_live_details_debug_enabled(),
                )

        items = int(stats.get("documents_reviewed") or 0)
        _finalize_success(
            cur,
            sync_type=SYNC_TYPE_DETAILS_LIVE,
            stats=stats,
            window_from=window_from,
            window_to=window_to,
            overlap_seconds=overlap_sec,
            items_processed=items,
        )
        conn.commit()
        cur.close()
        stats["duration_seconds"] = round(time.perf_counter() - t0, 3)
        return stats
    except Exception as e:
        logger.exception("live_sync_details")
        try:
            conn.rollback()
            c2 = conn.cursor()
            _finalize_error(c2, sync_type=SYNC_TYPE_DETAILS_LIVE, summary=str(e))
            conn.commit()
            c2.close()
        except Exception:
            pass
        raise
    finally:
        if got_lock:
            try:
                c3 = conn.cursor()
                _advisory_unlock(c3, ADVISORY_LOCK_DETAILS_LIVE)
                c3.close()
            except Exception:
                pass
        try:
            conn.close()
        except Exception:
            pass


def live_sync_related(*, strict_token: bool = True) -> dict[str, Any]:
    """``document_related`` para OC con emisión en lookback ~3 días."""
    t0 = time.perf_counter()
    now = _utc_now()
    win_days = _env_int("LIVE_SYNC_RELATED_WINDOW_DAYS", DEFAULT_RELATED_WINDOW_DAYS)
    overlap_days = _env_int("LIVE_SYNC_RELATED_OVERLAP_DAYS", DEFAULT_OVERLAP_DAYS_RELATED)

    window_from, window_to = _compute_window(
        now=now,
        window_days=win_days,
        overlap_days=overlap_days,
        state=None,
    )

    stats = sync_distribuidora_related_documents(
        strict_token=strict_token,
        lookback_days=win_days,
    )
    stats["sync_type"] = SYNC_TYPE_RELATED_LIVE
    stats["window_from"] = window_from.isoformat()
    stats["window_to"] = window_to.isoformat()
    stats["overlap_days"] = overlap_days

    if stats.get("skipped") or stats.get("omitido_concurrencia"):
        stats["duration_seconds"] = round(time.perf_counter() - t0, 3)
        return stats

    if stats.get("errors"):
        conn = get_connection()
        try:
            cur = conn.cursor()
            _finalize_error(
                cur,
                sync_type=SYNC_TYPE_RELATED_LIVE,
                summary=str(stats.get("errors")),
                items_processed=int(stats.get("rows_inserted") or 0),
            )
            conn.commit()
            cur.close()
        finally:
            conn.close()
        stats["duration_seconds"] = round(time.perf_counter() - t0, 3)
        return stats

    conn = get_connection()
    try:
        cur = conn.cursor()
        _finalize_success(
            cur,
            sync_type=SYNC_TYPE_RELATED_LIVE,
            stats=stats,
            window_from=window_from,
            window_to=window_to,
            overlap_days=overlap_days,
            items_processed=int(stats.get("rows_inserted") or 0),
        )
        conn.commit()
        cur.close()
    finally:
        conn.close()

    stats["duration_seconds"] = round(time.perf_counter() - t0, 3)
    return stats


def live_sync_probable_matches(*, strict_token: bool = True) -> dict[str, Any]:
    """Recalcula ``document_probable_matches`` para OCs en ventana ~5 días (solo BD)."""
    t0 = time.perf_counter()
    now = _utc_now()
    win_days = _env_int("LIVE_SYNC_PROBABLE_WINDOW_DAYS", DEFAULT_PROBABLE_WINDOW_DAYS)
    overlap_days = _env_int("LIVE_SYNC_PROBABLE_OVERLAP_DAYS", DEFAULT_OVERLAP_DAYS_PROBABLE)

    conn = get_connection()
    got_lock = False
    try:
        cur = conn.cursor()
        got_lock = _try_advisory_lock(cur, ADVISORY_LOCK_PROBABLE_LIVE)
        if not got_lock:
            cur.close()
            return {
                **_base_stats(SYNC_TYPE_PROBABLE_LIVE, now, now),
                "omitido_concurrencia": True,
                "duration_seconds": round(time.perf_counter() - t0, 3),
            }

        state = get_sync_state(
            cur, sync_type=SYNC_TYPE_PROBABLE_LIVE, mode=MODE_INCREMENTAL, office_id=OFFICE_ID
        )
        window_from, window_to = _compute_window(
            now=now,
            window_days=win_days,
            overlap_days=overlap_days,
            state=state,
        )
        cur.close()

        d0 = window_from.date()
        d1 = window_to.date()

        stats = build_probable_invoice_matches_may_2026(
            emission_from=d0,
            emission_to=d1,
        )
        stats["sync_type"] = SYNC_TYPE_PROBABLE_LIVE
        stats["window_from"] = window_from.isoformat()
        stats["window_to"] = window_to.isoformat()
        stats["overlap_days"] = overlap_days
        stats["mode"] = MODE_INCREMENTAL

        if stats.get("errors"):
            c2 = conn.cursor()
            _finalize_error(
                c2,
                sync_type=SYNC_TYPE_PROBABLE_LIVE,
                summary="; ".join(str(x) for x in stats["errors"][:5]),
                items_processed=int(stats.get("rows_upserted") or 0),
            )
            conn.commit()
            c2.close()
            stats["duration_seconds"] = round(time.perf_counter() - t0, 3)
            return stats

        c3 = conn.cursor()
        _finalize_success(
            c3,
            sync_type=SYNC_TYPE_PROBABLE_LIVE,
            stats=stats,
            window_from=window_from,
            window_to=window_to,
            overlap_days=overlap_days,
            items_processed=int(stats.get("rows_upserted") or 0),
        )
        conn.commit()
        c3.close()
        stats["duration_seconds"] = round(time.perf_counter() - t0, 3)
        return stats
    except Exception as e:
        logger.exception("live_sync_probable_matches")
        try:
            conn.rollback()
            c2 = conn.cursor()
            _finalize_error(c2, sync_type=SYNC_TYPE_PROBABLE_LIVE, summary=str(e))
            conn.commit()
            c2.close()
        except Exception:
            pass
        raise
    finally:
        if got_lock:
            try:
                c3 = conn.cursor()
                _advisory_unlock(c3, ADVISORY_LOCK_PROBABLE_LIVE)
                c3.close()
            except Exception:
                pass
        try:
            conn.close()
        except Exception:
            pass


def run_live_sync_on_demand(*, strict_token: bool = True) -> dict[str, Any]:
    """Cadena documents → details → related → probable (sync manual)."""
    t0 = time.perf_counter()
    started_at = _utc_now()

    if not bsale_token_distribuidora_configured():
        return {
            "ok": False,
            "status": "error",
            "message": "Token Bsale no configurado",
            "started_at": started_at.isoformat(),
            "finished_at": _utc_now().isoformat(),
            "duration_seconds": 0,
        }

    conn_lock = get_connection()
    got_global = False
    try:
        cur_lock = conn_lock.cursor()
        got_global = _try_advisory_lock(cur_lock, ADVISORY_LOCK_GLOBAL_LIVE_NOW)
        cur_lock.close()
        if not got_global:
            return {
                "ok": False,
                "status": "already_running",
                "message": "Ya hay una sincronización en ejecución",
                "started_at": started_at.isoformat(),
                "finished_at": _utc_now().isoformat(),
                "duration_seconds": round(time.perf_counter() - t0, 3),
            }
    finally:
        conn_lock.close()

    result: dict[str, Any] = {
        "ok": True,
        "status": "completed",
        "started_at": started_at.isoformat(),
        "documents": {},
        "details": {},
        "related": {},
        "probable_matches": {},
    }

    try:
        for name, fn in (
            ("documents", live_sync_documents),
            ("details", live_sync_details),
            ("related", live_sync_related),
            ("probable_matches", live_sync_probable_matches),
        ):
            step = fn(strict_token=strict_token)
            result[name] = step
            if step.get("omitido_concurrencia"):
                result["ok"] = False
                result["status"] = "already_running"
                result["message"] = f"Sync {name} omitido: otro proceso en curso"
                break
            if step.get("skipped"):
                result["ok"] = False
                result["status"] = "error"
                result["message"] = step.get("skip_reason") or "sync omitido"
                break
            if step.get("errors"):
                result["ok"] = False
                result["status"] = "error"
                result["message"] = str(step.get("errors"))
                break
    except Exception as e:
        logger.exception("run_live_sync_on_demand")
        result["ok"] = False
        result["status"] = "error"
        result["message"] = str(e)
    finally:
        if got_global:
            try:
                conn_unlock = get_connection()
                c_unlock = conn_unlock.cursor()
                _advisory_unlock(c_unlock, ADVISORY_LOCK_GLOBAL_LIVE_NOW)
                c_unlock.close()
                conn_unlock.close()
            except Exception:
                pass

    finished_at = _utc_now()
    result["finished_at"] = finished_at.isoformat()
    result["duration_seconds"] = round(time.perf_counter() - t0, 3)
    _print_summary("LIVE SYNC ON-DEMAND — SUMMARY", result)
    return result


def get_live_sync_panel_payload() -> dict[str, Any]:
    """Última actualización por capa live (``sync_state``)."""
    labels = {
        SYNC_TYPE_DOCUMENTS_LIVE: "Documents",
        SYNC_TYPE_DETAILS_LIVE: "Details",
        SYNC_TYPE_RELATED_LIVE: "Related",
        SYNC_TYPE_PROBABLE_LIVE: "Probables",
    }
    conn = get_connection()
    try:
        cur = conn.cursor()
        out: dict[str, Any] = {}
        global_busy = not _try_advisory_lock(cur, ADVISORY_LOCK_GLOBAL_LIVE_NOW)
        if not global_busy:
            _advisory_unlock(cur, ADVISORY_LOCK_GLOBAL_LIVE_NOW)

        for sync_type, label in labels.items():
            row = get_sync_state(
                cur, sync_type=sync_type, mode=MODE_INCREMENTAL, office_id=OFFICE_ID
            )
            if not row:
                out[sync_type] = {
                    "label": label,
                    "last_success_at": None,
                    "status": "never",
                    "items_processed": 0,
                    "error_summary": None,
                }
                continue
            ts = row.get("last_success_at") or row.get("updated_at")
            out[sync_type] = {
                "label": label,
                "last_success_at": ts.isoformat() if isinstance(ts, datetime) else None,
                "status": row.get("status") or "idle",
                "items_processed": int(row.get("items_processed") or 0),
                "error_summary": row.get("error_summary"),
                "last_window_from": (
                    row["last_window_from"].isoformat()
                    if isinstance(row.get("last_window_from"), datetime)
                    else None
                ),
                "last_window_to": (
                    row["last_window_to"].isoformat()
                    if isinstance(row.get("last_window_to"), datetime)
                    else None
                ),
            }
        cur.close()
        return {"live_sync": out, "live_sync_global_busy": global_busy}
    finally:
        conn.close()
