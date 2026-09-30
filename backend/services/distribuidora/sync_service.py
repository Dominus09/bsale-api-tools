"""
Orquestación sync Bsale → ``distribuidora.*`` (documentos, detalles, atributos, referencias).
"""

from __future__ import annotations

import json
import logging
import os
import random
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable

import requests

from backend.db import get_connection
from backend.repositories.distribuidora.attributes_repo import replace_document_attributes
from backend.repositories.distribuidora.details_repo import replace_document_details
from backend.repositories.distribuidora.documents_repo import (
    document_dict_from_bsale,
    replace_document_sellers,
    seller_tuples_from_sellers_api_response,
    set_document_primary_seller,
    upsert_documents,
)
from backend.repositories.distribuidora.references_repo import replace_document_references
from backend.repositories.distribuidora.sync_repo import (
    ensure_sync_state_row,
    finish_sync_log,
    get_last_sync,
    insert_sync_status_row,
    set_sync_state,
    start_sync_log,
)
from backend.services.distribuidora.bsale_client import BASE_BSALE, BsaleClient
from backend.services.distribuidora.bsale_params import (
    log_office_filter_debug_response,
    merge_bsale_office_query,
)
from backend.utils.db_tx import log_tx, pg_backend_pid, release_transaction, safe_rollback
from backend.utils.sync_order_audit import log_order_sync_audit
from backend.utils.sync_state import (
    MODE_BACKFILL,
    update_sync_state_error,
    update_sync_state_success,
)

logger = logging.getLogger(__name__)

ADVISORY_LOCK_KEY = 5_927_184_003
COMPANY_ID = 3
OFFICE_ID = 1
LIMIT_BSALE = 50
RESYNC_BSALE_BACKOFFS_SEC = (3, 5, 10, 20, 30)
RESYNC_HTTP_MAX_ATTEMPTS = 5
PROCESS_INCREMENTAL = "documents_incremental"
PROCESS_RESYNC = "documents_resync"
PROCESS_ORDERS = "documents_orders"
PROCESS_SALES = "documents_sales"
DOC_TYPES_OC = frozenset({33})
DOC_TYPES_SALES = frozenset({1, 6, 9})
_FIRST_SYNC_CUTOFF = datetime(2010, 1, 1, tzinfo=timezone.utc)
_token_missing_logged = False


def _sales_sliding_window_days() -> int:
    """Días hacia atrás desde hoy (UTC) para ``sync_bsale_distribuidora_sales_incremental``."""
    try:
        n = int(os.getenv("SALES_SYNC_WINDOW_DAYS", "10"))
    except ValueError:
        n = 10
    return max(1, min(366, n))


def _orders_emission_window_days() -> int:
    """Ventana de emisión para sync OC (captura OCs recientes por fecha documento)."""
    try:
        n = int(os.getenv("ORDERS_EMISSION_WINDOW_DAYS", "45"))
    except ValueError:
        n = 45
    return max(1, min(366, n))


def _orders_generation_window_days() -> int:
    """
    Ventana ``generationdaterange`` Bsale: OCs modificadas recientemente aunque
    su ``emission_date`` sea antigua.
    """
    try:
        n = int(os.getenv("ORDERS_GENERATION_WINDOW_DAYS", "14"))
    except ValueError:
        n = 14
    return max(1, min(366, n))


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

    Siempre envía ``officeid`` = sucursal Distribuidora (``OFFICE_ID``), según doc Bsale.
    """
    params = merge_bsale_office_query(params, OFFICE_ID, context="documents_get_resync")
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
            log_office_filter_debug_response(
                method="GET",
                path="/documents.json",
                params=params,
                response_url=getattr(r.request, "url", None),
                context="documents_get_resync",
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


def _notify_progress(stats: dict[str, Any]) -> None:
    cb = stats.get("_on_progress")
    if not callable(cb):
        return
    cb(
        {
            "documents_processed": int(stats.get("documents_processed") or 0),
            "updated_documents": int(stats.get("updated_documents") or 0),
            "document_errors": int(stats.get("document_errors") or 0),
            "message": "Procesando órdenes",
        }
    )


def _bsale_document_type_id(d: dict[str, Any]) -> int | None:
    dt = d.get("document_type") or d.get("documentType")
    if not isinstance(dt, dict):
        return None
    raw = dt.get("id")
    if raw is None:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def _log_orders_sync_summary(
    cur,
    *,
    emission_from: datetime,
    emission_to: datetime,
    stats: dict[str, Any],
    desde_ts: int,
    hasta_ts: int,
) -> dict[str, Any]:
    """Post-sync: OC en ventana según ``v_documents_latest`` y relación hacia boleta/factura (1, 6)."""
    cur.execute(
        """
        WITH oc AS (
            SELECT d.document_id, d.company_id, d.office_id
            FROM distribuidora.v_documents_latest d
            WHERE d.company_id = %s
              AND d.office_id = %s
              AND d.document_type_id = 33
              AND d.emission_date >= %s
              AND d.emission_date <= %s
        )
        SELECT
            COUNT(*)::int AS total_oc,
            COALESCE(
                COUNT(*) FILTER (
                    WHERE EXISTS (
                        SELECT 1
                        FROM distribuidora.document_related dr
                        INNER JOIN distribuidora.document_details dd
                            ON dd.detail_id = dr.detail_id
                        INNER JOIN distribuidora.v_documents_latest inv
                            ON inv.document_id = dr.related_document_id
                           AND inv.document_type_id IN (1, 6)
                           AND inv.company_id = oc.company_id
                           AND inv.office_id = oc.office_id
                        WHERE dd.document_id = oc.document_id
                    )
                ),
                0
            )::int AS oc_with_boleta_factura
        FROM oc
        """,
        (COMPANY_ID, OFFICE_ID, emission_from, emission_to),
    )
    row = cur.fetchone() or (0, 0)
    total_oc, with_inv = int(row[0] or 0), int(row[1] or 0)
    visible = max(0, total_oc - with_inv)
    proc = int(stats.get("documents_processed") or 0)
    skipped_t = int(stats.get("skipped_document_type_filter") or 0)
    logger.info(
        "sync-orders: rango epoch Bsale [%s, %s] | API docs guardados=%s | API omitidos_por_tipo=%s",
        desde_ts,
        hasta_ts,
        proc,
        skipped_t,
    )
    logger.info(
        "sync-orders: ventana BD emission_date [%s, %s] | OC totales=%s | OC visibles_pre_despacho=%s | "
        "OC ocultas_boleta_factura_relacionada=%s",
        emission_from.isoformat(),
        emission_to.isoformat(),
        total_oc,
        visible,
        with_inv,
    )
    return {
        "processed": proc,
        "visibles": visible,
        "ocultas": with_inv,
        "total_oc": total_oc,
        "emission_window_from": emission_from.astimezone(timezone.utc).isoformat(),
        "emission_window_to": emission_to.astimezone(timezone.utc).isoformat(),
    }


def _log_sales_sync_summary(
    cur,
    *,
    emission_from: datetime,
    emission_to: datetime,
    stats: dict[str, Any],
    desde_ts: int,
    hasta_ts: int,
) -> dict[str, Any]:
    cur.execute(
        """
        SELECT
            COUNT(*) FILTER (WHERE document_type_id = 1)::int AS n_boleta,
            COUNT(*) FILTER (WHERE document_type_id = 6)::int AS n_factura,
            COUNT(*) FILTER (WHERE document_type_id = 9)::int AS n_nc,
            COALESCE(
                SUM(
                    CASE
                        WHEN document_type_id = 9 THEN -COALESCE(total_amount, 0::numeric)
                        ELSE COALESCE(total_amount, 0::numeric)
                    END
                ),
                0::numeric
            ) AS monto_neto
        FROM distribuidora.v_documents_latest
        WHERE company_id = %s
          AND office_id = %s
          AND document_type_id IN (1, 6, 9)
          AND emission_date >= %s
          AND emission_date <= %s
        """,
        (COMPANY_ID, OFFICE_ID, emission_from, emission_to),
    )
    r0 = cur.fetchone()
    n_b, n_f, n_nc, m_neto = 0, 0, 0, 0
    if r0:
        n_b, n_f, n_nc = int(r0[0] or 0), int(r0[1] or 0), int(r0[2] or 0)
        m_neto = float(r0[3] or 0)

    cur.execute(
        """
        SELECT
            COALESCE(SUM(COALESCE(total_amount, 0::numeric)), 0::numeric) AS suma,
            COUNT(*)::int AS cnt
        FROM distribuidora.v_documents_latest
        WHERE company_id = %s
          AND office_id = %s
          AND document_type_id IN (1, 6)
          AND emission_date >= %s
          AND emission_date <= %s
        """,
        (COMPANY_ID, OFFICE_ID, emission_from, emission_to),
    )
    r1 = cur.fetchone()
    sum_real, cnt_real = (0.0, 0)
    if r1:
        sum_real, cnt_real = float(r1[0] or 0), int(r1[1] or 0)
    ticket = (sum_real / cnt_real) if cnt_real else 0.0

    proc = int(stats.get("documents_processed") or 0)
    skipped_t = int(stats.get("skipped_document_type_filter") or 0)
    logger.info(
        "sync-sales: rango epoch Bsale [%s, %s] | API docs guardados=%s | API omitidos_por_tipo=%s",
        desde_ts,
        hasta_ts,
        proc,
        skipped_t,
    )
    logger.info(
        "sync-sales: ventana BD emission_date [%s, %s] | boletas=%s facturas=%s nc=%s | "
        "monto_neto_suma_tipos_1_6_9=%s | ticket_promedio_solo_1_6=%s (n=%s)",
        emission_from.isoformat(),
        emission_to.isoformat(),
        n_b,
        n_f,
        n_nc,
        m_neto,
        round(ticket, 2),
        cnt_real,
    )
    return {
        "processed": proc,
        "boletas": n_b,
        "facturas": n_f,
        "nc": n_nc,
        "monto_neto": float(m_neto),
        "ticket_promedio_ventas_reales": round(ticket, 2),
        "ventas_reales_count": cnt_real,
        "emission_window_from": emission_from.astimezone(timezone.utc).isoformat(),
        "emission_window_to": emission_to.astimezone(timezone.utc).isoformat(),
    }


def _append_items_from_bsale_response(
    items: list[dict[str, Any]],
    pending: list[dict[str, Any]],
    stats: dict[str, Any],
) -> None:
    allowed = stats.get("_allowed_document_type_ids")
    for d in items:
        if allowed is not None:
            tid = _bsale_document_type_id(d)
            if tid not in allowed:
                stats["skipped_document_type_filter"] = int(stats.get("skipped_document_type_filter") or 0) + 1
                continue
        try:
            row = document_dict_from_bsale(
                d,
                company_id=COMPANY_ID,
                default_office_id=OFFICE_ID,
                sync_stats=stats,
            )
        except Exception as e:
            logger.error(
                "Error procesando documento %s: %s",
                d.get("id"),
                str(e),
                exc_info=True,
            )
            stats["document_errors"] = int(stats.get("document_errors") or 0) + 1
            _notify_progress(stats)
            continue
        if row is None:
            continue
        row["_bsale_document"] = d
        if int(row.get("document_type_id") or 0) == 33:
            log_order_sync_audit(d, phase="api_received")
        pending.append(row)


def _document_log_id_from_row(row: dict[str, Any]) -> Any:
    raw = row.get("_bsale_document")
    if isinstance(raw, dict):
        return raw.get("id", row.get("document_id"))
    return row.get("document_id")


def _process_one_pending_document_row(
    client: BsaleClient,
    cur,
    conn,
    row: dict[str, Any],
    stats: dict[str, Any],
) -> None:
    doc_log_id = _document_log_id_from_row(row)
    job = f"sync_doc:{doc_log_id}"
    try:
        try:
            upsert_documents(cur, [row], stats)
            # Liberar locks de documents ANTES de HTTP a Bsale (hijos/sellers).
            conn.commit()
            log_tx("COMMIT", job=job, conn=conn, step="after_upsert")
        except Exception as e:
            stats["document_upsert_failures"] = int(stats.get("document_upsert_failures") or 0) + 1
            stats["document_errors"] = int(stats.get("document_errors") or 0) + 1
            logger.error(
                "Error procesando documento %s: %s",
                doc_log_id,
                str(e),
                exc_info=True,
            )
            safe_rollback(conn, job=job)
            _notify_progress(stats)
            return
        if not stats.get("_documents_only_skip_children"):
            raw_doc = row.get("_bsale_document")
            local_document_id = int(row["document_id"])
            folio = row.get("number")
            try:
                folio_int = int(folio) if folio is not None else None
            except (TypeError, ValueError):
                folio_int = None
            if isinstance(raw_doc, dict):
                log_order_sync_audit(
                    raw_doc,
                    phase="before_children",
                    persisted_document_id=local_document_id,
                )
            _refresh_document_children(
                client,
                cur,
                conn,
                local_document_id,
                row.get("document_type_id"),
                stats,
                raw_document=raw_doc if isinstance(raw_doc, dict) else None,
                folio=folio_int,
            )
            if stats.get("last_children_details_pending"):
                stats["documents_header_ok_details_pending"] = (
                    int(stats.get("documents_header_ok_details_pending") or 0) + 1
                )
                logger.warning(
                    "document_sync_incomplete status=header_ok_details_pending "
                    "local_document_id=%s folio=%s bsale_id=%s",
                    local_document_id,
                    folio_int,
                    doc_log_id,
                )
            if isinstance(raw_doc, dict):
                log_order_sync_audit(
                    raw_doc,
                    phase="after_children",
                    persisted_document_id=local_document_id,
                    attributes_count=int(stats.get("attributes_rows") or 0),
                )
        release_transaction(conn, job=job)
        stats["documents_processed"] += 1
        _notify_progress(stats)
    except Exception as e:
        stats["document_errors"] = int(stats.get("document_errors") or 0) + 1
        logger.error(
            "Error procesando documento %s: %s",
            doc_log_id,
            str(e),
            exc_info=True,
        )
        safe_rollback(conn, job=job)
        _notify_progress(stats)


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
    for r in to_save:
        _process_one_pending_document_row(client, cur, conn, r, stats)


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
    for r in to_save:
        _process_one_pending_document_row(client, cur, conn, r, stats)


def _fetch_documents_single_day_resync(
    client: BsaleClient,
    cur,
    conn,
    day: date,
    stats: dict[str, Any],
    *,
    page_limit: int | None = None,
    extra_page_sleep_sec: float = 0.0,
) -> None:
    """Un día UTC completo: paginación con offset reiniciado y pausas entre llamadas a Bsale."""
    pl = page_limit if page_limit is not None else _resync_page_limit()
    desde_ts, hasta_ts = _utc_day_timestamp_bounds(day)
    logger.info("resync día=%s", day.isoformat())
    pending: list[dict[str, Any]] = []
    offset = 0
    while True:
        params = {
            "limit": pl,
            "offset": offset,
            "emissiondaterange": f"[{desde_ts},{hasta_ts}]",
        }
        data = _documents_get_resync(client, params)
        if stats.get("_count_document_pages"):
            stats["document_api_pages"] = int(stats.get("document_api_pages") or 0) + 1
        items = data.get("items") or []
        if not items:
            break
        _append_items_from_bsale_response(items, pending, stats)
        _flush_pending_when_large(client, cur, conn, pending, stats)
        offset += len(items)
        time.sleep(random.uniform(0.2, 0.5) + max(0.0, float(extra_page_sleep_sec or 0.0)))
    _flush_pending_tail(client, cur, conn, pending, stats)
    logger.info("resync día completado=%s", day.isoformat())


def _seller_tuples_from_bsale_document_json(
    client: BsaleClient,
    raw: dict[str, Any],
) -> list[tuple[int | None, str | None]]:
    """
    Obtiene vendedores desde el JSON del documento Bsale (clave ``sellers``).
    Suele ser ``{ "href": "https://.../sellers.json" }``; si ya viene ``items``, lo usa.
    """
    sellers = raw.get("sellers")
    if sellers is None:
        return []
    if isinstance(sellers, dict):
        href = sellers.get("href")
        if isinstance(href, str) and href.strip():
            try:
                data = client.get(href.strip())
            except Exception as e:
                logger.warning("sellers href document_id=%s: %s", raw.get("id"), e)
                return []
            return seller_tuples_from_sellers_api_response(data)
        items = sellers.get("items")
        if isinstance(items, list):
            return seller_tuples_from_sellers_api_response({"items": items})
    if isinstance(sellers, list):
        return seller_tuples_from_sellers_api_response({"items": sellers})
    return []


def _sync_document_sellers(
    client: BsaleClient,
    cur,
    local_document_id: int,
    raw_document: dict[str, Any] | None,
    stats: dict[str, Any],
    *,
    bsale_source_document_id: int | None = None,
) -> None:
    """
    Persiste ``distribuidora.document_sellers`` desde ``document.sellers`` o GET sellers.json.

    HTTP usa ``bsale_source_document_id``; persistencia bajo ``local_document_id``.
    Si ``sellers`` viene vacío, solo se eliminan filas previas (ningún INSERT).
    """
    from backend.utils.bsale_document_ids import resolve_bsale_source_document_id

    source_id = (
        int(bsale_source_document_id)
        if bsale_source_document_id is not None
        else resolve_bsale_source_document_id(
            local_document_id=local_document_id,
            raw_document=raw_document,
        )
    )
    tuples: list[tuple[int | None, str | None]] = []
    if isinstance(raw_document, dict):
        tuples = _seller_tuples_from_bsale_document_json(client, raw_document)
    if not tuples:
        try:
            data = client.get(f"/documents/{source_id}/sellers.json")
            tuples = seller_tuples_from_sellers_api_response(data)
        except Exception as e:
            logger.warning(
                "sellers.json local_document_id=%s bsale_source_document_id=%s: %s",
                local_document_id,
                source_id,
                e,
            )

    try:
        n = replace_document_sellers(cur, local_document_id, tuples)
        stats["document_sellers_rows"] = int(stats.get("document_sellers_rows") or 0) + n
        if tuples:
            sid0, name0 = tuples[0]
            set_document_primary_seller(cur, local_document_id, sid0, name0)
            stats["sellers_filled"] = int(stats.get("sellers_filled") or 0) + 1
    except Exception as e:
        stats["seller_sync_failures"] = int(stats.get("seller_sync_failures") or 0) + 1
        logger.error(
            "seller_sync_failed local_document_id=%s bsale_source_document_id=%s: %s",
            local_document_id,
            source_id,
            e,
            exc_info=True,
        )


def _bsale_total_amount(raw_document: dict[str, Any] | None) -> float:
    if not isinstance(raw_document, dict):
        return 0.0
    raw = raw_document.get("totalAmount")
    if raw is None:
        return 0.0
    try:
        return float(raw)
    except (TypeError, ValueError):
        return 0.0


def _fetch_document_detail_items_paginated(
    client: BsaleClient,
    source_id: int,
) -> list[dict[str, Any]]:
    """Misma paginación que reconciliación; no aceptar un solo GET sin limit/offset."""
    from backend.services.distribuidora.oc_source_resolver import fetch_all_document_details

    return fetch_all_document_details(client, int(source_id))


def _refresh_document_children(
    client: BsaleClient,
    cur,
    conn,
    local_document_id: int,
    document_type_id: int | None,
    stats: dict[str, Any],
    raw_document: dict[str, Any] | None = None,
    *,
    folio: int | None = None,
    bsale_source_document_id: int | None = None,
    raw_data_id: Any = None,
) -> None:
    """HTTP a Bsale fuera de transacción; persistencia en TX corta.

    * ``local_document_id``: PK estable en PostgreSQL (destino de INSERT/replace).
    * ``bsale_source_document_id`` / ``raw_document["id"]``: id vigente en Bsale
      para construir URLs de hijos. Nunca usar la PK local cuando el documento
      se resolvió por folio y el id Bsale difiere.

    Firma: ``conn`` es obligatorio para liberar locks antes del HTTP.

    Invariante OC (tipo 33, monto > 0): el header ya puede estar commiteado por el
    caller, pero **no** se considera sync completa si fallan o faltan details.
    En ese caso se marca ``header_ok_details_pending`` para reintento.
    """
    from backend.utils.bsale_document_ids import (
        ids_differ,
        resolve_bsale_source_document_id,
    )

    source_id = (
        int(bsale_source_document_id)
        if bsale_source_document_id is not None
        else resolve_bsale_source_document_id(
            local_document_id=local_document_id,
            raw_document=raw_document,
            raw_data_id=raw_data_id,
        )
    )
    folio_val = folio
    if folio_val is None and isinstance(raw_document, dict):
        try:
            folio_val = int(raw_document["number"]) if raw_document.get("number") is not None else None
        except (TypeError, ValueError):
            folio_val = None

    differ = ids_differ(local_document_id, source_id)
    expects_oc_details = int(document_type_id or 0) == 33 and (
        raw_document is None or _bsale_total_amount(raw_document) > 0
    )
    job = f"sync_children:local={local_document_id}:bsale={source_id}"
    logger.info(
        "refresh_children folio=%s local_document_id=%s bsale_source_document_id=%s "
        "ids_differ=%s document_type_id=%s expects_oc_details=%s",
        folio_val,
        local_document_id,
        source_id,
        differ,
        document_type_id,
        expects_oc_details,
    )
    # Garantizar que no hay TX abierta sosteniendo locks durante el HTTP.
    release_transaction(conn, job=job)

    detail_items: list[dict[str, Any]] | None = None
    details_fetch_ok = False
    ad: dict[str, Any] | None = None
    rd: dict[str, Any] | None = None
    try:
        detail_items = _fetch_document_detail_items_paginated(client, source_id)
        details_fetch_ok = True
    except Exception as e:
        logger.warning(
            "details local_document_id=%s bsale_source_document_id=%s: %s",
            local_document_id,
            source_id,
            e,
        )

    if document_type_id == 33:
        try:
            ad = client.get(f"/documents/{source_id}/attributes.json")
            if not isinstance(ad, dict):
                ad = {"_raw": ad}
        except Exception as e:
            logger.warning(
                "attributes local_document_id=%s bsale_source_document_id=%s: %s",
                local_document_id,
                source_id,
                e,
            )

    if document_type_id in (1, 6, 9, 33):
        try:
            rd = client.get(f"/documents/{source_id}/references.json")
            if not isinstance(rd, dict):
                rd = {"_raw": rd}
        except Exception as e:
            logger.warning(
                "references local_document_id=%s bsale_source_document_id=%s: %s",
                local_document_id,
                source_id,
                e,
            )

    # Sellers: resolver HTTP antes de abrir TX de escritura.
    seller_tuples: list[tuple[int | None, str | None]] = []
    if isinstance(raw_document, dict):
        seller_tuples = _seller_tuples_from_bsale_document_json(client, raw_document)
    if not seller_tuples:
        try:
            data = client.get(f"/documents/{source_id}/sellers.json")
            seller_tuples = seller_tuples_from_sellers_api_response(data)
        except Exception as e:
            logger.warning(
                "sellers.json local_document_id=%s bsale_source_document_id=%s: %s",
                local_document_id,
                source_id,
                e,
            )

    details_pending = False
    if not details_fetch_ok:
        details_pending = bool(expects_oc_details)
    elif expects_oc_details and not detail_items:
        # No borrar líneas locales con un replace vacío si Bsale aún no entrega ítems.
        details_pending = True
        logger.error(
            "header_ok_details_pending folio=%s local_document_id=%s "
            "bsale_source_document_id=%s reason=empty_details_with_total_amount",
            folio_val,
            local_document_id,
            source_id,
        )

    details_replaced = 0
    log_tx("TX_BEGIN", job=job, conn=conn, step="persist_children")
    try:
        if details_fetch_ok and not details_pending:
            details_replaced = replace_document_details(
                cur,
                local_document_id,
                detail_items or [],
                invalidate_cache=False,
            )
            stats["details_rows"] = int(stats.get("details_rows") or 0) + details_replaced
            ensure_weight = False
            if int(document_type_id or 0) == 33:
                if details_replaced > 0:
                    ensure_weight = True
                else:
                    try:
                        cur.execute(
                            """
                            SELECT EXISTS(
                                SELECT 1
                                FROM distribuidora.document_details
                                WHERE document_id = %s
                            )
                            AND NOT EXISTS(
                                SELECT 1
                                FROM distribuidora.order_weight_snapshots
                                WHERE document_id = %s
                            )
                            """,
                            (int(local_document_id), int(local_document_id)),
                        )
                        row_miss = cur.fetchone()
                        ensure_weight = bool(row_miss and row_miss[0])
                    except Exception:
                        ensure_weight = False
            if ensure_weight:
                try:
                    from backend.services.order_weight_service import (
                        recalculate_order_weight_in_transaction,
                    )

                    recalculate_order_weight_in_transaction(
                        cur,
                        document_id=int(local_document_id),
                        company_id=COMPANY_ID,
                        office_id=OFFICE_ID,
                        persist=True,
                    )
                    stats["order_weight_recalculated"] = (
                        int(stats.get("order_weight_recalculated") or 0) + 1
                    )
                except Exception as e:
                    logger.warning(
                        "order_weight_after_details failed local_document_id=%s: %s",
                        local_document_id,
                        e,
                    )
        if ad is not None:
            n = replace_document_attributes(cur, local_document_id, ad)
            stats["attributes_rows"] = int(stats.get("attributes_rows") or 0) + n
        if rd is not None:
            n = replace_document_references(cur, local_document_id, rd)
            stats["references_rows"] = int(stats.get("references_rows") or 0) + n
        try:
            n = replace_document_sellers(cur, local_document_id, seller_tuples)
            stats["document_sellers_rows"] = int(stats.get("document_sellers_rows") or 0) + n
            if seller_tuples:
                sid0, name0 = seller_tuples[0]
                set_document_primary_seller(cur, local_document_id, sid0, name0)
                stats["sellers_filled"] = int(stats.get("sellers_filled") or 0) + 1
        except Exception as e:
            stats["seller_sync_failures"] = int(stats.get("seller_sync_failures") or 0) + 1
            logger.error(
                "seller_sync_failed local_document_id=%s bsale_source_document_id=%s: %s",
                local_document_id,
                source_id,
                e,
                exc_info=True,
            )
        conn.commit()
        log_tx("COMMIT", job=job, conn=conn, step="persist_children")
    except Exception:
        safe_rollback(conn, job=job)
        raise

    if details_pending:
        stats["header_ok_details_pending"] = (
            int(stats.get("header_ok_details_pending") or 0) + 1
        )
        logger.warning(
            "refresh_children_incomplete folio=%s local_document_id=%s "
            "bsale_source_document_id=%s status=header_ok_details_pending "
            "details_fetch_ok=%s",
            folio_val,
            local_document_id,
            source_id,
            details_fetch_ok,
        )

    logger.info(
        "refresh_children_done folio=%s local_document_id=%s bsale_source_document_id=%s "
        "ids_differ=%s details_replaced=%s details_pending=%s",
        folio_val,
        local_document_id,
        source_id,
        differ,
        details_replaced,
        details_pending,
    )
    stats["last_children_local_document_id"] = local_document_id
    stats["last_children_bsale_source_document_id"] = source_id
    stats["last_children_ids_differ"] = differ
    stats["last_children_details_replaced"] = details_replaced
    stats["last_children_details_pending"] = details_pending


def _fetch_documents_window(
    client: BsaleClient,
    cur,
    conn,
    *,
    desde_ts: int,
    hasta_ts: int,
    stats: dict[str, Any],
    log_id: int | None,
    raw_items_counter_key: str | None = None,
    date_range_field: str = "emissiondaterange",
    finalize_log: bool = True,
) -> None:
    """Paginación por ``offset``; mismo cliente robusto que resync (429/5xx/red)."""
    if date_range_field not in ("emissiondaterange", "generationdaterange"):
        raise ValueError(f"date_range_field inválido: {date_range_field}")
    offset = 0
    pending: list[dict[str, Any]] = []
    pages = 0
    while True:
        params = {
            "limit": LIMIT_BSALE,
            "offset": offset,
            date_range_field: f"[{desde_ts},{hasta_ts}]",
        }
        # Paginación HTTP: nunca sostener TX abierta mientras se espera a Bsale.
        release_transaction(conn, job="fetch_documents_window")
        data = _documents_get_resync(client, params)
        items = data.get("items") or []
        pages += 1
        if not items:
            break

        if raw_items_counter_key:
            stats[raw_items_counter_key] = int(stats.get(raw_items_counter_key) or 0) + len(items)

        _append_items_from_bsale_response(items, pending, stats)
        _flush_pending_when_large(client, cur, conn, pending, stats)

        offset += len(items)
        time.sleep(random.uniform(0.15, 0.45))

    _flush_pending_tail(client, cur, conn, pending, stats)

    stats[f"api_pages_{date_range_field}"] = int(
        stats.get(f"api_pages_{date_range_field}") or 0
    ) + pages

    if not finalize_log:
        return

    upd = int(stats.get("updated_documents", 0) or 0)
    proc = int(stats.get("documents_processed", 0) or 0)
    stats["documents_updated"] = upd
    stats["documents_inserted"] = max(0, proc - upd)
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


def sync_bsale_distribuidora_incremental(
    *,
    strict_token: bool = False,
    process_name: str = PROCESS_INCREMENTAL,
    allowed_document_type_ids: frozenset[int] | None = None,
) -> dict[str, Any]:
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
        "process_name": process_name,
        "documents_processed": 0,
        "documents_inserted": 0,
        "documents_updated": 0,
        "updated_documents": 0,
        "documents_batch_failures": 0,
        "document_errors": 0,
        "document_upsert_failures": 0,
        "seller_sync_failures": 0,
        "skipped_other_office": 0,
        "skipped_other_company": 0,
        "skipped_document_type_filter": 0,
        "details_inserted": 0,
        "attributes_inserted": 0,
        "references_inserted": 0,
        "details_rows": 0,
        "attributes_rows": 0,
        "references_rows": 0,
        "sellers_filled": 0,
        "document_sellers_rows": 0,
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

        # Advisory lock es de sesión: commit inmediato para no dejar
        # idle in transaction sosteniendo locks de catálogo/relación.
        conn.commit()
        log_tx(
            "COMMIT",
            job=f"sync_incremental:{process_name}",
            conn=conn,
            step="after_advisory_lock",
            pg_pid=pg_backend_pid(conn),
        )
        if allowed_document_type_ids is not None:
            ensure_sync_state_row(cur, process_name)
        conn.commit()

        log_id = start_sync_log(cur, process_name)
        conn.commit()

        last_sync = get_last_sync(cur, process_name)
        if last_sync is None:
            last_sync = datetime(2000, 1, 1, tzinfo=timezone.utc)
        elif last_sync.tzinfo is None:
            last_sync = last_sync.replace(tzinfo=timezone.utc)

        now = datetime.now(timezone.utc)
        raw_items_counter_key: str | None = None

        if process_name == PROCESS_SALES:
            # Ventana fija en calendario UTC: captura NC u otros con emission_date antigua
            # pero creados recientemente (no basta el cursor last_sync).
            window_days = _sales_sliding_window_days()
            today_utc = now.date()
            from_day = today_utc - timedelta(days=window_days)
            desde = datetime(from_day.year, from_day.month, from_day.day, 0, 0, 0, tzinfo=timezone.utc)
            desde_ts = int(desde.timestamp())
            hasta_ts = int(now.timestamp())
            if desde_ts >= hasta_ts:
                desde_ts = hasta_ts - 3600
            stats["sales_sync_window_days"] = window_days
            stats["sales_window_from_date"] = from_day.isoformat()
            stats["sales_window_to_date"] = today_utc.isoformat()
            stats["sales_window_api_docs"] = 0
            raw_items_counter_key = "sales_window_api_docs"
            logger.info(
                "distribuidora sync incremental (%s): ventana deslizante %s días UTC "
                "(SALES_SYNC_WINDOW_DAYS) | epoch [%s, %s]",
                process_name,
                window_days,
                desde_ts,
                hasta_ts,
            )
            orders_dual_sync = False
        elif process_name == PROCESS_ORDERS:
            emission_days = _orders_emission_window_days()
            gen_days = _orders_generation_window_days()
            today_utc = now.date()
            from_emission = today_utc - timedelta(days=emission_days)
            from_gen = today_utc - timedelta(days=gen_days)
            desde = datetime(
                from_emission.year,
                from_emission.month,
                from_emission.day,
                0,
                0,
                0,
                tzinfo=timezone.utc,
            )
            desde_gen = datetime(
                from_gen.year,
                from_gen.month,
                from_gen.day,
                0,
                0,
                0,
                tzinfo=timezone.utc,
            )
            desde_ts = int(desde.timestamp())
            desde_gen_ts = int(desde_gen.timestamp())
            hasta_ts = int(now.timestamp())
            if desde_ts >= hasta_ts:
                desde_ts = hasta_ts - 3600
            stats["orders_emission_window_days"] = emission_days
            stats["orders_generation_window_days"] = gen_days
            stats["orders_emission_from_date"] = from_emission.isoformat()
            stats["orders_generation_from_date"] = from_gen.isoformat()
            logger.info(
                "sync-orders: emissiondaterange últimos %s d (%s) + generationdaterange últimos %s d (%s)",
                emission_days,
                from_emission.isoformat(),
                gen_days,
                from_gen.isoformat(),
            )
            orders_dual_sync = True
        else:
            if last_sync < _FIRST_SYNC_CUTOFF:
                desde = now - timedelta(days=30)
                logger.info(
                    "sync incremental (%s): primer ciclo amplio (30 días)",
                    process_name,
                )
            else:
                desde = last_sync - timedelta(hours=2)

            desde_ts = int(desde.timestamp())
            hasta_ts = int(now.timestamp())
            if desde_ts >= hasta_ts:
                desde_ts = hasta_ts - 3600
            orders_dual_sync = False

        if allowed_document_type_ids is not None:
            stats["_allowed_document_type_ids"] = allowed_document_type_ids

        client = BsaleClient(token)
        if orders_dual_sync:
            logger.info(
                "distribuidora sync incremental (%s): company_id=%s office_id=%s | "
                "emission epoch [%s, %s] luego generation epoch [%s, %s]",
                process_name,
                COMPANY_ID,
                OFFICE_ID,
                desde_ts,
                hasta_ts,
                desde_gen_ts,
                hasta_ts,
            )
            _fetch_documents_window(
                client,
                cur,
                conn,
                desde_ts=desde_ts,
                hasta_ts=hasta_ts,
                stats=stats,
                log_id=None,
                date_range_field="emissiondaterange",
                finalize_log=False,
            )
            _fetch_documents_window(
                client,
                cur,
                conn,
                desde_ts=desde_gen_ts,
                hasta_ts=hasta_ts,
                stats=stats,
                log_id=log_id,
                date_range_field="generationdaterange",
                finalize_log=True,
            )
        else:
            logger.info(
                "distribuidora sync incremental (%s): company_id=%s office_id=%s officeid=%s | "
                "epoch [%s, %s]",
                process_name,
                COMPANY_ID,
                OFFICE_ID,
                OFFICE_ID,
                desde_ts,
                hasta_ts,
            )
            _fetch_documents_window(
                client,
                cur,
                conn,
                desde_ts=desde_ts,
                hasta_ts=hasta_ts,
                stats=stats,
                log_id=log_id,
                raw_items_counter_key=raw_items_counter_key,
            )

        if process_name == PROCESS_SALES:
            logger.info(
                "[SYNC SALES WINDOW] Rango: %s → %s | Docs API: %s | Insertados: %s | Actualizados: %s",
                stats.get("sales_window_from_date"),
                stats.get("sales_window_to_date"),
                int(stats.get("sales_window_api_docs") or 0),
                int(stats.get("documents_inserted") or 0),
                int(stats.get("updated_documents") or 0),
            )

        stats.pop("_allowed_document_type_ids", None)

        snapshot_msg: str | None = None
        if process_name == PROCESS_ORDERS:
            snap = _log_orders_sync_summary(
                cur,
                emission_from=desde,
                emission_to=now,
                stats=stats,
                desde_ts=desde_ts,
                hasta_ts=hasta_ts,
            )
            snapshot_msg = json.dumps({**snap, "kind": "orders"}, ensure_ascii=False)
        elif process_name == PROCESS_SALES:
            snap = _log_sales_sync_summary(
                cur,
                emission_from=desde,
                emission_to=now,
                stats=stats,
                desde_ts=desde_ts,
                hasta_ts=hasta_ts,
            )
            snapshot_msg = json.dumps({**snap, "kind": "sales"}, ensure_ascii=False)

        proc_count = int(stats.get("documents_processed", 0))
        insert_sync_status_row(
            cur,
            sync_type="documents",
            records_processed=proc_count,
            status="success",
        )
        insert_sync_status_row(
            cur,
            sync_type="details",
            records_processed=int(stats.get("details_rows", 0)),
            status="success",
        )
        if process_name == PROCESS_ORDERS:
            insert_sync_status_row(
                cur, sync_type="orders", records_processed=proc_count, status="success"
            )
            insert_sync_status_row(cur, sync_type="sales", records_processed=0, status="success")
        elif process_name == PROCESS_SALES:
            insert_sync_status_row(cur, sync_type="orders", records_processed=0, status="success")
            insert_sync_status_row(
                cur, sync_type="sales", records_processed=proc_count, status="success"
            )
        else:
            insert_sync_status_row(cur, sync_type="orders", records_processed=0, status="success")
            insert_sync_status_row(cur, sync_type="sales", records_processed=0, status="success")

        set_sync_state(
            cur,
            process_name=process_name,
            last_sync=now,
            last_status="ok",
            last_message=snapshot_msg
            if snapshot_msg is not None
            else f"processed={stats['documents_processed']}",
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
            "sync distribuidora incremental OK (%s): processed=%s updated_documents=%s details=%s "
            "attr=%s ref=%s omitidos_tipo=%s s=%.2f",
            process_name,
            stats["documents_processed"],
            stats.get("updated_documents", 0),
            stats["details_inserted"],
            stats["attributes_inserted"],
            stats["references_inserted"],
            int(stats.get("skipped_document_type_filter") or 0),
            time.perf_counter() - t0,
        )
        logger.info("Documentos procesados: %s", stats["documents_processed"])
        logger.info("Errores: %s", int(stats.get("document_errors") or 0))
        logger.info("Sellers fallidos: %s", int(stats.get("seller_sync_failures") or 0))
        logger.info(
            "Inserts/upserts fallidos: %s",
            int(stats.get("document_upsert_failures") or 0),
        )
    except Exception as e:
        stats.pop("_allowed_document_type_ids", None)
        logger.exception("sync distribuidora incremental: %s", e)
        stats["errors"] = str(e)
        try:
            c2 = conn.cursor()
            if log_id is not None:
                finish_sync_log(c2, log_id, status="error", stats=stats, message=str(e))
            if process_name in (PROCESS_ORDERS, PROCESS_SALES):
                set_sync_state(
                    c2,
                    process_name=process_name,
                    last_status="error",
                    last_message=json.dumps(
                        {"error": str(e)[:2000], "kind": process_name},
                        ensure_ascii=False,
                    ),
                )
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


def sync_bsale_distribuidora_orders_incremental(*, strict_token: bool = False) -> dict[str, Any]:
    """Solo OC Bsale (``document_type_id`` 33); pre-despacho excluye por relación 1/6 en consultas."""
    return sync_bsale_distribuidora_incremental(
        strict_token=strict_token,
        process_name=PROCESS_ORDERS,
        allowed_document_type_ids=DOC_TYPES_OC,
    )


def sync_bsale_distribuidora_sales_incremental(*, strict_token: bool = False) -> dict[str, Any]:
    """
    Boletas (1), facturas (6) y notas de crédito (9).

    Usa siempre una ventana deslizante en ``emissiondaterange`` (últimos N días UTC,
    N = ``SALES_SYNC_WINDOW_DAYS``, default 10), además del upsert habitual, para capturar
    documentos creados hoy con ``emission_date`` antigua (p. ej. NC).
    """
    return sync_bsale_distribuidora_incremental(
        strict_token=strict_token,
        process_name=PROCESS_SALES,
        allowed_document_type_ids=DOC_TYPES_SALES,
    )


def resync_bsale_distribuidora_range(
    *,
    emission_from: datetime | None = None,
    emission_to: datetime | None = None,
    strict_token: bool = True,
    on_progress: Callable[[dict[str, Any]], None] | None = None,
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
        "updated_documents": 0,
        "documents_batch_failures": 0,
        "document_errors": 0,
        "document_upsert_failures": 0,
        "seller_sync_failures": 0,
        "skipped_other_office": 0,
        "skipped_other_company": 0,
        "details_inserted": 0,
        "attributes_inserted": 0,
        "references_inserted": 0,
        "details_rows": 0,
        "attributes_rows": 0,
        "references_rows": 0,
        "sellers_filled": 0,
        "document_sellers_rows": 0,
        "days_processed": 0,
        "duration_seconds": 0.0,
        "omitido_concurrencia": False,
        "errors": None,
    }
    if on_progress is not None:
        stats["_on_progress"] = on_progress

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
            stats.pop("_on_progress", None)
            stats["duration_seconds"] = round(time.perf_counter() - t0, 3)
            return stats

        conn.commit()
        log_tx(
            "COMMIT",
            job="resync_range",
            conn=conn,
            step="after_advisory_lock",
            pg_pid=pg_backend_pid(conn),
        )

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
        if on_progress is not None:
            logger.info(
                "resync_oc started from %s to %s",
                emission_from.isoformat(),
                emission_to.isoformat(),
            )
            on_progress(
                {
                    "documents_processed": 0,
                    "updated_documents": 0,
                    "document_errors": 0,
                    "message": "Iniciando resync",
                }
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
            "(Bsale GET /documents.json con officeid=%s)",
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

        upd = int(stats.get("updated_documents", 0) or 0)
        proc = int(stats.get("documents_processed", 0) or 0)
        stats["documents_updated"] = upd
        stats["documents_inserted"] = max(0, proc - upd)
        stats["details_inserted"] = stats.get("details_rows", 0)
        stats["attributes_inserted"] = stats.get("attributes_rows", 0)
        stats["references_inserted"] = stats.get("references_rows", 0)

        if on_progress is not None:
            err_n = int(stats.get("document_errors") or 0)
            logger.info(
                "resync_oc finished: processed=%s updated=%s errors=%s",
                proc,
                upd,
                err_n,
            )
            on_progress(
                {
                    "documents_processed": proc,
                    "updated_documents": upd,
                    "document_errors": err_n,
                    "message": "Finalizando",
                }
            )

        logger.info("Documentos procesados: %s", stats["documents_processed"])
        logger.info("Errores: %s", int(stats.get("document_errors") or 0))
        logger.info("Sellers fallidos: %s", int(stats.get("seller_sync_failures") or 0))
        logger.info(
            "Inserts/upserts fallidos: %s",
            int(stats.get("document_upsert_failures") or 0),
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
            "resync distribuidora OK: days=%s processed=%s updated_documents=%s s=%.2f",
            stats["days_processed"],
            stats["documents_processed"],
            stats.get("updated_documents", 0),
            time.perf_counter() - t0,
        )
    except Exception as e:
        logger.exception("resync distribuidora: %s", e)
        stats["errors"] = str(e)
        stats.pop("_on_progress", None)
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

    stats.pop("_on_progress", None)
    stats["duration_seconds"] = round(time.perf_counter() - t0, 3)
    return stats


BACKFILL_MAY_2026_START = date(2026, 5, 1)
BACKFILL_MAY_2026_END = date(2026, 5, 31)
BACKFILL_MAY_LOG_PROCESS = "backfill_documents_may_2026"


def _backfill_may_overlap_days() -> int:
    try:
        n = int(os.getenv("BACKFILL_MAY_OVERLAP_DAYS", "0"))
    except ValueError:
        n = 0
    return max(0, min(n, 7))


def _backfill_may_extra_page_sleep() -> float:
    try:
        return max(0.0, float(os.getenv("BACKFILL_MAY_EXTRA_PAGE_SLEEP_SEC", "0")))
    except ValueError:
        return 0.0


def _backfill_may_day_max_retries() -> int:
    try:
        n = int(os.getenv("BACKFILL_MAY_DAY_MAX_RETRIES", "5"))
    except ValueError:
        n = 5
    return max(1, min(20, n))


def _backfill_may_page_limit() -> int:
    raw = os.getenv("BACKFILL_MAY_PAGE_LIMIT")
    if raw is None or not str(raw).strip():
        return _resync_page_limit()
    try:
        return max(25, min(50, int(raw)))
    except ValueError:
        return _resync_page_limit()


def backfill_distribuidora_documents_may_2026_documents_only(
    *,
    strict_token: bool = True,
) -> dict[str, Any]:
    """
    Backfill oficial **solo tabla** ``distribuidora.documents`` (company 3, office 1),
    emisión por día UTC **2026-05-01 … 2026-05-31** (más días opcionales hacia atrás vía
    ``BACKFILL_MAY_OVERLAP_DAYS``).

    No sincroniza details, attributes, references ni sellers (FASE 7.5). Idempotente vía
    ``upsert_documents`` / ``ON CONFLICT``. Usa advisory lock principal de documentos,
    ``sync_status`` y ``sync_state`` (``documents`` + ``backfill``).
    """
    t0 = time.perf_counter()
    token = _bsale_token()
    if not token:
        if strict_token:
            raise ValueError("Ningún token Bsale: defina BSALE_TOKEN o BSALE_TOKEN_SPA.")
        return {
            "skipped": True,
            "skip_reason": "sin token",
            "duration_seconds": round(time.perf_counter() - t0, 3),
            "omitido_concurrencia": False,
        }

    overlap = _backfill_may_overlap_days()
    start_d = BACKFILL_MAY_2026_START - timedelta(days=overlap)
    end_d = BACKFILL_MAY_2026_END
    extra_sleep = _backfill_may_extra_page_sleep()
    page_lim = _backfill_may_page_limit()
    max_day_retries = _backfill_may_day_max_retries()

    stats: dict[str, Any] = {
        "mode": "backfill_documents_may_2026_documents_only",
        "documents_processed": 0,
        "documents_inserted": 0,
        "documents_updated": 0,
        "updated_documents": 0,
        "document_errors": 0,
        "document_upsert_failures": 0,
        "seller_sync_failures": 0,
        "details_rows": 0,
        "attributes_rows": 0,
        "references_rows": 0,
        "details_inserted": 0,
        "attributes_inserted": 0,
        "references_inserted": 0,
        "skipped_other_office": 0,
        "skipped_other_company": 0,
        "skipped_document_type_filter": 0,
        "days_processed": 0,
        "document_api_pages": 0,
        "calendar_start": start_d.isoformat(),
        "calendar_end": end_d.isoformat(),
        "overlap_days": overlap,
        "duration_seconds": 0.0,
        "omitido_concurrencia": False,
        "errors": None,
        "_documents_only_skip_children": True,
        "_count_document_pages": True,
    }

    conn = get_connection()
    got_lock = False
    log_id: int | None = None
    cur = conn.cursor()
    try:
        cur.execute("SELECT pg_try_advisory_lock(%s)", (ADVISORY_LOCK_KEY,))
        got_lock = bool(cur.fetchone()[0])
        if not got_lock:
            stats["omitido_concurrencia"] = True
            cur.close()
            stats["duration_seconds"] = round(time.perf_counter() - t0, 3)
            return stats

        conn.commit()
        log_tx(
            "COMMIT",
            job="backfill_documents_may_2026",
            conn=conn,
            step="after_advisory_lock",
            pg_pid=pg_backend_pid(conn),
        )

        log_id = start_sync_log(cur, BACKFILL_MAY_LOG_PROCESS)
        conn.commit()

        logger.info(
            "backfill_documents_may_2026: company_id=%s office_id=%s rango %s..%s overlap_days=%s",
            COMPANY_ID,
            OFFICE_ID,
            start_d.isoformat(),
            end_d.isoformat(),
            overlap,
        )

        client = BsaleClient(token)
        current = start_d
        while current <= end_d:
            day_ok = False
            last_err: Exception | None = None
            for attempt in range(max_day_retries):
                try:
                    _fetch_documents_single_day_resync(
                        client,
                        cur,
                        conn,
                        current,
                        stats,
                        page_limit=page_lim,
                        extra_page_sleep_sec=extra_sleep,
                    )
                    stats["days_processed"] = int(stats.get("days_processed") or 0) + 1
                    logger.info(
                        "backfill_may día OK %s (docs_proc=%s páginas=%s)",
                        current.isoformat(),
                        int(stats.get("documents_processed") or 0),
                        int(stats.get("document_api_pages") or 0),
                    )
                    day_ok = True
                    break
                except Exception as e:
                    last_err = e
                    logger.warning(
                        "backfill_may día %s intento %s/%s: %s",
                        current.isoformat(),
                        attempt + 1,
                        max_day_retries,
                        e,
                        exc_info=attempt + 1 == max_day_retries,
                    )
                    time.sleep(min(60.0, 5.0 * (attempt + 1)))
            if not day_ok:
                raise RuntimeError(
                    f"backfill_may: día {current.isoformat()} falló tras {max_day_retries} intentos"
                ) from last_err
            current += timedelta(days=1)

        upd = int(stats.get("updated_documents", 0) or 0)
        proc = int(stats.get("documents_processed", 0) or 0)
        stats["documents_updated"] = upd
        stats["documents_inserted"] = max(0, proc - upd)

        window_from = datetime(
            start_d.year, start_d.month, start_d.day, 0, 0, 0, tzinfo=timezone.utc
        )
        window_to = datetime(
            end_d.year, end_d.month, end_d.day, 23, 59, 59, tzinfo=timezone.utc
        )
        insert_sync_status_row(
            cur,
            sync_type="documents",
            records_processed=proc,
            status="success",
        )
        update_sync_state_success(
            cur,
            sync_type="documents",
            mode=MODE_BACKFILL,
            office_id=OFFICE_ID,
            last_window_from=window_from,
            last_window_to=window_to,
            last_watermark=window_to,
            overlap_days=overlap if overlap else None,
            overlap_seconds=None,
            items_processed=proc,
            status="success",
        )
        if log_id is not None:
            finish_sync_log(
                cur,
                log_id,
                status="ok",
                stats=stats,
                message="backfill_documents_may_2026 documents_only OK",
            )
        conn.commit()
        cur.close()
        logger.info(
            "backfill_documents_may_2026 OK: days=%s processed=%s inserted≈%s updated≈%s "
            "errors=%s páginas_api=%s s=%.2f",
            stats["days_processed"],
            proc,
            stats["documents_inserted"],
            upd,
            int(stats.get("document_errors") or 0),
            int(stats.get("document_api_pages") or 0),
            time.perf_counter() - t0,
        )
    except Exception as e:
        logger.exception("backfill_documents_may_2026: %s", e)
        stats["errors"] = str(e)
        try:
            c2 = conn.cursor()
            if log_id is not None:
                finish_sync_log(c2, log_id, status="error", stats=stats, message=str(e))
            update_sync_state_error(
                c2,
                sync_type="documents",
                mode=MODE_BACKFILL,
                office_id=OFFICE_ID,
                error_summary=str(e),
                status="error",
                items_processed=int(stats.get("documents_processed") or 0),
            )
            insert_sync_status_row(
                c2,
                sync_type="documents",
                records_processed=int(stats.get("documents_processed") or 0),
                status="error",
            )
            conn.commit()
            c2.close()
        except Exception:
            logger.exception("backfill_documents_may_2026: error al persistir fallo")
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
            logger.exception("backfill_may advisory unlock")
        try:
            conn.close()
        except Exception:
            pass

    stats["duration_seconds"] = round(time.perf_counter() - t0, 3)
    return stats


BACKFILL_MAY_DETAILS_LOG_PROCESS = "backfill_details_may_2026"


def _backfill_may_details_overlap_days() -> int:
    try:
        n = int(os.getenv("BACKFILL_MAY_DETAILS_OVERLAP_DAYS", "0"))
    except ValueError:
        n = 0
    return max(0, min(n, 7))


def _backfill_may_details_document_batch() -> int:
    try:
        n = int(os.getenv("BACKFILL_MAY_DETAILS_DOCUMENT_BATCH", "250"))
    except ValueError:
        n = 250
    return max(1, min(n, 2000))


def _backfill_may_details_doc_retries() -> int:
    try:
        n = int(os.getenv("BACKFILL_MAY_DETAILS_DOC_RETRIES", "4"))
    except ValueError:
        n = 4
    return max(1, min(10, n))


def _backfill_may_details_extra_sleep() -> float:
    try:
        return max(0.0, float(os.getenv("BACKFILL_MAY_DETAILS_EXTRA_SLEEP_SEC", "0")))
    except ValueError:
        return 0.0


def _count_document_details_rows(cur, document_id: int) -> int:
    cur.execute(
        """
        SELECT COUNT(*)::int
        FROM distribuidora.document_details
        WHERE document_id = %s
        """,
        (document_id,),
    )
    row = cur.fetchone()
    return int(row[0] or 0) if row else 0


def backfill_distribuidora_document_details_may_2026_only(
    *,
    strict_token: bool = True,
) -> dict[str, Any]:
    """
    Backfill oficial **solo** ``distribuidora.document_details`` para documentos ya
    presentes en BD con emisión UTC mayo 2026 (2026-05-01 … 2026-05-31 inclusive, más
    ``BACKFILL_MAY_DETAILS_OVERLAP_DAYS`` hacia atrás desde el 1).

    Por cada ``document_id``: ``GET /documents/{id}/details.json`` y ``replace_document_details``
    (DELETE + INSERT por documento; idempotente en re-ejecución). Company 3, office 1.

    Usa el mismo advisory lock que documentos para no solaparse con sync que toque details.
    """
    t0 = time.perf_counter()
    token = _bsale_token()
    if not token:
        if strict_token:
            raise ValueError("Ningún token Bsale: defina BSALE_TOKEN o BSALE_TOKEN_SPA.")
        return {
            "skipped": True,
            "skip_reason": "sin token",
            "duration_seconds": round(time.perf_counter() - t0, 3),
            "omitido_concurrencia": False,
        }

    overlap = _backfill_may_details_overlap_days()
    start_d = BACKFILL_MAY_2026_START - timedelta(days=overlap)
    end_d = BACKFILL_MAY_2026_END
    batch = _backfill_may_details_document_batch()
    max_retries = _backfill_may_details_doc_retries()
    extra_sleep = _backfill_may_details_extra_sleep()

    try:
        max_docs = int(os.getenv("BACKFILL_MAY_DETAILS_MAX_DOCUMENTS", "0"))
    except ValueError:
        max_docs = 0
    max_docs = max(0, min(max_docs, 500_000))

    resume_after_raw = (os.getenv("BACKFILL_MAY_DETAILS_RESUME_AFTER_DOCUMENT_ID") or "").strip()
    resume_after: int | None = None
    if resume_after_raw:
        try:
            resume_after = int(resume_after_raw)
        except ValueError:
            resume_after = None

    emission_from = datetime(
        start_d.year, start_d.month, start_d.day, 0, 0, 0, tzinfo=timezone.utc
    )
    emission_to_excl = datetime(
        end_d.year, end_d.month, end_d.day, 0, 0, 0, tzinfo=timezone.utc
    ) + timedelta(days=1)

    stats: dict[str, Any] = {
        "mode": "backfill_details_may_2026_only",
        "documents_processed": 0,
        "documents_with_zero_lines_after": 0,
        "documents_first_fill": 0,
        "documents_refreshed": 0,
        "details_rows_written": 0,
        "details_rows_replaced_proxy": 0,
        "document_errors": 0,
        "details_inserted": 0,
        "documents_inserted": 0,
        "documents_updated": 0,
        "attributes_inserted": 0,
        "references_inserted": 0,
        "calendar_start": start_d.isoformat(),
        "calendar_end": end_d.isoformat(),
        "overlap_days": overlap,
        "document_batches": 0,
        "duration_seconds": 0.0,
        "omitido_concurrencia": False,
        "errors": None,
    }

    conn = get_connection()
    got_lock = False
    log_id: int | None = None
    cur = conn.cursor()
    try:
        cur.execute("SELECT pg_try_advisory_lock(%s)", (ADVISORY_LOCK_KEY,))
        got_lock = bool(cur.fetchone()[0])
        if not got_lock:
            stats["omitido_concurrencia"] = True
            cur.close()
            stats["duration_seconds"] = round(time.perf_counter() - t0, 3)
            return stats

        conn.commit()
        log_tx(
            "COMMIT",
            job="backfill_details_may_2026",
            conn=conn,
            step="after_advisory_lock",
            pg_pid=pg_backend_pid(conn),
        )

        log_id = start_sync_log(cur, BACKFILL_MAY_DETAILS_LOG_PROCESS)
        conn.commit()

        logger.info(
            "backfill_details_may_2026: company_id=%s office_id=%s emission [%s, %s) overlap_days=%s",
            COMPANY_ID,
            OFFICE_ID,
            emission_from.isoformat(),
            emission_to_excl.isoformat(),
            overlap,
        )

        client = BsaleClient(token)
        processed_cap = 0
        last_id = resume_after if resume_after is not None else 0

        while True:
            cur.execute(
                """
                SELECT d.document_id, d.number, d.raw_data->>'id'
                FROM distribuidora.documents d
                WHERE d.company_id = %s
                  AND d.office_id = %s
                  AND d.emission_date >= %s
                  AND d.emission_date < %s
                  AND d.document_id > %s
                ORDER BY d.document_id
                LIMIT %s
                """,
                (COMPANY_ID, OFFICE_ID, emission_from, emission_to_excl, last_id, batch),
            )
            id_rows = cur.fetchall() or []
            # Liberar AccessShareLock del SELECT antes de HTTP por documento.
            conn.commit()
            if not id_rows:
                break
            stats["document_batches"] = int(stats.get("document_batches") or 0) + 1

            from backend.utils.bsale_document_ids import (
                ids_differ,
                resolve_bsale_source_document_id,
            )

            for document_id, number, raw_bsale_id in id_rows:
                if max_docs and processed_cap >= max_docs:
                    break
                doc_id = int(document_id)
                source_id = resolve_bsale_source_document_id(
                    local_document_id=doc_id,
                    raw_data_id=raw_bsale_id,
                )
                try:
                    folio_int = int(number) if number is not None else None
                except (TypeError, ValueError):
                    folio_int = None
                release_transaction(conn, job=f"backfill_details:{doc_id}")
                before_n = _count_document_details_rows(cur, doc_id)
                conn.commit()
                last_err: Exception | None = None
                written = 0
                for attempt in range(max_retries):
                    try:
                        logger.info(
                            "backfill_details folio=%s local_document_id=%s "
                            "bsale_source_document_id=%s ids_differ=%s",
                            folio_int,
                            doc_id,
                            source_id,
                            ids_differ(doc_id, source_id),
                        )
                        det = client.get(f"/documents/{source_id}/details.json", timeout=90)
                        items = det.get("items") if isinstance(det, dict) else []
                        if not isinstance(items, list):
                            items = []
                        written = replace_document_details(cur, doc_id, items)
                        conn.commit()
                        logger.info(
                            "backfill_details_done folio=%s local_document_id=%s "
                            "bsale_source_document_id=%s details_replaced=%s",
                            folio_int,
                            doc_id,
                            source_id,
                            written,
                        )
                        last_err = None
                        break
                    except Exception as e:
                        last_err = e
                        logger.warning(
                            "backfill_details local=%s bsale_source=%s intento %s/%s: %s",
                            doc_id,
                            source_id,
                            attempt + 1,
                            max_retries,
                            e,
                        )
                        try:
                            conn.rollback()
                        except Exception:
                            pass
                        time.sleep(min(45.0, 2.0 * (attempt + 1)))

                if last_err is not None:
                    stats["document_errors"] = int(stats.get("document_errors") or 0) + 1
                    logger.error(
                        "backfill_details doc_id=%s falló tras %s intentos: %s",
                        doc_id,
                        max_retries,
                        last_err,
                    )
                    processed_cap += 1
                    stats["documents_processed"] = int(stats.get("documents_processed") or 0) + 1
                    time.sleep(random.uniform(0.15, 0.45) + extra_sleep)
                    last_id = doc_id
                    continue

                stats["documents_processed"] = int(stats.get("documents_processed") or 0) + 1
                stats["details_rows_written"] = int(stats.get("details_rows_written") or 0) + int(
                    written
                )
                if before_n == 0 and written > 0:
                    stats["documents_first_fill"] = int(stats.get("documents_first_fill") or 0) + 1
                elif before_n > 0:
                    stats["documents_refreshed"] = int(stats.get("documents_refreshed") or 0) + 1
                    stats["details_rows_replaced_proxy"] = int(
                        stats.get("details_rows_replaced_proxy") or 0
                    ) + min(before_n, written)
                if written == 0:
                    stats["documents_with_zero_lines_after"] = (
                        int(stats.get("documents_with_zero_lines_after") or 0) + 1
                    )

                processed_cap += 1
                time.sleep(random.uniform(0.15, 0.45) + extra_sleep)
                last_id = doc_id

            if max_docs and processed_cap >= max_docs:
                logger.info("backfill_details_may_2026: tope MAX_DOCUMENTS=%s alcanzado", max_docs)
                break
            if len(id_rows) < batch:
                break

        stats["details_inserted"] = int(stats.get("details_rows_written") or 0)

        insert_sync_status_row(
            cur,
            sync_type="details",
            records_processed=int(stats.get("details_rows_written") or 0),
            status="success",
        )
        update_sync_state_success(
            cur,
            sync_type="details",
            mode=MODE_BACKFILL,
            office_id=OFFICE_ID,
            last_window_from=emission_from,
            last_window_to=emission_to_excl - timedelta(seconds=1),
            last_watermark=emission_to_excl - timedelta(seconds=1),
            overlap_days=overlap if overlap else None,
            overlap_seconds=None,
            items_processed=int(stats.get("details_rows_written") or 0),
            status="success",
        )
        if log_id is not None:
            finish_sync_log(
                cur,
                log_id,
                status="ok",
                stats=stats,
                message="backfill_details_may_2026 OK",
            )
        conn.commit()
        cur.close()
        logger.info(
            "backfill_details_may_2026 OK: docs=%s filas_details=%s sin_lineas=%s "
            "primer_llenado=%s refresco_docs=%s errores=%s s=%.2f",
            int(stats.get("documents_processed") or 0),
            int(stats.get("details_rows_written") or 0),
            int(stats.get("documents_with_zero_lines_after") or 0),
            int(stats.get("documents_first_fill") or 0),
            int(stats.get("documents_refreshed") or 0),
            int(stats.get("document_errors") or 0),
            time.perf_counter() - t0,
        )
    except Exception as e:
        logger.exception("backfill_details_may_2026: %s", e)
        stats["errors"] = str(e)
        try:
            c2 = conn.cursor()
            if log_id is not None:
                finish_sync_log(c2, log_id, status="error", stats=stats, message=str(e))
            update_sync_state_error(
                c2,
                sync_type="details",
                mode=MODE_BACKFILL,
                office_id=OFFICE_ID,
                error_summary=str(e),
                status="error",
                items_processed=int(stats.get("details_rows_written") or 0),
            )
            insert_sync_status_row(
                c2,
                sync_type="details",
                records_processed=int(stats.get("details_rows_written") or 0),
                status="error",
            )
            conn.commit()
            c2.close()
        except Exception:
            logger.exception("backfill_details_may_2026: error al persistir fallo")
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
            logger.exception("backfill_details_may advisory unlock")
        try:
            conn.close()
        except Exception:
            pass

    stats["duration_seconds"] = round(time.perf_counter() - t0, 3)
    return stats


def run_incremental_distribuidora_background() -> None:
    """Mismo flujo que el job programado: órdenes (33), ventas (1/6/9), luego relaciones."""
    try:
        sync_bsale_distribuidora_orders_incremental(strict_token=True)
        sync_bsale_distribuidora_sales_incremental(strict_token=True)
        from backend.services.distribuidora.sync_related_service import (
            run_sync_distribuidora_related_background,
        )

        run_sync_distribuidora_related_background()
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
        o = sync_bsale_distribuidora_orders_incremental(strict_token=strict_token)
        s = sync_bsale_distribuidora_sales_incremental(strict_token=strict_token)
        try:
            from backend.services.distribuidora.sync_related_service import (
                run_sync_distribuidora_related_background,
            )

            run_sync_distribuidora_related_background()
        except Exception:
            logger.exception("DistribuidoraSyncService.run_incremental: related falló")
        return {"orders": o, "sales": s}

    @staticmethod
    def run_resync(
        *,
        emission_from: datetime | None = None,
        emission_to: datetime | None = None,
        strict_token: bool = True,
        on_progress: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        return resync_bsale_distribuidora_range(
            emission_from=emission_from,
            emission_to=emission_to,
            strict_token=strict_token,
            on_progress=on_progress,
        )
