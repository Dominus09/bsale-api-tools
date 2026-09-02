"""
Sync incremental de relaciones **operacionales** OC → ventas/NC:

- ``GET /v1/documents/{document_id}/details.json`` y por cada ``detail.id``:
  ``GET /v1/documents.json?relateddetailid=``.

Si Bsale devuelve una OC intermedia (``document_type_id = 33``), se expande **solo** vía
``relateddetailid`` hasta ``RELATED_MAX_TYPE33_DEPTH`` (default 1) y se persisten únicamente
aristas hacia documentos terminales **1 / 6 / 9** desde el ``detail_id`` de la OC original
(no se guardan relaciones 33→33).

**No** se usa ``references.json`` aquí: la fuente de verdad para ``document_related`` es solo
``relateddetailid``. Las referencias tributarias/XML siguen en ``sync_service`` →
``distribuidora.document_references``.

Escribe ``distribuidora.document_related`` con deduplicación (``ON CONFLICT``) y filtro por office.

Incluye ``sync_related_documents_range`` para rellenar histórico por rango de emisión (día a día).

Depuración por número de OC: ``debug_sync_related_for_document`` o
``python -m backend.debug.debug_sync_related_oc [número]``.
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
from backend.repositories.distribuidora.details_repo import replace_document_details
from backend.repositories.distribuidora.sync_repo import (
    insert_sync_status_row,
)
from backend.services.distribuidora.bsale_client import BsaleClient
from backend.services.distribuidora.bsale_params import merge_bsale_office_query
from backend.services.distribuidora.oc_related_discovery_service import (
    CatchupApiError,
    DISCOVERY_MODE_FAST_CONFIRM,
    DISCOVERY_MODE_FULL,
    OcRelatedApiError,
    STOP_REASON_COMPLETED,
    STOP_REASON_RUNTIME_BUDGET,
    STOP_REASON_SKIPPED_ALREADY_RUNNING,
    apply_discovered_invoice_edges,
    classify_oc_discovery_result,
    compute_pending_rotation_offset,
    count_pending_ocs_in_lookback,
    create_bsale_client_for_related_discovery,
    discover_invoice_edges_for_oc,
    emission_date_bounds_for_document_ids,
    fetch_pending_oc_ids_for_incremental,
    fetch_recent_oc_ids_for_refresh,
    load_existing_invoice_relations_for_oc,
    merge_oc_candidate_ids,
    resolve_related_recent_pending_limit,
    resolve_related_sync_lookback_days,
    resolve_related_sync_max_runtime_sec,
)
from backend.utils.bsale_document_ids import (
    ids_differ,
    resolve_bsale_source_document_id,
)
from backend.utils.distribuidora_oc_sql import OC_PURCHASE_NOT_INVOICED_BY_RELATED_SQL

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
RELATED_DOCUMENT_TYPES_ALLOWED = frozenset({1, 6, 9})
RELATED_DETAIL_PAGE_LIMIT = 50
DETAILS_PAGE_LIMIT = 50
DEFAULT_RELATED_LOOKBACK_DAYS = 30
DEFAULT_RELATED_DETAIL_LIMIT = 250
DEFAULT_RELATED_PENDING_LIMIT = 400

# OC sin boleta/factura confirmada vía ``document_related`` (tipos 1/6, sin JOIN header).
_OC_WITHOUT_CONFIRMED_INVOICE_SQL = OC_PURCHASE_NOT_INVOICED_BY_RELATED_SQL


def _related_max_type33_depth() -> int:
    """Profundidad máxima de expansión OC (33) vía ``relateddetailid`` (default 1)."""
    try:
        v = int(os.getenv("RELATED_MAX_TYPE33_DEPTH", "1"))
    except ValueError:
        v = 1
    return max(0, min(v, 5))


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


def _oc_number_for_document(cur, document_id: int) -> int | None:
    cur.execute(
        "SELECT number FROM distribuidora.documents WHERE document_id = %s",
        (document_id,),
    )
    row = cur.fetchone()
    if not row or row[0] is None:
        return None
    return int(row[0])


def _related_log_ctx(oc_number: int | None, document_id: int) -> str:
    if oc_number is not None:
        return f"[RELATED][OC {oc_number}]"
    return f"[RELATED][DOC {document_id}]"


def _document_office_in_db(cur, document_id: int) -> int | None:
    cur.execute(
        "SELECT office_id FROM distribuidora.documents WHERE document_id = %s",
        (document_id,),
    )
    row = cur.fetchone()
    if not row or row[0] is None:
        return None
    return int(row[0])


def _doc_parts_for_type_and_office(d: dict[str, Any]) -> list[dict[str, Any]]:
    parts: list[dict[str, Any]] = [d]
    nested = d.get("document")
    if isinstance(nested, dict):
        parts.append(nested)
    return parts


def _office_id_from_blob(blob: dict[str, Any]) -> int | None:
    for part in _doc_parts_for_type_and_office(blob):
        off = part.get("office") or part.get("Office")
        if isinstance(off, dict):
            oid = _safe_int(off.get("id"))
            if oid is not None:
                return oid
    return None


def _related_item_office_id(it: dict[str, Any]) -> int | None:
    """Office del documento relacionado (raíz o anidado en ``document``)."""
    return _office_id_from_blob(it)


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


def _related_pending_limit() -> int:
    try:
        v = int(os.getenv("DISTRIBUIDORA_RELATED_PENDING_LIMIT", str(DEFAULT_RELATED_PENDING_LIMIT)))
    except ValueError:
        v = DEFAULT_RELATED_PENDING_LIMIT
    return max(1, v)


def _fetch_oc_document_ids_for_incremental(
    cur,
    *,
    lookback_days: int,
    limit_documents: int,
    pending_offset: int | None = None,
) -> tuple[list[int], dict[str, int]]:
    """
    Selección en dos buckets para el sync incremental:

    1. **Pendientes** (prioridad operacional):
       - cupo reciente (emisión DESC) siempre primero;
       - cupo aging con offset rotativo (anti-starvation hasta lookback).
    2. **Refresh**: OC recientes por ``document_id`` si queda presupuesto de ciclo.

    La unión prioriza pendientes y evita duplicados. El lookback es cobertura,
    no un batch completo por ciclo.
    """
    lb = max(1, lookback_days)
    pending_lim = _related_pending_limit()
    recent_lim = max(1, limit_documents)
    recent_pending_cap = resolve_related_recent_pending_limit(pending_lim)
    aging_lim = max(0, pending_lim - recent_pending_cap)

    total_pending = count_pending_ocs_in_lookback(cur, lookback_days=lb)
    aging_pool = max(0, total_pending - recent_pending_cap)
    if pending_offset is None:
        pending_offset = (
            compute_pending_rotation_offset(aging_pool, aging_lim)
            if aging_lim > 0
            else 0
        )

    pending_ids = fetch_pending_oc_ids_for_incremental(
        cur,
        lookback_days=lb,
        pending_limit=pending_lim,
        pending_offset=pending_offset,
        recent_pending_limit=recent_pending_cap,
    )
    recent_ids = fetch_recent_oc_ids_for_refresh(
        cur,
        lookback_days=lb,
        refresh_limit=recent_lim,
    )
    merged = merge_oc_candidate_ids(pending_ids, recent_ids)

    meta = {
        "pending_without_related": len(pending_ids),
        "pending_total_in_window": total_pending,
        "pending_offset": int(pending_offset),
        "recent_pending_cap": int(recent_pending_cap),
        "aging_pending_cap": int(aging_lim),
        "recent_refresh_candidates": len(recent_ids),
        "merged_unique": len(merged),
        "pending_ids": pending_ids,
        "refresh_ids": [x for x in recent_ids if x not in set(pending_ids)],
    }
    return merged, meta


def _fetch_oc_document_ids_for_emission_day(cur, day: date) -> list[int]:
    """``document_id`` distintos de OC (tipo 33) cuya emisión cae ese día calendario (UTC)."""
    day_start, day_end_excl = _utc_day_emission_bounds(day)
    cur.execute(
        """
        SELECT d.document_id
        FROM distribuidora.documents d
        WHERE d.document_type_id = %s
          AND d.company_id = %s
          AND d.office_id = %s
          AND d.emission_date IS NOT NULL
          AND d.emission_date >= %s
          AND d.emission_date < %s
        ORDER BY d.document_id
        """,
        (DOC_TYPE_OC, COMPANY_ID, OFFICE_ID, day_start, day_end_excl),
    )
    return [int(r[0]) for r in cur.fetchall()]


def _detail_ids_for_document(cur, document_id: int) -> list[int]:
    cur.execute(
        """
        SELECT detail_id
        FROM distribuidora.document_details
        WHERE document_id = %s
        ORDER BY detail_id
        """,
        (document_id,),
    )
    return [int(r[0]) for r in cur.fetchall()]


def _detail_ids_missing_for_document(
    cur,
    document_id: int,
    candidate_ids: list[int],
) -> list[int]:
    """
    ``detail_id`` presentes en ``candidate_ids`` que aún no están en ``document_details``
    para este ``document_id`` (evita FK al insertar ``document_related``).
    """
    uniq = list(dict.fromkeys(int(x) for x in candidate_ids if x is not None))
    if not uniq:
        return []
    cur.execute(
        """
        SELECT x.detail_id::bigint
        FROM unnest(%s::bigint[]) AS x(detail_id)
        WHERE NOT EXISTS (
            SELECT 1
            FROM distribuidora.document_details dd
            WHERE dd.document_id = %s
              AND dd.detail_id = x.detail_id
        )
        ORDER BY 1
        """,
        (uniq, document_id),
    )
    return [int(r[0]) for r in cur.fetchall()]


def _bsale_source_id_from_pg(cur, local_document_id: int) -> tuple[int, int | None]:
    """
    Resuelve id Bsale vigente desde ``raw_data->>'id'`` (si existe).

    Retorna ``(bsale_source_document_id, folio)``.
    """
    cur.execute(
        """
        SELECT number, raw_data->>'id'
        FROM distribuidora.documents
        WHERE document_id = %s
        LIMIT 1
        """,
        (int(local_document_id),),
    )
    row = cur.fetchone()
    folio = None
    raw_id = None
    if row:
        folio = row[0]
        raw_id = row[1]
    source = resolve_bsale_source_document_id(
        local_document_id=int(local_document_id),
        raw_data_id=raw_id,
    )
    try:
        folio_int = int(folio) if folio is not None else None
    except (TypeError, ValueError):
        folio_int = None
    return source, folio_int


def _fetch_all_detail_items_from_bsale(
    client: BsaleClient,
    document_id: int,
    *,
    throttle: float,
    log_ctx: str,
    bsale_source_document_id: int | None = None,
) -> tuple[list[dict[str, Any]], int]:
    """Paginación completa de ``details.json`` (items crudos para ``replace_document_details``)."""
    source_id = (
        int(bsale_source_document_id)
        if bsale_source_document_id is not None
        else int(document_id)
    )
    items_out: list[dict[str, Any]] = []
    api_calls = 0
    offset = 0
    while True:
        try:
            data = client.get(
                f"/documents/{source_id}/details.json",
                {"limit": DETAILS_PAGE_LIMIT, "offset": offset},
            )
        except Exception as e:
            logger.warning(
                "%s details.json (self-heal) local_document_id=%s "
                "bsale_source_document_id=%s offset=%s: %s",
                log_ctx,
                document_id,
                source_id,
                offset,
                e,
            )
            break
        api_calls += 1
        items = data.get("items") or []
        if not isinstance(items, list):
            break
        for it in items:
            if isinstance(it, dict):
                items_out.append(it)
        if len(items) < DETAILS_PAGE_LIMIT:
            break
        offset += len(items)
        if throttle > 0:
            time.sleep(throttle)
    return items_out, api_calls


def _self_heal_document_details_if_needed(
    client: BsaleClient,
    conn: PgConnection,
    cur,
    document_id: int,
    candidate_detail_ids: list[int],
    *,
    throttle: float,
    log_ctx: str,
    stats: dict[str, Any] | None,
) -> tuple[list[int], int]:
    """
    Si ``details.json`` expone ``detail_id`` que aún no están en BD, refresca **solo** este documento
    con ``replace_document_details`` y revalida. Retorna ``(still_missing, api_calls_extra)``.
    """
    try:
        max_attempts = int(os.getenv("RELATED_DETAILS_SELF_HEAL_MAX_ATTEMPTS", "2"))
    except ValueError:
        max_attempts = 2
    max_attempts = max(1, min(max_attempts, 5))

    extra_calls = 0
    missing = _detail_ids_missing_for_document(cur, document_id, candidate_detail_ids)
    if not missing:
        return [], 0

    logger.warning(
        "%s self-heal details: document_id=%s detail_ids_en_api_sin_BD=%s (n=%s)",
        log_ctx,
        document_id,
        missing,
        len(missing),
    )
    if stats is not None:
        stats["details_self_heal_missing_detected"] = int(
            stats.get("details_self_heal_missing_detected") or 0
        ) + len(missing)

    still_missing = list(missing)
    for attempt in range(1, max_attempts + 1):
        source_id, folio = _bsale_source_id_from_pg(cur, document_id)
        logger.info(
            "%s self-heal fetch folio=%s local_document_id=%s "
            "bsale_source_document_id=%s ids_differ=%s intento=%s",
            log_ctx,
            folio,
            document_id,
            source_id,
            ids_differ(document_id, source_id),
            attempt,
        )
        items, c_fetch = _fetch_all_detail_items_from_bsale(
            client,
            document_id,
            throttle=throttle,
            log_ctx=log_ctx,
            bsale_source_document_id=source_id,
        )
        extra_calls += c_fetch
        try:
            n_written = replace_document_details(cur, document_id, items)
            logger.info(
                "%s self-heal replace folio=%s local_document_id=%s "
                "bsale_source_document_id=%s details_replaced=%s",
                log_ctx,
                folio,
                document_id,
                source_id,
                n_written,
            )
        except Exception as e:
            logger.error(
                "%s self-heal replace_document_details falló document_id=%s intento=%s: %s",
                log_ctx,
                document_id,
                attempt,
                e,
                exc_info=True,
            )
            try:
                conn.rollback()
            except Exception:
                pass
            if stats is not None:
                stats["details_self_heal_refresh_failures"] = int(
                    stats.get("details_self_heal_refresh_failures") or 0
                ) + 1
            break

        conn.commit()
        if stats is not None:
            stats["details_self_heal_refreshes"] = int(stats.get("details_self_heal_refreshes") or 0) + 1
            stats["details_self_heal_rows_written"] = int(
                stats.get("details_self_heal_rows_written") or 0
            ) + int(n_written)

        prev_still = set(still_missing)
        still_missing = _detail_ids_missing_for_document(cur, document_id, candidate_detail_ids)
        recovered_this_attempt = sorted(prev_still - set(still_missing))
        logger.info(
            "%s self-heal details: document_id=%s intento=%s/%s filas_escritas=%s "
            "detail_ids_recuperados=%s",
            log_ctx,
            document_id,
            attempt,
            max_attempts,
            n_written,
            recovered_this_attempt,
        )
        if not still_missing:
            logger.info(
                "%s self-heal details OK: document_id=%s tras %s intento(s)",
                log_ctx,
                document_id,
                attempt,
            )
            break

        logger.warning(
            "%s self-heal details: document_id=%s tras intento %s siguen faltando detail_ids=%s",
            log_ctx,
            document_id,
            attempt,
            still_missing,
        )
        if attempt < max_attempts:
            if stats is not None:
                stats["details_self_heal_retries"] = int(stats.get("details_self_heal_retries") or 0) + 1
            if throttle > 0:
                time.sleep(min(2.0, throttle * 3))

    if still_missing:
        logger.error(
            "%s self-heal details agotado: document_id=%s detail_ids_aún_sin_BD=%s — "
            "se omitirá relateddetailid para esos ids",
            log_ctx,
            document_id,
            still_missing,
        )
        if stats is not None:
            stats["details_self_heal_still_missing"] = int(
                stats.get("details_self_heal_still_missing") or 0
            ) + len(still_missing)

    return still_missing, extra_calls


def _coerce_document_type_id(raw: Any) -> int | None:
    if raw is None:
        return None
    if isinstance(raw, dict):
        return _safe_int(raw.get("id"))
    return _safe_int(raw)


def _parse_related_document_blob(
    item: dict[str, Any],
) -> tuple[int | None, int | None, dict[str, Any], str | None]:
    """
    Soporta dos formatos de Bsale:

    1. Anidado (p. ej. ``references``): ``{ "document": { "id", "documentType", ... } }``
    2. Plano (``GET /documents.json?relateddetailid=``): ``{ "id", "documentType", "number", ... }``
       — el documento relacionado va en la raíz, no bajo ``document``.
    """
    doc = item.get("document")

    if isinstance(doc, dict) and doc.get("id"):
        related_id = _safe_int(doc.get("id"))
        rt_raw = (
            doc.get("documentType")
            or doc.get("document_type")
            or doc.get("document_type_id")
        )
        office_blob = dict(doc)
    else:
        related_id = _safe_int(
            item.get("id") or item.get("documentId") or item.get("document_id"),
        )
        rt_raw = (
            item.get("documentType")
            or item.get("document_type")
            or item.get("document_type_id")
        )
        office_blob = dict(item)
        if related_id is not None and office_blob.get("id") is None:
            office_blob["id"] = related_id

    related_type = _coerce_document_type_id(rt_raw)

    if related_id is None:
        return None, None, {}, "sin related_id (document.id o id/documentId en raíz)"

    return related_id, related_type, office_blob, None


def _office_allows_relation(cur, related_document_id: int, blob: dict[str, Any]) -> tuple[bool, str]:
    """
    No descartar relaciones válidas si la API omite ``office`` o viene inconsistente
    pero ``distribuidora.documents`` ya tiene la boleta/factura en nuestra sucursal.
    """
    api_off = _office_id_from_blob(blob)
    db_off = _document_office_in_db(cur, related_document_id)

    if db_off == OFFICE_ID:
        if api_off is not None and api_off != OFFICE_ID:
            return (
                True,
                f"aceptado por BD: documento {related_document_id} office_id={db_off} "
                f"(API reportaba office_id={api_off})",
            )
        if api_off is None:
            return True, f"aceptado por BD: documento {related_document_id} office_id={db_off} (API sin office)"
        return True, f"office API={api_off} alineado con BD"

    if api_off == OFFICE_ID:
        if db_off is not None and db_off != OFFICE_ID:
            return (
                False,
                f"rechazado: API office={api_off} pero en BD documento {related_document_id} "
                f"tiene office_id={db_off}",
            )
        return True, f"office API={api_off} (documento aún no en BD o coherente)"

    if api_off is not None and api_off != OFFICE_ID:
        if db_off == OFFICE_ID:
            return (
                True,
                f"aceptado por BD pese a API: related_document_id={related_document_id} "
                f"API office={api_off} → BD office_id={db_off}",
            )
        return (
            False,
            f"rechazado: API office_id={api_off} y BD office_id={db_off} (esperado {OFFICE_ID})",
        )

    if api_off is None and db_off is None:
        return (
            False,
            f"rechazado: sin office en payload y documento {related_document_id} no está en BD local",
        )

    return (
        False,
        f"rechazado: sin office claro en API; BD documento {related_document_id} office_id={db_off}",
    )


def _insert_related_triples(
    conn: PgConnection,
    cur,
    triples: list[tuple[int, int, int]],
    *,
    stats: dict[str, Any] | None = None,
    log_ctx: str = "",
) -> int:
    """Inserta relaciones con ``ON CONFLICT DO NOTHING``; un commit por fila + reintento deadlock."""
    attempted = 0
    inserted_new = 0
    conflicts = 0
    for detail_id, rid, tid in triples:
        attempted += 1

        def _insert_one(
            _did: int = detail_id,
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
                (_did, _rid, _tid),
            )
            rc = int(cur.rowcount or 0)
            conn.commit()
            return rc

        ins = _with_deadlock_retry(
            conn,
            f"document_related detail_id={detail_id} related_document_id={rid}",
            _insert_one,
        )
        if ins > 0:
            inserted_new += ins
        else:
            conflicts += 1

    logger.info(
        "%s INSERT resumen intentos=%s insertadas=%s conflictos_duplicado=%s",
        log_ctx,
        attempted,
        inserted_new,
        conflicts,
    )
    if stats is not None:
        stats["related_insert_attempts"] = int(stats.get("related_insert_attempts") or 0) + attempted
        stats["related_insert_conflicts"] = int(stats.get("related_insert_conflicts") or 0) + conflicts
    return inserted_new


def _fetch_all_items_for_relateddetailid(
    client: BsaleClient,
    detail_id: int,
    *,
    throttle: float,
    log_ctx: str,
) -> tuple[list[dict[str, Any]], int]:
    """Todas las páginas de ``/documents.json?relateddetailid=`` para un ``detail_id``."""
    merged: list[dict[str, Any]] = []
    api_calls = 0
    offset = 0
    while True:
        try:
            data = client.get(
                "/documents.json",
                merge_bsale_office_query(
                    {
                        "relateddetailid": detail_id,
                        "limit": RELATED_DETAIL_PAGE_LIMIT,
                        "offset": offset,
                    },
                    OFFICE_ID,
                    context="relateddetailid_pages",
                ),
            )
        except Exception as e:
            logger.warning("%s relateddetailid=%s offset=%s: %s", log_ctx, detail_id, offset, e)
            break
        api_calls += 1
        items = data.get("items") or []
        if not items:
            break
        for it in items:
            if isinstance(it, dict):
                merged.append(it)
        if len(items) < RELATED_DETAIL_PAGE_LIMIT:
            break
        offset += len(items)
        if throttle > 0:
            time.sleep(throttle)
    return merged, api_calls


def _collect_terminal_triples_from_related_oc(
    client: BsaleClient,
    cur,
    *,
    root_detail_id: int,
    root_oc_document_id: int,
    related_oc_document_id: int,
    depth: int,
    max_depth: int,
    visited: set[int],
    throttle: float,
    stats: dict[str, Any] | None,
    log_ctx: str,
) -> tuple[list[tuple[int, int, int]], int]:
    """
    Desde una OC intermedia (33), recorre ``details.json`` + ``relateddetailid`` y devuelve
    triples ``(root_detail_id, terminal_document_id, tipo 1|6|9)`` únicamente para terminales.
    No persiste vínculos 33→33.
    """
    api_calls = 0
    if related_oc_document_id in visited:
        if stats is not None:
            stats["related_type33_loops"] = int(stats.get("related_type33_loops") or 0) + 1
        logger.warning(
            "%s [RELATED][TYPE33_RESOLUTION] loops_detected=1 root_oc=%s current_depth=%s related_oc=%s",
            log_ctx,
            root_oc_document_id,
            depth,
            related_oc_document_id,
        )
        return [], api_calls

    loc = set(visited)
    loc.add(related_oc_document_id)
    if stats is not None:
        stats["related_type33_resolutions"] = int(stats.get("related_type33_resolutions") or 0) + 1

    logger.info(
        "%s [RELATED][TYPE33_RESOLUTION] root_oc=%s root_detail=%s current_depth=%s related_oc=%s max_depth=%s",
        log_ctx,
        root_oc_document_id,
        root_detail_id,
        depth,
        related_oc_document_id,
        max_depth,
    )

    detail_ids, c_det = _fetch_detail_ids_from_bsale_details(
        client,
        related_oc_document_id,
        throttle=throttle,
        log_ctx=log_ctx,
    )
    api_calls += c_det

    out: list[tuple[int, int, int]] = []
    branches_seen = 0
    terminals_here = 0

    for child_detail_id in detail_ids:
        items, c_rel = _fetch_all_items_for_relateddetailid(
            client,
            child_detail_id,
            throttle=throttle,
            log_ctx=log_ctx,
        )
        api_calls += c_rel
        for it in items:
            if not isinstance(it, dict):
                continue
            rid, tid, office_blob, parse_motivo = _parse_related_document_blob(it)
            if parse_motivo is not None or rid is None:
                continue
            if rid in (root_oc_document_id, related_oc_document_id):
                continue
            if tid is None:
                continue
            if tid in RELATED_DOCUMENT_TYPES_ALLOWED:
                allow_office, office_motivo = _office_allows_relation(cur, rid, office_blob)
                if not allow_office:
                    if stats is not None:
                        stats["related_skipped_other_office"] = (
                            int(stats.get("related_skipped_other_office") or 0) + 1
                        )
                    continue
                out.append((root_detail_id, rid, tid))
                terminals_here += 1
                if stats is not None:
                    stats["related_type33_terminal_found"] = (
                        int(stats.get("related_type33_terminal_found") or 0) + 1
                    )
                    if depth == 0:
                        stats["related_type33_depth1_hits"] = (
                            int(stats.get("related_type33_depth1_hits") or 0) + 1
                        )
                logger.info(
                    "%s [RELATED][TYPE33_RESOLUTION] terminal_found=1 root_oc=%s root_detail=%s "
                    "related_oc=%s current_depth=%s terminal_doc=%s terminal_type=%s (%s)",
                    log_ctx,
                    root_oc_document_id,
                    root_detail_id,
                    related_oc_document_id,
                    depth,
                    rid,
                    tid,
                    office_motivo,
                )
                continue

            if tid != DOC_TYPE_OC:
                continue

            branches_seen += 1
            if stats is not None:
                stats["related_type33_branches"] = int(stats.get("related_type33_branches") or 0) + 1

            if rid in loc:
                if stats is not None:
                    stats["related_type33_loops"] = int(stats.get("related_type33_loops") or 0) + 1
                logger.warning(
                    "%s [RELATED][TYPE33_RESOLUTION] loops_detected=1 root_oc=%s related_oc=%s nested_oc=%s",
                    log_ctx,
                    root_oc_document_id,
                    related_oc_document_id,
                    rid,
                )
                continue

            if depth + 1 >= max_depth:
                logger.info(
                    "%s [RELATED][TYPE33_RESOLUTION] max_depth alcanzado root_oc=%s depth=%s nested_oc=%s",
                    log_ctx,
                    root_oc_document_id,
                    depth,
                    rid,
                )
                continue

            allow_office, office_motivo = _office_allows_relation(cur, rid, office_blob)
            if not allow_office:
                if stats is not None:
                    stats["related_skipped_other_office"] = (
                        int(stats.get("related_skipped_other_office") or 0) + 1
                    )
                continue

            sub, ac_sub = _collect_terminal_triples_from_related_oc(
                client,
                cur,
                root_detail_id=root_detail_id,
                root_oc_document_id=root_oc_document_id,
                related_oc_document_id=rid,
                depth=depth + 1,
                max_depth=max_depth,
                visited=loc,
                throttle=throttle,
                stats=stats,
                log_ctx=log_ctx,
            )
            api_calls += ac_sub
            out.extend(sub)

    logger.info(
        "%s [RELATED][TYPE33_RESOLUTION] branches_resolved summary related_oc=%s depth=%s "
        "child_details=%s branches_seen=%s terminals_here=%s triples_out=%s",
        log_ctx,
        related_oc_document_id,
        depth,
        len(detail_ids),
        branches_seen,
        terminals_here,
        len(out),
    )
    return out, api_calls


def _documents_json_items_to_triples(
    client: BsaleClient,
    cur,
    detail_id: int,
    items: list[Any],
    *,
    oc_document_id: int,
    throttle: float,
    max_type33_depth: int,
    stats: dict[str, Any] | None,
    log_ctx: str,
) -> tuple[list[tuple[int, int, int]], int]:
    """
    Parsea cada ítem de ``/documents.json?relateddetailid=``.

    - Tipos **1 / 6 / 9**: triple directo ``(detail_id raíz OC, related_id, tipo)``.
    - Tipo **33** (OC mutada): expande ``details.json`` + ``relateddetailid`` hasta ``max_type33_depth``
      y solo añade triples hacia terminales 1/6/9 (``detail_id`` sigue siendo el de la OC original).
    """
    out: list[tuple[int, int, int]] = []
    api_extra = 0
    logger.info("%s[detail %s] related items API=%s", log_ctx, detail_id, len(items))
    for it in items:
        if not isinstance(it, dict):
            logger.warning("%s[detail %s] ítem no dict: %r", log_ctx, detail_id, it)
            continue

        rid, tid, office_blob, parse_motivo = _parse_related_document_blob(it)
        if rid == oc_document_id:
            logger.debug(
                "%s[detail %s] se omite ítem: related_document_id=%s es la propia OC",
                log_ctx,
                detail_id,
                rid,
            )
            continue
        if parse_motivo is not None or rid is None:
            logger.warning(
                "%s[detail %s] relación descartada (parser): %s",
                log_ctx,
                detail_id,
                parse_motivo or "sin related_id",
            )
            continue

        if tid is None:
            logger.warning(
                "%s[detail %s] relación descartada: sin tipo para related_document_id=%s",
                log_ctx,
                detail_id,
                rid,
            )
            continue

        if tid in RELATED_DOCUMENT_TYPES_ALLOWED:
            allow_office, office_motivo = _office_allows_relation(cur, rid, office_blob)
            if not allow_office:
                if stats is not None:
                    stats["related_skipped_other_office"] = (
                        int(stats.get("related_skipped_other_office") or 0) + 1
                    )
                logger.warning(
                    "%s[detail %s] relación descartada por office: related_document_id=%s → %s",
                    log_ctx,
                    detail_id,
                    rid,
                    office_motivo,
                )
                continue
            if allow_office and (
                "aceptado por BD" in office_motivo
                or "pese a API" in office_motivo
                or "API reportaba" in office_motivo
            ):
                logger.info(
                    "%s[detail %s] office eval related_document_id=%s → %s",
                    log_ctx,
                    detail_id,
                    rid,
                    office_motivo,
                )

            logger.info(
                "%s[detail %s] parsed relation (relateddetailid) → doc_id=%s type=%s",
                log_ctx,
                detail_id,
                rid,
                tid,
            )
            out.append((detail_id, rid, tid))
            continue

        if tid == DOC_TYPE_OC:
            if max_type33_depth <= 0:
                logger.info(
                    "%s [RELATED][TYPE33_RESOLUTION] omitido RELATED_MAX_TYPE33_DEPTH=0 detail=%s related_oc=%s",
                    log_ctx,
                    detail_id,
                    rid,
                )
                continue
            allow_office, office_motivo = _office_allows_relation(cur, rid, office_blob)
            if not allow_office:
                if stats is not None:
                    stats["related_skipped_other_office"] = (
                        int(stats.get("related_skipped_other_office") or 0) + 1
                    )
                logger.warning(
                    "%s[detail %s] OC intermedia descartada por office: related_document_id=%s → %s",
                    log_ctx,
                    detail_id,
                    rid,
                    office_motivo,
                )
                continue
            visited0: set[int] = {oc_document_id}
            ext, ac = _collect_terminal_triples_from_related_oc(
                client,
                cur,
                root_detail_id=detail_id,
                root_oc_document_id=oc_document_id,
                related_oc_document_id=rid,
                depth=0,
                max_depth=max_type33_depth,
                visited=visited0,
                throttle=throttle,
                stats=stats,
                log_ctx=log_ctx,
            )
            api_extra += ac
            out.extend(ext)
            continue

        logger.warning(
            "%s[detail %s] relación descartada: tipo no terminal ni OC=%s",
            log_ctx,
            detail_id,
            tid,
        )
    return out, api_extra


def _fetch_detail_ids_from_bsale_details(
    client: BsaleClient,
    document_id: int,
    *,
    throttle: float,
    log_ctx: str = "",
    bsale_source_document_id: int | None = None,
) -> tuple[list[int], int]:
    """
    ``GET /documents/{bsale_source}/details.json`` paginado.

    ``document_id`` es la clave local (log); la URL usa ``bsale_source_document_id``
    cuando se indica (p. ej. tras reemisión por folio).

    Retorna ``(detail_ids, llamadas_http)``.
    """
    source_id = (
        int(bsale_source_document_id)
        if bsale_source_document_id is not None
        else int(document_id)
    )
    ids: list[int] = []
    api_calls = 0
    offset = 0
    while True:
        try:
            data = client.get(
                f"/documents/{source_id}/details.json",
                {"limit": DETAILS_PAGE_LIMIT, "offset": offset},
            )
        except Exception as e:
            logger.warning(
                "%s details.json local_document_id=%s bsale_source_document_id=%s "
                "offset=%s: %s",
                log_ctx,
                document_id,
                source_id,
                offset,
                e,
            )
            break
        api_calls += 1
        items = data.get("items") or []
        if not isinstance(items, list):
            break
        for it in items:
            if isinstance(it, dict):
                did = _safe_int(it.get("id"))
                if did is not None:
                    ids.append(did)
        if len(items) < DETAILS_PAGE_LIMIT:
            break
        offset += len(items)
        if throttle > 0:
            time.sleep(throttle)
    # Orden estable y sin duplicados (paginación / API)
    ids = list(dict.fromkeys(ids))
    logger.info(
        "%s details.json local=%s bsale_source=%s detail_ids usados=%s (n=%s)",
        log_ctx,
        document_id,
        source_id,
        ids,
        len(ids),
    )
    return ids, api_calls


def _fetch_and_persist_relateddetailid_for_detail(
    client: BsaleClient,
    conn: PgConnection,
    cur,
    detail_id: int,
    *,
    oc_document_id: int,
    throttle: float,
    stats: dict[str, Any] | None,
    log_ctx: str,
) -> tuple[int, int, int]:
    """
    GET ``/documents.json?relateddetailid=`` paginado; inserta en ``document_related``.

    Retorna ``(items_api_total, filas_insertadas, llamadas_http)``.
    """
    items_api_total = 0
    rows_inserted = 0
    api_calls = 0
    max_type33_depth = _related_max_type33_depth()
    offset = 0
    while True:
        try:
            data = client.get(
                "/documents.json",
                merge_bsale_office_query(
                    {
                        "relateddetailid": detail_id,
                        "limit": RELATED_DETAIL_PAGE_LIMIT,
                        "offset": offset,
                    },
                    OFFICE_ID,
                    context="relateddetailid_triples",
                ),
            )
        except Exception as e:
            logger.warning("%s relateddetailid=%s offset=%s: %s", log_ctx, detail_id, offset, e)
            break
        api_calls += 1
        items = data.get("items") or []
        if not items:
            break
        items_api_total += len(items)
        triples, tc = _documents_json_items_to_triples(
            client,
            cur,
            detail_id,
            items,
            oc_document_id=oc_document_id,
            throttle=throttle,
            max_type33_depth=max_type33_depth,
            stats=stats,
            log_ctx=log_ctx,
        )
        api_calls += tc
        rows_inserted += _insert_related_triples(conn, cur, triples, stats=stats, log_ctx=log_ctx)
        offset += len(items)
        if throttle > 0:
            time.sleep(throttle)
    logger.info(
        "%s relateddetail resumen detail_id=%s items=%s insertadas=%s llamadas=%s",
        log_ctx,
        detail_id,
        items_api_total,
        rows_inserted,
        api_calls,
    )
    return items_api_total, rows_inserted, api_calls


def _sync_related_by_detail_for_oc_document(
    client: BsaleClient,
    conn: PgConnection,
    cur,
    document_id: int,
    *,
    throttle: float,
    stats: dict[str, Any] | None,
    log_ctx: str,
) -> tuple[int, int, int, int]:
    """
    ``details.json`` + ``relateddetailid`` por línea.

    Retorna ``(detail_ids_procesados, items_related_total, filas_insertadas, llamadas_http)``.
    """
    source_id, folio = _bsale_source_id_from_pg(cur, document_id)
    logger.info(
        "%s related OC folio=%s local_document_id=%s bsale_source_document_id=%s ids_differ=%s",
        log_ctx,
        folio,
        document_id,
        source_id,
        ids_differ(document_id, source_id),
    )
    detail_ids, calls_details = _fetch_detail_ids_from_bsale_details(
        client,
        document_id,
        throttle=throttle,
        log_ctx=log_ctx,
        bsale_source_document_id=source_id,
    )
    logger.info("%s details encontrados=%s detail_ids=%s", log_ctx, len(detail_ids), detail_ids)
    if not detail_ids:
        fallback = _detail_ids_for_document(cur, document_id)
        if fallback:
            logger.info(
                "%s details.json sin líneas; fallback BD detail_ids=%s",
                log_ctx,
                fallback,
            )
            detail_ids = fallback
        else:
            logger.warning("%s sin detail_ids (API ni BD) document_id=%s", log_ctx, document_id)

    still_missing_fk, heal_calls = _self_heal_document_details_if_needed(
        client,
        conn,
        cur,
        document_id,
        detail_ids,
        throttle=throttle,
        log_ctx=log_ctx,
        stats=stats,
    )
    calls_details += heal_calls
    still_missing_set = frozenset(still_missing_fk)
    if still_missing_set:
        logger.warning(
            "%s relateddetailid omitido para detail_ids sin fila en document_details "
            "(tras self-heal) document_id=%s detail_ids=%s",
            log_ctx,
            document_id,
            sorted(still_missing_set),
        )

    items_total = 0
    rows_ins = 0
    calls_rel = 0
    details_processed = 0
    for did in detail_ids:
        if did in still_missing_set:
            continue
        details_processed += 1
        if stats is not None:
            stats["relateddetail_details_processed"] = (
                int(stats.get("relateddetail_details_processed") or 0) + 1
            )
        it_tot, ins, c = _fetch_and_persist_relateddetailid_for_detail(
            client,
            conn,
            cur,
            did,
            oc_document_id=document_id,
            throttle=throttle,
            stats=stats,
            log_ctx=log_ctx,
        )
        items_total += it_tot
        rows_ins += ins
        calls_rel += c

    if stats is not None:
        stats["relateddetail_items_total"] = int(stats.get("relateddetail_items_total") or 0) + items_total

    calls_total = calls_details + calls_rel
    logger.info(
        "%s relateddetail flujo: document_id=%s details_api=%s details_procesados=%s "
        "omitidos_sin_fila=%s items=%s insertadas=%s api=%s",
        log_ctx,
        document_id,
        len(detail_ids),
        details_processed,
        len(still_missing_set),
        items_total,
        rows_ins,
        calls_total,
    )
    return details_processed, items_total, rows_ins, calls_total


def _fetch_and_persist_related_for_document(
    client: BsaleClient,
    conn: PgConnection,
    cur,
    document_id: int,
    *,
    throttle: float,
    stats: dict[str, Any] | None = None,
) -> tuple[int, int, int]:
    """
    ``details.json`` + ``documents.json?relateddetailid=`` por línea; persiste en ``document_related``.

    Retorna ``(items_api_total_relateddetail, filas_insertadas, llamadas_http)``.
    """
    oc_number = _oc_number_for_document(cur, document_id)
    log_ctx = _related_log_ctx(oc_number, document_id)
    detail_ids_bd = _detail_ids_for_document(cur, document_id)
    logger.info(
        "%s document_id=%s número_OC=%s detail_ids_en_BD=%s",
        log_ctx,
        document_id,
        oc_number,
        detail_ids_bd,
    )

    ndet, it_rel, ins_det, calls_det = _sync_related_by_detail_for_oc_document(
        client,
        conn,
        cur,
        document_id,
        throttle=throttle,
        stats=stats,
        log_ctx=log_ctx,
    )

    logger.info(
        "%s resumen related (solo relateddetailid): document_id=%s items_metric=%s "
        "filas_insertadas=%s details_procesados=%s",
        log_ctx,
        document_id,
        it_rel,
        ins_det,
        ndet,
    )

    return it_rel, ins_det, calls_det


def sync_related_for_single_oc(
    client: BsaleClient,
    *,
    document_id: int,
    throttle: float = 0.0,
) -> dict[str, Any]:
    """Sincroniza ``document_related`` para una sola OC (read/write controlado)."""
    conn = get_connection()
    stats: dict[str, Any] = {
        "document_id": int(document_id),
        "rows_inserted": 0,
        "items_api": 0,
        "http_calls": 0,
    }
    try:
        cur = conn.cursor()
        try:
            items, inserted, calls = _fetch_and_persist_related_for_document(
                client,
                conn,
                cur,
                int(document_id),
                throttle=float(throttle),
                stats=stats,
            )
            conn.commit()
            stats["items_api"] = int(items)
            stats["rows_inserted"] = int(inserted)
            stats["http_calls"] = int(calls)
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
    finally:
        conn.close()
    return stats


def _live_related_stats_template() -> dict[str, Any]:
    return {
        "live_mode": False,
        "discovery_mode": DISCOVERY_MODE_FULL,
        "max_runtime_sec": None,
        "ocs_pending_processed": 0,
        "ocs_refresh_processed": 0,
        "ocs_confirmed": 0,
        "ocs_skipped_already_confirmed": 0,
        "ocs_no_relation": 0,
        "api_calls_saved_by_early_exit": 0,
        "details_considered": 0,
        "details_queried": 0,
        "details_skipped_after_confirmation": 0,
        "fast_confirmations": 0,
        "full_edges_inserted": 0,
        "stop_reason": STOP_REASON_COMPLETED,
        "skipped_already_running": False,
    }


def _process_one_oc_related_sync(
    *,
    conn: PgConnection,
    cur,
    client,
    doc_id: int,
    stats: dict[str, Any],
    discovery_mode: str,
    helper_throttle: float,
    dry_run: bool = False,
) -> dict[str, Any] | None:
    """Descubre y opcionalmente persiste relaciones para una OC. Retorna oc_res o None si skip local."""
    if discovery_mode == DISCOVERY_MODE_FAST_CONFIRM:
        _pairs, confirmed_keys = load_existing_invoice_relations_for_oc(cur, doc_id)
        if confirmed_keys:
            stats["ocs_skipped_already_confirmed"] = int(
                stats.get("ocs_skipped_already_confirmed") or 0
            ) + 1
            stats["existing"] = int(stats.get("existing") or 0) + 1
            return None

    try:
        oc_res = discover_invoice_edges_for_oc(
            client,
            cur,
            doc_id,
            office_id=OFFICE_ID,
            throttle=helper_throttle,
            stats=stats,
            discovery_mode=discovery_mode,
        )
    except (CatchupApiError, OcRelatedApiError) as e:
        logger.warning("sync_related document_id=%s api: %s", doc_id, e)
        if e.rate_limited:
            stats["rate_limited"] = int(stats.get("rate_limited") or 0) + 1
        else:
            stats["api_errors"] = int(stats.get("api_errors") or 0) + 1
        stats["document_errors"] = int(stats.get("document_errors") or 0) + 1
        return None
    except Exception as e:
        logger.warning("sync_related document_id=%s: %s", doc_id, e)
        stats["document_errors"] = int(stats.get("document_errors") or 0) + 1
        stats["api_errors"] = int(stats.get("api_errors") or 0) + 1
        return None

    bucket = classify_oc_discovery_result(oc_res)
    if bucket == "existing":
        stats["existing"] = int(stats.get("existing") or 0) + 1
    elif bucket == "discovered":
        stats["discovered"] = int(stats.get("discovered") or 0) + 1
        stats["ocs_confirmed"] = int(stats.get("ocs_confirmed") or 0) + 1
        if oc_res.get("fast_confirmed"):
            stats["fast_confirmations"] = int(stats.get("fast_confirmations") or 0) + 1
    elif bucket == "no_relation":
        stats["no_relation"] = int(stats.get("no_relation") or 0) + 1
        stats["ocs_no_relation"] = int(stats.get("ocs_no_relation") or 0) + 1
    elif bucket == "rate_limited":
        stats["rate_limited"] = int(stats.get("rate_limited") or 0) + 1
    elif bucket == "api_error":
        stats["api_errors"] = int(stats.get("api_errors") or 0) + 1

    stats["api_calls"] = int(stats.get("api_calls") or 0) + int(oc_res.get("api_calls") or 0)
    stats["api_calls_saved_by_early_exit"] = int(
        stats.get("api_calls_saved_by_early_exit") or 0
    ) + int(oc_res.get("api_calls_saved_by_early_exit") or 0)
    stats["details_queried"] = int(stats.get("details_queried") or 0) + int(
        oc_res.get("details_queried") or 0
    )
    stats["details_skipped_after_confirmation"] = int(
        stats.get("details_skipped_after_confirmation") or 0
    ) + int(oc_res.get("details_skipped_after_confirmation") or 0)
    stats["relateddetail_details_processed"] = int(
        stats.get("relateddetail_details_processed") or 0
    ) + len(oc_res.get("detail_ids_consulted") or [])
    stats["relateddetail_items_total"] = int(
        stats.get("relateddetail_items_total") or 0
    ) + len(oc_res.get("edges") or [])
    stats["details_considered"] = int(stats.get("relateddetail_details_processed") or 0)

    try:
        ins = apply_discovered_invoice_edges(conn, cur, oc_res, stats=stats)
    except Exception as e:
        logger.warning("sync_related apply document_id=%s: %s", doc_id, e)
        stats["document_errors"] = int(stats.get("document_errors") or 0) + 1
        return oc_res

    stats["rows_inserted"] += ins
    if discovery_mode == DISCOVERY_MODE_FULL:
        stats["full_edges_inserted"] = int(stats.get("full_edges_inserted") or 0) + ins
    logger.debug(
        "related doc_id=%s status=%s edges=%s inserted=%s mode=%s",
        doc_id,
        oc_res.get("status"),
        len(oc_res.get("edges") or []),
        ins,
        discovery_mode,
    )
    return oc_res


def sync_distribuidora_related_documents(
    *,
    strict_token: bool = False,
    lookback_days: int | None = None,
    limit_details: int | None = None,
    limit_documents: int | None = None,
    live_mode: bool = False,
    max_runtime_sec: int | None = None,
) -> dict[str, Any]:
    """
    Por cada OC reciente: ``details.json`` y ``documents.json?relateddetailid=`` por ``detail.id``.

    **Live** (``live_mode=True``): modo ``fast_confirm`` — corta al primer tipo 1/6;
    prioriza bucket *pending* y luego *refresh* si queda presupuesto; respeta
    ``RELATED_SYNC_MAX_RUNTIME_SEC`` (default 240 s). Catchup manual no usa este límite.

    **Concurrencia**: ``pg_try_advisory_lock(ADVISORY_LOCK_RELATED)``. Si el cron anterior
    sigue activo, retorna ``stop_reason=SKIPPED_ALREADY_RUNNING`` sin procesar.

    Env:
      RELATED_SYNC_LOOKBACK_DAYS (canónico, default 30)
      LIVE_SYNC_RELATED_WINDOW_DAYS / DISTRIBUIDORA_RELATED_LOOKBACK_DAYS (legacy aliases)
      DISTRIBUIDORA_RELATED_DETAIL_LIMIT (default 250) — cupo bucket refresh por ``document_id``
      DISTRIBUIDORA_RELATED_PENDING_LIMIT (default 400) — cupo bucket OC sin factura confirmada
      RELATED_SYNC_RECENT_PENDING_LIMIT (default 100) — cupo pending recientes por ciclo
      RELATED_SYNC_MAX_RUNTIME_SEC (default 240, solo live)
    """
    token = _bsale_token()
    if not token:
        if strict_token:
            raise ValueError("Ningún token Bsale: defina BSALE_TOKEN o BSALE_TOKEN_SPA.")
        return {"skipped": True, "skip_reason": "sin token", "inserted": 0}

    lb = resolve_related_sync_lookback_days(lookback_days)
    lim_src = limit_documents if limit_documents is not None else limit_details
    lim = (
        lim_src
        if lim_src is not None
        else int(os.getenv("DISTRIBUIDORA_RELATED_DETAIL_LIMIT", str(DEFAULT_RELATED_DETAIL_LIMIT)))
    )

    t0 = time.perf_counter()
    runtime_limit = (
        max_runtime_sec
        if max_runtime_sec is not None
        else resolve_related_sync_max_runtime_sec(live_mode)
    )
    discovery_mode = DISCOVERY_MODE_FAST_CONFIRM if live_mode else DISCOVERY_MODE_FULL

    stats: dict[str, Any] = {
        "documents_considered": 0,
        "details_considered": 0,
        "references_items_total": 0,
        "relateddetail_details_processed": 0,
        "relateddetail_items_total": 0,
        "rows_inserted": 0,
        "api_calls": 0,
        "related_skipped_other_office": 0,
        "document_errors": 0,
        "duration_seconds": 0.0,
        "skipped": False,
        "omitido_concurrencia": False,
        "errors": None,
        "details_self_heal_missing_detected": 0,
        "details_self_heal_refreshes": 0,
        "details_self_heal_rows_written": 0,
        "details_self_heal_retries": 0,
        "details_self_heal_refresh_failures": 0,
        "details_self_heal_still_missing": 0,
        "related_type33_resolutions": 0,
        "related_type33_terminal_found": 0,
        "related_type33_branches": 0,
        "related_type33_loops": 0,
        "related_type33_depth1_hits": 0,
        "lookback_days": 0,
        "candidate_window_days": 0,
        "candidate_offset": 0,
        "pending_total_in_window": 0,
        "scanned": 0,
        "existing": 0,
        "discovered": 0,
        "no_relation": 0,
        "rate_limited": 0,
        "api_errors": 0,
        "retries": 0,
        "wait_seconds": 0.0,
        "oldest_candidate_date": None,
        "newest_candidate_date": None,
        "documents_pending_without_related": 0,
        "documents_recent_refresh": 0,
        "documents_merged_unique": 0,
        **_live_related_stats_template(),
    }
    stats["live_mode"] = live_mode
    stats["discovery_mode"] = discovery_mode
    stats["max_runtime_sec"] = runtime_limit

    conn = get_connection()
    got_lock = False
    try:
        cur = conn.cursor()
        cur.execute("SELECT pg_try_advisory_lock(%s)", (ADVISORY_LOCK_RELATED,))
        got_lock = bool(cur.fetchone()[0])
        if not got_lock:
            stats["omitido_concurrencia"] = True
            stats["skipped_already_running"] = True
            stats["stop_reason"] = STOP_REASON_SKIPPED_ALREADY_RUNNING
            cur.close()
            stats["duration_seconds"] = round(time.perf_counter() - t0, 3)
            return stats

        conn.commit()

        logger.info(
            "sync related distribuidora: office_id=%s; por OC: details.json + "
            "relateddetailid + resolución OC 33 (company_id=%s office_id=%s)",
            OFFICE_ID,
            COMPANY_ID,
            OFFICE_ID,
        )

        document_ids, pick_meta = _fetch_oc_document_ids_for_incremental(
            cur, lookback_days=lb, limit_documents=lim
        )
        stats["lookback_days"] = max(1, lb)
        stats["candidate_window_days"] = stats["lookback_days"]
        stats["documents_pending_without_related"] = pick_meta["pending_without_related"]
        stats["pending_total_in_window"] = pick_meta.get("pending_total_in_window", 0)
        stats["candidate_offset"] = pick_meta.get("pending_offset", 0)
        stats["documents_recent_refresh"] = pick_meta["recent_refresh_candidates"]
        stats["documents_merged_unique"] = pick_meta["merged_unique"]
        stats["documents_considered"] = len(document_ids)
        stats["scanned"] = len(document_ids)
        oldest, newest = emission_date_bounds_for_document_ids(cur, document_ids)
        stats["oldest_candidate_date"] = oldest
        stats["newest_candidate_date"] = newest
        conn.commit()
        logger.info(
            "sync related selección OC: lookback=%sd pending_sin_factura=%s "
            "pending_total=%s offset=%s refresh_recientes=%s total_unicos=%s "
            "(pending_limit=%s refresh_limit=%s)",
            stats["lookback_days"],
            stats["documents_pending_without_related"],
            stats["pending_total_in_window"],
            stats["candidate_offset"],
            stats["documents_recent_refresh"],
            stats["documents_merged_unique"],
            _related_pending_limit(),
            lim,
        )

        rate_stats: dict[str, Any] = {
            "requests_total": 0,
            "rate_limit_events": 0,
            "retry_count": 0,
            "wait_seconds_total": 0.0,
        }
        client = create_bsale_client_for_related_discovery(token, rate_stats=rate_stats)
        helper_throttle = float(os.getenv("DISTRIBUIDORA_RELATED_API_DELAY_SEC", "0"))

        pending_ids = pick_meta.get("pending_ids") or []
        refresh_ids = pick_meta.get("refresh_ids") or []
        phases: list[tuple[str, list[int]]] = [
            ("pending", pending_ids),
            ("refresh", refresh_ids if live_mode else []),
        ]
        if not live_mode:
            phases = [("all", document_ids)]

        def _runtime_exhausted() -> bool:
            if runtime_limit is None:
                return False
            return (time.perf_counter() - t0) >= float(runtime_limit)

        for phase_name, phase_ids in phases:
            if phase_name == "refresh" and _runtime_exhausted():
                logger.info(
                    "sync related live: omitiendo bucket refresh (presupuesto %.0fs agotado)",
                    float(runtime_limit or 0),
                )
                break
            for doc_id in phase_ids:
                if _runtime_exhausted():
                    stats["stop_reason"] = STOP_REASON_RUNTIME_BUDGET
                    logger.info(
                        "sync related live: stop_reason=%s tras %s OC (presupuesto %ss)",
                        STOP_REASON_RUNTIME_BUDGET,
                        stats.get("ocs_pending_processed", 0)
                        + stats.get("ocs_refresh_processed", 0),
                        runtime_limit,
                    )
                    break

                _process_one_oc_related_sync(
                    conn=conn,
                    cur=cur,
                    client=client,
                    doc_id=doc_id,
                    stats=stats,
                    discovery_mode=discovery_mode,
                    helper_throttle=helper_throttle,
                )
                if phase_name == "pending":
                    stats["ocs_pending_processed"] = int(
                        stats.get("ocs_pending_processed") or 0
                    ) + 1
                elif phase_name == "refresh":
                    stats["ocs_refresh_processed"] = int(
                        stats.get("ocs_refresh_processed") or 0
                    ) + 1

            if stats.get("stop_reason") == STOP_REASON_RUNTIME_BUDGET:
                break

        stats["retries"] = int(rate_stats.get("retry_count") or 0)
        stats["wait_seconds"] = float(rate_stats.get("wait_seconds_total") or 0.0)
        stats["details_considered"] = int(stats.get("relateddetail_details_processed") or 0)

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
            "sync related OK: mode=%s scanned=%s pending=%s refresh=%s discovered=%s "
            "inserted=%s fast_confirm=%s api_saved_early_exit=%s details_queried=%s "
            "details_skipped=%s stop_reason=%s lookback=%sd api=%s s=%.2f",
            stats.get("discovery_mode"),
            stats.get("scanned"),
            stats.get("ocs_pending_processed"),
            stats.get("ocs_refresh_processed"),
            stats.get("discovered"),
            stats["rows_inserted"],
            stats.get("fast_confirmations"),
            stats.get("api_calls_saved_by_early_exit"),
            stats.get("details_queried"),
            stats.get("details_skipped_after_confirmation"),
            stats.get("stop_reason"),
            stats.get("lookback_days"),
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

    Por cada ``document_id`` de esas OC: ``details.json`` / ``relateddetailid`` únicamente.
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
        "documents_processed": 0,
        "details_processed": 0,
        "rows_inserted": 0,
        "api_calls": 0,
        "relations_found": 0,
        "references_items_total": 0,
        "relateddetail_details_processed": 0,
        "relateddetail_items_total": 0,
        "related_skipped_other_office": 0,
        "duration_seconds": 0.0,
        "omitido_concurrencia": False,
        "errors": None,
        "related_insert_conflicts": 0,
        "related_insert_attempts": 0,
        "details_self_heal_missing_detected": 0,
        "details_self_heal_refreshes": 0,
        "details_self_heal_rows_written": 0,
        "details_self_heal_retries": 0,
        "details_self_heal_refresh_failures": 0,
        "details_self_heal_still_missing": 0,
        "related_type33_resolutions": 0,
        "related_type33_terminal_found": 0,
        "related_type33_branches": 0,
        "related_type33_loops": 0,
        "related_type33_depth1_hits": 0,
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

        conn.commit()

        logger.info(
            "sync related rango: office_id=%s; OC company_id=%s office_id=%s",
            OFFICE_ID,
            COMPANY_ID,
            OFFICE_ID,
        )

        client = BsaleClient(token)
        throttle = float(os.getenv("DISTRIBUIDORA_RELATED_API_DELAY_SEC", "0.12"))

        current = start_date
        while current <= end_date:
            logger.info("Procesando related día: %s", current.isoformat())
            document_ids = _fetch_oc_document_ids_for_emission_day(cur, current)
            for doc_id in document_ids:
                logger.info("Documento OC procesado (relateddetailid): %s", doc_id)
                items_total, ins, calls = _fetch_and_persist_related_for_document(
                    client, conn, cur, doc_id, throttle=throttle, stats=stats
                )
                stats["api_calls"] += calls
                stats["documents_processed"] += 1
                stats["details_processed"] = int(stats.get("relateddetail_details_processed") or 0)
                stats["rows_inserted"] += ins
                stats["relations_found"] += items_total
                logger.info(
                    "document_id=%s items_metric=%s filas_insertadas=%s (acum. details_lines=%s)",
                    doc_id,
                    items_total,
                    ins,
                    stats.get("relateddetail_details_processed", 0),
                )

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
        stats["details_processed"] = int(stats.get("relateddetail_details_processed") or 0)
        logger.info(
            "sync related range OK: days=%s documents=%s relateddetail_details=%s "
            "relateddetail_items=%s inserted=%s omitidas otra office=%s s=%.2f",
            stats["days_processed"],
            stats["documents_processed"],
            stats.get("relateddetail_details_processed", 0),
            stats.get("relateddetail_items_total", 0),
            stats["rows_inserted"],
            stats.get("related_skipped_other_office", 0),
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


BACKFILL_RELATED_MAY_2026_START = date(2026, 5, 1)
BACKFILL_RELATED_MAY_2026_END = date(2026, 5, 31)
BACKFILL_RELATED_MAY_LOG_PROCESS = "backfill_related_may_2026"


def backfill_distribuidora_related_may_2026_only(*, strict_token: bool = True) -> dict[str, Any]:
    """
    Backfill oficial ``distribuidora.document_related`` para OC (tipo 33) con emisión UTC
    **2026-05-01 … 2026-05-31** (día a día), solo vía **relateddetailid** (mismo núcleo que
    ``sync_related_documents_range``).

    Registra ``sync_logs``, ``sync_status`` (ya lo hace el rango) y ``sync_state`` operacional
    (``related`` + ``backfill``). Requiere documents + ``document_details`` mayo ya cargados.
    """
    from backend.repositories.distribuidora.sync_repo import (
        finish_sync_log,
        insert_sync_status_row,
        start_sync_log,
    )
    from backend.utils.sync_state import MODE_BACKFILL, update_sync_state_error, update_sync_state_success

    log_id: int | None = None
    conn0 = get_connection()
    try:
        c0 = conn0.cursor()
        log_id = start_sync_log(c0, BACKFILL_RELATED_MAY_LOG_PROCESS)
        conn0.commit()
        c0.close()
    finally:
        conn0.close()

    def _finish_stats(s: dict[str, Any]) -> dict[str, Any]:
        return {
            "documents_processed": int(s.get("documents_processed") or 0),
            "documents_inserted": 0,
            "documents_updated": 0,
            "details_inserted": int(s.get("relateddetail_details_processed") or 0),
            "attributes_inserted": 0,
            "references_inserted": int(s.get("rows_inserted") or 0),
        }

    emission_from = datetime(2026, 5, 1, 0, 0, 0, tzinfo=timezone.utc)
    emission_to_excl = datetime(2026, 6, 1, 0, 0, 0, tzinfo=timezone.utc)

    stats: dict[str, Any]
    try:
        stats = sync_related_documents_range(
            start_date=BACKFILL_RELATED_MAY_2026_START,
            end_date=BACKFILL_RELATED_MAY_2026_END,
            strict_token=strict_token,
        )
    except ValueError as e:
        stats = {
            "mode": "related_range",
            "skipped": True,
            "skip_reason": str(e),
            "rows_inserted": 0,
            "documents_processed": 0,
            "relateddetail_details_processed": 0,
            "relateddetail_items_total": 0,
            "related_insert_conflicts": 0,
            "related_insert_attempts": 0,
            "omitido_concurrencia": False,
        }
    except Exception as e:
        stats = {
            "mode": "related_range",
            "errors": str(e),
            "rows_inserted": 0,
            "documents_processed": 0,
            "relateddetail_details_processed": 0,
            "relateddetail_items_total": 0,
            "related_insert_conflicts": 0,
            "related_insert_attempts": 0,
            "omitido_concurrencia": False,
        }
        conn_e = get_connection()
        try:
            ce = conn_e.cursor()
            if log_id is not None:
                finish_sync_log(
                    ce,
                    log_id,
                    status="error",
                    stats=_finish_stats(stats),
                    message=str(e),
                )
            update_sync_state_error(
                ce,
                sync_type="related",
                mode=MODE_BACKFILL,
                office_id=OFFICE_ID,
                error_summary=str(e),
                status="error",
                items_processed=int(stats.get("rows_inserted") or 0),
            )
            insert_sync_status_row(
                ce,
                sync_type="related",
                records_processed=int(stats.get("rows_inserted") or 0),
                status="error",
            )
            conn_e.commit()
            ce.close()
        finally:
            conn_e.close()
        raise

    conn1 = get_connection()
    try:
        c1 = conn1.cursor()
        fs = _finish_stats(stats)
        if stats.get("skipped"):
            if log_id is not None:
                finish_sync_log(
                    c1,
                    log_id,
                    status="error",
                    stats=fs,
                    message=str(stats.get("skip_reason") or "skipped"),
                )
            insert_sync_status_row(
                c1,
                sync_type="related",
                records_processed=0,
                status="error",
            )
            update_sync_state_error(
                c1,
                sync_type="related",
                mode=MODE_BACKFILL,
                office_id=OFFICE_ID,
                error_summary=str(stats.get("skip_reason") or "skipped"),
                status="error",
                items_processed=0,
            )
            conn1.commit()
            c1.close()
            return stats

        if stats.get("omitido_concurrencia"):
            if log_id is not None:
                finish_sync_log(
                    c1,
                    log_id,
                    status="ok",
                    stats=fs,
                    message="omitido: advisory lock related ocupado",
                )
            insert_sync_status_row(
                c1,
                sync_type="related",
                records_processed=0,
                status="error",
            )
            update_sync_state_error(
                c1,
                sync_type="related",
                mode=MODE_BACKFILL,
                office_id=OFFICE_ID,
                error_summary="advisory lock related ocupado (sin procesar)",
                status="error",
                items_processed=0,
            )
            conn1.commit()
            c1.close()
            return stats

        update_sync_state_success(
            c1,
            sync_type="related",
            mode=MODE_BACKFILL,
            office_id=OFFICE_ID,
            last_window_from=emission_from,
            last_window_to=emission_to_excl - timedelta(seconds=1),
            last_watermark=emission_to_excl - timedelta(seconds=1),
            overlap_days=None,
            overlap_seconds=None,
            items_processed=int(stats.get("rows_inserted") or 0),
            status="success",
        )
        if log_id is not None:
            finish_sync_log(
                c1,
                log_id,
                status="ok",
                stats=fs,
                message="backfill_related_may_2026 OK",
            )
        conn1.commit()
        c1.close()
    finally:
        conn1.close()

    stats["backfill_may_2026"] = True
    return stats


def debug_sync_related_for_document(document_number: int) -> dict[str, Any]:
    """
    Ejecuta **solo** el flujo ``document_related`` para una OC identificada por ``number`` en BD.

    Útil para depurar (p. ej. ``python -m backend.debug.debug_sync_related_oc 66080``).
    Requiere el mismo advisory lock que el sync incremental de relaciones.
    """
    token = _bsale_token()
    if not token:
        return {"ok": False, "error": "sin token Bsale"}
    conn = get_connection()
    cur = conn.cursor()
    got_lock = False
    try:
        cur.execute("SELECT pg_try_advisory_lock(%s)", (ADVISORY_LOCK_RELATED,))
        got_lock = bool(cur.fetchone()[0])
        conn.commit()
        if not got_lock:
            return {
                "ok": False,
                "error": "Lock document_related en uso; detenga otro sync related e intente de nuevo.",
            }
        cur.execute(
            """
            SELECT document_id FROM distribuidora.documents
            WHERE company_id = %s AND office_id = %s AND document_type_id = %s AND number = %s
            LIMIT 1
            """,
            (COMPANY_ID, OFFICE_ID, DOC_TYPE_OC, document_number),
        )
        row = cur.fetchone()
        conn.commit()
        if not row:
            return {
                "ok": False,
                "error": f"OC número {document_number} no encontrada en distribuidora.documents",
            }
        document_id = int(row[0])
        client = BsaleClient(token)
        stats: dict[str, Any] = {}
        items_m, inserted, calls = _fetch_and_persist_related_for_document(
            client, conn, cur, document_id, throttle=0.0, stats=stats
        )
        cur.execute(
            """
            SELECT dr.detail_id, dr.related_document_id, dr.related_document_type
            FROM distribuidora.document_related dr
            INNER JOIN distribuidora.document_details dd ON dd.detail_id = dr.detail_id
            WHERE dd.document_id = %s
            ORDER BY dr.related_document_id
            """,
            (document_id,),
        )
        rel_after = [
            {"detail_id": int(a), "related_document_id": int(b), "related_document_type": int(c)}
            for a, b, c in cur.fetchall()
        ]
        return {
            "ok": True,
            "document_number": document_number,
            "document_id": document_id,
            "items_metric": items_m,
            "rows_inserted_this_run": inserted,
            "api_calls": calls,
            "stats_counters": stats,
            "document_related_rows_for_oc": rel_after,
        }
    finally:
        try:
            if got_lock:
                cur.execute("SELECT pg_advisory_unlock(%s)", (ADVISORY_LOCK_RELATED,))
        except Exception:
            logger.exception("debug_sync_related_for_document unlock")
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


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
