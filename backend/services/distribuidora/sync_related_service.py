"""
Sync incremental de relaciones OC → otros documentos:

1. ``GET /v1/documents/{document_id}/references.json`` (relaciones a nivel documento).
2. ``GET /v1/documents/{document_id}/details.json`` y por cada detalle
   ``GET /v1/documents.json?relateddetailid=`` (relaciones a nivel línea; Bsale no siempre
   expone todo en references).

Escribe ``distribuidora.document_related`` con deduplicación (``ON CONFLICT``) y filtro por office.

Incluye ``sync_related_documents_range`` para rellenar histórico por rango de emisión (día a día).

Depuración por número de OC: ``debug_sync_related_for_document`` o
``python -m backend.jobs.debug_sync_related_oc [número]``.
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
RELATED_DOCUMENT_TYPES_ALLOWED = frozenset({1, 6, 9})
RELATED_DETAIL_PAGE_LIMIT = 50
DETAILS_PAGE_LIMIT = 50

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


def _fetch_oc_document_ids(
    cur,
    *,
    lookback_days: int,
    limit_documents: int,
) -> list[int]:
    cur.execute(
        """
        SELECT DISTINCT d.document_id
        FROM distribuidora.documents d
        WHERE d.document_type_id = %s
          AND d.company_id = %s
          AND d.office_id = %s
          AND d.emission_date >= (NOW() AT TIME ZONE 'UTC' - (%s * interval '1 day'))
        ORDER BY d.document_id DESC
        LIMIT %s
        """,
        (DOC_TYPE_OC, COMPANY_ID, OFFICE_ID, max(1, lookback_days), limit_documents),
    )
    return [int(r[0]) for r in cur.fetchall()]


def _fetch_oc_document_ids_for_emission_day(cur, day: date) -> list[int]:
    """``document_id`` distintos de OC (tipo 33) cuya emisión cae ese día calendario (UTC)."""
    day_start, day_end_excl = _utc_day_emission_bounds(day)
    cur.execute(
        """
        SELECT DISTINCT d.document_id
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


def _related_document_from_reference_item(it: dict[str, Any]) -> dict[str, Any] | None:
    """Blob mínimo para compatibilidad; preferir ``_parse_related_document_blob``."""
    rid, _tid, blob, motivo = _parse_related_document_blob(it)
    if motivo is not None or rid is None:
        return None
    return blob


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


def _detail_id_from_reference_item(it: dict[str, Any]) -> int | None:
    det = it.get("detail") or it.get("Detail")
    if isinstance(det, dict):
        did = _safe_int(det.get("id"))
        if did is not None:
            return did
    return _safe_int(
        it.get("detailId")
        or it.get("detail_id")
        or it.get("relatedDetailId")
        or it.get("related_detail_id")
    )


def _reference_items_to_related_triples(
    cur,
    source_document_id: int,
    items: list[Any],
    valid_detail_ids: set[int],
    *,
    fallback_single_detail_id: int | None,
    stats: dict[str, Any] | None,
    oc_number: int | None,
    log_ctx: str,
) -> list[tuple[int, int, int]]:
    """``(detail_id, related_document_id, related_document_type_id)`` listos para insertar."""
    out: list[tuple[int, int, int]] = []
    if not valid_detail_ids:
        logger.warning(
            "%s references sin líneas locales: document_id=%s (document_details vacío)",
            log_ctx,
            source_document_id,
        )
        return out

    logger.info("%s references ítems API=%s", log_ctx, len(items))
    for it in items:
        if not isinstance(it, dict):
            logger.warning("%s references ítem no dict: %r", log_ctx, it)
            continue

        rid, tid, office_blob, parse_motivo = _parse_related_document_blob(it)
        if parse_motivo is not None or rid is None:
            logger.warning(
                "%s references relación descartada (parser): %s",
                log_ctx,
                parse_motivo or "sin related_id",
            )
            continue

        if tid is None:
            logger.warning(
                "%s references relación descartada: sin tipo para related_document_id=%s",
                log_ctx,
                rid,
            )
            continue
        if tid not in RELATED_DOCUMENT_TYPES_ALLOWED:
            logger.warning("%s references relación descartada: tipo inválido=%s", log_ctx, tid)
            continue

        allow_office, office_motivo = _office_allows_relation(cur, rid, office_blob)
        if not allow_office:
            if stats is not None:
                stats["related_skipped_other_office"] = (
                    int(stats.get("related_skipped_other_office") or 0) + 1
                )
            logger.warning(
                "%s references relación descartada por office: related_document_id=%s → %s",
                log_ctx,
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
                "%s references office eval: related_document_id=%s → %s",
                log_ctx,
                rid,
                office_motivo,
            )

        logger.info(
            "%s references parsed relation → doc_id=%s type=%s",
            log_ctx,
            rid,
            tid,
        )

        detail_id = _detail_id_from_reference_item(it)
        if detail_id is None and fallback_single_detail_id is not None:
            detail_id = fallback_single_detail_id
            logger.info(
                "%s references detail_id ausente en ítem; fallback línea única detail_id=%s",
                log_ctx,
                detail_id,
            )
        if detail_id is None:
            logger.warning(
                "%s references relación descartada: sin detail_id (OC multilínea?) "
                "related_document_id=%s keys=%s",
                log_ctx,
                rid,
                sorted(it.keys()),
            )
            continue
        if detail_id not in valid_detail_ids:
            logger.warning(
                "%s references relación descartada: detail_id=%s no está en document_details "
                "del document_id=%s (válidos=%s) related_document_id=%s",
                log_ctx,
                detail_id,
                source_document_id,
                sorted(valid_detail_ids),
                rid,
            )
            continue
        out.append((detail_id, rid, tid))
    return out


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


def _references_list_from_relateddetail_item(it: dict[str, Any]) -> list[Any]:
    """Lista de referencias embebidas en un ítem de ``relateddetailid`` (lista o ``references.items``)."""
    raw = it.get("references")
    if raw is None:
        return []
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict):
        inner = raw.get("items")
        if isinstance(inner, list):
            return inner
    return []


def _append_triple_from_ref_entry(
    cur,
    detail_id: int,
    ref: dict[str, Any],
    *,
    stats: dict[str, Any] | None,
    log_ctx: str,
    out: list[tuple[int, int, int]],
) -> bool:
    """
    Parsea una entrada de ``references`` (anidada o plana) y añade un triple si pasa office y tipos.
    Retorna True si se añadió una fila al listado ``out``.
    """
    nested = ref.get("document")
    if isinstance(nested, dict) and nested.get("id"):
        related_id = _safe_int(nested.get("id"))
        rt_raw = (
            nested.get("documentType")
            or nested.get("document_type")
            or nested.get("document_type_id")
        )
        office_blob = dict(nested)
    else:
        related_id = _safe_int(
            ref.get("id") or ref.get("documentId") or ref.get("document_id"),
        )
        rt_raw = (
            ref.get("documentType")
            or ref.get("document_type")
            or ref.get("document_type_id")
        )
        office_blob = dict(ref)
        if related_id is not None and office_blob.get("id") is None:
            office_blob["id"] = related_id

    related_type = _coerce_document_type_id(rt_raw)
    if related_id is None or related_type is None:
        return False
    if related_type not in RELATED_DOCUMENT_TYPES_ALLOWED:
        return False

    allow_office, office_motivo = _office_allows_relation(cur, related_id, office_blob)
    if not allow_office:
        if stats is not None:
            stats["related_skipped_other_office"] = (
                int(stats.get("related_skipped_other_office") or 0) + 1
            )
        logger.warning(
            "%s[detail %s] ref embebida descartada por office: related_document_id=%s → %s",
            log_ctx,
            detail_id,
            related_id,
            office_motivo,
        )
        return False

    out.append((detail_id, related_id, related_type))
    return True


def _documents_json_items_to_triples(
    cur,
    detail_id: int,
    items: list[Any],
    *,
    stats: dict[str, Any] | None,
    log_ctx: str,
) -> list[tuple[int, int, int]]:
    """Parsea ``items`` de ``/documents.json?relateddetailid=`` hacia triples insertables."""
    out: list[tuple[int, int, int]] = []
    logger.info("%s[detail %s] related items API=%s", log_ctx, detail_id, len(items))
    for it in items:
        if not isinstance(it, dict):
            logger.warning("%s[detail %s] ítem no dict: %r", log_ctx, detail_id, it)
            continue

        added_from_refs = False
        for ref in _references_list_from_relateddetail_item(it):
            if isinstance(ref, dict) and _append_triple_from_ref_entry(
                cur, detail_id, ref, stats=stats, log_ctx=log_ctx, out=out
            ):
                added_from_refs = True

        if added_from_refs:
            continue

        rid, tid, office_blob, parse_motivo = _parse_related_document_blob(it)
        if parse_motivo is not None or rid is None:
            logger.warning(
                "%s[detail %s] relación descartada (parser fallback): %s",
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
        if tid not in RELATED_DOCUMENT_TYPES_ALLOWED:
            logger.warning(
                "%s[detail %s] relación descartada: tipo inválido=%s",
                log_ctx,
                detail_id,
                tid,
            )
            continue

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
            "%s[detail %s] parsed relation (fallback) → doc_id=%s type=%s",
            log_ctx,
            detail_id,
            rid,
            tid,
        )
        out.append((detail_id, rid, tid))
    return out


def _fetch_detail_ids_from_bsale_details(
    client: BsaleClient,
    document_id: int,
    *,
    throttle: float,
    log_ctx: str = "",
) -> tuple[list[int], int]:
    """
    ``GET /documents/{document_id}/details.json`` paginado.

    Retorna ``(detail_ids, llamadas_http)``.
    """
    ids: list[int] = []
    api_calls = 0
    offset = 0
    while True:
        try:
            data = client.get(
                f"/documents/{document_id}/details.json",
                {"limit": DETAILS_PAGE_LIMIT, "offset": offset},
            )
        except Exception as e:
            logger.warning("%s details.json document_id=%s offset=%s: %s", log_ctx, document_id, offset, e)
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
    logger.info("%s details.json detail_ids usados=%s (n=%s)", log_ctx, ids, len(ids))
    return ids, api_calls


def _fetch_and_persist_relateddetailid_for_detail(
    client: BsaleClient,
    conn: PgConnection,
    cur,
    detail_id: int,
    *,
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
    offset = 0
    while True:
        try:
            data = client.get(
                "/documents.json",
                {
                    "relateddetailid": detail_id,
                    "limit": RELATED_DETAIL_PAGE_LIMIT,
                    "offset": offset,
                    "officeId": OFFICE_ID,
                },
            )
        except Exception as e:
            logger.warning("%s relateddetailid=%s offset=%s: %s", log_ctx, detail_id, offset, e)
            break
        api_calls += 1
        items = data.get("items") or []
        if not items:
            break
        items_api_total += len(items)
        triples = _documents_json_items_to_triples(
            cur,
            detail_id,
            items,
            stats=stats,
            log_ctx=log_ctx,
        )
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
    detail_ids, calls_details = _fetch_detail_ids_from_bsale_details(
        client,
        document_id,
        throttle=throttle,
        log_ctx=log_ctx,
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

    items_total = 0
    rows_ins = 0
    calls_rel = 0
    for did in detail_ids:
        if stats is not None:
            stats["relateddetail_details_processed"] = (
                int(stats.get("relateddetail_details_processed") or 0) + 1
            )
        it_tot, ins, c = _fetch_and_persist_relateddetailid_for_detail(
            client,
            conn,
            cur,
            did,
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
        "%s relateddetail flujo: document_id=%s details=%s items=%s insertadas=%s api=%s",
        log_ctx,
        document_id,
        len(detail_ids),
        items_total,
        rows_ins,
        calls_total,
    )
    return len(detail_ids), items_total, rows_ins, calls_total


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
    ``references.json`` + ``details.json`` / ``relateddetailid``; persiste en ``document_related``.

    Retorna ``(n_items_api_total, filas_insertadas, llamadas_http)``.
    ``n_items_api_total`` suma ítems de references + documentos devueltos por relateddetailid.
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

    api_calls = 0
    rows_inserted = 0
    items_metric = 0

    # --- 1) references.json ---
    try:
        data = client.get(f"/documents/{document_id}/references.json")
    except Exception as e:
        logger.warning("%s references.json error: %s", log_ctx, e)
        data = None

    if data is not None:
        items = data.get("items")
        if items is None:
            items = data.get("references") or []
        if not isinstance(items, list):
            items = []

        n_items = len(items)
        items_metric += n_items
        if stats is not None:
            stats["references_items_total"] = int(stats.get("references_items_total") or 0) + n_items

        detail_list = _detail_ids_for_document(cur, document_id)
        valid = set(detail_list)
        fallback = detail_list[0] if len(detail_list) == 1 else None

        triples = _reference_items_to_related_triples(
            cur,
            document_id,
            items,
            valid,
            fallback_single_detail_id=fallback,
            stats=stats,
            oc_number=oc_number,
            log_ctx=log_ctx,
        )
        ins_ref = _insert_related_triples(conn, cur, triples, stats=stats, log_ctx=log_ctx)
        rows_inserted += ins_ref

        logger.info(
            "%s references.json items=%s triples=%s insertadas=%s",
            log_ctx,
            n_items,
            len(triples),
            ins_ref,
        )

        if throttle > 0:
            time.sleep(throttle)

        api_calls += 1

    # --- 2) details.json + relateddetailid por línea ---
    ndet, it_rel, ins_det, calls_det = _sync_related_by_detail_for_oc_document(
        client,
        conn,
        cur,
        document_id,
        throttle=throttle,
        stats=stats,
        log_ctx=log_ctx,
    )
    api_calls += calls_det
    rows_inserted += ins_det
    items_metric += it_rel

    logger.info(
        "%s resumen related: document_id=%s items_metric=%s filas_insertadas=%s details_procesados=%s",
        log_ctx,
        document_id,
        items_metric,
        rows_inserted,
        ndet,
    )

    return items_metric, rows_inserted, api_calls


def sync_distribuidora_related_documents(
    *,
    strict_token: bool = False,
    lookback_days: int | None = None,
    limit_details: int | None = None,
    limit_documents: int | None = None,
) -> dict[str, Any]:
    """
    Por cada OC reciente: ``references.json`` + ``details.json`` y ``documents.json?relateddetailid=``.

    Env:
      DISTRIBUIDORA_RELATED_LOOKBACK_DAYS (default 7)
      DISTRIBUIDORA_RELATED_DETAIL_LIMIT (default 250) — límite de **documentos** OC a procesar
    """
    token = _bsale_token()
    if not token:
        if strict_token:
            raise ValueError("Ningún token Bsale: defina BSALE_TOKEN o BSALE_TOKEN_SPA.")
        return {"skipped": True, "skip_reason": "sin token", "inserted": 0}

    lb = lookback_days if lookback_days is not None else int(os.getenv("DISTRIBUIDORA_RELATED_LOOKBACK_DAYS", "7"))
    lim_src = limit_documents if limit_documents is not None else limit_details
    lim = lim_src if lim_src is not None else int(os.getenv("DISTRIBUIDORA_RELATED_DETAIL_LIMIT", "250"))

    t0 = time.perf_counter()
    stats: dict[str, Any] = {
        "documents_considered": 0,
        "details_considered": 0,
        "references_items_total": 0,
        "relateddetail_details_processed": 0,
        "relateddetail_items_total": 0,
        "rows_inserted": 0,
        "api_calls": 0,
        "related_skipped_other_office": 0,
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

        logger.info(
            "sync related distribuidora: office_id=%s; por OC: references.json + "
            "details.json / relateddetailid (company_id=%s office_id=%s)",
            OFFICE_ID,
            COMPANY_ID,
            OFFICE_ID,
        )

        document_ids = _fetch_oc_document_ids(cur, lookback_days=lb, limit_documents=lim)
        stats["documents_considered"] = len(document_ids)

        client = BsaleClient(token)
        throttle = float(os.getenv("DISTRIBUIDORA_RELATED_API_DELAY_SEC", "0.12"))

        for doc_id in document_ids:
            try:
                n_items, ins, calls = _fetch_and_persist_related_for_document(
                    client, conn, cur, doc_id, throttle=throttle, stats=stats
                )
            except Exception as e:
                logger.warning("sync_related document_id=%s: %s", doc_id, e)
                continue
            stats["api_calls"] += calls
            stats["rows_inserted"] += ins
            logger.debug(
                "related doc_id=%s items=%s inserted=%s",
                doc_id,
                n_items,
                ins,
            )

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
            "sync related OK: documents=%s references_items=%s relateddetail_details=%s "
            "relateddetail_items=%s inserted=%s omitidas otra office=%s api=%s s=%.2f",
            stats["documents_considered"],
            stats.get("references_items_total", 0),
            stats.get("relateddetail_details_processed", 0),
            stats.get("relateddetail_items_total", 0),
            stats["rows_inserted"],
            stats.get("related_skipped_other_office", 0),
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

    Por cada ``document_id`` de esas OC: ``references.json`` + ``details.json`` / ``relateddetailid``.
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
                logger.info("Documento OC procesado (references + relateddetailid): %s", doc_id)
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
            "sync related range OK: days=%s documents=%s references_items=%s "
            "relateddetail_details=%s relateddetail_items=%s inserted=%s "
            "omitidas otra office=%s s=%.2f",
            stats["days_processed"],
            stats["documents_processed"],
            stats.get("references_items_total", 0),
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


def debug_sync_related_for_document(document_number: int) -> dict[str, Any]:
    """
    Ejecuta **solo** el flujo ``document_related`` para una OC identificada por ``number`` en BD.

    Útil para depurar (p. ej. ``python -m backend.jobs.debug_sync_related_oc 66080``).
    Requiere el mismo advisory lock que el sync incremental de relaciones.
    """
    token = _bsale_token()
    if not token:
        return {"ok": False, "error": "sin token Bsale"}
    conn = get_connection()
    cur = conn.cursor()
    got_lock = False
    try:
        ensure_distribuidora_schema(cur)
        conn.commit()
        cur.execute(
            """
            SELECT document_id FROM distribuidora.documents
            WHERE company_id = %s AND office_id = %s AND document_type_id = %s AND number = %s
            LIMIT 1
            """,
            (COMPANY_ID, OFFICE_ID, DOC_TYPE_OC, document_number),
        )
        row = cur.fetchone()
        if not row:
            return {
                "ok": False,
                "error": f"OC número {document_number} no encontrada en distribuidora.documents",
            }
        document_id = int(row[0])
        cur.execute("SELECT pg_try_advisory_lock(%s)", (ADVISORY_LOCK_RELATED,))
        got_lock = bool(cur.fetchone()[0])
        if not got_lock:
            return {
                "ok": False,
                "error": "Lock document_related en uso; detenga otro sync related e intente de nuevo.",
            }
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
