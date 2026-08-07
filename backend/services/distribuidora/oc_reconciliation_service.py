"""Reconciliación de OCs por folio y source Bsale vigente."""

from __future__ import annotations

import logging
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator

import psycopg2

from backend.db import get_connection
from backend.repositories.distribuidora.attributes_repo import replace_document_attributes
from backend.repositories.distribuidora.details_repo import replace_document_details
from backend.repositories.distribuidora.documents_repo import (
    document_dict_from_bsale,
    upsert_documents,
)
from backend.repositories.distribuidora.references_repo import replace_document_references
from backend.services.distribuidora.bsale_client import BsaleClient
from backend.services.distribuidora.bsale_params import merge_bsale_office_query
from backend.services.distribuidora.oc_source_resolver import (
    BSALE_CANCELLED_STATE,
    COMPANY_ID,
    OC_DOCUMENT_TYPE_ID,
    OFFICE_ID,
    PAGE_LIMIT,
    compute_oc_source_hash,
    discover_oc_sources,
    fetch_all_document_details,
    find_cancelled_source_evidence,
    select_active_oc_source,
    source_updated_at,
    summarize_bsale_document,
)
from backend.services.order_weight_service import (
    calculate_order_weight,
    recalculate_order_weight,
    recalculate_order_weight_in_transaction,
)
from backend.utils.delivery_day_detect import detect_delivery_day_from_observation

logger = logging.getLogger(__name__)
ADVISORY_LOCK_OC_RECONCILIATION = 5_927_184_013
# Debe competir con los tres writers existentes de encabezado/detalles.
ADVISORY_LOCK_OC_WRITERS = (
    5_927_184_003,  # sync_service.ADVISORY_LOCK_KEY
    5_927_184_010,  # live_sync_service.ADVISORY_LOCK_DOCUMENTS_LIVE
    5_927_184_011,  # live_sync_service.ADVISORY_LOCK_DETAILS_LIVE
)

SOURCE_METADATA_COLUMNS = frozenset(
    {
        "source_document_id",
        "source_hash",
        "source_updated_at",
        "last_synced_at",
        "last_reconciliation_at",
    }
)


class ActiveSyncConflict(RuntimeError):
    """Conflicto temporal: otro writer de documentos/detalles está activo."""


def _is_global_reconciliation_error(exc: Exception) -> bool:
    if isinstance(
        exc,
        (
            psycopg2.OperationalError,
            psycopg2.InterfaceError,
            psycopg2.errors.UndefinedColumn,
            psycopg2.errors.UndefinedTable,
        ),
    ):
        return True
    message = str(exc)
    return (
        "Bsale 401 Unauthorized" in message
        or "Migración 044 pendiente" in message
        or "Migración 045 pendiente" in message
    )


def _num(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _equal(a: Any, b: Any, *, tolerance: float = 0.01) -> bool:
    na, nb = _num(a), _num(b)
    if na is not None and nb is not None:
        return abs(na - nb) < tolerance
    return a == b


def needs_detail_sync(
    *,
    pg_document: dict[str, Any] | None,
    pg_details: list[dict[str, Any]],
    bsale_document: dict[str, Any] | None,
    bsale_details: list[dict[str, Any]],
) -> bool:
    """True si el header existe/tiene monto pero faltan líneas locales.

    Un ``source_hash`` coincidente **no** exime de recuperar details.
    """
    if len(pg_details) > 0:
        return False
    total_pg = _num((pg_document or {}).get("total_amount")) or 0.0
    total_bs = _num((bsale_document or {}).get("totalAmount")) or 0.0
    return total_pg > 0 or total_bs > 0 or len(bsale_details) > 0


def _line_from_pg(row: tuple[Any, ...]) -> dict[str, Any]:
    return {
        "detail_id": int(row[0]),
        "variant_id": int(row[1]) if row[1] is not None else None,
        "quantity": _num(row[2]),
        "total_amount": _num(row[3]),
    }


def _line_from_bsale(item: dict[str, Any]) -> dict[str, Any]:
    variant = item.get("variant") if isinstance(item.get("variant"), dict) else {}
    return {
        "detail_id": int(item["id"]) if item.get("id") is not None else None,
        "variant_id": (
            int(variant["id"]) if variant.get("id") is not None else None
        ),
        "quantity": _num(item.get("quantity")),
        "total_amount": _num(item.get("totalAmount")),
    }


def _normalize_obs_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def observaciones_from_attributes_payload(payload: dict[str, Any] | None) -> str | None:
    """Extrae OBSERVACIONES desde payload Bsale ``attributes.json``."""
    if not isinstance(payload, dict):
        return None
    items = payload.get("items") or []
    if not isinstance(items, list):
        return None
    for item in items:
        if not isinstance(item, dict):
            continue
        name = item.get("name") or item.get("attributeName") or item.get("label")
        if name is None or str(name).strip().upper() != "OBSERVACIONES":
            continue
        val = item.get("value") or item.get("text") or item.get("content")
        return _normalize_obs_text(val)
    return None


def fetch_document_attributes_payload(
    client: BsaleClient,
    source_document_id: int,
) -> dict[str, Any]:
    raw = client.get(f"/documents/{int(source_document_id)}/attributes.json")
    if isinstance(raw, dict):
        return raw
    return {"items": raw if isinstance(raw, list) else []}


def fetch_document_references_payload(
    client: BsaleClient,
    source_document_id: int,
) -> dict[str, Any]:
    raw = client.get(f"/documents/{int(source_document_id)}/references.json")
    if isinstance(raw, dict):
        return raw
    return {"items": raw if isinstance(raw, list) else []}


def load_local_observaciones(document_id: int) -> str | None:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT attribute_value
            FROM distribuidora.document_attributes
            WHERE document_id = %s
              AND UPPER(BTRIM(attribute_name)) = 'OBSERVACIONES'
            ORDER BY created_at DESC NULLS LAST
            LIMIT 1
            """,
            (int(document_id),),
        )
        row = cur.fetchone()
        cur.close()
        return _normalize_obs_text(row[0] if row else None)
    finally:
        conn.close()


def load_local_invoice_link_flags(document_id: int) -> dict[str, Any]:
    """Flags read-only de facturación local (related + probable)."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM distribuidora.document_details dd
                INNER JOIN distribuidora.document_related dr
                    ON dr.detail_id = dd.detail_id
                INNER JOIN distribuidora.documents inv
                    ON inv.document_id = dr.related_document_id
                   AND inv.document_type_id IN (1, 6)
                   AND COALESCE(inv.state, 0) = 0
                WHERE dd.document_id = %s
            )
            """,
            (int(document_id),),
        )
        has_related = bool((cur.fetchone() or [False])[0])
        cur.execute(
            """
            SELECT score, candidate_document_id
            FROM distribuidora.document_probable_matches
            WHERE oc_document_id = %s AND score >= 60
            ORDER BY score DESC, candidate_document_id DESC
            LIMIT 1
            """,
            (int(document_id),),
        )
        prob = cur.fetchone()
        cur.close()
        return {
            "has_confirmed_invoice_link": has_related,
            "has_probable_match": prob is not None,
            "probable_score": float(prob[0]) if prob else None,
            "probable_candidate_document_id": int(prob[1]) if prob else None,
        }
    finally:
        conn.close()


def compare_oc_state(
    *,
    pg_document: dict[str, Any] | None,
    pg_details: list[dict[str, Any]],
    bsale_document: dict[str, Any],
    bsale_details: list[dict[str, Any]],
    pg_observaciones: str | None = None,
    bsale_observaciones: str | None = None,
) -> dict[str, Any]:
    """Diff operacional por encabezado, líneas y OBSERVACIONES (día de entrega)."""
    bsale_summary = summarize_bsale_document(bsale_document)
    header_fields = (
        ("number", pg_document.get("number") if pg_document else None, bsale_summary["number"]),
        (
            "total_amount",
            pg_document.get("total_amount") if pg_document else None,
            bsale_document.get("totalAmount"),
        ),
        (
            "net_amount",
            pg_document.get("net_amount") if pg_document else None,
            bsale_document.get("netAmount"),
        ),
        (
            "tax_amount",
            pg_document.get("tax_amount") if pg_document else None,
            bsale_document.get("taxAmount"),
        ),
        ("state", pg_document.get("state") if pg_document else None, bsale_summary["state"]),
        (
            "commercial_state",
            pg_document.get("commercial_state") if pg_document else None,
            bsale_summary["commercialState"],
        ),
    )
    header_diff = [
        {
            "field": field,
            "postgresql": pg_value,
            "bsale": bsale_value,
            "matches": _equal(pg_value, bsale_value),
        }
        for field, pg_value, bsale_value in header_fields
    ]

    pg_by_id = {
        int(line["detail_id"]): line
        for line in pg_details
        if line.get("detail_id") is not None
    }
    bsale_lines = [_line_from_bsale(item) for item in bsale_details]
    bsale_by_id = {
        int(line["detail_id"]): line
        for line in bsale_lines
        if line.get("detail_id") is not None
    }
    only_pg = sorted(set(pg_by_id) - set(bsale_by_id))
    only_bsale = sorted(set(bsale_by_id) - set(pg_by_id))
    line_diff: list[dict[str, Any]] = []
    for detail_id in sorted(set(pg_by_id) & set(bsale_by_id)):
        p = pg_by_id[detail_id]
        b = bsale_by_id[detail_id]
        for field in ("variant_id", "quantity", "total_amount"):
            if not _equal(p.get(field), b.get(field), tolerance=0.0001):
                line_diff.append(
                    {
                        "detail_id": detail_id,
                        "field": field,
                        "postgresql": p.get(field),
                        "bsale": b.get(field),
                        "matches": False,
                    }
                )

    pg_qty = sum(_num(line.get("quantity")) or 0 for line in pg_details)
    bsale_qty = sum(_num(line.get("quantity")) or 0 for line in bsale_lines)
    pg_total = sum(_num(line.get("total_amount")) or 0 for line in pg_details)
    bsale_total = sum(_num(line.get("total_amount")) or 0 for line in bsale_lines)
    pg_obs = _normalize_obs_text(pg_observaciones)
    bsale_obs = _normalize_obs_text(bsale_observaciones)
    attributes_match = (pg_obs or "").casefold() == (bsale_obs or "").casefold()
    pg_day = detect_delivery_day_from_observation(pg_obs)
    bsale_day = detect_delivery_day_from_observation(bsale_obs)
    delivery_day_match = pg_day == bsale_day
    matches = (
        pg_document is not None
        and all(item["matches"] for item in header_diff)
        and not only_pg
        and not only_bsale
        and not line_diff
        and len(pg_details) == len(bsale_lines)
        and _equal(pg_qty, bsale_qty)
        and _equal(pg_total, bsale_total)
        and attributes_match
    )
    return {
        "matches": matches,
        "header": header_diff,
        "lines": line_diff,
        "only_in_postgresql_detail_ids": only_pg,
        "only_in_bsale_detail_ids": only_bsale,
        "postgresql_quantity": pg_qty,
        "bsale_quantity": bsale_qty,
        "postgresql_line_total": pg_total,
        "bsale_line_total": bsale_total,
        "postgresql_lines": pg_details,
        "bsale_lines": bsale_lines,
        "attributes": {
            "postgresql_observaciones": pg_obs,
            "bsale_observaciones": bsale_obs,
            "matches": attributes_match,
            "postgresql_delivery_day": pg_day,
            "bsale_delivery_day": bsale_day,
            "delivery_day_matches": delivery_day_match,
        },
    }


def _load_local_oc(
    *,
    folio: int | None,
    local_document_id: int | None,
    company_id: int = COMPANY_ID,
    office_id: int = OFFICE_ID,
) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        if local_document_id is not None:
            cur.execute(
                """
                SELECT document_id, number, company_id, office_id, document_type_id,
                       total_amount, net_amount, tax_amount, state, commercial_state,
                       generation_date, raw_data,
                       NULLIF(to_jsonb(d)->>'source_document_id', '')::bigint
                           AS source_document_id,
                       to_jsonb(d)->>'source_hash' AS source_hash,
                       NULLIF(to_jsonb(d)->>'source_updated_at', '')::timestamptz
                           AS source_updated_at,
                       NULLIF(to_jsonb(d)->>'last_synced_at', '')::timestamptz
                           AS last_synced_at,
                       NULLIF(to_jsonb(d)->>'last_reconciliation_at', '')::timestamptz
                           AS last_reconciliation_at
                FROM distribuidora.documents d
                WHERE document_id = %s
                LIMIT 1
                """,
                (int(local_document_id),),
            )
        else:
            cur.execute(
                """
                SELECT document_id, number, company_id, office_id, document_type_id,
                       total_amount, net_amount, tax_amount, state, commercial_state,
                       generation_date, raw_data,
                       NULLIF(to_jsonb(d)->>'source_document_id', '')::bigint
                           AS source_document_id,
                       to_jsonb(d)->>'source_hash' AS source_hash,
                       NULLIF(to_jsonb(d)->>'source_updated_at', '')::timestamptz
                           AS source_updated_at,
                       NULLIF(to_jsonb(d)->>'last_synced_at', '')::timestamptz
                           AS last_synced_at,
                       NULLIF(to_jsonb(d)->>'last_reconciliation_at', '')::timestamptz
                           AS last_reconciliation_at
                FROM distribuidora.documents d
                WHERE company_id = %s AND office_id = %s
                  AND document_type_id = %s AND number = %s
                LIMIT 1
                """,
                (int(company_id), int(office_id), OC_DOCUMENT_TYPE_ID, int(folio)),
            )
        row = cur.fetchone()
        if not row:
            cur.close()
            return None, []
        columns = [desc[0] for desc in cur.description]
        document = dict(zip(columns, row))
        doc_id = int(document["document_id"])
        cur.execute(
            """
            SELECT detail_id, variant_id, quantity, total_amount
            FROM distribuidora.document_details
            WHERE document_id = %s
            ORDER BY line_number NULLS LAST, detail_id
            """,
            (doc_id,),
        )
        details = [_line_from_pg(item) for item in cur.fetchall()]
        cur.execute(
            """
            SELECT attribute_value
            FROM distribuidora.document_attributes
            WHERE document_id = %s
              AND UPPER(BTRIM(attribute_name)) = 'OBSERVACIONES'
            ORDER BY created_at DESC NULLS LAST
            LIMIT 1
            """,
            (doc_id,),
        )
        obs_row = cur.fetchone()
        document["observaciones"] = _normalize_obs_text(obs_row[0] if obs_row else None)
        cur.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM distribuidora.document_details dd
                INNER JOIN distribuidora.document_related dr
                    ON dr.detail_id = dd.detail_id
                INNER JOIN distribuidora.documents inv
                    ON inv.document_id = dr.related_document_id
                   AND inv.document_type_id IN (1, 6)
                   AND COALESCE(inv.state, 0) = 0
                WHERE dd.document_id = %s
            )
            """,
            (doc_id,),
        )
        has_related = bool((cur.fetchone() or [False])[0])
        cur.execute(
            """
            SELECT score, candidate_document_id
            FROM distribuidora.document_probable_matches
            WHERE oc_document_id = %s AND score >= 60
            ORDER BY score DESC, candidate_document_id DESC
            LIMIT 1
            """,
            (doc_id,),
        )
        prob = cur.fetchone()
        document["invoice_link"] = {
            "has_confirmed_invoice_link": has_related,
            "has_probable_match": prob is not None,
            "probable_score": float(prob[0]) if prob else None,
            "probable_candidate_document_id": int(prob[1]) if prob else None,
        }
        cur.close()
        return document, details
    finally:
        conn.close()


def _known_source_ids(document: dict[str, Any] | None) -> list[int]:
    if not document:
        return []
    values: list[Any] = [
        document.get("document_id"),
        document.get("source_document_id"),
    ]
    raw = document.get("raw_data")
    if isinstance(raw, dict):
        values.append(raw.get("id"))
    output: list[int] = []
    for value in values:
        try:
            source_id = int(value)
        except (TypeError, ValueError):
            continue
        if source_id > 0 and source_id not in output:
            output.append(source_id)
    return output


def _projected_weight(
    *,
    local_document_id: int | None,
    pg_details: list[dict[str, Any]],
    bsale_details: list[dict[str, Any]],
    company_id: int = COMPANY_ID,
    office_id: int = OFFICE_ID,
) -> dict[str, Any]:
    """Proyección en memoria (no persiste). Preferir recalc local si hay líneas PG."""
    before: dict[str, Any] = {}
    if local_document_id is not None:
        try:
            before = calculate_order_weight(
                int(local_document_id),
                company_id=int(company_id),
                office_id=int(office_id),
                persist_cache=False,
            )
        except Exception:
            logger.exception("No se pudo calcular peso read-only de OC %s", local_document_id)

    # Si hay líneas locales, el recalc en memoria ES la proyección (no depender de snapshot).
    if before and int(before.get("productos_totales") or 0) > 0:
        payload = before.get("weight") if isinstance(before.get("weight"), dict) else {}
        status = payload.get("status") or "unavailable"
        value = payload.get("value_kg")
        if value is None and status not in {"unavailable", "error"}:
            value = _num(before.get("peso_total_kg"))
        return {
            "before_kg": _num(before.get("peso_total_kg")),
            "after_projected_kg": _num(value),
            "peso_total_kg": _num(value),
            "status": status,
            "productos_sin_peso": int(before.get("productos_sin_peso") or 0),
            "productos_totales": int(before.get("productos_totales") or 0),
            "porcentaje_cobertura": _num(before.get("porcentaje_cobertura")),
            "projected_unresolved_lines": int(before.get("productos_sin_peso") or 0),
            "source": "local_details_recalc",
        }

    unit_by_variant: dict[int, float] = {}
    for line in before.get("lines") or []:
        variant_id = line.get("variant_id")
        unit = _num(line.get("peso_unitario_kg"))
        if variant_id is not None and unit is not None:
            unit_by_variant[int(variant_id)] = unit
    projected = 0.0
    unresolved = 0
    for item in bsale_details:
        line = _line_from_bsale(item)
        variant_id = line.get("variant_id")
        unit = unit_by_variant.get(int(variant_id)) if variant_id is not None else None
        if unit is None:
            unresolved += 1
            continue
        projected += (_num(line.get("quantity")) or 0) * unit
    total_lines = len(bsale_details)
    if total_lines <= 0:
        status = "unavailable"
        value = None
    elif unresolved == 0 and projected > 0:
        status = "calculated"
        value = round(projected, 3)
    elif unresolved > 0 and projected > 0:
        status = "partial"
        value = round(projected, 3)
    else:
        status = "unavailable"
        value = None
    coverage = None
    if total_lines > 0:
        coverage = round(100.0 * (total_lines - unresolved) / total_lines, 2)
    return {
        "before_kg": _num(before.get("peso_total_kg")),
        "after_projected_kg": value,
        "peso_total_kg": value,
        "status": status,
        "productos_sin_peso": unresolved,
        "productos_totales": total_lines,
        "porcentaje_cobertura": coverage,
        "projected_unresolved_lines": unresolved,
        "source": "bsale_lines_projection",
    }


def _has_weight_snapshot(document_id: int) -> bool:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'distribuidora'
              AND table_name = 'order_weight_snapshots'
            """
        )
        if not cur.fetchone():
            cur.close()
            return False
        cur.execute(
            """
            SELECT 1
            FROM distribuidora.order_weight_snapshots
            WHERE document_id = %s
            LIMIT 1
            """,
            (int(document_id),),
        )
        found = cur.fetchone() is not None
        cur.close()
        return found
    finally:
        conn.close()


def needs_weight_snapshot_sync(
    *,
    local_document_id: int | None,
    pg_details: list[dict[str, Any]],
) -> bool:
    """Details locales OK pero falta snapshot que consume planning-rows."""
    if local_document_id is None:
        return False
    if len(pg_details) <= 0:
        return False
    return not _has_weight_snapshot(int(local_document_id))


def _assert_source_schema(cur) -> None:
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'distribuidora'
          AND table_name = 'documents'
          AND column_name = ANY(%s::text[])
        """,
        (list(SOURCE_METADATA_COLUMNS),),
    )
    found = {row[0] for row in cur.fetchall()}
    missing = SOURCE_METADATA_COLUMNS - found
    if missing:
        raise RuntimeError(
            "Migración 044 pendiente; faltan columnas: " + ", ".join(sorted(missing))
        )


def _assert_plan_invalidation_schema(cur) -> None:
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'distribuidora'
          AND table_name = 'dispatch_plan'
          AND column_name = ANY(%s::text[])
        """,
        (["needs_recalculation", "invalidated_at", "invalidation_reason"],),
    )
    found = {row[0] for row in cur.fetchall()}
    missing = {
        "needs_recalculation",
        "invalidated_at",
        "invalidation_reason",
    } - found
    if missing:
        raise RuntimeError(
            "Migración 045 pendiente; faltan columnas: "
            + ", ".join(sorted(missing))
        )


def _mark_reconciliation_attempt(
    local_document_id: int,
    *,
    successful: bool,
) -> None:
    """Mueve el cursor rotativo; una revisión exitosa también refresca sync."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        if successful:
            cur.execute(
                """
                UPDATE distribuidora.documents
                SET last_reconciliation_at = NOW(),
                    last_synced_at = NOW()
                WHERE document_id = %s
                """,
                (int(local_document_id),),
            )
        else:
            cur.execute(
                """
                UPDATE distribuidora.documents
                SET last_reconciliation_at = NOW()
                WHERE document_id = %s
                """,
                (int(local_document_id),),
            )
        conn.commit()
        cur.close()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _invalidate_affected_dispatch_plans(cur, local_document_id: int) -> int:
    """Marca planes no despachados sin destruir su snapshot histórico."""
    cur.execute(
        """
        UPDATE distribuidora.dispatch_plan p
        SET needs_recalculation = TRUE,
            invalidated_at = NOW(),
            invalidation_reason = %s,
            updated_at = NOW()
        WHERE p.status <> 'dispatched'
          AND EXISTS (
              SELECT 1
              FROM distribuidora.dispatch_plan_orders dpo
              WHERE dpo.dispatch_plan_id = p.id
                AND dpo.oc_document_id = %s
          )
        """,
        (
            f"OC {int(local_document_id)} cambió durante reconciliación Bsale",
            int(local_document_id),
        ),
    )
    return int(cur.rowcount or 0)


def _apply_local_oc_cancellation(
    cur,
    *,
    local_document_id: int,
    folio: int,
    cancelled_evidence: dict[str, Any],
    previous_state: Any,
) -> int:
    """Marca la PK local como anulada sin borrar detalles ni snapshots."""
    cancelled_source_id = cancelled_evidence.get("id")
    cancelled_source_updated = None
    raw_generation = cancelled_evidence.get("generationDate")
    raw_modification = cancelled_evidence.get("modificationDate")
    for raw in (raw_modification, raw_generation):
        try:
            if raw is not None:
                cancelled_source_updated = datetime.fromtimestamp(int(raw), tz=timezone.utc)
                break
        except (TypeError, ValueError, OSError, OverflowError):
            continue

    cur.execute(
        """
        UPDATE distribuidora.documents
        SET state = %s,
            last_reconciliation_at = NOW(),
            last_synced_at = NOW(),
            source_updated_at = COALESCE(%s, source_updated_at),
            source_document_id = COALESCE(source_document_id, %s),
            updated_at = NOW()
        WHERE document_id = %s
        """,
        (
            BSALE_CANCELLED_STATE,
            cancelled_source_updated,
            int(cancelled_source_id) if cancelled_source_id is not None else None,
            int(local_document_id),
        ),
    )
    if cur.rowcount != 1:
        raise RuntimeError(
            f"No se pudo marcar anulación de document_id={local_document_id}"
        )
    invalidated = _invalidate_affected_dispatch_plans(cur, int(local_document_id))
    logger.info(
        "oc_cancelled_detected folio=%s local_document_id=%s previous_state=%s "
        "new_state=%s dispatch_plans_invalidated=%s cancelled_source_id=%s",
        folio,
        local_document_id,
        previous_state,
        BSALE_CANCELLED_STATE,
        invalidated,
        cancelled_source_id,
    )
    return invalidated


def _report_cancelled_oc(
    *,
    dry_run: bool,
    folio: int,
    pg_document: dict[str, Any],
    discovery: dict[str, Any],
    cancelled_evidence: dict[str, Any],
    wrote: bool,
    dispatch_plans_invalidated: int = 0,
    status: str,
) -> dict[str, Any]:
    local_id = int(pg_document["document_id"])
    previous_state = pg_document.get("state")
    discovery_report = {
        key: value for key, value in discovery.items() if key != "active_document"
    }
    return {
        "status": status,
        "dry_run": dry_run,
        "wrote": wrote,
        "folio": folio,
        "local_document_id": local_id,
        "previous_state": previous_state,
        "new_state": BSALE_CANCELLED_STATE,
        "cancelled_source_document_id": cancelled_evidence.get("id"),
        "current_bsale_source_document_id": None,
        "source_changed": False,
        "dispatch_plans_invalidated": dispatch_plans_invalidated,
        "details_preserved": True,
        "source_discovery": discovery_report,
        "cancelled_evidence": cancelled_evidence,
    }


def _handle_cancelled_oc(
    *,
    dry_run: bool,
    folio: int,
    pg_document: dict[str, Any],
    discovery: dict[str, Any],
    cancelled_evidence: dict[str, Any],
) -> dict[str, Any]:
    local_id = int(pg_document["document_id"])
    previous_state = pg_document.get("state")
    already_cancelled = int(previous_state or 0) != 0

    if already_cancelled:
        if not dry_run:
            _mark_reconciliation_attempt(local_id, successful=True)
        return _report_cancelled_oc(
            dry_run=dry_run,
            folio=folio,
            pg_document=pg_document,
            discovery=discovery,
            cancelled_evidence=cancelled_evidence,
            wrote=False,
            status="already_cancelled",
        )

    if dry_run:
        return _report_cancelled_oc(
            dry_run=True,
            folio=folio,
            pg_document=pg_document,
            discovery=discovery,
            cancelled_evidence=cancelled_evidence,
            wrote=False,
            status="dry_run_cancelled",
        )

    conn = get_connection()
    try:
        cur = conn.cursor()
        try:
            _assert_source_schema(cur)
            _assert_plan_invalidation_schema(cur)
            invalidated = _apply_local_oc_cancellation(
                cur,
                local_document_id=local_id,
                folio=folio,
                cancelled_evidence=cancelled_evidence,
                previous_state=previous_state,
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
    finally:
        conn.close()

    return _report_cancelled_oc(
        dry_run=False,
        folio=folio,
        pg_document=pg_document,
        discovery=discovery,
        cancelled_evidence=cancelled_evidence,
        wrote=True,
        dispatch_plans_invalidated=invalidated,
        status="cancelled",
    )


@contextmanager
def _oc_writer_locks() -> Iterator[None]:
    """Adquiere en una sesión los locks de todos los writers de OCs."""
    conn = get_connection()
    acquired: list[int] = []
    try:
        cur = conn.cursor()
        for lock_key in ADVISORY_LOCK_OC_WRITERS:
            cur.execute("SELECT pg_try_advisory_lock(%s)", (lock_key,))
            if not bool(cur.fetchone()[0]):
                raise ActiveSyncConflict(
                    "Otro sync de documentos/detalles está ejecutándose; reintente"
                )
            acquired.append(lock_key)
        conn.commit()
        cur.close()
        yield
    finally:
        for lock_key in reversed(acquired):
            try:
                cur = conn.cursor()
                cur.execute("SELECT pg_advisory_unlock(%s)", (lock_key,))
                cur.close()
            except Exception:
                logger.exception("No se pudo liberar advisory lock %s", lock_key)
        conn.close()


def _reconcile_one_oc(
    client: BsaleClient,
    *,
    folio: int | None = None,
    local_document_id: int | None = None,
    dry_run: bool = True,
    active_document: dict[str, Any] | None = None,
    user_email: str | None = None,
    company_id: int = COMPANY_ID,
    office_id: int = OFFICE_ID,
) -> dict[str, Any]:
    """Descubre source vigente, compara y opcionalmente persiste una OC."""
    if folio is None and local_document_id is None:
        raise ValueError("Indique folio o local_document_id")
    pg_document, pg_details = _load_local_oc(
        folio=folio,
        local_document_id=local_document_id,
        company_id=company_id,
        office_id=office_id,
    )
    if local_document_id is not None:
        if pg_document is None:
            raise ValueError(
                f"local_document_id={local_document_id} no existe en PostgreSQL"
            )
        if int(pg_document["document_id"]) != int(local_document_id):
            raise ValueError(
                "La PK local resuelta no coincide con document_id solicitado"
            )
    resolved_folio = int(folio or (pg_document or {}).get("number") or 0)
    if resolved_folio <= 0:
        raise ValueError("No se pudo resolver el folio local")

    if active_document is None:
        discovery = discover_oc_sources(
            client,
            folio=resolved_folio,
            known_source_ids=_known_source_ids(pg_document),
            company_id=company_id,
            office_id=office_id,
            document_type_id=OC_DOCUMENT_TYPE_ID,
        )
        selected = discovery.get("active_document")
    else:
        selected = active_document
        _, evaluated = select_active_oc_source(
            [active_document],
            folio=resolved_folio,
            company_id=company_id,
            office_id=office_id,
            document_type_id=OC_DOCUMENT_TYPE_ID,
        )
        discovery = {
            "folio": resolved_folio,
            "source": "reconciliation_window",
            "documents": evaluated,
            "active_document": active_document,
            "active_source_document_id": summarize_bsale_document(
                active_document,
                expected_company_id=company_id,
            ).get("id"),
        }
    if not isinstance(selected, dict):
        cancelled_evidence = find_cancelled_source_evidence(
            discovery,
            known_source_ids=_known_source_ids(pg_document),
        )
        if cancelled_evidence is not None and pg_document is not None:
            return _handle_cancelled_oc(
                dry_run=dry_run,
                folio=resolved_folio,
                pg_document=pg_document,
                discovery=discovery,
                cancelled_evidence=cancelled_evidence,
            )
        return {
            "status": "source_not_found",
            "dry_run": dry_run,
            "folio": resolved_folio,
            "local_document_id": (pg_document or {}).get("document_id"),
            "source_discovery": discovery,
            "wrote": False,
        }

    # No revivir una OC ya anulada/inactiva en PostgreSQL.
    if pg_document is not None and int(pg_document.get("state") or 0) != 0:
        local_id = int(pg_document["document_id"])
        if not dry_run:
            _mark_reconciliation_attempt(local_id, successful=True)
        discovery_report = {
            key: value for key, value in discovery.items() if key != "active_document"
        }
        return {
            "status": "already_cancelled",
            "dry_run": dry_run,
            "wrote": False,
            "folio": resolved_folio,
            "local_document_id": local_id,
            "previous_state": pg_document.get("state"),
            "new_state": pg_document.get("state"),
            "revival_skipped": True,
            "current_bsale_source_document_id": summarize_bsale_document(
                selected,
                expected_company_id=company_id,
            ).get("id"),
            "source_discovery": discovery_report,
        }

    selected_summary = summarize_bsale_document(
        selected,
        expected_company_id=company_id,
    )
    source_id = int(selected_summary["id"])
    # Defensa final, aun si el caller entregó ``active_document``.
    reselected, evaluated = select_active_oc_source(
        [selected],
        folio=resolved_folio,
        company_id=company_id,
        office_id=office_id,
        document_type_id=OC_DOCUMENT_TYPE_ID,
    )
    if reselected is None:
        raise ValueError(f"Source Bsale no elegible: {evaluated}")

    known_ids_for_log = _known_source_ids(pg_document)
    logger.info(
        "reconcile_oc_selected folio=%s local_document_id=%s "
        "previous_source_document_id=%s current_bsale_source_document_id=%s",
        resolved_folio,
        (pg_document or {}).get("document_id"),
        (pg_document or {}).get("source_document_id")
        or (known_ids_for_log[-1] if known_ids_for_log else None),
        source_id,
    )
    details = fetch_all_document_details(client, source_id)
    attributes_payload: dict[str, Any] | None = None
    references_payload: dict[str, Any] | None = None
    bsale_observaciones: str | None = None
    try:
        attributes_payload = fetch_document_attributes_payload(client, source_id)
        bsale_observaciones = observaciones_from_attributes_payload(attributes_payload)
    except Exception as exc:
        logger.warning(
            "reconcile_oc_attributes_fetch_failed folio=%s source_id=%s: %s",
            resolved_folio,
            source_id,
            exc,
        )
    try:
        references_payload = fetch_document_references_payload(client, source_id)
    except Exception as exc:
        logger.warning(
            "reconcile_oc_references_fetch_failed folio=%s source_id=%s: %s",
            resolved_folio,
            source_id,
            exc,
        )
    local_id = int((pg_document or {}).get("document_id") or 0) or None
    pg_observaciones = _normalize_obs_text(
        (pg_document or {}).get("observaciones")
    )
    digest = compute_oc_source_hash(selected, details)
    diff = compare_oc_state(
        pg_document=pg_document,
        pg_details=pg_details,
        bsale_document=selected,
        bsale_details=details,
        pg_observaciones=pg_observaciones,
        bsale_observaciones=bsale_observaciones,
    )
    stored_hash = (pg_document or {}).get("source_hash")
    hash_matches = stored_hash == digest and stored_hash is not None
    detail_sync_needed = needs_detail_sync(
        pg_document=pg_document,
        pg_details=pg_details,
        bsale_document=selected,
        bsale_details=details,
    )
    invoice_flags = dict((pg_document or {}).get("invoice_link") or {})
    weight = _projected_weight(
        local_document_id=local_id,
        pg_details=pg_details,
        bsale_details=details,
        company_id=company_id,
        office_id=office_id,
    )
    raw_pg = (pg_document or {}).get("raw_data")
    raw_pg_id = raw_pg.get("id") if isinstance(raw_pg, dict) else None
    previous_source_raw = (
        (pg_document or {}).get("source_document_id")
        or raw_pg_id
        or local_id
    )
    try:
        previous_source_id = (
            int(previous_source_raw) if previous_source_raw is not None else None
        )
    except (TypeError, ValueError):
        previous_source_id = None
    discovery_report = {
        key: value for key, value in discovery.items() if key != "active_document"
    }
    discovery_report["active_document"] = selected_summary
    pg_document_report = None
    if pg_document is not None:
        pg_document_report = {
            key: value for key, value in pg_document.items() if key != "raw_data"
        }
        pg_document_report["raw_source_document_id"] = raw_pg_id
    report: dict[str, Any] = {
        "status": "dry_run",
        "dry_run": dry_run,
        "wrote": False,
        "folio": resolved_folio,
        "local_document_id": local_id,
        "previous_source_document_id": previous_source_id,
        "current_bsale_source_document_id": source_id,
        "source_changed": previous_source_id is not None
        and source_id != previous_source_id,
        "source_hash": digest,
        "stored_source_hash": stored_hash,
        "source_hash_matches": hash_matches,
        "needs_detail_sync": detail_sync_needed,
        "header_synced": pg_document is not None,
        "details_synced": len(pg_details) > 0 and not detail_sync_needed,
        "needs_weight_snapshot_sync": False,
        "source_updated_at": (
            source_updated_at(selected).isoformat()
            if source_updated_at(selected)
            else None
        ),
        "postgresql_document": pg_document_report,
        "bsale_document": selected_summary,
        "postgresql_details": pg_details,
        "bsale_details": [_line_from_bsale(item) for item in details],
        "postgresql_details_count": len(pg_details),
        "bsale_details_count": len(details),
        "diff": diff,
        "weight": weight,
        "source_discovery": discovery_report,
        "delivery": {
            "postgresql_observaciones": pg_observaciones,
            "bsale_observaciones": bsale_observaciones,
            "postgresql_day": (diff.get("attributes") or {}).get(
                "postgresql_delivery_day"
            ),
            "bsale_day": (diff.get("attributes") or {}).get("bsale_delivery_day"),
            "source_priority": "observacion>comentario>ruta",
        },
        "invoice_link": invoice_flags,
    }
    weight_snapshot_needed = needs_weight_snapshot_sync(
        local_document_id=local_id,
        pg_details=pg_details,
    )
    report["needs_weight_snapshot_sync"] = weight_snapshot_needed
    if dry_run:
        if detail_sync_needed:
            report["status"] = "dry_run_needs_sync"
        elif weight_snapshot_needed:
            report["status"] = "dry_run_needs_weight_snapshot"
        else:
            report["status"] = (
                "dry_run_in_sync" if diff["matches"] else "dry_run_needs_sync"
            )
        return report
    # Hash coincidente NO es sync completa si faltan details locales.
    if hash_matches and diff["matches"] and not detail_sync_needed:
        if weight_snapshot_needed and local_id is not None:
            logger.warning(
                "oc_needs_weight_snapshot folio=%s local_document_id=%s "
                "pg_details=%s status=rebuilding_snapshot",
                resolved_folio,
                local_id,
                len(pg_details),
            )
            weight_result = recalculate_order_weight(
                document_id=int(local_id),
                company_id=company_id,
                office_id=office_id,
                user_email=user_email,
                persist=True,
            )
            _mark_reconciliation_attempt(local_id, successful=True)
            report.update(
                {
                    "status": "synced_weight_snapshot",
                    "wrote": True,
                    "weight_snapshot_rebuilt": True,
                    "needs_weight_snapshot_sync": False,
                    "peso_despues_kg": weight_result.get("peso_total_kg"),
                    "cobertura_despues": weight_result.get("porcentaje_cobertura"),
                    "weight": {
                        **weight,
                        "peso_total_kg": weight_result.get("peso_total_kg"),
                        "status": (weight_result.get("weight") or {}).get("status")
                        or weight.get("status"),
                        "productos_sin_peso": weight_result.get("productos_sin_peso"),
                        "productos_totales": weight_result.get("productos_totales"),
                        "porcentaje_cobertura": weight_result.get(
                            "porcentaje_cobertura"
                        ),
                    },
                }
            )
            return report
        if local_id is not None:
            _mark_reconciliation_attempt(local_id, successful=True)
        report["status"] = "already_in_sync"
        report["metadata_updated"] = local_id is not None
        return report
    if detail_sync_needed:
        logger.warning(
            "oc_needs_detail_sync folio=%s local_document_id=%s "
            "source_hash_matches=%s pg_details=%s bsale_details=%s",
            resolved_folio,
            local_id,
            hash_matches,
            len(pg_details),
            len(details),
        )

    attributes_written = 0
    references_written = 0
    related_inserts = 0
    details_written = 0
    details_pending = False
    weight_result: dict[str, Any] = {}
    invalidated_plans = 0
    conn = get_connection()
    try:
        cur = conn.cursor()
        try:
            _assert_source_schema(cur)
            _assert_plan_invalidation_schema(cur)
            row = document_dict_from_bsale(
                selected,
                company_id=company_id,
                default_office_id=office_id,
            )
            if row is None:
                raise RuntimeError("El source activo no pudo mapearse a documents")
            upsert_documents(cur, [row])
            persisted_local_id = int(row["document_id"])
            if local_id is not None and persisted_local_id != local_id:
                raise RuntimeError(
                    f"PK local cambió inesperadamente: {local_id} -> {persisted_local_id}"
                )
            local_id = persisted_local_id
            bsale_total = _num(selected.get("totalAmount")) or 0.0
            details_pending = len(details) == 0 and bsale_total > 0
            if details_pending:
                # No borrar líneas locales con replace vacío; reintentar en el próximo ciclo.
                details_written = 0
                logger.error(
                    "header_ok_details_pending folio=%s local_document_id=%s "
                    "bsale_source_document_id=%s reason=empty_bsale_details_with_total",
                    resolved_folio,
                    local_id,
                    source_id,
                )
            else:
                details_written = replace_document_details(
                    cur,
                    local_id,
                    details,
                    invalidate_cache=False,
                )
                if len(details) > 0 and details_written == 0:
                    raise RuntimeError(
                        f"OC {resolved_folio}: Bsale entregó {len(details)} líneas "
                        "pero replace_document_details escribió 0; abortando commit parcial"
                    )
            # Día de entrega vive en document_attributes (OBSERVACIONES), no en documents.
            if attributes_payload is not None:
                attributes_written = replace_document_attributes(
                    cur,
                    local_id,
                    attributes_payload,
                )
            if references_payload is not None:
                references_written = replace_document_references(
                    cur,
                    local_id,
                    references_payload,
                )
            if details_pending:
                cur.execute(
                    """
                    UPDATE distribuidora.documents
                    SET source_document_id = %s,
                        source_hash = NULL,
                        source_updated_at = %s,
                        last_reconciliation_at = NOW(),
                        updated_at = NOW()
                    WHERE document_id = %s
                    """,
                    (source_id, source_updated_at(selected), local_id),
                )
                weight_result = {}
            else:
                cur.execute(
                    """
                    UPDATE distribuidora.documents
                    SET source_document_id = %s,
                        source_hash = %s,
                        source_updated_at = %s,
                        last_synced_at = NOW(),
                        last_reconciliation_at = NOW(),
                        updated_at = NOW()
                    WHERE document_id = %s
                    """,
                    (source_id, digest, source_updated_at(selected), local_id),
                )
                weight_result = recalculate_order_weight_in_transaction(
                    cur,
                    document_id=local_id,
                    company_id=company_id,
                    office_id=office_id,
                    user_email=user_email,
                    persist=True,
                )
            invalidated_plans = _invalidate_affected_dispatch_plans(cur, local_id)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
    finally:
        conn.close()

    if details_pending:
        report.update(
            {
                "status": "header_ok_details_pending",
                "wrote": True,
                "local_document_id": local_id,
                "details_replaced": 0,
                "attributes_replaced": attributes_written,
                "references_replaced": references_written,
                "related_rows_inserted": 0,
                "needs_detail_sync": True,
                "details_synced": False,
                "dispatch_plans_invalidated": invalidated_plans,
                "peso_despues_kg": None,
                "cobertura_despues": None,
            }
        )
        logger.warning(
            "reconcile_oc_incomplete folio=%s local_document_id=%s "
            "status=header_ok_details_pending bsale_source_document_id=%s",
            resolved_folio,
            local_id,
            source_id,
        )
        return report

    # Facturación confirmada depende de document_related (API relateddetailid).
    related_stats: dict[str, Any] = {}
    try:
        from backend.services.distribuidora.sync_related_service import (
            sync_related_for_single_oc,
        )

        related_stats = sync_related_for_single_oc(
            client,
            document_id=int(local_id),
            throttle=0.0,
        )
        related_inserts = int(related_stats.get("rows_inserted") or 0)
    except Exception as exc:
        logger.warning(
            "reconcile_oc_related_sync_failed folio=%s local_document_id=%s: %s",
            resolved_folio,
            local_id,
            exc,
        )
        related_stats = {"error": str(exc)}
        related_inserts = 0

    obs_after = (
        load_local_observaciones(int(local_id)) if local_id is not None else None
    )
    report.update(
        {
            "status": "synced",
            "wrote": True,
            "local_document_id": local_id,
            "details_replaced": details_written,
            "attributes_replaced": attributes_written,
            "references_replaced": references_written,
            "related_rows_inserted": related_inserts,
            "related_sync": related_stats,
            "needs_detail_sync": False,
            "details_synced": details_written > 0 or len(details) == 0,
            "peso_despues_kg": weight_result.get("peso_total_kg"),
            "cobertura_despues": weight_result.get("porcentaje_cobertura"),
            "dispatch_plans_invalidated": invalidated_plans,
            "invoice_link_after": (
                load_local_invoice_link_flags(int(local_id))
                if local_id is not None
                else {}
            ),
            "delivery_after": {
                "observaciones": obs_after,
                "day": detect_delivery_day_from_observation(obs_after),
            },
        }
    )
    logger.info(
        "reconcile_oc_done folio=%s local_document_id=%s "
        "bsale_source_document_id=%s details_replaced=%s attributes_replaced=%s "
        "related_inserted=%s peso_total_kg=%s",
        resolved_folio,
        local_id,
        source_id,
        details_written,
        attributes_written,
        related_inserts,
        weight_result.get("peso_total_kg"),
    )
    return report


def reconcile_one_oc(
    client: BsaleClient,
    *,
    folio: int | None = None,
    local_document_id: int | None = None,
    dry_run: bool = True,
    active_document: dict[str, Any] | None = None,
    user_email: str | None = None,
    company_id: int = COMPANY_ID,
    office_id: int = OFFICE_ID,
) -> dict[str, Any]:
    """Ejecuta read-only sin lock; toda escritura compite con syncs existentes."""
    kwargs = {
        "folio": folio,
        "local_document_id": local_document_id,
        "dry_run": dry_run,
        "active_document": active_document,
        "user_email": user_email,
        "company_id": int(company_id),
        "office_id": int(office_id),
    }
    if dry_run:
        return _reconcile_one_oc(client, **kwargs)
    with _oc_writer_locks():
        return _reconcile_one_oc(client, **kwargs)


def _parse_candidate_folio(raw_number: Any) -> int | None:
    """Devuelve folio entero positivo o None si es inválido/ausente."""
    if raw_number is None:
        return None
    if isinstance(raw_number, str) and not raw_number.strip():
        return None
    try:
        folio = int(raw_number)
    except (TypeError, ValueError):
        return None
    if folio <= 0:
        return None
    return folio


def _allocate_lane_slots(limit: int) -> tuple[int, int]:
    """Divide el cupo total: ~80% reciente, ~20% histórico."""
    total = max(1, int(limit))
    if total == 1:
        return 1, 0
    recent_slots = (total * 4) // 5
    if recent_slots < 1:
        recent_slots = 1
    historical_slots = total - recent_slots
    return recent_slots, historical_slots


def _merge_lane_candidates(
    *,
    recent_rows: list[dict[str, Any]],
    historical_rows: list[dict[str, Any]],
    recent_slots: int,
    historical_slots: int,
    total_limit: int,
    exclude_document_ids: set[int] | None = None,
) -> list[dict[str, Any]]:
    """Une carriles, deduplica por document_id y reasigna cupos sobrantes."""
    excluded = exclude_document_ids or set()
    selected: list[dict[str, Any]] = []
    seen: set[int] = set()

    recent_selected = 0
    for row in recent_rows:
        document_id = int(row["document_id"])
        if document_id in excluded or document_id in seen:
            continue
        item = dict(row)
        item["candidate_lane"] = "recent"
        selected.append(item)
        seen.add(document_id)
        recent_selected += 1
        if recent_selected >= recent_slots:
            break

    historical_limit = historical_slots + max(0, recent_slots - recent_selected)
    historical_selected = 0
    for row in historical_rows:
        document_id = int(row["document_id"])
        if document_id in excluded or document_id in seen:
            continue
        item = dict(row)
        item["candidate_lane"] = "historical"
        selected.append(item)
        seen.add(document_id)
        historical_selected += 1
        if historical_selected >= historical_limit:
            break

    return selected[: max(1, int(total_limit))]


_ELIGIBLE_OC_CTE = """
    WITH eligible AS (
        SELECT
            d.document_id,
            d.number,
            d.emission_date,
            d.generation_date,
            d.last_reconciliation_at,
            COALESCE(d.generation_date, d.emission_date) AS lane_date,
            EXTRACT(
                EPOCH FROM (
                    NOW() - COALESCE(
                        d.last_reconciliation_at,
                        d.generation_date,
                        d.emission_date,
                        d.created_at
                    )
                )
            ) AS seconds_since_review
        FROM distribuidora.documents d
        WHERE d.company_id = %s
          AND d.office_id = %s
          AND d.document_type_id = %s
          AND d.state = 0
          AND COALESCE(d.commercial_state, 0) = 0
          AND d.number IS NOT NULL
          AND d.number > 0
          AND NOT EXISTS (
              SELECT 1
              FROM distribuidora.document_details dd
              INNER JOIN distribuidora.document_related dr
                  ON dr.detail_id = dd.detail_id
              INNER JOIN distribuidora.documents invoice
                  ON invoice.document_id = dr.related_document_id
                 AND invoice.company_id = d.company_id
                 AND invoice.office_id = d.office_id
                 AND invoice.document_type_id IN (1, 6)
                 AND invoice.state = 0
              WHERE dd.document_id = d.document_id
          )
          AND (
              EXISTS (
                  SELECT 1
                  FROM distribuidora.document_details current_detail
                  WHERE current_detail.document_id = d.document_id
              )
              OR EXISTS (
                  SELECT 1
                  FROM distribuidora.dispatch_plan_orders dpo
                  INNER JOIN distribuidora.dispatch_plan dp
                      ON dp.id = dpo.dispatch_plan_id
                     AND dp.status <> 'dispatched'
                  WHERE dpo.oc_document_id = d.document_id
              )
          )
    )
"""

_RECENT_CANDIDATES_SQL = (
    _ELIGIBLE_OC_CTE
    + """
    SELECT
        document_id,
        number,
        emission_date,
        generation_date,
        last_reconciliation_at,
        seconds_since_review,
        MAX(seconds_since_review) OVER () AS max_seconds_since_review
    FROM eligible
    WHERE lane_date >= NOW() - make_interval(days => %s)
    ORDER BY
        last_reconciliation_at ASC NULLS FIRST,
        generation_date DESC NULLS LAST,
        document_id DESC
    LIMIT %s
"""
)

_HISTORICAL_CANDIDATES_SQL = (
    _ELIGIBLE_OC_CTE
    + """
    SELECT
        document_id,
        number,
        emission_date,
        generation_date,
        last_reconciliation_at,
        seconds_since_review,
        MAX(seconds_since_review) OVER () AS max_seconds_since_review
    FROM eligible
    WHERE lane_date IS NULL
       OR lane_date < NOW() - make_interval(days => %s)
    ORDER BY
        last_reconciliation_at ASC NULLS FIRST,
        document_id ASC
    LIMIT %s
"""
)

# Compatibilidad para tests/diagnóstico que referencian el SQL antiguo.
_FULL_COVERAGE_CANDIDATES_SQL = _RECENT_CANDIDATES_SQL


def _fetch_candidate_rows(cur, sql: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
    cur.execute(sql, params)
    columns = [description[0] for description in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


def _load_full_coverage_batch(
    *,
    limit: int,
    recent_days: int = 30,
    company_id: int = COMPANY_ID,
    office_id: int = OFFICE_ID,
    exclude_document_ids: set[int] | None = None,
) -> tuple[list[dict[str, Any]], float | None, dict[str, Any]]:
    """OCs abiertas/sin factura en carriles reciente + histórico."""
    excluded = exclude_document_ids or set()
    requested_limit = max(1, int(limit))
    recent_days = max(1, int(recent_days))
    recent_slots, historical_slots = _allocate_lane_slots(requested_limit)
    # Over-fetch para compensar exclusiones y relleno histórico.
    recent_fetch_limit = recent_slots + len(excluded)
    historical_fetch_limit = (
        historical_slots + recent_slots + len(excluded)
    )

    conn = get_connection()
    try:
        cur = conn.cursor()
        base_params = (
            int(company_id),
            int(office_id),
            OC_DOCUMENT_TYPE_ID,
            recent_days,
        )
        recent_rows = _fetch_candidate_rows(
            cur,
            _RECENT_CANDIDATES_SQL,
            (*base_params, recent_fetch_limit),
        )
        historical_rows = _fetch_candidate_rows(
            cur,
            _HISTORICAL_CANDIDATES_SQL,
            (*base_params, historical_fetch_limit),
        )
        cur.close()
    finally:
        conn.close()

    selected = _merge_lane_candidates(
        recent_rows=recent_rows,
        historical_rows=historical_rows,
        recent_slots=recent_slots,
        historical_slots=historical_slots,
        total_limit=requested_limit,
        exclude_document_ids=excluded,
    )
    ages = [
        _num(row.get("seconds_since_review"))
        for row in selected
        if _num(row.get("seconds_since_review")) is not None
    ]
    max_age = max(ages) if ages else None
    meta = {
        "recent_slots": recent_slots,
        "historical_slots": historical_slots,
        "recent_candidates_loaded": len(recent_rows),
        "historical_candidates_loaded": len(historical_rows),
        "recent_selected": sum(
            1 for row in selected if row.get("candidate_lane") == "recent"
        ),
        "historical_selected": sum(
            1 for row in selected if row.get("candidate_lane") == "historical"
        ),
    }
    logger.info(
        "recent_candidates_loaded=%s historical_candidates_loaded=%s "
        "recent_slots=%s historical_slots=%s recent_selected=%s "
        "historical_selected=%s",
        meta["recent_candidates_loaded"],
        meta["historical_candidates_loaded"],
        meta["recent_slots"],
        meta["historical_slots"],
        meta["recent_selected"],
        meta["historical_selected"],
    )
    return selected, max_age, meta


def _fetch_recent_oc_documents(
    client: BsaleClient,
    *,
    window_days: int,
    office_id: int = OFFICE_ID,
) -> list[dict[str, Any]]:
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=max(30, int(window_days)))
    offset = 0
    output: list[dict[str, Any]] = []
    while True:
        params = merge_bsale_office_query(
            {
                "documenttypeid": OC_DOCUMENT_TYPE_ID,
                "emissiondaterange": f"[{int(start.timestamp())},{int(now.timestamp())}]",
                "limit": PAGE_LIMIT,
                "offset": offset,
            },
            int(office_id),
            context="reconcile_recent_ocs",
        )
        payload = client.get("/documents.json", params)
        items = payload.get("items") if isinstance(payload, dict) else None
        if not isinstance(items, list):
            break
        output.extend(item for item in items if isinstance(item, dict))
        if len(items) < PAGE_LIMIT:
            break
        offset += len(items)
    return output


def _batch_summary_counts(results: list[dict[str, Any]]) -> dict[str, int]:
    skipped = sum(
        1
        for item in results
        if item.get("status") == "oc_skipped"
        and item.get("reason") == "invalid_or_missing_folio"
    )
    cancelled = sum(
        1
        for item in results
        if item.get("status") in {"cancelled", "already_cancelled", "dry_run_cancelled"}
    )
    errors = sum(
        1
        for item in results
        if item.get("status") in {"error", "source_not_found"}
    )
    updated = sum(1 for item in results if item.get("wrote"))
    unchanged = sum(
        1
        for item in results
        if item.get("status")
        in {"already_in_sync", "dry_run_in_sync", "already_cancelled"}
    )
    return {
        "checked": len(results),
        "updated": updated,
        "unchanged": unchanged,
        "skipped": skipped,
        "cancelled": cancelled,
        "errors": errors,
        "invalid_folios": skipped,
        "ocs_checked": len(results),
        "ocs_updated": updated,
        "ocs_unchanged": unchanged,
    }


def reconcile_open_purchase_orders_batch(
    client: BsaleClient,
    *,
    execute: bool,
    limit: int = 100,
    recent_days: int = 30,
    company_id: int = COMPANY_ID,
    office_id: int = OFFICE_ID,
) -> dict[str, Any]:
    """Lote acotado para Scheduled Task; continúa ante errores por OC."""
    started = time.perf_counter()
    batch_limit = max(1, int(limit))
    recent_days = max(1, int(recent_days))
    logger.info(
        "reconciliation_cycle_started execute=%s limit=%s recent_days=%s "
        "company_id=%s office_id=%s",
        execute,
        batch_limit,
        recent_days,
        company_id,
        office_id,
    )
    results: list[dict[str, Any]] = []
    max_unreviewed_age: float | None = None
    cycle_finished = False
    finished_summary: dict[str, Any] | None = None
    lock_conn = get_connection()
    got_lock = False
    try:
        lock_cur = lock_conn.cursor()
        lock_cur.execute(
            "SELECT pg_try_advisory_lock(%s)",
            (ADVISORY_LOCK_OC_RECONCILIATION,),
        )
        got_lock = bool(lock_cur.fetchone()[0])
        lock_conn.commit()
        lock_cur.close()
        if not got_lock:
            counts = _batch_summary_counts([])
            result = {
                "status": "skipped_due_to_active_sync",
                "execute": execute,
                "limit": batch_limit,
                "errors": 0,
                "results": [],
                "duration_seconds": round(time.perf_counter() - started, 3),
                **counts,
            }
            cycle_finished = True
            finished_summary = result
            return result

        candidates, max_unreviewed_age, lane_meta = _load_full_coverage_batch(
            limit=batch_limit,
            recent_days=recent_days,
            company_id=company_id,
            office_id=office_id,
        )
        recent_checked = 0
        historical_checked = 0
        for candidate in candidates:
            local_id = int(candidate["document_id"])
            raw_number = candidate.get("number")
            folio = _parse_candidate_folio(raw_number)
            candidate_lane = candidate.get("candidate_lane") or "unknown"
            if folio is None:
                logger.info(
                    "oc_skipped local_document_id=%s reason=invalid_or_missing_folio "
                    "raw_number=%r candidate_lane=%s",
                    local_id,
                    raw_number,
                    candidate_lane,
                )
                results.append(
                    {
                        "folio": None,
                        "local_document_id": local_id,
                        "status": "oc_skipped",
                        "reason": "invalid_or_missing_folio",
                        "raw_number": raw_number,
                        "candidate_lane": candidate_lane,
                        "wrote": False,
                    }
                )
                continue

            if candidate_lane == "recent":
                recent_checked += 1
            elif candidate_lane == "historical":
                historical_checked += 1

            logger.info(
                "oc_checked folio=%s local_document_id=%s candidate_lane=%s "
                "last_reconciliation_at=%s",
                folio,
                local_id,
                candidate_lane,
                candidate.get("last_reconciliation_at"),
            )
            try:
                result = reconcile_one_oc(
                    client,
                    folio=folio,
                    local_document_id=local_id,
                    dry_run=not execute,
                    company_id=company_id,
                    office_id=office_id,
                )
                result["candidate_lane"] = candidate_lane
                if result.get("source_changed"):
                    logger.info(
                        "oc_source_changed folio=%s local_document_id=%s "
                        "candidate_lane=%s previous_source_document_id=%s "
                        "current_bsale_source_document_id=%s",
                        folio,
                        local_id,
                        candidate_lane,
                        result.get("previous_source_document_id"),
                        result.get("current_bsale_source_document_id"),
                    )
                if result.get("wrote"):
                    logger.info(
                        "oc_updated folio=%s local_document_id=%s "
                        "candidate_lane=%s source_document_id=%s details_replaced=%s",
                        folio,
                        local_id,
                        candidate_lane,
                        result.get("current_bsale_source_document_id"),
                        result.get("details_replaced"),
                    )
                elif result.get("status") in {
                    "cancelled",
                    "already_cancelled",
                    "dry_run_cancelled",
                }:
                    logger.info(
                        "oc_cancelled_detected folio=%s local_document_id=%s "
                        "candidate_lane=%s previous_state=%s new_state=%s "
                        "dispatch_plans_invalidated=%s",
                        folio,
                        local_id,
                        candidate_lane,
                        result.get("previous_state"),
                        result.get("new_state"),
                        result.get("dispatch_plans_invalidated") or 0,
                    )
                elif result.get("status") in {
                    "already_in_sync",
                    "dry_run_in_sync",
                }:
                    logger.info(
                        "oc_unchanged folio=%s local_document_id=%s "
                        "candidate_lane=%s source_document_id=%s",
                        folio,
                        local_id,
                        candidate_lane,
                        result.get("current_bsale_source_document_id"),
                    )
                elif result.get("status") == "source_not_found":
                    if execute:
                        _mark_reconciliation_attempt(local_id, successful=False)
                    logger.error(
                        "oc_failed folio=%s local_document_id=%s "
                        "candidate_lane=%s error=source_not_found",
                        folio,
                        local_id,
                        candidate_lane,
                    )
            except ActiveSyncConflict as exc:
                logger.warning(
                    "reconciliation_cycle_skipped_due_to_active_sync "
                    "folio=%s local_document_id=%s candidate_lane=%s error=%s",
                    folio,
                    local_id,
                    candidate_lane,
                    exc,
                )
                counts = _batch_summary_counts(results)
                result = {
                    "status": "skipped_due_to_active_sync",
                    "execute": execute,
                    "limit": batch_limit,
                    "recent_days": recent_days,
                    "company_id": int(company_id),
                    "office_id": int(office_id),
                    "duration_seconds": round(time.perf_counter() - started, 3),
                    "results": results,
                    "recent_slots": lane_meta.get("recent_slots"),
                    "historical_slots": lane_meta.get("historical_slots"),
                    "recent_candidates_loaded": lane_meta.get(
                        "recent_candidates_loaded"
                    ),
                    "historical_candidates_loaded": lane_meta.get(
                        "historical_candidates_loaded"
                    ),
                    "recent_checked": recent_checked,
                    "historical_checked": historical_checked,
                    **counts,
                }
                cycle_finished = True
                finished_summary = result
                return result
            except Exception as exc:
                if _is_global_reconciliation_error(exc):
                    raise
                if execute:
                    try:
                        _mark_reconciliation_attempt(local_id, successful=False)
                    except Exception:
                        logger.exception(
                            "No se pudo avanzar cursor fallido document_id=%s",
                            local_id,
                        )
                logger.exception(
                    "oc_failed folio=%s local_document_id=%s candidate_lane=%s "
                    "error=%s",
                    folio,
                    local_id,
                    candidate_lane,
                    exc,
                )
                result = {
                    "folio": folio,
                    "local_document_id": local_id,
                    "status": "error",
                    "error": str(exc),
                    "candidate_lane": candidate_lane,
                    "wrote": False,
                }
            results.append(result)

        counts = _batch_summary_counts(results)
        result = {
            "status": "completed",
            "execute": execute,
            "limit": batch_limit,
            "recent_days": recent_days,
            "company_id": int(company_id),
            "office_id": int(office_id),
            "new_versions_detected": sum(
                1 for item in results if item.get("source_changed")
            ),
            "max_unreviewed_age_seconds": max_unreviewed_age,
            "duration_seconds": round(time.perf_counter() - started, 3),
            "results": results,
            "recent_slots": lane_meta.get("recent_slots"),
            "historical_slots": lane_meta.get("historical_slots"),
            "recent_candidates_loaded": lane_meta.get("recent_candidates_loaded"),
            "historical_candidates_loaded": lane_meta.get(
                "historical_candidates_loaded"
            ),
            "recent_checked": recent_checked,
            "historical_checked": historical_checked,
            **counts,
        }
        cycle_finished = True
        finished_summary = result
        return result
    finally:
        if got_lock:
            try:
                unlock_cur = lock_conn.cursor()
                unlock_cur.execute(
                    "SELECT pg_advisory_unlock(%s)",
                    (ADVISORY_LOCK_OC_RECONCILIATION,),
                )
                unlock_cur.close()
            except Exception:
                logger.exception("No se pudo liberar lock de reconciliación OC")
        lock_conn.close()
        # Solo al final real del ciclo (incluye saltos). No emitir si una
        # excepción no controlada aborta antes de terminar los candidatos.
        if cycle_finished and finished_summary is not None:
            logger.info(
                "reconciliation_cycle_finished execute=%s checked=%s updated=%s "
                "unchanged=%s skipped=%s errors=%s invalid_folios=%s "
                "recent_checked=%s historical_checked=%s "
                "recent_slots=%s historical_slots=%s duration_seconds=%.3f",
                execute,
                finished_summary.get("checked", 0),
                finished_summary.get("updated", 0),
                finished_summary.get("unchanged", 0),
                finished_summary.get("skipped", 0),
                finished_summary.get("errors", 0),
                finished_summary.get("invalid_folios", 0),
                finished_summary.get("recent_checked", 0),
                finished_summary.get("historical_checked", 0),
                finished_summary.get("recent_slots", 0),
                finished_summary.get("historical_slots", 0),
                finished_summary.get(
                    "duration_seconds",
                    time.perf_counter() - started,
                ),
            )


def reconcile_recent_ocs(
    client: BsaleClient,
    *,
    window_days: int = 30,
    full_coverage_limit: int = 100,
    dry_run: bool = False,
    company_id: int = COMPANY_ID,
    office_id: int = OFFICE_ID,
) -> dict[str, Any]:
    """Carril rápido reciente + lote rotativo de OCs abiertas sin factura."""
    started = time.perf_counter()
    lock_conn = get_connection()
    got_lock = False
    try:
        lock_cur = lock_conn.cursor()
        lock_cur.execute(
            "SELECT pg_try_advisory_lock(%s)",
            (ADVISORY_LOCK_OC_RECONCILIATION,),
        )
        got_lock = bool(lock_cur.fetchone()[0])
        lock_conn.commit()
        lock_cur.close()
        if not got_lock:
            return {
                "window_days": max(30, int(window_days)),
                "full_coverage_limit": max(1, int(full_coverage_limit)),
                "omitido_concurrencia": True,
                "duration_seconds": round(time.perf_counter() - started, 3),
                "results": [],
            }

        items = _fetch_recent_oc_documents(
            client,
            window_days=max(30, window_days),
            office_id=office_id,
        )
        by_folio: dict[int, list[dict[str, Any]]] = {}
        for item in items:
            number = summarize_bsale_document(
                item,
                expected_company_id=company_id,
            ).get("number")
            if number is None or int(number) <= 0:
                continue
            by_folio.setdefault(int(number), []).append(item)

        results: list[dict[str, Any]] = []
        reviewed_local_ids: set[int] = set()
        for folio, candidates in sorted(by_folio.items()):
            active, _ = select_active_oc_source(
                candidates,
                folio=folio,
                company_id=company_id,
                office_id=office_id,
            )
            if active is None:
                continue
            try:
                result = reconcile_one_oc(
                    client,
                    folio=folio,
                    dry_run=dry_run,
                    active_document=active,
                    company_id=company_id,
                    office_id=office_id,
                )
            except Exception as exc:
                logger.exception("Reconcile OC folio=%s falló", folio)
                results.append(
                    {
                        "folio": folio,
                        "status": "error",
                        "error": str(exc),
                        "wrote": False,
                        "lane": "fast_recent",
                    }
                )
            else:
                result["lane"] = "fast_recent"
                results.append(result)
                if result.get("local_document_id") is not None:
                    reviewed_local_ids.add(int(result["local_document_id"]))

        full_candidates, max_unreviewed_age, _lane_meta = _load_full_coverage_batch(
            limit=max(1, int(full_coverage_limit)),
            recent_days=max(30, int(window_days)),
            company_id=company_id,
            office_id=office_id,
            exclude_document_ids=reviewed_local_ids,
        )
        full_results: list[dict[str, Any]] = []
        for candidate in full_candidates:
            local_id = int(candidate["document_id"])
            raw_number = candidate.get("number")
            folio = _parse_candidate_folio(raw_number)
            if folio is None:
                logger.info(
                    "oc_skipped local_document_id=%s reason=invalid_or_missing_folio "
                    "raw_number=%r",
                    local_id,
                    raw_number,
                )
                full_results.append(
                    {
                        "folio": None,
                        "local_document_id": local_id,
                        "status": "oc_skipped",
                        "reason": "invalid_or_missing_folio",
                        "raw_number": raw_number,
                        "wrote": False,
                        "lane": "full_open_uninvoiced",
                    }
                )
                continue
            try:
                result = reconcile_one_oc(
                    client,
                    folio=folio,
                    local_document_id=local_id,
                    dry_run=dry_run,
                    company_id=company_id,
                    office_id=office_id,
                )
                if result.get("status") == "source_not_found" and not dry_run:
                    _mark_reconciliation_attempt(local_id, successful=False)
            except Exception as exc:
                logger.exception(
                    "Reconcile cobertura completa OC folio=%s document_id=%s falló",
                    folio,
                    local_id,
                )
                if not dry_run:
                    try:
                        _mark_reconciliation_attempt(local_id, successful=False)
                    except Exception:
                        logger.exception(
                            "No se pudo avanzar cursor fallido document_id=%s",
                            local_id,
                        )
                result = {
                    "folio": folio,
                    "local_document_id": local_id,
                    "status": "error",
                    "error": str(exc),
                    "wrote": False,
                }
            result["lane"] = "full_open_uninvoiced"
            result["seconds_since_previous_review"] = _num(
                candidate.get("seconds_since_review")
            )
            results.append(result)
            full_results.append(result)

        errors = sum(
            1
            for item in results
            if item.get("status") in {"error", "source_not_found"}
        )
        modified = sum(1 for item in results if item.get("wrote"))
        new_versions = sum(1 for item in results if item.get("source_changed"))
        unchanged = sum(
            1 for item in results if item.get("status") == "already_in_sync"
        )
        return {
            "window_days": max(30, int(window_days)),
            "full_coverage_limit": max(1, int(full_coverage_limit)),
            "api_documents": len(items),
            "folios": len(by_folio),
            "ocs_reviewed": len(results),
            "new_versions_detected": new_versions,
            "ocs_modified": modified,
            "bsale_errors": errors,
            "duration_seconds": round(time.perf_counter() - started, 3),
            "max_unreviewed_age_seconds": max_unreviewed_age,
            "fast_lane": {
                "candidates": len(by_folio),
                "reviewed": sum(
                    1 for item in results if item.get("lane") == "fast_recent"
                ),
            },
            "full_coverage_lane": {
                "candidates": len(full_candidates),
                "reviewed": len(full_results),
            },
            # Compatibilidad con consumidores/alertas actuales.
            "synced": modified,
            "unchanged": unchanged,
            "errors": errors,
            "omitido_concurrencia": False,
            "results": results,
        }
    finally:
        if got_lock:
            try:
                unlock_cur = lock_conn.cursor()
                unlock_cur.execute(
                    "SELECT pg_advisory_unlock(%s)",
                    (ADVISORY_LOCK_OC_RECONCILIATION,),
                )
                unlock_cur.close()
            except Exception:
                logger.exception("No se pudo liberar lock de reconciliación OC")
        lock_conn.close()
