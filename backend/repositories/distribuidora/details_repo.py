"""Líneas ``distribuidora.document_details`` (reemplazo por documento)."""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

from psycopg2.extras import Json, execute_values

from backend.repositories.distribuidora.schema_preconditions import require_reissue_schema

logger = logging.getLogger(__name__)


def _safe_int(v: Any) -> int | None:
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _num(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, (int, float, Decimal)):
        return v
    try:
        return Decimal(str(v))
    except Exception:
        return None


def detail_dict_from_item(document_id: int, item: dict[str, Any]) -> dict[str, Any]:
    variant = item.get("variant") or {}
    return {
        "detail_id": int(item["id"]),
        "document_id": document_id,
        "line_number": item.get("lineNumber"),
        "variant_id": int(variant["id"]) if variant.get("id") is not None else None,
        "variant_description": variant.get("description"),
        "variant_code": variant.get("code"),
        "quantity": _num(item.get("quantity")),
        "net_unit_value": _num(item.get("netUnitValue")),
        "total_unit_value": _num(item.get("totalUnitValue")),
        "net_amount": _num(item.get("netAmount")),
        "tax_amount": _num(item.get("taxAmount")),
        "total_amount": _num(item.get("totalAmount")),
        "net_discount": _num(item.get("netDiscount")),
        "total_discount": _num(item.get("totalDiscount")),
        "discount_percentage": _num(item.get("discountPercentage")),
        "related_detail_id": _safe_int(item.get("relatedDetailId")),
        "note": item.get("note"),
        "raw_data": Json(item),
    }


# Columnas copiadas a ``document_detail_history`` (mismo orden en INSERT y SELECT).
_ARCHIVED_DETAIL_COLUMNS = (
    "detail_id",
    "document_id",
    "line_number",
    "variant_id",
    "variant_description",
    "variant_code",
    "quantity",
    "net_unit_value",
    "total_unit_value",
    "net_amount",
    "tax_amount",
    "total_amount",
    "net_discount",
    "total_discount",
    "discount_percentage",
    "related_detail_id",
    "note",
    "raw_data",
    "created_at",
    "updated_at",
)


def _parse_detail_rows(document_id: int, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[int] = set()
    for it in items:
        try:
            r = detail_dict_from_item(document_id, it)
        except (KeyError, TypeError, ValueError):
            continue
        if r["detail_id"] in seen:
            continue
        seen.add(r["detail_id"])
        rows.append(r)
    return rows


def _archive_and_upsert(
    cur,
    document_id: int,
    rows: list[dict[str, Any]],
    superseded_by_source_document_id: int | None,
) -> list[int]:
    """Líneas vigentes = ``rows``; las que dejan de estarlo pasan a historial. Retorna ids archivados."""
    cur.execute(
        """
        SELECT detail_id
        FROM distribuidora.document_details
        WHERE document_id = %s
        FOR UPDATE
        """,
        (document_id,),
    )
    existing = {int(r[0]) for r in (cur.fetchall() or [])}
    new_ids = [int(r["detail_id"]) for r in rows]
    removed = sorted(existing - set(new_ids))

    if removed:
        cols = ", ".join(_ARCHIVED_DETAIL_COLUMNS)
        refresh = ", ".join(
            f"{c} = EXCLUDED.{c}" for c in _ARCHIVED_DETAIL_COLUMNS if c != "detail_id"
        )
        cur.execute(
            f"""
            INSERT INTO distribuidora.document_detail_history (
                {cols}, superseded_at, superseded_by_source_document_id
            )
            SELECT {cols}, NOW(), %s
            FROM distribuidora.document_details
            WHERE document_id = %s AND detail_id = ANY(%s)
            ON CONFLICT (detail_id) DO UPDATE SET
                {refresh},
                superseded_at = EXCLUDED.superseded_at,
                superseded_by_source_document_id = EXCLUDED.superseded_by_source_document_id
            """,
            (superseded_by_source_document_id, document_id, removed),
        )
        cur.execute(
            """
            DELETE FROM distribuidora.document_details
            WHERE document_id = %s AND detail_id = ANY(%s)
            """,
            (document_id, removed),
        )

    if not rows:
        return removed

    cur.execute(
        """
        DELETE FROM distribuidora.document_detail_history
        WHERE document_id = %s AND detail_id = ANY(%s)
        """,
        (document_id, new_ids),
    )
    cols_list = list(rows[0].keys())
    update_set = ", ".join(
        f"{c} = EXCLUDED.{c}" for c in cols_list if c not in ("detail_id", "document_id")
    )
    sql = f"""
        INSERT INTO distribuidora.document_details (
            {", ".join(cols_list)}, created_at, updated_at
        ) VALUES %s
        ON CONFLICT (detail_id) DO UPDATE SET
            {update_set},
            updated_at = NOW()
        WHERE distribuidora.document_details.document_id = EXCLUDED.document_id
        RETURNING detail_id
    """
    template = "(" + ",".join(["%s"] * len(cols_list)) + ",NOW(),NOW())"
    values = [tuple(r[c] for c in cols_list) for r in rows]
    returned = execute_values(
        cur, sql, values, template=template, page_size=len(values), fetch=True
    )
    written = {int(r[0]) for r in (returned or [])}
    foreign = sorted(set(new_ids) - written)
    if foreign:
        raise ValueError(
            f"detail_id ya pertenecen a otro documento local (document_id={document_id}): {foreign}"
        )
    return removed


def replace_document_details(
    cur,
    document_id: int,
    items: list[dict[str, Any]],
    *,
    invalidate_cache: bool = True,
    superseded_by_source_document_id: int | None = None,
) -> int:
    """
    Deja en ``document_details`` exactamente las líneas de ``items`` (revisión vigente).

    Las líneas que desaparecen se mueven a ``document_detail_history`` en vez de
    borrarse: ``document_related`` que apunta a ellas sobrevive y se resuelve al mismo
    ``document_id`` vía ``v_document_related_resolved``. Una línea que reaparece
    vuelve a ser vigente y sale del historial.

    Sin el schema de 048 lanza ``SchemaPreconditionError`` (no hay modo DELETE+INSERT).
    """
    require_reissue_schema(cur)
    rows = _parse_detail_rows(document_id, items)
    archived = _archive_and_upsert(cur, document_id, rows, superseded_by_source_document_id)
    if archived:
        logger.info(
            "document_details archived document_id=%s superseded_by_source_document_id=%s "
            "archived_detail_ids=%s current=%s",
            document_id,
            superseded_by_source_document_id,
            archived,
            len(rows),
        )
    written = len(rows)
    if written and invalidate_cache:
        try:
            from backend.services.order_weight_service import invalidate_order_weight_cache

            invalidate_order_weight_cache(int(document_id))
        except Exception:
            pass
    return written
