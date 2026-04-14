"""Líneas ``distribuidora.document_details`` (reemplazo por documento)."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from psycopg2.extras import Json, execute_values


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


def replace_document_details(cur, document_id: int, items: list[dict[str, Any]]) -> int:
    cur.execute(
        "DELETE FROM distribuidora.document_details WHERE document_id = %s",
        (document_id,),
    )
    if not items:
        return 0
    rows: list[dict[str, Any]] = []
    for it in items:
        try:
            rows.append(detail_dict_from_item(document_id, it))
        except (KeyError, TypeError, ValueError):
            continue
    if not rows:
        return 0
    cols = list(rows[0].keys())
    sql = f"""
        INSERT INTO distribuidora.document_details (
            {", ".join(cols)}, created_at, updated_at
        ) VALUES %s
    """
    template = "(" + ",".join(["%s"] * len(cols)) + ",NOW(),NOW())"
    values = [tuple(r[c] for c in cols) for r in rows]
    execute_values(cur, sql, values, template=template, page_size=len(values))
    return len(rows)
