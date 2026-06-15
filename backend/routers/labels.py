"""Generador de etiquetas sucursales: lookup por barcode + empresa + lista de precios."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from backend.db import get_connection

router = APIRouter(tags=["labels"])

_LABEL_PRODUCT_SQL = """
SELECT
    v.bsale_id AS variant_id,
    BTRIM(v.bar_code) AS barcode,
    v.code AS sku,
    p.name AS product_name,
    v.description AS variant_name,
    pt.name AS product_type,
    COALESCE(vp.price_gross, vp.price_net)::numeric AS price,
    vp.price_list_id,
    pl.name AS price_list_name
FROM bsale.variants v
INNER JOIN bsale.products p
    ON p.company_id = v.company_id
   AND p.bsale_id = v.product_id
LEFT JOIN bsale.product_types pt
    ON pt.company_id = p.company_id
   AND pt.bsale_id = p.product_type_id
LEFT JOIN bsale.variant_prices vp
    ON vp.company_id = v.company_id
   AND vp.variant_id = v.bsale_id
   AND vp.price_list_id = %s
LEFT JOIN bsale.price_lists pl
    ON pl.company_id = vp.company_id
   AND pl.bsale_id = vp.price_list_id
WHERE v.company_id = %s
  AND BTRIM(v.bar_code) = BTRIM(%s)
ORDER BY v.bsale_id
LIMIT 1
"""


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _serialize_label_row(row: tuple) -> dict[str, Any]:
    (
        variant_id,
        barcode,
        sku,
        product_name,
        variant_name,
        product_type,
        price,
        price_list_id,
        price_list_name,
    ) = row
    pname = (product_name or "").strip()
    vname = (variant_name or "").strip()
    display = pname
    if vname and vname.lower() not in pname.lower():
        display = f"{pname} {vname}".strip()
    return {
        "variant_id": int(variant_id),
        "barcode": (barcode or "").strip(),
        "sku": (sku or "").strip() or None,
        "product_name": pname,
        "variant_name": vname or None,
        "product_type": (product_type or "").strip() or None,
        "display_name": display or pname or vname,
        "price": _to_float(price),
        "price_list_id": int(price_list_id) if price_list_id is not None else None,
        "price_list_name": (price_list_name or "").strip() or None,
    }


def _fetch_label_product(
    cur: Any,
    *,
    company_id: int,
    price_list_id: int,
    barcode: str,
) -> dict[str, Any] | None:
    bc = (barcode or "").strip()
    if not bc:
        return None
    cur.execute(
        _LABEL_PRODUCT_SQL,
        (price_list_id, company_id, bc),
    )
    row = cur.fetchone()
    if not row:
        return None
    return _serialize_label_row(row)


class LabelResolveItemIn(BaseModel):
    barcode: str = Field(..., min_length=1, max_length=50)
    quantity: int = Field(1, ge=1, le=9999)


class LabelResolveBody(BaseModel):
    company_id: int = Field(..., ge=1)
    price_list_id: int = Field(..., ge=1)
    items: list[LabelResolveItemIn] = Field(..., min_length=1, max_length=500)


@router.get("/labels/product")
def get_label_product(
    company_id: int = Query(..., ge=1),
    price_list_id: int = Query(..., ge=1),
    barcode: str = Query(..., min_length=1, max_length=50),
) -> dict[str, Any]:
    """Resuelve un producto por código de barras para etiquetas de sucursal."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        product = _fetch_label_product(
            cur,
            company_id=company_id,
            price_list_id=price_list_id,
            barcode=barcode,
        )
        cur.close()
    finally:
        conn.close()

    if product is None:
        raise HTTPException(status_code=404, detail="Producto no encontrado")
    return product


@router.post("/labels/resolve")
def resolve_label_products(body: LabelResolveBody) -> dict[str, Any]:
    """Resuelve lote de barcodes (Excel / cola de impresión)."""
    conn = get_connection()
    resolved: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    try:
        cur = conn.cursor()
        for idx, item in enumerate(body.items):
            product = _fetch_label_product(
                cur,
                company_id=body.company_id,
                price_list_id=body.price_list_id,
                barcode=item.barcode,
            )
            if product is None:
                errors.append(
                    {
                        "line": idx,
                        "barcode": item.barcode.strip(),
                        "error": "Producto no encontrado",
                    }
                )
                continue
            resolved.append({**product, "quantity": item.quantity})
        cur.close()
    finally:
        conn.close()

    return {"resolved": resolved, "errors": errors}
