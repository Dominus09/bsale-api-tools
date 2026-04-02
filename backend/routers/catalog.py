from decimal import Decimal

from fastapi import APIRouter, HTTPException, Query

from backend.db import get_connection

router = APIRouter()

_ALLOWED_PRICE_LISTS = frozenset({"factura", "comoditi", "melinka"})

_CATALOG_QUERY = """
SELECT
    variant_id AS id,
    TRIM(product || ' ' || COALESCE(variant, '')) AS name,
    bar_code AS barcode,
    stock,
    CASE
        WHEN %s = 'factura' THEN price_13
        WHEN %s = 'comoditi' THEN price_14
        WHEN %s = 'melinka' THEN price_16
    END AS price
FROM bsale.catalog_view
"""


def _to_float(value) -> float:
    if value is None:
        return 0.0
    if isinstance(value, Decimal):
        return float(value)
    return float(value)


@router.get("/catalog")
def get_catalog(
    price_list: str,
    in_stock: bool | None = Query(default=None),
):
    key = (price_list or "").strip().lower()
    if not key:
        raise HTTPException(
            status_code=400,
            detail="price_list es obligatorio",
        )
    if key not in _ALLOWED_PRICE_LISTS:
        raise HTTPException(
            status_code=400,
            detail="price_list debe ser factura, comoditi o melinka",
        )

    query = _CATALOG_QUERY.strip()
    if in_stock is True:
        query += "\nWHERE COALESCE(stock, 0) > 0"

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(query, (key, key, key))
        rows = cur.fetchall()
        cur.close()
    finally:
        conn.close()

    out = []
    for r in rows:
        _id, name, barcode, stock_raw, price_raw = r
        stock = 0 if stock_raw is None else int(stock_raw)
        price_val = _to_float(price_raw)
        out.append(
            {
                "id": _id,
                "name": (name or "").strip(),
                "barcode": barcode,
                "price": price_val,
                "stock": stock,
            }
        )
    return out
