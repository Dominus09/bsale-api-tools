from decimal import Decimal

from fastapi import APIRouter, HTTPException, Query

from backend.db import get_connection

router = APIRouter()

_ALLOWED_PRICE_LISTS = frozenset({"factura", "comoditi", "melinka"})

_CATALOG_QUERY = """
SELECT
    variant_id AS id,
    TRIM(product || ' ' || COALESCE(variant, '')) AS name,
    product_type AS type,
    product,
    variant,
    bar_code AS barcode,
    COALESCE(stock, 0) AS stock,
    image_url AS image,
    CASE
        WHEN %s = 'factura' THEN price_13
        WHEN %s = 'comoditi' THEN price_14
        WHEN %s = 'melinka' THEN price_16
    END AS price
FROM bsale.catalog_view
ORDER BY
    product_type ASC,
    product ASC,
    variant ASC
"""


def _to_float(value) -> float:
    if value is None:
        return 0.0
    if isinstance(value, Decimal):
        return float(value)
    return float(value)


def _clean_name(name: str | None) -> str:
    return " ".join((name or "").split())


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
        query = query.replace(
            "FROM bsale.catalog_view",
            "FROM bsale.catalog_view WHERE COALESCE(stock, 0) > 0",
        )

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
        (
            _id,
            name,
            type_val,
            _product,
            _variant,
            barcode,
            stock_raw,
            image_val,
            price_raw,
        ) = r

         # TEMP: quitar tras validar product_type en producción

        image_out = (str(image_val).strip() if image_val is not None else "") or None
        if image_out == "":
            image_out = None

        out.append(
            {
                "id": int(_id) if _id is not None else 0,
                "name": _clean_name(name),
                "type": "" if type_val is None else str(type_val).strip(),
                "barcode": "" if barcode is None else str(barcode).strip(),
                "price": _to_float(price_raw),
                "stock": int(stock_raw) if stock_raw is not None else 0,
                "image": image_out,
            }
        )

    return out
