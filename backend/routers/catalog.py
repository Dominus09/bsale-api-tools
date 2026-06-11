from decimal import Decimal

from fastapi import APIRouter, HTTPException, Query

from backend.db import get_connection
from backend.utils.sale_quantity import build_commercial_rules

router = APIRouter()

_ALLOWED_PRICE_LISTS = frozenset({"factura", "comoditi", "melinka"})
_CATALOG_COMPANY_ID = 3

_CATALOG_QUERY = """
SELECT
    cv.variant_id AS id,
    TRIM(cv.product || ' ' || COALESCE(cv.variant, '')) AS name,
    cv.product_type AS type,
    cv.product,
    cv.variant,
    cv.bar_code AS barcode,
    COALESCE(cv.stock, 0) AS stock,
    cv.image_url AS image,
    CASE
        WHEN %s = 'factura' THEN cv.price_13
        WHEN %s = 'comoditi' THEN cv.price_14
        WHEN %s = 'melinka' THEN cv.price_16
    END AS price,
    v.description AS variant_description,
    COALESCE(
        NULLIF(v.units_per_box, 0),
        NULLIF(pm.units_per_box, 0),
        (regexp_match(UPPER(COALESCE(v.description, '')),
                      'SEC[[:space:]]*([0-9]+)'))[1]::integer
    ) AS units_per_box,
    pm.sale_type AS pm_sale_type,
    pm.quantity_step AS pm_quantity_step
FROM bsale.catalog_view cv
LEFT JOIN bsale.variants v
    ON v.company_id = %s
   AND v.bsale_id = cv.variant_id
LEFT JOIN LATERAL (
    SELECT pm_inner.*
    FROM bsale.products_master pm_inner
    WHERE (
        NULLIF(BTRIM(cv.bar_code), '') IS NOT NULL
        AND BTRIM(pm_inner.barcode) = BTRIM(cv.bar_code)
    )
    OR pm_inner.variant_id = cv.variant_id
    ORDER BY
        CASE
            WHEN NULLIF(BTRIM(cv.bar_code), '') IS NOT NULL
             AND BTRIM(pm_inner.barcode) = BTRIM(cv.bar_code)
                THEN 0
            WHEN pm_inner.variant_id = cv.variant_id
                THEN 1
            ELSE 2
        END,
        pm_inner.updated_at DESC NULLS LAST,
        pm_inner.id DESC
    LIMIT 1
) pm ON TRUE
ORDER BY
    cv.product_type ASC,
    cv.product ASC,
    cv.variant ASC
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
            "ORDER BY",
            "WHERE COALESCE(cv.stock, 0) > 0\nORDER BY",
        )

    conn = get_connection()

    try:
        cur = conn.cursor()
        cur.execute(
            query,
            (key, key, key, _CATALOG_COMPANY_ID),
        )
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
            variant_description,
            units_per_box_raw,
            pm_sale_type,
            pm_quantity_step,
        ) = r
        image_out = (str(image_val).strip() if image_val is not None else "") or None
        if image_out == "":
            image_out = None

        rules = build_commercial_rules(
            variant_id=int(_id) if _id is not None else 0,
            product_name=name,
            barcode=barcode,
            units_per_box=units_per_box_raw,
            pm_sale_type=pm_sale_type,
            pm_quantity_step=pm_quantity_step,
            variant_description=variant_description,
        )

        out.append(
            {
                "id": int(_id) if _id is not None else 0,
                "name": _clean_name(name),
                "type": "" if type_val is None else str(type_val).strip(),
                "barcode": "" if barcode is None else str(barcode).strip(),
                "price": _to_float(price_raw),
                "stock": int(stock_raw) if stock_raw is not None else 0,
                "image": image_out,
                "units_per_box": rules.units_per_box,
                "sale_type": rules.sale_type,
                "quantity_step": rules.quantity_step,
            }
        )

    return out
