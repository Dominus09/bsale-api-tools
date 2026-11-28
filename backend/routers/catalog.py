from decimal import Decimal
import logging
import time

from fastapi import APIRouter, HTTPException, Query

from backend.db import get_connection
from backend.services.catalog_v2.source_flag import resolve_catalog_source
from backend.utils.sale_quantity import build_commercial_rules

logger = logging.getLogger(__name__)

router = APIRouter()

# factura → lista 13 (Ruta/Web), melinka → lista 16. La 14 (COMODITI) está descontinuada.
_ALLOWED_PRICE_LISTS = frozenset({"factura", "melinka"})
_DISCONTINUED_PRICE_LISTS = frozenset({"comoditi", "14"})
_CATALOG_COMPANY_ID = 3

_CATALOG_QUERY = """
WITH pm_by_barcode AS (
    SELECT DISTINCT ON (BTRIM(barcode))
        BTRIM(barcode) AS barcode_key, id, units_per_box, sale_type, quantity_step
    FROM bsale.products_master
    WHERE NULLIF(BTRIM(barcode), '') IS NOT NULL
    ORDER BY BTRIM(barcode), updated_at DESC NULLS LAST, id DESC
), pm_by_variant AS (
    SELECT DISTINCT ON (variant_id)
        variant_id, units_per_box, sale_type, quantity_step
    FROM bsale.products_master
    WHERE variant_id IS NOT NULL
    ORDER BY variant_id, updated_at DESC NULLS LAST, id DESC
)
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
        WHEN %s = 'melinka' THEN cv.price_16
    END AS price,
    v.description AS variant_description,
    COALESCE(
        NULLIF(v.units_per_box, 0),
        NULLIF(COALESCE(pm_bar.units_per_box, pm_var.units_per_box), 0),
        (regexp_match(UPPER(COALESCE(v.description, '')),
                      'SEC[[:space:]]*([0-9]+)'))[1]::integer
    ) AS units_per_box,
    COALESCE(pm_bar.sale_type, pm_var.sale_type) AS pm_sale_type,
    COALESCE(pm_bar.quantity_step, pm_var.quantity_step) AS pm_quantity_step
FROM bsale.catalog_view cv
LEFT JOIN bsale.variants v
    ON v.company_id = %s
   AND v.bsale_id = cv.variant_id
LEFT JOIN pm_by_barcode pm_bar
    ON pm_bar.barcode_key = NULLIF(BTRIM(cv.bar_code), '')
LEFT JOIN pm_by_variant pm_var
    ON pm_bar.id IS NULL
   AND pm_var.variant_id = cv.variant_id
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

    if key in _DISCONTINUED_PRICE_LISTS:
        raise HTTPException(
            status_code=400,
            detail="La lista COMODITI está descontinuada",
        )

    if key not in _ALLOWED_PRICE_LISTS:
        raise HTTPException(
            status_code=400,
            detail="price_list debe ser factura o melinka",
        )

    # Feature flag reversible por consumidor. Default legacy.
    # Picking/OC/order_pricing NUNCA pasan por este handler con V2 global.
    # Si V2 está activo y falla: NO caer a legacy (evitar stock/precio obsoleto).
    source = resolve_catalog_source(consumer="public_web")
    if source == "v2":
        try:
            from backend.services.catalog_v2.projection import public_items_from_v2

            items = public_items_from_v2(key, in_stock=in_stock)
            logger.info(
                "catalog_source=v2 consumer=public_web price_list=%s count=%s",
                key,
                len(items),
            )
            return items
        except Exception as e:
            logger.exception(
                "catalog_v2_unavailable_no_legacy_fallback price_list=%s err=%s",
                key,
                e,
            )
            raise HTTPException(
                status_code=503,
                detail={
                    "message": (
                        "Catálogo temporalmente no disponible. "
                        "Stock y precios no confirmados; intente más tarde."
                    ),
                    "code": "catalog_v2_unavailable",
                    "commercial_status": "unavailable",
                    "price_status": "unconfirmed",
                    "stock_status": "unconfirmed",
                    "rollback_es": (
                        "Rollback manual: CATALOG_SOURCE_PUBLIC=legacy "
                        "(no hay fallback silencioso a catalog_view)."
                    ),
                },
            ) from e

    query = _CATALOG_QUERY.strip()

    if in_stock is True:
        query = query.replace(
            "AND pm_var.variant_id = cv.variant_id\nORDER BY",
            "AND pm_var.variant_id = cv.variant_id\nWHERE COALESCE(cv.stock, 0) > 0\nORDER BY",
            1,
        )

    started = time.perf_counter()
    conn = get_connection()

    try:
        cur = conn.cursor()
        cur.execute(
            query,
            (key, key, _CATALOG_COMPANY_ID),
        )
        rows = cur.fetchall()
        cur.close()
    finally:
        conn.close()

    db_ms = (time.perf_counter() - started) * 1000
    map_started = time.perf_counter()
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

    logger.info(
        "catalog_source=legacy consumer=public_web price_list=%s in_stock=%s count=%s db_ms=%.1f map_ms=%.1f",
        key, in_stock is True, len(out), db_ms, (time.perf_counter() - map_started) * 1000,
    )
    return out
