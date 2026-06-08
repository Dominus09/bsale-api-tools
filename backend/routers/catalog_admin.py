"""Panel administrativo de calidad del catálogo web (solo RUTs en CATALOG_ADMIN_RUTS)."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from backend.db import get_connection
from backend.utils.catalog_admin_rut import require_catalog_admin_rut
from backend.utils.sale_quantity import build_commercial_rules, extract_sec_from_text

router = APIRouter()

_CATALOG_COMPANY_ID = 3

_HEALTH_SUMMARY_SQL = """
WITH base AS (
    SELECT
        cv.variant_id,
        cv.bar_code AS barcode,
        cv.product,
        cv.variant,
        cv.image_url,
        v.description AS variant_description,
        COALESCE(
            NULLIF(v.units_per_box, 0),
            NULLIF(pm.units_per_box, 0),
            (regexp_match(UPPER(COALESCE(v.description, '')),
                          'SEC[[:space:]]*([0-9]+)'))[1]::integer
        ) AS units_per_box,
        pm.sale_type,
        pm.quantity_step
    FROM bsale.catalog_view cv
    LEFT JOIN bsale.variants v
        ON v.company_id = %s
       AND v.bsale_id = cv.variant_id
    LEFT JOIN bsale.products_master pm
        ON pm.variant_id = cv.variant_id
        OR (
            NULLIF(BTRIM(cv.bar_code), '') IS NOT NULL
            AND pm.barcode = BTRIM(cv.bar_code)
        )
)
SELECT
    COUNT(*)::bigint AS total,
    COUNT(*) FILTER (
        WHERE image_url IS NULL OR BTRIM(COALESCE(image_url::text, '')) = ''
    )::bigint AS sin_foto,
    COUNT(*) FILTER (
        WHERE units_per_box IS NULL OR units_per_box <= 0
    )::bigint AS sin_sec,
    COUNT(*) FILTER (
        WHERE units_per_box > 0
          AND (sale_type IS NULL OR BTRIM(sale_type) = '')
    )::bigint AS sin_tipo_venta,
    COUNT(*) FILTER (
        WHERE units_per_box > 0
          AND sale_type = 'PARCIAL'
          AND (quantity_step IS NULL OR quantity_step <= 0)
    )::bigint AS sec_sin_step,
    COUNT(*) FILTER (
        WHERE (units_per_box IS NULL OR units_per_box <= 0)
          AND (sale_type IS NULL OR sale_type = 'UNITARIO')
    )::bigint AS unitario_por_falta_sec
FROM base
"""

_HEALTH_DETAIL_SQL = """
SELECT
    cv.variant_id,
    BTRIM(cv.bar_code) AS barcode,
    TRIM(cv.product || ' ' || COALESCE(cv.variant, '')) AS product_name,
    cv.image_url,
    v.description AS variant_description,
    COALESCE(
        NULLIF(v.units_per_box, 0),
        NULLIF(pm.units_per_box, 0),
        (regexp_match(UPPER(COALESCE(v.description, '')),
                      'SEC[[:space:]]*([0-9]+)'))[1]::integer
    ) AS units_per_box,
    pm.sale_type,
    pm.quantity_step
FROM bsale.catalog_view cv
LEFT JOIN bsale.variants v
    ON v.company_id = %s
   AND v.bsale_id = cv.variant_id
LEFT JOIN bsale.products_master pm
    ON pm.variant_id = cv.variant_id
    OR (
        NULLIF(BTRIM(cv.bar_code), '') IS NOT NULL
        AND pm.barcode = BTRIM(cv.bar_code)
    )
ORDER BY product_name ASC NULLS LAST, cv.variant_id ASC
LIMIT %s OFFSET %s
"""


def _has_photo(image_url: Any) -> bool:
    return bool(image_url and str(image_url).strip())


def _catalog_row_status(rules, has_photo: bool) -> tuple[str, str]:
    """Retorna (status_key, status_label)."""
    if not has_photo:
        return "critico", "Falta información crítica"
    if rules.auto_unitario_no_sec:
        return "advertencia", "Advertencia"
    if rules.missing_sale_type or rules.missing_quantity_step:
        return "incompleto", "Incompleto"
    return "completo", "Completo"


@router.get("/catalog/admin/health-summary")
def catalog_health_summary(
    rut: str = Query(..., description="RUT del administrador de catálogo"),
) -> dict[str, Any]:
    require_catalog_admin_rut(rut)
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(_HEALTH_SUMMARY_SQL, (_CATALOG_COMPANY_ID,))
        row = cur.fetchone()
        cur.close()
    finally:
        conn.close()

    if not row:
        return {
            "title": "Estado del catálogo",
            "total": 0,
            "sin_fotografia": 0,
            "sin_sec": 0,
            "sin_tipo_venta": 0,
            "sec_sin_quantity_step": 0,
            "unitario_por_falta_sec": 0,
        }

    total, sin_foto, sin_sec, sin_tipo, sec_sin_step, unitario_auto = row
    return {
        "title": "Estado del catálogo",
        "total": int(total or 0),
        "sin_fotografia": int(sin_foto or 0),
        "sin_sec": int(sin_sec or 0),
        "sin_tipo_venta": int(sin_tipo or 0),
        "sec_sin_quantity_step": int(sec_sin_step or 0),
        "unitario_por_falta_sec": int(unitario_auto or 0),
    }


@router.get("/catalog/admin/health-detail")
def catalog_health_detail(
    rut: str = Query(..., description="RUT del administrador de catálogo"),
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
) -> dict[str, Any]:
    require_catalog_admin_rut(rut)
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(_HEALTH_DETAIL_SQL, (_CATALOG_COMPANY_ID, limit, offset))
        columns = [d[0] for d in cur.description]
        rows = [dict(zip(columns, r)) for r in cur.fetchall()]
        cur.close()
    finally:
        conn.close()

    items: list[dict[str, Any]] = []
    for row in rows:
        rules = build_commercial_rules(
            variant_id=int(row["variant_id"]),
            product_name=row.get("product_name"),
            barcode=row.get("barcode"),
            units_per_box=row.get("units_per_box"),
            pm_sale_type=row.get("sale_type"),
            pm_quantity_step=row.get("quantity_step"),
            variant_description=row.get("variant_description"),
        )
        has_photo = _has_photo(row.get("image_url"))
        status_key, status_label = _catalog_row_status(rules, has_photo)
        items.append(
            {
                "variant_id": rules.variant_id,
                "barcode": rules.barcode,
                "product_name": rules.product_name,
                "has_photo": has_photo,
                "sec": rules.units_per_box,
                "sale_type": rules.sale_type,
                "quantity_step": rules.quantity_step,
                "auto_unitario_no_sec": rules.auto_unitario_no_sec,
                "missing_sale_type": rules.missing_sale_type,
                "missing_quantity_step": rules.missing_quantity_step,
                "status": status_key,
                "status_label": status_label,
            }
        )

    return {"items": items, "limit": limit, "offset": offset, "count": len(items)}
