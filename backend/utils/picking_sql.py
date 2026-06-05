"""Fragmentos SQL compartidos para picking (columnas reales en producción)."""

from __future__ import annotations

# bsale.products_master tiene product_type (TEXT), no product_type_name.
PM_TIPO_PRODUCTO_EXPR = "COALESCE(NULLIF(BTRIM(pm.product_type), ''), 'OTROS')"

# units_per_box vive en bsale.variants, no en products_master.
VARIANTS_JOIN = """
LEFT JOIN bsale.variants v
    ON v.company_id = 3
   AND NULLIF(BTRIM(v.bar_code), '') = NULLIF(BTRIM(dd.variant_code), '')
"""

PM_JOIN = """
LEFT JOIN bsale.products_master pm
    ON pm.barcode = NULLIF(BTRIM(dd.variant_code), '')
"""

CAJAS_AGG_EXPR = """
CASE
    WHEN MAX(v.units_per_box) IS NOT NULL AND MAX(v.units_per_box) > 0
    THEN CEIL(SUM(dd.quantity) / MAX(v.units_per_box)::numeric)
    ELSE NULL
END
"""

CAJAS_LINE_EXPR = """
CASE
    WHEN v.units_per_box IS NOT NULL AND v.units_per_box > 0
    THEN CEIL(dd.quantity / v.units_per_box::numeric)
    ELSE NULL
END
"""
