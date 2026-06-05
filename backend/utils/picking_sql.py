"""Fragmentos SQL compartidos para picking (columnas reales en producción)."""

from __future__ import annotations

# Tipo: products_master → Bsale product_types (nunca solo PM).
PM_TIPO_PRODUCTO_EXPR = """
COALESCE(
    NULLIF(BTRIM(pm.product_type), ''),
    NULLIF(BTRIM(pt.name), ''),
    'OTROS'
)
"""

VARIANTS_JOIN = """
LEFT JOIN bsale.variants v
    ON v.company_id = 3
   AND (
        (dd.variant_id IS NOT NULL AND v.bsale_id = dd.variant_id)
        OR NULLIF(BTRIM(v.bar_code), '') = NULLIF(BTRIM(dd.variant_code), '')
        OR NULLIF(BTRIM(v.code), '') = NULLIF(BTRIM(dd.variant_code), '')
   )
"""

PM_JOIN = """
LEFT JOIN bsale.products_master pm
    ON pm.barcode = COALESCE(
        NULLIF(BTRIM(v.bar_code), ''),
        NULLIF(BTRIM(dd.variant_code), '')
    )
"""

BSALE_PRODUCT_JOIN = """
LEFT JOIN bsale.products p
    ON p.company_id = v.company_id
   AND p.bsale_id = v.product_id
LEFT JOIN bsale.product_types pt
    ON pt.company_id = p.company_id
   AND pt.bsale_id = p.product_type_id
"""

# EAN desde variants.bar_code; variant_code en document_details suele ser SKU (code).
BARCODE_EXPR = """
COALESCE(NULLIF(BTRIM(v.bar_code), ''), NULLIF(BTRIM(dd.variant_code), ''))
"""

PRODUCTO_EXPR = """
COALESCE(NULLIF(BTRIM(p.name), ''), NULLIF(BTRIM(dd.variant_description), ''))
"""

VARIANTE_EXPR = """
COALESCE(NULLIF(BTRIM(v.description), ''), NULLIF(BTRIM(dd.variant_description), ''))
"""

CAJAS_AGG_EXPR = """
CASE
    WHEN MAX(v.units_per_box) IS NOT NULL AND MAX(v.units_per_box) > 0
    THEN ROUND(SUM(dd.quantity) / MAX(v.units_per_box)::numeric, 2)
    ELSE NULL
END
"""

CAJAS_LINE_EXPR = """
CASE
    WHEN v.units_per_box IS NOT NULL AND v.units_per_box > 0
    THEN ROUND(dd.quantity / v.units_per_box::numeric, 2)
    ELSE NULL
END
"""
