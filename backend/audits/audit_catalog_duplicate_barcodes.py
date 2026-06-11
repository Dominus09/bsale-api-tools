"""
Verifica barcodes duplicados en GET /api/catalog (antes vs después del JOIN LATERAL).

Uso:
  python -m backend.audits.audit_catalog_duplicate_barcodes
  python -m backend.audits.audit_catalog_duplicate_barcodes 7809562401293 7809562401330
"""

from __future__ import annotations

import json
import sys
from collections import Counter

from backend.db import get_connection
from backend.routers.catalog import _CATALOG_COMPANY_ID, _CATALOG_QUERY, _clean_name, _to_float
from backend.utils.sale_quantity import build_commercial_rules

_PRICE_LIST = "factura"

_CATALOG_QUERY_LEGACY = """
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
LEFT JOIN bsale.products_master pm
    ON pm.variant_id = cv.variant_id
    OR (
        NULLIF(BTRIM(cv.bar_code), '') IS NOT NULL
        AND pm.barcode = BTRIM(cv.bar_code)
    )
ORDER BY cv.product_type ASC, cv.product ASC, cv.variant ASC
"""


def _rows_to_products(rows: list[tuple]) -> list[dict]:
    out: list[dict] = []
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
        rules = build_commercial_rules(
            variant_id=int(_id) if _id is not None else 0,
            product_name=name,
            barcode=barcode,
            units_per_box=units_per_box_raw,
            pm_sale_type=pm_sale_type,
            pm_quantity_step=pm_quantity_step,
            variant_description=variant_description,
        )
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
                "units_per_box": rules.units_per_box,
                "sale_type": rules.sale_type,
                "quantity_step": rules.quantity_step,
            }
        )
    return out


def _duplicate_barcode_counts(products: list[dict]) -> dict[str, int]:
    counts = Counter(p["barcode"] for p in products if p.get("barcode"))
    return {bc: n for bc, n in counts.items() if n > 1}


def _fetch_catalog(cur, sql: str) -> list[dict]:
    cur.execute(sql, (_PRICE_LIST, _PRICE_LIST, _PRICE_LIST, _CATALOG_COMPANY_ID))
    return _rows_to_products(cur.fetchall())


def main() -> int:
    sample_barcodes = sys.argv[1:] or ["7809562401293", "7809562401330"]
    conn = get_connection()
    try:
        cur = conn.cursor()
        legacy_products = _fetch_catalog(cur, _CATALOG_QUERY_LEGACY.strip())
        current_products = _fetch_catalog(cur, _CATALOG_QUERY.strip())
        cur.close()
    finally:
        conn.close()

    legacy_dup = _duplicate_barcode_counts(legacy_products)
    current_dup = _duplicate_barcode_counts(current_products)

    samples = {
        bc: [p for p in current_products if p.get("barcode") == bc]
        for bc in sample_barcodes
    }

    report = {
        "total_filas_legacy": len(legacy_products),
        "total_filas_actual": len(current_products),
        "barcodes_duplicados_legacy": len(legacy_dup),
        "barcodes_duplicados_actual": len(current_dup),
        "conteo_duplicados_legacy": legacy_dup,
        "conteo_duplicados_actual": current_dup,
        "muestras_barcode": samples,
    }
    print(json.dumps(report, indent=2, ensure_ascii=False, default=str))

    if current_dup:
        print("\nFALLO: aún hay barcodes duplicados en la respuesta actual.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
