"""
Traza GET /api/catalog para un barcode: SQL intermedio + Product final.

Replica exactamente backend/routers/catalog.py + build_commercial_rules().
Solo lectura. No modifica el endpoint.

Uso:
  python -m backend.audits.audit_catalog_barcode_trace
  python -m backend.audits.audit_catalog_barcode_trace 7809562401293
"""

from __future__ import annotations

import json
import sys

from backend.db import get_connection
from backend.routers.catalog import _CATALOG_COMPANY_ID, _CATALOG_QUERY, _clean_name, _to_float
from backend.utils.sale_quantity import build_commercial_rules

_DEFAULT_BARCODE = "7809562401293"
_PRICE_LIST = "factura"

_CATALOG_VIEW_SQL = """
SELECT
    variant_id,
    BTRIM(bar_code) AS barcode,
    TRIM(product || ' ' || COALESCE(variant, '')) AS name,
    product_type,
    COALESCE(stock, 0) AS stock
FROM bsale.catalog_view
WHERE BTRIM(bar_code) = %s
ORDER BY variant_id
"""

_JOIN_MULTIPLICATION_SQL = """
SELECT
    cv.variant_id AS catalog_variant_id,
    BTRIM(cv.bar_code) AS barcode,
    v.company_id AS v_company_id,
    pm.id AS pm_id,
    pm.variant_id AS pm_variant_id,
    BTRIM(pm.barcode) AS pm_barcode,
    pm.sale_type,
    pm.quantity_step,
    COALESCE(
        NULLIF(v.units_per_box, 0),
        NULLIF(pm.units_per_box, 0),
        (regexp_match(UPPER(COALESCE(v.description, '')),
                      'SEC[[:space:]]*([0-9]+)'))[1]::integer
    ) AS units_per_box_raw,
    CASE
        WHEN pm.variant_id = cv.variant_id
         AND BTRIM(pm.barcode) = BTRIM(cv.bar_code)
            THEN 'variant_id_y_barcode'
        WHEN pm.variant_id = cv.variant_id
            THEN 'solo_variant_id'
        WHEN BTRIM(pm.barcode) = BTRIM(cv.bar_code)
            THEN 'solo_barcode'
        ELSE 'ninguno'
    END AS pm_match_tipo
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
WHERE BTRIM(cv.bar_code) = %s
ORDER BY cv.variant_id, pm.id NULLS LAST
"""

_PM_VARIANTS_SQL = """
SELECT
    'products_master' AS fuente,
    pm.id,
    pm.variant_id,
    BTRIM(pm.barcode) AS barcode,
    pm.sale_type,
    pm.quantity_step,
    pm.units_per_box
FROM bsale.products_master pm
WHERE BTRIM(pm.barcode) = %s
   OR pm.variant_id IN (
        SELECT variant_id FROM bsale.catalog_view WHERE BTRIM(bar_code) = %s
   )
UNION ALL
SELECT
    'variants' AS fuente,
    NULL::integer,
    v.bsale_id,
    BTRIM(v.bar_code),
    NULL,
    NULL,
    v.units_per_box
FROM bsale.variants v
WHERE BTRIM(v.bar_code) = %s
ORDER BY fuente, id NULLS LAST, variant_id
"""


def _trace_catalog_sql(cur, barcode: str) -> list[dict]:
    """Filas exactas de _CATALOG_QUERY antes del mapeo Python."""
    base = _CATALOG_QUERY.strip()
    query = base.replace(
        "ORDER BY",
        "WHERE BTRIM(cv.bar_code) = %s\nORDER BY",
        1,
    )
    cur.execute(
        query,
        (_PRICE_LIST, _PRICE_LIST, _PRICE_LIST, _CATALOG_COMPANY_ID, barcode),
    )
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def _map_to_api_product(row: dict) -> dict:
    """Mismo mapeo que get_catalog() en catalog.py."""
    rules = build_commercial_rules(
        variant_id=int(row["id"]) if row.get("id") is not None else 0,
        product_name=row.get("name"),
        barcode=row.get("barcode"),
        units_per_box=row.get("units_per_box"),
        pm_sale_type=row.get("pm_sale_type"),
        pm_quantity_step=row.get("pm_quantity_step"),
        variant_description=row.get("variant_description"),
    )
    image_val = row.get("image")
    image_out = (str(image_val).strip() if image_val is not None else "") or None
    if image_out == "":
        image_out = None

    return {
        "id": int(row["id"]) if row.get("id") is not None else 0,
        "name": _clean_name(row.get("name")),
        "type": "" if row.get("type") is None else str(row["type"]).strip(),
        "barcode": "" if row.get("barcode") is None else str(row["barcode"]).strip(),
        "price": _to_float(row.get("price")),
        "stock": int(row["stock"]) if row.get("stock") is not None else 0,
        "image": image_out,
        "units_per_box": rules.units_per_box,
        "sale_type": rules.sale_type,
        "quantity_step": rules.quantity_step,
    }


def _print_section(title: str, payload: object) -> None:
    print(f"\n## {title}")
    print(json.dumps(payload, indent=2, ensure_ascii=False, default=str))


def main() -> int:
    barcode = (sys.argv[1] if len(sys.argv) > 1 else _DEFAULT_BARCODE).strip()
    conn = get_connection()
    try:
        cur = conn.cursor()

        cur.execute(_CATALOG_VIEW_SQL, (barcode,))
        cv_cols = [d[0] for d in cur.description]
        cv_rows = [dict(zip(cv_cols, r)) for r in cur.fetchall()]
        _print_section(
            f"1) catalog_view ANTES de JOINs ({len(cv_rows)} fila(s))",
            cv_rows,
        )

        cur.execute(_JOIN_MULTIPLICATION_SQL, (_CATALOG_COMPANY_ID, barcode))
        j_cols = [d[0] for d in cur.description]
        join_rows = [dict(zip(j_cols, r)) for r in cur.fetchall()]
        _print_section(
            f"2) DESPUÉS de JOIN variants(company_id=3) + products_master OR ({len(join_rows)} fila(s))",
            join_rows,
        )

        cur.execute(_PM_VARIANTS_SQL, (barcode, barcode, barcode))
        pv_cols = [d[0] for d in cur.description]
        pv_rows = [dict(zip(pv_cols, r)) for r in cur.fetchall()]
        _print_section("3) products_master + variants fuente", pv_rows)

        sql_rows = _trace_catalog_sql(cur, barcode)
        intermediate = []
        for i, row in enumerate(sql_rows, start=1):
            intermediate.append(
                {
                    "fila_sql": i,
                    "barcode": row.get("barcode"),
                    "id": row.get("id"),
                    "name": row.get("name"),
                    "variant_description": row.get("variant_description"),
                    "units_per_box_raw": row.get("units_per_box"),
                    "pm_sale_type": row.get("pm_sale_type"),
                    "pm_quantity_step": row.get("pm_quantity_step"),
                }
            )
        _print_section(
            f"4) SQL _CATALOG_QUERY intermedio antes de build_commercial_rules ({len(intermediate)} fila(s))",
            intermediate,
        )

        api_products = [_map_to_api_product(r) for r in sql_rows]
        logs = []
        for p in api_products:
            label = "otra"
            if p["sale_type"] == "PARCIAL":
                label = f"Mínimo {p['quantity_step']} unidades"
            elif p["sale_type"] == "ENTERA" and p["units_per_box"]:
                label = f"Caja x {p['units_per_box']} unidades"
            logs.append(
                {
                    "barcode": p["barcode"],
                    "company_id": _CATALOG_COMPANY_ID,
                    "sale_type": p["sale_type"],
                    "quantity_step": p["quantity_step"],
                    "units_per_box": p["units_per_box"],
                    "api_id": p["id"],
                    "etiqueta_ui": label,
                }
            )
        _print_section(
            f"5) Product final API (como GET /api/catalog) ({len(api_products)} fila(s))",
            {"log_temporal": logs, "productos": api_products},
        )

        diagnosis = {
            "barcode": barcode,
            "filas_catalog_view": len(cv_rows),
            "filas_despues_join": len(join_rows),
            "filas_api": len(api_products),
            "pm_ids_en_join": sorted({r["pm_id"] for r in join_rows if r.get("pm_id") is not None}),
            "punto_multiplicacion": (
                "catalog_view: varias filas con mismo barcode"
                if len(cv_rows) > 1
                else (
                    "JOIN OR products_master: una fila catalog_view → varios pm_id"
                    if len(join_rows) > len(cv_rows)
                    else "revisar build_commercial_rules"
                )
            ),
        }
        _print_section("6) Diagnóstico automático", diagnosis)

        cur.close()
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
