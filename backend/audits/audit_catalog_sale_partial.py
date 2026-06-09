"""Auditoría PARCIAL: quantity_step sospechosos y barcodes duplicados. Solo lectura."""

from __future__ import annotations

import json
import sys

from backend.db import get_connection

_QUERIES: list[tuple[str, str]] = [
    (
        "partial_suspicious_step",
        """
        WITH pm_enriched AS (
            SELECT
                BTRIM(pm.barcode) AS barcode,
                TRIM(COALESCE(pm.product_name, '') || ' ' || COALESCE(pm.variant_name, '')) AS name,
                pm.sale_type,
                pm.quantity_step,
                COALESCE(
                    NULLIF(pm.units_per_box, 0),
                    NULLIF(v.units_per_box, 0),
                    (regexp_match(
                        UPPER(COALESCE(v.description, pm.variant_name, '')),
                        'SEC[[:space:]]*([0-9]+)'
                    ))[1]::integer
                ) AS units_per_box
            FROM bsale.products_master pm
            LEFT JOIN bsale.variants v
                ON v.company_id = 3
               AND (
                    pm.variant_id = v.bsale_id
                    OR (
                        NULLIF(BTRIM(pm.barcode), '') IS NOT NULL
                        AND BTRIM(v.bar_code) = BTRIM(pm.barcode)
                    )
               )
        )
        SELECT barcode, name, units_per_box, sale_type, quantity_step
        FROM pm_enriched
        WHERE sale_type = 'PARCIAL'
          AND (
                quantity_step IS NULL
                OR quantity_step % 5 <> 0
                OR quantity_step > 10
          )
        ORDER BY units_per_box DESC NULLS LAST, quantity_step DESC NULLS LAST, name
        """,
    ),
    (
        "duplicate_barcodes",
        """
        SELECT
            BTRIM(barcode) AS barcode,
            COUNT(*)::bigint AS cantidad_registros,
            STRING_AGG(
                DISTINCT TRIM(COALESCE(product_name, '') || ' ' || COALESCE(variant_name, '')),
                ' | '
            ) AS productos_involucrados
        FROM bsale.products_master
        WHERE NULLIF(BTRIM(barcode), '') IS NOT NULL
        GROUP BY BTRIM(barcode)
        HAVING COUNT(*) > 1
        ORDER BY COUNT(*) DESC, BTRIM(barcode)
        """,
    ),
]


def main() -> int:
    conn = get_connection()
    out: dict[str, list[dict]] = {}
    try:
        cur = conn.cursor()
        for key, sql in _QUERIES:
            cur.execute(sql)
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
            out[key] = rows
            print(f"## {key} ({len(rows)} filas)")
            print(json.dumps(rows, ensure_ascii=False, indent=2, default=str))
            print()
        cur.close()
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
