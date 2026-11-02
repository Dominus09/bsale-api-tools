#!/usr/bin/env python3
"""READ-ONLY: detalle de facturas por oficina para variantes del ranking (Coolify).

No escribe. No imprime secretos ni payload/note/user.

Uso en contenedor backend:
  python -m backend.scripts.inspect_office_compare_variants
"""

from __future__ import annotations

import json
import os
import sys

import psycopg2
from psycopg2.extras import RealDictCursor

COMPANY_ID = 3
VARIANT_IDS = (12594, 24486, 12948)
OFFICE_IDS = (1, 3, 4, 5)


def main() -> int:
    conn = psycopg2.connect(
        host=os.environ["PG_HOST"],
        dbname=os.environ["PG_DB"],
        user=os.environ["PG_USER"],
        password=os.environ["PG_PASSWORD"],
        port=os.environ.get("PG_PORT", "5432"),
    )
    conn.set_session(readonly=True, autocommit=True)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SET statement_timeout = '20s'")
    cur.execute("SET lock_timeout = '3s'")

    cur.execute(
        """
        SELECT v.bsale_id AS variant_id, v.description AS variant_name, v.code, v.bar_code,
               p.name AS product_name
        FROM bsale_raw.variants v
        LEFT JOIN bsale_raw.products p
          ON p.company_id = v.company_id AND p.bsale_id = v.product_id
        WHERE v.company_id = %s AND v.bsale_id = ANY(%s)
        ORDER BY v.bsale_id
        """,
        (COMPANY_ID, list(VARIANT_IDS)),
    )
    print("=== CATALOGO ===")
    print(json.dumps([dict(r) for r in cur.fetchall()], default=str, ensure_ascii=False, indent=2))

    cur.execute(
        """
        WITH fact AS (
          SELECT d.variant_id, r.office_id, o.name AS office_name,
                 r.admission_date::date AS fecha, r.document, r.document_number AS folio,
                 d.quantity, d.cost AS unit_cost, r.bsale_id AS reception_id,
                 r.details_complete,
                 CASE
                   WHEN coalesce(r.internal_dispatch_id, 0) <> 0 THEN 'transferencia_interna'
                   WHEN strpos(upper(regexp_replace(translate(coalesce(r.document, ''),
                        'áéíóúüÁÉÍÓÚÜ', 'aeiouuAEIOUU'), '\\s+', ' ', 'g')), 'FACTURA') > 0
                     THEN 'factura'
                   WHEN strpos(upper(regexp_replace(translate(coalesce(r.document, ''),
                        'áéíóúüÁÉÍÓÚÜ', 'aeiouuAEIOUU'), '\\s+', ' ', 'g')), 'GUIA') > 0
                     THEN 'guia_despacho'
                   ELSE 'otro'
                 END AS movement_class
          FROM bsale_raw.stock_receptions r
          JOIN bsale_raw.stock_reception_details d
            ON d.company_id = r.company_id AND d.reception_id = r.bsale_id
           AND d.missing_since IS NULL
          LEFT JOIN bsale_raw.offices o
            ON o.company_id = r.company_id AND o.bsale_id = r.office_id
          WHERE r.company_id = %s AND r.missing_since IS NULL
            AND d.variant_id = ANY(%s) AND r.office_id = ANY(%s)
            AND r.admission_date >= DATE '2025-01-01'
        ),
        last_fact AS (
          SELECT DISTINCT ON (variant_id, office_id)
                 variant_id, office_id, office_name, fecha, folio, quantity, unit_cost,
                 reception_id, details_complete
          FROM fact
          WHERE movement_class = 'factura'
            AND details_complete IS TRUE AND unit_cost > 0 AND quantity > 0
          ORDER BY variant_id, office_id, fecha DESC, reception_id DESC
        )
        SELECT * FROM last_fact
        ORDER BY variant_id, office_id
        """,
        (COMPANY_ID, list(VARIANT_IDS), list(OFFICE_IDS)),
    )
    print("=== ULTIMA FACTURA POR OFICINA ===")
    print(json.dumps([dict(r) for r in cur.fetchall()], default=str, ensure_ascii=False, indent=2))

    cur.close()
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
