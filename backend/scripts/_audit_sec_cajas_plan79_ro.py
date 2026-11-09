"""Auditoría SOLO LECTURA SEC/cajas PLAN-00079.

No escribe. No actualiza maestros. Listar SKU sin units_per_box confiable.
"""

from __future__ import annotations

from backend.db import get_connection


def main() -> None:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            WITH pick AS (
              SELECT id FROM distribuidora.dispatch_plan_pickings
              WHERE dispatch_plan_id = 79 AND is_current LIMIT 1
            ),
            docs AS (
              SELECT related_document_id
              FROM distribuidora.dispatch_plan_picking_clients c
              JOIN pick p ON p.id = c.picking_id
              WHERE related_document_id IS NOT NULL
            )
            SELECT
              v.bsale_id,
              v.code,
              v.bar_code,
              v.description,
              v.units_per_box,
              ROUND(SUM(dd.quantity)::numeric, 2) AS qty
            FROM distribuidora.document_details dd
            JOIN bsale.variants v
              ON v.bsale_id = dd.variant_id AND v.company_id = 3
            WHERE dd.document_id IN (SELECT related_document_id FROM docs)
              AND (v.units_per_box IS NULL OR v.units_per_box <= 0)
            GROUP BY v.bsale_id, v.code, v.bar_code, v.description, v.units_per_box
            ORDER BY qty DESC
            """
        )
        rows = cur.fetchall()
        cur.close()
    finally:
        conn.close()

    print(f"PLAN-00079 SKU sin units_per_box estructurado: {len(rows)}")
    print("Nota: el token SEC en descripción NO se usa como equivalencia definitiva.")
    for r in rows[:30]:
        print(f"  variant={r[0]} upb={r[4]} qty={r[5]} | {r[3]}")


if __name__ == "__main__":
    main()
