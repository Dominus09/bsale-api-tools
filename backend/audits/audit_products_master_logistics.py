"""
Auditoría maestro logístico → stdout (markdown-friendly).

Uso:
  python -m backend.audits.audit_products_master_logistics
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone

from backend.db import get_connection
from backend.utils.product_logistics import fetch_logistics_stats


def _count(cur, sql: str) -> int:
    cur.execute(sql)
    row = cur.fetchone()
    return int(row[0] or 0) if row else 0


def run_audit() -> dict:
    conn = get_connection()
    try:
        cur = conn.cursor()
        pm = fetch_logistics_stats(cur)
        variants_total = _count(cur, "SELECT COUNT(*) FROM bsale.variants")
        variants_barcode = _count(
            cur,
            """
            SELECT COUNT(*) FROM bsale.variants
            WHERE NULLIF(BTRIM(bar_code), '') IS NOT NULL
            """,
        )
        variants_upb = _count(
            cur,
            """
            SELECT COUNT(*) FROM bsale.variants
            WHERE units_per_box IS NOT NULL AND units_per_box > 0
            """,
        )
        products_total = _count(cur, "SELECT COUNT(*) FROM bsale.products")
        variants_sin_pm = _count(
            cur,
            """
            SELECT COUNT(DISTINCT BTRIM(v.bar_code))
            FROM bsale.variants v
            WHERE NULLIF(BTRIM(v.bar_code), '') IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM bsale.products_master pm
                  WHERE pm.barcode = BTRIM(v.bar_code)
              )
            """,
        )
        cur.close()
        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "products_master": pm,
            "variants": {
                "total": variants_total,
                "with_barcode": variants_barcode,
                "with_units_per_box": variants_upb,
            },
            "products": {"total": products_total},
            "gaps": {"variants_without_pm": variants_sin_pm},
        }
    finally:
        conn.close()


def format_markdown(data: dict) -> str:
    pm = data["products_master"]
    v = data["variants"]
    lines = [
        "# Auditoría maestro logístico",
        "",
        f"**Generado:** {data['generated_at']} (UTC)",
        "",
        "## bsale.products_master",
        "",
        f"| Métrica | Cantidad |",
        f"|---------|----------|",
        f"| Total productos | {pm.get('total', 0):,} |",
        f"| Con barcode | {pm.get('with_barcode', 0):,} |",
        f"| Con units_per_box | {pm.get('with_units_per_box', 0):,} |",
        f"| Con proveedor | {pm.get('with_supplier', 0):,} |",
        f"| Con peso (weight_box_kg) | {pm.get('with_weight', 0):,} |",
        f"| Con dimensiones completas | {pm.get('with_dimensions', 0):,} |",
        f"| logistics_completed | {pm.get('logistics_completed', 0):,} |",
        f"| Completitud logística % | {pm.get('completeness_pct', 0)}% |",
        "",
        "## bsale.variants",
        "",
        f"| Métrica | Cantidad |",
        f"|---------|----------|",
        f"| Total variantes | {v['total']:,} |",
        f"| Con barcode | {v['with_barcode']:,} |",
        f"| Con units_per_box | {v['with_units_per_box']:,} |",
        "",
        "## bsale.products",
        "",
        f"| Total | {data['products']['total']:,} |",
        "",
        "## Brechas",
        "",
        f"- Variantes con barcode sin fila en products_master: **{data['gaps']['variants_without_pm']:,}**",
        "",
        "## Regenerar",
        "",
        "```bash",
        "python -m backend.audits.audit_products_master_logistics --write-doc",
        "```",
        "",
    ]
    return "\n".join(lines)


def main() -> int:
    write_doc = "--write-doc" in sys.argv
    try:
        data = run_audit()
    except Exception as exc:
        print(f"Error de auditoría: {exc}", file=sys.stderr)
        return 1
    md = format_markdown(data)
    print(md)
    if write_doc:
        from pathlib import Path

        root = Path(__file__).resolve().parents[2]
        path = root / "docs" / "PRODUCTS_MASTER_LOGISTICS_AUDIT.md"
        path.write_text(md, encoding="utf-8")
        print(f"Escrito: {path}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
