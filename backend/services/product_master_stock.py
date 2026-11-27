"""Read-only consolidated available stock for the barcode-based logistics master.

Legacy bsale.stocks is synchronized for all active Bsale companies.
Do not combine RAW + legacy, subtract reservations again or join by variant ID alone.
"""
from __future__ import annotations

from fastapi import HTTPException

# An inventory record belongs to precisely one company/variant/office. The
# existing stock sync reconciles this composite key. Group BEFORE joining the
# master to avoid fan-out from companies, branches, or product mappings.
STOCK_CTE = """
WITH company_barcode_stock AS (
    SELECT BTRIM(v.bar_code) AS stock_barcode,
           s.company_id,
           SUM(s.quantity_available) AS available_units,
           COUNT(*) AS stock_rows,
           COUNT(s.quantity_available) AS known_rows,
           COUNT(DISTINCT s.variant_id) AS variants_with_stock_rows,
           MAX(s.updated_at) AS stock_updated_at
    FROM bsale.stocks s
    JOIN bsale.companies c ON c.company_id = s.company_id AND c.active = TRUE
    JOIN bsale.variants v ON v.company_id = s.company_id AND v.bsale_id = s.variant_id
    WHERE NULLIF(BTRIM(v.bar_code), '') IS NOT NULL
    GROUP BY BTRIM(v.bar_code), s.company_id
), consolidated_stock AS (
    SELECT stock_barcode,
           SUM(available_units) AS stock_consolidated,
           COUNT(*) FILTER (WHERE known_rows > 0) AS stock_companies,
           BOOL_OR(known_rows < stock_rows) AS stock_partial,
           BOOL_OR(variants_with_stock_rows > 1) AS stock_shared_barcode,
           MAX(stock_updated_at) AS stock_updated_at
    FROM company_barcode_stock
    GROUP BY stock_barcode
)
"""
STOCK_JOIN = """
LEFT JOIN consolidated_stock st
  ON st.stock_barcode = BTRIM(products_master.barcode)
"""
STOCK_SELECT = """,
    st.stock_consolidated, st.stock_companies, st.stock_partial,
    st.stock_shared_barcode, st.stock_updated_at
"""


def stock_filter_sql(status: str) -> str | None:
    return {
        "all": None,
        "with": "st.stock_consolidated > 0",
        "without": "st.stock_consolidated <= 0",
        "unknown": "st.stock_consolidated IS NULL",
    }[status]


def stock_coverage(cur, company_ids: list[int] | None) -> dict:
    # Auth scope originates in require_management_access/require_erp_app.
    # A restricted manager must not receive group-wide stock or its counts.
    cur.execute("SELECT company_id FROM bsale.companies WHERE active = TRUE ORDER BY company_id")
    active_ids = [int(r[0]) for r in cur.fetchall()]
    if company_ids is not None and not set(active_ids).issubset(set(company_ids)):
        raise HTTPException(403, "El stock consolidado requiere acceso a todas las empresas activas.")
    cur.execute("""
        SELECT c.company_id,
               COUNT(s.quantity_available) AS known_stock_rows,
               MAX(s.updated_at) AS last_stock_sync
        FROM bsale.companies c
        LEFT JOIN bsale.stocks s ON s.company_id = c.company_id
        WHERE c.active = TRUE
        GROUP BY c.company_id
        ORDER BY c.company_id
    """)
    rows = cur.fetchall()
    known = [r for r in rows if r[1] > 0]
    timestamps = [r[2] for r in known if r[2] is not None]
    return {
        "available": bool(known),
        "companies_total": len(active_ids),
        "companies_with_data": len(known),
        "latest_sync_at": max(timestamps).isoformat() if timestamps else None,
        "source": "bsale.stocks.quantity_available",
        "scope": "all_active_companies_all_offices",
    }
