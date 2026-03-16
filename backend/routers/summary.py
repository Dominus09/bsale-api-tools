from fastapi import APIRouter
from backend.db import get_connection

router = APIRouter()

@router.get("/margin-summary")
def margin_summary(company_id: int):

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
    SELECT
        COUNT(*) AS total_products,

        COUNT(*) FILTER (WHERE margin_status = 'MARGEN_BAJO') AS low_margin,

        COUNT(*) FILTER (WHERE margin_status = 'MARGEN_ALTO') AS high_margin,

        COUNT(*) FILTER (WHERE margin_status = 'MARGEN_ULTRA_ALTO') AS ultra_high_margin

    FROM bsale.erp_margin_dashboard
    WHERE company_id = %s
    """, (company_id,))

    row = cur.fetchone()

    result = {
        "total_products": row[0],
        "low_margin": row[1],
        "high_margin": row[2],
        "ultra_high_margin": row[3]
    }

    cur.close()
    conn.close()

    return result
