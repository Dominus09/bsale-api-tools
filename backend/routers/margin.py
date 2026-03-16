from fastapi import APIRouter
from db import get_connection

router = APIRouter()


@router.get("/margin-analysis")
def margin_analysis(company_id: int):

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            product_name,
            bar_code,
            price_list_name,
            product_type_name,
            cost_gross,
            price_gross,
            margin_percent,
            min_margin,
            max_margin,
            margin_status,
            suggested_price_min,
            suggested_price_max
        FROM bsale.erp_margin_dashboard
        WHERE company_id = %s
        ORDER BY margin_percent ASC
    """, (company_id,))

    rows = cur.fetchall()

    columns = [desc[0] for desc in cur.description]

    result = [
        dict(zip(columns, row))
        for row in rows
    ]

    cur.close()
    conn.close()

    return result
