from fastapi import APIRouter
from backend.db import get_connection

router = APIRouter()

@router.get("/products-without-cost")
def products_without_cost(company_id: int):

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM bsale.product_margins
        WHERE company_id = %s
        AND average_cost_net IS NULL
    """, (company_id,))

    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description]

    result = [dict(zip(columns, row)) for row in rows]

    cur.close()
    conn.close()

    return result
