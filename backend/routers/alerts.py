from fastapi import APIRouter
from backend.db import get_connection

router = APIRouter()


@router.get("/margin-alerts")
def margin_alerts(company_id: int):

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM bsale.erp_alerts
        WHERE company_id = %s
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
