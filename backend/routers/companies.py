from fastapi import APIRouter
from backend.db import get_connection

router = APIRouter()

@router.get("/companies")
def companies():

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT company_id, name
        FROM bsale.companies
        ORDER BY company_id
    """)

    rows = cur.fetchall()

    result = [
        {"company_id": r[0], "name": r[1]}
        for r in rows
    ]

    cur.close()
    conn.close()

    return result
