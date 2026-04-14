from fastapi import APIRouter

from backend.db import get_connection

router = APIRouter(prefix="/erp", tags=["ERP"])

# DASHBOARD
@router.get("/dashboard")
def get_dashboard():

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM bsale.erp_dashboard
    """)

    row = cur.fetchone()
    columns = [desc[0] for desc in cur.description]

    result = dict(zip(columns, row))

    cur.close()
    conn.close()

    return result


# ALERTAS
@router.get("/alerts")
def get_alerts():

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM bsale.erp_alerts
        LIMIT 200
    """)

    columns = [desc[0] for desc in cur.description]

    rows = cur.fetchall()

    data = [dict(zip(columns,row)) for row in rows]

    cur.close()
    conn.close()

    return data


# MARGENES
@router.get("/margins")
def get_margins():

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM bsale.erp_margin_dashboard
        LIMIT 500
    """)

    columns = [desc[0] for desc in cur.description]

    rows = cur.fetchall()

    data = [dict(zip(columns,row)) for row in rows]

    cur.close()
    conn.close()

    return data
