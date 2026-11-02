from fastapi import APIRouter, Depends

from backend.db import get_connection
from backend.utils.auth_staff import require_staff_user

router = APIRouter()


@router.get("/companies")
def companies(_user: dict = Depends(require_staff_user)):
    """Listado de empresas ERP — requiere sesión staff con acceso ERP."""
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT company_id, name
        FROM bsale.companies
        ORDER BY company_id
        """
    )

    rows = cur.fetchall()

    result = [{"company_id": r[0], "name": r[1]} for r in rows]

    cur.close()
    conn.close()

    return result
