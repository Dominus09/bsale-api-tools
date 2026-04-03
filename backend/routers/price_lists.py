from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Query

from backend.db import get_connection

router = APIRouter()


@router.get("/price-lists")
def list_active_price_lists(
    company_id: Optional[int] = Query(None, description="ID de empresa Bsale"),
) -> List[Dict[str, Any]]:
    """
    Listas de precio activas (state = 0) para la empresa.
    id = bsale_id (equivale a variant_prices.price_list_id).
    Sin company_id válido o sin filas: [] (nunca 404).
    """
    if company_id is None:
        return []

    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT bsale_id AS id, name
        FROM bsale.price_lists
        WHERE company_id = %s AND state = 0
        ORDER BY name
        """,
        (company_id,),
    )
    rows = cur.fetchall()
    result = [{"id": r[0], "name": r[1]} for r in rows]
    cur.close()
    conn.close()
    return result
