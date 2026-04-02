from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse

from backend.client_rut import require_valid_rut, city_is_melinka
from backend.db import get_connection

router = APIRouter()

_ALLOWED_PRICE_LISTS = frozenset({"factura", "comoditi", "melinka"})
_NON_MELINKA_PRICE_LISTS = frozenset({"factura", "comoditi"})

_FORBIDDEN_CATALOG = JSONResponse(
    status_code=403,
    content={"error": "No autorizado"},
)


def _display_name(product_name: str | None, variant_name: str | None) -> str:
    parts = [
        (product_name or "").strip(),
        (variant_name or "").strip(),
    ]
    return " ".join(p for p in parts if p)


_CATALOG_STOCK_EXPR = "COALESCE(GREATEST(SUM(s.quantity_available), 0), 0)"


@router.get("/catalog")
def get_catalog(
    price_list: str,
    rut: str,
    in_stock: bool | None = Query(default=None),
):
    key = (price_list or "").strip().lower()
    if not key:
        raise HTTPException(
            status_code=400,
            detail="price_list es obligatorio",
        )
    if key not in _ALLOWED_PRICE_LISTS:
        raise HTTPException(
            status_code=400,
            detail="price_list debe ser factura, comoditi o melinka",
        )

    rut_clean = require_valid_rut(rut)

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT city FROM bsale.clients WHERE rut_clean = %s",
            (rut_clean,),
        )
        client_row = cur.fetchone()
        if not client_row:
            return JSONResponse(
                status_code=404,
                content={
                    "error": "Cliente no encontrado. Contactar al +56 9 9271 4314"
                },
            )

        is_melinka = city_is_melinka(client_row[0])
        if is_melinka and key != "melinka":
            return _FORBIDDEN_CATALOG
        if not is_melinka and key not in _NON_MELINKA_PRICE_LISTS:
            return _FORBIDDEN_CATALOG

        having = ""
        if in_stock is True:
            having = f"\n            HAVING {_CATALOG_STOCK_EXPR} > 0"

        cur.execute(
            f"""
            SELECT
                v.id AS variant_id,
                p.name AS product_name,
                v.description AS variant_name,
                v.bar_code AS barcode,
                vp.price_gross AS price,
                {_CATALOG_STOCK_EXPR} AS stock
            FROM bsale.variants v
            JOIN bsale.products p
                ON p.company_id = v.company_id AND p.bsale_id = v.product_id
            JOIN bsale.variant_prices vp
                ON vp.company_id = v.company_id AND vp.variant_id = v.bsale_id
            JOIN bsale.price_lists pl
                ON pl.company_id = vp.company_id AND pl.bsale_id = vp.price_list_id
            LEFT JOIN bsale.stocks s
                ON s.company_id = v.company_id AND s.variant_id = v.bsale_id
            WHERE LOWER(pl.name) = %s
            GROUP BY
                v.id,
                p.name,
                v.description,
                v.bar_code,
                vp.price_gross
            {having}
            """,
            (key,),
        )
        rows = cur.fetchall()
        cur.close()
    finally:
        conn.close()

    return [
        {
            "id": r[0],
            "name": _display_name(r[1], r[2]),
            "barcode": r[3],
            "price": r[4],
            "stock": 0 if r[5] is None else int(r[5]),
        }
        for r in rows
    ]
