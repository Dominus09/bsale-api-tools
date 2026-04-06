from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from backend.db import get_connection

router = APIRouter()


class PatchProductMasterBody(BaseModel):
    supplier_id: int | None = None
    is_active: bool | None = None


@router.get("/products-master")
def list_products_master(
    supplier_id: Optional[int] = Query(None),
    without_supplier: bool = Query(False),
    product_type: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
) -> List[Dict[str, Any]]:
    sql = """
        SELECT
            id,
            barcode,
            sku,
            product_name,
            variant_name,
            product_type,
            companies,
            supplier_id,
            is_active,
            created_at,
            updated_at
        FROM bsale.products_master
    """
    where_parts: list[str] = []
    params: list[Any] = []

    if supplier_id is not None:
        where_parts.append("supplier_id = %s")
        params.append(supplier_id)

    if without_supplier:
        where_parts.append("supplier_id IS NULL")

    if product_type is not None and product_type.strip():
        where_parts.append("product_type = %s")
        params.append(product_type.strip())

    if search is not None and search.strip():
        term = f"%{search.strip()}%"
        where_parts.append("(product_name ILIKE %s OR barcode ILIKE %s)")
        params.extend([term, term])

    if where_parts:
        sql += " WHERE " + " AND ".join(where_parts)

    sql += " ORDER BY product_name ASC NULLS LAST, barcode ASC"

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, tuple(params))
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        result = [dict(zip(columns, row)) for row in rows]
        cur.close()
    finally:
        conn.close()

    return result


@router.patch("/products-master/{barcode}")
def patch_product_master(barcode: str, body: PatchProductMasterBody):
    clean_barcode = (barcode or "").strip()
    if not clean_barcode:
        raise HTTPException(status_code=400, detail="barcode es obligatorio")

    updates: list[str] = []
    params: list[Any] = []

    if body.supplier_id is not None:
        updates.append("supplier_id = %s")
        params.append(body.supplier_id)

    if body.is_active is not None:
        updates.append("is_active = %s")
        params.append(body.is_active)

    if not updates:
        raise HTTPException(
            status_code=400,
            detail="Debe enviar supplier_id o is_active",
        )

    updates.append("updated_at = NOW()")
    params.append(clean_barcode)

    sql = f"""
        UPDATE bsale.products_master
        SET {", ".join(updates)}
        WHERE barcode = %s
        RETURNING barcode, supplier_id, is_active, updated_at
    """

    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(sql, tuple(params))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Producto no encontrado")
        conn.commit()
    except HTTPException:
        conn.rollback()
        raise
    except Exception:
        conn.rollback()
        raise HTTPException(status_code=500, detail="Error actualizando producto")
    finally:
        cur.close()
        conn.close()

    return {
        "barcode": row[0],
        "supplier_id": row[1],
        "is_active": row[2],
        "updated_at": row[3],
    }
