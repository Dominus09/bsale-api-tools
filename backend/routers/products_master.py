from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Body, HTTPException, Query

from backend.db import get_connection

router = APIRouter()

_DEFAULT_PAGE_SIZE = 500
_MAX_PAGE_SIZE = 1000


def _parse_supplier_id_query(supplier_id: Optional[str]) -> tuple[bool, Optional[int]]:
    """Devuelve (filtrar_solo_null, id_numérico). Solo uno aplica."""
    if supplier_id is None:
        return False, None
    raw = supplier_id.strip()
    if raw == "":
        return False, None
    if raw.lower() in ("null", "none"):
        return True, None
    try:
        n = int(raw)
    except ValueError:
        raise HTTPException(status_code=400, detail="supplier_id inválido")
    return False, n


def _products_master_where(
    supplier_id: Optional[str],
    without_supplier: bool,
    product_type: Optional[str],
    search: Optional[str],
) -> Tuple[str, List[Any]]:
    where_parts: list[str] = []
    params: list[Any] = []

    filter_null, filter_sid = _parse_supplier_id_query(supplier_id)
    if filter_null:
        where_parts.append("supplier_id IS NULL")
    elif filter_sid is not None:
        where_parts.append("supplier_id = %s")
        params.append(filter_sid)

    if without_supplier:
        where_parts.append("supplier_id IS NULL")

    if product_type is not None and product_type.strip():
        where_parts.append("product_type = %s")
        params.append(product_type.strip())

    if search is not None and search.strip():
        term = f"%{search.strip()}%"
        where_parts.append("(product_name ILIKE %s OR barcode ILIKE %s)")
        params.extend([term, term])

    where_sql = " WHERE " + " AND ".join(where_parts) if where_parts else ""
    return where_sql, params


def _serialize_pm_row(row: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in row.items():
        if isinstance(v, Decimal):
            out[k] = float(v)
        elif isinstance(v, (datetime, date)):
            out[k] = v.isoformat()
        else:
            out[k] = v
    return out


@router.get("/products-master")
def list_products_master(
    supplier_id: Optional[str] = Query(
        None,
        description="ID numérico del proveedor, o la cadena null para supplier_id IS NULL",
    ),
    without_supplier: bool = Query(False),
    product_type: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    limit: int = Query(
        _DEFAULT_PAGE_SIZE,
        ge=1,
        le=_MAX_PAGE_SIZE,
        description="Tamaño de página (máx. 1000)",
    ),
    offset: int = Query(0, ge=0, description="Desplazamiento para paginación"),
) -> Dict[str, Any]:
    """
    Lista paginada de bsale.products_master.
    Siempre devuelve { items, total, limit, offset }.
    """
    where_sql, params = _products_master_where(
        supplier_id, without_supplier, product_type, search
    )
    base = "FROM bsale.products_master"

    count_sql = f"SELECT COUNT(*)::bigint {base}{where_sql}"
    data_sql = f"""
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
        {base}{where_sql}
        ORDER BY product_name ASC NULLS LAST, barcode ASC
        LIMIT %s OFFSET %s
    """
    data_params = list(params) + [limit, offset]

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(count_sql, tuple(params))
        total_row = cur.fetchone()
        total = int(total_row[0]) if total_row and total_row[0] is not None else 0

        cur.execute(data_sql, tuple(data_params))
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        result = [_serialize_pm_row(dict(zip(columns, row))) for row in rows]
        cur.close()
    finally:
        conn.close()

    return {
        "items": result,
        "total": total,
        "limit": limit,
        "offset": offset,
    }


@router.get("/products-master/count-without-supplier")
def count_products_master_without_supplier() -> Dict[str, Any]:
    """Cuenta filas en bsale.products_master con supplier_id IS NULL."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT COUNT(*)::bigint
            FROM bsale.products_master
            WHERE supplier_id IS NULL
            """
        )
        row = cur.fetchone()
        cur.close()
    finally:
        conn.close()

    total = int(row[0]) if row and row[0] is not None else 0
    return {"count": total}


@router.patch("/products-master/{barcode}")
def patch_product_master(barcode: str, body: Dict[str, Any] = Body(...)):
    clean_barcode = (barcode or "").strip()
    if not clean_barcode:
        raise HTTPException(status_code=400, detail="barcode es obligatorio")

    updates: list[str] = []
    params: list[Any] = []

    if "supplier_id" in body:
        sid = body["supplier_id"]
        if sid is not None and not isinstance(sid, int):
            raise HTTPException(
                status_code=400,
                detail="supplier_id debe ser un entero o null",
            )
        updates.append("supplier_id = %s")
        params.append(sid)

    if "is_active" in body:
        active = body["is_active"]
        if not isinstance(active, bool):
            raise HTTPException(
                status_code=400,
                detail="is_active debe ser booleano",
            )
        updates.append("is_active = %s")
        params.append(active)

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
