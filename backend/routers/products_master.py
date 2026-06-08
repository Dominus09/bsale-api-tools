from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from backend.db import get_connection
from backend.utils.product_logistics import (
    calc_volume_m3,
    calc_weight_unit_kg,
    fetch_logistics_stats,
)

router = APIRouter()

_DEFAULT_PAGE_SIZE = 500
_MAX_PAGE_SIZE = 1000

_LOGISTICS_PATCH_FIELDS = frozenset(
    {
        "supplier_id",
        "is_active",
        "units_per_box",
        "sale_type",
        "quantity_step",
        "weight_box_kg",
        "height_cm",
        "width_cm",
        "length_cm",
        "logistics_completed",
    }
)

_PM_SELECT = """
    id,
    barcode,
    sku,
    product_id,
    variant_id,
    product_name,
    variant_name,
    product_type,
    companies,
    supplier_id,
    units_per_box,
    sale_type,
    quantity_step,
    weight_box_kg,
    height_cm,
    width_cm,
    length_cm,
    logistics_completed,
    last_bsale_sync_at,
    is_active,
    created_at,
    updated_at
"""


class ProductMasterLogisticsPatch(BaseModel):
    supplier_id: Optional[int] = None
    is_active: Optional[bool] = None
    units_per_box: Optional[int] = Field(None, ge=1)
    sale_type: Optional[str] = Field(None, pattern=r"^(ENTERA|PARCIAL|UNITARIO)$")
    quantity_step: Optional[int] = Field(None, ge=1)
    weight_box_kg: Optional[float] = Field(None, ge=0)
    height_cm: Optional[float] = Field(None, ge=0)
    width_cm: Optional[float] = Field(None, ge=0)
    length_cm: Optional[float] = Field(None, ge=0)
    logistics_completed: Optional[bool] = None


def _parse_supplier_id_query(supplier_id: Optional[str]) -> tuple[bool, Optional[int]]:
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
    logistics_incomplete: bool,
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
        where_parts.append(
            "(product_name ILIKE %s OR variant_name ILIKE %s OR barcode ILIKE %s)"
        )
        params.extend([term, term, term])

    if logistics_incomplete:
        where_parts.append("logistics_completed = FALSE")

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
    out["weight_unit_kg"] = calc_weight_unit_kg(
        out.get("weight_box_kg"),
        out.get("units_per_box"),
    )
    out["volume_m3"] = calc_volume_m3(
        out.get("height_cm"),
        out.get("width_cm"),
        out.get("length_cm"),
    )
    return out


def _apply_patch(
    row_id: int | None,
    barcode: str | None,
    body: ProductMasterLogisticsPatch,
) -> Dict[str, Any]:
    payload = body.model_dump(exclude_unset=True)
    if not payload:
        raise HTTPException(status_code=400, detail="Sin campos para actualizar")

    updates: list[str] = []
    params: list[Any] = []
    for key, value in payload.items():
        if key not in _LOGISTICS_PATCH_FIELDS:
            raise HTTPException(status_code=400, detail=f"Campo no permitido: {key}")
        updates.append(f"{key} = %s")
        params.append(value)

    updates.append("updated_at = NOW()")

    if row_id is not None:
        where_clause = "id = %s"
        params.append(row_id)
    elif barcode:
        where_clause = "barcode = %s"
        params.append(barcode.strip())
    else:
        raise HTTPException(status_code=400, detail="id o barcode requerido")

    sql = f"""
        UPDATE bsale.products_master
        SET {", ".join(updates)}
        WHERE {where_clause}
        RETURNING {_PM_SELECT}
    """

    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(sql, tuple(params))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Producto no encontrado")
        conn.commit()
        columns = [desc[0] for desc in cur.description]
        return _serialize_pm_row(dict(zip(columns, row)))
    except HTTPException:
        conn.rollback()
        raise
    except Exception:
        conn.rollback()
        raise HTTPException(status_code=500, detail="Error actualizando producto")
    finally:
        cur.close()
        conn.close()


@router.get("/products-master/logistics-stats")
def products_master_logistics_stats() -> Dict[str, Any]:
    """KPIs del maestro logístico para dashboard Distribuidora."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        stats = fetch_logistics_stats(cur)
        cur.close()
    finally:
        conn.close()
    return stats


@router.get("/products-master")
def list_products_master(
    supplier_id: Optional[str] = Query(None),
    without_supplier: bool = Query(False),
    product_type: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    logistics_incomplete: bool = Query(False),
    limit: int = Query(_DEFAULT_PAGE_SIZE, ge=1, le=_MAX_PAGE_SIZE),
    offset: int = Query(0, ge=0),
) -> Dict[str, Any]:
    where_sql, params = _products_master_where(
        supplier_id, without_supplier, product_type, search, logistics_incomplete
    )
    base = "FROM bsale.products_master"

    count_sql = f"SELECT COUNT(*)::bigint {base}{where_sql}"
    data_sql = f"""
        SELECT {_PM_SELECT}
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


@router.patch("/products-master/id/{row_id}")
def patch_product_master_by_id(row_id: int, body: ProductMasterLogisticsPatch):
    if row_id < 1:
        raise HTTPException(status_code=400, detail="id inválido")
    return _apply_patch(row_id=row_id, barcode=None, body=body)


@router.patch("/products-master/{barcode}")
def patch_product_master(barcode: str, body: ProductMasterLogisticsPatch):
    clean = (barcode or "").strip()
    if not clean:
        raise HTTPException(status_code=400, detail="barcode es obligatorio")
    return _apply_patch(row_id=None, barcode=clean, body=body)
