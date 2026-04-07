from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from backend.db import get_connection

router = APIRouter(tags=["purchases"])


def _rows_to_dicts(cur) -> List[Dict[str, Any]]:
    rows = cur.fetchall()
    if not rows:
        return []
    columns = [desc[0] for desc in cur.description]
    return [dict(zip(columns, row)) for row in rows]


def _jsonable_value(v: Any) -> Any:
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    return v


def _serialize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    return {k: _jsonable_value(v) for k, v in row.items()}


@router.get("/purchase-analysis")
def purchase_analysis(
    company_id: int,
    office_id: Optional[int] = None,
    status: Optional[str] = Query(
        None,
        description="Filtrar por status: COMPRAR, REVISAR, NO_COMPRAR",
    ),
    limit: int = Query(5000, ge=1, le=20000),
) -> List[Dict[str, Any]]:
    """
    Filas de bsale.vw_purchase_analysis para la empresa (y opcionalmente sucursal).
    """
    sql = """
        SELECT
            company_id,
            office_id,
            variant_id,
            product_type_name,
            product_name,
            variant_name,
            barcode,
            ventas_7_dias,
            ventas_30_dias,
            promedio_diario,
            stock_actual,
            costo_bruto,
            dias_cobertura,
            demanda_proyectada,
            unidades_a_comprar,
            units_per_box,
            units_per_box_eff,
            cajas_sugeridas,
            status,
            costo_total_compra
        FROM bsale.vw_purchase_analysis
        WHERE company_id = %s
    """
    params: list[Any] = [company_id]
    if office_id is not None:
        sql += " AND office_id = %s"
        params.append(office_id)
    if status is not None and status.strip():
        sql += " AND status = %s"
        params.append(status.strip().upper())
    sql += """
        ORDER BY
            CASE status
                WHEN 'COMPRAR' THEN 1
                WHEN 'REVISAR' THEN 2
                ELSE 3
            END,
            costo_total_compra DESC NULLS LAST,
            variant_id
        LIMIT %s
    """
    params.append(limit)

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, tuple(params))
        data = _rows_to_dicts(cur)
        cur.close()
        return [_serialize_row(r) for r in data]
    finally:
        conn.close()


class ManualItemCreateBody(BaseModel):
    company_id: int
    office_id: int
    supplier_id: int
    product_type_name: Optional[str] = None
    product_name: Optional[str] = None
    variant_name: Optional[str] = None
    barcode: Optional[str] = None
    units_per_box: Optional[int] = None
    costo_bruto: Optional[float] = None
    cantidad: float = Field(..., gt=0)


@router.get("/purchase-manual-items")
def list_purchase_manual_items(
    company_id: int,
    office_id: int,
    supplier_id: Optional[int] = None,
    pending_only: bool = True,
) -> List[Dict[str, Any]]:
    sql = """
        SELECT
            id,
            company_id,
            office_id,
            supplier_id,
            product_type_name,
            product_name,
            variant_name,
            barcode,
            units_per_box,
            costo_bruto,
            cantidad,
            oc_id,
            consumed_at,
            created_at
        FROM bsale.purchase_manual_items
        WHERE company_id = %s AND office_id = %s
    """
    params: list[Any] = [company_id, office_id]
    if supplier_id is not None:
        sql += " AND supplier_id = %s"
        params.append(supplier_id)
    if pending_only:
        sql += " AND oc_id IS NULL"
    sql += " ORDER BY created_at DESC, id DESC"

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, tuple(params))
        data = _rows_to_dicts(cur)
        cur.close()
        return [_serialize_row(r) for r in data]
    finally:
        conn.close()


@router.post("/purchase-manual-items")
def create_purchase_manual_item(body: ManualItemCreateBody) -> Dict[str, Any]:
    sql = """
        INSERT INTO bsale.purchase_manual_items (
            company_id,
            office_id,
            supplier_id,
            product_type_name,
            product_name,
            variant_name,
            barcode,
            units_per_box,
            costo_bruto,
            cantidad
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING
            id,
            company_id,
            office_id,
            supplier_id,
            product_type_name,
            product_name,
            variant_name,
            barcode,
            units_per_box,
            costo_bruto,
            cantidad,
            oc_id,
            consumed_at,
            created_at
    """
    params = (
        body.company_id,
        body.office_id,
        body.supplier_id,
        body.product_type_name,
        body.product_name,
        body.variant_name,
        body.barcode,
        body.units_per_box,
        body.costo_bruto,
        body.cantidad,
    )
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        row = cur.fetchone()
        columns = [desc[0] for desc in cur.description]
        conn.commit()
        cur.close()
        return _serialize_row(dict(zip(columns, row)))
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


@router.delete("/purchase-manual-items/{item_id}")
def delete_purchase_manual_item(
    item_id: int,
    company_id: int,
) -> Dict[str, str]:
    sql = """
        DELETE FROM bsale.purchase_manual_items
        WHERE id = %s AND company_id = %s AND oc_id IS NULL
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, (item_id, company_id))
        deleted = cur.rowcount
        cur.close()
        if deleted == 0:
            conn.rollback()
            raise HTTPException(
                status_code=404,
                detail="Ítem no encontrado, ya consumido en una OC o empresa distinta",
            )
        conn.commit()
        return {"status": "ok"}
    except HTTPException:
        conn.rollback()
        raise
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


@router.get("/purchase-orders")
def list_purchase_orders(
    company_id: int,
    office_id: Optional[int] = None,
    limit: int = Query(200, ge=1, le=1000),
) -> List[Dict[str, Any]]:
    sql = """
        SELECT
            o.oc_id,
            o.company_id,
            o.office_id,
            o.supplier_id,
            s.name AS supplier_name,
            o.fecha_emision,
            o.fecha_entrega,
            o.total_oc,
            o.forma_pago,
            o.responsable,
            o.observacion,
            o.status,
            o.created_at
        FROM bsale.oc_document o
        LEFT JOIN bsale.suppliers s ON s.id = o.supplier_id
        WHERE o.company_id = %s
    """
    params: list[Any] = [company_id]
    if office_id is not None:
        sql += " AND o.office_id = %s"
        params.append(office_id)
    sql += " ORDER BY o.fecha_emision DESC NULLS LAST, o.oc_id DESC LIMIT %s"
    params.append(limit)

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, tuple(params))
        data = _rows_to_dicts(cur)
        cur.close()
        return [_serialize_row(r) for r in data]
    finally:
        conn.close()


@router.get("/purchase-orders/{oc_id}")
def get_purchase_order(
    oc_id: int,
    company_id: int,
) -> Dict[str, Any]:
    header_sql = """
        SELECT
            o.oc_id,
            o.company_id,
            o.office_id,
            o.supplier_id,
            s.name AS supplier_name,
            o.fecha_emision,
            o.fecha_entrega,
            o.total_oc,
            o.forma_pago,
            o.responsable,
            o.observacion,
            o.status,
            o.created_at
        FROM bsale.oc_document o
        LEFT JOIN bsale.suppliers s ON s.id = o.supplier_id
        WHERE o.oc_id = %s AND o.company_id = %s
    """
    details_sql = """
        SELECT
            oc_detail_id,
            oc_id,
            company_id,
            office_id,
            variant_id,
            product_type_name,
            product_name,
            variant_name,
            barcode,
            cantidad,
            units_per_box,
            cajas,
            costo_unitario,
            costo_total,
            created_at
        FROM bsale.oc_details
        WHERE oc_id = %s AND company_id = %s
        ORDER BY oc_detail_id
    """
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(header_sql, (oc_id, company_id))
        h = cur.fetchone()
        if not h:
            raise HTTPException(status_code=404, detail="OC no encontrada")
        hcols = [desc[0] for desc in cur.description]
        header = _serialize_row(dict(zip(hcols, h)))

        cur.execute(details_sql, (oc_id, company_id))
        details = [_serialize_row(r) for r in _rows_to_dicts(cur)]
        return {"header": header, "details": details}
    finally:
        cur.close()
        conn.close()


class GeneratePurchaseOrderBody(BaseModel):
    company_id: int
    office_id: int
    supplier_id: int
    fecha_emision: Optional[datetime] = None
    fecha_entrega: Optional[date] = None
    forma_pago: Optional[str] = None
    responsable: Optional[str] = None
    observacion: Optional[str] = None
    manual_ids: Optional[List[int]] = None


@router.post("/purchase-orders/generate")
def generate_purchase_order(body: GeneratePurchaseOrderBody) -> Dict[str, Any]:
    manual_ids = body.manual_ids
    if manual_ids:
        try:
            manual_ids = [int(x) for x in manual_ids]
        except (TypeError, ValueError):
            raise HTTPException(status_code=400, detail="manual_ids debe ser lista de enteros")

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT bsale.generate_purchase_order(
                %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """,
            (
                body.company_id,
                body.office_id,
                body.supplier_id,
                body.fecha_emision,
                body.fecha_entrega,
                body.forma_pago,
                body.responsable,
                body.observacion,
                manual_ids if manual_ids else None,
            ),
        )
        new_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        return {"oc_id": int(new_id)}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(e)) from e
    finally:
        conn.close()
