from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Body, HTTPException, Query
from pydantic import BaseModel, Field
from psycopg2 import errors as pg_errors

from backend.db import get_connection

router = APIRouter(tags=["purchases"])

TZ_CL = ZoneInfo("America/Santiago")


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


def _parse_office_state(raw: Any) -> Optional[int]:
    if raw is None:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def _to_santiago(dt: Any) -> Optional[datetime]:
    if dt is None or not isinstance(dt, datetime):
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(TZ_CL)


@router.get("/purchase-data-freshness")
def purchase_data_freshness(company_id: int) -> Dict[str, Any]:
    """
    Indicadores de frescura de datos para compras (informativo).
    Stock: MAX(updated_at) en bsale.stocks. Ventas: MAX(emission_date) en bsale.documents.
    Umbrales de stock y reglas de ventas usan hora Chile (America/Santiago).
    """
    sql_stock = """
        SELECT MAX(updated_at) AS last_stock_update
        FROM bsale.stocks
        WHERE company_id = %s
    """
    sql_sales = """
        SELECT MAX(emission_date) AS last_sales_update
        FROM bsale.documents
        WHERE company_id = %s
    """
    last_stock_raw: Any = None
    last_sales_raw: Any = None
    stock_column_missing = False

    conn = get_connection()
    try:
        cur = conn.cursor()
        try:
            cur.execute(sql_stock, (company_id,))
            row_s = cur.fetchone()
            last_stock_raw = row_s[0] if row_s else None
        except pg_errors.UndefinedColumn:
            conn.rollback()
            stock_column_missing = True
            last_stock_raw = None
        cur.execute(sql_sales, (company_id,))
        row_d = cur.fetchone()
        last_sales_raw = row_d[0] if row_d else None
        cur.close()
    finally:
        conn.close()

    now_cl = datetime.now(TZ_CL)
    last_stock_iso = last_stock_raw.isoformat() if isinstance(last_stock_raw, datetime) else None
    last_sales_iso = last_sales_raw.isoformat() if isinstance(last_sales_raw, datetime) else None

    # --- Stock (solo antigüedad del último sync) ---
    stock_block: Dict[str, Any]
    if stock_column_missing:
        stock_block = {
            "status": "DESACTUALIZADO",
            "minutes_ago": None,
            "message": "Falta columna updated_at en stocks. Ejecuta backend/sql/stocks_add_updated_at.sql y vuelve a correr sync de stock.",
        }
    elif last_stock_raw is None or not isinstance(last_stock_raw, datetime):
        stock_block = {
            "status": "DESACTUALIZADO",
            "minutes_ago": None,
            "message": "Sin registros de stock para esta empresa o nunca sincronizado.",
        }
    else:
        ls = _to_santiago(last_stock_raw)
        assert ls is not None
        delta_min = int((now_cl - ls).total_seconds() // 60)
        if delta_min < 30:
            st = "OK"
        elif delta_min < 60:
            st = "REVISAR"
        else:
            st = "DESACTUALIZADO"
        stock_block = {
            "status": st,
            "minutes_ago": delta_min,
            "message": f"actualizado hace {delta_min} minutos",
        }

    # --- Ventas (corte 05:00 Chile; no mezclar con lógica de stock) ---
    cutoff = now_cl.replace(hour=5, minute=0, second=0, microsecond=0)
    sales_block: Dict[str, Any]
    if now_cl < cutoff:
        sales_block = {
            "status": "ESPERANDO ACTUALIZACIÓN",
            "message": "Actualización diaria de ventas después de las 05:00 (hora Chile).",
        }
    elif last_sales_raw is None or not isinstance(last_sales_raw, datetime):
        sales_block = {
            "status": "ERROR / NO ACTUALIZADO",
            "message": "No hay documentos con fecha de emisión en la base.",
        }
    else:
        le = _to_santiago(last_sales_raw)
        assert le is not None
        if le.date() == now_cl.date():
            sales_block = {
                "status": "OK",
                "message": f"actualizado hoy a las {le.strftime('%H:%M')}",
            }
        else:
            sales_block = {
                "status": "ERROR / NO ACTUALIZADO",
                "message": "Las ventas no reflejan el día actual.",
            }

    return {
        "company_id": company_id,
        "last_stock_update": last_stock_iso,
        "last_sales_update": last_sales_iso,
        "stock": stock_block,
        "sales": sales_block,
    }


@router.get("/purchase-offices")
def list_purchase_offices(company_id: int) -> List[Dict[str, Any]]:
    """
    Sucursales activas en análisis de compra: solo filas con bsale.offices.state = 0
    (convención de este proyecto / Bsale para sucursal habilitada).
    Orden por nombre de sucursal; sin sufijos de estado en label.
    """
    sql = """
        SELECT DISTINCT
            pa.office_id,
            COALESCE(NULLIF(TRIM(ofc.name), ''), '') AS office_name,
            ofc.state AS office_state
        FROM bsale.vw_purchase_analysis pa
        INNER JOIN bsale.offices ofc
            ON ofc.company_id = pa.company_id
           AND ofc.bsale_id = pa.office_id
           AND ofc.state = 0
        WHERE pa.company_id = %s
        ORDER BY
            NULLIF(TRIM(ofc.name), '') ASC NULLS LAST,
            pa.office_id ASC
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, (company_id,))
        rows = _rows_to_dicts(cur)
        cur.close()
        out: List[Dict[str, Any]] = []
        for r in rows:
            oid = int(r["office_id"])
            name = (r.get("office_name") or "").strip()
            st = _parse_office_state(r.get("office_state"))
            label = name if name else f"Sucursal {oid}"
            out.append(
                {
                    "office_id": oid,
                    "office_name": name or None,
                    "office_state": st,
                    "is_active": True,
                    "label": label,
                }
            )
        return out
    finally:
        conn.close()


@router.get("/purchase-analysis")
def purchase_analysis(
    company_id: int,
    office_id: Optional[int] = None,
    supplier_id: Optional[int] = Query(
        None,
        description="Filtrar filas cuyo barcode en products_master tenga este proveedor",
    ),
    status: Optional[str] = Query(
        None,
        description="Filtrar por status: COMPRAR, REVISAR, NO_COMPRAR",
    ),
    limit: int = Query(8000, ge=1, le=20000),
) -> List[Dict[str, Any]]:
    """
    Filas de bsale.vw_purchase_analysis para la empresa (y opcionalmente sucursal y proveedor).
    """
    join_pm = ""
    if supplier_id is not None:
        join_pm = """
        INNER JOIN bsale.products_master pm
            ON pm.barcode = pa.barcode
           AND pm.supplier_id = %s
        """
    sql = f"""
        SELECT
            pa.company_id,
            pa.office_id,
            pa.variant_id,
            pa.product_type_name,
            pa.product_name,
            pa.variant_name,
            pa.barcode,
            pa.ventas_7_dias,
            pa.ventas_30_dias,
            pa.promedio_diario,
            pa.stock_actual,
            pa.costo_bruto,
            pa.dias_cobertura,
            pa.demanda_proyectada,
            pa.unidades_a_comprar,
            pa.units_per_box,
            pa.units_per_box_eff,
            pa.cajas_sugeridas,
            pa.status,
            pa.costo_total_compra
        FROM bsale.vw_purchase_analysis pa
        {join_pm}
        WHERE pa.company_id = %s
    """
    params: list[Any] = []
    if supplier_id is not None:
        params.append(supplier_id)
    params.append(company_id)
    if office_id is not None:
        sql += " AND pa.office_id = %s"
        params.append(office_id)
    if status is not None and status.strip():
        sql += " AND pa.status = %s"
        params.append(status.strip().upper())
    sql += """
        ORDER BY
            CASE pa.status
                WHEN 'COMPRAR' THEN 1
                WHEN 'REVISAR' THEN 2
                ELSE 3
            END,
            pa.costo_total_compra DESC NULLS LAST,
            pa.variant_id
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
            NULLIF(TRIM(ofc.name), '') AS office_name,
            ofc.state AS office_state,
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
        LEFT JOIN bsale.offices ofc
            ON ofc.company_id = o.company_id
           AND ofc.bsale_id = o.office_id
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
            c.name AS company_name,
            o.office_id,
            NULLIF(TRIM(ofc.name), '') AS office_name,
            ofc.state AS office_state,
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
        LEFT JOIN bsale.companies c ON c.company_id = o.company_id
        LEFT JOIN bsale.offices ofc
            ON ofc.company_id = o.company_id
           AND ofc.bsale_id = o.office_id
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


class PurchaseLineIn(BaseModel):
    variant_id: Optional[int] = None
    product_type_name: Optional[str] = None
    product_name: Optional[str] = None
    variant_name: Optional[str] = None
    barcode: Optional[str] = None
    cantidad: float = Field(..., gt=0)
    units_per_box: Optional[int] = None
    costo_unitario: float = Field(..., ge=0)


class GeneratePurchaseOrderFromLinesBody(BaseModel):
    company_id: int
    office_id: int
    supplier_id: int
    fecha_entrega: Optional[date] = None
    forma_pago: Optional[str] = None
    responsable: Optional[str] = None
    observacion: Optional[str] = None
    lines: List[PurchaseLineIn] = Field(..., min_length=1)


_OC_STATUSES = frozenset(
    {"BORRADOR", "GENERADA", "ENVIADA", "RECIBIDA", "ANULADA"},
)


def _units_per_box_eff(units: Optional[int]) -> int:
    if units is None or units <= 0:
        return 1
    return int(units)


@router.post("/purchase-orders/generate-from-lines")
def generate_purchase_order_from_lines(
    body: GeneratePurchaseOrderFromLinesBody,
) -> Dict[str, Any]:
    """
    Crea OC e inserta líneas explícitas (cantidades editadas, manuales sin variant_id).
    """
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO bsale.oc_document (
                company_id,
                office_id,
                supplier_id,
                fecha_emision,
                fecha_entrega,
                forma_pago,
                responsable,
                observacion,
                status,
                total_oc
            )
            VALUES (%s, %s, %s, CURRENT_TIMESTAMP, %s, %s, %s, %s, 'GENERADA', 0)
            RETURNING oc_id
            """,
            (
                body.company_id,
                body.office_id,
                body.supplier_id,
                body.fecha_entrega,
                body.forma_pago,
                body.responsable,
                body.observacion,
            ),
        )
        oc_row = cur.fetchone()
        if not oc_row:
            raise HTTPException(status_code=500, detail="No se pudo crear la OC")
        oc_id = int(oc_row[0])

        insert_detail = """
            INSERT INTO bsale.oc_details (
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
                costo_total
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        for ln in body.lines:
            upe = _units_per_box_eff(ln.units_per_box)
            cajas = float(ln.cantidad) / float(upe)
            costo_total = float(ln.cantidad) * float(ln.costo_unitario)
            cur.execute(
                insert_detail,
                (
                    oc_id,
                    body.company_id,
                    body.office_id,
                    ln.variant_id,
                    ln.product_type_name,
                    ln.product_name,
                    ln.variant_name,
                    ln.barcode,
                    ln.cantidad,
                    ln.units_per_box,
                    cajas,
                    ln.costo_unitario,
                    costo_total,
                ),
            )

        cur.execute(
            """
            SELECT COALESCE(SUM(costo_total), 0)
            FROM bsale.oc_details
            WHERE oc_id = %s
            """,
            (oc_id,),
        )
        total_row = cur.fetchone()
        total_oc = float(total_row[0]) if total_row and total_row[0] is not None else 0.0
        cur.execute(
            "UPDATE bsale.oc_document SET total_oc = %s WHERE oc_id = %s",
            (total_oc, oc_id),
        )
        conn.commit()
        return {"oc_id": oc_id}
    except HTTPException:
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(e)) from e
    finally:
        cur.close()
        conn.close()


class PatchPurchaseOrderBody(BaseModel):
    status: str


@router.patch("/purchase-orders/{oc_id}")
def patch_purchase_order(
    oc_id: int,
    company_id: int = Query(...),
    body: PatchPurchaseOrderBody = Body(...),
):
    st = (body.status or "").strip().upper()
    if st not in _OC_STATUSES:
        raise HTTPException(
            status_code=400,
            detail=f"status debe ser uno de: {', '.join(sorted(_OC_STATUSES))}",
        )
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE bsale.oc_document
            SET status = %s
            WHERE oc_id = %s AND company_id = %s
            RETURNING oc_id, status
            """,
            (st, oc_id, company_id),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="OC no encontrada")
        conn.commit()
        return {"oc_id": int(row[0]), "status": row[1]}
    except HTTPException:
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(e)) from e
    finally:
        cur.close()
        conn.close()
