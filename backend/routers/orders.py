from datetime import date
from decimal import Decimal

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field, field_validator

from backend.db import get_connection

router = APIRouter(tags=["Pedidos"])

_ALLOWED_ORDER_STATUSES = frozenset({"pendiente", "generado", "anulado", "revisar"})


class OrderClient(BaseModel):
    id: int
    name: str
    rut: str


class OrderItemIn(BaseModel):
    id: int
    name: str
    barcode: str
    quantity: int = Field(ge=1)
    price: float


class CreateOrderBody(BaseModel):
    """Cuerpo nuevo: client_name, client_rut, … Legacy: client { id, name, rut }."""

    client: OrderClient | None = None
    client_id: int | None = None
    client_name: str | None = None
    client_rut: str | None = None
    items: list[OrderItemIn]
    total: float
    price_list: str
    payment_method: str
    document_type: str | None = None
    contact_name: str
    contact_phone: str
    delivery_date: str | None = None
    notes: str | None = None


class UpdateOrderStatusBody(BaseModel):
    status: str

    @field_validator("status")
    @classmethod
    def validate_status(cls, v: object) -> str:
        if not isinstance(v, str):
            raise ValueError("status debe ser texto")
        s = v.strip().lower()
        if s not in _ALLOWED_ORDER_STATUSES:
            raise ValueError(
                "status debe ser pendiente, generado, anulado o revisar"
            )
        return s


def _delivery_date_value(raw: str | None) -> date | None:
    if not raw:
        return None
    s = raw.strip()
    if not s:
        return None
    try:
        return date.fromisoformat(s)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="delivery_date debe ser YYYY-MM-DD",
        )


def _resolve_client(body: CreateOrderBody) -> tuple[int | None, str | None, str | None]:
    if body.client is not None:
        return (
            body.client.id,
            (body.client.name or "").strip() or None,
            (body.client.rut or "").strip() or None,
        )
    cid = body.client_id
    name = (body.client_name or "").strip() or None
    rut = (body.client_rut or "").strip() or None
    if not name or not rut:
        raise HTTPException(
            status_code=400,
            detail="Indique client { id, name, rut } o client_name y client_rut",
        )
    return cid, name, rut


@router.get("/orders")
def get_orders():
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                id,
                client_name,
                client_rut,
                payment_method,
                price_list,
                delivery_date,
                total,
                status,
                created_at
            FROM app.orders
            ORDER BY created_at DESC
            """
        )
        rows = cur.fetchall()
        cur.close()
    finally:
        conn.close()

    out = []
    for r in rows:
        total_raw = r[6]
        total_f = float(total_raw) if total_raw is not None else 0.0
        created = r[8]
        created_str = created.isoformat() if created is not None else ""
        dd = r[5]
        delivery_str = dd.isoformat() if dd is not None else None
        out.append(
            {
                "id": r[0],
                "client_name": r[1],
                "rut": r[2],
                "payment_method": r[3],
                "price_list": r[4],
                "delivery_date": delivery_str,
                "total": total_f,
                "status": r[7],
                "created_at": created_str,
            }
        )
    return out


@router.get("/orders/{order_id}")
def get_order(order_id: int):
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                id,
                client_name,
                client_rut,
                payment_method,
                price_list,
                delivery_date,
                notes,
                contact_name,
                contact_phone,
                total,
                status,
                created_at
            FROM app.orders
            WHERE id = %s
            """,
            (order_id,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Pedido no encontrado")

        cur.execute(
            """
            SELECT product_name, barcode, quantity, price
            FROM app.order_items
            WHERE order_id = %s
            ORDER BY id ASC
            """,
            (order_id,),
        )
        item_rows = cur.fetchall()
        cur.close()
    finally:
        conn.close()

    total_raw = row[9]
    total_f = float(total_raw) if total_raw is not None else 0.0
    created = row[11]
    created_str = created.isoformat() if created is not None else ""
    dd = row[5]
    delivery_str = dd.isoformat() if dd is not None else None
    client_rut_val = row[2]

    items = []
    for ir in item_rows:
        p_name, bc, qty, price_raw = ir
        price_f = float(price_raw) if price_raw is not None else 0.0
        items.append(
            {
                "product_name": p_name,
                "barcode": bc,
                "quantity": int(qty) if qty is not None else 0,
                "price": price_f,
            }
        )

    return {
        "id": row[0],
        "client_name": row[1],
        "client_rut": client_rut_val,
        "rut": client_rut_val,
        "payment_method": row[3],
        "price_list": row[4],
        "delivery_date": delivery_str,
        "notes": row[6],
        "contact_name": row[7],
        "contact_phone": row[8],
        "total": total_f,
        "status": row[10],
        "created_at": created_str,
        "items": items,
    }


@router.put("/orders/{order_id}/status")
def update_order_status(order_id: int, body: UpdateOrderStatusBody):
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE app.orders
            SET status = %s
            WHERE id = %s
            RETURNING id, status
            """,
            (body.status, order_id),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Order not found")
        conn.commit()
    except HTTPException:
        conn.rollback()
        raise
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

    return {"id": row[0], "status": row[1]}


@router.post("/orders")
def create_order(body: CreateOrderBody):
    if not body.items:
        raise HTTPException(status_code=400, detail="items no puede estar vacío")

    phone = (body.contact_phone or "").strip()
    if not phone:
        raise HTTPException(status_code=400, detail="contact_phone es obligatorio")

    pay = (body.payment_method or "").strip()
    if not pay:
        raise HTTPException(status_code=400, detail="payment_method es obligatorio")

    doc = (body.document_type or "").strip() or None

    delivery = _delivery_date_value(body.delivery_date)

    client_id, client_name, client_rut = _resolve_client(body)

    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO app.orders (
                client_id,
                client_name,
                client_rut,
                price_list,
                payment_method,
                document_type,
                contact_name,
                contact_phone,
                delivery_date,
                notes,
                total,
                status
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                client_id,
                client_name,
                client_rut,
                (body.price_list or "").strip() or None,
                pay,
                doc,
                (body.contact_name or "").strip() or None,
                phone,
                delivery,
                (body.notes or "").strip() or None,
                Decimal(str(body.total)),
                "pendiente",
            ),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=500, detail="No se pudo crear el pedido")
        order_id = int(row[0])

        for it in body.items:
            qty = int(it.quantity)
            price_dec = Decimal(str(it.price))
            subtotal = price_dec * qty
            cur.execute(
                """
                INSERT INTO app.order_items (
                    order_id,
                    product_id,
                    product_name,
                    barcode,
                    quantity,
                    price,
                    subtotal
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    order_id,
                    it.id,
                    (it.name or "").strip() or None,
                    (it.barcode or "").strip() or None,
                    qty,
                    price_dec,
                    subtotal,
                ),
            )

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

    return {"status": "ok", "order_id": order_id}
