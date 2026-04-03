from datetime import date
from decimal import Decimal

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from backend.db import get_connection

router = APIRouter()


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
    client: OrderClient
    items: list[OrderItemIn]
    total: float
    price_list: str
    payment_method: str
    document_type: str
    contact_name: str
    contact_phone: str
    delivery_date: str | None = None
    notes: str | None = None


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

    doc = (body.document_type or "").strip()
    if not doc:
        raise HTTPException(status_code=400, detail="document_type es obligatorio")

    delivery: date | None = None
    if body.delivery_date:
        raw = body.delivery_date.strip()
        if raw:
            try:
                delivery = date.fromisoformat(raw)
            except ValueError:
                raise HTTPException(
                    status_code=400,
                    detail="delivery_date debe ser YYYY-MM-DD",
                )

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
                body.client.id,
                (body.client.name or "").strip() or None,
                (body.client.rut or "").strip() or None,
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
