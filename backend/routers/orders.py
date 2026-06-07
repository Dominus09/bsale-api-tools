from datetime import date
from decimal import Decimal

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field, field_validator

from backend.client_rut import clean_rut_for_lookup
from backend.db import get_connection

router = APIRouter(tags=["Pedidos"])

# Empresa catálogo / distribuidora web (misma que sync_clients y rutero).
_CLIENT_COMPANY_ID = 3

_ALLOWED_ORDER_STATUSES = frozenset({"pendiente", "generado", "anulado", "revisar"})

# Misma correspondencia que GET /api/catalog (slug → lista Bsale).
_PRICE_LIST_SLUG_TO_ID: dict[str, int] = {
    "factura": 13,
    "comoditi": 14,
    "melinka": 16,
}


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
    price_list_id: int | None = None
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


def _price_list_id_from_slug(slug: str | None) -> int | None:
    if not slug or not str(slug).strip():
        return None
    return _PRICE_LIST_SLUG_TO_ID.get(str(slug).strip().lower())


def _effective_price_list_id(
    slug: str | None,
    stored_id: int | None,
) -> int | None:
    if stored_id is not None:
        return int(stored_id)
    return _price_list_id_from_slug(slug)


def _resolve_price_list_commercial(
    body: CreateOrderBody,
) -> tuple[str | None, int | None]:
    slug = (body.price_list or "").strip().lower() or None
    if not slug:
        raise HTTPException(status_code=400, detail="price_list es obligatorio")
    if slug not in _PRICE_LIST_SLUG_TO_ID:
        raise HTTPException(
            status_code=400,
            detail="price_list debe ser factura, comoditi o melinka",
        )

    derived_id = _PRICE_LIST_SLUG_TO_ID[slug]
    if body.price_list_id is not None and int(body.price_list_id) != derived_id:
        raise HTTPException(
            status_code=400,
            detail="price_list_id no coincide con price_list",
        )
    return slug, derived_id


def _commercial_fields_from_row(
    *,
    price_list: str | None,
    price_list_id_raw,
    document_type_raw,
    payment_method_raw,
    seller_name_raw,
) -> dict[str, str | int | None]:
    pl = (str(price_list).strip() if price_list is not None else None) or None
    plid = _effective_price_list_id(
        pl,
        int(price_list_id_raw) if price_list_id_raw is not None else None,
    )
    doc = (
        str(document_type_raw).strip() if document_type_raw is not None else None
    ) or None
    pay = (
        str(payment_method_raw).strip() if payment_method_raw is not None else None
    ) or None
    seller = (
        str(seller_name_raw).strip() if seller_name_raw is not None else None
    ) or None
    return {
        "price_list": pl,
        "price_list_id": plid,
        "document_type": doc,
        "payment_method": pay,
        "seller_name": seller,
    }


def _lookup_client_seller_and_city(
    cur,
    *,
    client_id: int | None,
    client_rut: str | None,
) -> tuple[int | None, str | None, str | None]:
    """
    Vendedor asignado al cliente desde bsale.clients.vendedor (atributo Bsale «Vendedor»)
    enriquecido con bsale.vendedores_app (nombre legible e id interno).
    """
    rut_clean: str | None = None
    if client_rut and str(client_rut).strip():
        rut_clean = clean_rut_for_lookup(str(client_rut).strip())

    if client_id is None and not rut_clean:
        return None, None, None

    cur.execute(
        """
        SELECT
            NULLIF(BTRIM(c.city), '') AS client_city,
            va.id AS seller_id,
            COALESCE(
                NULLIF(BTRIM(va.nombre), ''),
                NULLIF(BTRIM(c.vendedor::text), '')
            ) AS seller_name
        FROM bsale.clients c
        LEFT JOIN bsale.vendedores_app va
            ON LOWER(TRIM(COALESCE(va.codigo, '')))
             = LOWER(TRIM(COALESCE(c.vendedor::text, '')))
           AND va.activo IS TRUE
        WHERE c.company_id = %s
          AND (
              (%s IS NOT NULL AND c.bsale_id = %s)
              OR (%s IS NOT NULL AND c.rut_clean = %s)
          )
        ORDER BY
            CASE WHEN %s IS NOT NULL AND c.bsale_id = %s THEN 0 ELSE 1 END,
            c.bsale_id
        LIMIT 1
        """,
        (
            _CLIENT_COMPANY_ID,
            client_id,
            client_id,
            rut_clean,
            rut_clean,
            client_id,
            client_id,
        ),
    )
    row = cur.fetchone()
    if not row:
        return None, None, None
    city, seller_id, seller_name = row
    sid = int(seller_id) if seller_id is not None else None
    return sid, (str(seller_name).strip() if seller_name else None), (
        str(city).strip() if city else None
    )


def _seller_fields_from_row(
    seller_id_raw,
    seller_name_raw,
    client_city_raw,
) -> dict[str, int | str | None]:
    return {
        "seller_id": int(seller_id_raw) if seller_id_raw is not None else None,
        "seller_name": (
            str(seller_name_raw).strip() if seller_name_raw is not None else None
        )
        or None,
        "client_city": (
            str(client_city_raw).strip() if client_city_raw is not None else None
        )
        or None,
    }


@router.get("/orders")
def get_orders(
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    status: str | None = Query(None),
):
    status_filter: str | None = None
    if status is not None and status.strip():
        s = status.strip().lower()
        if s not in _ALLOWED_ORDER_STATUSES:
            raise HTTPException(
                status_code=400,
                detail="status debe ser pendiente, generado, anulado o revisar",
            )
        status_filter = s

    offset = (page - 1) * limit

    query = """
        SELECT
            id,
            client_name,
            client_rut,
            payment_method,
            price_list,
            price_list_id,
            document_type,
            delivery_date,
            total,
            status,
            seller_id,
            seller_name,
            client_city,
            created_at
        FROM app.orders
    """
    params: list = []

    if status_filter is not None:
        query += " WHERE status = %s"
        params.append(status_filter)

    query += " ORDER BY created_at DESC LIMIT %s OFFSET %s"
    params.extend([limit, offset])

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(query, tuple(params))
        rows = cur.fetchall()
        cur.close()
    finally:
        conn.close()

    out = []
    for r in rows:
        total_raw = r[8]
        total_f = float(total_raw) if total_raw is not None else 0.0
        created = r[13]
        created_str = created.isoformat() if created is not None else ""
        dd = r[7]
        delivery_str = dd.isoformat() if dd is not None else None
        seller_extra = _seller_fields_from_row(r[10], r[11], r[12])
        commercial = _commercial_fields_from_row(
            price_list=r[4],
            price_list_id_raw=r[5],
            document_type_raw=r[6],
            payment_method_raw=r[3],
            seller_name_raw=r[11],
        )
        out.append(
            {
                "id": r[0],
                "client_name": r[1],
                "rut": r[2],
                "delivery_date": delivery_str,
                "total": total_f,
                "status": r[9],
                **commercial,
                **seller_extra,
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
                price_list_id,
                document_type,
                delivery_date,
                notes,
                contact_name,
                contact_phone,
                total,
                status,
                seller_id,
                seller_name,
                client_city,
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

    total_raw = row[11]
    total_f = float(total_raw) if total_raw is not None else 0.0
    created = row[16]
    created_str = created.isoformat() if created is not None else ""
    dd = row[7]
    delivery_str = dd.isoformat() if dd is not None else None
    client_rut_val = row[2]
    seller_extra = _seller_fields_from_row(row[13], row[14], row[15])
    commercial = _commercial_fields_from_row(
        price_list=row[4],
        price_list_id_raw=row[5],
        document_type_raw=row[6],
        payment_method_raw=row[3],
        seller_name_raw=row[14],
    )

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
        "delivery_date": delivery_str,
        "notes": row[8],
        "contact_name": row[9],
        "contact_phone": row[10],
        "total": total_f,
        "status": row[12],
        **commercial,
        **seller_extra,
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
    if not doc:
        raise HTTPException(status_code=400, detail="document_type es obligatorio")
    if doc.lower() in _PRICE_LIST_SLUG_TO_ID:
        raise HTTPException(
            status_code=400,
            detail="document_type no puede ser una lista de precios",
        )

    price_list_slug, price_list_id = _resolve_price_list_commercial(body)

    delivery = _delivery_date_value(body.delivery_date)

    client_id, client_name, client_rut = _resolve_client(body)

    conn = get_connection()
    cur = conn.cursor()
    try:
        seller_id, seller_name, client_city = _lookup_client_seller_and_city(
            cur,
            client_id=client_id,
            client_rut=client_rut,
        )

        cur.execute(
            """
            INSERT INTO app.orders (
                client_id,
                client_name,
                client_rut,
                price_list,
                price_list_id,
                payment_method,
                document_type,
                contact_name,
                contact_phone,
                delivery_date,
                notes,
                total,
                status,
                seller_id,
                seller_name,
                client_city
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                client_id,
                client_name,
                client_rut,
                price_list_slug,
                price_list_id,
                pay,
                doc,
                (body.contact_name or "").strip() or None,
                phone,
                delivery,
                (body.notes or "").strip() or None,
                Decimal(str(body.total)),
                "pendiente",
                seller_id,
                seller_name,
                client_city,
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
