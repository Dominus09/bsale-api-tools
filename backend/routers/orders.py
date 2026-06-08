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

# Lista de precios (catálogo): slug → ID Bsale. Independiente del tipo de documento tributario.
_PRICE_LIST_SLUG_TO_ID: dict[str, int] = {
    "factura": 13,
    "comoditi": 14,
    "melinka": 16,
}
_ALLOWED_PRICE_LISTS = frozenset(_PRICE_LIST_SLUG_TO_ID)

# Tipo de documento solicitado en checkout (tributario): no se cruza con lista de precios.
_ALLOWED_DOCUMENT_TYPES = frozenset({"factura", "boleta"})

_ORDERS_COLUMNS_CACHE: frozenset[str] | None = None


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


def _orders_table_columns(cur, *, refresh: bool = False) -> frozenset[str]:
    """Columnas reales de ``app.orders`` (sin asumir migraciones pendientes)."""
    global _ORDERS_COLUMNS_CACHE
    if _ORDERS_COLUMNS_CACHE is not None and not refresh:
        return _ORDERS_COLUMNS_CACHE
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'app'
          AND table_name = 'orders'
        """
    )
    _ORDERS_COLUMNS_CACHE = frozenset(r[0] for r in cur.fetchall())
    return _ORDERS_COLUMNS_CACHE


def _pick_order_columns(
    available: frozenset[str],
    names: list[str],
) -> list[str]:
    return [c for c in names if c in available]


def _row_to_dict(cur, row) -> dict:
    colnames = [d[0] for d in cur.description]
    return dict(zip(colnames, row))


def _price_list_id_from_slug(slug: str | None) -> int | None:
    if not slug or not str(slug).strip():
        return None
    return _PRICE_LIST_SLUG_TO_ID.get(str(slug).strip().lower())


def _resolve_price_list_commercial(body: CreateOrderBody) -> str:
    slug = (body.price_list or "").strip().lower() or None
    if not slug:
        raise HTTPException(status_code=400, detail="price_list es obligatorio")
    if slug not in _ALLOWED_PRICE_LISTS:
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
    return slug


def _resolve_document_type(raw: str | None) -> str:
    """Tipo documento tributario (factura / boleta), independiente de ``price_list``."""
    doc = (raw or "").strip().lower() or None
    if not doc:
        raise HTTPException(status_code=400, detail="document_type es obligatorio")
    if doc not in _ALLOWED_DOCUMENT_TYPES:
        raise HTTPException(
            status_code=400,
            detail="document_type debe ser factura o boleta",
        )
    return doc


def _commercial_fields_from_row(row: dict) -> dict[str, str | int | None]:
    pl = (str(row.get("price_list") or "").strip() or None)
    doc = (str(row.get("document_type") or "").strip() or None)
    pay = (str(row.get("payment_method") or "").strip() or None)
    seller = (str(row.get("seller_name") or "").strip() or None)
    return {
        "price_list": pl,
        # Nunca se lee de BD: la columna puede no existir; se deriva del slug.
        "price_list_id": _price_list_id_from_slug(pl),
        "document_type": doc or None,
        "payment_method": pay or None,
        "seller_name": seller or None,
    }


def _seller_fields_from_row(row: dict) -> dict[str, int | str | None]:
    sid = row.get("seller_id")
    sname = row.get("seller_name")
    city = row.get("client_city")
    return {
        "seller_id": int(sid) if sid is not None else None,
        "seller_name": (str(sname).strip() if sname is not None else None) or None,
        "client_city": (str(city).strip() if city is not None else None) or None,
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


def _insert_order(
    cur,
    *,
    available_cols: frozenset[str],
    values: dict[str, object],
) -> int:
    """INSERT solo con columnas que existen en ``app.orders``."""
    skip = frozenset({"price_list_id", "price_list_name"})
    cols = [k for k in values if k in available_cols and k not in skip]
    if not cols:
        raise HTTPException(status_code=500, detail="Sin columnas válidas para insertar pedido")
    placeholders = ", ".join("%s" for _ in cols)
    col_sql = ", ".join(cols)
    cur.execute(
        f"""
        INSERT INTO app.orders ({col_sql})
        VALUES ({placeholders})
        RETURNING id
        """,
        tuple(values[c] for c in cols),
    )
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=500, detail="No se pudo crear el pedido")
    return int(row[0])


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

    conn = get_connection()
    try:
        cur = conn.cursor()
        available = _orders_table_columns(cur)
        select_cols = _pick_order_columns(
            available,
            [
                "id",
                "client_name",
                "client_rut",
                "payment_method",
                "price_list",
                "document_type",
                "delivery_date",
                "total",
                "status",
                "seller_id",
                "seller_name",
                "client_city",
                "created_at",
            ],
        )
        if not select_cols:
            raise HTTPException(status_code=500, detail="Tabla app.orders no disponible")

        query = f"""
            SELECT {", ".join(select_cols)}
            FROM app.orders
        """
        params: list = []

        if status_filter is not None:
            query += " WHERE status = %s"
            params.append(status_filter)

        query += " ORDER BY created_at DESC LIMIT %s OFFSET %s"
        params.extend([limit, offset])

        cur.execute(query, tuple(params))
        rows = [_row_to_dict(cur, r) for r in cur.fetchall()]
        cur.close()
    finally:
        conn.close()

    out = []
    for row in rows:
        total_raw = row.get("total")
        total_f = float(total_raw) if total_raw is not None else 0.0
        created = row.get("created_at")
        created_str = created.isoformat() if created is not None else ""
        dd = row.get("delivery_date")
        delivery_str = dd.isoformat() if dd is not None else None
        out.append(
            {
                "id": row.get("id"),
                "client_name": row.get("client_name"),
                "rut": row.get("client_rut"),
                "delivery_date": delivery_str,
                "total": total_f,
                "status": row.get("status"),
                **_commercial_fields_from_row(row),
                **_seller_fields_from_row(row),
                "created_at": created_str,
            }
        )
    return out


@router.get("/orders/{order_id}")
def get_order(order_id: int):
    conn = get_connection()
    try:
        cur = conn.cursor()
        available = _orders_table_columns(cur)
        select_cols = _pick_order_columns(
            available,
            [
                "id",
                "client_name",
                "client_rut",
                "payment_method",
                "price_list",
                "document_type",
                "delivery_date",
                "notes",
                "contact_name",
                "contact_phone",
                "total",
                "status",
                "seller_id",
                "seller_name",
                "client_city",
                "created_at",
            ],
        )
        col_sql = ", ".join(select_cols)
        cur.execute(
            f"""
            SELECT {col_sql}
            FROM app.orders
            WHERE id = %s
            """,
            (order_id,),
        )
        raw = cur.fetchone()
        if not raw:
            raise HTTPException(status_code=404, detail="Pedido no encontrado")
        row = _row_to_dict(cur, raw)

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

    total_raw = row.get("total")
    total_f = float(total_raw) if total_raw is not None else 0.0
    created = row.get("created_at")
    created_str = created.isoformat() if created is not None else ""
    dd = row.get("delivery_date")
    delivery_str = dd.isoformat() if dd is not None else None
    client_rut_val = row.get("client_rut")

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
        "id": row.get("id"),
        "client_name": row.get("client_name"),
        "client_rut": client_rut_val,
        "rut": client_rut_val,
        "delivery_date": delivery_str,
        "notes": row.get("notes"),
        "contact_name": row.get("contact_name"),
        "contact_phone": row.get("contact_phone"),
        "total": total_f,
        "status": row.get("status"),
        **_commercial_fields_from_row(row),
        **_seller_fields_from_row(row),
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

    document_type_slug = _resolve_document_type(body.document_type)
    price_list_slug = _resolve_price_list_commercial(body)

    delivery = _delivery_date_value(body.delivery_date)

    client_id, client_name, client_rut = _resolve_client(body)

    conn = get_connection()
    cur = conn.cursor()
    try:
        available = _orders_table_columns(cur)
        seller_id, seller_name, client_city = _lookup_client_seller_and_city(
            cur,
            client_id=client_id,
            client_rut=client_rut,
        )

        insert_values: dict[str, object] = {
            "client_id": client_id,
            "client_name": client_name,
            "client_rut": client_rut,
            "price_list": price_list_slug,
            "payment_method": pay,
            "document_type": document_type_slug,
            "contact_name": (body.contact_name or "").strip() or None,
            "contact_phone": phone,
            "delivery_date": delivery,
            "notes": (body.notes or "").strip() or None,
            "total": Decimal(str(body.total)),
            "status": "pendiente",
            "seller_id": seller_id,
            "seller_name": seller_name,
            "client_city": client_city,
        }

        order_id = _insert_order(cur, available_cols=available, values=insert_values)

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
