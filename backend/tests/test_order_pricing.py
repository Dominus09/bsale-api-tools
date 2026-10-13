"""Pedidos del catálogo: precios y total se calculan en backend desde la lista vigente (13/16)."""

from __future__ import annotations

from decimal import Decimal

import pytest
from fastapi import HTTPException

from backend.routers import auth as auth_router
from backend.routers import orders
from backend.utils.catalog_client import lookup_catalog_client
from backend.utils.order_pricing import CatalogPrice, fetch_catalog_prices, price_order_items

# variant_id → (nombre, barcode, price_13, price_16)
_CATALOG = {
    101: ("ARROZ 1 KG", "780001", Decimal("1200"), Decimal("1250")),
    102: ("ACEITE 1 LT", "780002", Decimal("2500"), Decimal("2600")),
    103: ("CRIOLLITAS", "780003", Decimal("919.87"), Decimal("920.0")),
    104: ("SIN PRECIO", "780004", None, None),
    105: ("PRECIO CERO", "780005", Decimal("0"), Decimal("0")),
}

# (bsale_id, company_id, first_name, last_name, city, seller_id, seller_name)
_CLIENT_QUELLON = (500, 3, "Almacén", "Sur", "Quellón", 7, "Juan")
_CLIENT_MELINKA = (600, 3, "Almacén", "Melinka", "MELINKA", 8, "Pedro")
_CLIENT_NO_CITY = (700, 3, "Sin", "Ciudad", None, None, None)


class _FakeCursor:
    def __init__(self, db: "_FakeDb"):
        self.db = db
        self._result: list[tuple] = []

    def execute(self, sql: str, params=None):
        self.db.executed.append((sql, params))
        if "FROM bsale.clients" in sql:
            self._result = [self.db.client] if self.db.client else []
        elif "FROM bsale.catalog_view" in sql:
            col = 2 if "price_13" in sql else 3
            assert "price_14" not in sql
            ids = params[0]
            self._result = [
                (vid, name, bc, (p13, p16)[col - 2])
                for vid, (name, bc, p13, p16) in self.db.catalog.items()
                if vid in ids
            ]
        elif "information_schema.columns" in sql:
            self._result = [(c,) for c in self.db.order_columns]
        elif "INSERT INTO app.orders" in sql:
            self.db.inserted_order = (sql, params)
            self._result = [(9001,)]
        elif "INSERT INTO app.order_items" in sql:
            self.db.inserted_items.append(params)
            self._result = []
        else:  # pragma: no cover - consulta inesperada
            raise AssertionError(f"SQL inesperado: {sql}")

    def fetchone(self):
        return self._result[0] if self._result else None

    def fetchall(self):
        return list(self._result)

    def close(self):
        pass


class _FakeDb:
    order_columns = (
        "client_id client_name client_rut price_list payment_method document_type "
        "contact_name contact_phone delivery_date notes total status seller_id "
        "seller_name client_city"
    ).split()

    def __init__(self, client=_CLIENT_QUELLON, catalog=None):
        self.client = client
        self.catalog = dict(_CATALOG if catalog is None else catalog)
        self.executed: list = []
        self.inserted_order = None
        self.inserted_items: list = []
        self.committed = False

    def cursor(self):
        return _FakeCursor(self)

    def commit(self):
        self.committed = True

    def rollback(self):
        pass

    def close(self):
        pass

    @property
    def order_values(self) -> dict:
        sql, params = self.inserted_order
        cols = sql.split("(", 1)[1].split(")", 1)[0]
        return dict(zip([c.strip() for c in cols.split(",")], params))


@pytest.fixture
def db(monkeypatch):
    fake = _FakeDb()
    monkeypatch.setattr(orders, "get_connection", lambda: fake)
    monkeypatch.setattr(orders, "validate_order_items", lambda cur, items: [])
    monkeypatch.setattr(orders, "_ORDERS_COLUMNS_CACHE", None)
    return fake


def _body(items, total, price_list="factura", **extra) -> orders.CreateOrderBody:
    return orders.CreateOrderBody(
        client={"id": 500, "name": "Almacén Sur", "rut": "11111111-1"},
        items=[
            {"id": vid, "name": f"item {vid}", "barcode": "", "quantity": q, "price": p}
            for vid, q, p in items
        ],
        total=total,
        price_list=price_list,
        payment_method="transferencia",
        document_type="factura",
        contact_name="",
        contact_phone="+56 9 1234 5678",
        **extra,
    )


def _detail(exc: pytest.ExceptionInfo) -> dict:
    assert isinstance(exc.value.detail, dict)
    return exc.value.detail


# --- Cálculo puro --------------------------------------------------------------


def _catalog_13() -> dict[int, CatalogPrice]:
    return {
        vid: CatalogPrice(vid, name, bc, p13)
        for vid, (name, bc, p13, _p16) in _CATALOG.items()
    }


def test_pricing_uses_catalog_price_and_exact_decimal_total():
    result = price_order_items(
        [{"id": 103, "name": "x", "quantity": 3, "price": 919.87}],
        _catalog_13(),
        sent_total=919.87 * 3,  # 2759.6099999999997 en float
    )
    assert result.ok
    assert result.lines[0].price == Decimal("919.87")
    assert result.total == Decimal("2759.61")


def test_pricing_flags_manipulated_price():
    result = price_order_items(
        [{"id": 101, "name": "x", "quantity": 2, "price": 1}],
        _catalog_13(),
        sent_total=2,
    )
    assert not result.ok
    issue = result.price_issues[0]
    assert issue["reason"] == "precio_distinto"
    assert issue["current_price"] == 1200.0


def test_pricing_flags_manipulated_total_with_correct_prices():
    result = price_order_items(
        [{"id": 101, "name": "x", "quantity": 2, "price": 1200}],
        _catalog_13(),
        sent_total=100,
    )
    assert result.price_issues == []
    assert result.total_matches is False
    assert result.total == Decimal("2400")


@pytest.mark.parametrize("vid", [104, 105, 999])
def test_pricing_flags_products_without_current_price(vid):
    result = price_order_items(
        [{"id": vid, "name": "x", "quantity": 1, "price": 1000}],
        _catalog_13(),
        sent_total=1000,
    )
    assert [i["reason"] for i in result.price_issues] == ["sin_precio"]
    assert result.lines == []


def test_fetch_catalog_prices_never_reads_list_14(db):
    cur = db.cursor()
    with pytest.raises(ValueError):
        fetch_catalog_prices(cur, [101], "comoditi")
    assert fetch_catalog_prices(cur, [101], "melinka")[101].price == Decimal("1250")


# --- POST /orders -------------------------------------------------------------


def test_order_list_13_ok_stores_backend_prices_and_total(db):
    res = orders.create_order(_body([(101, 2, 1200), (102, 1, 2500)], total=4900))
    assert res["order_id"] == 9001
    assert res["price_list_id"] == 13
    assert db.committed
    values = db.order_values
    assert values["total"] == Decimal("4900")
    assert values["price_list"] == "factura"
    assert values["client_city"] == "Quellón"
    assert values["seller_name"] == "Juan"
    # (order_id, product_id, name, barcode, qty, price, subtotal) desde catálogo
    assert db.inserted_items == [
        (9001, 101, "ARROZ 1 KG", "780001", 2, Decimal("1200"), Decimal("2400")),
        (9001, 102, "ACEITE 1 LT", "780002", 1, Decimal("2500"), Decimal("2500")),
    ]


def test_order_list_16_ok_for_melinka(db):
    db.client = _CLIENT_MELINKA
    res = orders.create_order(
        _body([(101, 2, 1250), (103, 1, 920)], total=3420, price_list="melinka")
    )
    assert res["price_list_id"] == 16
    assert db.order_values["total"] == Decimal("3420")
    assert [it[5] for it in db.inserted_items] == [Decimal("1250"), Decimal("920.0")]


def test_order_rejects_manipulated_price(db):
    with pytest.raises(HTTPException) as exc:
        orders.create_order(_body([(101, 2, 10)], total=20))
    assert exc.value.status_code == 409
    detail = _detail(exc)
    assert detail["error"] == "price_mismatch"
    assert detail["items"][0]["current_price"] == 1200.0
    assert db.inserted_order is None and not db.committed


def test_order_rejects_manipulated_total(db):
    with pytest.raises(HTTPException) as exc:
        orders.create_order(_body([(101, 2, 1200)], total=1))
    detail = _detail(exc)
    assert exc.value.status_code == 409
    assert detail["total_matches"] is False
    assert detail["expected_total"] == 2400.0
    assert db.inserted_order is None


def test_order_rejects_outdated_price(db):
    """Carrito cotizado a 1200; el precio vigente en Bsale subió a 1300."""
    db.catalog[101] = ("ARROZ 1 KG", "780001", Decimal("1300"), Decimal("1350"))
    with pytest.raises(HTTPException) as exc:
        orders.create_order(_body([(101, 1, 1200)], total=1200))
    detail = _detail(exc)
    assert detail["items"] == [
        {
            "id": 101,
            "name": "ARROZ 1 KG",
            "reason": "precio_distinto",
            "sent_price": 1200.0,
            "current_price": 1300.0,
        }
    ]
    assert db.inserted_order is None


def test_order_rejects_product_without_current_price(db):
    with pytest.raises(HTTPException) as exc:
        orders.create_order(_body([(101, 1, 1200), (104, 1, 500)], total=1700))
    detail = _detail(exc)
    assert [i["reason"] for i in detail["items"]] == ["sin_precio"]
    assert detail["expected_total"] is None
    assert db.inserted_order is None


@pytest.mark.parametrize(
    "client,price_list,expected",
    [
        (_CLIENT_QUELLON, "melinka", "factura"),
        (_CLIENT_NO_CITY, "melinka", "factura"),
        (_CLIENT_MELINKA, "factura", "melinka"),
    ],
)
def test_order_rejects_price_list_not_matching_city(db, client, price_list, expected):
    db.client = client
    with pytest.raises(HTTPException) as exc:
        orders.create_order(_body([(101, 1, 1200)], total=1200, price_list=price_list))
    detail = _detail(exc)
    assert exc.value.status_code == 409
    assert detail["error"] == "price_list_mismatch"
    assert detail["expected_price_list"] == expected
    assert db.inserted_order is None


@pytest.mark.parametrize(
    "price_list,price_list_id",
    [("comoditi", None), ("COMODITI", None), ("factura", 14)],
)
def test_order_rejects_comoditi_before_touching_db(db, price_list, price_list_id):
    with pytest.raises(HTTPException) as exc:
        orders.create_order(
            _body([(101, 1, 1200)], total=1200, price_list=price_list, price_list_id=price_list_id)
        )
    assert exc.value.status_code == 400
    assert db.executed == []


def test_order_rejects_unregistered_client(db):
    db.client = None
    with pytest.raises(HTTPException) as exc:
        orders.create_order(_body([(101, 1, 1200)], total=1200))
    assert exc.value.status_code == 400
    assert db.inserted_order is None


def test_order_rejects_duplicated_lines(db):
    with pytest.raises(HTTPException) as exc:
        orders.create_order(_body([(101, 1, 1200), (101, 1, 1200)], total=2400))
    assert exc.value.status_code == 400


# --- Identificación del cliente (login y pedidos usan la misma fila) ----------


def test_client_lookup_prefers_catalog_company_and_session_id(db):
    cur = db.cursor()
    lookup_catalog_client(cur, rut_clean="11111111-1", client_id=500)
    sql, params = db.executed[-1]
    assert "(c.company_id = %s) DESC" in sql
    assert params == ("11111111-1", 3, 500, 500)


def test_client_seller_only_from_catalog_company(db):
    db.client = (900, 2, "Otra", "Empresa", "melinca", 3, "Vendedor empresa 2")
    client = lookup_catalog_client(db.cursor(), rut_clean="22222222-2")
    assert client.seller_id is None and client.seller_name is None
    assert client.city == "melinca"


def test_login_client_uses_shared_lookup(monkeypatch, db):
    db.client = _CLIENT_MELINKA
    monkeypatch.setattr(auth_router, "get_connection", lambda: db)
    res = auth_router.login_client(auth_router.LoginClientRequest(rut="11111111-1"))
    assert res["id"] == 600
    assert res["is_melinka"] is True
    assert "(c.company_id = %s) DESC" in db.executed[-1][0]


# --- Histórico ----------------------------------------------------------------


def test_historical_comoditi_order_read_unchanged(monkeypatch):
    class _HistCursor:
        description = None

        def __init__(self):
            self._rows = []

        def execute(self, sql, params=None):
            if "information_schema.columns" in sql:
                self._rows = [("id",), ("price_list",), ("total",), ("status",)]
                self.description = [("column_name",)]
            elif "FROM app.orders" in sql:
                self._rows = [(4, "comoditi", Decimal("271440.0"), "generado")]
                self.description = [("id",), ("price_list",), ("total",), ("status",)]
            elif "FROM app.order_items" in sql:
                self._rows = [("PRODUCTO", "780009", 12, Decimal("22620.0"))]
            else:  # pragma: no cover
                raise AssertionError(sql)

        def fetchone(self):
            return self._rows[0] if self._rows else None

        def fetchall(self):
            return list(self._rows)

        def close(self):
            pass

    class _HistConn:
        def cursor(self):
            return _HistCursor()

        def close(self):
            pass

    monkeypatch.setattr(orders, "get_connection", lambda: _HistConn())
    monkeypatch.setattr(orders, "_ORDERS_COLUMNS_CACHE", None)
    order = orders.get_order(4)
    assert order["price_list"] == "comoditi"
    assert order["price_list_id"] == 14
    assert order["total"] == 271440.0
    assert order["items"][0]["price"] == 22620.0
