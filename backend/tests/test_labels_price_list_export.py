"""GET /labels/price-list-export: todas las filas de bsale.variant_prices (company + lista), solo lectura."""

from __future__ import annotations

from decimal import Decimal

from backend.routers import labels


class FakeCursor:
    def __init__(self, rows):
        self.rows = rows
        self.executed: list[tuple[str, tuple]] = []

    def execute(self, sql, params):
        self.executed.append((sql, params))

    def fetchall(self):
        return self.rows

    def close(self):
        pass


class FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor
        self.closed = False
        self.committed = False

    def cursor(self):
        return self._cursor

    def commit(self):
        self.committed = True

    def close(self):
        self.closed = True


def test_export_returns_all_rows_with_price_gross(monkeypatch):
    rows = [
        ("Abarrotes", "Arroz", "1 kg", " 7802100505323 ", "ARR1", Decimal("1990.00"), "Lista 12", 10888),
        (None, None, None, None, None, None, None, 10892),
    ]
    cur = FakeCursor(rows)
    conn = FakeConnection(cur)
    monkeypatch.setattr(labels, "get_connection", lambda: conn)

    body = labels.export_price_list(company_id=3, price_list_id=12)

    assert body["company_id"] == 3 and body["price_list_id"] == 12
    assert body["rows"] == [
        {"product_type": "Abarrotes", "product_name": "Arroz", "variant_name": "1 kg",
         "barcode": "7802100505323", "sku": "ARR1", "price_gross": 1990.0,
         "price_list_name": "Lista 12", "variant_id": 10888},
        {"product_type": None, "product_name": None, "variant_name": None, "barcode": None, "sku": None,
         "price_gross": None, "price_list_name": None, "variant_id": 10892},
    ]
    ((sql, params),) = cur.executed
    assert params == (3, 12)
    assert "FROM bsale.variant_prices vp" in sql and "vp.price_gross" in sql
    assert "COALESCE" not in sql and "LIMIT" not in sql and "ORDER BY p.name" in sql
    assert not any(k in sql.upper() for k in ("INSERT", "UPDATE", "DELETE"))
    assert conn.closed and not conn.committed


def test_label_lookup_sql_unchanged():
    assert "COALESCE(vp.price_gross, vp.price_net)" in labels._LABEL_PRODUCT_SQL
    assert "LIMIT 1" in labels._LABEL_PRODUCT_SQL
