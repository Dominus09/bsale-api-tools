"""bsale.price_lists: listas que dejan de aparecer en Bsale se marcan inactivas, nunca se borran."""

from __future__ import annotations

from typing import Any

import pytest

from backend.services.bsale import catalog_company_sync as cat
from backend.services.bsale.sync_common import CompanySyncError
from backend.tests.bsale_sync_fakes import FakeConn


def snapshot(company_id: int, price_lists: list[tuple[int, str, int]]) -> cat.CatalogSnapshot:
    s = cat.CatalogSnapshot(company_id=company_id)
    s.price_lists = [(company_id, pid, name, state) for pid, name, state in price_lists]
    s.products = [(company_id, 10, "P", None, "[]", "[]", 1.0)]
    s.variants = [(company_id, 500, 10, "SKU", "780", "750cc")]
    return s


def conn_with(retired_ids: list[int], existing: int = 5) -> FakeConn:
    def handler(sql: str, params: Any) -> Any:
        if sql.startswith("UPDATE bsale.price_lists"):
            return [(pid,) for pid in retired_ids]
        if "COUNT(*)" in sql:
            return [(existing,)]
        return None

    return FakeConn(handler)


@pytest.fixture(autouse=True)
def no_batch(monkeypatch):
    monkeypatch.setattr(cat, "execute_batch", lambda cur, sql, rows, page_size=None: None)


def test_price_list_missing_from_bsale_is_marked_inactive():
    conn = conn_with(retired_ids=[14])
    s = snapshot(3, [(12, "SUPERMERCADO", 0), (13, "RUTA/WEB", 0), (16, "MELINKA", 0), (2, "Base", 1)])
    res = cat.persist_company_catalog(conn, s)
    assert res["price_lists_retired"] == [14]
    updates = conn.sql_matching("UPDATE bsale.price_lists")
    assert len(updates) == 1
    sql, params = updates[0]
    assert "SET state = %s" in sql and "NOT (bsale_id = ANY(%s))" in sql
    assert "state IS DISTINCT FROM %s" in sql
    assert params == (cat.PRICE_LIST_STATE_INACTIVE, 3, [2, 12, 13, 16], cat.PRICE_LIST_STATE_INACTIVE)
    assert cat.PRICE_LIST_STATE_INACTIVE == 1
    assert conn.commits == 1 and conn.rollbacks == 0


def test_price_lists_are_never_deleted():
    conn = conn_with(retired_ids=[4])
    cat.persist_company_catalog(conn, snapshot(1, [(2, "Minimarket", 0), (3, "Ruta/Web", 0)]))
    assert conn.sql_matching("DELETE") == []


def test_all_lists_present_retires_nothing():
    conn = conn_with(retired_ids=[])
    res = cat.persist_company_catalog(conn, snapshot(2, [(3, "QUILLOTANA V", 0), (13, "RUTA/WEB", 0)]))
    assert res["price_lists_retired"] == []
    assert conn.commits == 1


def test_empty_price_lists_with_existing_rows_fails_without_retiring():
    conn = conn_with(retired_ids=[2, 3, 4], existing=3)
    with pytest.raises(CompanySyncError, match="price_lists=0"):
        cat.persist_company_catalog(conn, snapshot(1, []))
    assert conn.sql_matching("UPDATE bsale.price_lists") == []
    assert conn.commits == 0 and conn.rollbacks == 1
