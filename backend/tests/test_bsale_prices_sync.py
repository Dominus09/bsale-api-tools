"""Sync de precios por lista: reconciliación sólo de listas completas y desacople de costos."""

from __future__ import annotations

from typing import Any

import pytest

from backend.services.bsale import prices_costs_sync as pc
from backend.services.bsale import snapshot_reconcile as snap
from backend.services.bsale.companies import BsaleCompany
from backend.services.bsale.http_client import BsalePaginationError, BsaleRetryExhaustedError
from backend.services.bsale.sync_common import CompanySyncError
from backend.tests.bsale_sync_fakes import FakeBsaleClient, FakeConn

COMPANY = BsaleCompany(company_id=1, name="Minimarkets", token_env="BSALE_TOKEN_Mini", token="t")


def det(variant_id: int, net: float = 100, gross: float = 119) -> dict[str, Any]:
    return {"variant": {"id": variant_id}, "variantValue": net, "variantValueWithTaxes": gross}


class PricesDb:
    """PostgreSQL falso: precios existentes por lista, stale configurable y registro de conexiones."""

    def __init__(self, existing_by_list: dict[int, int], stale: int = 0) -> None:
        self.existing_by_list = existing_by_list
        self.stale = stale
        self.conns: list[FakeConn] = []

    def handler(self, sql: str, params: Any) -> Any:
        if "FROM bsale.variants" in sql:
            return [(1,)]
        if "GROUP BY price_list_id" in sql:
            return list(self.existing_by_list.items())
        if "COUNT(*)" in sql and "NOT EXISTS" in sql:
            return [(self.stale,)]
        if "COUNT(*)" in sql and "ANY(" in sql:
            return [(sum(self.existing_by_list.get(pid, 0) for pid in params[1]),)]
        if sql.startswith("INSERT INTO bsale.variant_prices"):
            return {"rows": [], "rowcount": 1}
        if sql.startswith("DELETE FROM bsale.variant_prices"):
            return {"rows": [], "rowcount": self.stale}
        return None

    def factory(self) -> FakeConn:
        conn = FakeConn(self.handler)
        self.conns.append(conn)
        return conn

    @property
    def prices_conn(self) -> FakeConn:
        return self.conns[0]

    def all_sql(self, fragment: str) -> list[tuple[str, Any]]:
        return [x for c in self.conns for x in c.sql_matching(fragment)]


def make_client(
    lists: list[dict[str, Any]] | Exception,
    details: dict[int, Any],
    counts: dict[int, int | None] | None = None,
    cost: Any = None,
) -> FakeBsaleClient:
    items: dict[str, Any] = {"price_lists.json": lists}
    items.update({f"price_lists/{pid}/details.json": v for pid, v in details.items()})
    return FakeBsaleClient(
        items,
        json_by_path={"variants/1/costs.json": cost if cost is not None else {"averageCost": 50}},
        counts={f"price_lists/{pid}/details.json": c for pid, c in (counts or {}).items()},
    )


@pytest.fixture
def recorders(monkeypatch):
    temp_rows: list[tuple] = []
    cost_rows: list[tuple] = []
    monkeypatch.setattr(
        snap, "execute_values", lambda cur, sql, rows, page_size=None, template=None: temp_rows.extend(rows)
    )
    monkeypatch.setattr(
        pc, "execute_batch", lambda cur, sql, rows, page_size=None: cost_rows.extend(rows)
    )
    monkeypatch.delenv("BSALE_RECONCILE_MAX_STALE_PCT_VARIANT_PRICES", raising=False)
    monkeypatch.delenv("BSALE_RECONCILE_MAX_STALE_PCT", raising=False)
    return temp_rows, cost_rows


def run(client: FakeBsaleClient, db: PricesDb) -> dict[str, Any]:
    return pc.sync_company_prices_costs(
        COMPANY, client_factory=lambda c: client, connection_factory=db.factory
    )


def delete_scope(db: PricesDb) -> list[Any]:
    return [p for _, p in db.all_sql("DELETE FROM bsale.variant_prices")]


def test_all_lists_complete_reconciles_scoped_to_those_lists(recorders):
    temp_rows, cost_rows = recorders
    client = make_client(
        [{"id": 7, "name": "A", "state": 0}, {"id": 8, "name": "B", "state": 0}],
        {7: [det(1)], 8: [det(1, 90, 107.1)]},
    )
    db = PricesDb({7: 10, 8: 10}, stale=1)
    res = run(client, db)
    assert res["ok"] is True
    assert sorted(temp_rows) == [(1, 1, 7, 100, 119), (1, 1, 8, 90, 107.1)]
    assert delete_scope(db) == [(1, [7, 8])]
    assert db.prices_conn.commits == 1
    assert cost_rows and cost_rows[0][:3] == (1, 1, 50)


def test_active_empty_list_with_existing_prices_is_degraded_and_preserved(recorders):
    temp_rows, _ = recorders
    client = make_client(
        [
            {"id": 2, "name": "Minimarket", "state": 0},
            {"id": 3, "name": "Ruta/Web", "state": 0},
            {"id": 4, "name": "COMODITI", "state": 0},
        ],
        {2: [det(1)], 3: [det(1)], 4: []},
    )
    db = PricesDb({2: 4387, 3: 4387, 4: 4387})
    res = run(client, db)
    assert res["ok"] is False
    assert 4 in res["degraded_price_lists"] and "activa vacía" in res["degraded_price_lists"][4]
    assert res["price_lists"][4]["status"] == pc.LIST_DEGRADED
    assert {r[2] for r in temp_rows} == {2, 3}
    assert delete_scope(db) == [(1, [2, 3])]
    scoped_counts = [
        p for s, p in db.all_sql("ANY(%s)") if s.startswith("SELECT COUNT(*)") and "NOT EXISTS" not in s
    ]
    assert scoped_counts == [(1, [2, 3])]
    assert db.prices_conn.commits == 1 and db.prices_conn.rollbacks == 0


def test_inactive_list_confirmed_empty_is_reconciled(recorders):
    client = make_client(
        [{"id": 7, "state": 1}, {"id": 8, "state": 0}], {7: [], 8: [det(1)]}
    )
    db = PricesDb({7: 3, 8: 100}, stale=3)
    res = run(client, db)
    assert res["ok"] is True
    assert delete_scope(db) == [(1, [7, 8])]


def test_reported_count_mismatch_degrades_only_that_list(recorders):
    temp_rows, _ = recorders
    client = make_client(
        [{"id": 7, "state": 0}, {"id": 8, "state": 0}],
        {7: [det(1)], 8: [det(1)]},
        counts={8: 5},
    )
    db = PricesDb({7: 1, 8: 5})
    res = run(client, db)
    assert res["ok"] is False
    assert res["price_lists"][8]["status"] == pc.LIST_INCONSISTENT
    assert "reported_count=5 != fetched=1" in res["degraded_price_lists"][8]
    assert {r[2] for r in temp_rows} == {7}
    assert delete_scope(db) == [(1, [7])]


def test_missing_reported_count_is_inconsistent(recorders):
    client = make_client([{"id": 7, "state": 0}], {7: [det(1)]}, counts={7: None})
    db = PricesDb({7: 1})
    res = run(client, db)
    assert res["ok"] is False
    assert res["price_lists"][7]["status"] == pc.LIST_INCONSISTENT
    assert delete_scope(db) == []
    assert db.all_sql("CREATE TEMP TABLE") == []


@pytest.mark.parametrize(
    "exc",
    [
        BsalePaginationError("Página vacía en offset 50 con count=120 informado", endpoint="/v1/price_lists/8/details.json"),
        BsaleRetryExhaustedError("Reintentos agotados (HTTP 429)", endpoint="/v1/price_lists/8/details.json", status=429, attempt=5),
    ],
    ids=["empty_page_before_count", "http_429_exhausted"],
)
def test_failed_list_next_to_valid_list(recorders, exc):
    temp_rows, _ = recorders
    client = make_client(
        [{"id": 7, "name": "OK", "state": 0}, {"id": 8, "name": "FAIL", "state": 0}],
        {7: [det(1)], 8: exc},
    )
    db = PricesDb({7: 1, 8: 1505})
    res = run(client, db)
    assert res["ok"] is False
    assert res["price_lists"][8]["status"] == pc.LIST_ERROR
    assert res["price_lists"][7]["status"] == pc.LIST_OK
    assert temp_rows == [(1, 1, 7, 100, 119)]
    assert res["upserted"]["prices"] == 1
    assert delete_scope(db) == [(1, [7])]
    assert db.prices_conn.commits == 1


def test_list_missing_from_endpoint_with_existing_prices_is_degraded(recorders):
    client = make_client([{"id": 7, "state": 0}], {7: [det(1)]})
    db = PricesDb({7: 1, 9: 30})
    res = run(client, db)
    assert res["ok"] is False
    assert "ausente de price_lists.json" in res["degraded_price_lists"][9]
    assert delete_scope(db) == [(1, [7])]


def test_all_lists_degraded_skips_reconcile(recorders):
    client = make_client([{"id": 7, "state": 0}], {7: []})
    db = PricesDb({7: 500})
    res = run(client, db)
    assert res["ok"] is False
    assert db.all_sql("CREATE TEMP TABLE") == []
    assert db.all_sql("INSERT INTO bsale.variant_prices") == []
    assert delete_scope(db) == []


def test_cost_failure_after_valid_prices_keeps_prices_committed(recorders):
    temp_rows, cost_rows = recorders
    err = BsaleRetryExhaustedError(
        "Reintentos agotados (HTTP 429)", endpoint="/v1/variants/1/costs.json", status=429, attempt=5
    )
    client = make_client(
        [{"id": 7, "state": 0}, {"id": 8, "state": 0}], {7: [det(1)], 8: [det(1)]}, cost=err
    )
    db = PricesDb({7: 1, 8: 1}, stale=0)
    res = run(client, db)
    assert res["ok"] is False
    assert "costs:" in res["error"] and "429" in res["costs_error"]
    assert "prices_error" not in res and not res.get("degraded_price_lists")
    assert len(temp_rows) == 2 and res["upserted"]["prices"] == 1
    assert delete_scope(db) == [(1, [7, 8])]
    assert db.prices_conn.commits == 1 and db.prices_conn.rollbacks == 0
    assert cost_rows == []


def test_price_lists_endpoint_failure_still_runs_costs(recorders):
    _, cost_rows = recorders
    err = BsaleRetryExhaustedError("agotado", endpoint="/v1/price_lists.json", status=503, attempt=5)
    client = make_client(err, {})
    db = PricesDb({7: 10})
    res = run(client, db)
    assert res["ok"] is False and "prices:" in res["error"]
    assert db.all_sql("variant_prices") == []
    assert len(cost_rows) == 1


def test_threshold_still_applies_within_scope(recorders, monkeypatch):
    monkeypatch.setenv("BSALE_RECONCILE_MAX_STALE_PCT_VARIANT_PRICES", "20")
    _, cost_rows = recorders
    client = make_client([{"id": 7, "state": 0}], {7: [det(1)]})
    db = PricesDb({7: 10}, stale=5)
    res = run(client, db)
    assert res["ok"] is False and "reconciliación masiva" in res["prices_error"]
    assert delete_scope(db) == []
    assert db.prices_conn.commits == 0 and db.prices_conn.rollbacks == 1
    assert len(cost_rows) == 1


def test_scoped_snapshot_rejects_rows_outside_scope():
    with pytest.raises(CompanySyncError, match="fuera del alcance"):
        snap.upsert_and_reconcile_snapshot(
            FakeConn(),
            snap.VARIANT_PRICES_SPEC,
            1,
            [(1, 1, 9, 1, 1)],
            scope_column="price_list_id",
            scope_values=[7],
        )
