"""Sync de precios: alcance de listas administradas, reconciliación por lista y desacople de costos."""

from __future__ import annotations

import logging
from typing import Any

import pytest

from backend.services.bsale import prices_costs_sync as pc
from backend.services.bsale import snapshot_reconcile as snap
from backend.services.bsale.companies import BsaleCompany
from backend.services.bsale.http_client import BsalePaginationError, BsaleRetryExhaustedError
from backend.services.bsale.managed_price_lists import MANAGED_PRICE_LISTS, managed_price_lists
from backend.services.bsale.sync_common import CompanySyncError
from backend.tests.bsale_sync_fakes import FakeBsaleClient, FakeConn


def company(cid: int) -> BsaleCompany:
    return BsaleCompany(company_id=cid, name=f"c{cid}", token_env=f"ENV{cid}", token="t")


def det(variant_id: int, net: float = 100, gross: float = 119) -> dict[str, Any]:
    return {"variant": {"id": variant_id}, "variantValue": net, "variantValueWithTaxes": gross}


def pl(pid: int, state: int = 0, name: str | None = None) -> dict[str, Any]:
    return {"id": pid, "name": name or f"L{pid}", "state": state}


class PricesDb:
    """PostgreSQL falso: precios existentes por lista (incluye listas no administradas)."""

    def __init__(self, existing_by_list: dict[int, int], stale: int = 0) -> None:
        self.existing_by_list = existing_by_list
        self.stale = stale
        self.conns: list[FakeConn] = []

    def _sum(self, ids: list[int]) -> int:
        return sum(self.existing_by_list.get(pid, 0) for pid in ids)

    def handler(self, sql: str, params: Any) -> Any:
        if "FROM bsale.variants" in sql:
            return [(1,)]
        if "GROUP BY price_list_id" in sql:
            return [(pid, n) for pid, n in self.existing_by_list.items() if pid in params[1]]
        if "COUNT(*)" in sql and "NOT EXISTS" in sql:
            return [(self.stale,)]
        if "COUNT(*)" in sql and "ANY(" in sql:
            return [(self._sum(params[1]),)]
        if "COUNT(*)" in sql:
            return [(self._sum(list(self.existing_by_list)),)]
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


def run(client: FakeBsaleClient, db: PricesDb, cid: int = 1, managed=None) -> dict[str, Any]:
    return pc.sync_company_prices_costs(
        company(cid),
        client_factory=lambda c: client,
        connection_factory=db.factory,
        managed_lists=managed,
    )


def delete_scope(db: PricesDb) -> list[Any]:
    return [p for _, p in db.all_sql("DELETE FROM bsale.variant_prices")]


def details_requested(client: FakeBsaleClient) -> list[str]:
    return [p for p in client.requested if p.endswith("/details.json")]


# ---------------------------------------------------------------- configuración


def test_company_1_manages_only_2_and_3():
    assert managed_price_lists(1) == (2, 3)


def test_company_2_manages_only_3_and_13():
    assert managed_price_lists(2) == (3, 13)


def test_company_3_manages_only_12_13_16():
    assert managed_price_lists(3) == (12, 13, 16)


def test_comoditi_lists_are_not_managed():
    assert 4 not in MANAGED_PRICE_LISTS[1]
    assert 14 not in MANAGED_PRICE_LISTS[2]
    assert 14 not in MANAGED_PRICE_LISTS[3]


def test_company_without_configuration_fails_prices_but_runs_costs(recorders):
    _, cost_rows = recorders
    client = make_client([pl(2)], {2: [det(1)]})
    db = PricesDb({2: 1})
    res = run(client, db, cid=99)
    assert res["ok"] is False and "sin listas de precios administradas" in res["prices_error"]
    assert details_requested(client) == []
    assert db.all_sql("variant_prices") == []
    assert len(cost_rows) == 1


# ---------------------------------------------------------------- alcance administrado


def test_only_managed_lists_are_downloaded(recorders):
    client = make_client(
        [pl(2), pl(3), pl(4, state=1), pl(9, state=0)],
        {2: [det(1)], 3: [det(1)], 4: [det(1)], 9: [det(1)]},
    )
    res = run(client, PricesDb({2: 1, 3: 1}))
    assert res["ok"] is True
    assert details_requested(client) == ["price_lists/2/details.json", "price_lists/3/details.json"]
    assert res["managed_price_lists"] == [2, 3]
    assert res["ignored_price_lists"] == [4, 9]
    assert set(res["price_lists"]) == {2, 3}


def test_unmanaged_list_missing_from_bsale_is_not_an_error(recorders):
    client = make_client([pl(2), pl(3)], {2: [det(1)], 3: [det(1)]})
    db = PricesDb({2: 10, 3: 10, 4: 4387})
    res = run(client, db)
    assert res["ok"] is True
    assert "error" not in res and not res["degraded_price_lists"]
    assert all(4 not in p[1] for p in delete_scope(db))


def test_unmanaged_historical_list_does_not_participate_in_stale_count(recorders):
    client = make_client([pl(2), pl(3), pl(4, state=1)], {2: [det(1)], 3: [det(1)]})
    db = PricesDb({2: 10, 3: 10, 4: 4387}, stale=1)
    res = run(client, db)
    assert res["ok"] is True
    reconcile = res["reconcile"]["prices"]
    assert reconcile["existing_count"] == 20
    assert reconcile["scope"] == {"price_list_id": [2, 3]}
    assert delete_scope(db) == [(1, [2, 3])]
    existing_query = db.all_sql("GROUP BY price_list_id")
    assert existing_query and existing_query[0][1] == (1, [2, 3])


def test_managed_list_missing_from_bsale_is_degraded_and_preserved(recorders):
    temp_rows, _ = recorders
    client = make_client([pl(12), pl(16)], {12: [det(1)], 16: [det(1)]})
    db = PricesDb({12: 10, 13: 6118, 16: 10})
    res = run(client, db, cid=3)
    assert res["ok"] is False
    assert res["price_lists"][13]["status"] == pc.LIST_MISSING
    assert "ausente de price_lists.json" in res["degraded_price_lists"][13]
    assert {r[2] for r in temp_rows} == {12, 16}
    assert delete_scope(db) == [(3, [12, 16])]
    assert "price_lists/13/details.json" not in client.requested


def test_managed_list_inactive_in_bsale_is_degraded_and_not_downloaded(recorders):
    client = make_client([pl(2), pl(3, state=1)], {2: [det(1)], 3: [det(1)]})
    db = PricesDb({2: 10, 3: 4387})
    res = run(client, db)
    assert res["ok"] is False
    assert res["price_lists"][3]["status"] == pc.LIST_INACTIVE
    assert "state=1" in res["degraded_price_lists"][3]
    assert details_requested(client) == ["price_lists/2/details.json"]
    assert delete_scope(db) == [(1, [2])]


def test_logs_managed_lists_and_per_list_status(recorders, caplog):
    client = make_client(
        [pl(12), pl(13), pl(14, state=1), pl(16)],
        {12: [det(1), det(2), det(3)], 13: [det(1)], 16: [det(1)]},
    )
    with caplog.at_level(logging.INFO, logger=pc.__name__):
        res = run(client, PricesDb({12: 10, 13: 10, 16: 10}), cid=3)
    assert res["ok"] is True
    text = caplog.text
    assert "company_id=3 managed_price_lists=[12,13,16]" in text
    assert "company_id=3 price_list_id=12 status=OK fetched=3" in text
    assert "company_id=3 price_list_id=13 status=OK fetched=1" in text
    assert "price_list_id=14 status" not in text
    assert "ERROR" not in text and "DEGRADED" not in text


# ---------------------------------------------------------------- validación por lista


def test_all_managed_lists_complete_reconciles_scoped(recorders):
    temp_rows, cost_rows = recorders
    client = make_client([pl(2), pl(3)], {2: [det(1)], 3: [det(1, 90, 107.1)]})
    db = PricesDb({2: 10, 3: 10}, stale=1)
    res = run(client, db)
    assert res["ok"] is True
    assert sorted(temp_rows) == [(1, 1, 2, 100, 119), (1, 1, 3, 90, 107.1)]
    assert delete_scope(db) == [(1, [2, 3])]
    assert db.prices_conn.commits == 1
    assert cost_rows and cost_rows[0][:3] == (1, 1, 50)


def test_managed_empty_list_with_existing_prices_is_degraded_and_preserved(recorders):
    temp_rows, _ = recorders
    client = make_client([pl(2), pl(3)], {2: [det(1)], 3: []})
    db = PricesDb({2: 10, 3: 4387})
    res = run(client, db)
    assert res["ok"] is False
    assert "vacía con 4387" in res["degraded_price_lists"][3]
    assert res["price_lists"][3]["status"] == pc.LIST_DEGRADED
    assert {r[2] for r in temp_rows} == {2}
    assert delete_scope(db) == [(1, [2])]
    assert db.prices_conn.commits == 1 and db.prices_conn.rollbacks == 0


def test_reported_count_mismatch_degrades_only_that_list(recorders):
    temp_rows, _ = recorders
    client = make_client([pl(2), pl(3)], {2: [det(1)], 3: [det(1)]}, counts={3: 5})
    db = PricesDb({2: 10, 3: 5})
    res = run(client, db)
    assert res["ok"] is False
    assert res["price_lists"][3]["status"] == pc.LIST_INCONSISTENT
    assert "reported_count=5 != fetched=1" in res["degraded_price_lists"][3]
    assert {r[2] for r in temp_rows} == {2}
    assert delete_scope(db) == [(1, [2])]


def test_missing_reported_count_is_inconsistent(recorders):
    client = make_client([pl(2), pl(3)], {2: [det(1)], 3: [det(1)]}, counts={2: None, 3: None})
    db = PricesDb({2: 1, 3: 1})
    res = run(client, db)
    assert res["ok"] is False
    assert res["price_lists"][2]["status"] == pc.LIST_INCONSISTENT
    assert delete_scope(db) == []
    assert db.all_sql("CREATE TEMP TABLE") == []


@pytest.mark.parametrize(
    "exc",
    [
        BsalePaginationError("Página vacía en offset 50 con count=120 informado", endpoint="/v1/price_lists/3/details.json"),
        BsaleRetryExhaustedError("Reintentos agotados (HTTP 429)", endpoint="/v1/price_lists/3/details.json", status=429, attempt=5),
    ],
    ids=["empty_page_before_count", "http_429_exhausted"],
)
def test_failed_list_next_to_valid_list(recorders, exc):
    temp_rows, _ = recorders
    client = make_client([pl(2), pl(3)], {2: [det(1)], 3: exc})
    db = PricesDb({2: 1, 3: 4387})
    res = run(client, db)
    assert res["ok"] is False
    assert res["price_lists"][3]["status"] == pc.LIST_ERROR
    assert res["price_lists"][2]["status"] == pc.LIST_OK
    assert temp_rows == [(1, 1, 2, 100, 119)]
    assert delete_scope(db) == [(1, [2])]
    assert db.prices_conn.commits == 1


def test_all_managed_lists_degraded_skips_reconcile(recorders):
    client = make_client([pl(2), pl(3)], {2: [], 3: []})
    db = PricesDb({2: 500, 3: 500})
    res = run(client, db)
    assert res["ok"] is False
    assert db.all_sql("CREATE TEMP TABLE") == []
    assert db.all_sql("INSERT INTO bsale.variant_prices") == []
    assert delete_scope(db) == []


def test_threshold_still_applies_within_scope(recorders, monkeypatch):
    monkeypatch.setenv("BSALE_RECONCILE_MAX_STALE_PCT_VARIANT_PRICES", "20")
    _, cost_rows = recorders
    client = make_client([pl(2), pl(3)], {2: [det(1)], 3: [det(1)]})
    db = PricesDb({2: 5, 3: 5}, stale=5)
    res = run(client, db)
    assert res["ok"] is False and "reconciliación masiva" in res["prices_error"]
    assert delete_scope(db) == []
    assert db.prices_conn.commits == 0 and db.prices_conn.rollbacks == 1
    assert len(cost_rows) == 1


def test_injected_managed_config_is_validated(recorders):
    client = make_client([pl(7)], {7: [det(1)]})
    res = run(client, PricesDb({7: 10}), managed={1: (7, 7)})
    assert res["ok"] is False and "duplicadas" in res["prices_error"]


# ---------------------------------------------------------------- desacople precios / costos


def test_cost_failure_after_valid_prices_keeps_prices_committed(recorders):
    temp_rows, cost_rows = recorders
    err = BsaleRetryExhaustedError(
        "Reintentos agotados (HTTP 429)", endpoint="/v1/variants/1/costs.json", status=429, attempt=5
    )
    client = make_client([pl(2), pl(3)], {2: [det(1)], 3: [det(1)]}, cost=err)
    db = PricesDb({2: 10, 3: 10})
    res = run(client, db)
    assert res["ok"] is False
    assert "costs:" in res["error"] and "429" in res["costs_error"]
    assert "prices_error" not in res and not res["degraded_price_lists"]
    assert len(temp_rows) == 2
    assert delete_scope(db) == [(1, [2, 3])]
    assert db.prices_conn.commits == 1 and db.prices_conn.rollbacks == 0
    assert cost_rows == []


def test_price_lists_endpoint_failure_still_runs_costs(recorders):
    _, cost_rows = recorders
    err = BsaleRetryExhaustedError("agotado", endpoint="/v1/price_lists.json", status=503, attempt=5)
    client = make_client(err, {})
    db = PricesDb({2: 10})
    res = run(client, db)
    assert res["ok"] is False and "prices:" in res["error"]
    assert db.all_sql("variant_prices") == []
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
