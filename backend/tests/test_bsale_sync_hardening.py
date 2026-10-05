"""Empresas estrictas, product_taxes, transacción por empresa, status global, lock y reconciliación."""

from __future__ import annotations

import pytest

from backend.services.bsale import catalog_company_sync as cat
from backend.services.bsale import catalog_job as job
from backend.services.bsale import snapshot_reconcile as snap
from backend.services.bsale import stock_sync as st
from backend.services.bsale.companies import BsaleCompany, CompanyConfigError, load_active_companies
from backend.services.bsale.http_client import BsaleRetryExhaustedError
from backend.services.bsale.sync_common import CompanySyncError
from backend.tests.bsale_sync_fakes import FakeBsaleClient, FakeConn

COMPANY = BsaleCompany(company_id=3, name="La Quillotana SPA", token_env="BSALE_TOKEN_SPA", token="t")


def _companies_conn(rows):
    return FakeConn(lambda sql, p: rows if "FROM bsale.companies" in sql else None)


ENV = {"BSALE_TOKEN_Mini": "secret-mini", "BSALE_TOKEN_Romero": "secret-romero", "BSALE_TOKEN_SPA": "secret-spa"}
ROWS = [
    (1, "Minimarkets La Quillotana", "BSALE_TOKEN_Mini"),
    (2, "Carlos Romero", "BSALE_TOKEN_Romero"),
    (3, "La Quillotana SPA", "BSALE_TOKEN_SPA"),
]


# ---------------------------------------------------------------- companies


def test_companies_ok_resolves_tokens_without_exposing_them():
    cur = _companies_conn(ROWS).cursor()
    companies = load_active_companies(cur, getenv=ENV.get)
    assert [c.company_id for c in companies] == [1, 2, 3]
    assert companies[2].token == "secret-spa"
    assert "secret-spa" not in repr(companies)


def test_missing_token_for_active_company_fails():
    env = {k: v for k, v in ENV.items() if k != "BSALE_TOKEN_Romero"}
    cur = _companies_conn(ROWS).cursor()
    with pytest.raises(CompanyConfigError, match="BSALE_TOKEN_Romero"):
        load_active_companies(cur, getenv=env.get)


def test_null_token_name_fails():
    rows = [ROWS[0], (2, "Carlos Romero", None), ROWS[2]]
    with pytest.raises(CompanyConfigError, match="company_id=2"):
        load_active_companies(_companies_conn(rows).cursor(), getenv=ENV.get)


def test_zero_active_companies_fails():
    with pytest.raises(CompanyConfigError, match="No hay empresas activas"):
        load_active_companies(_companies_conn([]).cursor(), getenv=ENV.get)


def test_required_company_missing_fails():
    with pytest.raises(CompanyConfigError, match=r"\[2\]"):
        load_active_companies(_companies_conn([ROWS[0], ROWS[2]]).cursor(), getenv=ENV.get)


# ---------------------------------------------------------------- catalog / product_taxes

TAXES = [{"id": 1, "name": "IVA", "percentage": "19"}, {"id": 2, "name": "ILA", "percentage": "10"}]
PRODUCT = {"id": 10, "name": "Pisco", "product_type": {"id": "4"}, "product_taxes": {"href": "https://api.bsale.io/v1/products/10/product_taxes.json"}}
VARIANT = {"id": 500, "product": {"id": 10}, "code": "SKU", "barCode": "780", "description": "750cc"}


def _catalog_client(product_taxes):
    return FakeBsaleClient(
        {
            "taxes.json": TAXES,
            "product_types.json": [{"id": 4, "name": "Licores", "state": 0}],
            "price_lists.json": [{"id": 7, "name": "Mayorista", "state": 0}],
            "offices.json": [{"id": 1, "name": "Casa matriz", "state": 0}],
            "products.json": [PRODUCT],
            PRODUCT["product_taxes"]["href"]: product_taxes,
            "variants.json": [VARIANT],
        }
    )


def test_product_taxes_resolved_factor():
    client = _catalog_client([{"tax": {"id": "1"}}, {"tax": {"id": "2"}}])
    s = cat.fetch_company_catalog(client, 3)
    assert s.products[0][6] == 1.29
    assert s.variants[0][:3] == (3, 500, 10)


def test_product_taxes_failure_never_saves_product_with_factor_one():
    err = BsaleRetryExhaustedError("agotado", endpoint="/v1/products/10/product_taxes.json", status=502, attempt=5)
    client = _catalog_client(err)
    opened: list[FakeConn] = []

    def factory():
        c = FakeConn()
        opened.append(c)
        return c

    res = cat.sync_company_catalog(COMPANY, client_factory=lambda c: client, connection_factory=factory)
    assert res["ok"] is False
    assert "product_id=10" in res["error"] and "product_taxes" in res["error"]
    assert opened == []


def test_unknown_tax_id_fails_company():
    client = _catalog_client([{"tax": {"id": 99}}])
    with pytest.raises(CompanySyncError, match="tax_id=99"):
        cat.fetch_company_catalog(client, 3)


def test_catalog_persist_single_commit_and_zero_products_guard():
    client = _catalog_client([{"tax": {"id": 1}}])
    s = cat.fetch_company_catalog(client, 3)
    s.products = []
    conn = FakeConn(lambda sql, p: [(120,)] if "COUNT(*)" in sql else None)
    with pytest.raises(CompanySyncError, match="products=0"):
        cat.persist_company_catalog(conn, s)
    assert conn.commits == 0 and conn.rollbacks == 1


def test_catalog_persist_db_error_rolls_back(monkeypatch):
    s = cat.fetch_company_catalog(_catalog_client([{"tax": {"id": 1}}]), 3)
    calls = []

    def boom(cur, sql, rows, page_size=None):
        calls.append(sql)
        if "bsale.variants" in sql:
            raise RuntimeError("db down")

    monkeypatch.setattr(cat, "execute_batch", boom)
    conn = FakeConn(lambda sql, p: [(0,)] if "COUNT(*)" in sql else None)
    with pytest.raises(RuntimeError):
        cat.persist_company_catalog(conn, s)
    assert conn.commits == 0 and conn.rollbacks == 1
    assert len(calls) == 6


# ---------------------------------------------------------------- stock / prices reconciliation


class _Rec:
    def __init__(self):
        self.temp_rows: list = []

    def __call__(self, cur, sql, rows, page_size=None, template=None):
        self.temp_rows.extend(rows)


def _reconcile_conn(existing: int, stale: int = 1):
    def handler(sql, params):
        if "COUNT(*)" in sql and "NOT EXISTS" in sql:
            return [(stale,)]
        if "COUNT(*)" in sql:
            return [(existing,)]
        if sql.startswith("INSERT INTO bsale."):
            return {"rows": [], "rowcount": 2}
        if sql.startswith("DELETE FROM"):
            return {"rows": [], "rowcount": 5}
        return None

    return FakeConn(handler)


def test_stock_partial_download_does_not_cleanup():
    err = BsaleRetryExhaustedError("agotado", endpoint="/v1/stocks.json", status=503, attempt=5)
    client = FakeBsaleClient({"stocks.json": err})
    opened: list[FakeConn] = []
    res = st.sync_company_stock(
        COMPANY, client_factory=lambda c: client, connection_factory=lambda: opened.append(FakeConn()) or opened[-1]
    )
    assert res["ok"] is False
    assert opened == []


def test_stock_complete_snapshot_reconciles_stale_rows(monkeypatch):
    rec = _Rec()
    monkeypatch.setattr(snap, "execute_values", rec)
    client = FakeBsaleClient(
        {
            "stocks.json": [
                {"variant": {"id": 1}, "office": {"id": 1}, "quantityAvailable": 5, "quantityReserved": 0},
                {"variant": {"id": 2}, "office": {"id": 1}, "quantityAvailable": 0, "quantityReserved": 0},
            ]
        }
    )
    conn = _reconcile_conn(existing=10, stale=1)
    res = st.sync_company_stock(COMPANY, client_factory=lambda c: client, connection_factory=lambda: conn)
    assert res["ok"] is True
    assert res["deleted"] == 5
    assert res["reconcile"]["existing_count"] == 10
    assert res["reconcile"]["snapshot_count"] == 2
    assert res["reconcile"]["stale_count"] == 1
    assert res["reconcile"]["stale_percentage"] == 10.0
    assert rec.temp_rows == [(3, 1, 1, 5, 0), (3, 2, 1, 0, 0)]
    deletes = conn.sql_matching("DELETE FROM bsale.stocks")
    assert len(deletes) == 1 and deletes[0][1] == (3,)
    assert "t.company_id = %s" in deletes[0][0]
    order = [s.split()[0] for s, _ in conn.executed if s.split()[0] in ("CREATE", "INSERT", "DELETE")]
    assert order == ["CREATE", "INSERT", "DELETE"]
    assert conn.commits == 1 and conn.rollbacks == 0


def test_stock_empty_snapshot_with_existing_rows_fails_without_delete(monkeypatch):
    monkeypatch.setattr(snap, "execute_values", _Rec())
    client = FakeBsaleClient({"stocks.json": []})
    conn = _reconcile_conn(existing=50)
    res = st.sync_company_stock(COMPANY, client_factory=lambda c: client, connection_factory=lambda: conn)
    assert res["ok"] is False
    assert conn.sql_matching("DELETE") == []
    assert conn.rollbacks == 1


@pytest.mark.parametrize("spec", [snap.STOCKS_SPEC, snap.VARIANT_PRICES_SPEC])
def test_fuse_empty_snapshot_with_existing_rows_fails(monkeypatch, spec):
    monkeypatch.setattr(snap, "execute_values", _Rec())
    conn = _reconcile_conn(existing=100, stale=100)
    with pytest.raises(CompanySyncError, match="snapshot vacío"):
        snap.upsert_and_reconcile_snapshot(conn, spec, 3, [])
    assert conn.sql_matching("DELETE") == []
    assert conn.sql_matching(f"INSERT INTO {spec.table}") == []


@pytest.mark.parametrize("spec", [snap.STOCKS_SPEC, snap.VARIANT_PRICES_SPEC])
def test_fuse_stale_above_threshold_fails_without_writes(monkeypatch, spec):
    monkeypatch.setattr(snap, "execute_values", _Rec())
    monkeypatch.setenv(spec.threshold_env, "15")
    conn = _reconcile_conn(existing=100, stale=40)
    with pytest.raises(CompanySyncError, match=r"40/100 \(40.0%\) > umbral 15.0%"):
        snap.upsert_and_reconcile_snapshot(conn, spec, 3, [(3, 1, 1, 5, 0)])
    assert conn.sql_matching("DELETE") == []
    assert conn.sql_matching(f"INSERT INTO {spec.table}") == []


@pytest.mark.parametrize("spec", [snap.STOCKS_SPEC, snap.VARIANT_PRICES_SPEC])
def test_fuse_stale_below_threshold_reconciles(monkeypatch, spec):
    monkeypatch.setattr(snap, "execute_values", _Rec())
    monkeypatch.delenv(spec.threshold_env, raising=False)
    monkeypatch.setenv("BSALE_RECONCILE_MAX_STALE_PCT", "25")
    conn = _reconcile_conn(existing=100, stale=20)
    res = snap.upsert_and_reconcile_snapshot(conn, spec, 3, [(3, 1, 1, 5, 0)])
    assert res["stale_percentage"] == 20.0 and res["max_stale_percentage"] == 25.0
    assert len(conn.sql_matching(f"DELETE FROM {spec.table}")) == 1


def test_fuse_failure_in_stock_phase_rolls_back(monkeypatch):
    monkeypatch.setattr(snap, "execute_values", _Rec())
    monkeypatch.setenv("BSALE_RECONCILE_MAX_STALE_PCT_STOCKS", "5")
    client = FakeBsaleClient(
        {"stocks.json": [{"variant": {"id": 1}, "office": {"id": 1}, "quantityAvailable": 1, "quantityReserved": 0}]}
    )
    conn = _reconcile_conn(existing=100, stale=50)
    res = st.sync_company_stock(COMPANY, client_factory=lambda c: client, connection_factory=lambda: conn)
    assert res["ok"] is False and "reconciliación masiva" in res["error"]
    assert conn.commits == 0 and conn.rollbacks == 1
    assert conn.sql_matching("DELETE") == []


def test_snapshot_rejects_rows_from_other_company():
    with pytest.raises(CompanySyncError):
        snap.upsert_and_reconcile_snapshot(_reconcile_conn(0), snap.STOCKS_SPEC, 3, [(1, 1, 1, 0, 0)])


# ---------------------------------------------------------------- global status / lock


class _Recorder:
    def __init__(self):
        self.started = None
        self.finished = None

    def start(self, **kw):
        self.started = kw

    def finish(self, **kw):
        self.finished = kw


def _three_companies():
    return [BsaleCompany(cid, f"c{cid}", f"ENV{cid}", "t") for cid in (1, 2, 3)]


def test_one_company_failure_never_returns_success(monkeypatch):
    monkeypatch.setattr(job, "load_companies_strict", lambda cf: _three_companies())

    def phase(company, **kw):
        if company.company_id == 2:
            return {"ok": False, "error": "HTTP 503"}
        return {"ok": True}

    rec = _Recorder()
    stats = job.run_sync(
        company_phases={"catalog": phase},
        global_steps=[("refresh", lambda: {"ok": True})],
        recorder=rec,
        connection_factory=lambda: FakeConn(),
    )
    assert stats["status"] == "partial"
    assert stats["companies_processed"] == [1, 3]
    assert job.exit_code_for_status(stats["status"]) != 0
    assert rec.finished["status"] == "partial"


def test_all_ok_is_success(monkeypatch):
    monkeypatch.setattr(job, "load_companies_strict", lambda cf: _three_companies())
    rec = _Recorder()
    stats = job.run_sync(
        company_phases={"catalog": lambda c, **kw: {"ok": True}},
        global_steps=[("refresh", lambda: {"ok": True})],
        recorder=rec,
        connection_factory=lambda: FakeConn(),
    )
    assert stats["status"] == "success"
    assert job.exit_code_for_status("success") == 0


def test_global_db_step_failure_is_not_success(monkeypatch):
    monkeypatch.setattr(job, "load_companies_strict", lambda cf: _three_companies())
    stats = job.run_sync(
        company_phases={"catalog": lambda c, **kw: {"ok": True}},
        global_steps=[("refresh_products_master", lambda: {"ok": False, "error": "x"})],
        recorder=_Recorder(),
        connection_factory=lambda: FakeConn(),
    )
    assert stats["status"] != "success"


def test_company_config_error_fails_job(monkeypatch):
    def bad(cf):
        raise CompanyConfigError("company_id=2: variable de entorno BSALE_TOKEN_Romero no definida")

    monkeypatch.setattr(job, "load_companies_strict", bad)
    rec = _Recorder()
    stats = job.run_sync(company_phases={}, recorder=rec, connection_factory=lambda: FakeConn())
    assert stats["status"] == "failed"
    assert rec.finished["status"] == "failed"


def test_duplicate_execution_blocked_by_lock(monkeypatch):
    lock_conn = FakeConn(lambda sql, p: [(False,)] if "pg_try_advisory_lock" in sql else None)

    def must_not_run(**kw):
        raise AssertionError("no debe ejecutarse un segundo sync")

    monkeypatch.setattr(job, "run_sync", must_not_run)
    code, stats = job.run_locked(connection_factory=lambda: lock_conn)
    assert code == job.EXIT_LOCKED
    assert stats["status"] == "locked"
    assert lock_conn.autocommit is True
    assert lock_conn.closed is True


def test_lock_acquired_runs_and_unlocks(monkeypatch):
    lock_conn = FakeConn(lambda sql, p: [(True,)] if "pg_try_advisory_lock" in sql else None)
    monkeypatch.setattr(job, "run_sync", lambda **kw: {"status": "success"})
    code, _ = job.run_locked(connection_factory=lambda: lock_conn)
    assert code == 0
    assert lock_conn.sql_matching("pg_advisory_unlock")
