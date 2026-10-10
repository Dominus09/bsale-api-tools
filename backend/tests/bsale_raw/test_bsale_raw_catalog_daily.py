"""Catálogo diario: motor ``product_taxes`` + orquestador ``sync-catalog``. Sin red ni BD real.

HTTP: stack real (``BsaleHttpClient`` + ``RateLimitedSession``) sobre adapters falsos. BD: fakes en
memoria con la semántica de la SQL (frescura por fila, ausencias, cursor de fallas, runs, rollback).
"""

from __future__ import annotations

import copy
import io
import json
import re
from contextlib import contextmanager
from datetime import timedelta
from urllib.parse import parse_qs, urlsplit

import pytest
import requests
from requests.adapters import BaseAdapter

from backend.jobs.bsale_raw import cli
from backend.services.bsale_raw import catalog_daily
from backend.services.bsale_raw.catalog_daily import (
    CATALOG_ORDER,
    CatalogReport,
    format_catalog_report,
    run_catalog_sync,
)
from backend.services.bsale_raw.core import product_tax_engine as pte
from backend.services.bsale_raw.core.engine import UnsupportedSyncError, run_entity_sync
from backend.services.bsale_raw.core.models import SyncMode, payload_hash
from backend.services.bsale_raw.core.product_tax_engine import (
    MARK_PRODUCT_TAXES_MISSING_SQL,
    TAX_COLUMNS,
    UPSERT_PRODUCT_TAXES_SQL,
    product_tax_rate_config,
    run_product_tax_sync,
)
from backend.services.bsale_raw.core.reconcile import ExistingRow
from backend.services.bsale_raw.core.registry import REGISTRY
from backend.services.bsale_raw.core.store import EntityOutcome, LockBusyError, RunHandle, SourceConfig
from backend.tests.bsale_raw._raw_sql_schema import parse
from backend.tests.bsale_raw.test_bsale_raw_catalog_resources import product, variant
from backend.tests.bsale_raw.test_bsale_raw_pipeline import (
    BASE,
    ENV,
    TOKEN,
    FakeStore,
    TickClock,
    client_factory_for,
    make_response,
)

API = "https://api.bsale.io/v1"
TAX_RE = re.compile(r"/v1/products/(\d+)/product_taxes\.json$")


# --- fake Bsale: products/{id}/product_taxes.json -------------------------------------------------


def tax_item(product_id: int, tax_id: int, n: int) -> dict:
    item_id = product_id * 100 + n
    return {"href": f"{API}/products/{product_id}/product_taxes/{item_id}.json", "id": item_id,
            "product": {"href": f"{API}/products/{product_id}.json", "id": str(product_id)},
            "tax": {"href": f"{API}/taxes/{tax_id}.json", "id": str(tax_id)}}


def tax_page(product_id: int, items: list, *, offset=0, limit=50, count=None) -> dict:
    return {"href": f"{API}/products/{product_id}/product_taxes.json", "count": len(items) if count is None else count,
            "limit": limit, "offset": offset, "items": items[offset:offset + limit]}


class TaxesBsale(BaseAdapter):
    """``responses[p]``: lista de tax_ids (paginada por offset), dict (200 tal cual), ``(status, body)`` o
    Exception. Sin entrada = ``[1]`` (sólo IVA)."""

    def __init__(self, responses=None, store=None):
        super().__init__()
        self.responses = dict(responses or {})
        self.store = store
        self.calls: list[int] = []
        self.tx_violations: list[str] = []

    def send(self, request, **kwargs):
        if self.store is not None and self.store.in_tx:
            self.tx_violations.append(request.url)
        match = TAX_RE.search(urlsplit(request.url).path)
        assert match, request.url
        assert request.headers.get("access_token") == TOKEN
        product_id = int(match.group(1))
        self.calls.append(product_id)
        action = self.responses.get(product_id, [1])
        if isinstance(action, BaseException):
            raise action
        if isinstance(action, tuple):
            status, body = action
        elif isinstance(action, list):
            q = parse_qs(urlsplit(request.url).query)
            items = [tax_item(product_id, t, n) for n, t in enumerate(action)]
            status, body = 200, tax_page(product_id, items, offset=int(q["offset"][0]), limit=int(q["limit"][0]))
        else:
            status, body = 200, action
        return make_response(request, status, json.dumps(body).encode())

    def close(self):
        pass


# --- fake BD --------------------------------------------------------------------------------------


class FakeTaxTx:
    def __init__(self, store):
        self.s = store

    def upsert_product_taxes(self, rows, *, sync_run_id, last_source):
        if self.s.fail_upsert_after is not None:
            if self.s.fail_upsert_after <= 0:
                raise RuntimeError(f"fallo BD con {TOKEN}")
            self.s.fail_upsert_after -= 1
        now = self.s.now()
        applied = set()
        for r in rows:
            key = (r.company_id, r.product_id)
            prev = self.s.taxes_rows.get(key)
            if prev is not None and prev["api_fetched_at"] > r.api_fetched_at:
                continue
            self.s.taxes_rows[key] = {
                "tax_ids": list(r.tax_ids), "items_count": r.items_count, "payload": copy.deepcopy(r.payload),
                "payload_hash": r.payload_hash, "api_fetched_at": r.api_fetched_at, "missing_since": None,
                "first_seen_at": prev["first_seen_at"] if prev else now,
                "last_changed_at": now if prev is None or prev["payload_hash"] != r.payload_hash else prev["last_changed_at"],
                "last_source": last_source, "sync_run_id": sync_run_id,
            }
            applied.add(r.product_id)
        return applied

    def mark_product_taxes_missing(self, company_id, product_ids, snapshot_started_at):
        n = 0
        for pid in product_ids:
            row = self.s.taxes_rows.get((company_id, pid))
            if row and row["missing_since"] is None and row["api_fetched_at"] <= snapshot_started_at:
                row["missing_since"] = self.s.now()
                n += 1
        return n

    def write_failures(self, company_id, failures, *, started_at, sync_run_id):
        if self.s.fail_final:
            raise RuntimeError("fallo inyectado en la transacción final")
        self.s.failures = copy.deepcopy(failures)

    def finish_success(self, handle, outcome):
        self.s.runs[handle.run_id].update(status=outcome.status, error=outcome.error, summary=outcome.summary())
        self.s.state[(outcome.company_id, outcome.resource, outcome.sync_state_scope)] = outcome.status


class FakeTaxStore:
    """``catalog``: FakeStore del pipeline (lee products / taxes vigentes como la SQL real)."""

    def __init__(self, products=(), taxes=(1, 2, 3, 4, 5, 6, 7, 8), *, catalog: FakeStore | None = None):
        self.catalog = catalog
        self.products = sorted(products)
        self.taxes = set(taxes)
        self.taxes_rows: dict = {}
        self.failures: dict = {}
        self.runs: dict = {}
        self.state: dict = {}
        self.held: set = set()
        self.events: list[str] = []
        self.in_tx = False
        self.fail_upsert_after: int | None = None
        self.fail_final = False
        self.db_clock = TickClock(BASE + timedelta(hours=1))

    def now(self):
        return self.db_clock()

    def resolve_source(self, company_id):
        return SourceConfig(company_id=company_id, cpn_id=21884, name="SPA", token_env="BSALE_TOKEN_SPA")

    @contextmanager
    def advisory_lock(self, company_id, resource, scope):
        key = (company_id, resource, scope)
        if key in self.held:
            raise LockBusyError(f"lock ocupado company_id={company_id} resource={resource} scope={scope}")
        self.held.add(key)
        self.events.append("lock")
        try:
            yield
        finally:
            self.held.discard(key)
            self.events.append("unlock")

    def start_run(self, *, mode, trigger, host, company_id, resource, scope, state_scope=None):
        run_id = len(self.runs) + 1
        self.runs[run_id] = {"mode": mode, "trigger": trigger, "resource": resource, "status": "RUNNING"}
        self.events.append("start_run")
        return RunHandle(run_id=run_id, entity_run_id=run_id, started_at=BASE)

    def finish_failed(self, handle, outcome):
        self.runs[handle.run_id].update(status=outcome.status, error=outcome.error, summary=outcome.summary())

    def _live(self, table, company_id):
        return sorted(bid for (cid, bid), r in self.catalog.tables[table].items()
                      if cid == company_id and r["missing_since"] is None)

    def select_products(self, company_id):
        return self._live("bsale_raw.products", company_id) if self.catalog else list(self.products)

    def select_taxes(self, company_id):
        return set(self._live("bsale_raw.taxes", company_id)) if self.catalog else set(self.taxes)

    def read_existing_product_taxes(self, company_id):
        return {pid: ExistingRow(pid, r["payload_hash"], r["api_fetched_at"], r["missing_since"])
                for (cid, pid), r in self.taxes_rows.items() if cid == company_id}

    def read_failures(self, company_id):
        return copy.deepcopy(self.failures)

    @contextmanager
    def product_tax_transaction(self):
        backup = copy.deepcopy((self.taxes_rows, self.failures, self.runs, self.state))
        self.in_tx = True
        self.events.append("tx_begin")
        try:
            yield FakeTaxTx(self)
        except BaseException:
            self.taxes_rows, self.failures, self.runs, self.state = backup
            self.events.append("tx_rollback")
            raise
        finally:
            self.in_tx = False

    def row(self, pid, company_id=3):
        return self.taxes_rows.get((company_id, pid))


def seed_row(store, pid, tax_ids, *, fetched_at=BASE, missing_since=None):
    payload = [tax_page(pid, [tax_item(pid, t, n) for n, t in enumerate(tax_ids)])]
    store.taxes_rows[(3, pid)] = {
        "tax_ids": list(tax_ids), "items_count": len(tax_ids), "payload": payload, "payload_hash": payload_hash(payload),
        "api_fetched_at": fetched_at, "missing_since": missing_since, "first_seen_at": fetched_at,
        "last_changed_at": fetched_at, "last_source": "FULL_RECONCILE", "sync_run_id": None,
    }


def sync_taxes(store, adapter, **kw):
    kw.setdefault("clock", TickClock(BASE + timedelta(hours=2)))
    return run_product_tax_sync(
        store=store, company_id=kw.pop("company_id", 3), client_factory=client_factory_for(adapter),
        getenv=kw.pop("env", ENV).get, host="test", **kw,
    )


def world(products, responses=None, **kw):
    store = FakeTaxStore(products, **kw)
    return store, TaxesBsale(responses, store=store)


# --- registry / migración -------------------------------------------------------------------------


def test_product_taxes_spec_is_not_a_generic_pipeline_resource():
    spec = REGISTRY.get("product_taxes")
    assert spec.parent == "products" and spec.raw_table == "bsale_raw.product_taxes"
    assert spec.list_endpoint == "/v1/products/{parent_id}/product_taxes.json"
    assert "product_taxes" not in REGISTRY.pipeline_names()
    with pytest.raises(UnsupportedSyncError):
        run_entity_sync(store=FakeStore(), company_id=3, resource="product_taxes")


def test_upsert_sql_matches_migration_and_has_stale_guard():
    tables, _ = parse()
    assert set(TAX_COLUMNS) == set(tables["product_taxes"].columns)
    assert "ON CONFLICT (company_id, product_id)" in UPSERT_PRODUCT_TAXES_SQL
    assert "WHERE t.api_fetched_at <= EXCLUDED.api_fetched_at" in UPSERT_PRODUCT_TAXES_SQL
    assert "missing_since = NULL" in UPSERT_PRODUCT_TAXES_SQL
    assert MARK_PRODUCT_TAXES_MISSING_SQL.startswith("UPDATE bsale_raw.product_taxes SET missing_since = now()")
    assert "api_fetched_at <= %s" in MARK_PRODUCT_TAXES_MISSING_SQL
    source = open(pte.__file__, encoding="utf-8").read()
    assert not re.search(r"\bDELETE\b", source)
    assert not re.search(r"tax_factor|percentage|\* 1\.19", source)


def test_rate_config_is_conservative_and_bounded():
    assert product_tax_rate_config({}.get).requests_per_second == 1.0
    assert product_tax_rate_config({"BSALE_RAW_PRODUCT_TAX_RPS": "2"}.get).requests_per_second == 2.0
    for bad in ("0", "2.5", "-1"):
        with pytest.raises(ValueError):
            product_tax_rate_config({"BSALE_RAW_PRODUCT_TAX_RPS": bad}.get)


# --- product_taxes: contenido -----------------------------------------------------------------------


def test_multiple_taxes_in_order_and_confirmed_empty_with_exact_payload():
    store, adapter = world([10, 11, 12], {10: [8, 1], 11: [], 12: [1]})
    out = sync_taxes(store, adapter)
    assert out.status == "SUCCESS", out.error
    assert store.row(10)["tax_ids"] == [8, 1] and store.row(10)["items_count"] == 2
    assert store.row(11)["tax_ids"] == [] and store.row(11)["items_count"] == 0
    expected = [tax_page(10, [tax_item(10, 8, 0), tax_item(10, 1, 1)])]
    assert store.row(10)["payload"] == expected
    assert store.row(10)["payload_hash"] == payload_hash(expected)
    assert out.point["with_taxes"] == 2 and out.point["without_taxes"] == 1
    assert out.rows_inserted == 3 and adapter.calls == [10, 11, 12] and not adapter.tx_violations
    assert store.runs[1]["trigger"] == "MANUAL" and store.runs[1]["status"] == "SUCCESS"


def test_complete_pagination_keeps_every_page():
    many = [1 + (n % 8) for n in range(51)]
    store, adapter = world([10], {10: many})
    out = sync_taxes(store, adapter)
    assert out.status == "SUCCESS", out.error
    row = store.row(10)
    assert row["tax_ids"] == many and row["items_count"] == 51
    assert [p["offset"] for p in row["payload"]] == [0, 50] and out.requests == 2


def test_incomplete_pagination_is_a_failure_not_a_partial_list():
    truncated = tax_page(10, [tax_item(10, 1, 0)], count=3)
    store, adapter = world([10, 11], {10: truncated})
    out = sync_taxes(store, adapter)
    assert out.status == "PARTIAL"
    assert store.row(10) is None and store.row(11)["tax_ids"] == [1]
    assert "10" in out.point["failed_sample"] and set(store.failures) == {10}


@pytest.mark.parametrize("status", [404, 500])
def test_http_error_never_becomes_empty_taxes(status):
    store, adapter = world([10, 11], {10: (status, {"error": "x"})})
    seed_row(store, 10, [1, 8], fetched_at=BASE - timedelta(days=1))
    before = copy.deepcopy(store.row(10))
    out = sync_taxes(store, adapter)
    assert out.status == "PARTIAL"
    assert store.row(10) == before  # la relación anterior queda intacta, nunca se vacía
    assert store.failures[10]["attempts"] == 1 and f"status={status}" in store.failures[10]["error"]


def test_item_from_other_product_or_without_tax_is_rejected():
    other = tax_page(10, [tax_item(99, 1, 0)])
    no_tax = tax_page(11, [{"id": 5, "product": {"id": "11"}}])
    store, adapter = world([10, 11, 12], {10: other, 11: no_tax})
    out = sync_taxes(store, adapter)
    assert out.status == "PARTIAL" and store.row(10) is None and store.row(11) is None
    assert store.row(12)["tax_ids"] == [1]


def test_unknown_tax_is_stored_and_reported_as_partial():
    store, adapter = world([10, 11], {10: [1, 99]}, taxes=(1, 8))
    out = sync_taxes(store, adapter)
    assert out.status == "PARTIAL" and "bsale_raw.taxes" in out.error
    assert store.row(10)["tax_ids"] == [1, 99]
    assert out.point["unknown_tax_products"] == {"10": [99]}


# --- product_taxes: estados por producto ------------------------------------------------------------


def test_four_states_are_distinguishable():
    store, adapter = world(
        [10, 11, 12] + list(range(20, 31)) + [40],
        {10: [1], 11: [], 12: (404, {"error": "not found"}),
         **{p: requests.ConnectionError("caída") for p in range(20, 31)}},
    )
    out = sync_taxes(store, adapter)
    assert out.status == "PARTIAL" and out.point["aborted"]
    assert store.row(10)["items_count"] == 1          # consultado con impuestos
    assert store.row(11)["items_count"] == 0          # consultado sin impuestos (confirmado)
    assert store.row(12) is None and 12 in store.failures   # consulta fallida
    assert store.row(40) is None and 40 not in store.failures and 40 not in adapter.calls  # no consultado
    assert out.point["not_attempted"] == 2 and out.point["failed"] == 11


def test_sustained_429_stops_to_protect_shared_quota():
    store, adapter = world(list(range(1, 20)), {p: (429, {"error": "rate"}) for p in range(1, 20)})
    out = sync_taxes(store, adapter)
    assert out.status == "FAILED" and "429" in (out.point["aborted"] or "")
    assert out.http_429 >= pte.MAX_HTTP_429 and len(set(adapter.calls)) < 19
    assert not store.taxes_rows and set(store.failures) == set(adapter.calls)


def test_absent_product_marked_missing_and_reappearance_clears_it():
    store, adapter = world([10])
    seed_row(store, 10, [1])
    for pid in range(11, 20):
        seed_row(store, pid, [1])
    store.products = list(range(10, 19))  # 19 desaparece de bsale_raw.products
    out = sync_taxes(store, adapter)
    assert out.status == "SUCCESS" and out.rows_missing == 1
    assert store.row(19)["missing_since"] is not None and store.row(19)["tax_ids"] == [1]

    store.products = list(range(10, 20))  # reaparece
    out = sync_taxes(store, adapter, clock=TickClock(BASE + timedelta(hours=3)))
    assert out.status == "SUCCESS" and store.row(19)["missing_since"] is None and out.rows_missing == 0


def test_missing_fuse_blocks_marking():
    store, adapter = world([10])
    for pid in range(10, 15):
        seed_row(store, pid, [1])
    out = sync_taxes(store, adapter)
    assert out.status == "PARTIAL" and out.fuse["tripped"] and "fusible" in out.error
    assert all(store.row(p)["missing_since"] is None for p in range(11, 15))


def test_stale_guard_never_overwrites_newer_row():
    store, adapter = world([10, 11], {10: [8]})
    seed_row(store, 10, [1], fetched_at=BASE + timedelta(days=5))
    out = sync_taxes(store, adapter)
    assert out.status == "SUCCESS" and out.rows_skipped_newer == 1
    assert store.row(10)["tax_ids"] == [1]


def test_idempotent_second_run_and_retry_clears_failure():
    store, adapter = world([10, 11], {11: (404, {"error": "x"})})
    first = sync_taxes(store, adapter)
    assert first.status == "PARTIAL" and set(store.failures) == {11}
    adapter.responses[11] = []
    second = sync_taxes(store, adapter, clock=TickClock(BASE + timedelta(hours=3)))
    assert second.status == "SUCCESS" and second.rows_unchanged == 1 and second.rows_inserted == 1
    assert store.failures == {} and store.row(11)["items_count"] == 0
    assert len(store.taxes_rows) == 2


def test_batches_commit_and_db_failure_keeps_saved_batches(monkeypatch):
    monkeypatch.setattr(pte, "WRITE_BATCH", 2)
    store, adapter = world([10, 11, 12, 13])
    store.fail_upsert_after = 1
    out = sync_taxes(store, adapter)
    assert out.status == "FAILED" and "2 relaciones de lotes anteriores quedaron guardadas" in out.error
    assert set(p for (_, p) in store.taxes_rows) == {10, 11}
    assert TOKEN not in out.error and TOKEN not in json.dumps(store.runs[1]["summary"], default=str)
    assert store.events.count("tx_begin") == 2 and not adapter.tx_violations


def test_final_transaction_failure_reports_failed_without_losing_batches():
    store, adapter = world([10, 11])
    store.fail_final = True
    out = sync_taxes(store, adapter)
    assert out.status == "FAILED" and store.row(10) is not None and store.runs[1]["status"] == "FAILED"


# --- product_taxes: dry-run, locks, límites --------------------------------------------------------


def test_dry_run_writes_nothing_and_takes_no_lock():
    store, adapter = world([10, 11, 12], {11: []})
    seed_row(store, 10, [1])
    before = copy.deepcopy(store.taxes_rows)
    out = sync_taxes(store, adapter, dry_run=True, limit=2)
    assert out.status == "SUCCESS" and out.dry_run and out.sync_run_id is None
    assert store.taxes_rows == before and not store.runs and "lock" not in store.events
    assert adapter.calls == [10, 11] and out.rows_unchanged == 1 and out.rows_inserted == 1


def test_limit_only_in_dry_run():
    store, adapter = world([10])
    with pytest.raises(UnsupportedSyncError):
        sync_taxes(store, adapter, limit=5)


def test_lock_busy_is_skipped_without_http():
    store, adapter = world([10])
    store.held.add((3, "product_taxes", "global"))
    out = sync_taxes(store, adapter)
    assert out.status == "SKIPPED" and not adapter.calls and not store.runs


def test_missing_token_fails_without_http_or_run():
    store, adapter = world([10])
    out = sync_taxes(store, adapter, env={})
    assert out.status == "FAILED" and "BSALE_TOKEN_SPA" in out.error and not adapter.calls and not store.runs


# --- orquestador ----------------------------------------------------------------------------------


class LockStore:
    def __init__(self):
        self.held: set = set()
        self.events: list = []

    def resolve_source(self, company_id):
        return SourceConfig(company_id=company_id, cpn_id=21884, name="SPA", token_env="BSALE_TOKEN_SPA")

    @contextmanager
    def advisory_lock(self, company_id, resource, scope):
        key = (company_id, resource, scope)
        if key in self.held:
            raise LockBusyError(f"lock ocupado company_id={company_id} resource={resource} scope={scope}")
        self.held.add(key)
        self.events.append(("lock", key))
        try:
            yield
        finally:
            self.held.discard(key)


def outcome(resource, status="SUCCESS", **kw):
    o = EntityOutcome(company_id=3, resource=resource, scope="global", mode="FULL_RECONCILE", status=status, **kw)
    return o


class Recorder:
    def __init__(self, statuses=None, raise_on=None):
        self.statuses = statuses or {}
        self.raise_on = raise_on
        self.calls: list = []

    def entity(self, company_id, resource, dry_run):
        self.calls.append((resource, dry_run))
        if resource == self.raise_on:
            raise RuntimeError(f"conexión caída {TOKEN}")
        return outcome(resource, self.statuses.get(resource, "SUCCESS"), error=self.statuses.get(f"{resource}_error"))

    def taxes(self, company_id, dry_run, limit):
        self.calls.append(("product_taxes", dry_run, limit))
        return outcome("product_taxes", self.statuses.get("product_taxes", "SUCCESS"))


def catalog(rec=None, store=None, **kw):
    rec = rec or Recorder()
    store = store or LockStore()
    report = run_catalog_sync(store=store, company_id=kw.pop("company_id", 3), entity_sync=rec.entity,
                              tax_sync=rec.taxes, getenv=ENV.get, **kw)
    return report, rec, store


def statuses(report):
    return {s.resource: s.status for s in report.steps}


def test_catalog_runs_only_catalog_resources_in_order_under_lock():
    report, rec, store = catalog()
    assert report.status == "SUCCESS"
    assert [c[0] for c in rec.calls] == list(CATALOG_ORDER)
    assert CATALOG_ORDER == ("taxes", "product_types", "products", "variants", "product_taxes")
    assert store.events == [("lock", (3, "catalog", "daily"))]


def test_variants_and_product_taxes_need_products():
    report, rec, _ = catalog(Recorder({"products": "FAILED"}))
    assert statuses(report) == {"taxes": "SUCCESS", "product_types": "SUCCESS", "products": "FAILED",
                                "variants": "SKIPPED_DEPENDENCY", "product_taxes": "SKIPPED_DEPENDENCY"}
    assert [c[0] for c in rec.calls] == ["taxes", "product_types", "products"]
    assert report.status == "PARTIAL"


def test_product_taxes_needs_taxes_but_variants_still_run():
    report, rec, _ = catalog(Recorder({"taxes": "FAILED"}))
    assert statuses(report)["variants"] == "SUCCESS"
    assert statuses(report)["product_taxes"] == "SKIPPED_DEPENDENCY"
    assert "taxes FAILED" in report.step("product_taxes").error


def test_product_taxes_failure_does_not_undo_variants():
    report, _, _ = catalog(Recorder({"product_taxes": "PARTIAL"}))
    assert statuses(report)["variants"] == "SUCCESS" and report.status == "PARTIAL"


def test_resource_lock_busy_counts_as_not_success_for_dependencies():
    report, _, _ = catalog(Recorder({"products": "SKIPPED"}))
    assert statuses(report)["variants"] == "SKIPPED_DEPENDENCY"
    assert "no ejecutado" in report.step("products").error and report.status == "PARTIAL"


def test_exception_in_resource_is_isolated_and_redacted():
    report, rec, _ = catalog(Recorder(raise_on="product_types"))
    assert statuses(report)["product_types"] == "FAILED" and statuses(report)["products"] == "SUCCESS"
    assert TOKEN not in report.step("product_types").error and TOKEN not in format_catalog_report(report)


def test_skip_product_taxes_flag():
    report, rec, _ = catalog(skip_product_taxes=True)
    assert report.status == "SUCCESS" and statuses(report)["product_taxes"] == "SKIPPED_BY_FLAG"
    assert "product_taxes" not in [c[0] for c in rec.calls]


def test_dry_run_propagates_and_takes_no_lock():
    report, rec, store = catalog(dry_run=True, product_taxes_limit=20)
    assert all(c[1] is True for c in rec.calls) and rec.calls[-1] == ("product_taxes", True, 20)
    assert not store.events and report.dry_run


def test_catalog_lock_busy_is_skipped_without_calls():
    store = LockStore()
    store.held.add((3, "catalog", "daily"))
    report, rec, _ = catalog(store=store)
    assert report.status == "SKIPPED" and not rec.calls and not report.steps


def test_only_company_3_and_limit_only_in_dry_run():
    report, rec, _ = catalog(company_id=1)
    assert report.status == "FAILED" and not rec.calls
    report, rec, _ = catalog(product_taxes_limit=5)
    assert report.status == "FAILED" and not rec.calls


def test_all_failed_is_failed():
    report, _, _ = catalog(Recorder({r: "FAILED" for r in ("taxes", "product_types", "products")}))
    assert report.status == "FAILED"


def test_missing_token_fails_before_any_resource():
    rec = Recorder()
    report = run_catalog_sync(store=LockStore(), company_id=3, entity_sync=rec.entity, tax_sync=rec.taxes,
                              getenv={}.get)
    assert report.status == "FAILED" and "BSALE_TOKEN_SPA" in report.error and not rec.calls


# --- CLI ------------------------------------------------------------------------------------------


def run_cli(argv, report=None):
    out, err = io.StringIO(), io.StringIO()
    calls = []

    def runner(**kw):
        calls.append(kw)
        return report or CatalogReport(company_id=3, status="SUCCESS")

    code = cli.main(argv, catalog_runner=runner, out=out, err=err)
    return code, out.getvalue(), err.getvalue(), calls


@pytest.mark.parametrize("status,code", [("SUCCESS", 0), ("PARTIAL", 2), ("FAILED", 1), ("SKIPPED", 3)])
def test_cli_exit_codes(status, code):
    rc, out, _, calls = run_cli(["sync-catalog", "--company", "3"], CatalogReport(company_id=3, status=status))
    assert rc == code and f"status={status}" in out
    assert calls == [{"company_id": 3, "dry_run": False, "skip_product_taxes": False, "product_taxes_limit": None}]


@pytest.mark.parametrize("argv", [
    ["sync-catalog", "--company", "1"],
    ["sync-catalog", "--company", "2"],
    ["sync-catalog", "--company", "3", "--product-taxes-limit", "5"],
    ["sync-catalog", "--company", "3", "--dry-run", "--product-taxes-limit", "0"],
    ["sync-catalog", "--company", "3", "--dry-run", "--skip-product-taxes", "--product-taxes-limit", "5"],
    ["sync-catalog"],
])
def test_cli_usage_errors(argv):
    rc, _, _, calls = run_cli(argv)
    assert rc == 64 and not calls


def test_cli_flags_are_forwarded():
    rc, _, _, calls = run_cli(["sync-catalog", "--company", "3", "--dry-run", "--product-taxes-limit", "25"])
    assert rc == 0 and calls[0] == {"company_id": 3, "dry_run": True, "skip_product_taxes": False,
                                    "product_taxes_limit": 25}
    rc, _, _, calls = run_cli(["sync-catalog", "--company", "3", "--skip-product-taxes"])
    assert calls[0]["skip_product_taxes"] is True


def test_sync_resource_choices_do_not_include_product_taxes():
    rc, _, _, _ = run_cli(["sync", "--company", "3", "--resource", "product_taxes", "--mode", "full-reconcile"])
    assert rc == 64


# --- punta a punta: motor real de entidades + motor real de product_taxes ---------------------------


def tax_def(i, pct):
    return {"href": f"{API}/taxes/{i}.json", "id": i, "name": f"Impuesto {i}", "percentage": pct,
            "forAllProducts": 0, "ledgerAccount": "", "code": "", "state": 0}


def product_type(i):
    return {"href": f"{API}/product_types/{i}.json", "id": i, "name": f"Tipo {i}", "isEditable": 1, "state": 0,
            "imagestionCategoryId": 0, "prestashopCategoryId": 0,
            "attributes": {"href": f"{API}/product_types/{i}/attributes.json"}}


class CatalogBsale(BaseAdapter):
    """Listados paginados por endpoint + product_taxes por producto."""

    def __init__(self, listings, taxes):
        super().__init__()
        self.listings = listings
        self.taxes = TaxesBsale(taxes)
        self.paths: list[str] = []

    def send(self, request, **kwargs):
        path = urlsplit(request.url).path
        self.paths.append(path)
        assert request.headers.get("access_token") == TOKEN
        if TAX_RE.search(path):
            return self.taxes.send(request, **kwargs)
        q = parse_qs(urlsplit(request.url).query)
        assert set(q) == {"limit", "offset"}, request.url  # barrido sin state ni expand
        name = path.removeprefix("/v1/").removesuffix(".json")
        items = self.listings[name]
        offset, limit = int(q["offset"][0]), int(q["limit"][0])
        body = {"href": f"{API}/{name}.json", "count": len(items), "limit": limit, "offset": offset,
                "items": items[offset:offset + limit]}
        return make_response(request, 200, json.dumps(body).encode())

    def close(self):
        pass


def e2e(entity_store, tax_store, adapter, *, at, dry_run=False):
    factory = client_factory_for(adapter)

    def entity_sync(company_id, resource, dry):
        return run_entity_sync(store=entity_store, company_id=company_id, resource=resource,
                               mode=SyncMode.FULL_RECONCILE, dry_run=dry, client_factory=factory,
                               clock=TickClock(at), getenv=ENV.get, trigger="CATALOG_DAILY", host="test")

    def tax_sync(company_id, dry, limit):
        return run_product_tax_sync(store=tax_store, company_id=company_id, dry_run=dry, limit=limit,
                                    client_factory=factory, clock=TickClock(at + timedelta(minutes=5)),
                                    getenv=ENV.get, trigger="CATALOG_DAILY", host="test")

    return run_catalog_sync(store=LockStore(), company_id=3, dry_run=dry_run, entity_sync=entity_sync,
                            tax_sync=tax_sync, getenv=ENV.get)


def test_end_to_end_new_product_new_variants_deactivation_and_taxes():
    day1 = {
        "taxes": [tax_def(1, "19.0"), tax_def(8, "31.5")],
        "product_types": [product_type(4)],
        "products": [product(100), product(101)],
        "variants": [variant(31304, product_id=100)],
    }
    entity_store = FakeStore()
    tax_store = FakeTaxStore(catalog=entity_store)
    adapter = CatalogBsale(day1, {100: [1, 8], 101: []})
    report = e2e(entity_store, tax_store, adapter, at=BASE)
    assert report.status == "SUCCESS", format_catalog_report(report)
    assert tax_store.row(100)["tax_ids"] == [1, 8] and tax_store.row(101)["items_count"] == 0

    day2 = copy.deepcopy(day1)
    day2["products"] = [product(100), product(101, state=1), product(102, name="Nuevo")]
    new_variant = variant(31306, product_id=102, extraField={"keep": True})
    day2["variants"] += [new_variant, variant(31323, product_id=100)]
    adapter = CatalogBsale(day2, {100: [1, 8], 101: [], 102: [1, 5]})
    report = e2e(entity_store, tax_store, adapter, at=BASE + timedelta(days=1))
    assert report.status == "PARTIAL"  # tax 5 no existe en bsale_raw.taxes: se guarda y se reporta
    assert statuses(report) == {"taxes": "SUCCESS", "product_types": "SUCCESS", "products": "SUCCESS",
                                "variants": "SUCCESS", "product_taxes": "PARTIAL"}
    products = entity_store.rows(3, "bsale_raw.products")
    variants = entity_store.rows(3, "bsale_raw.variants")
    assert products[101]["state"] == 1 and products[101]["missing_since"] is None  # desactivado, no borrado
    assert products[102]["name"] == "Nuevo"
    assert variants[31306]["product_id"] == 102 and variants[31323]["product_id"] == 100
    assert variants[31306]["missing_since"] is None and variants[31323]["missing_since"] is None
    assert variants[31306]["payload"] == new_variant  # payload exacto, incluidos campos no tipados
    assert tax_store.row(102)["tax_ids"] == [1, 5]
    assert report.step("product_taxes").outcome.point["unknown_tax_products"] == {"102": [5]}
    assert report.step("products").outcome.rows_inserted == 1 and report.step("variants").outcome.rows_inserted == 2
    text = format_catalog_report(report)
    assert TOKEN not in text and "resource=product_taxes status=PARTIAL" in text


def test_end_to_end_dry_run_writes_nothing():
    listings = {"taxes": [tax_def(1, "19.0")], "product_types": [product_type(4)],
                "products": [product(100)], "variants": [variant(31304, product_id=100)]}
    entity_store = FakeStore()
    tax_store = FakeTaxStore([100])
    report = e2e(entity_store, tax_store, CatalogBsale(listings, {}), at=BASE, dry_run=True)
    assert report.status == "SUCCESS", format_catalog_report(report)
    assert all(not rows for rows in entity_store.tables.values()) and not entity_store.runs
    assert not tax_store.taxes_rows and not tax_store.runs


# --- probe read-only de expand -----------------------------------------------------------------------


def _probe_result(expanded_ids, ref_ids, *, count_ok=True):
    from backend.debug.bsale_product_taxes_expand_probe import _compare

    pages = []
    for offset in (0, 5):
        products = {}
        for pid, (exp, ref) in enumerate(zip(expanded_ids, ref_ids), start=offset + 1):
            expanded = {"items": None} if exp is None else {"items": len(exp), "count": len(exp), "tax_ids": exp,
                                                            "has_next": False}
            reference = {"status": 200, "count": len(ref), "tax_ids": ref}
            products[str(pid)] = {"expanded": expanded, "reference": reference, "verdict": _compare(expanded, reference)}
        pages.append({"offset": offset, "status": 200, "count": 10 if count_ok else 9, "products": products})
    return {"baseline": {"count": 10}, "pages": pages, "explicit": {}}


def test_probe_verdicts_require_evidence():
    from backend.debug.bsale_product_taxes_expand_probe import verdict

    assert verdict(_probe_result([["1"], []], [["1"], []])) == "EXPAND_COMPLETE"
    assert verdict(_probe_result([None, None], [["1"], []])) == "EXPAND_IGNORED"
    assert verdict(_probe_result([["1"], ["1"]], [["1"], []])).startswith("EXPAND_UNRELIABLE")
    assert verdict(_probe_result([["1"], ["8"]], [["1"], ["8"]])).startswith("INCONCLUSIVE (sin producto sin impuestos")
    assert verdict(_probe_result([["1"], []], [["1"], []], count_ok=False)).startswith("INCONCLUSIVE")
    assert verdict({"aborted": "429", "pages": []}).startswith("INCONCLUSIVE")


def test_catalog_module_touches_no_foreign_resources():
    source = open(catalog_daily.__file__, encoding="utf-8").read()
    for word in ("stock", "cost", "price", "document", "nightly", "distribuidora"):
        assert not re.search(rf"\b(import|from)\b[^\n]*{word}", source), word
