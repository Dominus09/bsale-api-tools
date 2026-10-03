"""Fase 4D1: stock RAW por empresa + sucursal (scanner / full reconcile). Sin red ni BD real."""

from __future__ import annotations

import copy
import io
import json
import logging
from datetime import timedelta
from decimal import Decimal
from urllib.parse import parse_qs, urlsplit

import pytest
import requests
from requests.adapters import BaseAdapter

from backend.jobs.bsale_raw import cli
from backend.services.bsale.companies import BsaleCompany
from backend.services.bsale_raw.core import engine, stock_engine
from backend.services.bsale_raw.core.client import build_company_client
from backend.services.bsale_raw.core.engine import UnsupportedSyncError
from backend.services.bsale_raw.core.models import SyncMode
from backend.services.bsale_raw.core.rate_limit import (
    CompanyRateLimiters,
    PriorityRateLimiter,
    RateLimitConfig,
    RequestPriority,
    TokenBucket,
)
from backend.services.bsale_raw.core.registry import REGISTRY, KeyKind
from backend.services.bsale_raw.core.snapshot import build_stock_rows, fetch_snapshot
from backend.services.bsale_raw.core.stock_engine import STOCK_SCAN_DRIFT, run_stock_sync
from backend.services.bsale_raw.core.store import (
    EntityOutcome,
    PgRawTx,
    advisory_lock_keys,
    build_stock_delete_stale,
    build_stock_upsert,
)
from backend.tests.bsale_raw._raw_sql_schema import SQL_DIR, parse
from backend.tests.bsale_raw.test_bsale_raw_pipeline import (
    BASE,
    ENV,
    TOKEN,
    FakeConnection,
    FakeStore,
    TickClock,
    client_factory_for,
    make_response,
    office,
    run,
)

API = "https://api.bsale.io/v1"
STOCKS = REGISTRY.get("stocks")
TABLE = STOCKS.raw_table
SCANNER = SyncMode.SCANNER
RECONCILE = SyncMode.FULL_RECONCILE


def stock(variant_id: int, office_id: int = 1, *, sid: int | None = None, quantity=10.0, reserved=0.0,
          available=10.0, **over) -> dict:
    sid = variant_id * 100 + office_id if sid is None else sid
    item = {"href": f"{API}/stocks/{sid}.json", "id": sid, "quantity": quantity, "quantityReserved": reserved,
            "quantityAvailable": available,
            "variant": {"href": f"{API}/variants/{variant_id}.json", "id": str(variant_id)},
            "office": {"href": f"{API}/offices/{office_id}.json", "id": str(office_id)}}
    item.update(over)
    return item


class StockBsale(BaseAdapter):
    """``/v1/stocks.json`` paginado por limit/offset y filtrado por ``officeid`` / ``variantid``.

    ``counts``: count informado en cada llamada (endpoint mutable). ``pages``: páginas explícitas.
    ``script``: acciones previas (Exception o (status, body, headers)). ``on_call(n)``: hook por llamada.
    """

    def __init__(self, items=None, *, store=None, counts=None, pages=None, script=None, on_call=None,
                 ignore_office_filter=False, ignore_variant_filter=False):
        super().__init__()
        self.ignore_variant_filter = ignore_variant_filter
        self.items = list(items or [])
        self.store = store
        self.counts = list(counts) if counts is not None else None
        self.pages = pages
        self.script = list(script or [])
        self.on_call = on_call
        self.ignore_office_filter = ignore_office_filter
        self.calls: list = []
        self.tx_violations: list[str] = []

    def send(self, request, **kwargs):
        if self.store is not None:
            if self.store.in_tx:
                self.tx_violations.append(request.url)
            self.store.events.append("http")
        self.calls.append(request)
        if self.on_call is not None:
            self.on_call(len(self.calls))
        if self.script:
            action = self.script.pop(0)
            if isinstance(action, BaseException):
                raise action
            status, body, headers = action
            return make_response(request, status, body, headers)
        q = parse_qs(urlsplit(request.url).query)
        offset, limit = int(q["offset"][0]), int(q["limit"][0])
        items = self.items
        if "officeid" in q and not self.ignore_office_filter:
            items = [it for it in items if it["office"]["id"] == q["officeid"][0]]
        if "variantid" in q and not self.ignore_variant_filter:
            items = [it for it in items if it["variant"]["id"] == q["variantid"][0]]
        if self.pages is not None:
            page = self.pages[min(len(self.calls), len(self.pages)) - 1]
        else:
            page = items[offset: offset + limit]
        count = len(items)
        if self.counts is not None:
            count = self.counts[min(len(self.calls), len(self.counts)) - 1]
        body = {"href": f"{API}/stocks.json", "count": count, "limit": limit, "offset": offset, "items": page}
        return make_response(request, 200, json.dumps(body).encode())

    def close(self):
        pass


def setup(items=None, **kw):
    store = FakeStore()
    adapter = StockBsale(items, store=store, **kw)
    return store, adapter


def go(store, adapter, *, company_id=3, office_id=1, mode=SCANNER, dry_run=False, clock=None, env=None):
    return run_stock_sync(
        store=store, company_id=company_id, office_id=office_id, mode=mode, dry_run=dry_run,
        client_factory=client_factory_for(adapter), clock=clock or TickClock(BASE),
        getenv=(ENV if env is None else env).get, host="test",
    )


def old():
    return BASE - timedelta(days=1)


# --- spec / schema ---------------------------------------------------------------------------


def test_stock_spec_enabled_with_real_modes_and_typed_columns():
    assert STOCKS.pipeline_enabled and STOCKS.key_kind is KeyKind.STOCK and STOCKS.partition_by_office
    assert STOCKS.pipeline_modes == (SCANNER, RECONCILE, SyncMode.POINT)
    assert STOCKS.request_priority is RequestPriority.P2_STOCK
    assert [(c.column, c.payload_key) for c in STOCKS.typed_columns] == [
        ("bsale_stock_id", "id"), ("quantity", "quantity"),
        ("quantity_reserved", "quantityReserved"), ("quantity_available", "quantityAvailable"),
    ]


def test_schema_pk_is_company_variant_office_and_stock_id_not_unique():
    tables, indexes = parse()
    table = tables["stocks"]
    assert table.pk == ("company_id", "variant_id", "office_id")
    assert not table.uniques
    for col in STOCKS.typed_columns:
        assert col.column in table.columns, col.column
    assert {"payload", "payload_hash", "first_seen_at", "last_seen_at", "last_changed_at", "api_fetched_at",
            "last_source", "sync_run_id"} <= set(table.columns)
    stock_ix = [ix for ix in indexes.values() if ix.table == "stocks"]
    assert stock_ix and not any(ix.unique for ix in stock_ix)
    assert indexes["ix_raw_stocks_bsale_stock_id"].columns == ("company_id", "bsale_stock_id")


def test_engine_modes_accepted_by_schema_checks():
    sql = (SQL_DIR / "002_sync_control.sql").read_text(encoding="utf-8")
    for mode in STOCKS.pipeline_modes:
        assert f"'{mode.value}'" in sql


# --- scope / endpoint / cantidades -----------------------------------------------------------


def test_scanner_scope_office_endpoint_and_quantities():
    items = [stock(1, quantity=12.0, reserved=3.0, available=9.0), stock(2, quantity=5.5, reserved=0.0, available=5.5),
             stock(3, office_id=4)]
    store, adapter = setup(items)
    out = go(store, adapter)
    assert out.status == "SUCCESS", out.error
    assert (out.scope, out.mode, out.api_count, out.rows_received, out.rows_inserted) == ("office:1", "SCANNER", 2, 2, 2)
    call = adapter.calls[0]
    assert urlsplit(call.url).path == "/v1/stocks.json"
    assert parse_qs(urlsplit(call.url).query) == {"limit": ["50"], "offset": ["0"], "officeid": ["1"]}
    rows = store.stock_rows(3)
    assert set(rows) == {(1, 1), (2, 1)}
    r1 = rows[(1, 1)]
    assert (r1["quantity"], r1["quantity_reserved"], r1["quantity_available"]) == (
        Decimal("12.0"), Decimal("3.0"), Decimal("9.0"))
    assert r1["bsale_stock_id"] == 101 and r1["payload"] == items[0] and r1["last_source"] == "SCANNER"
    assert store.sync_state[(3, "stocks", "office:1")]["status"] == "SUCCESS"
    assert store.entity_runs[out.sync_run_id + 1]["scope"] == "office:1"


def test_available_is_never_recalculated():
    """OC 33 reserva antes de la salida física: available se guarda tal cual, aunque no cuadre."""
    store, adapter = setup([stock(1, quantity=10.0, reserved=4.0, available=8.0)])
    out = go(store, adapter)
    row = store.stock_rows(3)[(1, 1)]
    assert out.status == "SUCCESS"
    assert (row["quantity"], row["quantity_reserved"], row["quantity_available"]) == (
        Decimal("10.0"), Decimal("4.0"), Decimal("8.0"))


def test_explicit_zero_and_null_preserved():
    items = [stock(1, quantity=0, reserved=0, available=0), stock(2, quantity=None, reserved=None, available=None)]
    store, adapter = setup(items)
    assert go(store, adapter).status == "SUCCESS"
    rows = store.stock_rows(3)
    assert (rows[(1, 1)]["quantity"], rows[(1, 1)]["quantity_available"]) == (Decimal("0"), Decimal("0"))
    assert rows[(2, 1)]["quantity"] is None and rows[(2, 1)]["quantity_reserved"] is None


def test_bsale_stock_id_is_not_identity():
    items = [stock(1, sid=555), stock(2, sid=555)]
    store, adapter = setup(items)
    assert go(store, adapter).status == "SUCCESS"
    assert set(store.stock_rows(3)) == {(1, 1), (2, 1)}

    adapter.items = [stock(1, sid=999, quantity=3.0), stock(2, sid=555)]
    out = go(store, adapter, clock=TickClock(BASE + timedelta(minutes=5)))
    assert out.rows_inserted == 0 and out.rows_updated == 1
    assert store.stock_rows(3)[(1, 1)]["bsale_stock_id"] == 999 and len(store.stock_rows(3)) == 2


def test_absent_row_is_not_zeroed_nor_deleted_by_scanner():
    store, adapter = setup([stock(1)])
    absent = stock(99, quantity=7.0, available=7.0)
    store.seed_stock(3, absent, fetched_at=old())
    before = copy.deepcopy(store.stock_rows(3)[(99, 1)])
    out = go(store, adapter)
    assert out.status == "SUCCESS" and out.rows_deleted == 0 and out.rows_missing == 0
    assert store.stock_rows(3)[(99, 1)] == before
    assert out.fuse["deletes"] is False and out.fuse["absent_not_seen"] == 1


def test_multi_page_fetched_at_per_page_and_no_http_n_plus_one():
    items = [stock(i) for i in range(1, 121)]
    clock = TickClock(BASE)
    store, adapter = setup(items)
    out = go(store, adapter, clock=clock)
    assert out.status == "SUCCESS" and out.pages == 3 and out.requests == 3 and out.rows_inserted == 120
    assert [parse_qs(urlsplit(c.url).query)["offset"][0] for c in adapter.calls] == ["0", "50", "100"]
    assert {urlsplit(c.url).path for c in adapter.calls} == {"/v1/stocks.json"}
    rows = store.stock_rows(3)
    page1 = {rows[(i, 1)]["api_fetched_at"] for i in range(1, 51)}
    page2 = {rows[(i, 1)]["api_fetched_at"] for i in range(51, 101)}
    assert len(page1) == 1 and len(page2) == 1 and min(page1) < min(page2)
    assert min(page1) > out.snapshot_started_at
    assert not adapter.tx_violations


# --- política de snapshot mutable -------------------------------------------------------------


def test_scanner_tolerates_count_drift_within_tolerance():
    items = [stock(i) for i in range(1, 101)]
    store, adapter = setup(items, counts=[100, 104])
    out = go(store, adapter)
    assert out.status == "SUCCESS", out.error
    assert out.fuse["count_first"] == 100 and out.fuse["count_last"] == 104
    assert out.fuse["count_tolerance"] == STOCK_SCAN_DRIFT.tolerance(100) == 10


def test_scanner_fails_on_count_drift_beyond_tolerance():
    items = [stock(i) for i in range(1, 101)]
    store, adapter = setup(items, counts=[100, 130])
    out = go(store, adapter)
    assert out.status == "FAILED" and "count cambió" in out.error and store.stock_rows(3) == {}


def test_reconcile_requires_strict_snapshot():
    items = [stock(i) for i in range(1, 101)]
    store, adapter = setup(items, counts=[100, 101])
    out = go(store, adapter, mode=RECONCILE)
    assert out.status == "FAILED" and "count cambió" in out.error and store.stock_rows(3) == {}


def test_scanner_rejects_evident_truncation():
    store, adapter = setup(pages=[[stock(i) for i in range(1, 51)], []], counts=[200])
    out = go(store, adapter)
    assert out.status == "FAILED" and "truncada" in out.error and store.stock_rows(3) == {}


def test_duplicate_composite_key_fails_without_writes():
    page1 = [stock(i) for i in range(1, 51)]
    store, adapter = setup(pages=[page1, [stock(50), stock(51)]], counts=[52])
    for mode in (SCANNER, RECONCILE):
        out = go(store, adapter, mode=mode)
        assert out.status == "FAILED" and "duplicadas" in out.error
    assert store.stock_rows(3) == {}


def test_wrong_office_in_response_fails_without_writes():
    store, adapter = setup([stock(1, office_id=1), stock(2, office_id=4)], ignore_office_filter=True)
    out = go(store, adapter)
    assert out.status == "FAILED" and "sucursales [4]" in out.error and store.stock_rows(3, table=TABLE) == {}


@pytest.mark.parametrize(
    "bad",
    [{"variant": None}, {"variant": {"id": "x"}}, {"office": "1"}, {"quantity": "mucho"},
     {"quantityReserved": True}, {"quantityAvailable": [1]}],
)
def test_invalid_item_fails_before_any_write(bad):
    store, adapter = setup([stock(1), {**stock(2), **bad}], ignore_office_filter=True)
    out = go(store, adapter)
    assert out.status == "FAILED" and store.stock_rows(3) == {}
    assert "upsert_stock" not in store.events


# --- frescura / cambios ------------------------------------------------------------------------


def test_stale_upsert_rejected_and_counted():
    store, adapter = setup([stock(1, quantity=1.0), stock(2)])
    store.seed_stock(3, stock(1, quantity=50.0), fetched_at=BASE + timedelta(hours=1))
    out = go(store, adapter)
    assert out.status == "SUCCESS" and out.rows_skipped_newer == 1 and out.rows_inserted == 1
    assert store.stock_rows(3)[(1, 1)]["payload"]["quantity"] == 50.0


def test_targeted_refresh_during_scan_survives_old_scanner():
    clock = TickClock(BASE)
    items = [stock(i) for i in range(1, 61)]
    store = FakeStore()

    def targeted(n):
        if n == 2:  # mientras el scanner pide la página 2, llega un refresh dirigido de la variante 1
            store.seed_stock(3, stock(1, quantity=0.0, reserved=5.0, available=0.0), fetched_at=clock())

    adapter = StockBsale(items, store=store, on_call=targeted)
    out = go(store, adapter, clock=clock)
    assert out.status == "SUCCESS" and out.rows_skipped_newer == 1 and out.rows_inserted == 59
    row = store.stock_rows(3)[(1, 1)]
    assert row["payload"]["quantityReserved"] == 5.0 and row["last_source"] == "SCANNER" and row["sync_run_id"] is None


def test_changed_vs_unchanged_timestamps_and_run2_semantics():
    clock = TickClock(BASE)
    store, adapter = setup([stock(1), stock(2), stock(3)])
    out1 = go(store, adapter, clock=clock)
    assert (out1.rows_inserted, out1.rows_updated, out1.rows_unchanged) == (3, 0, 0)
    snap = copy.deepcopy(store.stock_rows(3))

    adapter.items = [stock(1, quantity=9.0, reserved=1.0, available=8.0), stock(2), stock(3)]  # una venta real
    out2 = go(store, adapter, clock=clock)
    assert (out2.rows_inserted, out2.rows_updated, out2.rows_unchanged, out2.rows_deleted) == (0, 1, 2, 0)
    rows = store.stock_rows(3)
    assert rows[(1, 1)]["last_changed_at"] > snap[(1, 1)]["last_changed_at"]
    assert rows[(1, 1)]["quantity_reserved"] == Decimal("1.0")
    for key in rows:
        assert rows[key]["first_seen_at"] == snap[key]["first_seen_at"]
        assert rows[key]["last_seen_at"] > snap[key]["last_seen_at"]
        assert rows[key]["api_fetched_at"] > snap[key]["api_fetched_at"]
    for key in ((2, 1), (3, 1)):
        assert rows[key]["last_changed_at"] == snap[key]["last_changed_at"]
        assert rows[key]["payload_hash"] == snap[key]["payload_hash"]


# --- aislamiento / lock / limiter ------------------------------------------------------------


def test_company_and_office_isolation():
    store, adapter = setup([stock(1, office_id=1), stock(2, office_id=4)])
    store.seed_stock(1, stock(1, office_id=1, quantity=99.0), fetched_at=old())
    store.seed_stock(3, stock(7, office_id=4), fetched_at=old())
    c1 = copy.deepcopy(store.stock_rows(1))
    o4 = copy.deepcopy(store.stock_rows(3, office_id=4))
    for mode in (SCANNER, RECONCILE):
        out = go(store, adapter, mode=mode)
        assert out.status == "SUCCESS", out.error
        assert out.rows_deleted == 0
    assert store.stock_rows(1) == c1 and store.stock_rows(3, office_id=4) == o4
    assert set(store.stock_rows(3, office_id=1)) == {(1, 1)}


def test_advisory_lock_is_per_office():
    assert advisory_lock_keys(3, "stocks", "office:1") != advisory_lock_keys(3, "stocks", "office:4")
    store, adapter = setup([stock(1, office_id=1), stock(2, office_id=4)])
    store.held.add(advisory_lock_keys(3, "stocks", "office:1"))
    busy = go(store, adapter, office_id=1)
    assert busy.status == "SKIPPED" and cli.exit_code(busy) == cli.EXIT_LOCKED
    other = go(store, adapter, office_id=4)
    assert other.status == "SUCCESS" and set(store.stock_rows(3)) == {(2, 4)}


def test_limiter_shared_with_other_resources_per_company():
    assert stock_engine.default_client_factory is engine.default_client_factory
    limiters = CompanyRateLimiters(lambda cid: PriorityRateLimiter(TokenBucket(RateLimitConfig(5.0, 10))))
    company = BsaleCompany(company_id=3, name="SPA", token_env="BSALE_TOKEN_SPA", token=TOKEN)
    stock_client = build_company_client(company, limiters, STOCKS.request_priority)
    catalog_client = build_company_client(company, limiters, REGISTRY.get("products").request_priority)
    assert stock_client.session._limiter is catalog_client.session._limiter
    assert stock_client.session.priority == RequestPriority.P2_STOCK
    assert RequestPriority.P0_TARGETED < stock_client.session.priority < catalog_client.session.priority


# --- red / BD ---------------------------------------------------------------------------------


def test_429_retried_and_counted():
    store, adapter = setup([stock(1)], script=[(429, b"{}", {"Retry-After": "0"})])
    out = go(store, adapter)
    assert out.status == "SUCCESS" and out.http_429 == 1 and out.requests == 2


def test_timeout_retried_then_exhausted_without_writes():
    store, adapter = setup([stock(1)], script=[requests.Timeout("lento")])
    assert go(store, adapter).status == "SUCCESS"

    store, adapter = setup([stock(1)], script=[requests.Timeout("lento")] * 3)
    out = go(store, adapter)
    assert out.status == "FAILED" and store.stock_rows(3) == {} and out.requests == 3


def test_malformed_json_fails_without_writes():
    store, adapter = setup([stock(1)], script=[(200, b"<html>no json</html>", {})])
    out = go(store, adapter)
    assert out.status == "FAILED" and "JSON" in out.error and store.stock_rows(3) == {}


def test_rollback_on_write_failure():
    store, adapter = setup([stock(1, quantity=1.0)])
    store.seed_stock(3, stock(2), fetched_at=old())
    before = copy.deepcopy(store.tables[TABLE])
    store.fail_on = "upsert"
    out = go(store, adapter)
    assert out.status == "FAILED" and store.tables[TABLE] == before
    assert (out.rows_inserted, out.rows_updated) == (0, 0)
    assert store.runs[out.sync_run_id]["status"] == "FAILED"
    assert store.sync_state[(3, "stocks", "office:1")]["last_success_at"] is None


def test_orphan_and_inactive_variants_are_stored_as_delivered():
    store, adapter = setup([stock(424242), stock(5)])
    store.tables["bsale_raw.variants"][(3, 5)] = {"state": 1}
    out = go(store, adapter)
    assert out.status == "SUCCESS" and set(store.stock_rows(3)) == {(424242, 1), (5, 1)}
    assert store.tables["bsale_raw.variants"] == {(3, 5): {"state": 1}}


def test_token_never_appears():
    store, adapter = setup([stock(1)], script=[(401, f'{{"error": "bad {TOKEN}"}}'.encode(), {})])
    out = go(store, adapter)
    assert out.status == "FAILED" and TOKEN not in out.error
    assert TOKEN not in repr(store.runs) + repr(store.entity_runs) + repr(store.sync_state)
    assert TOKEN not in cli.format_outcome(out)


def test_no_transaction_open_during_http():
    store, adapter = setup([stock(i) for i in range(1, 80)])
    out = go(store, adapter)
    assert out.status == "SUCCESS" and not adapter.tx_violations
    first_tx = store.events.index("start_run")
    http = [i for i, e in enumerate(store.events) if e == "http"]
    assert all(i > first_tx for i in http)
    assert store.events.index("upsert_stock") > max(http)


# --- dry-run ----------------------------------------------------------------------------------


def test_dry_run_writes_nothing_and_predicts():
    store, adapter = setup([stock(1), stock(2, quantity=3.0), *(stock(i) for i in range(3, 11))])
    for i in range(2, 12):
        store.seed_stock(3, stock(i), fetched_at=old())
    before = copy.deepcopy((store.tables, store.runs, store.entity_runs, store.sync_state))
    for mode in (SCANNER, RECONCILE):
        out = go(store, adapter, mode=mode, dry_run=True)
        assert out.dry_run and out.status == "SUCCESS" and out.sync_run_id is None
        assert (out.rows_inserted, out.rows_updated, out.rows_unchanged) == (1, 1, 8)
        assert out.rows_deleted == (1 if mode is RECONCILE else 0)
    assert (store.tables, store.runs, store.entity_runs, store.sync_state) == before
    for forbidden in ("lock", "start_run", "tx_begin", "upsert_stock", "delete_stale_stock"):
        assert forbidden not in store.events


def test_dry_run_output_format():
    store, adapter = setup([stock(1), stock(2)])
    out = go(store, adapter, dry_run=True)
    lines = cli.format_outcome(out).splitlines()
    assert lines[:4] == ["dry_run=true", "company=3", "resource=stocks", "scope=office:1"]
    assert "mode=SCANNER" in lines and "api_count=2" in lines and "received=2" in lines
    assert "inserted=2" in lines and "status=SUCCESS" in lines
    assert not any("payload" in line or "quantity" in line for line in lines)


# --- full reconcile ---------------------------------------------------------------------------


def test_reconcile_deletes_only_absent_rows_of_office_never_zeroes():
    store, adapter = setup([stock(i) for i in range(1, 10)])
    for i in range(1, 11):
        store.seed_stock(3, stock(i), fetched_at=old())
    out = go(store, adapter, mode=RECONCILE)
    assert out.status == "SUCCESS" and out.rows_deleted == 1 and out.rows_missing == 0
    rows = store.stock_rows(3)
    assert (10, 1) not in rows and len(rows) == 9
    assert all(r["quantity"] is None or r["quantity"] != 0 for r in rows.values())
    assert store.sync_state[(3, "stocks", "office:1")]["last_full_reconcile_at"] is not None


def test_reconcile_never_deletes_row_refreshed_during_snapshot():
    clock = TickClock(BASE)
    store = FakeStore()
    for i in range(1, 11):
        store.seed_stock(3, stock(i), fetched_at=old())

    def targeted(n):
        if n == 1:
            store.seed_stock(3, stock(10, quantity=4.0), fetched_at=clock())

    adapter = StockBsale([stock(i) for i in range(1, 10)], store=store, on_call=targeted)
    out = go(store, adapter, mode=RECONCILE, clock=clock)
    assert out.status == "SUCCESS" and out.rows_deleted == 0
    assert store.stock_rows(3)[(10, 1)]["payload"]["quantity"] == 4.0
    assert out.fuse["protected_newer"] == 1


@pytest.mark.parametrize("received", [0, 7])
def test_reconcile_fuse_blocks_writes(received):
    store, adapter = setup([stock(i, quantity=1.0) for i in range(1, received + 1)])
    for i in range(1, 11):
        store.seed_stock(3, stock(i), fetched_at=old())
    before = copy.deepcopy(store.tables[TABLE])
    out = go(store, adapter, mode=RECONCILE)
    assert out.status == "FAILED" and out.fuse["tripped"] and store.tables[TABLE] == before
    assert "upsert_stock" not in store.events


def test_scanner_with_empty_office_never_deletes():
    store, adapter = setup([])
    for i in range(1, 6):
        store.seed_stock(3, stock(i), fetched_at=old())
    out = go(store, adapter)
    assert out.status == "SUCCESS" and out.rows_received == 0 and len(store.stock_rows(3)) == 5


# --- SQL real (cursor falso): lotes, nunca por fila ------------------------------------------


def test_stock_upsert_sql_freshness_and_composite_key():
    sql, template = build_stock_upsert(STOCKS)
    assert sql.startswith("INSERT INTO bsale_raw.stocks AS t (company_id, variant_id, office_id, bsale_stock_id")
    assert "ON CONFLICT (company_id, variant_id, office_id) DO UPDATE SET" in sql
    assert "WHERE t.api_fetched_at <= EXCLUDED.api_fetched_at" in sql
    assert sql.rstrip().endswith("RETURNING variant_id, office_id")
    set_clause = sql.split("DO UPDATE SET", 1)[1].split("WHERE", 1)[0]
    assert "first_seen_at" not in set_clause and "missing_since" not in sql
    assert "quantity_available = EXCLUDED.quantity_available" in set_clause
    assert "CASE WHEN t.payload_hash IS DISTINCT FROM EXCLUDED.payload_hash" in set_clause
    assert template.count("%s") == 12 and template.count("now()") == 3


def test_stock_delete_sql_scoped_to_office_and_fresh_rows():
    sql = build_stock_delete_stale(STOCKS)
    assert sql.startswith("DELETE FROM bsale_raw.stocks WHERE company_id = %s AND office_id = %s")
    assert "variant_id = ANY(%s)" in sql and "api_fetched_at <= %s" in sql


def test_pg_stock_batches_never_per_row():
    adapter = StockBsale([stock(i) for i in range(1, 1201)])
    snap = fetch_snapshot(client_factory_for(adapter)(None, TOKEN, STOCKS), "stocks.json", params={"officeid": 1})
    rows = build_stock_rows(STOCKS, 3, snap, office_id=1)
    assert len(adapter.calls) == 24 and len(rows) == 1200
    pages = [[(i, 1) for i in range(s, min(s + 500, 1201))] for s in (1, 501, 1001)]
    conn = FakeConnection(next_results=[[], *pages])
    tx = PgRawTx(conn.cursor())
    tx.read_existing_stock(STOCKS, 3, 1)
    applied = tx.upsert_stock(STOCKS, rows, sync_run_id=7, last_source="SCANNER")
    tx.delete_stale_stock(STOCKS, 3, 1, [5000], BASE)
    assert len(applied) == 1200
    statements = [s for s, _ in conn.executed]
    assert len(statements) == 5
    assert statements[0].startswith("SELECT variant_id, office_id") and "FOR UPDATE" not in statements[0]
    assert sum(s.startswith("INSERT INTO bsale_raw.stocks") for s in statements) == 3
    assert sum(s.startswith("DELETE FROM bsale_raw.stocks") for s in statements) == 1


# --- CLI --------------------------------------------------------------------------------------


@pytest.mark.parametrize("mode_arg, mode", [("scanner", SCANNER), ("full-reconcile", RECONCILE)])
@pytest.mark.parametrize("dry_run", [True, False])
def test_cli_stocks_with_office(mode_arg, mode, dry_run):
    seen = {}

    def runner(**kw):
        seen.update(kw)
        return EntityOutcome(company_id=3, resource="stocks", scope="office:1", mode=mode.value,
                             status="SUCCESS", dry_run=dry_run)

    argv = ["sync", "--company", "3", "--resource", "stocks", "--office", "1", "--mode", mode_arg]
    buf = io.StringIO()
    assert cli.main(argv + (["--dry-run"] if dry_run else []), runner=runner, out=buf) == cli.EXIT_SUCCESS
    assert seen == {"company_id": 3, "resource": "stocks", "mode": mode, "dry_run": dry_run, "office_id": 1,
                    "variant_id": None, "document_id": None}
    assert "scope=office:1" in buf.getvalue()


@pytest.mark.parametrize(
    "argv",
    [
        ["--resource", "stocks", "--mode", "scanner"],  # sin --office
        ["--resource", "stocks", "--office", "0", "--mode", "scanner"],
        ["--resource", "stocks", "--office", "x", "--mode", "scanner"],
        ["--resource", "offices", "--office", "1", "--mode", "full-reconcile"],  # --office en entidad
        ["--resource", "offices", "--mode", "scanner"],  # scanner sólo para stock
        ["--resource", "stocks", "--office", "1", "--mode", "incremental"],
        ["--resource", "stocks", "--office", "1", "--variant", "10888", "--mode", "scanner"],
    ],
)
def test_cli_usage_errors(argv):
    never = lambda **kw: pytest.fail("no debe ejecutarse")  # noqa: E731
    err = io.StringIO()
    code = cli.main(["sync", "--company", "3", *argv], runner=never, out=io.StringIO(), err=err)
    assert code == cli.EXIT_USAGE


def test_engine_rejects_misuse():
    with pytest.raises(UnsupportedSyncError):
        run_stock_sync(store=FakeStore(), company_id=3, office_id=1, resource="offices")
    with pytest.raises(UnsupportedSyncError):
        run_stock_sync(store=FakeStore(), company_id=3, office_id=0)
    with pytest.raises(UnsupportedSyncError):
        run_stock_sync(store=FakeStore(), company_id=3, office_id=1, mode=SyncMode.INCREMENTAL)


def test_previous_resources_still_work():
    store = FakeStore()
    from backend.tests.bsale_raw.test_bsale_raw_pipeline import FakeBsale

    adapter = FakeBsale([office(1), office(2)], store=store)
    out = run(store, adapter)
    assert out.status == "SUCCESS" and out.rows_inserted == 2 and out.scope == "global"
    assert store.stock_rows(3) == {}


def test_log_never_contains_payload(caplog):
    caplog.set_level(logging.DEBUG)
    store, adapter = setup([stock(1, quantity=123456.0)])
    assert go(store, adapter).status == "SUCCESS"
    assert "123456" not in caplog.text and TOKEN not in caplog.text
