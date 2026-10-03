"""Fase 4D2: refresh dirigido de stock (mode POINT) por variante [+ sucursal]. Sin red ni BD real."""

from __future__ import annotations

import copy
import io
import json
from datetime import timedelta
from decimal import Decimal
from urllib.parse import parse_qs, urlsplit

import pytest
import requests

from backend.jobs.bsale_raw import cli
from backend.services.bsale_raw.core import stock_engine
from backend.services.bsale_raw.core.engine import UnsupportedSyncError
from backend.services.bsale_raw.core.models import SyncMode
from backend.services.bsale_raw.core.rate_limit import RequestPriority
from backend.services.bsale_raw.core.registry import POINT_STATE_SCOPE, point_scope, variant_scope
from backend.services.bsale_raw.core.stock_engine import (
    MAX_POINT_VARIANTS,
    refresh_stock_point,
    refresh_stock_variants,
    run_stock_sync,
)
from backend.services.bsale_raw.core.store import EntityOutcome, PgRawTx, advisory_lock_keys
from backend.tests.bsale_raw.test_bsale_raw_pipeline import (
    BASE,
    ENV,
    TOKEN,
    FakeConnection,
    FakeStore,
    TickClock,
    client_factory_for,
)
from backend.tests.bsale_raw.test_bsale_raw_stock import STOCKS, TABLE, StockBsale, go, old, stock

POINT = SyncMode.POINT


def setup(items=None, **kw):
    store = FakeStore()
    return store, StockBsale(items, store=store, **kw)


def point(store, adapter, variant=10888, office=1, *, dry_run=False, clock=None, env=None, factory=None):
    return refresh_stock_point(
        store=store, company_id=3, variant_id=variant, office_id=office, dry_run=dry_run,
        client_factory=factory or client_factory_for(adapter), clock=clock or TickClock(BASE),
        getenv=(ENV if env is None else env).get, host="test",
    )


def batch(store, adapter, variants, office=1, *, clock=None):
    return refresh_stock_variants(
        store=store, company_id=3, variant_ids=variants, office_id=office,
        client_factory=client_factory_for(adapter), clock=clock or TickClock(BASE), getenv=ENV.get, host="test",
    )


def result(out, variant=10888):
    return out.point["results"][str(variant)]


def query(request) -> dict:
    return {k: v[0] for k, v in parse_qs(urlsplit(request.url).query).items()}


# --- variant + office -------------------------------------------------------------------------


def test_variant_office_one_row_inserted_with_exact_quantities():
    store, adapter = setup([stock(10888, 1, quantity=120.0, reserved=30.0, available=90.0), stock(10888, 4)])
    out = point(store, adapter)
    assert out.status == "SUCCESS" and out.mode == "POINT" and out.scope == "variant:10888:office:1"
    assert (out.rows_received, out.rows_inserted, out.rows_updated, out.requests) == (1, 1, 0, 1)
    assert set(store.stock_rows(3)) == {(10888, 1)}
    row = store.stock_rows(3)[(10888, 1)]
    assert (row["quantity"], row["quantity_reserved"], row["quantity_available"]) == (
        Decimal("120.0"), Decimal("30.0"), Decimal("90.0"))
    assert row["last_source"] == "POINT" and row["sync_run_id"] == out.sync_run_id
    assert result(out) == {"status": "FETCHED", "offices": {"1": "inserted"}}
    assert query(adapter.calls[0]) == {"limit": query(adapter.calls[0])["limit"], "offset": "0",
                                       "variantid": "10888", "officeid": "1"}


def test_variant_office_zero_rows_no_write_no_fabricated_zero():
    store, adapter = setup([stock(10888, 4)])
    store.seed_stock(3, stock(10888, 1, quantity=7.0), fetched_at=old())
    before = copy.deepcopy(store.tables[TABLE])
    out = point(store, adapter)
    assert out.status == "SUCCESS" and out.rows_received == 0
    assert (out.rows_inserted, out.rows_updated, out.rows_deleted) == (0, 0, 0)
    assert result(out) == {"status": "NO_ROWS", "offices": {}}
    assert store.tables[TABLE] == before
    assert "delete_stale_stock" not in store.events


def test_variant_office_multiple_rows_is_ambiguous_and_fails():
    rows = [stock(10888, 1, sid=1), stock(10888, 1, sid=2)]
    store, adapter = setup(pages=[rows])
    adapter.counts = [2]
    out = point(store, adapter)
    assert out.status == "FAILED" and "ambigua" in out.error
    assert result(out)["status"] == "FAILED" and store.stock_rows(3) == {}


def test_wrong_office_in_response_fails():
    store, adapter = setup([stock(10888, 4)], ignore_office_filter=True)
    out = point(store, adapter)
    assert out.status == "FAILED" and "sucursales" in out.error and store.stock_rows(3) == {}


def test_wrong_variant_in_response_fails():
    store, adapter = setup([stock(10892, 1)], ignore_variant_filter=True)
    out = point(store, adapter)
    assert out.status == "FAILED" and "variantes" in out.error and store.stock_rows(3) == {}


def test_invalid_quantity_fails():
    store, adapter = setup([stock(10888, 1, quantity="mucho")])
    out = point(store, adapter)
    assert out.status == "FAILED" and store.stock_rows(3) == {}


# --- variant sin office -----------------------------------------------------------------------


def test_variant_all_offices_upserts_every_office_and_keeps_missing():
    store, adapter = setup([stock(10888, 1), stock(10888, 4, quantity=3.0), stock(10892, 1)])
    store.seed_stock(3, stock(10888, 7, quantity=11.0), fetched_at=old())
    out = point(store, adapter, office=None)
    assert out.status == "SUCCESS" and out.scope == "variant:10888"
    assert (out.rows_received, out.rows_inserted, out.requests) == (2, 2, 1)
    assert set(store.stock_rows(3)) == {(10888, 1), (10888, 4), (10888, 7)}
    assert store.stock_rows(3)[(10888, 7)]["payload"]["quantity"] == 11.0
    assert result(out)["offices"] == {"1": "inserted", "4": "inserted"}
    assert query(adapter.calls[0]).keys() == {"limit", "offset", "variantid"}


def test_variant_all_offices_duplicate_key_fails():
    store, adapter = setup([stock(10888, 1, sid=1), stock(10888, 1, sid=2)])
    out = point(store, adapter, office=None)
    assert out.status == "FAILED" and "duplicadas" in out.error and store.stock_rows(3) == {}


def test_variant_all_offices_zero_rows_is_no_rows():
    store, adapter = setup([])
    out = point(store, adapter, office=None)
    assert out.status == "SUCCESS" and result(out)["status"] == "NO_ROWS" and store.stock_rows(3) == {}


# --- hash / timestamps / frescura -------------------------------------------------------------


def test_unchanged_then_changed_timestamps():
    clock = TickClock(BASE)
    store, adapter = setup([stock(10888, 1)])
    first = point(store, adapter, clock=clock)
    snap = copy.deepcopy(store.stock_rows(3)[(10888, 1)])
    again = point(store, adapter, clock=clock)
    assert (again.rows_inserted, again.rows_updated, again.rows_unchanged) == (0, 0, 1)
    row = store.stock_rows(3)[(10888, 1)]
    assert row["last_changed_at"] == snap["last_changed_at"] and row["first_seen_at"] == snap["first_seen_at"]
    assert row["api_fetched_at"] > snap["api_fetched_at"] and row["last_seen_at"] > snap["last_seen_at"]

    adapter.items = [stock(10888, 1, quantity=9.0, reserved=1.0, available=8.0)]
    changed = point(store, adapter, clock=clock)
    assert (changed.rows_updated, changed.rows_unchanged) == (1, 0)
    row = store.stock_rows(3)[(10888, 1)]
    assert row["last_changed_at"] > snap["last_changed_at"] and row["first_seen_at"] == snap["first_seen_at"]
    assert result(changed)["offices"] == {"1": "updated"} and first.status == "SUCCESS"


def test_stale_point_is_rejected_by_freshness():
    store, adapter = setup([stock(10888, 1, quantity=1.0)])
    store.seed_stock(3, stock(10888, 1, quantity=50.0), fetched_at=BASE + timedelta(days=1))
    before = copy.deepcopy(store.tables[TABLE])
    out = point(store, adapter)
    assert out.status == "SUCCESS" and out.rows_skipped_newer == 1 and store.tables[TABLE] == before
    assert result(out)["offices"] == {"1": "skipped_newer"}


def test_point_beats_old_scanner_and_scanner_counts_skipped_newer():
    """El scanner leyó la variante 1 en la página 1; durante la página 2 un POINT trae dato más nuevo."""
    clock = TickClock(BASE)
    items = [stock(i) for i in range(1, 61)]
    store, scan_adapter = setup(items)
    point_adapter = StockBsale([stock(1, quantity=0.0, reserved=5.0, available=0.0)], store=store)
    seen = {}

    def targeted(n):
        if n == 2:
            seen["point"] = point(store, point_adapter, variant=1, clock=clock)

    scan_adapter.on_call = targeted
    out = go(store, scan_adapter, clock=clock)
    assert seen["point"].status == "SUCCESS" and seen["point"].rows_updated + seen["point"].rows_inserted == 1
    assert out.status == "SUCCESS" and out.rows_skipped_newer == 1 and out.rows_inserted == 59
    row = store.stock_rows(3)[(1, 1)]
    assert row["last_source"] == "POINT" and row["payload"]["quantityReserved"] == 5.0
    assert row["sync_run_id"] == seen["point"].sync_run_id


# --- no delete / no zero ----------------------------------------------------------------------


def test_point_never_deletes_nor_zeroes_other_rows():
    store, adapter = setup([stock(10888, 1)])
    for v, o in [(10888, 4), (10892, 1), (10961, 1)]:
        store.seed_stock(3, stock(v, o, quantity=5.0), fetched_at=old())
    point(store, adapter)
    point(store, adapter, office=None)
    rows = store.stock_rows(3)
    assert {k for k in rows} == {(10888, 1), (10888, 4), (10892, 1), (10961, 1)}
    assert all(rows[k]["payload"]["quantity"] == 5.0 for k in [(10888, 4), (10892, 1), (10961, 1)])
    assert "delete_stale_stock" not in store.events


# --- lote (futura OC 33) ----------------------------------------------------------------------


def test_batch_one_request_per_variant_one_run():
    variants = [10888, 10892, 10961, 10963, 13108, 16387]
    store, adapter = setup([stock(v, 1) for v in variants] + [stock(v, 4) for v in variants])
    out = batch(store, adapter, variants)
    assert out.status == "SUCCESS" and out.requests == 6 and len(adapter.calls) == 6
    assert sorted(int(query(c)["variantid"]) for c in adapter.calls) == variants
    assert all(query(c)["officeid"] == "1" for c in adapter.calls)
    assert len(store.runs) == 1 and len(store.entity_runs) == 1
    assert out.scope == "variants:6:office:1" and out.rows_inserted == 6
    assert set(store.stock_rows(3)) == {(v, 1) for v in variants}
    assert store.events.count("upsert_stock") == 1


def test_batch_partial_when_some_variants_fail():
    store, adapter = setup([stock(10888, 1), stock(10892, 1, quantity="x")])
    out = batch(store, adapter, [10888, 10892, 10961])
    assert out.status == "PARTIAL" and cli.exit_code(out) == cli.EXIT_PARTIAL
    assert result(out, 10888)["status"] == "FETCHED"
    assert result(out, 10892)["status"] == "FAILED"
    assert result(out, 10961)["status"] == "NO_ROWS"
    assert set(store.stock_rows(3)) == {(10888, 1)} and "10892" in out.error


def test_batch_dedupes_and_validates_inputs():
    store, adapter = setup([stock(10888, 1)])
    out = batch(store, adapter, [10888, 10888])
    assert out.requests == 1 and out.point["variant_ids"] == [10888]
    for bad in ([], [0], [-1], ["10888"], [True]):
        with pytest.raises(UnsupportedSyncError):
            batch(store, adapter, bad)
    with pytest.raises(UnsupportedSyncError):
        batch(store, adapter, range(1, MAX_POINT_VARIANTS + 2))
    with pytest.raises(UnsupportedSyncError):
        refresh_stock_point(store=store, company_id=3, variant_id=10888, office_id=0)
    with pytest.raises(UnsupportedSyncError):
        refresh_stock_point(store=store, company_id=3, variant_id=10888, resource="offices")


def test_run_stock_sync_rejects_point_mode():
    with pytest.raises(UnsupportedSyncError):
        run_stock_sync(store=FakeStore(), company_id=3, office_id=1, mode=POINT)


# --- runs / state / scope ---------------------------------------------------------------------


def test_run_tracking_and_single_point_state_row():
    clock = TickClock(BASE)
    store, adapter = setup([stock(10888, 1), stock(10892, 1)])
    scan = go(store, adapter, clock=clock)
    office_state = copy.deepcopy(store.sync_state[(3, "stocks", "office:1")])
    a = point(store, adapter, clock=clock)
    b = point(store, adapter, variant=10892, clock=clock)
    assert scan.status == a.status == b.status == "SUCCESS"
    assert store.sync_state[(3, "stocks", "office:1")] == office_state
    point_states = [k for k in store.sync_state if k[2] not in ("office:1",)]
    assert point_states == [(3, "stocks", POINT_STATE_SCOPE)]
    assert store.sync_state[(3, "stocks", "point")]["last_sync_run_id"] == b.sync_run_id
    run = store.runs[a.sync_run_id]
    assert run["mode"] == "POINT" and run["summary"]["point"]["variant_ids"] == [10888]
    assert run["summary"]["point"]["results"]["10888"]["offices"] == {"1": "unchanged"}
    scopes = sorted(r["scope"] for r in store.entity_runs.values())
    assert scopes == ["office:1", "variant:10888:office:1", "variant:10892:office:1"]


def test_scope_helpers():
    assert variant_scope(10888) == "variant:10888"
    assert variant_scope(10888, 1) == "variant:10888:office:1"
    assert point_scope([10888], 1) == "variant:10888:office:1"
    assert point_scope([1, 2, 3]) == "variants:3"
    assert point_scope([1, 2], 4) == "variants:2:office:4"


def test_dry_run_writes_nothing_and_predicts():
    store, adapter = setup([stock(10888, 1, quantity=2.0), stock(10888, 4)])
    store.seed_stock(3, stock(10888, 1), fetched_at=old())
    before = copy.deepcopy(store.tables[TABLE])
    out = point(store, adapter, office=None, dry_run=True)
    assert out.status == "SUCCESS" and out.dry_run and out.sync_run_id is None
    assert (out.rows_inserted, out.rows_updated) == (1, 1)
    assert result(out)["offices"] == {"1": "updated", "4": "inserted"}
    assert store.tables[TABLE] == before and store.runs == {} and store.sync_state == {}
    assert "tx_begin" not in store.events


# --- locks / prioridad / limitador ------------------------------------------------------------


def test_point_takes_no_lock_and_runs_while_scanner_holds_office_lock():
    store, adapter = setup([stock(10888, 1)])
    store.held.add(advisory_lock_keys(3, "stocks", "office:1"))
    out = point(store, adapter)
    assert out.status == "SUCCESS" and "lock" not in store.events


def test_point_uses_p0_on_same_limiter_and_client_factory():
    store, adapter = setup([stock(10888, 1)])
    base = client_factory_for(adapter)
    seen = {}

    def factory(source, token, spec):
        client = base(source, token, spec)
        seen["spec_priority"] = spec.request_priority
        seen["session_priority"] = client.session.priority
        seen["spec_name"] = spec.name
        return client

    out = point(store, adapter, factory=factory)
    assert out.status == "SUCCESS"
    assert seen == {"spec_priority": RequestPriority.P0_TARGETED, "session_priority": RequestPriority.P0_TARGETED,
                    "spec_name": "stocks"}
    assert STOCKS.request_priority is RequestPriority.P2_STOCK  # el spec registrado no se muta
    assert stock_engine.default_client_factory.__module__.endswith("engine")
    assert RequestPriority.P0_TARGETED < RequestPriority.P2_STOCK < RequestPriority.P4_CATALOG


# --- red / BD / secretos ----------------------------------------------------------------------


def test_429_retried_and_counted():
    store, adapter = setup([stock(10888, 1)], script=[(429, b"{}", {"Retry-After": "0"})])
    out = point(store, adapter)
    assert out.status == "SUCCESS" and out.http_429 == 1 and out.requests == 2


def test_timeout_exhausted_fails_without_writes():
    store, adapter = setup([stock(10888, 1)], script=[requests.Timeout("lento")] * 3)
    out = point(store, adapter)
    assert out.status == "FAILED" and store.stock_rows(3) == {} and out.requests == 3


def test_malformed_response_fails():
    store, adapter = setup([stock(10888, 1)], script=[(200, b"<html>no json</html>", {})])
    out = point(store, adapter)
    assert out.status == "FAILED" and "JSON" in out.error and store.stock_rows(3) == {}
    store, adapter = setup(script=[(200, json.dumps([1, 2]).encode(), {})])
    assert point(store, adapter).status == "FAILED"


def test_rollback_on_write_failure():
    store, adapter = setup([stock(10888, 1, quantity=1.0)])
    store.seed_stock(3, stock(10888, 1), fetched_at=old())
    before = copy.deepcopy(store.tables[TABLE])
    store.fail_on = "upsert"
    out = point(store, adapter)
    assert out.status == "FAILED" and store.tables[TABLE] == before
    assert (out.rows_inserted, out.rows_updated) == (0, 0) and result(out)["offices"] == {}
    assert store.sync_state[(3, "stocks", "point")]["status"] == "FAILED"


def test_token_never_appears():
    store, adapter = setup([stock(10888, 1)], script=[(401, f'{{"error": "bad {TOKEN}"}}'.encode(), {})])
    out = point(store, adapter)
    assert out.status == "FAILED" and TOKEN not in out.error
    assert TOKEN not in repr(store.runs) + repr(store.entity_runs) + repr(store.sync_state) + repr(out.point)
    assert TOKEN not in cli.format_outcome(out)


def test_missing_token_fails_before_http():
    store, adapter = setup([stock(10888, 1)])
    out = point(store, adapter, env={})
    assert out.status == "FAILED" and adapter.calls == []


def test_no_transaction_open_during_http():
    store, adapter = setup([stock(v, 1) for v in range(1, 6)])
    out = batch(store, adapter, list(range(1, 6)))
    assert out.status == "SUCCESS" and not adapter.tx_violations
    http = [i for i, e in enumerate(store.events) if e == "http"]
    assert store.events.index("upsert_stock") > max(http)


def test_variants_select_sql_is_batched_without_for_update():
    conn = FakeConnection(next_results=[[(10888, 1, "h", BASE), (10888, 4, "h2", BASE)]])
    tx = PgRawTx(conn.cursor())
    existing = tx.read_existing_stock_variants(STOCKS, 3, [10888, 10892])
    assert set(existing) == {(10888, 1), (10888, 4)}
    (sql, params), = conn.executed
    assert "variant_id = ANY(%s)" in sql and "FOR UPDATE" not in sql and params == (3, [10888, 10892])
    assert PgRawTx(FakeConnection().cursor()).read_existing_stock_variants(STOCKS, 3, []) == {}


# --- CLI --------------------------------------------------------------------------------------


@pytest.mark.parametrize("office_args, office_id", [(["--office", "1"], 1), ([], None)])
@pytest.mark.parametrize("dry_run", [True, False])
def test_cli_point(office_args, office_id, dry_run):
    seen = {}

    def runner(**kw):
        seen.update(kw)
        return EntityOutcome(company_id=3, resource="stocks", scope=point_scope([10888], office_id), mode="POINT",
                             status="SUCCESS", dry_run=dry_run, rows_received=1, rows_updated=1, requests=1,
                             point={"office_id": office_id, "variant_ids": [10888],
                                    "results": {"10888": {"status": "FETCHED", "offices": {"1": "updated"}}}})

    argv = ["sync", "--company", "3", "--resource", "stocks", "--variant", "10888", *office_args, "--mode", "point"]
    buf = io.StringIO()
    assert cli.main(argv + (["--dry-run"] if dry_run else []), runner=runner, out=buf) == cli.EXIT_SUCCESS
    assert seen == {"company_id": 3, "resource": "stocks", "mode": POINT, "dry_run": dry_run,
                    "office_id": office_id, "variant_id": 10888}
    text = buf.getvalue()
    assert "mode=POINT" in text and "variants=1" in text and "no_rows=0" in text and "status=SUCCESS" in text


@pytest.mark.parametrize(
    "argv",
    [
        ["--resource", "stocks", "--mode", "point"],  # sin --variant
        ["--resource", "stocks", "--office", "1", "--mode", "point"],
        ["--resource", "stocks", "--variant", "0", "--mode", "point"],
        ["--resource", "stocks", "--variant", "-5", "--mode", "point"],
        ["--resource", "stocks", "--variant", "abc", "--mode", "point"],
        ["--resource", "offices", "--variant", "10888", "--mode", "point"],
        ["--resource", "products", "--variant", "10888", "--mode", "full-reconcile"],
        ["--resource", "stocks", "--variant", "10888", "--mode", "full-reconcile", "--office", "1"],
    ],
)
def test_cli_point_usage_errors(argv):
    never = lambda **kw: pytest.fail("no debe ejecutarse")  # noqa: E731
    code = cli.main(["sync", "--company", "3", *argv], runner=never, out=io.StringIO(), err=io.StringIO())
    assert code == cli.EXIT_USAGE
