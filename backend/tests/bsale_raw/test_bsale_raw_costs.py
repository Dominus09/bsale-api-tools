"""Motor de costos ``bsale_raw.variant_costs`` (scan-costs / refresh-costs). Sin red ni BD real.

HTTP: stack real (``BsaleHttpClient`` + ``RateLimitedSession``) sobre un adapter falso de
``/variants/{id}/costs.json``. BD: ``FakeCostStore`` en memoria con la semántica de la SQL de
``PgCostStore`` (frescura por fila, cursor, runs, rollback); la SQL real se valida con un cursor falso.
"""

from __future__ import annotations

import copy
import io
import json
import re
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest
import requests
from requests.adapters import BaseAdapter

from backend.jobs.bsale_raw import cli
from backend.services.bsale_raw.core import cost_engine
from backend.services.bsale_raw.core.cost_engine import (
    COST_COLUMNS,
    UPSERT_COSTS_SQL,
    CostCursor,
    CostPayloadError,
    PgCostStore,
    PgCostTx,
    build_cost_row,
    cost_rate_config,
    format_cost_outcome,
    refresh_costs,
    run_cost_scan,
)
from backend.services.bsale_raw.core.engine import UnsupportedSyncError
from backend.services.bsale_raw.core.models import payload_hash
from backend.services.bsale_raw.core.rate_limit import RequestPriority
from backend.services.bsale_raw.core.reconcile import ExistingRow
from backend.services.bsale_raw.core.store import LockBusyError, RunHandle, SourceConfig, SourceConfigError
from backend.tests.bsale_raw.test_bsale_raw_pipeline import (
    BASE,
    ENV,
    TOKEN,
    FakeConnection,
    TickClock,
    client_factory_for,
    make_response,
)

COST_RE = re.compile(r"/v1/variants/(\d+)/costs\.json$")


def cost_body(variant_id: int, average="1550.6", total=None, history=None) -> dict:
    if history is None:
        history = [
            {"reception_detail": {"id": variant_id * 10}, "admissionDate": 1696118400, "cost": 1500, "availableFifo": 2},
            {"reception_detail": {"id": variant_id * 10 + 1}, "admissionDate": 1727740800, "cost": 1601.2,
             "availableFifo": 3},
        ]
    return {"averageCost": average, "totalCost": total if total is not None else 7752, "history": history}


class CostsBsale(BaseAdapter):
    """``responses[v]``: dict (200), ``(status, body)`` o Exception. Sin entrada = ``cost_body(v)``."""

    def __init__(self, responses=None, store=None):
        super().__init__()
        self.responses = dict(responses or {})
        self.store = store
        self.calls: list[int] = []
        self.priorities: list = []
        self.tx_violations: list[str] = []

    def send(self, request, **kwargs):
        if self.store is not None and self.store.in_tx:
            self.tx_violations.append(request.url)
        match = COST_RE.search(request.url.split("?")[0])
        assert match, request.url
        assert request.headers.get("access_token") == TOKEN
        variant_id = int(match.group(1))
        self.calls.append(variant_id)
        action = self.responses.get(variant_id, cost_body(variant_id))
        if isinstance(action, BaseException):
            raise action
        status, body = action if isinstance(action, tuple) else (200, action)
        return make_response(request, status, json.dumps(body).encode())

    def close(self):
        pass


def factory_for(adapter):
    base = client_factory_for(adapter)

    def factory(source, token, spec):
        adapter.priorities.append(spec.request_priority)
        return base(source, token, spec)

    return factory


class FakeCostTx:
    def __init__(self, store):
        self.s = store

    def read_existing_costs(self, company_id, variant_ids):
        return {
            v: ExistingRow(bsale_id=v, payload_hash=r["payload_hash"], api_fetched_at=r["api_fetched_at"],
                           missing_since=None)
            for (c, v), r in self.s.costs.items() if c == company_id and v in set(variant_ids)
        }

    def upsert_costs(self, rows, *, sync_run_id, last_source):
        if self.s.fail_upsert:
            raise RuntimeError(f"fallo BD con {TOKEN}")
        applied = set()
        for r in rows:
            key = (r.company_id, r.variant_id)
            prev = self.s.costs.get(key)
            if prev is not None and prev["api_fetched_at"] > r.api_fetched_at:
                continue
            self.s.costs[key] = {
                "average_cost": r.average_cost, "total_cost": r.total_cost, "history_count": r.history_count,
                "last_admission_date": r.last_admission_date, "history_complete": False,
                "payload": copy.deepcopy(r.payload), "payload_hash": r.payload_hash,
                "api_fetched_at": r.api_fetched_at, "last_source": last_source, "sync_run_id": sync_run_id,
            }
            applied.add(r.variant_id)
        return applied

    def write_cursor(self, company_id, cursor, *, sync_run_id):
        self.s.cursor = cursor
        self.s.events.append("cursor")

    def finish_success(self, handle, outcome):
        self.s.runs[handle.run_id].update(status=outcome.status, summary=outcome.summary())
        self.s.state[(outcome.company_id, outcome.resource, outcome.sync_state_scope)] = outcome.status


class FakeCostStore:
    def __init__(self, variants=(), *, token_env="BSALE_TOKEN_SPA"):
        self.variants = {3: sorted(variants)}
        self.token_env = token_env
        self.costs: dict = {}
        self.cursor = CostCursor()
        self.runs: dict = {}
        self.state: dict = {}
        self.events: list[str] = []
        self.locks: set = set()
        self.busy: set = set()
        self.in_tx = False
        self.fail_upsert = False
        self.fail_source = None

    def resolve_source(self, company_id):
        if self.fail_source:
            raise self.fail_source
        if company_id not in self.variants:
            raise SourceConfigError(f"company_id={company_id} no existe en bsale_raw.sources")
        return SourceConfig(company_id=company_id, cpn_id=1, name="SPA", token_env=self.token_env)

    @contextmanager
    def advisory_lock(self, company_id, resource, scope):
        key = (company_id, resource, scope)
        if key in self.busy or key in self.locks:
            raise LockBusyError(f"lock ocupado company_id={company_id} resource={resource} scope={scope}")
        self.locks.add(key)
        self.events.append("lock")
        try:
            yield
        finally:
            self.locks.discard(key)
            self.events.append("unlock")

    def start_run(self, *, mode, trigger, host, company_id, resource, scope, state_scope=None):
        run_id = len(self.runs) + 1
        self.runs[run_id] = {"mode": mode, "scope": scope, "state_scope": state_scope, "status": "RUNNING"}
        self.events.append("start_run")
        return RunHandle(run_id=run_id, entity_run_id=run_id, started_at=BASE)

    def finish_failed(self, handle, outcome):
        self.runs[handle.run_id].update(status=outcome.status, error=outcome.error, summary=outcome.summary())

    def read_cursor(self, company_id):
        return self.cursor

    def select_variants(self, company_id, after_variant_id, limit):
        return [v for v in self.variants.get(company_id, []) if v > after_variant_id][:limit]

    def read_existing_costs(self, company_id, variant_ids):
        return FakeCostTx(self).read_existing_costs(company_id, variant_ids)

    @contextmanager
    def cost_transaction(self):
        snapshot = (copy.deepcopy(self.costs), self.cursor, copy.deepcopy(self.runs), dict(self.state))
        self.in_tx = True
        self.events.append("tx_begin")
        try:
            yield FakeCostTx(self)
        except BaseException:
            self.costs, self.cursor, self.runs, self.state = snapshot
            raise
        finally:
            self.in_tx = False


def scan(store, adapter, **kw):
    kw.setdefault("clock", TickClock(BASE))
    return run_cost_scan(
        store=store, company_id=kw.pop("company_id", 3), client_factory=factory_for(adapter),
        getenv=kw.pop("env", ENV).get, host="test", **kw,
    )


def point(store, adapter, variant_ids, **kw):
    kw.setdefault("clock", TickClock(BASE))
    return refresh_costs(
        store=store, company_id=3, variant_ids=variant_ids, client_factory=factory_for(adapter),
        getenv=kw.pop("env", ENV).get, host="test", **kw,
    )


def world(variants, responses=None):
    store = FakeCostStore(variants)
    return store, CostsBsale(responses, store=store)


# --- filas: valores originales de Bsale ------------------------------------------------------------


def test_row_keeps_original_net_values_and_full_payload():
    body = cost_body(29567, average="218478.99", total="436957.98")
    row = build_cost_row(3, 29567, copy.deepcopy(body), BASE)
    assert row.average_cost == Decimal("218478.99") and row.total_cost == Decimal("436957.98")
    assert row.payload == body and row.payload_hash == payload_hash(body)
    assert row.history_count == 2 and row.last_admission_date == date(2024, 10, 1)


def test_row_float_and_int_values_keep_decimal_text():
    row = build_cost_row(3, 1, {"averageCost": 1550.6, "totalCost": 0, "history": []}, BASE)
    assert row.average_cost == Decimal("1550.6") and str(row.average_cost) == "1550.6"
    assert row.total_cost == Decimal("0") and row.history_count == 0 and row.last_admission_date is None


def test_row_zero_null_and_missing_history():
    zero = build_cost_row(3, 1, {"averageCost": 0}, BASE)
    assert zero.average_cost == Decimal("0") and zero.history_count is None and zero.total_cost is None
    assert build_cost_row(3, 1, {"averageCost": None, "history": []}, BASE).average_cost is None


def test_row_has_no_derived_tax_fields():
    fields = set(cost_engine.CostRow.__dataclass_fields__)
    assert fields == {"company_id", "variant_id", "average_cost", "total_cost", "history_count",
                      "last_admission_date", "payload", "payload_hash", "api_fetched_at"}
    assert not any("gross" in c or "tax" in c or "iva" in c for c in COST_COLUMNS)


@pytest.mark.parametrize("body, needle", [
    ({"totalCost": 1, "history": []}, "sin averageCost"),
    ({"averageCost": "abc"}, "averageCost/totalCost"),
    ({"averageCost": True}, "averageCost/totalCost"),
    ({"averageCost": "1", "history": {"a": 1}}, "history no es lista"),
    ({"averageCost": "1", "history": [1]}, "history no es lista"),
    ({"averageCost": "1", "history": [{"admissionDate": "ayer"}]}, "admissionDate"),
])
def test_row_rejects_unexpected_shapes(body, needle):
    with pytest.raises(CostPayloadError, match=needle):
        build_cost_row(3, 1, body, BASE)


# --- SCANNER ---------------------------------------------------------------------------------------


def test_scan_first_batch_writes_rows_cursor_and_run():
    store, adapter = world([10, 11, 12, 13, 14])
    out = scan(store, adapter, batch_size=3)
    assert out.status == "SUCCESS" and out.mode == "SCANNER" and out.scope == "variant_range:10-12"
    assert adapter.calls == [10, 11, 12]
    assert sorted(v for _, v in store.costs) == [10, 11, 12]
    assert out.rows_inserted == 3 and out.rows_received == 3 and out.requests == 3
    assert store.cursor.last_variant_id == 12 and store.cursor.lap == 1
    assert out.point["lap_completed"] is False
    row = store.costs[(3, 10)]
    assert row["last_source"] == "SCANNER" and row["history_complete"] is False
    assert row["average_cost"] == Decimal("1550.6") and row["payload"] == cost_body(10)
    assert store.runs[1]["state_scope"] == "scanner" and store.state[(3, "variant_costs", "scanner")] == "SUCCESS"
    assert adapter.priorities == [RequestPriority.P5_COSTS]
    assert adapter.tx_violations == [] and store.locks == set()


def test_scan_progresses_completes_lap_and_wraps():
    store, adapter = world([10, 11, 12, 13, 14])
    scan(store, adapter, batch_size=3)
    second = scan(store, adapter, batch_size=3)
    assert second.scope == "variant_range:13-14" and second.point["lap_completed"] is True
    assert store.cursor.last_completed_lap == 1 and store.cursor.last_variant_id == 14
    third = scan(store, adapter, batch_size=3)
    assert third.point["wrapped"] is True and third.point["lap"] == 2 and third.scope == "variant_range:10-12"
    assert store.cursor.lap == 2 and store.cursor.last_completed_lap == 1
    assert adapter.calls == [10, 11, 12, 13, 14, 10, 11, 12]


def test_scan_uses_variants_dynamically_including_new_ones():
    store, adapter = world([10, 11])
    scan(store, adapter, batch_size=5)
    store.variants[3] = [10, 11, 20]
    out = scan(store, adapter, batch_size=5)
    assert out.point["wrapped"] is False and adapter.calls[-1:] == [20]
    out = scan(store, adapter, batch_size=5)
    assert out.point["wrapped"] is True and adapter.calls[-3:] == [10, 11, 20]


def test_scan_never_deletes_or_touches_rows_outside_batch():
    store, adapter = world([10, 11])
    store.costs[(3, 99)] = {"payload_hash": "x", "api_fetched_at": BASE - timedelta(days=9), "average_cost": 5}
    scan(store, adapter, batch_size=5)
    assert store.costs[(3, 99)]["average_cost"] == 5 and {v for _, v in store.costs} == {10, 11, 99}
    src = Path(cost_engine.__file__).read_text(encoding="utf-8").upper()
    assert "DELETE FROM" not in src and "TRUNCATE" not in src


def test_scan_rerun_same_lap_is_unchanged_and_stale_guard_protects_newer_rows():
    store, adapter = world([10, 11])
    scan(store, adapter, batch_size=5)
    store.cursor = CostCursor()
    store.costs[(3, 11)]["api_fetched_at"] = BASE + timedelta(days=1)
    out = scan(store, adapter, batch_size=5, clock=TickClock(BASE + timedelta(minutes=5)))
    assert out.rows_unchanged == 1 and out.rows_skipped_newer == 1 and out.rows_updated == 0
    assert store.costs[(3, 11)]["sync_run_id"] == 1


def test_scan_changed_cost_updates_row():
    store, adapter = world([10])
    scan(store, adapter)
    store.cursor = CostCursor()
    adapter.responses[10] = cost_body(10, average="1601.2")
    out = scan(store, adapter, clock=TickClock(BASE + timedelta(hours=1)))
    assert out.rows_updated == 1 and store.costs[(3, 10)]["average_cost"] == Decimal("1601.2")


def test_scan_isolates_failed_variant_partial_and_cursor_advances():
    store, adapter = world([10, 11, 12], responses={11: (404, {"error": "not found"}), 12: {"history": []}})
    out = scan(store, adapter)
    assert out.status == "PARTIAL" and cli.exit_code(out) == cli.EXIT_PARTIAL
    assert set(store.costs) == {(3, 10)}
    assert out.point["failed"] == 2 and set(out.point["failed_sample"]) == {"11", "12"}
    assert "404" in out.point["failed_sample"]["11"] and "sin averageCost" in out.point["failed_sample"]["12"]
    assert store.cursor.last_variant_id == 12 and store.cursor.retry == {11: 1, 12: 1}
    assert out.point["retry_pending"] == 2 and store.runs[1]["summary"]["point"]["cursor"]["retry"] == {"11": 1, "12": 1}


def test_scan_transport_outage_fails_without_writes_or_cursor():
    store, adapter = world([10, 11], responses={10: (500, {}), 11: (500, {})})
    out = scan(store, adapter)
    assert out.status == "FAILED" and cli.exit_code(out) == cli.EXIT_FAILED
    assert store.costs == {} and store.cursor == CostCursor() and store.runs[1]["status"] == "FAILED"
    assert "ninguna variante con costo válido" in out.error and out.http_5xx > 0


def test_scan_all_404_advances_cursor_and_queues_retries_but_reports_failed():
    store, adapter = world([10, 11, 12], responses={10: (404, {}), 11: (404, {})})
    out = scan(store, adapter, batch_size=2)
    assert out.status == "FAILED" and store.costs == {} and store.runs[1]["status"] == "FAILED"
    assert store.cursor.last_variant_id == 11 and store.cursor.retry == {10: 1, 11: 1}


def test_end_of_lap_batch_of_deleted_variant_does_not_block_the_scanner():
    store, adapter = world([10, 11], responses={11: (404, {})})
    scan(store, adapter, batch_size=1)
    out = scan(store, adapter, batch_size=1)
    assert out.status == "FAILED" and store.cursor.last_variant_id == 11
    third = scan(store, adapter, batch_size=1)
    assert third.point["wrapped"] is True and third.point["lap"] == 2


def test_scan_stops_after_consecutive_api_failures_and_keeps_cursor_at_last_attempt():
    ids = list(range(1, 31))
    responses = {v: (401, {"error": "unauthorized"}) for v in ids if v > 2}
    store, adapter = world(ids, responses)
    out = scan(store, adapter, batch_size=30)
    assert out.status == "PARTIAL" and out.point["not_attempted"] == 30 - 2 - cost_engine.MAX_CONSECUTIVE_FAILURES
    assert len(adapter.calls) == 2 + cost_engine.MAX_CONSECUTIVE_FAILURES
    assert store.cursor.last_variant_id == adapter.calls[-1] and out.point["lap_completed"] is False
    assert sorted(store.cursor.retry) == list(range(3, 3 + cost_engine.MAX_CONSECUTIVE_FAILURES))


def test_consecutive_404_do_not_abort_the_batch():
    ids = list(range(1, 26))
    store, adapter = world(ids, {v: (404, {}) for v in ids if v < 20})
    out = scan(store, adapter, batch_size=25)
    assert out.point["not_attempted"] == 0 and len(adapter.calls) == 25 and out.status == "PARTIAL"
    assert store.cursor.last_variant_id == 25


def test_failed_variant_is_retried_first_in_the_next_run_and_recovered():
    store, adapter = world([10, 11, 12, 13], responses={11: (500, {})})
    scan(store, adapter, batch_size=2)
    assert store.cursor.retry == {11: 1} and store.cursor.last_variant_id == 11
    del adapter.responses[11]
    out = scan(store, adapter, batch_size=3, clock=TickClock(BASE + timedelta(minutes=15)))
    assert out.point["retried"] == 1 and adapter.calls[-3:] == [11, 12, 13]
    assert (3, 11) in store.costs and store.cursor.retry == {} and store.cursor.last_variant_id == 13


def test_retry_queue_gives_up_after_max_attempts_and_lap_picks_it_up_again():
    store, adapter = world([10, 11], responses={10: (404, {})})
    for _ in range(cost_engine.MAX_RETRY_ATTEMPTS):
        out = scan(store, adapter, batch_size=5)
    assert 10 not in store.cursor.retry and out.point["retry_exhausted"] == [10]
    adapter.responses.pop(10)
    scan(store, adapter, batch_size=5)
    assert (3, 10) in store.costs


def test_retries_never_starve_the_regular_scan():
    store, adapter = world(list(range(1, 400)))
    store.cursor = CostCursor(last_variant_id=200, retry={v: 1 for v in range(1, 150)})
    out = scan(store, adapter, batch_size=50)
    assert out.point["retried"] == 49 and out.point["first_variant_id"] == 201
    assert store.cursor.last_variant_id == 201 and len(store.cursor.retry) == 149 - 49


def test_retry_queue_is_bounded():
    queue, dropped = cost_engine.next_retry(
        {v: 1 for v in range(1, cost_engine.MAX_RETRY_TRACKED + 11)}, cost_engine.CostFetch()
    )
    assert len(queue) == cost_engine.MAX_RETRY_TRACKED and len(dropped) == 10


def test_interrupted_process_writes_nothing_and_releases_lock():
    store, adapter = world([10, 11], responses={11: KeyboardInterrupt()})
    with pytest.raises(KeyboardInterrupt):
        scan(store, adapter)
    assert store.costs == {} and store.cursor == CostCursor() and store.locks == set()
    assert "unlock" in store.events and "tx_begin" not in store.events


def test_429_is_retried_with_retry_after_and_counted():
    store, adapter = world([10])
    calls = iter([(429, {"error": "rate"}), (429, {"error": "rate"})])

    def flaky(request, original=adapter.send):
        nxt = next(calls, None)
        if nxt is not None:
            adapter.calls.append(-1)
            return make_response(request, nxt[0], json.dumps(nxt[1]).encode(), {"Retry-After": "0"})
        return original(request)

    adapter.send = lambda request, **kw: flaky(request)
    out = scan(store, adapter)
    assert out.status == "SUCCESS" and out.http_429 == 2 and out.requests == 3 and (3, 10) in store.costs


def test_persistent_429_counts_as_api_failure_not_as_variant_answer():
    store, adapter = world([10], responses={10: (429, {"error": "rate"})})
    out = scan(store, adapter)
    assert out.status == "FAILED" and store.cursor == CostCursor() and out.http_429 >= 1


def test_scan_dry_run_reads_only():
    store, adapter = world([10, 11])
    store.costs[(3, 10)] = {"payload_hash": payload_hash(cost_body(10)), "api_fetched_at": BASE - timedelta(days=1)}
    before = copy.deepcopy(store.costs)
    out = scan(store, adapter, dry_run=True)
    assert out.status == "SUCCESS" and out.dry_run and out.sync_run_id is None
    assert out.rows_unchanged == 1 and out.rows_inserted == 1
    assert store.costs == before and store.cursor == CostCursor() and store.runs == {}
    assert "lock" not in store.events and "tx_begin" not in store.events
    assert format_cost_outcome(out).startswith("dry_run=true")


def test_scan_overlapping_run_is_skipped():
    store, adapter = world([10])
    store.busy.add((3, "variant_costs", "global"))
    out = scan(store, adapter)
    assert out.status == "SKIPPED" and cli.exit_code(out) == cli.EXIT_LOCKED
    assert "ya en ejecución" in out.error and adapter.calls == [] and store.runs == {}


def test_scan_empty_universe_fails_without_http():
    store, adapter = world([])
    out = scan(store, adapter)
    assert out.status == "FAILED" and "sin variantes vigentes" in out.error and adapter.calls == []


def test_scan_missing_token_fails_without_http_or_run():
    store, adapter = world([10])
    out = scan(store, adapter, env={})
    assert out.status == "FAILED" and "BSALE_TOKEN_SPA" in out.error
    assert adapter.calls == [] and store.runs == {} and "lock" not in store.events


def test_scan_db_failure_rolls_back_and_redacts_token():
    store, adapter = world([10])
    store.fail_upsert = True
    out = scan(store, adapter)
    assert out.status == "FAILED" and TOKEN not in out.error and "***" in out.error
    assert store.costs == {} and store.cursor == CostCursor() and out.rows_inserted == 0
    assert TOKEN not in json.dumps(store.runs, default=str)


def test_scan_rejects_invalid_batch():
    store, adapter = world([10])
    for bad in (0, -1, cost_engine.MAX_BATCH + 1, True, "10"):
        with pytest.raises(UnsupportedSyncError):
            scan(store, adapter, batch_size=bad)


# --- POINT ------------------------------------------------------------------------------------------


def test_point_refresh_writes_without_lock_or_cursor_at_p0():
    store, adapter = world([10, 11])
    out = point(store, adapter, [11, 10, 11])
    assert out.status == "SUCCESS" and out.mode == "POINT" and out.scope == "variants:2"
    assert adapter.calls == [10, 11] and adapter.priorities == [RequestPriority.P0_TARGETED]
    assert store.costs[(3, 10)]["last_source"] == "POINT" and store.cursor == CostCursor()
    assert "lock" not in store.events and store.runs[1]["state_scope"] == "point"


def test_point_refresh_while_scanner_lock_is_held():
    store, adapter = world([10])
    store.busy.add((3, "variant_costs", "global"))
    assert point(store, adapter, [10]).status == "SUCCESS"


def test_newer_point_written_during_scanner_fetch_is_not_overwritten():
    store, adapter = world([10, 11])
    point_adapter = CostsBsale({11: cost_body(11, average="999.5")}, store=store)
    original = adapter.send

    def send(request, **kw):
        response = original(request, **kw)
        if request.url.endswith("/variants/11/costs.json"):
            res = point(store, point_adapter, [11], clock=TickClock(BASE + timedelta(hours=1)))
            assert res.status == "SUCCESS"
        return response

    adapter.send = send
    out = scan(store, adapter)
    assert out.status == "SUCCESS" and out.rows_skipped_newer == 1 and out.rows_inserted == 1
    assert store.costs[(3, 11)]["average_cost"] == Decimal("999.5") and store.costs[(3, 11)]["last_source"] == "POINT"
    assert store.cursor.last_variant_id == 11 and adapter.tx_violations == [] and point_adapter.tx_violations == []


def test_second_scanner_cannot_move_cursor_while_first_holds_lock():
    store, adapter = world([10, 11, 12])
    original = adapter.send
    inner: list = []

    def send(request, **kw):
        if not inner:
            inner.append(scan(store, CostsBsale(store=store)))
        return original(request, **kw)

    adapter.send = send
    out = scan(store, adapter, batch_size=2)
    assert inner[0].status == "SKIPPED" and inner[0].sync_run_id is None
    assert out.status == "SUCCESS" and store.cursor.last_variant_id == 11 and len(store.runs) == 1


def test_lock_released_after_db_failure_and_next_run_proceeds():
    store, adapter = world([10])
    store.fail_upsert = True
    assert scan(store, adapter).status == "FAILED" and store.locks == set()
    store.fail_upsert = False
    assert scan(store, adapter).status == "SUCCESS" and store.cursor.last_variant_id == 10


def test_raw_semantics_zero_null_history_and_decimals_preserved():
    hist = [{"reception_detail": {"id": 5}, "admissionDate": "1727740800", "cost": 1601.27, "availableFifo": 0}]
    store, adapter = world([10, 11, 12], responses={
        10: {"averageCost": 0, "totalCost": 0, "history": []},
        11: {"averageCost": None, "totalCost": None},
        12: {"averageCost": "3770.0701", "totalCost": "15080.2804", "history": hist, "extra": {"x": 1}},
    })
    scan(store, adapter)
    zero, null, exact = store.costs[(3, 10)], store.costs[(3, 11)], store.costs[(3, 12)]
    assert zero["average_cost"] == Decimal("0") and zero["average_cost"] is not None and zero["history_count"] == 0
    assert null["average_cost"] is None and null["total_cost"] is None and null["history_count"] is None
    assert str(exact["average_cost"]) == "3770.0701" and str(exact["total_cost"]) == "15080.2804"
    assert exact["payload"] == {"averageCost": "3770.0701", "totalCost": "15080.2804", "history": hist, "extra": {"x": 1}}
    assert exact["history_complete"] is False and exact["last_admission_date"] == date(2024, 10, 1)


def test_point_refresh_validation():
    store, adapter = world([10])
    for bad in ([], [0], [-3], [True]):
        with pytest.raises(UnsupportedSyncError):
            point(store, adapter, bad)
    with pytest.raises(UnsupportedSyncError):
        point(store, adapter, range(1, cost_engine.MAX_POINT_VARIANTS + 2))


# --- ritmo propio -----------------------------------------------------------------------------------


def test_cost_rate_config_default_env_and_ceiling():
    assert cost_rate_config({}.get).requests_per_second == 2.0
    assert cost_rate_config({"BSALE_RAW_COST_RPS": "1.5"}.get).requests_per_second == 1.5
    for bad in ("0", "-1", "6"):
        with pytest.raises(ValueError):
            cost_rate_config({"BSALE_RAW_COST_RPS": bad}.get)


# --- CLI ---------------------------------------------------------------------------------------------


def cli_run(argv, outcome=None):
    seen = {}

    def runner(**kw):
        seen.update(kw)
        return outcome

    out, err = io.StringIO(), io.StringIO()
    code = cli.main(argv, cost_runner=runner, out=out, err=err)
    return code, seen, out.getvalue(), err.getvalue()


def test_cli_scan_costs_dispatch_and_exit_codes():
    store, adapter = world([10, 11], responses={11: (404, {})})
    outcome = scan(store, adapter)
    code, seen, out, _ = cli_run(["scan-costs", "--company", "3", "--batch", "50", "--dry-run"], outcome)
    assert code == cli.EXIT_PARTIAL
    assert seen == {"command": "scan-costs", "company_id": 3, "dry_run": True, "batch": 50, "variant_ids": None}
    assert "status=PARTIAL" in out and "failed=1" in out and TOKEN not in out and "averageCost" not in out


def test_cli_refresh_costs_dispatch():
    store, adapter = world([10])
    code, seen, out, _ = cli_run(["refresh-costs", "--company", "3", "--variant", "10", "--variant", "12"],
                                 point(store, adapter, [10]))
    assert code == cli.EXIT_SUCCESS and seen["variant_ids"] == [10, 12] and seen["command"] == "refresh-costs"


@pytest.mark.parametrize("argv", [
    ["scan-costs", "--company", "0"],
    ["scan-costs", "--company", "3", "--batch", "0"],
    ["scan-costs", "--company", "3", "--batch", str(cost_engine.MAX_BATCH + 1)],
    ["refresh-costs", "--company", "3"],
    ["refresh-costs", "--company", "3", "--variant", "0"],
    ["scan-costs"],
])
def test_cli_cost_usage_errors(argv):
    code, seen, _, _ = cli_run(argv)
    assert code == cli.EXIT_USAGE and seen == {}


def test_cli_generic_sync_still_rejects_variant_costs():
    never = lambda **kw: pytest.fail("no debe ejecutarse")  # noqa: E731
    argv = ["sync", "--company", "3", "--resource", "variant_costs", "--mode", "scanner"]
    assert cli.main(argv, runner=never, out=io.StringIO(), err=io.StringIO()) == cli.EXIT_USAGE


# --- SQL real (cursor falso) -------------------------------------------------------------------------


def test_upsert_sql_freshness_and_columns():
    sql = UPSERT_COSTS_SQL
    assert sql.startswith("INSERT INTO bsale_raw.variant_costs AS t")
    assert "ON CONFLICT (company_id, variant_id) DO UPDATE SET" in sql
    assert "WHERE t.api_fetched_at <= EXCLUDED.api_fetched_at" in sql and sql.endswith("RETURNING variant_id")
    assert "DELETE" not in sql.upper() and "GROSS" not in sql.upper() and "TAX" not in sql.upper()
    assert cost_engine.COST_TEMPLATE.count("%s") == len(COST_COLUMNS) - 4  # history_complete + 3 now()


def test_pg_upsert_through_execute_values():
    conn = FakeConnection(next_results=[[(10,)]])
    row = build_cost_row(3, 10, cost_body(10), BASE)
    applied = PgCostTx(conn.cursor()).upsert_costs([row], sync_run_id=7, last_source="SCANNER")
    assert applied == {10}
    sql = conn.executed[-1][0]
    assert "INSERT INTO bsale_raw.variant_costs AS t" in sql and "'SCANNER', 7" in sql and "false" in sql


def test_pg_cursor_read_write_and_variant_batch_sql():
    conn = FakeConnection(next_results=[
        [({"last_variant_id": 12, "lap": 2, "last_completed_lap": 1, "last_completed_lap_at": "x"}, BASE)],
        [(13,), (14,)],
    ])
    store = PgCostStore(lambda: conn, read_only=True)
    cursor = store.read_cursor(3)
    assert cursor == CostCursor(12, 2, BASE, 1, "x")
    assert store.select_variants(3, 12, 2) == [13, 14]
    sql, params = conn.executed[-1]
    assert "FROM bsale_raw.variants" in sql and "missing_since IS NULL" in sql and params == (3, 12, 2)
    assert conn.session.get("readonly") is True
    PgCostTx(conn.cursor()).write_cursor(3, cursor, sync_run_id=9)
    sql, params = conn.executed[-1]
    assert "INSERT INTO bsale_raw.sync_cursors" in sql and params[:4] == (3, "variant_costs", "global", "scanner")


def test_pg_cursor_defaults_when_absent_or_malformed():
    assert PgCostStore(lambda: FakeConnection(next_results=[[]]), read_only=True).read_cursor(3) == CostCursor()
    assert CostCursor.from_value("basura", None) == CostCursor()
    assert CostCursor.from_value({"last_variant_id": -5, "lap": 0}, None) == CostCursor()


def test_dry_run_store_cannot_write():
    store = PgCostStore(lambda: FakeConnection(), read_only=True)
    with pytest.raises(RuntimeError, match="sólo lectura"):
        with store.cost_transaction():
            pass
