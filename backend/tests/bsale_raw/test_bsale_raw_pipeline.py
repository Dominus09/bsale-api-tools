"""Tests del motor ``bsale_raw`` fase 4A (offices). Sin red y sin BD real.

HTTP: stack real (``BsaleHttpClient`` + ``RateLimitedSession`` + limitador) sobre un adapter falso
de ``requests``. BD: ``FakeStore`` en memoria con la misma semántica que la SQL de ``PgRawStore``
(frescura, missing_since, rollback); la SQL real se valida aparte con un cursor falso.
"""

from __future__ import annotations

import copy
import io
import itertools
import json
import logging
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest
import requests
from requests.adapters import BaseAdapter
from requests.structures import CaseInsensitiveDict

import backend.services.bsale_raw.resources  # noqa: F401
from backend.jobs.bsale_raw import cli
from backend.services.bsale.http_client import BsaleHttpClient
from backend.services.bsale_raw.core.engine import UnsupportedSyncError, run_entity_sync
from backend.services.bsale_raw.core.models import SyncMode, payload_hash
from backend.services.bsale_raw.core.rate_limit import (
    PriorityRateLimiter,
    RateLimitConfig,
    RateLimitedSession,
    TokenBucket,
)
from backend.services.bsale_raw.core.reconcile import ExistingRow, max_missing_pct
from backend.services.bsale_raw.core.registry import REGISTRY
from backend.services.bsale_raw.core.snapshot import (
    SnapshotValidationError,
    build_rows,
    fetch_snapshot,
)
from backend.services.bsale_raw.core.store import (
    ADVISORY_LOCK_NAMESPACE,
    EntityOutcome,
    LockBusyError,
    PgRawStore,
    PgRawTx,
    RunHandle,
    SourceConfig,
    SourceConfigError,
    advisory_lock_keys,
    build_entity_upsert,
    build_mark_missing,
)

OFFICES = REGISTRY.get("offices")
TABLE = OFFICES.raw_table
TOKEN = "tok-SECRET-9f8e7d6c"
BASE = datetime(2026, 10, 2, 12, 0, tzinfo=timezone.utc)
ENV = {"BSALE_TOKEN_SPA": TOKEN, "BSALE_TOKEN_Mini": "tok-mini", "BSALE_TOKEN_Romero": "tok-romero"}


def office(i: int, **over) -> dict:
    item = {
        "href": f"https://api.bsale.io/v1/offices/{i}.json",
        "id": i,
        "name": f"Sucursal {i}",
        "description": "",
        "address": "Calle Interna 123",
        "latitude": "",
        "longitude": "",
        "isVirtual": 0,
        "country": "Chile",
        "municipality": "Quillota",
        "city": "Quillota",
        "zipCode": "",
        "costCenter": "",
        "state": 0,
        "defaultPriceList": 1,
    }
    item.update(over)
    return item


@dataclass
class TickClock:
    current: datetime
    step: timedelta = timedelta(seconds=1)

    def __call__(self) -> datetime:
        self.current += self.step
        return self.current


# --- fake BD ----------------------------------------------------------------------------------


class FakeTx:
    def __init__(self, store: "FakeStore") -> None:
        self.s = store

    def lock_existing(self, spec, company_id):
        self.s.events.append("lock_existing")
        return self.s._existing(spec, company_id)

    def upsert(self, spec, rows, *, sync_run_id, last_source):
        table = self.s.tables[spec.raw_table]
        now = self.s.now()
        applied = set()
        for r in rows:
            key = (r.company_id, r.bsale_id)
            prev = table.get(key)
            if prev is not None and prev["api_fetched_at"] > r.api_fetched_at:
                continue  # WHERE t.api_fetched_at <= EXCLUDED.api_fetched_at
            new = {
                **r.typed,
                "payload": r.payload,
                "payload_hash": r.payload_hash,
                "last_seen_at": now,
                "api_fetched_at": r.api_fetched_at,
                "missing_since": None,
                "last_source": last_source,
                "sync_run_id": sync_run_id,
            }
            if prev is None:
                new.update(first_seen_at=now, last_changed_at=now)
            else:
                new.update(
                    first_seen_at=prev["first_seen_at"],
                    last_changed_at=now if prev["payload_hash"] != r.payload_hash else prev["last_changed_at"],
                )
            table[key] = new
            applied.add(r.bsale_id)
        if self.s.fail_on == "upsert":
            raise RuntimeError("fallo inyectado en upsert")
        return applied

    def mark_missing(self, spec, company_id, bsale_ids, snapshot_started_at):
        table = self.s.tables[spec.raw_table]
        n = 0
        for bid in bsale_ids:
            row = table.get((company_id, bid))
            if row and row["missing_since"] is None and row["api_fetched_at"] <= snapshot_started_at:
                row["missing_since"] = self.s.now()
                n += 1
        if self.s.fail_on == "mark_missing":
            raise RuntimeError("fallo inyectado en mark_missing")
        return n

    def read_existing_stock(self, spec, company_id, office_id):
        self.s.events.append("read_existing_stock")
        return self.s._existing_stock(spec, company_id, office_id)

    def read_existing_stock_variants(self, spec, company_id, variant_ids):
        self.s.events.append("read_existing_stock_variants")
        return self.s._existing_stock_variants(spec, company_id, variant_ids)

    def upsert_stock(self, spec, rows, *, sync_run_id, last_source):
        self.s.events.append("upsert_stock")
        table = self.s.tables[spec.raw_table]
        now = self.s.now()
        applied = set()
        for r in rows:
            key = (r.company_id, r.variant_id, r.office_id)
            prev = table.get(key)
            if prev is not None and prev["api_fetched_at"] > r.api_fetched_at:
                continue  # WHERE t.api_fetched_at <= EXCLUDED.api_fetched_at
            new = {
                **r.typed,
                "payload": r.payload,
                "payload_hash": r.payload_hash,
                "last_seen_at": now,
                "api_fetched_at": r.api_fetched_at,
                "last_source": last_source,
                "sync_run_id": sync_run_id,
            }
            if prev is None:
                new.update(first_seen_at=now, last_changed_at=now)
            else:
                new.update(
                    first_seen_at=prev["first_seen_at"],
                    last_changed_at=now if prev["payload_hash"] != r.payload_hash else prev["last_changed_at"],
                )
            table[key] = new
            applied.add((r.variant_id, r.office_id))
        if self.s.fail_on == "upsert":
            raise RuntimeError("fallo inyectado en upsert")
        return applied

    def delete_stale_stock(self, spec, company_id, office_id, variant_ids, snapshot_started_at):
        self.s.events.append("delete_stale_stock")
        table = self.s.tables[spec.raw_table]
        n = 0
        for vid in variant_ids:
            key = (company_id, vid, office_id)
            row = table.get(key)
            if row is not None and row["api_fetched_at"] <= snapshot_started_at:
                del table[key]
                n += 1
        return n

    def finish_success(self, handle, outcome):
        self.s._close(handle, outcome)
        st = self.s.sync_state[(outcome.company_id, outcome.resource, outcome.sync_state_scope)]
        now = self.s.now()
        st.update(
            last_attempt_at=handle.started_at,
            last_success_at=now,
            rows_received=outcome.rows_received,
            duration_ms=outcome.duration_ms,
            status=outcome.status,
            last_sync_run_id=handle.run_id,
        )
        if outcome.mode == "FULL_RECONCILE":
            st["last_full_reconcile_at"] = now


class FakeStore:
    tx_class = FakeTx

    def __init__(self, db_clock: TickClock | None = None) -> None:
        self.db_clock = db_clock or TickClock(BASE)
        self.sources = {
            1: SourceConfig(1, 96674, "Mini", "BSALE_TOKEN_Mini"),
            2: SourceConfig(2, 5807, "Romero", "BSALE_TOKEN_Romero"),
            3: SourceConfig(3, 21884, "La Quillotana SPA", "BSALE_TOKEN_SPA"),
        }
        self.tables: dict[str, dict] = {REGISTRY.get(n).raw_table: {} for n in REGISTRY.pipeline_names()}
        self.runs: dict[int, dict] = {}
        self.entity_runs: dict[int, dict] = {}
        self.sync_state: dict[tuple, dict] = {}
        self.held: set = set()
        self.events: list[str] = []
        self.in_tx = False
        self.fail_on: str | None = None
        self._tx_now: datetime | None = None
        self._ids = itertools.count(1)

    def now(self) -> datetime:
        return self._tx_now or self.db_clock()

    def seed(self, company_id: int, payload: dict, *, fetched_at: datetime, missing_since=None, table=TABLE) -> None:
        self.tables[table][(company_id, int(payload["id"]))] = {
            "payload": payload,
            "payload_hash": payload_hash(payload),
            "first_seen_at": fetched_at,
            "last_seen_at": fetched_at,
            "last_changed_at": fetched_at,
            "api_fetched_at": fetched_at,
            "missing_since": missing_since,
            "last_source": "FULL_RECONCILE",
            "sync_run_id": None,
        }

    def rows(self, company_id: int, table: str = TABLE) -> dict[int, dict]:
        return {bid: r for (cid, bid), r in self.tables[table].items() if cid == company_id}

    def resolve_source(self, company_id):
        self.events.append("resolve_source")
        if company_id not in self.sources:
            raise SourceConfigError(f"company_id={company_id} no existe en bsale_raw.sources")
        return self.sources[company_id]

    @contextmanager
    def advisory_lock(self, company_id, resource, scope):
        key = advisory_lock_keys(company_id, resource, scope)
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
        state_key = (company_id, resource, state_scope or scope)
        with self.transaction():
            now = self.now()
            run_id = next(self._ids)
            self.runs[run_id] = {"mode": mode, "trigger": trigger, "status": "RUNNING", "started_at": now,
                                 "error": None, "summary": None}
            entity_run_id = next(self._ids)
            self.entity_runs[entity_run_id] = {"sync_run_id": run_id, "company_id": company_id,
                                               "resource": resource, "scope": scope, "status": "RUNNING"}
            st = self.sync_state.setdefault(
                state_key,
                {"last_success_at": None, "last_full_reconcile_at": None, "last_error_at": None, "last_error": None},
            )
            st.update(last_attempt_at=now, status="RUNNING", last_sync_run_id=run_id)
        self.events.append("start_run")
        return RunHandle(run_id=run_id, entity_run_id=entity_run_id, started_at=now)

    def _existing(self, spec, company_id):
        return {
            bid: ExistingRow(bid, r["payload_hash"], r["api_fetched_at"], r["missing_since"])
            for (cid, bid), r in self.tables[spec.raw_table].items()
            if cid == company_id
        }

    def read_existing(self, spec, company_id):
        self.events.append("read_existing")
        return self._existing(spec, company_id)

    def _existing_stock(self, spec, company_id, office_id):
        return {
            (vid, oid): ExistingRow(vid, r["payload_hash"], r["api_fetched_at"], None)
            for (cid, vid, oid), r in self.tables[spec.raw_table].items()
            if cid == company_id and oid == office_id
        }

    def read_existing_stock(self, spec, company_id, office_id):
        self.events.append("read_existing_stock")
        return self._existing_stock(spec, company_id, office_id)

    def _existing_stock_variants(self, spec, company_id, variant_ids):
        wanted = set(variant_ids)
        return {
            (vid, oid): ExistingRow(vid, r["payload_hash"], r["api_fetched_at"], None)
            for (cid, vid, oid), r in self.tables[spec.raw_table].items()
            if cid == company_id and vid in wanted
        }

    def read_existing_stock_variants(self, spec, company_id, variant_ids):
        self.events.append("read_existing_stock_variants")
        return self._existing_stock_variants(spec, company_id, variant_ids)

    def seed_stock(self, company_id: int, payload: dict, *, fetched_at: datetime, table="bsale_raw.stocks") -> None:
        key = (company_id, int(payload["variant"]["id"]), int(payload["office"]["id"]))
        self.tables[table][key] = {
            "payload": payload,
            "payload_hash": payload_hash(payload),
            "first_seen_at": fetched_at,
            "last_seen_at": fetched_at,
            "last_changed_at": fetched_at,
            "api_fetched_at": fetched_at,
            "last_source": "SCANNER",
            "sync_run_id": None,
        }

    def stock_rows(self, company_id: int, office_id: int | None = None, table="bsale_raw.stocks") -> dict:
        return {
            (vid, oid): r for (cid, vid, oid), r in self.tables[table].items()
            if cid == company_id and (office_id is None or oid == office_id)
        }

    @contextmanager
    def transaction(self):
        backup = copy.deepcopy((self.tables, self.runs, self.entity_runs, self.sync_state))
        self.in_tx = True
        self._tx_now = self.db_clock()
        self.events.append("tx_begin")
        try:
            yield self.tx_class(self)
        except BaseException:
            self.tables, self.runs, self.entity_runs, self.sync_state = backup
            self.events.append("tx_rollback")
            raise
        else:
            self.events.append("tx_commit")
        finally:
            self.in_tx = False
            self._tx_now = None

    def _close(self, handle, outcome):
        self.entity_runs[handle.entity_run_id].update(outcome.summary())
        self.runs[handle.run_id].update(status=outcome.status, error=outcome.error, summary=outcome.summary())

    def finish_failed(self, handle, outcome):
        with self.transaction():
            self._close(handle, outcome)
            st = self.sync_state[(outcome.company_id, outcome.resource, outcome.sync_state_scope)]
            st.update(
                last_attempt_at=handle.started_at,
                last_error_at=self.now(),
                last_error=outcome.error,
                rows_received=outcome.rows_received,
                duration_ms=outcome.duration_ms,
                status=outcome.status,
                last_sync_run_id=handle.run_id,
            )


# --- fake Bsale -------------------------------------------------------------------------------


def make_response(request, status: int, body: bytes, headers: dict | None = None) -> requests.Response:
    r = requests.Response()
    r.status_code = status
    r._content = body
    r.headers = CaseInsensitiveDict(headers or {})
    r.url = request.url
    r.request = request
    r.encoding = "utf-8"
    return r


class FakeBsale(BaseAdapter):
    """Listado paginado por limit/offset. ``script``: acciones previas (Exception, (status, body, headers) o None)."""

    def __init__(self, items=None, *, count=None, script=None, store: FakeStore | None = None, pages=None):
        super().__init__()
        self.items = list(items or [])
        self.count = count
        self.script = list(script or [])
        self.store = store
        self.pages = pages  # páginas explícitas por número de llamada
        self.calls: list = []
        self.tx_violations: list[str] = []

    def send(self, request, **kwargs):
        if self.store is not None:
            if self.store.in_tx:
                self.tx_violations.append(request.url)
            self.store.events.append("http")
        self.calls.append(request)
        if self.script:
            action = self.script.pop(0)
            if isinstance(action, BaseException):
                raise action
            if action is not None:
                status, body, headers = action
                return make_response(request, status, body, headers)
        q = parse_qs(urlsplit(request.url).query)
        offset, limit = int(q["offset"][0]), int(q["limit"][0])
        if self.pages is not None:
            page = self.pages[min(len(self.calls), len(self.pages)) - 1]
        else:
            page = self.items[offset: offset + limit]
        count = len(self.items) if self.count is None else self.count
        body = {"href": "https://api.bsale.io/v1/offices.json", "count": count, "limit": limit,
                "offset": offset, "items": page}
        return make_response(request, 200, json.dumps(body).encode())

    def close(self):
        pass


def client_factory_for(adapter: FakeBsale):
    def factory(source, token, spec):
        limiter = PriorityRateLimiter(TokenBucket(RateLimitConfig(requests_per_second=10.0, burst=1000)))
        session = RateLimitedSession(limiter, spec.request_priority)
        session.mount("https://", adapter)
        return BsaleHttpClient(token, session=session, sleep=lambda s: None, max_attempts=3)

    return factory


def run(store: FakeStore, adapter: FakeBsale, *, company_id=3, dry_run=False, clock=None, env=None,
        resource="offices"):
    return run_entity_sync(
        store=store,
        company_id=company_id,
        resource=resource,
        mode=SyncMode.FULL_RECONCILE,
        dry_run=dry_run,
        client_factory=client_factory_for(adapter),
        clock=clock or TickClock(BASE),
        getenv=(ENV if env is None else env).get,
        host="test",
    )


def fresh(items=None, **kw):
    store = FakeStore()
    adapter = FakeBsale(items, store=store, **kw)
    return store, adapter


# --- paginación / validación ------------------------------------------------------------------


def test_full_pagination_reads_every_page():
    store, adapter = fresh([office(i) for i in range(1, 121)])
    out = run(store, adapter)
    assert out.status == "SUCCESS", out.error
    assert (out.api_count, out.rows_received, out.rows_inserted) == (120, 120, 120)
    assert out.pages == 3 and out.requests == 3
    assert len(store.rows(3)) == 120
    offsets = [parse_qs(urlsplit(c.url).query)["offset"][0] for c in adapter.calls]
    assert offsets == ["0", "50", "100"]
    assert {urlsplit(c.url).path for c in adapter.calls} == {"/v1/offices.json"}
    assert {urlsplit(c.url).netloc for c in adapter.calls} == {"api.bsale.io"}


def test_items_share_fetched_at_per_page_and_after_snapshot_start():
    adapter = FakeBsale([office(i) for i in range(1, 61)])
    clock = TickClock(BASE)
    started = clock()
    client = client_factory_for(adapter)(None, TOKEN, OFFICES)
    snap = fetch_snapshot(client, OFFICES.list_endpoint, clock=clock)
    first, second = snap.items[0].fetched_at, snap.items[55].fetched_at
    assert all(it.fetched_at == first for it in snap.items[:50])
    assert first < second and started < first


def test_truncated_response_rejected():
    store, adapter = fresh(pages=[[office(i) for i in range(1, 51)], []], count=120)
    out = run(store, adapter)
    assert out.status == "FAILED" and "truncada" in out.error
    assert store.rows(3) == {}


def test_count_changing_mid_snapshot_rejected():
    adapter = FakeBsale([office(i) for i in range(1, 61)])
    client = client_factory_for(adapter)(None, TOKEN, OFFICES)
    original = client.get_json
    calls = {"n": 0}

    def get_json(endpoint, params):
        data = original(endpoint, params)
        calls["n"] += 1
        if calls["n"] == 2:
            data["count"] = 61
        return data

    client.get_json = get_json
    with pytest.raises(SnapshotValidationError, match="count cambió"):
        fetch_snapshot(client, OFFICES.list_endpoint)


def test_more_items_than_count_rejected():
    store, adapter = fresh([office(1), office(2)], count=1)
    out = run(store, adapter)
    assert out.status == "FAILED" and store.rows(3) == {}


def test_duplicate_bsale_id_rejected_without_writes():
    page1 = [office(i) for i in range(1, 51)]
    page2 = [office(50), office(51)]
    store, adapter = fresh(pages=[page1, page2], count=52)
    out = run(store, adapter)
    assert out.status == "FAILED" and "duplicados" in out.error
    assert store.rows(3) == {}
    assert store.runs[out.sync_run_id]["status"] == "FAILED"


def test_item_without_numeric_id_rejected():
    store, adapter = fresh([office(1), {**office(2), "id": "abc"}])
    out = run(store, adapter)
    assert out.status == "FAILED" and "id" in out.error and store.rows(3) == {}


def test_malformed_json_fails_without_writes():
    store, adapter = fresh([office(1)], script=[(200, b"<html>not json</html>", {})])
    out = run(store, adapter)
    assert out.status == "FAILED" and "JSON" in out.error
    assert store.rows(3) == {}
    assert out.requests == 1


def test_api_429_is_retried_and_counted():
    store, adapter = fresh([office(1), office(2)], script=[(429, b"{}", {"Retry-After": "0"})])
    out = run(store, adapter)
    assert out.status == "SUCCESS", out.error
    assert out.http_429 == 1 and out.requests == 2 and out.rows_inserted == 2


def test_api_429_exhausted_fails_without_writes():
    too_many = (429, b"{}", {"Retry-After": "0"})
    store, adapter = fresh([office(1)], script=[too_many, too_many, too_many])
    out = run(store, adapter)
    assert out.status == "FAILED" and "Reintentos agotados" in out.error
    assert out.http_429 == 3 and store.rows(3) == {}


def test_timeout_network_fails_without_writes():
    store, adapter = fresh([office(1)], script=[requests.Timeout("read timeout")] * 3)
    out = run(store, adapter)
    assert out.status == "FAILED" and "Timeout" in out.error
    assert out.requests == 3 and store.rows(3) == {}
    assert store.sync_state[(3, "offices", "global")]["status"] == "FAILED"


def test_empty_valid_snapshot_on_empty_table_succeeds():
    store, adapter = fresh([])
    out = run(store, adapter)
    assert out.status == "SUCCESS" and out.api_count == 0 and out.rows_received == 0


def test_empty_snapshot_with_existing_rows_trips_fuse():
    store, adapter = fresh([])
    store.seed(3, office(1), fetched_at=BASE - timedelta(days=1))
    out = run(store, adapter)
    assert out.status == "FAILED" and "vacío" in out.error
    assert store.rows(3)[1]["missing_since"] is None


# --- fusible / faltantes ----------------------------------------------------------------------


def test_fuse_over_20_percent_blocks_all_writes():
    store, adapter = fresh([office(i, name="nuevo") for i in range(1, 8)])
    for i in range(1, 11):
        store.seed(3, office(i), fetched_at=BASE - timedelta(days=1))
    before = copy.deepcopy(store.rows(3))
    out = run(store, adapter)
    assert out.status == "FAILED" and "fusible" in out.error
    assert store.rows(3) == before  # ni upsert ni missing_since
    assert out.fuse["tripped"] and out.fuse["missing_pct"] == 30.0 and out.fuse["threshold_pct"] == 20.0
    entity = store.entity_runs[next(iter(store.entity_runs))]
    assert entity["status"] == "FAILED" and entity["fuse"]["tripped"]
    assert out.rows_inserted == out.rows_updated == out.rows_missing == 0


def test_under_fuse_marks_missing_without_deleting():
    store, adapter = fresh([office(i) for i in range(1, 10)])
    for i in range(1, 11):
        store.seed(3, office(i), fetched_at=BASE - timedelta(days=1))
    out = run(store, adapter)
    assert out.status == "SUCCESS", out.error
    assert out.rows_missing == 1 and out.rows_deleted == 0
    rows = store.rows(3)
    assert len(rows) == 10 and rows[10]["missing_since"] is not None
    assert all(rows[i]["missing_since"] is None for i in range(1, 10))


def test_row_refreshed_during_snapshot_is_never_marked_missing():
    store, adapter = fresh([office(1)])
    store.seed(3, office(1), fetched_at=BASE - timedelta(days=1))
    store.seed(3, office(99), fetched_at=BASE + timedelta(hours=1))  # p. ej. webhook durante el snapshot
    out = run(store, adapter)
    assert out.status == "SUCCESS", out.error
    assert out.rows_missing == 0 and out.fuse["protected_newer"] == 1
    assert store.rows(3)[99]["missing_since"] is None


def test_reappeared_row_clears_missing_since():
    store, adapter = fresh([office(1)])
    store.seed(3, office(1), fetched_at=BASE - timedelta(days=1), missing_since=BASE - timedelta(hours=5))
    out = run(store, adapter)
    assert out.status == "SUCCESS" and out.rows_unchanged == 1
    assert store.rows(3)[1]["missing_since"] is None


def test_fuse_threshold_configurable_and_validated():
    assert max_missing_pct("offices", {}.get) == 20.0
    assert max_missing_pct("offices", {"BSALE_RAW_MAX_MISSING_PCT_OFFICES": "35"}.get) == 35.0
    with pytest.raises(ValueError):
        max_missing_pct("offices", {"BSALE_RAW_MAX_MISSING_PCT": "150"}.get)


# --- hash / timestamps / idempotencia ---------------------------------------------------------


def test_payload_hash_canonical_and_sensitive():
    a = office(1)
    b = dict(reversed(list(a.items())))
    assert payload_hash(a) == payload_hash(b)
    assert payload_hash(a) != payload_hash({**a, "name": "otro"})


def test_payload_stored_exactly_as_received():
    item = office(7, extraField={"nested": [1, "2", None]}, state="0")
    store, adapter = fresh([item])
    out = run(store, adapter)
    assert out.status == "SUCCESS"
    row = store.rows(3)[7]
    assert row["payload"] == item and row["payload_hash"] == payload_hash(item)
    assert (row["state"], row["name"], row["is_virtual"], row["cost_center"]) == (0, "Sucursal 7", 0, "")


def test_idempotency_run1_inserts_run2_unchanged():
    items = [office(i) for i in range(1, 6)]
    store, adapter = fresh(items)
    clock = TickClock(BASE)
    out1 = run(store, adapter, clock=clock)
    assert (out1.rows_inserted, out1.rows_updated, out1.rows_unchanged) == (5, 0, 0)
    snap1 = copy.deepcopy(store.rows(3))

    out2 = run(store, adapter, clock=clock)
    assert out2.status == "SUCCESS"
    assert (out2.rows_inserted, out2.rows_updated, out2.rows_unchanged, out2.rows_skipped_newer) == (0, 0, 5, 0)
    for bid, r2 in store.rows(3).items():
        r1 = snap1[bid]
        assert r2["first_seen_at"] == r1["first_seen_at"]
        assert r2["last_changed_at"] == r1["last_changed_at"]
        assert r2["last_seen_at"] > r1["last_seen_at"]
        assert r2["api_fetched_at"] > r1["api_fetched_at"]
        assert r2["sync_run_id"] == out2.sync_run_id


def test_changed_payload_updates_last_changed_but_keeps_first_seen():
    store, adapter = fresh([office(1), office(2)])
    clock = TickClock(BASE)
    run(store, adapter, clock=clock)
    snap1 = copy.deepcopy(store.rows(3))
    adapter.items = [office(1, name="Renombrada"), office(2)]
    out = run(store, adapter, clock=clock)
    assert (out.rows_updated, out.rows_unchanged) == (1, 1)
    r1, r2 = store.rows(3)[1], store.rows(3)[2]
    assert r1["first_seen_at"] == snap1[1]["first_seen_at"]
    assert r1["last_changed_at"] > snap1[1]["last_changed_at"]
    assert r1["name"] == "Renombrada" and r1["payload"]["name"] == "Renombrada"
    assert r2["last_changed_at"] == snap1[2]["last_changed_at"]


def test_stale_upsert_rejected_when_target_is_newer():
    store, adapter = fresh([office(1, name="viejo")])
    newer = BASE + timedelta(hours=2)
    store.seed(3, office(1, name="webhook"), fetched_at=newer)
    out = run(store, adapter)
    assert out.status == "SUCCESS"
    assert out.rows_skipped_newer == 1 and out.rows_updated == 0
    row = store.rows(3)[1]
    assert row["payload"]["name"] == "webhook" and row["api_fetched_at"] == newer


# --- runs / state -----------------------------------------------------------------------------


def test_sync_run_success_records_everything():
    store, adapter = fresh([office(1), office(2)])
    out = run(store, adapter)
    assert store.runs[out.sync_run_id]["status"] == "SUCCESS"
    assert store.runs[out.sync_run_id]["mode"] == "FULL_RECONCILE"
    assert store.runs[out.sync_run_id]["trigger"] == "MANUAL"
    entity = next(e for e in store.entity_runs.values() if e["sync_run_id"] == out.sync_run_id)
    assert (entity["company_id"], entity["resource"], entity["scope"]) == (3, "offices", "global")
    for key in ("rows_received", "rows_inserted", "rows_updated", "rows_unchanged", "rows_skipped_newer",
                "rows_missing", "rows_deleted", "api_count", "requests", "duration_ms", "snapshot_started_at",
                "fuse", "error"):
        assert key in entity
    assert entity["rows_inserted"] == 2 and entity["api_count"] == 2 and entity["error"] is None
    st = store.sync_state[(3, "offices", "global")]
    assert st["status"] == "SUCCESS" and st["last_success_at"] and st["last_full_reconcile_at"]
    assert st["rows_received"] == 2 and st["last_sync_run_id"] == out.sync_run_id


def test_sync_run_failed_and_state_error_semantics():
    store, adapter = fresh([office(1)])
    clock = TickClock(BASE)
    ok = run(store, adapter, clock=clock)
    st_ok = copy.deepcopy(store.sync_state[(3, "offices", "global")])

    adapter.script = [requests.ConnectionError("down")] * 3
    bad = run(store, adapter, clock=clock)
    assert bad.status == "FAILED"
    assert store.runs[bad.sync_run_id]["status"] == "FAILED" and store.runs[bad.sync_run_id]["error"]
    st = store.sync_state[(3, "offices", "global")]
    assert st["last_success_at"] == st_ok["last_success_at"]
    assert st["last_full_reconcile_at"] == st_ok["last_full_reconcile_at"]
    assert st["last_attempt_at"] > st_ok["last_attempt_at"]
    assert st["last_error_at"] is not None and "ConnectionError" in st["last_error"]
    assert st["status"] == "FAILED" and st["last_sync_run_id"] == bad.sync_run_id != ok.sync_run_id


# --- lock / transacciones / aislamiento -------------------------------------------------------


def test_advisory_lock_busy_skips_without_http_or_writes():
    store, adapter = fresh([office(1)])
    store.held.add(advisory_lock_keys(3, "offices", "global"))
    out = run(store, adapter)
    assert out.status == "SKIPPED" and "lock" in out.error
    assert adapter.calls == [] and store.runs == {} and store.rows(3) == {}
    assert cli.exit_code(out) == cli.EXIT_LOCKED


def test_advisory_lock_keys_scoped_and_int32():
    k = advisory_lock_keys(3, "offices", "global")
    assert k == advisory_lock_keys(3, "offices", "global")
    assert k != advisory_lock_keys(1, "offices", "global")
    assert k != advisory_lock_keys(3, "taxes", "global")
    assert k != advisory_lock_keys(3, "offices", "office:1")
    assert k[0] == ADVISORY_LOCK_NAMESPACE and all(-(2**31) <= x < 2**31 for x in k)


def test_lock_released_after_run():
    store, adapter = fresh([office(1)])
    run(store, adapter)
    assert store.held == set() and store.events[-1] == "unlock"


def test_no_http_while_transaction_open_and_order():
    store, adapter = fresh([office(i) for i in range(1, 80)])
    out = run(store, adapter)
    assert out.status == "SUCCESS"
    assert adapter.tx_violations == []
    ev = store.events
    first_http, last_http = ev.index("http"), len(ev) - 1 - ev[::-1].index("http")
    assert ev.index("lock") < ev.index("start_run") < first_http
    data_tx = ev.index("lock_existing")
    assert last_http < data_tx < ev.index("unlock")


def test_transaction_rollback_leaves_raw_untouched():
    store, adapter = fresh([office(i) for i in range(1, 10)])
    for i in range(1, 11):
        store.seed(3, office(i), fetched_at=BASE - timedelta(days=1))
    before = copy.deepcopy(store.rows(3))
    store.fail_on = "mark_missing"
    out = run(store, adapter)
    assert out.status == "FAILED" and "mark_missing" in out.error
    assert store.rows(3) == before
    assert "tx_rollback" in store.events
    assert out.rows_inserted == out.rows_unchanged == out.rows_missing == 0
    assert store.runs[out.sync_run_id]["status"] == "FAILED"
    assert store.sync_state[(3, "offices", "global")]["last_success_at"] is None


def test_company_isolation():
    store, adapter = fresh([office(1, name="C3")])
    for i in range(1, 11):
        store.seed(1, office(i, name="C1"), fetched_at=BASE - timedelta(days=1))
    before_c1 = copy.deepcopy(store.rows(1))
    out = run(store, adapter, company_id=3)
    assert out.status == "SUCCESS" and out.rows_inserted == 1 and out.rows_missing == 0
    assert store.rows(1) == before_c1
    assert store.rows(3)[1]["payload"]["name"] == "C3"
    assert (1, "offices", "global") not in store.sync_state


def test_company_uses_its_own_token_env():
    store, adapter = fresh([office(1)])
    run(store, adapter, company_id=3)
    assert {c.headers["access_token"] for c in adapter.calls} == {TOKEN}


def test_unknown_company_or_missing_token_fails_before_http():
    store, adapter = fresh([office(1)])
    out = run(store, adapter, company_id=42)
    assert out.status == "FAILED" and "no existe" in out.error and adapter.calls == []
    out = run(store, adapter, env={})
    assert out.status == "FAILED" and "BSALE_TOKEN_SPA" in out.error and adapter.calls == []
    assert store.runs == {}


# --- secretos / dry-run -----------------------------------------------------------------------


def test_token_never_in_errors_logs_or_cli(caplog):
    caplog.set_level(logging.DEBUG)
    leak = json.dumps({"error": f"invalid token {TOKEN}"}).encode()
    store, adapter = fresh([office(1)], script=[(401, leak, {})])
    out = run(store, adapter)
    assert out.status == "FAILED" and "401" in out.error
    buf = io.StringIO()
    cli.main(["sync", "--company", "3", "--resource", "offices", "--mode", "full-reconcile"],
             runner=lambda **kw: out, out=buf)
    for text in (out.error, caplog.text, buf.getvalue(), repr(store.runs), repr(store.entity_runs),
                 repr(store.sync_state)):
        assert TOKEN not in text
    assert "***" in out.error


def test_payload_never_logged():
    store, adapter = fresh([office(1, address="Dirección Privada 999")])
    logger = logging.getLogger("backend.services.bsale_raw")
    records: list[str] = []
    handler = logging.Handler()
    handler.emit = lambda rec: records.append(rec.getMessage())
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    try:
        out = run(store, adapter)
    finally:
        logger.removeHandler(handler)
    assert out.status == "SUCCESS" and records
    assert not any("Privada" in r for r in records)
    assert "Privada" not in cli.format_outcome(out)


def test_dry_run_writes_nothing():
    store, adapter = fresh([office(1), office(2, name="cambio"), office(3), *(office(i) for i in range(4, 8))])
    for i in (2, 4, 5, 6, 7, 9):
        store.seed(3, office(i), fetched_at=BASE - timedelta(days=1))
    before = copy.deepcopy((store.tables, store.runs, store.entity_runs, store.sync_state))
    out = run(store, adapter, dry_run=True)
    assert out.dry_run and out.status == "SUCCESS" and out.sync_run_id is None
    assert (out.rows_inserted, out.rows_updated, out.rows_unchanged, out.rows_missing) == (2, 1, 4, 1)
    assert (store.tables, store.runs, store.entity_runs, store.sync_state) == before
    for forbidden in ("lock", "start_run", "tx_begin"):
        assert forbidden not in store.events


def test_dry_run_reports_fuse_without_writing():
    store, adapter = fresh([office(1)])
    for i in range(1, 6):
        store.seed(3, office(i), fetched_at=BASE - timedelta(days=1))
    out = run(store, adapter, dry_run=True)
    assert out.status == "FAILED" and out.fuse["tripped"] and "tx_begin" not in store.events


CONFIG_RESOURCES = ["offices", "taxes", "document_types", "product_types", "price_lists"]
CATALOG_RESOURCES = ["products", "variants"]
STOCK_RESOURCES = ["stocks"]
DOCUMENT_RESOURCES = ["documents"]  # sólo POINT por id (fase 4E1); nunca barrido completo
NOT_YET_ENABLED = ["clients", "variant_prices", "variant_costs",
                   "document_details", "stock_receptions", "stock_consumptions"]


def test_only_configuration_catalog_stock_and_document_point_enabled():
    assert REGISTRY.pipeline_names() == CONFIG_RESOURCES + CATALOG_RESOURCES + STOCK_RESOURCES + DOCUMENT_RESOURCES
    with pytest.raises(UnsupportedSyncError):  # stock nunca entra al motor de entidades
        run_entity_sync(store=FakeStore(), company_id=3, resource="stocks")
    with pytest.raises(UnsupportedSyncError):  # documentos: full scan global prohibido
        run_entity_sync(store=FakeStore(), company_id=3, resource="documents")
    assert REGISTRY.get("documents").pipeline_modes == (SyncMode.POINT,)
    for name in NOT_YET_ENABLED:
        if name in REGISTRY.names():
            assert not REGISTRY.get(name).pipeline_enabled, name
        with pytest.raises(UnsupportedSyncError):
            run_entity_sync(store=FakeStore(), company_id=3, resource=name)
    with pytest.raises(UnsupportedSyncError):
        run_entity_sync(store=FakeStore(), company_id=3, resource="offices", mode=SyncMode.INCREMENTAL)


# --- CLI --------------------------------------------------------------------------------------


def test_cli_output_and_args():
    seen = {}

    def runner(**kw):
        seen.update(kw)
        return EntityOutcome(company_id=3, resource="offices", scope="global", mode="FULL_RECONCILE",
                             status="SUCCESS", dry_run=True, api_count=4, rows_received=4, rows_inserted=4,
                             requests=1, duration_ms=12)

    buf = io.StringIO()
    code = cli.main(["sync", "--company", "3", "--resource", "offices", "--mode", "full-reconcile", "--dry-run"],
                    runner=runner, out=buf)
    assert code == cli.EXIT_SUCCESS
    assert seen == {"company_id": 3, "resource": "offices", "mode": SyncMode.FULL_RECONCILE, "dry_run": True,
                    "office_id": None, "variant_id": None, "document_id": None}
    lines = buf.getvalue().splitlines()
    assert lines[0] == "dry_run=true"
    keys = [line.split("=", 1)[0] for line in lines[1:]]
    assert keys == ["company", "resource", "scope", "mode", "api_count", "received", "inserted", "updated",
                    "unchanged", "skipped_newer", "missing", "deleted", "requests", "duration_ms", "status"]
    assert "scope=global" in lines
    assert "status=SUCCESS" in lines


def test_cli_rejects_unsupported_resource_and_mode():
    never = lambda **kw: pytest.fail("no debe ejecutarse")  # noqa: E731
    base = ["sync", "--company", "3", "--mode", "full-reconcile"]
    for name in NOT_YET_ENABLED + DOCUMENT_RESOURCES:
        assert cli.main([*base, "--resource", name], runner=never, out=io.StringIO()) == cli.EXIT_USAGE
    assert cli.main(["sync", "--company", "3", "--resource", "offices", "--mode", "incremental"],
                    runner=never, out=io.StringIO()) == cli.EXIT_USAGE


def test_cli_exit_codes():
    def outcome(status):
        return EntityOutcome(company_id=3, resource="offices", scope="global", mode="FULL_RECONCILE", status=status)

    assert cli.exit_code(outcome("SUCCESS")) == 0
    assert cli.exit_code(outcome("FAILED")) == 1
    assert cli.exit_code(outcome("PARTIAL")) == 2
    assert cli.exit_code(outcome("SKIPPED")) == 3


def test_single_entrypoint_no_loose_scripts():
    folder = Path(cli.__file__).parent
    assert sorted(p.name for p in folder.glob("*.py")) == ["__init__.py", "__main__.py", "cli.py"]


# --- SQL real de PgRawStore (cursor falso) ----------------------------------------------------


def test_upsert_sql_freshness_and_timestamps():
    sql, template = build_entity_upsert(OFFICES)
    assert sql.startswith("INSERT INTO bsale_raw.offices AS t (")
    assert "ON CONFLICT (company_id, bsale_id) DO UPDATE SET" in sql
    assert "WHERE t.api_fetched_at <= EXCLUDED.api_fetched_at" in sql
    assert sql.rstrip().endswith("RETURNING bsale_id")
    set_clause = sql.split("DO UPDATE SET", 1)[1].split("WHERE", 1)[0]
    assert "first_seen_at" not in set_clause
    assert "last_seen_at = EXCLUDED.last_seen_at" in set_clause
    assert "CASE WHEN t.payload_hash IS DISTINCT FROM EXCLUDED.payload_hash" in set_clause
    assert "missing_since = NULL" in set_clause
    assert template.count("%s") == 11 and template.count("now()") == 3


def test_mark_missing_sql_never_deletes():
    sql = build_mark_missing(OFFICES)
    assert sql.startswith("UPDATE bsale_raw.offices SET missing_since = now()")
    assert "api_fetched_at <= %s" in sql and "missing_since IS NULL" in sql and "company_id = %s" in sql
    assert "DELETE" not in sql.upper()


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn
        self.connection = conn
        self.results: list = []
        self.rowcount = 0

    def execute(self, sql, params=None):
        text = sql.decode() if isinstance(sql, bytes) else sql
        self.conn.executed.append((text, params))
        if self.conn.fail_sql and self.conn.fail_sql in text:
            raise RuntimeError("fallo SQL inyectado")
        self.results = self.conn.next_results.pop(0) if self.conn.next_results else []

    def mogrify(self, template, args):
        return (template % tuple(repr(a) for a in args)).encode()

    def fetchone(self):
        return self.results[0] if self.results else None

    def fetchall(self):
        return list(self.results)

    def close(self):
        pass


class FakeConnection:
    encoding = "UTF8"

    def __init__(self, next_results=None, fail_sql=None):
        self.executed: list = []
        self.next_results = list(next_results or [])
        self.fail_sql = fail_sql
        self.autocommit = False
        self.commits = 0
        self.rollbacks = 0
        self.closed = False
        self.session: dict = {}

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = True

    def set_session(self, **kw):
        self.session.update(kw)


def test_pg_upsert_through_execute_values():
    conn = FakeConnection(next_results=[[(1,), (2,)]])
    adapter = FakeBsale([office(1), office(2)])
    snap = fetch_snapshot(client_factory_for(adapter)(None, TOKEN, OFFICES), OFFICES.list_endpoint)
    rows = build_rows(OFFICES, 3, snap)
    applied = PgRawTx(conn.cursor()).upsert(OFFICES, rows, sync_run_id=77, last_source="FULL_RECONCILE")
    assert applied == {1, 2}
    sql = conn.executed[-1][0]
    assert "INSERT INTO bsale_raw.offices AS t" in sql and "'FULL_RECONCILE', 77" in sql


def test_pg_transaction_commit_and_rollback():
    conn = FakeConnection()
    store = PgRawStore(lambda: conn)
    with store.transaction() as tx:
        tx.cur.execute("SELECT 1")
    assert conn.commits == 1 and conn.autocommit is True
    assert any("lock_timeout" in s for s, _ in conn.executed)
    with pytest.raises(RuntimeError):
        with store.transaction():
            raise RuntimeError("boom")
    assert conn.rollbacks == 1 and conn.autocommit is True


def test_pg_advisory_lock_sql_and_busy():
    conn = FakeConnection(next_results=[[(True,)], []])
    store = PgRawStore(lambda: conn)
    with store.advisory_lock(3, "offices", "global"):
        pass
    k1, k2 = advisory_lock_keys(3, "offices", "global")
    assert conn.executed[0] == ("SELECT pg_try_advisory_lock(%s, %s)", (k1, k2))
    assert conn.executed[1] == ("SELECT pg_advisory_unlock(%s, %s)", (k1, k2))
    assert conn.autocommit is True and conn.closed

    busy = FakeConnection(next_results=[[(False,)]])
    with pytest.raises(LockBusyError):
        with PgRawStore(lambda: busy).advisory_lock(3, "offices", "global"):
            pytest.fail("no debe entrar")


def test_pg_read_only_store_for_dry_run():
    conn = FakeConnection(next_results=[[(3, 21884, "SPA", "BSALE_TOKEN_SPA", True, "BSALE_TOKEN_SPA")]])
    store = PgRawStore(lambda: conn, read_only=True)
    assert store.resolve_source(3).token_env == "BSALE_TOKEN_SPA"
    assert conn.session == {"readonly": True, "autocommit": True}
    with pytest.raises(RuntimeError):
        with store.transaction():
            pass
    with pytest.raises(RuntimeError):
        store.start_run(mode="FULL_RECONCILE", trigger="MANUAL", host=None, company_id=3,
                        resource="offices", scope="global")


@pytest.mark.parametrize(
    "row, message",
    [
        (None, "no existe"),
        ((3, 21884, "SPA", "BSALE_TOKEN_SPA", False, "BSALE_TOKEN_SPA"), "inactiva"),
        ((3, 21884, "SPA", "BSALE_TOKEN_SPA", True, "BSALE_TOKEN_Mini"), "no coincide"),
    ],
)
def test_pg_resolve_source_guards(row, message):
    conn = FakeConnection(next_results=[[row] if row else []])
    with pytest.raises(SourceConfigError, match=message):
        PgRawStore(lambda: conn).resolve_source(3)


def test_pg_finish_failed_never_touches_last_success():
    conn = FakeConnection()
    store = PgRawStore(lambda: conn)
    outcome = EntityOutcome(company_id=3, resource="offices", scope="global", mode="FULL_RECONCILE",
                            status="FAILED", error="x")
    store.finish_failed(RunHandle(1, 2, BASE), outcome)
    state_sql = next(s for s, _ in conn.executed if "sync_state" in s)
    assert "last_success_at" not in state_sql and "last_error_at" in state_sql
    assert conn.commits == 1
