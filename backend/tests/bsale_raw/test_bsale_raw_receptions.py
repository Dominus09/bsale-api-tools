"""Motor de recepciones (``core/reception_engine.py``): sin red ni BD (Bsale y store falsos)."""

from __future__ import annotations

import copy
import io
import itertools
import json
import logging
import re
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from urllib.parse import parse_qs, urlsplit

import pytest
from requests.adapters import BaseAdapter

from backend.jobs.bsale_raw import cli
from backend.services.bsale_raw.core import reception_engine as re_
from backend.services.bsale_raw.core.models import payload_hash
from backend.services.bsale_raw.core.reception_engine import (
    DETAIL_COLUMNS,
    INSERT_INCOMPLETE_SQL,
    MARK_DETAILS_MISSING_SQL,
    MARK_RECEPTIONS_MISSING_SQL,
    RECEPTION_COLUMNS,
    UPSERT_DETAILS_SQL,
    UPSERT_RECEPTIONS_SQL,
    ReceptionRequest,
    StoredDetail,
    StoredReception,
    build_details,
    format_reception_outcome,
    node_problem,
    run_reception_sync,
)
from backend.services.bsale_raw.core.store import LockBusyError, RunHandle, SourceConfig
from backend.tests.bsale_raw._raw_sql_schema import parse
from backend.tests.bsale_raw.test_bsale_raw_pipeline import BASE, ENV, TOKEN, TickClock, client_factory_for, make_response

API = "https://api.bsale.io/v1"
NOTE = "NOTA SECRETA de Juan Pérez +56911111111"
USER_ID = 987654
DAY = date(2026, 7, 31)
ITEM_RE = re.compile(r"^/v1/stocks/receptions/(\d+)\.json$")
DETAILS_RE = re.compile(r"^/v1/stocks/receptions/(\d+)/details\.json$")


def ts(day: date) -> int:
    return int(datetime(day.year, day.month, day.day, tzinfo=timezone.utc).timestamp())


def reception(rid: int, day: date = DAY, *, office=1, document="FACTURA", number="123") -> dict:
    return {
        "href": f"{API}/stocks/receptions/{rid}.json", "id": rid, "admissionDate": ts(day), "document": document,
        "documentNumber": number, "note": NOTE, "internalDispatchId": 0,
        "office": {"href": f"{API}/offices/{office}.json", "id": str(office)},
        "user": {"href": f"{API}/users/{USER_ID}.json", "id": str(USER_ID)},
        "details": {"href": f"{API}/stocks/receptions/{rid}/details.json"},
    }


def line(rid: int, n: int, *, variant=None, quantity=None, cost=None) -> dict:
    did = rid * 1000 + n
    return {
        "href": f"{API}/stocks/receptions/{rid}/details/{did}.json", "id": did,
        "quantity": 2.0 + n if quantity is None else quantity, "cost": 1000.5 + n if cost is None else cost,
        "variantStock": 10, "serialNumber": "",
        "variant": {"href": f"{API}/variants/{variant or 5000 + n}.json", "id": str(variant or 5000 + n)},
    }


# --- Bsale falso -------------------------------------------------------------------------------------


class ReceptionsBsale(BaseAdapter):
    """``receptions``: cabeceras; ``lines[rid]``: líneas. ``expand``: full | ignored | corrupt; ``expand_limit``:
    nodo truncado a N líneas; ``details_status[rid]``: status fijo de details.json; ``status``: status fijo de todo."""

    def __init__(self, receptions=(), lines=None, *, store=None, expand="full", expand_limit=25,
                 ignore_date_filter=False):
        super().__init__()
        self.receptions = {r["id"]: r for r in receptions}
        self.lines = {rid: list(v) for rid, v in (lines or {}).items()}
        self.store = store
        self.expand = expand
        self.expand_limit = expand_limit
        self.ignore_date_filter = ignore_date_filter
        self.details_status: dict[int, int] = {}
        self.day_status: dict[int, int] = {}
        self.status: int | None = None
        self.calls: list[tuple[str, dict]] = []
        self.tx_violations: list[str] = []

    def add(self, rec: dict, lines: list[dict]) -> None:
        self.receptions[rec["id"]] = rec
        self.lines[rec["id"]] = lines

    def node(self, rid: int) -> dict:
        lines = copy.deepcopy(self.lines.get(rid, []))
        href = f"{API}/stocks/receptions/{rid}/details.json"
        if self.expand == "ignored":
            return {"href": href}
        if self.expand == "corrupt" and lines:
            lines[0]["quantity"] = "no-numérico"
        return {"href": href, "count": len(lines), "limit": self.expand_limit, "offset": 0,
                "items": lines[:self.expand_limit]}

    def decorate(self, rec: dict, query: dict) -> dict:
        rec = copy.deepcopy(rec)
        if query.get("expand") == "[details]":
            rec["details"] = self.node(rec["id"])
        return rec

    def body(self, path: str, query: dict) -> tuple[int, dict]:
        if self.status is not None:
            return self.status, {"error": "falla"}
        offset, limit = int(query.get("offset", 0)), min(int(query.get("limit", 25)), 50)
        if path == "/v1/stocks/receptions.json":
            rows = [self.receptions[k] for k in sorted(self.receptions)]
            if "admissiondate" in query:
                day_ts = int(query["admissiondate"])
                if day_ts in self.day_status:
                    return self.day_status[day_ts], {"error": "falla"}
                if not self.ignore_date_filter:
                    rows = [r for r in rows if r["admissionDate"] == day_ts]
            page = [self.decorate(r, query) for r in rows[offset:offset + limit]]
            return 200, {"href": "x", "count": len(rows), "limit": limit, "offset": offset, "items": page}
        if m := ITEM_RE.match(path):
            rid = int(m.group(1))
            if rid not in self.receptions:
                return 404, {"error": "not found"}
            return 200, self.decorate(self.receptions[rid], query)
        if m := DETAILS_RE.match(path):
            rid = int(m.group(1))
            if rid in self.details_status:
                return self.details_status[rid], {"error": "falla"}
            lines = self.lines.get(rid, [])
            return 200, {"href": "x", "count": len(lines), "limit": limit, "offset": offset,
                         "items": copy.deepcopy(lines[offset:offset + limit])}
        return 404, {"error": "ruta"}

    def send(self, request, **kwargs):
        assert request.method == "GET", request.method
        assert request.headers.get("access_token") == TOKEN
        if self.store is not None and self.store.in_tx:
            self.tx_violations.append(request.url)
        url = urlsplit(request.url)
        query = {k: v[0] for k, v in parse_qs(url.query).items()}
        self.calls.append((url.path, query))
        status, body = self.body(url.path, query)
        return make_response(request, status, json.dumps(body).encode())

    def close(self):
        pass

    def paths(self, pattern: str) -> list[tuple[str, dict]]:
        return [c for c in self.calls if re.search(pattern, c[0])]


# --- store falso -------------------------------------------------------------------------------------


class FakeReceptionTx:
    def __init__(self, store):
        self.s = store

    def lock_receptions(self, company_id, ids):
        self.s.events.append(("lock", tuple(sorted(set(ids)))))
        return self.s.read_receptions(company_id, ids)

    def _row(self, b, sync_run_id, last_source, prev):
        now = self.s.now()
        return {
            "typed": dict(b.typed), "payload": copy.deepcopy(b.payload), "payload_hash": b.payload_hash,
            "content_hash": b.content_hash, "details_hash": b.details_hash, "version_hash": b.version_hash, "details_complete": b.complete,
            "details_count": b.details_count, "details_source": b.details_source, "api_fetched_at": b.api_fetched_at,
            "missing_since": None, "last_source": last_source, "sync_run_id": sync_run_id,
            "first_seen_at": prev["first_seen_at"] if prev else now,
        }

    def upsert_receptions(self, bundles, *, sync_run_id, last_source):
        applied = set()
        for b in bundles:
            key = (b.company_id, b.reception_id)
            prev = self.s.receptions.get(key)
            if prev is not None and prev["api_fetched_at"] > b.api_fetched_at:
                continue
            self.s.receptions[key] = self._row(b, sync_run_id, last_source, prev)
            applied.add(b.reception_id)
        return applied

    def insert_incomplete(self, bundles, *, sync_run_id, last_source):
        inserted = set()
        for b in bundles:
            key = (b.company_id, b.reception_id)
            if key not in self.s.receptions:
                self.s.receptions[key] = self._row(b, sync_run_id, last_source, None)
                inserted.add(b.reception_id)
        return inserted

    def upsert_details(self, bundle, *, sync_run_id, last_source):
        for d in bundle.details or ():
            self.s.details[(bundle.company_id, bundle.reception_id, d.detail_id)] = {
                "variant_id": d.variant_id, "quantity": d.quantity, "cost": d.cost, "payload": copy.deepcopy(d.payload),
                "payload_hash": d.payload_hash, "content_hash": d.content_hash, "missing_since": None,
                "last_source": last_source,
            }

    def mark_details_missing(self, company_id, reception_id, keep_ids):
        marked = []
        for (cid, rid, did), row in self.s.details.items():
            if cid == company_id and rid == reception_id and did not in keep_ids and row["missing_since"] is None:
                row["missing_since"] = self.s.now()
                marked.append(did)
        return sorted(marked)

    def mark_receptions_missing(self, company_id, ids, started_at):
        marked = []
        for rid in ids:
            row = self.s.receptions.get((company_id, rid))
            if row and row["missing_since"] is None and row["api_fetched_at"] <= started_at:
                row["missing_since"] = self.s.now()
                marked.append(rid)
        return sorted(marked)

    def insert_change(self, event, *, sync_run_id):
        self.s.changes.append(event)

    def write_cursor(self, company_id, scope, name, value, *, started_at, sync_run_id):
        self.s.cursors[(company_id, scope, name)] = copy.deepcopy(value)

    def finish_success(self, handle, outcome):
        self.s.runs[handle.run_id].update(status=outcome.status, error=outcome.error, summary=outcome.summary())


class FakeReceptionStore:
    def __init__(self):
        self.receptions: dict = {}
        self.details: dict = {}
        self.changes: list = []
        self.cursors: dict = {}
        self.runs: dict = {}
        self.held: set = set()
        self.events: list = []
        self.in_tx = False
        self.transactions = 0
        self.fail_tx_on: int | None = None
        self.db_clock = TickClock(BASE + timedelta(days=30))

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
        self.events.append("lock_run")
        try:
            yield
        finally:
            self.held.discard(key)

    def start_run(self, *, mode, trigger, host, company_id, resource, scope, state_scope=None):
        run_id = len(self.runs) + 1
        self.runs[run_id] = {"mode": mode, "scope": scope, "state_scope": state_scope, "status": "RUNNING"}
        return RunHandle(run_id=run_id, entity_run_id=run_id, started_at=BASE)

    def finish_failed(self, handle, outcome):
        self.runs[handle.run_id].update(status=outcome.status, error=outcome.error)

    def read_cursor(self, company_id, scope, name):
        value = self.cursors.get((company_id, scope, name))
        return copy.deepcopy(value)

    def _stored(self, rid, row):
        return StoredReception(rid, row["content_hash"], row["details_hash"], row["version_hash"],
                               row["api_fetched_at"], row["missing_since"], row["details_complete"],
                               row["typed"]["admission_date"])

    def read_receptions(self, company_id, ids):
        stored = {rid: self._stored(rid, self.receptions[(company_id, rid)])
                  for rid in ids if (company_id, rid) in self.receptions}
        details: dict = {}
        for (cid, rid, did), row in self.details.items():
            if cid == company_id and rid in ids:
                details.setdefault(rid, {})[did] = StoredDetail(did, row["content_hash"], row["variant_id"],
                                                                row["quantity"], row["cost"], row["missing_since"])
        return stored, details

    def read_day_receptions(self, company_id, day):
        return {rid: self._stored(rid, row) for (cid, rid), row in self.receptions.items()
                if cid == company_id and row["typed"]["admission_date"] == day}

    def read_incomplete_ids(self, company_id, limit):
        return sorted(rid for (cid, rid), row in self.receptions.items()
                      if cid == company_id and not row["details_complete"] and row["missing_since"] is None)[:limit]

    @contextmanager
    def reception_transaction(self):
        self.transactions += 1
        backup = copy.deepcopy((self.receptions, self.details, self.changes, self.cursors, self.runs))
        self.in_tx = True
        try:
            if self.fail_tx_on is not None and self.transactions == self.fail_tx_on:
                raise RuntimeError(f"fallo BD inyectado {TOKEN}")
            yield FakeReceptionTx(self)
        except BaseException:
            self.receptions, self.details, self.changes, self.cursors, self.runs = backup
            raise
        finally:
            self.in_tx = False

    # ayudas de lectura
    def rec(self, rid):
        return self.receptions.get((3, rid))

    def lines_of(self, rid, *, live=True):
        return {did: r for (cid, r_id, did), r in self.details.items()
                if r_id == rid and (not live or r["missing_since"] is None)}

    def kinds(self, rid=None):
        return [e.event_kind for e in self.changes if rid is None or e.reception_id == rid]


_HOURS = itertools.count(1)


def sync(store, adapter, request, *, dry_run=False, env=None, company_id=3):
    clock = TickClock(BASE + timedelta(hours=next(_HOURS) * 10))
    return run_reception_sync(
        store=store, company_id=company_id, request=request, dry_run=dry_run,
        client_factory=client_factory_for(adapter), clock=clock, getenv=(env or ENV).get, host="test",
    )


def dates(d_from=DAY, d_to=None, **kw):
    return ReceptionRequest(mode=re_.MODE_DATES, date_from=d_from, date_to=d_to or d_from, **kw)


def point(*ids, **kw):
    return ReceptionRequest(mode=re_.MODE_POINT, reception_ids=tuple(ids), **kw)


def world(n_lines=(3, 2), day=DAY, **kw):
    store = FakeReceptionStore()
    recs = [reception(100 + i, day) for i in range(len(n_lines))]
    lines = {r["id"]: [line(r["id"], k) for k in range(n)] for r, n in zip(recs, n_lines)}
    return store, ReceptionsBsale(recs, lines, store=store, **kw)


# --- SQL / migración ---------------------------------------------------------------------------------


def test_sql_matches_migration_and_guards():
    tables, _ = parse()
    assert set(RECEPTION_COLUMNS) == set(tables["stock_receptions"].columns)
    assert set(DETAIL_COLUMNS) == set(tables["stock_reception_details"].columns)
    assert "WHERE t.api_fetched_at <= EXCLUDED.api_fetched_at" in UPSERT_RECEPTIONS_SQL
    assert "ON CONFLICT (company_id, bsale_id) DO NOTHING" in INSERT_INCOMPLETE_SQL
    assert "ON CONFLICT (company_id, reception_id, bsale_id)" in UPSERT_DETAILS_SQL
    assert MARK_DETAILS_MISSING_SQL.startswith("UPDATE bsale_raw.stock_reception_details SET missing_since = now()")
    assert "api_fetched_at <= %s" in MARK_RECEPTIONS_MISSING_SQL
    source = open(re_.__file__, encoding="utf-8").read()
    assert not re.search(r"\bDELETE\b|\.post\(|\.put\(|\.patch\(", source)
    assert not re.search(r"\* ?1\.19|iva_|gross|bruto =|average_cost|variant_costs \(", source, re.IGNORECASE)
    assert "admissiondaterange" not in source.replace("``admissiondaterange`` se IGNORA", "")


# --- expand -----------------------------------------------------------------------------------------


def test_expand_complete_stores_lines_without_details_endpoint():
    store, bsale = world((3, 2))
    out = sync(store, bsale, dates())
    assert out.status == "SUCCESS", out.error
    assert not bsale.paths(r"/details\.json$")
    assert out.point["expand_ok"] == 2 and out.point["fallback_used"] == 0
    row = store.rec(100)
    assert row["details_complete"] is True and row["details_source"] == "EXPAND" and row["details_count"] == 3
    listed = bsale.decorate(bsale.receptions[100], {"expand": "[details]"})
    assert row["payload"] == listed  # payload original completo, con su nodo details
    assert row["payload_hash"] == payload_hash(listed)
    assert row["content_hash"] == payload_hash({k: v for k, v in listed.items() if k != "details"})
    assert row["typed"] == {"office_id": 1, "admission_date": DAY, "document_number": "123", "document": "FACTURA",
                            "user_id": USER_ID, "internal_dispatch_id": 0, "admission_date_unix": ts(DAY)}
    lines = store.lines_of(100)
    assert len(lines) == 3
    first = lines[100000]
    assert first["cost"] == Decimal("1000.5") and first["quantity"] == Decimal("2.0") and first["variant_id"] == 5000
    assert first["payload"]["cost"] == 1000.5  # payload original, sin cálculos
    assert store.kinds() == ["CREATED", "CREATED"]
    assert not bsale.tx_violations


def test_truncated_expand_over_50_lines_uses_paginated_details_endpoint():
    store, bsale = world((60, 2))
    out = sync(store, bsale, dates())
    assert out.status == "SUCCESS", out.error
    calls = bsale.paths(r"/receptions/100/details\.json$")
    assert [c[1]["offset"] for c in calls] == ["0", "50"]
    assert len(store.lines_of(100)) == 60 and store.rec(100)["details_source"] == "DETAILS_ENDPOINT"
    assert store.rec(101)["details_source"] == "EXPAND"
    assert out.point["fallback_used"] == 1 and out.point["fallback_ok"] == 1
    assert "truncado (25 de 60)" in out.point["fallback_sample"]["100"]


@pytest.mark.parametrize("mode", ["ignored", "corrupt"])
def test_missing_or_inconsistent_expand_falls_back(mode):
    store, bsale = world((3,), expand=mode)
    out = sync(store, bsale, dates())
    assert out.status == "SUCCESS", out.error
    assert len(store.lines_of(100)) == 3 and store.rec(100)["details_source"] == "DETAILS_ENDPOINT"


def test_detail_failure_never_saves_incomplete_reception_as_complete():
    store, bsale = world((60, 2))
    bsale.details_status[100] = 500
    out = sync(store, bsale, dates())
    assert out.status == "PARTIAL" and "detalle incompleto" in out.error
    row = store.rec(100)
    assert row["details_complete"] is False and row["details_hash"] is None and not store.lines_of(100)
    assert store.rec(101)["details_complete"] is True
    failures = store.cursors[(3, "global", "failures")]["receptions"]
    assert set(failures) == {"100"} and failures["100"]["attempts"] == 1

    bsale.details_status.clear()
    retry = sync(store, bsale, ReceptionRequest(mode=re_.MODE_RETRY, limit=10))
    assert retry.status == "SUCCESS", retry.error
    assert store.rec(100)["details_complete"] is True and len(store.lines_of(100)) == 60
    assert store.cursors[(3, "global", "failures")]["receptions"] == {}
    assert store.kinds(100) == ["CREATED", "MODIFIED"]


def test_detail_failure_keeps_previous_complete_version():
    store, bsale = world((60,))
    sync(store, bsale, dates())
    before = copy.deepcopy((store.rec(100), store.lines_of(100)))
    bsale.lines[100][0]["cost"] = 1.0
    bsale.details_status[100] = 500
    out = sync(store, bsale, dates())
    assert out.status == "PARTIAL" and out.point["deferred"] == 1
    assert (store.rec(100), store.lines_of(100)) == before


def test_duplicate_lines_are_rejected():
    rid = 100
    dup = [line(rid, 0), line(rid, 0)]
    with pytest.raises(re_.ReceptionPayloadError, match="repetida"):
        build_details(rid, dup, 2)
    with pytest.raises(re_.ReceptionPayloadError, match="variant"):
        build_details(rid, [{**line(rid, 0), "variant": None}], 1)
    with pytest.raises(re_.ReceptionPayloadError):
        build_details(rid, [line(rid, 0)], 2)
    assert node_problem({"count": 2, "items": [{}], "offset": 0}) == "details truncado (1 de 2)"
    assert node_problem({"count": 1, "items": [{}], "next": "x"}).startswith("details con paginación")
    assert node_problem({"href": "x"}) == "details no expandido"
    store, bsale = world((2,))
    bsale.lines[100] = dup
    out = sync(store, bsale, dates())
    assert out.status == "PARTIAL" and store.rec(100)["details_complete"] is False


def test_null_and_zero_costs_are_stored_as_is_and_counted():
    store, bsale = world((3,))
    bsale.lines[100][0]["cost"] = 0
    bsale.lines[100][1]["cost"] = None
    out = sync(store, bsale, dates())
    assert out.status == "SUCCESS"
    assert out.point["zero_cost_lines"] == 1 and out.point["null_cost_lines"] == 1
    costs = sorted(str(r["cost"]) for r in store.lines_of(100).values())
    assert costs == ["0", "1002.5", "None"]


# --- día / filtros -----------------------------------------------------------------------------------


def test_day_filter_is_sent_and_validated():
    store, bsale = world((1, 1))
    bsale.add(reception(300, DAY - timedelta(days=1)), [line(300, 0)])
    out = sync(store, bsale, dates(DAY - timedelta(days=1), DAY))
    assert out.status == "SUCCESS", out.error
    listing = bsale.paths(r"/receptions\.json$")
    assert [c[1]["admissiondate"] for c in listing] == [str(ts(DAY - timedelta(days=1))), str(ts(DAY))]
    assert all(c[1]["expand"] == "[details]" for c in listing)
    assert store.rec(300)["typed"]["admission_date"] == DAY - timedelta(days=1)
    assert out.point["days_done"] == ["2026-07-30", "2026-07-31"]


def test_ignored_date_filter_fails_the_day_on_first_page_without_writing():
    store, bsale = world((1, 1), ignore_date_filter=True)
    bsale.add(reception(300, DAY - timedelta(days=9)), [line(300, 0)])
    out = sync(store, bsale, dates())
    assert out.status == "FAILED" and "filtro ignorado" in out.error
    assert len(bsale.paths(r"/receptions\.json$")) == 1
    assert not store.receptions and not store.changes


def test_duplicate_reception_in_day_snapshot_fails_the_day():
    store, bsale = world((1,))
    real_body = bsale.body

    def body(path, query):
        status, data = real_body(path, query)
        if path.endswith("receptions.json"):
            data["items"] = data["items"] * 2
            data["count"] = len(data["items"])
        return status, data

    bsale.body = body
    out = sync(store, bsale, dates())
    assert out.status == "FAILED" and "repetidas" in out.error and not store.receptions


# --- lotes por fechas ---------------------------------------------------------------------------------


FIRST = DAY - timedelta(days=4)


def range_world(days=5):
    """Una recepción (2 líneas) por día, de ``FIRST`` a ``FIRST + days - 1``."""
    store = FakeReceptionStore()
    recs = [reception(1000 + i, FIRST + timedelta(days=i)) for i in range(days)]
    lines = {r["id"]: [line(r["id"], 0), line(r["id"], 1)] for r in recs}
    return store, ReceptionsBsale(recs, lines, store=store)


def test_date_range_writes_one_transaction_per_day_and_reports_resume_point():
    store, bsale = range_world()
    out = sync(store, bsale, dates(FIRST, DAY))
    assert out.status == "SUCCESS", out.error
    assert store.transactions == 5 + 1 and len(store.receptions) == 5  # un día por transacción + cierre de corrida
    assert out.point["resume_from"] is None and "resume_from=" in format_reception_outcome(out)
    assert all("admissiondate" in c[1] for c in bsale.paths(r"/receptions\.json$"))


def test_date_range_interruption_resumes_without_duplicates_or_gaps():
    store, bsale = range_world()
    store.fail_tx_on = 3  # falla la escritura del tercer día
    out = sync(store, bsale, dates(FIRST, DAY))
    assert out.status == "FAILED" and TOKEN not in out.error
    assert out.point["resume_from"] == (FIRST + timedelta(days=2)).isoformat()
    assert {rid for (_, rid) in store.receptions} == {1000, 1001}
    store.fail_tx_on = None
    again = sync(store, bsale, dates(date.fromisoformat(out.point["resume_from"]), DAY))
    assert again.status == "SUCCESS", again.error
    assert len(store.receptions) == 5 and store.kinds().count("CREATED") == 5 and len(store.changes) == 5


def test_rerun_of_a_loaded_range_is_idempotent():
    store, bsale = range_world()
    sync(store, bsale, dates(FIRST, DAY))
    again = sync(store, bsale, dates(FIRST, DAY))
    assert again.status == "SUCCESS" and again.point["unchanged"] == 5 and len(store.changes) == 5


def test_scope_starts_2025_01_01_and_allows_370_days():
    store, bsale = world((1,))
    with pytest.raises(re_.UnsupportedSyncError, match="2025-01-01"):
        sync(store, bsale, dates(date(2024, 12, 31), date(2025, 1, 31)))
    assert not bsale.calls
    re_.validate_request(3, dates(date(2025, 1, 1), date(2026, 1, 5)))  # 370 días
    with pytest.raises(re_.UnsupportedSyncError, match="370"):
        re_.validate_request(3, dates(date(2025, 1, 1), date(2026, 1, 6)))
    assert not hasattr(re_, "MODE_HISTORY")  # sin barrido global del histórico


def test_receptions_outside_the_range_are_never_touched():
    store, bsale = range_world()
    sync(store, bsale, dates(FIRST, DAY))
    old = copy.deepcopy(store.rec(1000))
    bsale.receptions.pop(1000)  # desaparece en Bsale, pero su día no se consulta
    out = sync(store, bsale, dates(FIRST + timedelta(days=1), DAY))
    assert out.rows_missing == 0 and store.rec(1000) == old


def test_variant_stock_change_is_stored_but_never_logged_as_modified():
    store, bsale = world((3,))
    sync(store, bsale, dates())
    before = copy.deepcopy(store.rec(100))
    for item in bsale.lines[100]:
        item["variantStock"] = 999
    out = sync(store, bsale, dates())
    assert out.status == "SUCCESS" and out.point["modified"] == 0 and out.point["unchanged"] == 1
    assert store.kinds() == ["CREATED"]
    row = store.rec(100)
    assert row["version_hash"] == before["version_hash"] and row["content_hash"] == before["content_hash"]
    assert row["payload_hash"] != before["payload_hash"]  # el payload original sí se actualiza
    assert all(r["payload"]["variantStock"] == 999 for r in store.lines_of(100).values())
    bsale.lines[100][0]["cost"] = 1.5
    sync(store, bsale, dates())
    assert store.kinds() == ["CREATED", "MODIFIED"] and store.changes[-1].cost_changed == (100000,)


# --- reconciliación ----------------------------------------------------------------------------------


def test_cost_and_quantity_changes_are_logged():
    store, bsale = world((3,))
    sync(store, bsale, dates())
    bsale.lines[100][0]["cost"] = 777.25
    bsale.lines[100][1]["quantity"] = 99
    out = sync(store, bsale, dates())
    assert out.status == "SUCCESS" and out.point["modified"] == 1
    event = store.changes[-1]
    assert event.event_kind == "MODIFIED" and event.details_changed and not event.header_changed
    assert event.cost_changed == (100000,) and event.quantity_changed == (100001,)
    assert store.lines_of(100)[100000]["cost"] == Decimal("777.25")


def test_header_change_logged_and_unchanged_not_logged():
    store, bsale = world((2,))
    sync(store, bsale, dates())
    sync(store, bsale, dates())
    assert store.kinds() == ["CREATED"]
    bsale.receptions[100]["documentNumber"] = "999"
    out = sync(store, bsale, dates())
    assert out.point["modified"] == 1 and store.changes[-1].header_changed and not store.changes[-1].details_changed
    assert store.rec(100)["typed"]["document_number"] == "999"


def test_disappeared_and_new_lines():
    store, bsale = world((3,))
    sync(store, bsale, dates())
    removed = bsale.lines[100].pop(2)
    bsale.lines[100].append(line(100, 7))
    out = sync(store, bsale, dates())
    assert out.point["details_missing"] == 1
    event = store.changes[-1]
    assert event.removed == (removed["id"],) and event.added == (100007,)
    gone = store.lines_of(100, live=False)[removed["id"]]
    assert gone["missing_since"] is not None  # nunca se borra
    bsale.lines[100].append(removed)
    sync(store, bsale, dates())
    assert store.lines_of(100)[removed["id"]]["missing_since"] is None
    assert store.changes[-1].added == (removed["id"],)


def test_missing_reception_confirmed_by_404_then_reappears():
    store, bsale = world((1, 1, 1, 1, 1, 1))
    sync(store, bsale, dates())
    saved = bsale.receptions.pop(101)
    out = sync(store, bsale, dates())
    assert out.status == "SUCCESS", out.error
    assert bsale.paths(r"/receptions/101\.json$")  # confirmación individual antes de marcar
    assert store.rec(101)["missing_since"] is not None and out.rows_missing == 1
    assert store.kinds(101) == ["CREATED", "MISSING"]
    bsale.receptions[101] = saved
    out = sync(store, bsale, dates())
    assert out.point["reappeared"] == 1 and store.rec(101)["missing_since"] is None
    assert store.kinds(101) == ["CREATED", "MISSING", "REAPPEARED"]


def test_reception_moved_to_another_day_is_updated_not_missing():
    store, bsale = world((1, 1))
    sync(store, bsale, dates())
    bsale.receptions[101]["admissionDate"] = ts(DAY - timedelta(days=3))
    out = sync(store, bsale, dates())
    assert out.rows_missing == 0 and store.rec(101)["missing_since"] is None
    assert store.rec(101)["typed"]["admission_date"] == DAY - timedelta(days=3)


def test_missing_fuse_blocks_marking_and_confirmation():
    store, bsale = world((1, 1, 1, 1, 1))
    sync(store, bsale, dates())
    bsale.receptions.clear()
    bsale.calls.clear()
    out = sync(store, bsale, dates())
    assert out.status == "PARTIAL" and "fusible" in out.error and out.fuse["days"]["2026-07-31"]["tripped"]
    assert all(r["missing_since"] is None for r in store.receptions.values())
    assert not bsale.paths(r"/receptions/\d+\.json$")


def test_stale_guard_keeps_newer_row():
    store, bsale = world((2,))
    sync(store, bsale, dates())
    future = datetime(2100, 1, 1, tzinfo=timezone.utc)
    store.rec(100)["api_fetched_at"] = future
    keep = copy.deepcopy(store.rec(100))
    bsale.lines[100][0]["cost"] = 1.0
    out = sync(store, bsale, dates())
    assert out.rows_skipped_newer == 1 and store.rec(100) == keep
    assert store.lines_of(100)[100000]["cost"] == Decimal("1000.5")


# --- dry-run / locks / confidencialidad / HTTP --------------------------------------------------------


def test_dry_run_reads_but_never_writes_or_locks():
    store, bsale = world((60, 2))
    out = sync(store, bsale, dates(), dry_run=True)
    assert out.status == "SUCCESS" and out.dry_run
    assert out.rows_inserted == 2 and out.point["created"] == 2
    assert store.transactions == 0 and not store.runs and "lock_run" not in store.events
    assert not store.receptions and not store.cursors and not store.changes


def test_lock_busy_is_skipped_without_http():
    store, bsale = world((1,))
    store.held.add((3, "stock_receptions", "global"))
    out = sync(store, bsale, dates())
    assert out.status == "SKIPPED" and not bsale.calls and cli.exit_code(out) == cli.EXIT_LOCKED


def test_note_and_user_never_leave_the_payload(caplog):
    caplog.set_level(logging.DEBUG)
    store, bsale = world((60, 2))
    bsale.details_status[100] = 500
    out = sync(store, bsale, dates())
    texts = [format_reception_outcome(out), json.dumps(out.summary(), default=str), caplog.text,
             json.dumps([e.__dict__ for e in store.changes], default=str)]
    for text in texts:
        assert "NOTA SECRETA" not in text and "Juan" not in text and str(USER_ID) not in text and TOKEN not in text
    assert store.rec(101)["payload"]["note"] == NOTE  # RAW conserva el original


def test_http_failure_on_one_day_is_partial_and_other_days_continue():
    store, bsale = world((1,))
    bsale.add(reception(300, DAY - timedelta(days=1)), [line(300, 0)])
    bsale.day_status[ts(DAY - timedelta(days=1))] = 500
    out = sync(store, bsale, dates(DAY - timedelta(days=1), DAY))
    assert out.status == "PARTIAL" and out.point["days_failed"].keys() == {"2026-07-30"}
    assert store.rec(100) is not None and store.rec(300) is None


def test_http_429_aborts_the_run():
    store, bsale = world((1,))
    bsale.status = 429
    out = sync(store, bsale, dates(DAY - timedelta(days=5), DAY))
    assert out.status == "FAILED" and "429" in out.error
    assert out.http_429 >= re_.MAX_HTTP_429 and len(bsale.calls) <= 2 * 3


def test_request_budget_stops_at_day_boundary_with_resume_point():
    store, bsale = range_world()
    out = sync(store, bsale, dates(FIRST, DAY, max_requests=2))
    assert out.status == "PARTIAL" and "tope de 2 requests" in out.error
    assert out.requests == 2 and len(store.receptions) == 2
    assert out.point["resume_from"] == (FIRST + timedelta(days=2)).isoformat()


def test_point_refresh_and_404():
    store, bsale = world((2, 1))
    sync(store, bsale, dates())
    run_locks = store.events.count("lock_run")
    del bsale.receptions[101]
    bsale.lines[100][0]["cost"] = 5
    out = sync(store, bsale, point(100, 101, 555))
    assert out.status == "SUCCESS", out.error
    assert out.point["not_found"] == [101, 555] and store.rec(101)["missing_since"] is not None
    assert store.changes[-2].event_kind in ("MODIFIED", "MISSING") and out.point["modified"] == 1
    assert store.events.count("lock_run") == run_locks  # POINT no toma el lock de corrida
    assert ("lock", (100,)) in store.events  # pero bloquea cada recepción en su transacción


def test_only_company_3_and_valid_requests():
    store, bsale = world((1,))
    with pytest.raises(re_.UnsupportedSyncError):
        sync(store, bsale, dates(), company_id=1)
    with pytest.raises(re_.UnsupportedSyncError):
        sync(store, bsale, dates(DAY, DAY - timedelta(days=1)))
    with pytest.raises(re_.UnsupportedSyncError):
        sync(store, bsale, ReceptionRequest(mode="history"))
    assert not bsale.calls


# --- CLI ---------------------------------------------------------------------------------------------


def run_cli(argv):
    seen = {}

    def runner(**kw):
        seen.update(kw)
        from backend.services.bsale_raw.core.store import EntityOutcome

        return EntityOutcome(company_id=3, resource="stock_receptions", scope="x", mode="SCANNER", status="PARTIAL",
                             point={"modality": kw["request"].mode})

    out, err = io.StringIO(), io.StringIO()
    code = cli.main(argv, reception_runner=runner, out=out, err=err)
    return code, seen, out.getvalue(), err.getvalue()


@pytest.mark.parametrize("argv,mode", [
    (["--date", "2026-07-31"], "dates"),
    (["--from", "2026-01-01", "--to", "2026-10-13"], "dates"),
    (["--from", "2025-01-01", "--to", "2025-12-31"], "dates"),
    (["--reception", "10", "--reception", "11"], "point"),
    (["--retry-failures", "--limit", "5"], "retry"),
])
def test_cli_dispatch(argv, mode):
    code, seen, out, _ = run_cli(["sync-receptions", "--company", "3", *argv, "--dry-run"])
    assert code == cli.EXIT_PARTIAL and seen["request"].mode == mode and seen["dry_run"] is True
    assert "status=PARTIAL" in out


@pytest.mark.parametrize("argv", [
    ["--company", "1", "--date", "2026-07-31"],
    ["--company", "3"],
    ["--company", "3", "--date", "31-07-2026"],
    ["--company", "3", "--from", "2026-07-01"],
    ["--company", "3", "--date", "2026-07-31", "--limit", "3"],
    ["--company", "3", "--from", "2025-01-01", "--to", "2026-07-31"],
    ["--company", "3", "--from", "2024-12-01", "--to", "2025-01-31"],
    ["--company", "3", "--date", "2013-05-02"],
    ["--company", "3", "--reception", "-4"],
])
def test_cli_usage_errors(argv):
    code, seen, _, err = run_cli(["sync-receptions", *argv])
    assert code == cli.EXIT_USAGE and not seen and "uso inválido" in err


@pytest.mark.parametrize("argv", [["--history"], ["--history", "--pages", "20"], ["--date", "2026-07-31", "--pages", "3"]])
def test_cli_has_no_global_history_mode(argv):
    code, seen, _, _ = run_cli(["sync-receptions", "--company", "3", *argv])
    assert code == cli.EXIT_USAGE and not seen
