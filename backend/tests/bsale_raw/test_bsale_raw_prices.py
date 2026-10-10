"""Motor de precios ``bsale_raw.variant_prices`` (sync-prices / refresh-prices). Sin red ni BD real.

HTTP: stack real (``BsaleHttpClient`` + ``RateLimitedSession``) sobre un adapter falso de
``/price_lists/{id}/details.json``. BD: ``FakePriceStore`` en memoria con la semántica de la SQL de
``PgPriceStore`` (frescura por fila, ``missing_since``, runs, rollback); la SQL real se valida aparte.
"""

from __future__ import annotations

import copy
import io
import json
import logging
import re
from contextlib import contextmanager
from datetime import timedelta
from decimal import Decimal
from urllib.parse import parse_qs, urlsplit

import pytest
import requests
from requests.adapters import BaseAdapter

from backend.jobs.bsale_raw import cli
from backend.services.bsale_raw.core import price_engine
from backend.services.bsale_raw.core.engine import UnsupportedSyncError
from backend.services.bsale_raw.core.models import SyncMode, payload_hash
from backend.services.bsale_raw.core.price_engine import (
    MARK_PRICES_MISSING_SQL,
    PRICE_COLUMNS,
    UPSERT_PRICES_SQL,
    ListSelection,
    PgPriceStore,
    PgPriceTx,
    PriceListInfo,
    PriceSyncReport,
    build_price_rows,
    format_price_point,
    format_price_report,
    price_rate_config,
    refresh_prices,
    select_lists,
    sync_prices,
)
from backend.services.bsale_raw.core.rate_limit import RequestPriority
from backend.services.bsale_raw.core.reconcile import ExistingRow
from backend.services.bsale_raw.core.snapshot import FetchedItem, Snapshot, SnapshotValidationError
from backend.services.bsale_raw.core.store import EntityOutcome, LockBusyError, RunHandle, SourceConfig, SourceConfigError
from backend.tests.bsale_raw.test_bsale_raw_pipeline import (
    BASE,
    ENV,
    TOKEN,
    FakeConnection,
    TickClock,
    client_factory_for,
    make_response,
)

DETAILS_RE = re.compile(r"/v1/price_lists/(\d+)/details\.json$")
OLD = BASE - timedelta(days=1)
FUTURE = BASE + timedelta(days=1)


def detail(detail_id: int, variant_id: int, value=1260.5042, with_taxes=1500) -> dict:
    return {
        "href": f"https://api.bsale.io/v1/price_lists/12/details/{detail_id}.json",
        "id": detail_id,
        "variantValue": value,
        "variantValueWithTaxes": with_taxes,
        "variant": {"href": f"https://api.bsale.io/v1/variants/{variant_id}.json", "id": str(variant_id)},
    }


def details_for(list_id: int, variants) -> list[dict]:
    return [detail(list_id * 100000 + v, v) for v in variants]


class PricesBsale(BaseAdapter):
    """``lists[id]``: detalles de la lista. ``errors[id]``: (status, body) o Exception para toda request."""

    def __init__(self, lists=None, *, errors=None, drift=(), truncate=(), ignore_filter=(), store=None):
        super().__init__()
        self.lists = {k: list(v) for k, v in (lists or {}).items()}
        self.errors = dict(errors or {})
        self.drift = set(drift)
        self.truncate = set(truncate)
        self.ignore_filter = set(ignore_filter)
        self.store = store
        self.calls: list[tuple[int, dict]] = []
        self.tx_violations: list[str] = []

    def send(self, request, **kwargs):
        if self.store is not None and self.store.in_tx:
            self.tx_violations.append(request.url)
        parts = urlsplit(request.url)
        match = DETAILS_RE.search(parts.path)
        assert match, request.url
        assert request.headers.get("access_token") == TOKEN
        list_id = int(match.group(1))
        q = {k: v[0] for k, v in parse_qs(parts.query).items()}
        self.calls.append((list_id, q))
        if list_id in self.errors:
            action = self.errors[list_id]
            if isinstance(action, BaseException):
                raise action
            status, body = action
            return make_response(request, status, json.dumps(body).encode())
        items = self.lists.get(list_id, [])
        if "variantid" in q and list_id not in self.ignore_filter:
            items = [d for d in items if int(d["variant"]["id"]) == int(q["variantid"])]
        offset, limit = int(q["offset"]), int(q["limit"])
        count = len(items)
        list_calls = sum(1 for lid, _ in self.calls if lid == list_id)
        if list_id in self.drift:
            count += list_calls - 1
        page = items[offset: offset + limit]
        if list_id in self.truncate and offset > 0:
            page = []
        body = {"href": request.url, "count": count, "limit": limit, "offset": offset, "items": page}
        return make_response(request, 200, json.dumps(body).encode())

    def close(self):
        pass


def factory_for(adapter, priorities=None):
    base = client_factory_for(adapter)

    def factory(source, token, spec):
        if priorities is not None:
            priorities.append(spec.request_priority)
        return base(source, token, spec)

    return factory


class FakePriceTx:
    def __init__(self, store):
        self.s = store

    def read_existing_prices(self, company_id, price_list_id):
        return {
            v: ExistingRow(bsale_id=v, payload_hash=r["payload_hash"], api_fetched_at=r["api_fetched_at"],
                           missing_since=r["missing_since"])
            for (c, pl, v), r in self.s.prices.items() if c == company_id and pl == price_list_id
        }

    def read_existing_price_pairs(self, company_id, variant_ids):
        wanted = set(variant_ids)
        return {
            (pl, v): ExistingRow(bsale_id=v, payload_hash=r["payload_hash"], api_fetched_at=r["api_fetched_at"],
                                 missing_since=r["missing_since"])
            for (c, pl, v), r in self.s.prices.items() if c == company_id and v in wanted
        }

    def upsert_prices(self, rows, *, sync_run_id, last_source):
        if self.s.fail_upsert:
            raise RuntimeError(f"fallo BD con {TOKEN}")
        applied = set()
        for r in rows:
            key = (r.company_id, r.price_list_id, r.variant_id)
            prev = self.s.prices.get(key)
            if prev is not None and prev["api_fetched_at"] > r.api_fetched_at:
                continue
            self.s.prices[key] = {
                "bsale_detail_id": r.bsale_detail_id, "variant_value": r.variant_value,
                "variant_value_with_taxes": r.variant_value_with_taxes, "payload": copy.deepcopy(r.payload),
                "payload_hash": r.payload_hash, "api_fetched_at": r.api_fetched_at, "missing_since": None,
                "first_seen_at": prev.get("first_seen_at") if prev else r.api_fetched_at,
                "last_source": last_source, "sync_run_id": sync_run_id,
            }
            applied.add((r.price_list_id, r.variant_id))
        return applied

    def mark_prices_missing(self, company_id, price_list_id, variant_ids, snapshot_started_at):
        marked = 0
        for v in variant_ids:
            row = self.s.prices.get((company_id, price_list_id, v))
            if row and row["missing_since"] is None and row["api_fetched_at"] <= snapshot_started_at:
                row["missing_since"] = BASE
                marked += 1
        return marked

    def finish_success(self, handle, outcome):
        self.s.runs[handle.run_id].update(status=outcome.status, summary=outcome.summary())
        key = (outcome.company_id, outcome.resource, outcome.sync_state_scope)
        self.s.state[key] = {"status": outcome.status, "last_success_at": BASE}


class FakePriceStore:
    def __init__(self, lists=None, *, catalog=None):
        self.lists = list(lists if lists is not None else [
            PriceListInfo(3, "QUILLOTANA V", 1),
            PriceListInfo(12, "SUPERMERCADO LA QUILLOTANA", 0),
            PriceListInfo(13, "RUTA/WEB (BOLETA O FACTURA)", 0),
            PriceListInfo(16, "MELINKA", 0),
        ])
        self.catalog = set(catalog if catalog is not None else range(1, 1000))
        self.prices: dict = {}
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
        if company_id != 3:
            raise SourceConfigError(f"company_id={company_id} no existe en bsale_raw.sources")
        return SourceConfig(company_id=3, cpn_id=21884, name="SPA", token_env="BSALE_TOKEN_SPA")

    @contextmanager
    def advisory_lock(self, company_id, resource, scope):
        key = (company_id, resource, scope)
        if key in self.busy or key in self.locks:
            raise LockBusyError(f"lock ocupado company_id={company_id} resource={resource} scope={scope}")
        self.locks.add(key)
        self.events.append(f"lock:{scope}")
        try:
            yield
        finally:
            self.locks.discard(key)

    def start_run(self, *, mode, trigger, host, company_id, resource, scope, state_scope=None):
        run_id = len(self.runs) + 1
        self.runs[run_id] = {"mode": mode, "resource": resource, "scope": scope, "state_scope": state_scope,
                             "status": "RUNNING"}
        return RunHandle(run_id=run_id, entity_run_id=run_id, started_at=BASE)

    def finish_failed(self, handle, outcome):
        self.runs[handle.run_id].update(status=outcome.status, error=outcome.error, summary=outcome.summary())
        key = (outcome.company_id, outcome.resource, outcome.sync_state_scope)
        prev = self.state.get(key, {})
        self.state[key] = {"status": outcome.status, "last_success_at": prev.get("last_success_at")}

    def read_price_lists(self, company_id):
        return list(self.lists)

    def read_catalogued_variants(self, company_id, variant_ids):
        return {v for v in variant_ids if v in self.catalog}

    def read_existing_prices(self, company_id, price_list_id):
        return FakePriceTx(self).read_existing_prices(company_id, price_list_id)

    def read_existing_price_pairs(self, company_id, variant_ids):
        return FakePriceTx(self).read_existing_price_pairs(company_id, variant_ids)

    def read_last_success(self, company_id, resource, scopes):
        return {s: self.state[(company_id, resource, s)]["last_success_at"]
                for s in scopes if (company_id, resource, s) in self.state}

    @contextmanager
    def price_transaction(self):
        snapshot = (copy.deepcopy(self.prices), copy.deepcopy(self.runs), copy.deepcopy(self.state))
        self.in_tx = True
        try:
            yield FakePriceTx(self)
        except BaseException:
            self.prices, self.runs, self.state = snapshot
            raise
        finally:
            self.in_tx = False

    def price(self, list_id, variant_id):
        return self.prices.get((3, list_id, variant_id))


def metadata_ok(new_lists=None, status="SUCCESS", error=None):
    """Simula ``run_entity_sync(price_lists)``: actualiza las listas guardadas sólo si no es dry-run."""
    calls = []

    def runner(*, store, company_id, dry_run, **kw):
        calls.append({"company_id": company_id, "dry_run": dry_run, **kw})
        if new_lists is not None and not dry_run and status == "SUCCESS":
            store.lists = list(new_lists)
        out = EntityOutcome(company_id=company_id, resource="price_lists", scope="global", mode="FULL_RECONCILE",
                            dry_run=dry_run, status=status, error=error, rows_received=len(store.lists))
        if not dry_run and status == "SUCCESS":
            store.state[(company_id, "price_lists", "global")] = {"status": status, "last_success_at": BASE}
        return out

    runner.calls = calls
    return runner


def run_sync(store, adapter, *, metadata=None, env=None, priorities=None, **kw):
    kw.setdefault("clock", TickClock(BASE))
    return sync_prices(
        store=store, company_id=kw.pop("company_id", 3), client_factory=factory_for(adapter, priorities),
        metadata_runner=metadata or metadata_ok(), getenv=(env or ENV).get, host="test", **kw,
    )


def run_point(store, adapter, variant_ids, *, priorities=None, **kw):
    kw.setdefault("clock", TickClock(BASE))
    return refresh_prices(
        store=store, company_id=3, variant_ids=variant_ids, client_factory=factory_for(adapter, priorities),
        getenv=kw.pop("env", ENV).get, host="test", **kw,
    )


def world(lists=None, **adapter_kw):
    store = FakePriceStore()
    data = lists if lists is not None else {12: details_for(12, [1, 2, 3]), 13: details_for(13, [1, 2, 3]),
                                            16: details_for(16, [1, 2, 3])}
    return store, PricesBsale(data, store=store, **adapter_kw)


def result(report, list_id):
    return next(r for r in report.lists if r.price_list_id == list_id)


# --- filas: valores originales de Bsale -----------------------------------------------------------


def _snap(items):
    return Snapshot(items=[FetchedItem(i, BASE) for i in items], api_count=len(items), pages=1)


def test_row_keeps_original_values_exact_and_full_payload():
    body = detail(7, 10, value=1260.5042, with_taxes="1500.0001")
    rows = build_price_rows(3, 12, _snap([copy.deepcopy(body)]))
    row = rows[0]
    assert (row.price_list_id, row.variant_id, row.bsale_detail_id) == (12, 10, 7)
    assert row.variant_value == Decimal("1260.5042") and str(row.variant_value) == "1260.5042"
    assert row.variant_value_with_taxes == Decimal("1500.0001")
    assert row.payload == body and row.payload_hash == payload_hash(body)


def test_row_zero_and_null_prices_kept_as_is():
    zero = build_price_rows(3, 12, _snap([detail(1, 10, value=0, with_taxes=0)]))[0]
    assert zero.variant_value == Decimal("0") and zero.variant_value_with_taxes == Decimal("0")
    null = build_price_rows(3, 12, _snap([detail(1, 10, value=None, with_taxes=None)]))[0]
    assert null.variant_value is None and null.variant_value_with_taxes is None


def test_row_has_no_derived_tax_or_margin_fields():
    fields = set(price_engine.PriceRow.__dataclass_fields__)
    assert fields == {"company_id", "price_list_id", "variant_id", "bsale_detail_id", "variant_value",
                      "variant_value_with_taxes", "payload", "payload_hash", "api_fetched_at"}
    assert not any(w in c for c in PRICE_COLUMNS for w in ("gross", "net", "iva", "margin", "factor"))


@pytest.mark.parametrize("mutate, message", [
    (lambda d: d.pop("variantValue"), "sin variantValue"),
    (lambda d: d.pop("variant"), "sin variant.id"),
    (lambda d: d.pop("id"), "sin id"),
    (lambda d: d.update(variantValue="mil"), "no numérico"),
    (lambda d: d.update(variant="7"), "inválido"),
])
def test_invalid_detail_invalidates_snapshot(mutate, message):
    body = detail(1, 10)
    mutate(body)
    with pytest.raises(SnapshotValidationError, match=message):
        build_price_rows(3, 12, _snap([body]))


def test_duplicate_variant_in_list_rejected_not_overwritten():
    with pytest.raises(SnapshotValidationError, match="más de un detalle"):
        build_price_rows(3, 12, _snap([detail(1, 10, value=100), detail(2, 10, value=200)]))


def test_duplicate_detail_id_rejected():
    with pytest.raises(SnapshotValidationError, match="ids de detalle duplicados"):
        build_price_rows(3, 12, _snap([detail(1, 10), detail(1, 11)]))


# --- sync-prices: listas ---------------------------------------------------------------------------


def test_active_lists_synced_dynamically_inactive_untouched():
    store, adapter = world()
    report = run_sync(store, adapter)
    assert report.status == "SUCCESS", report.error
    assert [r.price_list_id for r in report.lists] == [12, 13, 16]
    assert {lid for lid, _ in adapter.calls} == {12, 13, 16}
    for list_id in (12, 13, 16):
        r = result(report, list_id)
        assert r.outcome.rows_inserted == 3 and r.outcome.status == "SUCCESS" and r.active
        assert store.price(list_id, 2)["variant_value"] == Decimal("1260.5042")
    assert not any(pl == 3 for _, pl, _ in store.prices)


def test_lists_not_hardcoded():
    store, adapter = world({7: details_for(7, [1]), 12: details_for(12, [1])})
    store.lists = [PriceListInfo(7, "OTRA", 0), PriceListInfo(12, "SUPER", 1)]
    report = run_sync(store, adapter)
    assert [r.price_list_id for r in report.lists] == [7]
    assert report.status == "SUCCESS"


def test_inactive_lists_on_demand_only():
    store, adapter = world({3: details_for(3, [1, 2]), 12: details_for(12, [1])})
    report = run_sync(store, adapter, selection="inactive")
    assert [r.price_list_id for r in report.lists] == [3]
    assert report.status == "SUCCESS" and store.price(3, 1) is not None
    assert result(report, 3).active is False and result(report, 3).state == 1
    assert not any(pl == 12 for _, pl, _ in store.prices)


def test_inactive_list_requested_without_flag_is_reported():
    store, adapter = world()
    report = run_sync(store, adapter, price_list_ids=[3])
    assert report.status == "FAILED"
    assert "fuera de --lists active" in result(report, 3).outcome.error
    assert adapter.calls == []


def test_inaccessible_inactive_list_does_not_block_active_ones():
    store, adapter = world({12: details_for(12, [1]), 13: details_for(13, [1]), 16: details_for(16, [1])},
                           errors={3: (404, {"error": "not found"})})
    report = run_sync(store, adapter, selection="all")
    assert report.status == "PARTIAL"
    assert result(report, 3).outcome.status == "FAILED" and "404" in result(report, 3).outcome.error
    assert all(result(report, lid).outcome.status == "SUCCESS" for lid in (12, 13, 16))


def test_new_list_detected_and_synced():
    store, adapter = world({**{lid: details_for(lid, [1]) for lid in (12, 13, 16)}, 20: details_for(20, [5])})
    new = [*store.lists, PriceListInfo(20, "NUEVA", 0)]
    report = run_sync(store, adapter, metadata=metadata_ok(new))
    assert report.new_lists == [20]
    assert result(report, 20).outcome.status == "SUCCESS" and store.price(20, 5) is not None


def test_deactivated_list_detected_not_synced_prices_kept():
    store, adapter = world()
    store.prices[(3, 13, 1)] = {"payload_hash": "h", "api_fetched_at": OLD, "missing_since": None}
    new = [PriceListInfo(3, "QV", 1), PriceListInfo(12, "S", 0), PriceListInfo(13, "R", 1), PriceListInfo(16, "M", 0)]
    report = run_sync(store, adapter, metadata=metadata_ok(new))
    assert report.deactivated_lists == [13]
    assert [r.price_list_id for r in report.lists] == [12, 16]
    assert store.price(13, 1)["missing_since"] is None


def test_removed_and_reactivated_lists_reported():
    store, adapter = world({12: details_for(12, [1]), 3: details_for(3, [1])})
    new = [PriceListInfo(3, "QV", 0), PriceListInfo(12, "S", 0), PriceListInfo(13, "R", 0, missing_since=BASE),
           PriceListInfo(16, "M", 0, missing_since=BASE)]
    report = run_sync(store, adapter, metadata=metadata_ok(new))
    assert report.reactivated_lists == [3] and report.removed_lists == [13, 16]
    assert [r.price_list_id for r in report.lists] == [3, 12]


def test_select_lists_rules():
    lists = [PriceListInfo(1, "a", 0), PriceListInfo(2, "b", 1), PriceListInfo(3, "c", 0, missing_since=BASE),
             PriceListInfo(4, "d", None)]
    assert [p.bsale_id for p in select_lists(lists, ListSelection.ACTIVE, None)[0]] == [1]
    assert [p.bsale_id for p in select_lists(lists, ListSelection.INACTIVE, None)[0]] == [2, 4]
    assert [p.bsale_id for p in select_lists(lists, ListSelection.ALL, None)[0]] == [1, 2, 4]
    chosen, unavailable = select_lists(lists, ListSelection.ALL, [1, 3, 9])
    assert [p.bsale_id for p in chosen] == [1]
    assert dict(unavailable) == {3: "ausente en Bsale (price_lists.missing_since)",
                                 9: "no existe en bsale_raw.price_lists"}


def test_metadata_failure_is_explicit_and_caps_status_at_partial():
    store, adapter = world()
    store.state[(3, "price_lists", "global")] = {"status": "SUCCESS", "last_success_at": OLD}
    report = run_sync(store, adapter, metadata=metadata_ok(status="FAILED", error="HTTP 503"))
    assert report.status == "PARTIAL"
    assert all(r.outcome.status == "SUCCESS" for r in report.lists)
    assert "metadata price_lists FAILED" in report.error and "listas guardadas" in report.error
    assert OLD.isoformat() in report.error


def test_metadata_refreshed_with_entity_engine(monkeypatch):
    seen = {}

    def fake_entity_sync(**kw):
        seen.update(kw)
        return EntityOutcome(company_id=3, resource="price_lists", scope="global", mode="FULL_RECONCILE",
                             status="SUCCESS")

    monkeypatch.setattr(price_engine, "run_entity_sync", fake_entity_sync)
    store, adapter = world()
    report = sync_prices(store=store, company_id=3, client_factory=factory_for(adapter), getenv=ENV.get,
                         clock=TickClock(BASE), host="test")
    assert report.status == "SUCCESS"
    assert seen["resource"] == "price_lists" and seen["mode"] is SyncMode.FULL_RECONCILE
    assert seen["store"] is store and seen["dry_run"] is False


def test_no_lists_is_failed():
    store, adapter = world()
    store.lists = [PriceListInfo(3, "QV", 1)]
    report = run_sync(store, adapter)
    assert report.status == "FAILED" and "sin listas" in report.error


# --- snapshot por lista ----------------------------------------------------------------------------


def test_pagination_reads_every_page():
    variants = list(range(1, 121))
    store, adapter = world({12: details_for(12, variants), 13: [], 16: []})
    report = run_sync(store, adapter)
    out = result(report, 12).outcome
    assert out.pages == 3 and out.api_count == 120 and out.rows_inserted == 120 and out.requests == 3
    assert [int(q["offset"]) for lid, q in adapter.calls if lid == 12] == [0, 50, 100]
    assert all(q["limit"] == "50" for _, q in adapter.calls)


def test_count_drift_fails_only_that_list_without_writes():
    store, adapter = world({12: details_for(12, range(1, 80)), 13: details_for(13, [1]), 16: details_for(16, [1])},
                           drift={12})
    report = run_sync(store, adapter)
    assert report.status == "PARTIAL"
    assert result(report, 12).outcome.status == "FAILED" and "count cambió" in result(report, 12).outcome.error
    assert not any(pl == 12 for _, pl, _ in store.prices)
    assert store.price(13, 1) and store.price(16, 1)


def test_duplicate_details_fail_list_and_keep_previous_prices():
    store, adapter = world({12: [detail(1, 10, value=100), detail(2, 10, value=200)], 13: [], 16: []})
    store.prices[(3, 12, 10)] = {"variant_value": Decimal("50"), "payload_hash": "h", "api_fetched_at": OLD,
                                 "missing_since": None}
    report = run_sync(store, adapter)
    assert result(report, 12).outcome.status == "FAILED"
    assert "más de un detalle" in result(report, 12).outcome.error
    assert store.price(12, 10)["variant_value"] == Decimal("50")


def test_uncatalogued_variant_saved_and_reported():
    store, adapter = world({12: details_for(12, [1, 5000]), 13: [], 16: []})
    report = run_sync(store, adapter)
    out = result(report, 12).outcome
    assert out.status == "SUCCESS" and store.price(12, 5000) is not None
    assert out.point["uncatalogued"] == 1 and out.point["uncatalogued_sample"] == [5000]
    assert "uncatalogued=1" in format_price_report(report)


def test_zero_price_and_exact_decimals_persisted():
    store, adapter = world({12: [detail(1, 10, value=0, with_taxes=0), detail(2, 11, value=0.0001, with_taxes=1)],
                            13: [], 16: []})
    run_sync(store, adapter)
    assert store.price(12, 10)["variant_value"] == Decimal("0")
    assert store.price(12, 11)["variant_value"] == Decimal("0.0001")


def test_payload_stored_exactly_as_received():
    body = detail(1, 10)
    body["extra"] = {"nested": [1, "x"]}
    store, adapter = world({12: [body], 13: [], 16: []})
    run_sync(store, adapter)
    assert store.price(12, 10)["payload"] == body
    assert store.price(12, 10)["bsale_detail_id"] == 1


def test_stale_guard_never_overwrites_newer_row():
    store, adapter = world({12: [detail(1, 10, value=999)], 13: [], 16: []})
    store.prices[(3, 12, 10)] = {"variant_value": Decimal("5"), "payload_hash": "h", "api_fetched_at": FUTURE,
                                 "missing_since": None}
    report = run_sync(store, adapter)
    assert result(report, 12).outcome.rows_skipped_newer == 1
    assert store.price(12, 10)["variant_value"] == Decimal("5")


def test_idempotent_second_run_unchanged():
    store, adapter = world()
    run_sync(store, adapter)
    report = run_sync(store, adapter, clock=TickClock(BASE + timedelta(hours=1)))
    assert all(r.outcome.rows_unchanged == 3 and r.outcome.rows_inserted == 0 for r in report.lists)


# --- ausencias: missing_since, reaparición, fusible ----------------------------------------------------


def _seed(store, list_id, variants, fetched=OLD, missing=None):
    for v in variants:
        store.prices[(3, list_id, v)] = {"variant_value": Decimal("1"), "payload_hash": f"h{v}",
                                         "api_fetched_at": fetched, "missing_since": missing}


def test_absent_price_marked_missing_never_deleted():
    store, adapter = world({12: details_for(12, range(1, 10)), 13: [], 16: []})
    _seed(store, 12, range(1, 11))
    report = run_sync(store, adapter)
    out = result(report, 12).outcome
    assert out.status == "SUCCESS" and out.rows_missing == 1 and out.rows_deleted == 0
    assert store.price(12, 10)["missing_since"] == BASE
    assert len([k for k in store.prices if k[1] == 12]) == 10


def test_reappearing_price_clears_missing_since():
    store, adapter = world({12: details_for(12, [1]), 13: [], 16: []})
    _seed(store, 12, [1], missing=OLD)
    run_sync(store, adapter)
    assert store.price(12, 1)["missing_since"] is None


def test_price_refreshed_after_snapshot_start_never_marked_missing():
    store, adapter = world({12: details_for(12, range(1, 10)), 13: [], 16: []})
    _seed(store, 12, range(1, 10))
    _seed(store, 12, [10], fetched=FUTURE)  # POINT posterior al inicio del snapshot
    report = run_sync(store, adapter)
    out = result(report, 12).outcome
    assert out.rows_missing == 0 and out.fuse["protected_newer"] == 1
    assert store.price(12, 10)["missing_since"] is None


def test_empty_snapshot_with_known_prices_trips_fuse():
    store, adapter = world({12: [], 13: [], 16: []})
    _seed(store, 12, [1, 2])
    report = run_sync(store, adapter)
    out = result(report, 12).outcome
    assert out.status == "FAILED" and "snapshot vacío" in out.error
    assert all(store.price(12, v)["missing_since"] is None for v in (1, 2))


def test_empty_snapshot_on_empty_list_succeeds():
    store, adapter = world({12: [], 13: [], 16: []})
    report = run_sync(store, adapter)
    assert report.status == "SUCCESS" and all(r.outcome.rows_received == 0 for r in report.lists)


def test_missing_fuse_blocks_all_writes_of_list():
    store, adapter = world({12: details_for(12, range(1, 8)), 13: [], 16: []})
    _seed(store, 12, range(1, 11))  # 3/10 = 30 % > 20 %
    report = run_sync(store, adapter)
    out = result(report, 12).outcome
    assert out.status == "FAILED" and "fusible" in out.error
    assert all(store.price(12, v)["missing_since"] is None for v in range(1, 11))
    assert all(store.price(12, v)["payload_hash"] == f"h{v}" for v in range(1, 8))


def test_missing_fuse_threshold_configurable():
    store, adapter = world({12: details_for(12, range(1, 8)), 13: [], 16: []})
    _seed(store, 12, range(1, 11))
    report = run_sync(store, adapter, env={**ENV, "BSALE_RAW_MAX_MISSING_PCT_VARIANT_PRICES": "50"})
    assert result(report, 12).outcome.rows_missing == 3
    bad = run_sync(store, adapter, env={**ENV, "BSALE_RAW_MAX_MISSING_PCT_VARIANT_PRICES": "150"})
    assert bad.status == "FAILED" and "entre 0 y 100" in bad.error


def test_http_failure_never_marks_missing():
    store, adapter = world({13: [], 16: []}, errors={12: (500, {"error": "boom"})})
    _seed(store, 12, [1, 2])
    report = run_sync(store, adapter)
    assert result(report, 12).outcome.status == "FAILED"
    assert all(store.price(12, v)["missing_since"] is None for v in (1, 2))


def test_truncated_snapshot_never_marks_missing():
    store, adapter = world({12: details_for(12, range(1, 80)), 13: [], 16: []}, truncate={12})
    _seed(store, 12, range(1, 80))
    report = run_sync(store, adapter)
    assert "truncada" in result(report, 12).outcome.error
    assert all(store.price(12, v)["missing_since"] is None for v in range(1, 80))


# --- errores y aislamiento -------------------------------------------------------------------------


def test_one_failing_list_keeps_others_and_its_own_data():
    store, adapter = world({12: details_for(12, [1]), 16: details_for(16, [1])}, errors={13: (500, {"e": 1})})
    _seed(store, 13, [1, 2])
    report = run_sync(store, adapter)
    assert report.status == "PARTIAL" and cli.price_exit_code(report) == 2
    assert result(report, 13).outcome.status == "FAILED" and result(report, 13).outcome.http_5xx >= 1
    assert result(report, 12).outcome.status == result(report, 16).outcome.status == "SUCCESS"
    assert store.price(13, 1)["payload_hash"] == "h1"
    assert "listas sin sincronizar: [13]" in report.error


def test_all_lists_failing_is_failed():
    store, adapter = world(errors={12: (500, {}), 13: (500, {}), 16: (500, {})})
    report = run_sync(store, adapter)
    assert report.status == "FAILED" and cli.price_exit_code(report) == 1


def test_db_failure_rolls_back_list_and_records_failure():
    store, adapter = world()
    store.fail_upsert = True
    report = run_sync(store, adapter)
    assert report.status == "FAILED" and store.prices == {}
    assert all(r.outcome.rows_inserted == 0 for r in report.lists)
    assert all(run["status"] == "FAILED" for run in store.runs.values())
    assert TOKEN not in format_price_report(report)


def test_runs_and_state_per_list_with_last_success():
    store, adapter = world()
    store.state[(3, "variant_prices", "price_list:13")] = {"status": "SUCCESS", "last_success_at": OLD}
    adapter.errors[13] = (500, {})
    report = run_sync(store, adapter)
    assert {r["scope"] for r in store.runs.values()} == {"price_list:12", "price_list:13", "price_list:16"}
    assert all(r["mode"] == "FULL_RECONCILE" and r["resource"] == "variant_prices" for r in store.runs.values())
    assert result(report, 12).last_success_at == BASE
    assert result(report, 13).last_success_at == OLD
    assert store.state[(3, "variant_prices", "price_list:13")]["status"] == "FAILED"


# --- concurrencia ----------------------------------------------------------------------------------


def test_company_lock_busy_skips_without_http_or_writes():
    store, adapter = world()
    store.busy.add((3, "variant_prices", "sync-prices"))
    meta = metadata_ok()
    report = run_sync(store, adapter, metadata=meta)
    assert report.status == "SKIPPED" and cli.price_exit_code(report) == 3
    assert adapter.calls == [] and meta.calls == [] and store.runs == {}


def test_list_lock_busy_skips_that_list_only():
    store, adapter = world()
    store.busy.add((3, "variant_prices", "price_list:13"))
    report = run_sync(store, adapter)
    assert result(report, 13).outcome.status == "SKIPPED"
    assert report.status == "PARTIAL"
    assert not any(lid == 13 for lid, _ in adapter.calls)


def test_locks_order_and_no_http_inside_transaction():
    store, adapter = world()
    run_sync(store, adapter)
    assert store.events[0] == "lock:sync-prices"
    assert store.events[1:] == ["lock:price_list:12", "lock:price_list:13", "lock:price_list:16"]
    assert adapter.tx_violations == []


# --- dry-run ---------------------------------------------------------------------------------------


def test_dry_run_writes_nothing_and_predicts():
    store, adapter = world({12: details_for(12, range(1, 10)), 13: details_for(13, [1]), 16: []})
    _seed(store, 12, range(1, 11))
    before = copy.deepcopy(store.prices)
    meta = metadata_ok([PriceListInfo(12, "S", 0)])
    report = run_sync(store, adapter, metadata=meta, dry_run=True)
    assert report.status == "SUCCESS" and report.dry_run
    assert store.prices == before and store.runs == {} and store.events == []
    assert meta.calls[0]["dry_run"] is True and len(store.lists) == 4
    out = result(report, 12).outcome
    assert out.rows_updated == 9 and out.rows_missing == 1 and out.sync_run_id is None
    assert format_price_report(report).startswith("dry_run=true")


# --- POINT -----------------------------------------------------------------------------------------


def test_point_all_active_lists_p0_without_lock():
    store, adapter = world()
    priorities = []
    out = run_point(store, adapter, [2], priorities=priorities)
    assert out.status == "SUCCESS" and out.mode == "POINT" and out.rows_inserted == 3
    assert {lid for lid, _ in adapter.calls} == {12, 13, 16}
    assert all(q["variantid"] == "2" for _, q in adapter.calls)
    assert priorities == [RequestPriority.P0_TARGETED]
    assert store.events == [] and store.runs[1]["state_scope"] == "point"
    assert store.price(12, 2)["last_source"] == "POINT"
    text = format_price_point(out)
    assert "command=refresh-prices" in text and "price_lists=12,13,16" in text and "fetched=3" in text
    assert "pair=13:2 status=FETCHED class=inserted" in text and TOKEN not in text


def test_point_specific_list_including_inactive():
    store, adapter = world({3: details_for(3, [2]), 12: details_for(12, [2])})
    out = run_point(store, adapter, [2], price_list_ids=[3])
    assert out.status == "SUCCESS" and {lid for lid, _ in adapter.calls} == {3}
    assert store.price(3, 2) is not None and store.price(12, 2) is None


def test_point_unknown_list_fails_without_http():
    store, adapter = world()
    out = run_point(store, adapter, [2], price_list_ids=[99])
    assert out.status == "FAILED" and "no existe" in out.error and adapter.calls == []


def test_point_no_rows_never_invents_zero_nor_missing():
    store, adapter = world()
    _seed(store, 12, [500])
    out = run_point(store, adapter, [500])
    assert out.status == "SUCCESS" and out.rows_received == 0
    assert set(r["status"] for r in out.point["results"].values()) == {"NO_ROWS"}
    assert store.price(13, 500) is None and store.price(12, 500)["missing_since"] is None


def test_point_ignored_filter_fails_pair():
    store, adapter = world(ignore_filter={12})
    out = run_point(store, adapter, [2])
    assert out.status == "PARTIAL"
    assert out.point["results"]["12:2"]["status"] == "FAILED"
    assert store.price(12, 2) is None and store.price(13, 2) is not None


def test_point_respects_stale_guard_and_clears_missing():
    store, adapter = world()
    _seed(store, 12, [2], fetched=FUTURE)
    _seed(store, 13, [2], missing=OLD)
    out = run_point(store, adapter, [2])
    assert out.point["results"]["12:2"]["class"] == "skipped_newer"
    assert store.price(13, 2)["missing_since"] is None


def test_point_dry_run_and_limits():
    store, adapter = world()
    out = run_point(store, adapter, [2], dry_run=True)
    assert out.rows_inserted == 3 and store.prices == {} and store.runs == {}
    with pytest.raises(UnsupportedSyncError):
        run_point(store, adapter, list(range(1, 60)))
    with pytest.raises(UnsupportedSyncError):
        run_point(store, adapter, [])


def test_point_all_failed_is_failed():
    store, adapter = world(errors={12: (500, {}), 13: (500, {}), 16: (500, {})})
    out = run_point(store, adapter, [2])
    assert out.status == "FAILED" and store.runs[1]["status"] == "FAILED"


# --- credenciales ----------------------------------------------------------------------------------


def test_token_never_in_errors_output_or_logs(caplog):
    caplog.set_level(logging.DEBUG)
    store, adapter = world(errors={12: requests.ConnectionError(f"conexión rechazada token={TOKEN}")})
    store.fail_upsert = True
    report = run_sync(store, adapter)
    text = format_price_report(report) + json.dumps([r.outcome.summary() for r in report.lists], default=str)
    text += json.dumps(store.runs, default=str)
    assert TOKEN not in text and TOKEN not in caplog.text
    assert "***" in result(report, 12).outcome.error or "ConnectionError" in result(report, 12).outcome.error


def test_missing_token_fails_before_http():
    store, adapter = world()
    report = run_sync(store, adapter, env={"OTRA": "x"})
    assert report.status == "FAILED" and "BSALE_TOKEN_SPA" in report.error and adapter.calls == []


# --- SQL real --------------------------------------------------------------------------------------


def test_upsert_sql_freshness_reappearance_and_no_delete():
    assert "ON CONFLICT (company_id, price_list_id, variant_id)" in UPSERT_PRICES_SQL
    assert "WHERE t.api_fetched_at <= EXCLUDED.api_fetched_at" in UPSERT_PRICES_SQL
    assert "missing_since = NULL" in UPSERT_PRICES_SQL
    assert "first_seen_at = " not in UPSERT_PRICES_SQL.split("DO UPDATE SET")[1]
    assert "api_fetched_at <= %s" in MARK_PRICES_MISSING_SQL and "missing_since IS NULL" in MARK_PRICES_MISSING_SQL
    sqls = [v for k, v in vars(price_engine).items() if k.endswith("_SQL") and isinstance(v, str)]
    assert sqls and not any(re.search(r"\b(DELETE|TRUNCATE|DROP)\b", s, re.IGNORECASE) for s in sqls)
    assert not any("bsale.variant_prices" in s or "bsale.price_lists" in s for s in sqls)


def test_pg_upsert_and_mark_missing_through_cursor():
    conn = FakeConnection(next_results=[[(12, 10)]])
    tx = PgPriceTx(conn.cursor())
    rows = build_price_rows(3, 12, _snap([detail(1, 10)]))
    assert tx.upsert_prices(rows, sync_run_id=7, last_source="FULL_RECONCILE") == {(12, 10)}
    sql, _ = conn.executed[0]
    assert sql.startswith("INSERT INTO bsale_raw.variant_prices AS t")
    conn.executed.clear()
    tx.cur.rowcount = 2
    assert tx.mark_prices_missing(3, 12, [5, 6], BASE) == 2
    assert conn.executed == [(MARK_PRICES_MISSING_SQL, (3, 12, [5, 6], BASE))]
    assert tx.mark_prices_missing(3, 12, [], BASE) == 0


def test_pg_store_reads_lists_and_catalog():
    conn = FakeConnection(next_results=[[(12, "SUPER", 0, None), (3, "QV", 1, None)], [(10,)], [("price_list:12", BASE)]])
    store = PgPriceStore(lambda: conn, read_only=True)
    lists = store.read_price_lists(3)
    assert lists[0] == PriceListInfo(12, "SUPER", 0) and lists[0].active and not lists[1].active
    assert store.read_catalogued_variants(3, [10, 11]) == {10}
    assert store.read_last_success(3, "variant_prices", ["price_list:12"]) == {"price_list:12": BASE}
    assert all(sql.lstrip().upper().startswith("SELECT") for sql, _ in conn.executed)


def test_rate_config_default_env_and_bounds():
    assert price_rate_config(lambda k: None).requests_per_second == 2.0
    assert price_rate_config({"BSALE_RAW_PRICE_RPS": "1.5"}.get).requests_per_second == 1.5
    for bad in ("0", "6", "-1"):
        with pytest.raises(ValueError):
            price_rate_config({"BSALE_RAW_PRICE_RPS": bad}.get)


# --- CLI -------------------------------------------------------------------------------------------


def _cli(argv, runner):
    out, err = io.StringIO(), io.StringIO()
    code = cli.main(argv, price_runner=runner, out=out, err=err)
    return code, out.getvalue(), err.getvalue()


def _report(status):
    return PriceSyncReport(company_id=3, selection="active", dry_run=False, status=status)


def test_cli_sync_prices_args_and_exit_codes():
    calls = []

    def runner(**kw):
        calls.append(kw)
        return _report(kw.get("status_hint", "SUCCESS"))

    code, out, _ = _cli(["sync-prices", "--company", "3", "--lists", "all", "--price-list", "12",
                         "--price-list", "13", "--dry-run"], runner)
    assert code == 0 and "command=sync-prices" in out and "status=SUCCESS" in out
    assert calls[0] == {"command": "sync-prices", "company_id": 3, "dry_run": True, "lists": "all",
                        "price_list_ids": [12, 13], "variant_ids": None}
    for status, expected in (("PARTIAL", 2), ("FAILED", 1), ("SKIPPED", 3)):
        assert _cli(["sync-prices", "--company", "3"], lambda **kw: _report(status))[0] == expected


def test_cli_refresh_prices_args_and_output():
    calls = []

    def runner(**kw):
        calls.append(kw)
        return EntityOutcome(company_id=3, resource="variant_prices", scope="variant:2", mode="POINT",
                             status="SUCCESS", point={"variant_ids": [2], "price_list_ids": [12],
                                                      "results": {"12:2": {"status": "FETCHED", "class": "inserted"}}})

    code, out, _ = _cli(["refresh-prices", "--company", "3", "--variant", "2", "--price-list", "12"], runner)
    assert code == 0 and "pair=12:2 status=FETCHED class=inserted" in out
    assert calls[0]["variant_ids"] == [2] and calls[0]["price_list_ids"] == [12]


@pytest.mark.parametrize("argv", [
    ["sync-prices", "--company", "0"],
    ["sync-prices", "--company", "3", "--price-list", "-1"],
    ["sync-prices", "--company", "3", "--lists", "todas"],
    ["refresh-prices", "--company", "3"],
    ["refresh-prices", "--company", "3", "--variant", "0"],
    ["refresh-prices", "--company", "3", *sum((["--variant", str(v)] for v in range(1, 60)), [])],
])
def test_cli_usage_errors(argv):
    def runner(**kw):
        raise AssertionError("no debe ejecutarse")

    assert _cli(argv, runner)[0] == 64


def test_cli_help_lists_price_commands():
    out = io.StringIO()
    parser = cli.build_parser([])
    parser.print_help(out)
    assert "sync-prices" in out.getvalue() and "refresh-prices" in out.getvalue()
