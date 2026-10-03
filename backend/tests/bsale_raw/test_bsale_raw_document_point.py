"""Fase 4E1: refresh POINT de UNA OC 33 (bundle atómico) + stock POINT post-COMMIT. Sin red ni BD real."""

from __future__ import annotations

import copy
import io
import itertools
import json
import logging
import re
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from urllib.parse import parse_qs, urlsplit

import pytest
import requests
from requests.adapters import BaseAdapter

from backend.jobs.bsale_raw import cli
from backend.services.bsale_raw.core.document_bundle import (
    CHILD_SPEC_NAMES,
    EMPTY_STORED,
    StoredDocument,
    build_child_rows,
    build_header,
    make_bundle,
    plan_document,
)
from backend.services.bsale_raw.core.document_engine import refresh_document_point
from backend.services.bsale_raw.core.engine import UnsupportedSyncError
from backend.services.bsale_raw.core.models import SyncMode
from backend.services.bsale_raw.core.rate_limit import RequestPriority
from backend.services.bsale_raw.core.registry import REGISTRY
from backend.services.bsale_raw.core.snapshot import FetchedItem, Snapshot
from backend.services.bsale_raw.core.store import EntityOutcome, PgRawStore, PgRawTx, advisory_lock_keys
from backend.tests.bsale_raw.test_bsale_raw_pipeline import (
    BASE,
    ENV,
    TOKEN,
    FakeConnection,
    FakeStore,
    FakeTx,
    TickClock,
    client_factory_for,
    make_response,
)
from backend.tests.bsale_raw.test_bsale_raw_stock import stock

API = "https://api.bsale.io/v1"
DOC = 500
DOC_TABLE = "bsale_raw.documents"
CHILD_TABLES = {
    "details": "bsale_raw.document_details",
    "references": "bsale_raw.document_references",
    "sellers": "bsale_raw.document_sellers",
}
LOG = "bsale_raw.document_change_log"
STOCK_TABLE = "bsale_raw.stocks"
DOC_TOKEN = "doctoken-7f3a9c"  # token propio del documento Bsale (va en payload, nunca en salida)
CLIENT_NAME = "Comercial Pérez Limitada"
SPEC = REGISTRY.get("documents")
CHILD_SPECS = {k: REGISTRY.get(n) for k, n in CHILD_SPEC_NAMES.items()}


# --- fixtures Bsale ---------------------------------------------------------------------------


def node(path: str, rid) -> dict:
    return {"href": f"{API}/{path}/{rid}.json", "id": str(rid)}


def header(doc=DOC, *, type_id=33, office_id=1, **over) -> dict:
    item = {
        "href": f"{API}/documents/{doc}.json", "id": doc, "emissionDate": 1784505600,
        "generationDate": 1784653800, "number": 9876, "totalAmount": 11900.0, "netAmount": 10000.0,
        "state": 0, "commercialState": "pendiente", "informedSii": 2, "token": DOC_TOKEN,
        "urlPdf": f"https://app.bsale.cl/view/{DOC_TOKEN}.pdf", "clientName": CLIENT_NAME,
        "document_type": node("document_types", type_id), "client": node("clients", 77), "user": node("users", 9),
        "details": {"href": f"{API}/documents/{doc}/details.json"},
        "references": {"href": f"{API}/documents/{doc}/references.json"},
        "sellers": {"href": f"{API}/documents/{doc}/sellers.json"},
        "attributes": {"href": f"{API}/documents/{doc}/attributes.json"},
    }
    if office_id is not None:
        item["office"] = node("offices", office_id)
    item.update(over)
    return item


def detail(did: int, variant: int | None, *, doc=DOC, qty=1.0, discount=0.0, **over) -> dict:
    item = {"href": f"{API}/documents/{doc}/details/{did}.json", "id": did, "lineNumber": did, "quantity": qty,
            "netUnitValue": 1000.0, "totalUnitValue": 1190.0, "netDiscount": 0.0, "totalDiscount": discount,
            "totalAmount": 1190.0 * qty, "relatedDetailId": 0}
    if variant is not None:
        item["variant"] = {"href": f"{API}/variants/{variant}.json", "id": variant, "code": f"SKU{variant}"}
    item.update(over)
    return item


def reference(rid: int, *, doc=DOC, number="F-1001", **over) -> dict:
    item = {"href": f"{API}/documents/{doc}/references/{rid}.json", "id": rid, "number": number,
            "referenceDate": 1784505600, "reason": "Factura", "dte_code": node("dte_codes", 33)}
    item.update(over)
    return item


def seller(uid: int, **over) -> dict:
    item = {"href": f"{API}/users/{uid}.json", "id": uid, "firstName": "Ana", "lastName": "Pérez"}
    item.update(over)
    return item


# --- fake BD ----------------------------------------------------------------------------------


class DocTx(FakeTx):
    def _maybe_fail(self, step):
        if self.s.fail_on == step:
            raise RuntimeError(f"fallo inyectado en {step}")

    def lock_document(self, spec, child_specs, company_id, document_id):
        self.s.events.append("lock_document")
        return self.s._stored(company_id, document_id)

    def upsert_document(self, spec, bundle, *, sync_run_id, last_source):
        self.s.events.append("upsert_document")
        self._maybe_fail("upsert_document")
        table = self.s.tables[DOC_TABLE]
        key = (bundle.company_id, bundle.document_id)
        prev = table.get(key)
        if prev is not None and prev["api_fetched_at"] > bundle.api_fetched_at:
            return False
        now = self.s.now()
        changed = prev is None or prev["version_hash"] != bundle.version_hash
        table[key] = {
            **bundle.typed,
            "details_count": len(bundle.details), "details_complete": True,
            "children_fetched_at": bundle.children_fetched_at, "attributes_payload": copy.deepcopy(bundle.attributes),
            "children_hash": bundle.children_hash, "version_hash": bundle.version_hash,
            "version_changed_at": now if changed else prev["version_changed_at"],
            "payload": bundle.header, "payload_hash": bundle.payload_hash,
            "first_seen_at": prev["first_seen_at"] if prev else now, "last_seen_at": now,
            "last_changed_at": now if changed else prev["last_changed_at"],
            "api_fetched_at": bundle.api_fetched_at, "missing_since": None,
            "last_source": last_source, "sync_run_id": sync_run_id,
            "watch_terminal_seen_at": None, "watch_stable_reads": 0, "watch_closed_at": None,
        }
        return True

    def replace_document_children(self, spec, kind, bundle, *, sync_run_id, last_source):
        self.s.events.append(f"replace_{kind}")
        table = self.s.tables[CHILD_TABLES[kind]]
        rows = bundle.children(kind)
        keep = {r.key for r in rows}
        stale = [k for k in table if k[:2] == (bundle.company_id, bundle.document_id) and k[2] not in keep]
        for k in stale:
            del table[k]
        now = self.s.now()
        for r in rows:
            key = (bundle.company_id, bundle.document_id, r.key)
            prev = table.get(key)
            table[key] = {
                "document_version_hash": bundle.version_hash, **r.typed, "payload": r.payload,
                "payload_hash": r.payload_hash, "first_seen_at": prev["first_seen_at"] if prev else now,
                "last_seen_at": now,
                "last_changed_at": prev["last_changed_at"] if prev and prev["payload_hash"] == r.payload_hash else now,
                "api_fetched_at": bundle.api_fetched_at, "last_source": last_source, "sync_run_id": sync_run_id,
            }
        self._maybe_fail(f"replace_{kind}")
        return len(stale)

    def insert_document_change(self, bundle, stored, plan, *, sync_run_id, detected_by):
        self.s.events.append("insert_change")
        self._maybe_fail("change_log")
        change_id = next(self.s._log_ids)
        self.s.tables[LOG][change_id] = {
            "company_id": bundle.company_id, "document_id": bundle.document_id,
            "document_type_id": bundle.document_type_id, "change_kind": plan.change_kind,
            "detected_at": self.s.now(), "api_fetched_at": bundle.api_fetched_at, "detected_by": detected_by,
            "sync_run_id": sync_run_id, "previous_version_hash": stored.version_hash,
            "version_hash": bundle.version_hash, "previous_payload_hash": stored.payload_hash,
            "payload_hash": bundle.payload_hash, "previous_state": stored.state, "state": bundle.typed.get("state"),
            "previous_commercial_state": stored.commercial_state,
            "commercial_state": bundle.typed.get("commercial_state"),
            **{f"{part}_changed": value for part, value in plan.changed.items()},
            "previous_variant_ids": list(plan.previous_variant_ids),
            "current_variant_ids": list(plan.current_variant_ids),
            "affected_variant_ids": list(plan.affected_variant_ids),
            "stock_refresh_requested_at": self.s.now() if plan.affected_variant_ids else None,
            "stock_refresh_done_at": None,
        }
        return change_id

    def mark_stock_refresh_done(self, company_id, document_id, change_ids):
        self.s.events.append("mark_done")
        self._maybe_fail("mark_done")
        n = 0
        for change_id in change_ids:
            row = self.s.tables[LOG].get(change_id)
            if row and row["stock_refresh_requested_at"] is not None and row["stock_refresh_done_at"] is None:
                row["stock_refresh_done_at"] = self.s.now()
                n += 1
        return n


class DocStore(FakeStore):
    tx_class = DocTx

    def __init__(self, db_clock=None):
        super().__init__(db_clock)
        for table in [*CHILD_TABLES.values(), LOG]:
            self.tables[table] = {}
        self._log_ids = itertools.count(1)

    def _stored(self, company_id, document_id):
        def rows(kind):
            return {k[2]: r for k, r in self.tables[CHILD_TABLES[kind]].items() if k[:2] == (company_id, document_id)}

        pending = sorted(
            (cid, list(r["affected_variant_ids"])) for cid, r in self.tables[LOG].items()
            if (r["company_id"], r["document_id"]) == (company_id, document_id)
            and r["stock_refresh_requested_at"] is not None and r["stock_refresh_done_at"] is None
        )
        children = dict(
            details={k: (r["payload_hash"], r["variant_id"]) for k, r in rows("details").items()},
            references={k: r["payload_hash"] for k, r in rows("references").items()},
            sellers={k: r["payload_hash"] for k, r in rows("sellers").items()},
            pending=pending,
        )
        head = self.tables[DOC_TABLE].get((company_id, document_id))
        if head is None:
            return StoredDocument(header_exists=False, **children)
        return StoredDocument(
            header_exists=True, api_fetched_at=head["api_fetched_at"], payload_hash=head["payload_hash"],
            version_hash=head["version_hash"], state=head["state"], commercial_state=head["commercial_state"],
            office_id=head["office_id"], attributes_payload=copy.deepcopy(head["attributes_payload"]), **children,
        )

    def read_document(self, spec, child_specs, company_id, document_id):
        self.events.append("read_document")
        return self._stored(company_id, document_id)

    # vistas para asserts
    def doc(self, document_id=DOC):
        return self.tables[DOC_TABLE].get((3, document_id))

    def children(self, kind, document_id=DOC):
        return {k[2]: r for k, r in self.tables[CHILD_TABLES[kind]].items() if k[:2] == (3, document_id)}

    def log(self, document_id=DOC):
        return [r for _, r in sorted(self.tables[LOG].items()) if r["document_id"] == document_id]

    def stocks(self):
        return {(k[1], k[2]): r for k, r in self.tables[STOCK_TABLE].items() if k[0] == 3}


# --- fake Bsale -------------------------------------------------------------------------------

_HEADER_RE = re.compile(r"^/v1/documents/(\d+)\.json$")
_CHILD_RE = re.compile(r"^/v1/documents/(\d+)/(details|references|sellers)\.json$")


class DocBsale(BaseAdapter):
    """Enruta header / hijos paginados / stocks. ``fail[kind]``: acciones consumidas por llamada
    (Exception o (status, body)). ``counts[kind]``: count forzado. ``reverse``: invierte el orden."""

    def __init__(self, store=None, *, docs=None, children=None, stocks=None, fail=None, counts=None,
                 on_call=None, reverse=False):
        super().__init__()
        self.store = store
        self.docs = dict(docs or {})
        self.kids = dict(children or {})
        self.stock_items = list(stocks or [])
        self.fail = {k: list(v) for k, v in (fail or {}).items()}
        self.counts = dict(counts or {})
        self.on_call = on_call
        self.reverse = reverse
        self.calls: list = []
        self.tx_violations: list[str] = []

    def kind_of(self, path):
        if _HEADER_RE.match(path):
            return "header"
        m = _CHILD_RE.match(path)
        if m:
            return m.group(2)
        return "stocks" if path == "/v1/stocks.json" else "other"

    def send(self, request, **kwargs):
        parts = urlsplit(request.url)
        kind = self.kind_of(parts.path)
        if self.store is not None:
            if self.store.in_tx:
                self.tx_violations.append(request.url)
            self.store.events.append(f"http:{kind}")
        self.calls.append(request)
        if self.on_call is not None:
            self.on_call(kind, len(self.calls))
        actions = self.fail.get(kind)
        if actions:
            action = actions.pop(0)
            if isinstance(action, BaseException):
                raise action
            status, body = action
            return make_response(request, status, body)
        q = {k: v[0] for k, v in parse_qs(parts.query).items()}
        if kind == "header":
            doc = int(_HEADER_RE.match(parts.path).group(1))
            if doc not in self.docs:
                return make_response(request, 404, b'{"error": "not found"}')
            return make_response(request, 200, json.dumps(self.docs[doc]).encode())
        if kind == "stocks":
            items = [it for it in self.stock_items
                     if ("variantid" not in q or it["variant"]["id"] == q["variantid"])
                     and ("officeid" not in q or it["office"]["id"] == q["officeid"])]
        elif kind in ("details", "references", "sellers"):
            doc = int(_CHILD_RE.match(parts.path).group(1))
            items = list(self.kids.get((doc, kind), []))
            if self.reverse:
                items.reverse()
        else:
            return make_response(request, 404, b"{}")
        offset, limit = int(q["offset"]), int(q["limit"])
        body = {"count": self.counts.get(kind, len(items)), "limit": limit, "offset": offset,
                "items": items[offset: offset + limit]}
        return make_response(request, 200, json.dumps(body).encode())

    def close(self):
        pass

    def paths(self, kind=None):
        return [urlsplit(c.url).path for c in self.calls if kind is None or self.kind_of(urlsplit(c.url).path) == kind]

    def stock_queries(self):
        return [{k: v[0] for k, v in parse_qs(urlsplit(c.url).query).items() if k in ("variantid", "officeid")}
                for c in self.calls if self.kind_of(urlsplit(c.url).path) == "stocks"]

    def set(self, *, details=None, references=None, sellers=None, doc=DOC, **header_over):
        if header_over or doc not in self.docs:
            self.docs[doc] = header(doc, **header_over)
        for kind, items in (("details", details), ("references", references), ("sellers", sellers)):
            if items is not None:
                self.kids[(doc, kind)] = list(items)


def variants_of(details) -> set[int]:
    return {d["variant"]["id"] for d in details if "variant" in d}


def setup(details=(), references=(), sellers=(seller(9),), *, office=1, stocks=None, **kw):
    store = DocStore()
    if stocks is None:
        stocks = [stock(v, office or 1) for v in sorted(variants_of(details))]
    adapter = DocBsale(store, stocks=stocks, **kw)
    adapter.set(details=details, references=references, sellers=sellers, office_id=office)
    return store, adapter


def go(store, adapter, *, doc=DOC, dry_run=False, clock=None, env=None, factory=None):
    return refresh_document_point(
        store=store, company_id=3, document_id=doc, dry_run=dry_run,
        client_factory=factory or client_factory_for(adapter), clock=clock or TickClock(BASE),
        getenv=(ENV if env is None else env).get, host="test",
    )


def snapshot_of(items) -> Snapshot:
    return Snapshot(items=[FetchedItem(i, BASE) for i in items], api_count=len(items), pages=1)


def bundle_of(details=(), references=(), sellers=(), *, doc=DOC, fetched_at=BASE, **header_over):
    h = header(doc, **header_over)
    typed = build_header(SPEC, doc, h, frozenset({33}))
    children = {
        kind: build_child_rows(kind, CHILD_SPECS[kind], doc, snapshot_of(items))
        for kind, items in (("details", details), ("references", references), ("sellers", sellers))
    }
    return make_bundle(company_id=3, document_id=doc, header=h, typed=typed, children=children,
                       api_fetched_at=fetched_at, children_fetched_at=fetched_at)


def consistent(store, doc=DOC) -> bool:
    version = store.doc(doc)["version_hash"]
    return all(r["document_version_hash"] == version for kind in CHILD_TABLES for r in store.children(kind, doc).values())


A, B, C, D = 10888, 10892, 10961, 10963


# --- header -----------------------------------------------------------------------------------


def test_oc33_created_with_full_bundle_then_stock_post_commit():
    store, adapter = setup([detail(1, A, qty=2.0), detail(2, B)], [reference(70)], [seller(9)])
    out = go(store, adapter)
    assert out.status == "SUCCESS" and out.mode == "POINT" and out.scope == "document:500"
    assert (out.rows_received, out.rows_inserted, out.rows_updated, out.rows_unchanged) == (1, 1, 0, 0)
    doc = store.doc()
    assert doc["document_type_id"] == 33 and doc["office_id"] == 1 and doc["client_id"] == 77 and doc["user_id"] == 9
    assert doc["number"] == 9876 and doc["state"] == 0 and doc["commercial_state"] == "pendiente"
    assert doc["emission_date"] == date(2026, 7, 20)
    assert doc["generation_date"] == datetime.fromtimestamp(1784653800, timezone.utc)
    assert doc["total_amount"] == Decimal("11900.0") and doc["informed_sii"] == 2
    assert doc["details_count"] == 2 and doc["details_complete"] is True and doc["children_fetched_at"] is not None
    assert doc["payload"] == adapter.docs[DOC] and doc["last_source"] == "POINT"
    assert doc["attributes_payload"] == {"href": f"{API}/documents/{DOC}/attributes.json"}
    assert doc["watch_terminal_seen_at"] is None and doc["watch_closed_at"] is None
    details = store.children("details")
    assert set(details) == {1, 2} and details[1]["variant_id"] == A and details[1]["quantity"] == Decimal("2.0")
    assert store.children("references")[70]["number"] == "F-1001"
    assert store.children("references")[70]["reference_date"] == date(2026, 7, 20)
    assert store.children("references")[70]["dte_code_id"] == 33
    assert set(store.children("sellers")) == {9}
    assert consistent(store)
    (log,) = store.log()
    assert log["change_kind"] == "CREATED" and log["detected_by"] == "POINT" and log["sync_run_id"] == out.sync_run_id
    assert log["affected_variant_ids"] == [A, B] and log["previous_variant_ids"] == []
    assert log["stock_refresh_requested_at"] is not None and log["stock_refresh_done_at"] is not None
    assert adapter.paths()[:4] == [f"/v1/documents/{DOC}.json", f"/v1/documents/{DOC}/details.json",
                                   f"/v1/documents/{DOC}/references.json", f"/v1/documents/{DOC}/sellers.json"]
    assert sorted(adapter.stock_queries(), key=lambda q: q["variantid"]) == [
        {"variantid": str(A), "officeid": "1"}, {"variantid": str(B), "officeid": "1"}]
    assert set(store.stocks()) == {(A, 1), (B, 1)} and store.stocks()[(A, 1)]["last_source"] == "POINT"
    assert out.document["stock_refresh"] == "SUCCESS" and out.requests == 4
    assert not adapter.tx_violations


def test_other_document_type_rejected_without_write_or_child_requests():
    store, adapter = setup([detail(1, A)])
    adapter.set(type_id=1)
    out = go(store, adapter)
    assert out.status == "FAILED" and "document_type_id=1" in out.error
    assert store.doc() is None and store.children("details") == {} and store.log() == []
    assert adapter.paths() == [f"/v1/documents/{DOC}.json"]
    assert store.sync_state[(3, "documents", "point")]["status"] == "FAILED"


def test_document_not_found_404():
    store, adapter = setup([detail(1, A)])
    out = go(store, adapter, doc=999)
    assert out.status == "FAILED" and "404" in out.error and store.tables[DOC_TABLE] == {}


@pytest.mark.parametrize("body", [b"<html>no json</html>", json.dumps([1, 2]).encode()])
def test_malformed_header_fails(body):
    store, adapter = setup([detail(1, A)], fail={"header": [(200, body)]})
    out = go(store, adapter)
    assert out.status == "FAILED" and store.tables[DOC_TABLE] == {} and len(adapter.calls) == 1


def test_header_with_other_id_fails():
    store, adapter = setup([detail(1, A)])
    adapter.docs[DOC] = header(DOC, id=501)
    assert go(store, adapter).status == "FAILED" and store.doc() is None


@pytest.mark.parametrize("over", [
    {"emissionDate": "ayer"}, {"totalAmount": "mucho"}, {"office": {"id": "x"}}, {"state": "x"},
    {"generationDate": True}, {"office": {"id": "0"}}, {"document_type": None},
])
def test_invalid_typed_header_fields_fail_without_write(over):
    store, adapter = setup([detail(1, A)])
    adapter.docs[DOC] = header(DOC, **over)
    out = go(store, adapter)
    assert out.status == "FAILED" and store.doc() is None and store.log() == []


# --- details ----------------------------------------------------------------------------------


def test_zero_lines_is_complete_and_needs_no_stock():
    store, adapter = setup([])
    out = go(store, adapter)
    assert out.status == "SUCCESS" and store.doc()["details_count"] == 0 and store.doc()["details_complete"]
    assert out.document["stock_refresh"] == "NOT_NEEDED" and adapter.stock_queries() == []
    (log,) = store.log()
    assert log["affected_variant_ids"] == [] and log["stock_refresh_requested_at"] is None


def test_one_line():
    store, adapter = setup([detail(1, A)])
    out = go(store, adapter)
    assert out.status == "SUCCESS" and set(store.children("details")) == {1}
    assert out.document["current_variants"] == [A] and adapter.stock_queries() == [{"variantid": str(A), "officeid": "1"}]


def test_multipage_details_read_completely_without_n_plus_one():
    lines = [detail(i, 20000 + i) for i in range(1, 121)]
    store, adapter = setup(lines, stocks=[])
    out = go(store, adapter)
    assert out.status == "SUCCESS" and len(store.children("details")) == 120 and store.doc()["details_count"] == 120
    assert len(adapter.paths("details")) == 3 and out.requests == 1 + 3 + 1 + 1
    assert not any("/variants/" in p for p in adapter.paths())
    assert out.document["stock_refresh"] == "SUCCESS" and len(adapter.stock_queries()) == 120  # NO_ROWS por variante


def test_duplicate_detail_id_fails():
    store, adapter = setup([detail(1, A), detail(1, B)])
    out = go(store, adapter)
    assert out.status == "FAILED" and "repetidos" in out.error and store.doc() is None


@pytest.mark.parametrize("bad", [
    {"quantity": "dos"}, {"lineNumber": "uno"}, {"variant": {"id": "x"}}, {"variant": {"id": 0}},
    {"href": f"{API}/documents/777/details/1.json"}, {"document": {"id": "777"}}, {"id": None},
])
def test_invalid_detail_fails_without_write(bad):
    store, adapter = setup([{**detail(1, A), **bad}], stocks=[])
    assert go(store, adapter).status == "FAILED" and store.doc() is None and store.children("details") == {}


def test_details_pagination_failure_keeps_previous_version():
    clock = TickClock(BASE)
    store, adapter = setup([detail(1, A), detail(2, B)])
    assert go(store, adapter, clock=clock).status == "SUCCESS"
    before = copy.deepcopy(store.tables)
    adapter.set(details=[detail(i, A) for i in range(1, 11)])
    adapter.counts = {"details": 60}  # count dice 60, llegan 10: truncado
    out = go(store, adapter, clock=clock)
    assert out.status == "FAILED" and "truncada" in out.error
    assert store.tables[DOC_TABLE] == before[DOC_TABLE] and store.tables[LOG] == before[LOG]
    assert all(store.tables[t] == before[t] for t in CHILD_TABLES.values())


def run_twice(first, second, *, references=((), ()), sellers=((seller(9),), (seller(9),)), header2=None):
    clock = TickClock(BASE)
    variants = sorted(variants_of(first) | variants_of(second))
    store, adapter = setup(first, references[0], sellers[0], stocks=[stock(v, 1) for v in variants])
    out1 = go(store, adapter, clock=clock)
    snap = copy.deepcopy(store.tables)
    adapter.set(details=second, references=references[1], sellers=sellers[1], **(header2 or {}))
    out2 = go(store, adapter, clock=clock)
    assert out1.status == "SUCCESS" and out2.status == "SUCCESS", (out1.error, out2.error)
    return store, adapter, out1, out2, snap


def test_line_removed_disappears_and_variant_still_refreshed():
    store, adapter, _, out2, _ = run_twice([detail(1, A), detail(2, B), detail(3, C)], [detail(1, A), detail(2, B)])
    assert set(store.children("details")) == {1, 2} and store.doc()["details_count"] == 2
    log = store.log()[-1]
    assert log["change_kind"] == "MODIFIED" and log["details_changed"] and not log["header_changed"]
    assert not log["references_changed"] and not log["sellers_changed"] and not log["attributes_changed"]
    assert log["affected_variant_ids"] == [A, B, C] and log["current_variant_ids"] == [A, B]
    assert out2.document["children_removed"] == {"details": 1, "references": 0, "sellers": 0}
    assert {"variantid": str(C), "officeid": "1"} in adapter.stock_queries()[-3:]
    assert consistent(store)


def test_line_added():
    store, _, _, out2, _ = run_twice([detail(1, A)], [detail(1, A), detail(2, D)])
    assert set(store.children("details")) == {1, 2} and out2.document["affected_variants"] == [A, D]


@pytest.mark.parametrize("change", [{"qty": 5.0}, {"discount": 100.0}])
def test_quantity_or_discount_change_changes_children_and_version_only(change):
    store, _, out1, out2, snap = run_twice([detail(1, A), detail(2, B)], [detail(1, A, **change), detail(2, B)])
    old, new = snap[DOC_TABLE][(3, DOC)], store.doc()
    assert new["payload_hash"] == old["payload_hash"]
    assert new["children_hash"] != old["children_hash"] and new["version_hash"] != old["version_hash"]
    assert new["version_changed_at"] > old["version_changed_at"] and new["last_changed_at"] > old["last_changed_at"]
    log = store.log()[-1]
    assert log["details_changed"] and not log["header_changed"] and out2.rows_updated == 1
    assert store.children("details")[2]["last_changed_at"] == snap[CHILD_TABLES["details"]][(3, DOC, 2)]["last_changed_at"]
    assert consistent(store)


def test_variant_changed_to_null_keeps_line_and_refreshes_old_variant():
    store, adapter, _, out2, _ = run_twice([detail(1, A), detail(2, B)], [detail(1, None), detail(2, B)])
    assert store.children("details")[1]["variant_id"] is None and len(store.children("details")) == 2
    assert out2.document["current_variants"] == [B] and out2.document["affected_variants"] == [A, B]
    assert out2.document["details_without_variant"] == 1


def test_all_lines_removed_no_stale_child_survives():
    store, _, _, out2, _ = run_twice([detail(1, A), detail(2, B)], [])
    assert store.children("details") == {} and store.doc()["details_count"] == 0
    assert out2.document["affected_variants"] == [A, B]


# --- references / sellers / attributes --------------------------------------------------------


def test_reference_added_only_marks_references_changed():
    store, _, _, out2, _ = run_twice([detail(1, A)], [detail(1, A)], references=((), (reference(70),)))
    log = store.log()[-1]
    assert log["references_changed"] and not log["details_changed"] and not log["header_changed"]
    assert set(store.children("references")) == {70} and out2.document["affected_variants"] == [A]


def test_reference_removed_disappears():
    store, _, _, _, _ = run_twice([detail(1, A)], [detail(1, A)],
                                  references=((reference(70), reference(71)), (reference(70),)))
    assert set(store.children("references")) == {70} and consistent(store)


def test_references_pagination():
    refs = [reference(i) for i in range(1, 56)]
    store, adapter = setup([detail(1, A)], refs)
    assert go(store, adapter).status == "SUCCESS" and len(store.children("references")) == 55
    assert len(adapter.paths("references")) == 2


@pytest.mark.parametrize("kind", ["details", "references", "sellers"])
def test_child_failure_writes_nothing_and_previous_version_survives(kind):
    clock = TickClock(BASE)
    store, adapter = setup([detail(1, A)], [reference(70)], [seller(9)])
    assert go(store, adapter, clock=clock).status == "SUCCESS"
    before = copy.deepcopy(store.tables)
    adapter.set(details=[detail(1, A, qty=9.0)], references=[], sellers=[seller(10)])
    adapter.fail = {kind: [requests.Timeout("lento")] * 3}
    out = go(store, adapter, clock=clock)
    assert out.status == "FAILED" and store.tables == before


def test_seller_added_and_removed():
    store, _, _, _, _ = run_twice([detail(1, A)], [detail(1, A)], sellers=((seller(9),), (seller(10), seller(11))))
    assert set(store.children("sellers")) == {10, 11}
    log = store.log()[-1]
    assert log["sellers_changed"] and not log["details_changed"] and not log["references_changed"]


def test_duplicate_seller_fails():
    store, adapter = setup([detail(1, A)], sellers=[seller(9), seller(9)])
    assert go(store, adapter).status == "FAILED" and store.doc() is None


def test_attributes_changed_and_empty():
    store, adapter, _, _, _ = run_twice([detail(1, A)], [detail(1, A)], header2={"attributes": [{"name": "Ruta", "value": "R2"}]})
    log = store.log()[-1]
    assert log["attributes_changed"] and log["header_changed"] and not log["details_changed"]
    assert store.doc()["attributes_payload"] == [{"name": "Ruta", "value": "R2"}]
    assert not any(p.endswith("/attributes.json") for p in adapter.paths())

    store, adapter = setup([detail(1, A)])
    adapter.docs[DOC] = {k: v for k, v in header(DOC).items() if k != "attributes"}
    assert go(store, adapter).status == "SUCCESS" and store.doc()["attributes_payload"] is None


# --- atomicidad -------------------------------------------------------------------------------


@pytest.mark.parametrize("step", ["upsert_document", "replace_details", "replace_references", "replace_sellers",
                                  "change_log"])
def test_db_error_rolls_back_everything(step):
    clock = TickClock(BASE)
    store, adapter = setup([detail(1, A), detail(2, B)], [reference(70)])
    assert go(store, adapter, clock=clock).status == "SUCCESS"
    before = copy.deepcopy(store.tables)
    stock_calls = len(adapter.stock_queries())
    adapter.set(details=[detail(1, A, qty=7.0)], references=[])
    store.fail_on = step
    out = go(store, adapter, clock=clock)
    assert out.status == "FAILED" and store.tables == before
    assert (out.rows_inserted, out.rows_updated, out.rows_unchanged) == (0, 0, 0)
    assert len(adapter.stock_queries()) == stock_calls  # sin stock tras un bundle revertido


# --- versión ----------------------------------------------------------------------------------


def test_same_bundle_same_version_no_fake_modified():
    clock = TickClock(BASE)
    store, adapter = setup([detail(1, A), detail(2, B)], [reference(70)])
    go(store, adapter, clock=clock)
    snap = copy.deepcopy(store.doc())
    out = go(store, adapter, clock=clock)
    doc = store.doc()
    assert out.status == "SUCCESS" and out.rows_unchanged == 1 and out.document["change_kind"] is None
    assert doc["version_hash"] == snap["version_hash"] and doc["version_changed_at"] == snap["version_changed_at"]
    assert doc["last_changed_at"] == snap["last_changed_at"] and doc["last_seen_at"] > snap["last_seen_at"]
    assert doc["api_fetched_at"] > snap["api_fetched_at"] and doc["first_seen_at"] == snap["first_seen_at"]
    assert len(store.log()) == 1 and out.document["stock_refresh"] == "NOT_NEEDED"


def test_reordered_children_keep_same_version_hash():
    lines = [detail(i, 20000 + i) for i in range(1, 8)]
    refs = [reference(70), reference(71)]
    sellers = [seller(9), seller(10)]
    straight = bundle_of(lines, refs, sellers)
    reordered = bundle_of(list(reversed(lines)), list(reversed(refs)), list(reversed(sellers)))
    assert straight.version_hash == reordered.version_hash and straight.children_hash == reordered.children_hash

    clock = TickClock(BASE)
    store, adapter = setup(lines, refs, sellers, stocks=[])
    go(store, adapter, clock=clock)
    adapter.reverse = True
    out = go(store, adapter, clock=clock)
    assert out.rows_unchanged == 1 and len(store.log()) == 1


def test_version_hash_components():
    base = bundle_of([detail(1, A)])
    assert bundle_of([detail(1, A)]).version_hash == base.version_hash
    qty = bundle_of([detail(1, A, qty=2.0)])
    assert qty.payload_hash == base.payload_hash and qty.version_hash != base.version_hash
    head = bundle_of([detail(1, A)], totalAmount=1.0)
    assert head.children_hash == base.children_hash and head.version_hash != base.version_hash


# --- frescura / concurrencia ------------------------------------------------------------------


def test_old_bundle_cannot_overwrite_newer_version():
    store, adapter = setup([detail(1, A)])
    newer = go(store, adapter, clock=TickClock(BASE + timedelta(days=1)))
    before = copy.deepcopy(store.tables)
    stock_calls = len(adapter.stock_queries())
    adapter.set(details=[detail(1, A, qty=50.0), detail(2, B)])
    out = go(store, adapter, clock=TickClock(BASE))
    assert newer.status == out.status == "SUCCESS" and out.rows_skipped_newer == 1
    assert out.document["stock_refresh"] == "SKIPPED_STALE" and out.document["stale"]
    assert store.tables == before and len(store.log()) == 1
    assert len(adapter.stock_queries()) == stock_calls


@pytest.mark.parametrize("existing", [True, False])
def test_concurrent_refresh_older_bundle_loses(existing):
    """A lee el header; mientras pide sellers, B completa un refresh más nuevo. A no debe pisar a B."""
    clock = TickClock(BASE)
    store, adapter = setup([detail(1, A)], stocks=[stock(A, 1), stock(B, 1), stock(C, 1)])
    if existing:
        go(store, adapter, clock=clock)
    other = DocBsale(store, stocks=adapter.stock_items)
    other.set(details=[detail(1, A), detail(2, C)], references=[], sellers=[seller(9)])
    seen = {}

    def interleave(kind, n):
        if kind == "sellers" and "b" not in seen:
            seen["b"] = go(store, other, clock=clock)

    adapter.set(details=[detail(1, A), detail(2, B)])
    adapter.on_call = interleave
    out = go(store, adapter, clock=clock)
    assert seen["b"].status == "SUCCESS" and out.status == "SUCCESS" and out.rows_skipped_newer == 1
    assert set(store.children("details")) == {1, 2} and store.children("details")[2]["variant_id"] == C
    assert consistent(store) and out.document["stock_refresh"] == "SKIPPED_STALE"


# --- previous / current / affected ------------------------------------------------------------


def test_affected_is_previous_union_current():
    store, adapter, _, out2, _ = run_twice([detail(1, A), detail(2, B), detail(3, C)],
                                           [detail(1, A), detail(2, B), detail(4, D)])
    log = store.log()[-1]
    assert log["previous_variant_ids"] == [A, B, C] and log["current_variant_ids"] == [A, B, D]
    assert log["affected_variant_ids"] == [A, B, C, D]
    queried = {int(q["variantid"]) for q in adapter.stock_queries()[-4:]}
    assert queried == {A, B, C, D}


def test_plan_document_pure():
    stored = StoredDocument(header_exists=True, api_fetched_at=BASE - timedelta(hours=1), version_hash="old",
                            payload_hash="x", office_id=1, details={1: ("h", A), 2: ("h", B), 3: ("h", C)},
                            pending=[(7, [99])])
    plan = plan_document(stored, bundle_of([detail(1, A), detail(2, B), detail(4, D)]))
    assert plan.affected_variant_ids == [A, B, C, D] and plan.stock_variant_ids == [99, A, B, C, D]
    assert plan.pending_change_ids == [7] and plan.stock_office_id == 1 and plan.change_kind == "MODIFIED"
    created = plan_document(EMPTY_STORED, bundle_of([detail(1, A)]))
    assert created.change_kind == "CREATED" and all(created.changed.values())


# --- stock post-COMMIT ------------------------------------------------------------------------


def test_stock_only_after_document_commit():
    store, adapter = setup([detail(1, A), detail(2, B)])
    go(store, adapter)
    events = store.events
    upsert = events.index("upsert_document")
    commit = events.index("tx_commit", upsert)
    stock_http = [i for i, e in enumerate(events) if e == "http:stocks"]
    assert stock_http and min(stock_http) > commit
    assert events.index("mark_done") > max(stock_http)


def test_stock_uses_document_office():
    store, adapter = setup([detail(1, A)], office=4)
    out = go(store, adapter)
    assert out.status == "SUCCESS" and adapter.stock_queries() == [{"variantid": str(A), "officeid": "4"}]
    assert set(store.stocks()) == {(A, 4)}


def test_document_without_office_refreshes_all_offices():
    store, adapter = setup([detail(1, A)], office=None, stocks=[stock(A, 1), stock(A, 4)])
    out = go(store, adapter)
    assert out.status == "SUCCESS" and store.doc()["office_id"] is None
    assert adapter.stock_queries() == [{"variantid": str(A)}] and set(store.stocks()) == {(A, 1), (A, 4)}


def test_office_change_refreshes_all_offices():
    store, adapter, _, out2, _ = run_twice([detail(1, A)], [detail(1, A)], header2={"office_id": 4})
    assert out2.document["stock_office_id"] is None and adapter.stock_queries()[-1] == {"variantid": str(A)}


def test_stock_failure_keeps_document_and_leaves_pending():
    store, adapter = setup([detail(1, A), detail(2, B)], fail={"stocks": [requests.Timeout("lento")] * 6})
    out = go(store, adapter)
    assert out.status == "PARTIAL" and cli.exit_code(out) == cli.EXIT_PARTIAL
    assert out.document["stock_refresh"] == "FAILED" and "documento confirmado" in out.error
    assert store.doc() is not None and consistent(store)
    (log,) = store.log()
    assert log["stock_refresh_requested_at"] is not None and log["stock_refresh_done_at"] is None
    assert store.runs[out.sync_run_id]["status"] == "PARTIAL"
    assert store.sync_state[(3, "documents", "point")]["status"] == "PARTIAL"


def test_stock_partial_does_not_mark_done():
    store, adapter = setup([detail(1, A), detail(2, B)], fail={"stocks": [requests.Timeout("lento")] * 3})
    out = go(store, adapter)
    assert out.status == "PARTIAL" and out.document["stock_refresh"] == "PARTIAL"
    assert store.log()[0]["stock_refresh_done_at"] is None


def test_pending_stock_retried_without_fake_modified():
    clock = TickClock(BASE)
    store, adapter = setup([detail(1, A), detail(2, B)], fail={"stocks": [requests.Timeout("lento")] * 6})
    assert go(store, adapter, clock=clock).status == "PARTIAL"
    out = go(store, adapter, clock=clock)
    assert out.status == "SUCCESS" and out.document["change_kind"] is None and out.rows_unchanged == 1
    assert out.document["pending_change_ids"] == [1] and out.document["stock_variants"] == [A, B]
    (log,) = store.log()
    assert log["stock_refresh_done_at"] is not None and set(store.stocks()) == {(A, 1), (B, 1)}
    third = go(store, adapter, clock=clock)
    assert third.document["stock_refresh"] == "NOT_NEEDED" and len(store.log()) == 1


def test_older_pending_rows_are_not_lost_across_versions():
    clock = TickClock(BASE)
    store, adapter = setup([detail(1, A)], stocks=[stock(A, 1), stock(B, 1), stock(C, 1)])
    adapter.fail = {"stocks": [requests.Timeout("x")] * 30}
    assert go(store, adapter, clock=clock).status == "PARTIAL"  # log 1: {A}
    adapter.set(details=[detail(2, B)])
    assert go(store, adapter, clock=clock).status == "PARTIAL"  # log 2: {A, B}
    adapter.fail = {}
    adapter.set(details=[detail(3, C)])
    out = go(store, adapter, clock=clock)  # log 3: {B, C} + pendientes {A} y {A, B}
    assert out.status == "SUCCESS" and out.document["stock_variants"] == [A, B, C]
    assert [r["stock_refresh_done_at"] is not None for r in store.log()] == [True, True, True]
    assert out.document["stock_done_change_ids"] == [3, 1, 2]


def test_mark_done_failure_keeps_document_and_pending():
    store, adapter = setup([detail(1, A)])
    store.fail_on = "mark_done"
    out = go(store, adapter)
    assert out.status == "PARTIAL" and store.doc() is not None
    assert store.log()[0]["stock_refresh_done_at"] is None and "cierre" in out.error


def test_document_and_stock_use_p0_on_same_factory():
    store, adapter = setup([detail(1, A)])
    base = client_factory_for(adapter)
    seen = []

    def factory(source, token, spec):
        client = base(source, token, spec)
        seen.append((spec.name, client.session.priority))
        return client

    assert go(store, adapter, factory=factory).status == "SUCCESS"
    assert seen == [("documents", RequestPriority.P0_TARGETED), ("stocks", RequestPriority.P0_TARGETED)]
    assert SPEC.request_priority is RequestPriority.P1_OC33  # el spec registrado no se muta


# --- change log -------------------------------------------------------------------------------


def test_state_change_logged_and_document_never_deleted():
    store, _, _, out2, snap = run_twice([detail(1, A)], [detail(1, A)], header2={"state": 1})
    log = store.log()[-1]
    assert (log["previous_state"], log["state"]) == (0, 1) and log["header_changed"]
    assert log["previous_version_hash"] == snap[DOC_TABLE][(3, DOC)]["version_hash"]
    assert log["previous_payload_hash"] == snap[DOC_TABLE][(3, DOC)]["payload_hash"]
    assert store.doc()["state"] == 1 and set(store.children("details")) == {1}
    assert store.doc()["watch_terminal_seen_at"] is None


def test_created_log_has_no_previous_values():
    store, adapter = setup([detail(1, A)])
    go(store, adapter)
    (log,) = store.log()
    assert log["previous_version_hash"] is None and log["previous_payload_hash"] is None
    assert log["previous_state"] is None and log["document_type_id"] == 33
    assert all(log[f"{p}_changed"] for p in ("header", "details", "references", "sellers", "attributes"))


# --- dry-run ----------------------------------------------------------------------------------


def test_dry_run_writes_nothing_and_reports_plan():
    clock = TickClock(BASE)
    store, adapter = setup([detail(1, A), detail(2, B), detail(3, C)])
    go(store, adapter, clock=clock)
    before = copy.deepcopy((store.tables, store.runs, store.entity_runs, store.sync_state))
    events = len(store.events)
    stock_calls = len(adapter.stock_queries())
    adapter.set(details=[detail(1, A), detail(4, D)])
    out = go(store, adapter, dry_run=True, clock=clock)
    assert out.status == "SUCCESS" and out.dry_run and out.sync_run_id is None
    assert (store.tables, store.runs, store.entity_runs, store.sync_state) == before
    assert "tx_begin" not in store.events[events:] and len(adapter.stock_queries()) == stock_calls
    doc = out.document
    assert doc["change_kind"] == "MODIFIED" and doc["stock_refresh"] == "DRY_RUN"
    assert doc["previous_variants"] == [A, B, C] and doc["current_variants"] == [A, D]
    assert doc["affected_variants"] == [A, B, C, D] and doc["details"] == 2 and out.rows_updated == 1


# --- CLI --------------------------------------------------------------------------------------


@pytest.mark.parametrize("dry_run", [True, False])
def test_cli_document_point(dry_run):
    seen = {}

    def runner(**kw):
        seen.update(kw)
        return EntityOutcome(
            company_id=3, resource="documents", scope="document:123456", mode="POINT", status="SUCCESS",
            dry_run=dry_run, requests=4, duration_ms=321,
            document={"document_id": 123456, "document_type_id": 33, "office_id": 1, "details": 6, "references": 0,
                      "sellers": 1, "change_kind": "CREATED", "version_changed": True, "previous_variants": [],
                      "current_variants": [1, 2, 3, 4, 5, 6], "affected_variants": [1, 2, 3, 4, 5, 6],
                      "pending_change_ids": [], "stock_variants": [1, 2, 3, 4, 5, 6], "stock_office_id": 1,
                      "stock_refresh": "SUCCESS", "stock_requests": 6},
        )

    argv = ["sync", "--company", "3", "--resource", "documents", "--document", "123456", "--mode", "point"]
    buf = io.StringIO()
    assert cli.main(argv + (["--dry-run"] if dry_run else []), runner=runner, out=buf) == cli.EXIT_SUCCESS
    assert seen == {"company_id": 3, "resource": "documents", "mode": SyncMode.POINT, "dry_run": dry_run,
                    "office_id": None, "variant_id": None, "document_id": 123456}
    lines = buf.getvalue().splitlines()
    if dry_run:
        assert lines.pop(0) == "dry_run=true"
    keys = [line.split("=", 1)[0] for line in lines]
    assert keys[:4] == ["company", "resource", "scope", "mode"] and keys[-1] == "status"
    for expected in ("scope=document:123456", "document_type_id=33", "office_id=1", "details=6", "references=0",
                     "sellers=1", "version_changed=true", "previous_variants=0", "current_variants=6",
                     "affected_variants=6", "stock_refresh=SUCCESS", "requests=4", "status=SUCCESS"):
        assert expected in lines, expected


@pytest.mark.parametrize("argv", [
    ["--resource", "documents", "--mode", "point"],
    ["--resource", "documents", "--document", "123", "--mode", "full-reconcile"],
    ["--resource", "documents", "--mode", "full-reconcile"],
    ["--resource", "documents", "--document", "0", "--mode", "point"],
    ["--resource", "documents", "--document", "-4", "--mode", "point"],
    ["--resource", "documents", "--document", "F-123", "--mode", "point"],
    ["--resource", "documents", "--variant", "10888", "--mode", "point"],
    ["--resource", "documents", "--document", "123", "--variant", "10888", "--mode", "point"],
    ["--resource", "documents", "--document", "123", "--office", "1", "--mode", "point"],
    ["--resource", "stocks", "--document", "123", "--mode", "point"],
    ["--resource", "stocks", "--document", "123", "--variant", "1", "--mode", "point"],
    ["--resource", "products", "--document", "123", "--mode", "full-reconcile"],
    ["--resource", "offices", "--document", "123", "--mode", "point"],
])
def test_cli_document_usage_errors(argv):
    never = lambda **kw: pytest.fail("no debe ejecutarse")  # noqa: E731
    code = cli.main(["sync", "--company", "3", *argv], runner=never, out=io.StringIO(), err=io.StringIO())
    assert code == cli.EXIT_USAGE


def test_cli_output_from_real_outcome_is_sanitized():
    store, adapter = setup([detail(1, A)], [reference(70)], [seller(9)])
    text = cli.format_outcome(go(store, adapter))
    for secret in (TOKEN, DOC_TOKEN, CLIENT_NAME, "Pérez", "urlPdf", "app.bsale.cl"):
        assert secret not in text


# --- seguridad --------------------------------------------------------------------------------


def test_token_never_appears():
    store, adapter = setup([detail(1, A)], fail={"details": [(401, f'{{"error": "bad {TOKEN}"}}'.encode())]})
    out = go(store, adapter)
    assert out.status == "FAILED" and TOKEN not in out.error
    assert TOKEN not in repr(store.runs) + repr(store.entity_runs) + repr(store.sync_state) + repr(out.document)
    assert TOKEN not in cli.format_outcome(out)


def test_payload_and_pii_not_in_logs_or_summary(caplog):
    caplog.set_level(logging.DEBUG)
    store, adapter = setup([detail(1, A)], [reference(70)], [seller(9)])
    out = go(store, adapter)
    assert out.status == "SUCCESS"
    summary = json.dumps(store.runs[out.sync_run_id]["summary"], default=str)
    for secret in (TOKEN, DOC_TOKEN, CLIENT_NAME, "Pérez", "app.bsale.cl"):
        assert secret not in caplog.text and secret not in summary
    assert "document:500" in caplog.text


@pytest.mark.parametrize("href", [
    "https://evil.example.com/v1/documents/500/details.json",
    "http://api.bsale.io/v1/documents/500/details.json",
    "https://api.bsale.io/v1/documents/501/details.json",
    "https://api.bsale.io/v1/documents/500/details.json?access_token=x",
    "https://api.bsale.io.evil.com/v1/documents/500/details.json",
])
def test_child_link_must_match_documented_bsale_route(href):
    store, adapter = setup([detail(1, A)])
    adapter.docs[DOC] = header(DOC, details={"href": href})
    out = go(store, adapter)
    assert out.status == "FAILED" and "href" in out.error and store.doc() is None
    assert adapter.paths() == [f"/v1/documents/{DOC}.json"]
    assert all(urlsplit(c.url).netloc == "api.bsale.io" for c in adapter.calls)


def test_engine_rejects_misuse():
    for bad in (0, -1, "500", True, None):
        with pytest.raises(UnsupportedSyncError):
            refresh_document_point(store=DocStore(), company_id=3, document_id=bad)
    with pytest.raises(UnsupportedSyncError):
        refresh_document_point(store=DocStore(), company_id=3, document_id=500, resource="stocks")


def test_missing_token_fails_before_http():
    store, adapter = setup([detail(1, A)])
    out = go(store, adapter, env={})
    assert out.status == "FAILED" and adapter.calls == [] and store.runs == {}


# --- SQL real (cursor falso): set-based, acotado al documento ---------------------------------


def test_lock_document_sql_xact_lock_then_for_update_and_set_reads():
    conn = FakeConnection(next_results=[
        [(None,)], [(BASE, "ph", "vh", 0, "pendiente", 1, {"a": 1})],
        [(1, "h1", A), (2, "h2", None)], [], [(9, "hs")], [(5, [A, B])],
    ])
    stored = PgRawTx(conn.cursor()).lock_document(SPEC, CHILD_SPECS, 3, DOC)
    statements = [s for s, _ in conn.executed]
    assert conn.executed[0] == ("SELECT pg_advisory_xact_lock(%s, %s)", advisory_lock_keys(3, "documents", "document:500"))
    assert statements[1].startswith("SELECT api_fetched_at") and statements[1].endswith("FOR UPDATE")
    assert len(statements) == 6 and all("document_id = %s" in s for s in statements[2:])
    assert stored.header_exists and stored.version_hash == "vh" and stored.office_id == 1
    assert stored.details == {1: ("h1", A), 2: ("h2", None)} and stored.sellers == {9: "hs"}
    assert stored.pending == [(5, [A, B])] and stored.variant_ids() == [A]


def test_read_document_for_dry_run_has_no_locks():
    conn = FakeConnection(next_results=[[], [], [], [], []])
    stored = PgRawStore(lambda: conn, read_only=True).read_document(SPEC, CHILD_SPECS, 3, DOC)
    assert not stored.header_exists and len(conn.executed) == 5
    assert not any("FOR UPDATE" in s or "advisory" in s for s, _ in conn.executed)


def test_document_upsert_sql_freshness_version_and_no_watch_columns():
    bundle = bundle_of([detail(1, A)])
    conn = FakeConnection(next_results=[[(DOC,)]])
    assert PgRawTx(conn.cursor()).upsert_document(SPEC, bundle, sync_run_id=7, last_source="POINT") is True
    ((sql, params),) = conn.executed
    assert sql.startswith("INSERT INTO bsale_raw.documents AS t") and "ON CONFLICT (company_id, bsale_id)" in sql
    assert "WHERE t.api_fetched_at <= EXCLUDED.api_fetched_at" in sql
    assert "version_changed_at = CASE WHEN t.version_hash IS DISTINCT FROM EXCLUDED.version_hash" in sql
    assert "last_changed_at = CASE WHEN t.version_hash IS DISTINCT FROM EXCLUDED.version_hash" in sql
    assert "watch_" not in sql and sql.count("%s") == len(params)
    assert params[0:2] == (3, DOC) and bundle.version_hash in params and "POINT" in params
    assert PgRawTx(FakeConnection(next_results=[[]]).cursor()).upsert_document(
        SPEC, bundle, sync_run_id=7, last_source="POINT") is False


def test_children_replace_is_set_based_and_parent_scoped():
    bundle = bundle_of([detail(i, 20000 + i) for i in range(1, 121)], [reference(70)], [seller(9)])
    conn = FakeConnection()
    tx = PgRawTx(conn.cursor())
    for kind in ("details", "references", "sellers"):
        tx.replace_document_children(CHILD_SPECS[kind], kind, bundle, sync_run_id=7, last_source="POINT")
    statements = [s for s, _ in conn.executed]
    assert len(statements) == 6  # 1 DELETE + 1 INSERT por colección (120 líneas en un solo lote)
    deletes = [(s, p) for s, p in conn.executed if s.startswith("DELETE")]
    assert [p[:2] for _, p in deletes] == [(3, DOC)] * 3 and len(deletes[0][1][2]) == 120
    assert "NOT (bsale_id = ANY(%s::bigint[]))" in deletes[0][0] and "NOT (user_id = ANY(%s::bigint[]))" in deletes[2][0]
    inserts = [s for s in statements if s.startswith("INSERT")]
    assert "ON CONFLICT (company_id, document_id, bsale_id)" in inserts[0]
    assert "ON CONFLICT (company_id, document_id, user_id)" in inserts[2]
    assert all("api_fetched_at <=" not in s for s in inserts) and bundle.version_hash in inserts[0]


def test_empty_children_delete_all_of_that_document_only():
    bundle = bundle_of([])
    conn = FakeConnection()
    PgRawTx(conn.cursor()).replace_document_children(CHILD_SPECS["details"], "details", bundle,
                                                     sync_run_id=7, last_source="POINT")
    ((sql, params),) = conn.executed
    assert sql.startswith("DELETE FROM bsale_raw.document_details") and params == (3, DOC, [])


def test_change_log_and_mark_done_sql():
    bundle = bundle_of([detail(1, A)])
    plan = plan_document(EMPTY_STORED, bundle)
    conn = FakeConnection(next_results=[[(42,)]])
    tx = PgRawTx(conn.cursor())
    assert tx.insert_document_change(bundle, EMPTY_STORED, plan, sync_run_id=7, detected_by="POINT") == 42
    ((sql, params),) = conn.executed
    assert "INSERT INTO bsale_raw.document_change_log" in sql and sql.count("%s") == len(params)
    assert params[3] == "CREATED" and params[-4:] == ([], [A], [A], True)
    assert tx.mark_stock_refresh_done(3, DOC, []) == 0 and len(conn.executed) == 1
    tx.mark_stock_refresh_done(3, DOC, [42, 43])
    assert "stock_refresh_done_at IS NULL" in conn.executed[-1][0] and conn.executed[-1][1] == (3, DOC, [42, 43])
