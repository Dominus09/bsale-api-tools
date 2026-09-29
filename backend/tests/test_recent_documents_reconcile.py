"""Regresión OC 69882: folio aislado que escapa a la ventana live y se recupera por reconciliación."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import MagicMock, patch

from backend.repositories.distribuidora.documents_repo import document_dict_from_bsale
from backend.services.distribuidora import live_sync_service
from backend.services.distribuidora.recent_documents_reconcile_service import (
    compute_live_emission_window,
    diagnose_oc_skip,
    emission_visible_in_window,
    reconcile_recent_oc_documents,
    run_oc_folio_canary,
)

NOW = datetime(2026, 9, 29, 15, 0, 0, tzinfo=timezone.utc)
SEPT28_MIDNIGHT = int(datetime(2026, 9, 28, tzinfo=timezone.utc).timestamp())


def _oc(
    bsale_id: int,
    number: int,
    *,
    generated: datetime,
    office: int = 1,
    doc_type: int = 33,
    state: int = 0,
    total: int = 2_150_000,
) -> dict[str, Any]:
    return {
        "id": bsale_id,
        "number": number,
        "emissionDate": int(
            datetime(generated.year, generated.month, generated.day, tzinfo=timezone.utc).timestamp()
        ),
        "generationDate": int(generated.timestamp()),
        "totalAmount": total,
        "state": state,
        "commercialState": 0,
        "document_type": {"id": str(doc_type)},
        "office": {"id": str(office)},
        "client": {"id": "3411"},
    }


class FakeBsale:
    """Responde ``/documents.json`` filtrando como Bsale (officeid, tipo, rangos, number)."""

    def __init__(self, docs: list[dict[str, Any]], details: dict[int, int] | None = None):
        self.docs = docs
        self.details = details or {}
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def get(self, path: str, params: dict[str, Any] | None = None, **_kw) -> dict[str, Any]:
        params = dict(params or {})
        self.calls.append((path, params))
        if path.endswith("/details.json"):
            doc_id = int(path.split("/")[2])
            n = self.details.get(doc_id, 0)
            off = int(params.get("offset") or 0)
            lim = int(params.get("limit") or 50)
            items = [{"id": doc_id * 100 + i, "lineNumber": i + 1} for i in range(n)]
            return {"items": items[off : off + lim]}
        if path.startswith("/clients/"):
            return {"company": "Cliente Test"}
        out = list(self.docs)
        if "officeid" in params:
            out = [d for d in out if int(d["office"]["id"]) == int(params["officeid"])]
        if "documenttypeid" in params:
            out = [d for d in out if int(d["document_type"]["id"]) == int(params["documenttypeid"])]
        if "number" in params:
            out = [d for d in out if int(d["number"]) == int(params["number"])]
        for field, key in (("generationdaterange", "generationDate"), ("emissiondaterange", "emissionDate")):
            if field in params:
                lo, hi = (int(x) for x in params[field].strip("[]").split(","))
                out = [d for d in out if lo <= int(d[key]) <= hi]
        out.sort(key=lambda d: d["id"])
        off = int(params.get("offset") or 0)
        lim = int(params.get("limit") or 50)
        return {"items": out[off : off + lim]}


class FakeStore:
    """Imita ``upsert_documents``: clave lógica (company, office, type, number); PK estable."""

    def __init__(self) -> None:
        self.rows: dict[tuple[int, int, int, int], dict[str, Any]] = {}
        self.details: dict[int, int] = {}
        self.upserts = 0

    def upsert(self, doc: dict[str, Any], *, company_id: int, office_id: int, details: int) -> dict[str, Any]:
        row = document_dict_from_bsale(doc, company_id=company_id, default_office_id=office_id)
        assert row is not None
        key = (row["company_id"], row["office_id"], row["document_type_id"], row["number"])
        existing = self.rows.get(key)
        if existing is not None:
            row["document_id"] = existing["document_id"]
        row["source_document_id"] = int(doc["id"])
        self.rows[key] = row
        self.details[row["document_id"]] = details
        self.upserts += 1
        return row

    def folios(self, company_id: int, office_id: int) -> set[int]:
        return {k[3] for k in self.rows if k[0] == company_id and k[1] == office_id and k[2] == 33}


def _reconcile(bsale: FakeBsale, store: FakeStore, *, apply: bool = True, **kw):
    def _load(folios):
        present = store.folios(3, 1)
        return {f for f in folios if f in present}

    def _repair(folio: int, active: dict[str, Any]) -> dict[str, Any]:
        n = len(bsale.get(f"/documents/{active['id']}/details.json", {"limit": 50})["items"])
        row = store.upsert(active, company_id=3, office_id=1, details=n)
        return {"status": "synced", "wrote": True, "local_document_id": row["document_id"], "details_replaced": n}

    return reconcile_recent_oc_documents(
        bsale,
        company_id=3,
        office_id=1,
        apply=apply,
        load_local_folios=_load,
        repair_one=_repair,
        now=kw.pop("now", NOW),
        **kw,
    )


def test_emission_midnight_is_invisible_to_short_live_window():
    window = compute_live_emission_window(now=NOW, last_watermark=NOW - timedelta(minutes=5))
    assert not emission_visible_in_window(SEPT28_MIDNIGHT, window)
    at_midnight = datetime(2026, 9, 28, 1, 0, tzinfo=timezone.utc)
    assert emission_visible_in_window(
        SEPT28_MIDNIGHT, compute_live_emission_window(now=at_midnight, last_watermark=at_midnight)
    )


def test_regression_69882_escapes_live_window_and_reconcile_inserts_once():
    t_run1 = datetime(2026, 9, 28, 16, 0, tzinfo=timezone.utc)
    oc81 = _oc(4000081, 69881, generated=t_run1 - timedelta(minutes=30))
    oc83 = _oc(4000083, 69883, generated=t_run1 - timedelta(minutes=20))
    oc82 = _oc(4000082, 69882, generated=t_run1 - timedelta(minutes=25))
    bsale = FakeBsale([oc81, oc83], details={4000081: 3, 4000082: 12, 4000083: 4})
    store = FakeStore()

    # Run live: la API devuelve 69881 y 69883; el watermark avanza a t_run1.
    for d in bsale.get("/documents.json", {"officeid": 1})["items"]:
        store.upsert(d, company_id=3, office_id=1, details=bsale.details[d["id"]])
    watermark = t_run1

    # 69882 aparece después en la API, con generationDate anterior al watermark - overlap.
    bsale.docs.append(oc82)
    next_run = watermark + timedelta(hours=6)
    lo = int((next_run - timedelta(hours=2)).timestamp())
    live_items = bsale.get(
        "/documents.json",
        {"officeid": 1, "generationdaterange": f"[{lo},{int(next_run.timestamp())}]"},
    )["items"]
    assert 69882 not in {d["number"] for d in live_items}
    assert 69882 not in store.folios(3, 1)

    out = _reconcile(bsale, store, now=next_run)
    assert out["missing_folios"] == [69882]
    assert out["repaired"] == 1
    key = (3, 1, 33, 69882)
    assert store.rows[key]["company_id"] == 3 and store.rows[key]["office_id"] == 1
    assert store.rows[key]["source_document_id"] == 4000082
    assert store.details[store.rows[key]["document_id"]] == 12
    assert sum(1 for k in store.rows if k[3] == 69882) == 1

    again = _reconcile(bsale, store, now=next_run)
    assert again["missing"] == 0 and again["repaired"] == 0
    assert store.upserts == 3


def test_reissue_with_new_source_id_keeps_single_row_and_pk():
    t0 = NOW - timedelta(hours=10)
    old = _oc(4000082, 69882, generated=t0)
    bsale = FakeBsale([old], details={4000082: 5, 4100082: 7})
    store = FakeStore()
    _reconcile(bsale, store)
    local_pk = store.rows[(3, 1, 33, 69882)]["document_id"]

    old["state"] = 8888
    old["number"] = 0
    bsale.docs.append(_oc(4100082, 69882, generated=t0 + timedelta(hours=1)))
    out = _reconcile(bsale, store)
    assert out["missing"] == 0
    assert len([k for k in store.rows if k[3] == 69882]) == 1
    assert store.rows[(3, 1, 33, 69882)]["document_id"] == local_pk

    store.rows.clear()
    out = _reconcile(bsale, store)
    assert out["results"][0]["bsale_document_id"] == 4100082
    assert store.rows[(3, 1, 33, 69882)]["source_document_id"] == 4100082


def test_pagination_collects_all_pages_and_respects_budget():
    base = NOW - timedelta(hours=20)
    docs = [_oc(5_000_000 + i, 70_000 + i, generated=base + timedelta(minutes=i)) for i in range(110)]
    bsale = FakeBsale(docs)
    store = FakeStore()
    out = _reconcile(bsale, store, apply=False)
    assert out["api_pages"] == 3 and not out["api_truncated_by_budget"]
    assert out["missing"] == 110

    limited = _reconcile(bsale, store, apply=False, max_pages=2)
    assert limited["api_truncated_by_budget"] is True
    assert limited["bsale_items"] == 100

    budget = _reconcile(bsale, store, max_repairs=10)
    assert budget["repaired"] == 10
    assert sum(1 for r in budget["results"] if r["status"] == "deferred_budget") == 100
    assert _reconcile(bsale, store, max_repairs=200)["repaired"] == 100
    assert _reconcile(bsale, store)["missing"] == 0


def test_same_folio_other_office_is_not_confused():
    t = NOW - timedelta(hours=3)
    other_office = _oc(4000999, 69882, generated=t, office=2)
    bsale = FakeBsale([other_office, _oc(4000082, 69882, generated=t)], details={4000082: 2})
    store = FakeStore()
    store.upsert(other_office, company_id=3, office_id=2, details=9)

    out = _reconcile(bsale, store)
    assert out["missing_folios"] == [69882]
    assert store.rows[(3, 1, 33, 69882)]["source_document_id"] == 4000082
    assert store.rows[(3, 2, 33, 69882)]["source_document_id"] == 4000999


def test_only_type_33_is_reconciled():
    t = NOW - timedelta(hours=2)
    invoice = _oc(4000500, 69882, generated=t, doc_type=6)
    bsale = FakeBsale([invoice])

    class LeakyBsale(FakeBsale):
        def get(self, path, params=None, **kw):
            params = dict(params or {})
            params.pop("documenttypeid", None)
            return super().get(path, params, **kw)

    store = FakeStore()
    assert _reconcile(bsale, store)["bsale_items"] == 0
    leaky = _reconcile(LeakyBsale([invoice]), store)
    assert leaky["bsale_ignored_other_scope"] == 1
    assert leaky["missing"] == 0 and not store.rows


def test_canary_reports_bsale_and_local_fields():
    t = datetime(2026, 9, 28, 14, 30, tzinfo=timezone.utc)
    oc82 = _oc(4000082, 69882, generated=t)
    bsale = FakeBsale([oc82], details={4000082: 12})
    rep = run_oc_folio_canary(
        bsale, folio=69882, company_id=3, office_id=1, local_loader=lambda f, ids: [], now=NOW
    )
    assert rep["found_in_bsale"] and not rep["found_locally"]
    assert rep["bsale_document_id"] == 4000082
    assert rep["document_type_id"] == 33 and rep["bsale_office_id"] == 1
    assert rep["details_count"] == 12 and rep["client"] == "Cliente Test"
    diag = rep["diagnosis"]
    assert diag["emission_is_utc_midnight"] is True
    assert "live_sync_emission_window_blind" in diag["reasons"]
    assert diag["primary_cause"] == "eligible_but_never_persisted_check_upsert_failures_or_orders_job"


def test_canary_detects_folio_only_in_other_office_and_pk_collision():
    t = NOW - timedelta(hours=5)
    bsale = FakeBsale([_oc(4000082, 69882, generated=t, office=7)])
    rep = run_oc_folio_canary(
        bsale, folio=69882, company_id=3, office_id=1, local_loader=lambda f, ids: [], now=NOW
    )
    assert rep["diagnosis"]["primary_cause"] == "bsale_folio_only_in_other_office"
    assert rep["bsale_office_id"] == 7

    oc = _oc(4000082, 69882, generated=t)
    collision = [
        {
            "document_id": 4000082,
            "company_id": 3,
            "office_id": 1,
            "document_type_id": 6,
            "number": 55555,
            "state": 0,
            "source_document_id": None,
            "raw_bsale_id": 4000082,
            "matched_by": ["document_id=bsale_id"],
        }
    ]
    diag = diagnose_oc_skip(
        folio=69882,
        company_id=3,
        office_id=1,
        office_hits=[oc],
        any_office_hits=[oc],
        local_rows=collision,
        now=NOW,
    )
    assert diag["primary_cause"] == "local_pk_collision_document_id"


def test_live_sync_documents_uses_generation_date_range(monkeypatch):
    monkeypatch.delenv("LIVE_SYNC_DOCUMENTS_DATE_FIELD", raising=False)
    captured: list[str] = []

    def _fake_fetch(*_a, **kw):
        captured.append(kw["date_range_field"])

    conn = MagicMock()
    conn.cursor.return_value.fetchone.return_value = (True,)
    with patch.object(live_sync_service, "bsale_token_distribuidora_configured", return_value=True), patch.object(
        live_sync_service, "get_connection", return_value=conn
    ), patch.object(live_sync_service, "get_sync_state", return_value=None), patch.object(
        live_sync_service, "_fetch_documents_window", side_effect=_fake_fetch
    ), patch.object(live_sync_service, "update_sync_state_success"), patch.object(
        live_sync_service, "BsaleClient"
    ), patch.object(live_sync_service, "_bsale_token", return_value="x"), patch.object(
        live_sync_service, "log_tx"
    ), patch.object(live_sync_service, "pg_backend_pid", return_value=1):
        stats = live_sync_service.live_sync_documents(strict_token=True)
    assert captured == ["generationdaterange", "generationdaterange"]
    assert stats["date_range_field"] == "generationdaterange"
