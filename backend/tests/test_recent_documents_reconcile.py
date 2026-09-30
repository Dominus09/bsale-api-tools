"""Live restaurado (pre-a8d9896) + herramientas aisladas: canario por folio y reconcile recent."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import requests

from backend.jobs import live_sync_documents as job
from backend.repositories.distribuidora.documents_repo import document_dict_from_bsale
from backend.services.distribuidora import live_sync_service, sync_service
from backend.services.distribuidora.recent_documents_reconcile_service import (
    diagnose_oc_skip,
    emission_day_window,
    emission_visible_in_window,
    reconcile_recent_oc_documents,
    run_oc_folio_canary,
)

UTC = timezone.utc
NOW = datetime(2026, 9, 29, 15, 0, 0, tzinfo=UTC)


def _midnight(dt: datetime) -> int:
    return int(datetime(dt.year, dt.month, dt.day, tzinfo=UTC).timestamp())


def _oc(
    bsale_id: int,
    number: int,
    *,
    generated: datetime,
    emission: datetime | None = None,
    office: int = 1,
    doc_type: int = 33,
    state: int = 0,
    total: int = 2_150_000,
) -> dict[str, Any]:
    return {
        "id": bsale_id,
        "number": number,
        "emissionDate": _midnight(emission or generated),
        "generationDate": int(generated.timestamp()),
        "totalAmount": total,
        "state": state,
        "commercialState": 0,
        "document_type": {"id": str(doc_type)},
        "office": {"id": str(office)},
        "client": {"id": "3411"},
    }


class FakeBsale:
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
        if "emissiondaterange" in params:
            lo, hi = (int(x) for x in params["emissiondaterange"].strip("[]").split(","))
            out = [d for d in out if lo <= int(d["emissionDate"]) <= hi]
        out.sort(key=lambda d: d["id"])
        off = int(params.get("offset") or 0)
        lim = int(params.get("limit") or 50)
        return {"items": out[off : off + lim]}


class FakeStore:
    """Imita ``upsert_documents``: clave lógica (company, office, type, number); PK estable."""

    def __init__(self) -> None:
        self.rows: dict[tuple[int, int, int, int], dict[str, Any]] = {}
        self.details: dict[int, int] = {}

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
        return row

    def folios(self, company_id: int, office_id: int) -> set[int]:
        return {k[3] for k in self.rows if k[0] == company_id and k[1] == office_id and k[2] == 33}


# ---------------------------------------------------------------------------
# Live normal restaurado: mismos params Bsale que pre-a8d9896 (5b7d2df)
# ---------------------------------------------------------------------------


def _capture_live_urls(*, now: datetime, watermark: datetime | None) -> list[str]:
    """Corre ``live_sync_documents`` real hasta la capa HTTP y devuelve las URLs preparadas."""
    urls: list[str] = []

    def _session_get(url, headers=None, params=None, timeout=None):
        prepared = requests.Request("GET", url, params=params).prepare()
        urls.append(prepared.url)
        return SimpleNamespace(
            status_code=200,
            json=lambda: {"items": []},
            text="",
            request=SimpleNamespace(url=prepared.url),
        )

    fake_client = SimpleNamespace(session=SimpleNamespace(get=_session_get), access_token="T")
    conn = MagicMock()
    conn.cursor.return_value.fetchone.return_value = (True,)
    state = {"last_watermark": watermark} if watermark else None
    with patch.object(live_sync_service, "bsale_token_distribuidora_configured", return_value=True), patch.object(
        live_sync_service, "_utc_now", return_value=now
    ), patch.object(live_sync_service, "get_connection", return_value=conn), patch.object(
        live_sync_service, "get_sync_state", return_value=state
    ), patch.object(live_sync_service, "update_sync_state_success"), patch.object(
        live_sync_service, "BsaleClient", return_value=fake_client
    ), patch.object(live_sync_service, "_bsale_token", return_value="T"), patch.object(
        live_sync_service, "log_tx"
    ), patch.object(live_sync_service, "pg_backend_pid", return_value=1), patch.object(
        sync_service, "release_transaction"
    ):
        live_sync_service.live_sync_documents(strict_token=True)
    return urls


def _pre_a8d9896_url(desde: int, hasta: int) -> str:
    """URL que generaba ``_fetch_documents_window`` + ``_documents_get_resync`` en 5b7d2df."""
    return (
        "https://api.bsale.io/v1/documents.json"
        f"?limit=50&offset=0&emissiondaterange=%5B{desde}%2C{hasta}%5D&officeid=1"
    )


def test_live_restored_params_recent_watermark_equal_pre_a8d9896():
    wm = NOW - timedelta(minutes=5)
    urls = _capture_live_urls(now=NOW, watermark=wm)
    desde = int((NOW - timedelta(hours=2)).timestamp())
    hasta = int(NOW.timestamp())
    expected = _pre_a8d9896_url(desde, hasta)
    assert urls == [expected, expected], "dos pasadas: OC (33) y ventas (1/6/9)"


def test_live_restored_params_old_watermark_uses_overlap_like_pre_a8d9896():
    wm = NOW - timedelta(hours=6)
    urls = _capture_live_urls(now=NOW, watermark=wm)
    desde = int((wm - timedelta(seconds=900)).timestamp())
    expected = _pre_a8d9896_url(desde, int(NOW.timestamp()))
    assert urls == [expected, expected]


def test_live_restored_has_no_recent_changes():
    urls = _capture_live_urls(now=NOW, watermark=None)
    for url in urls:
        assert "generationdaterange" not in url
        assert "documenttypeid" not in url
    assert not hasattr(live_sync_service, "live_documents_emission_window")
    assert not hasattr(sync_service, "_local_document_unchanged")


# ---------------------------------------------------------------------------
# Dispatch del job: sin args = live normal; herramientas nunca caen al live
# ---------------------------------------------------------------------------


def test_job_without_args_runs_live_only():
    with patch.object(job, "_main_live", return_value=0) as live, patch.object(job, "_main_tools") as tools:
        assert job.main([]) == 0
    live.assert_called_once()
    tools.assert_not_called()


def test_job_canary_dry_run_does_not_touch_live_or_repair():
    with patch.object(job, "live_sync_documents") as live, patch.object(
        job, "_run_canary", return_value={"found_in_bsale": True}
    ) as canary, patch.object(job, "_run_repair") as repair, patch(
        "backend.utils.bsale_token_env.require_bsale_token", return_value="T"
    ):
        rc = job.main(["--company-id", "3", "--office-id", "1", "--oc-number", "69924", "--dry-run"])
    assert rc == 0
    canary.assert_called_once()
    repair.assert_not_called()
    live.assert_not_called()


def test_job_reconcile_recent_does_not_touch_live():
    with patch.object(job, "live_sync_documents") as live, patch.object(
        job, "_run_reconcile_recent", return_value={"errors": 0}
    ) as rec, patch("backend.utils.bsale_token_env.require_bsale_token", return_value="T"):
        rc = job.main(["--reconcile-recent", "--days", "3", "--dry-run"])
    assert rc == 0
    rec.assert_called_once()
    assert rec.call_args.kwargs["apply"] is False
    live.assert_not_called()


def test_reconcile_recent_does_not_modify_live_params():
    before = _capture_live_urls(now=NOW, watermark=NOW - timedelta(minutes=5))
    _reconcile(FakeBsale([_oc(1, 69882, generated=NOW)]), FakeStore())
    after = _capture_live_urls(now=NOW, watermark=NOW - timedelta(minutes=5))
    assert before == after


# ---------------------------------------------------------------------------
# Reconcile recent (herramienta manual)
# ---------------------------------------------------------------------------


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


def test_reconcile_recovers_gap_once_with_details():
    oc82 = _oc(4000082, 69882, generated=NOW - timedelta(days=1), emission=NOW - timedelta(days=1))
    bsale = FakeBsale([oc82], details={4000082: 12})
    store = FakeStore()
    out = _reconcile(bsale, store, days=3)
    assert out["missing_folios"] == [69882] and out["repaired"] == 1
    assert store.details[store.rows[(3, 1, 33, 69882)]["document_id"]] == 12
    again = _reconcile(bsale, store, days=3)
    assert again["missing"] == 0 and again["repaired"] == 0


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
    assert _reconcile(bsale, store)["missing"] == 0
    assert len([k for k in store.rows if k[3] == 69882]) == 1
    assert store.rows[(3, 1, 33, 69882)]["document_id"] == local_pk


def test_reconcile_pagination_and_budget():
    docs = [_oc(5_000_000 + i, 70_000 + i, generated=NOW - timedelta(minutes=i)) for i in range(110)]
    bsale = FakeBsale(docs)
    store = FakeStore()
    out = _reconcile(bsale, store, apply=False)
    assert out["api_pages"] == 3 and not out["api_truncated_by_budget"]
    assert out["missing"] == 110
    assert _reconcile(bsale, store, apply=False, max_pages=2)["api_truncated_by_budget"] is True
    assert _reconcile(bsale, store, max_repairs=10)["repaired"] == 10
    assert _reconcile(bsale, store, max_repairs=200)["repaired"] == 100
    assert _reconcile(bsale, store)["missing"] == 0


def test_reconcile_same_folio_other_office_not_confused():
    other = _oc(4000999, 69882, generated=NOW, office=2)
    bsale = FakeBsale([other, _oc(4000082, 69882, generated=NOW)], details={4000082: 2})
    store = FakeStore()
    store.upsert(other, company_id=3, office_id=2, details=9)
    out = _reconcile(bsale, store)
    assert out["missing_folios"] == [69882]
    assert store.rows[(3, 1, 33, 69882)]["source_document_id"] == 4000082
    assert store.rows[(3, 2, 33, 69882)]["source_document_id"] == 4000999


def test_reconcile_only_type_33_filtered_client_side():
    invoice = _oc(4000500, 69882, generated=NOW, doc_type=6)
    oc = _oc(4000501, 69883, generated=NOW)
    store = FakeStore()
    out = _reconcile(FakeBsale([invoice, oc]), store)
    assert out["bsale_ignored_other_types"] == 1
    assert out["missing_folios"] == [69883]


def test_emission_three_days_old_inside_reconcile_window():
    old = _midnight(NOW - timedelta(days=2))
    assert emission_visible_in_window(old, emission_day_window(NOW, days_back=3))


# ---------------------------------------------------------------------------
# Canario (read-only)
# ---------------------------------------------------------------------------


def test_canary_69924_reports_bsale_and_local_fields():
    now = datetime(2026, 9, 30, 16, 11, tzinfo=UTC)
    oc = _oc(3921412, 69924, generated=datetime(2026, 9, 30, 15, 55, 11, tzinfo=UTC), total=218694)
    bsale = FakeBsale([oc], details={3921412: 6})
    rep = run_oc_folio_canary(
        bsale, folio=69924, company_id=3, office_id=1, local_loader=lambda f, ids: [], now=now
    )
    assert rep["found_in_bsale"] and not rep["found_locally"]
    assert rep["bsale_document_id"] == 3921412 and rep["number"] == 69924
    assert rep["document_type_id"] == 33 and rep["bsale_office_id"] == 1
    assert rep["details_count"] == 6 and rep["client"] == "Cliente Test"
    assert rep["local_document_id"] is None
    assert all(path in ("/documents.json",) or path.startswith(("/documents/", "/clients/")) for path, _ in bsale.calls)


def test_canary_detects_other_office_and_pk_collision():
    bsale = FakeBsale([_oc(4000082, 69882, generated=NOW, office=7)])
    rep = run_oc_folio_canary(
        bsale, folio=69882, company_id=3, office_id=1, local_loader=lambda f, ids: [], now=NOW
    )
    assert rep["diagnosis"]["primary_cause"] == "bsale_folio_only_in_other_office"

    oc = _oc(4000082, 69882, generated=NOW)
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
