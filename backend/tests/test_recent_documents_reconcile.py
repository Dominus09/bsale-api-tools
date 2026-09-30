"""Regresión OCs 69882 / 69924: live por emissionDate día completo (hoy + ayer) y reconciliación."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import MagicMock, patch

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
        "netAmount": round(total / 1.19),
        "taxAmount": total - round(total / 1.19),
        "state": state,
        "commercialState": 0,
        "document_type": {"id": str(doc_type)},
        "office": {"id": str(office)},
        "client": {"id": "3411"},
    }


class FakeBsale:
    """Filtra como Bsale documenta: officeid, documenttypeid, number, emissiondaterange.

    Cualquier otro parámetro (p. ej. ``generationdaterange``) se ignora, igual que la API
    ignora parámetros no reconocidos.
    """

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


# ---------------------------------------------------------------------------
# Live documents (A)
# ---------------------------------------------------------------------------


def _run_live(bsale: FakeBsale, store: FakeStore, *, now: datetime, watermark: datetime | None = None):
    conn = MagicMock()
    conn.cursor.return_value.fetchone.return_value = (True,)
    state = {"last_watermark": watermark} if watermark else None

    def _resync_get(_client, params):
        return bsale.get("/documents.json", {**params, "officeid": 1})

    def _process(_client, _cur, _conn, row, stats):
        doc = row["_bsale_document"]
        n = len(bsale.get(f"/documents/{doc['id']}/details.json", {"limit": 50})["items"])
        store.upsert(doc, company_id=3, office_id=1, details=n)
        stats["documents_processed"] += 1

    with patch.object(live_sync_service, "bsale_token_distribuidora_configured", return_value=True), patch.object(
        live_sync_service, "_utc_now", return_value=now
    ), patch.object(live_sync_service, "get_connection", return_value=conn), patch.object(
        live_sync_service, "get_sync_state", return_value=state
    ), patch.object(live_sync_service, "update_sync_state_success"), patch.object(
        live_sync_service, "BsaleClient"
    ), patch.object(live_sync_service, "_bsale_token", return_value="x"), patch.object(
        live_sync_service, "log_tx"
    ), patch.object(live_sync_service, "pg_backend_pid", return_value=1), patch.object(
        sync_service, "_documents_get_resync", side_effect=_resync_get
    ), patch.object(sync_service, "_process_one_pending_document_row", side_effect=_process), patch.object(
        sync_service, "release_transaction"
    ), patch.object(sync_service.time, "sleep"):
        return live_sync_service.live_sync_documents(strict_token=True)


def test_live_window_starts_yesterday_midnight_and_ends_now():
    start, end = live_sync_service.live_documents_emission_window(NOW)
    assert start == datetime(2026, 9, 28, tzinfo=UTC)
    assert end == NOW


def test_live_sends_historic_range_shape_never_generationdaterange():
    bsale = FakeBsale([])
    store = FakeStore()
    stats = _run_live(bsale, store, now=NOW)
    doc_calls = [p for path, p in bsale.calls if path == "/documents.json"]
    assert len(doc_calls) == 1, "una sola pasada (OC + ventas filtradas en cliente)"
    lo, hi = emission_day_window(NOW)
    params = doc_calls[0]
    assert "generationdaterange" not in params
    assert "documenttypeid" not in params
    assert params["emissiondaterange"] == f"[{lo},{hi}]"
    assert params["officeid"] == 1
    assert stats["documents_filter"] == "emissiondaterange"
    assert stats["range_from_epoch"] == lo and stats["range_to_epoch"] == hi
    assert stats["document_type_ids_client_filter"] == [1, 6, 9, 33]


def test_case_69924_created_today_is_fetched():
    now = datetime(2026, 9, 30, 16, 11, tzinfo=UTC)
    oc = _oc(3921412, 69924, generated=datetime(2026, 9, 30, 15, 55, 11, tzinfo=UTC), total=218694)
    assert oc["emissionDate"] == _midnight(now)
    bsale = FakeBsale([oc], details={3921412: 6})
    store = FakeStore()
    _run_live(bsale, store, now=now, watermark=now - timedelta(minutes=5))
    row = store.rows[(3, 1, 33, 69924)]
    assert row["source_document_id"] == 3921412
    assert store.details[row["document_id"]] == 6


def test_case_69882_emitted_yesterday_generated_today_enters():
    now = datetime(2026, 9, 29, 15, 20, tzinfo=UTC)
    oc = _oc(
        3915000,
        69882,
        generated=datetime(2026, 9, 29, 15, 11, tzinfo=UTC),
        emission=datetime(2026, 9, 28, tzinfo=UTC),
    )
    bsale = FakeBsale([oc], details={3915000: 12})
    store = FakeStore()
    _run_live(bsale, store, now=now, watermark=now - timedelta(minutes=2))
    assert 69882 in store.folios(3, 1)
    assert store.details[store.rows[(3, 1, 33, 69882)]["document_id"]] == 12


def test_late_oc_with_advanced_watermark_still_enters_and_is_idempotent():
    t1 = datetime(2026, 9, 29, 12, 0, tzinfo=UTC)
    oc81 = _oc(4000081, 69881, generated=t1 - timedelta(minutes=30))
    oc83 = _oc(4000083, 69883, generated=t1 - timedelta(minutes=20))
    bsale = FakeBsale([oc81, oc83], details={4000081: 3, 4000082: 12, 4000083: 4})
    store = FakeStore()
    _run_live(bsale, store, now=t1)
    assert store.folios(3, 1) == {69881, 69883}

    # 69882 aparece horas después, emitida ayer; el watermark ya está muy adelante.
    bsale.docs.append(
        _oc(4000082, 69882, generated=t1 - timedelta(hours=26), emission=t1 - timedelta(days=1))
    )
    t2 = t1 + timedelta(hours=6)
    _run_live(bsale, store, now=t2, watermark=t2 - timedelta(minutes=5))
    assert store.folios(3, 1) == {69881, 69882, 69883}

    _run_live(bsale, store, now=t2 + timedelta(minutes=5), watermark=t2)
    assert sum(1 for k in store.rows if k[3] == 69882) == 1
    assert len(store.rows) == 3
    assert store.details[store.rows[(3, 1, 33, 69882)]["document_id"]] == 12


def test_live_paginates_all_pages():
    docs = [
        _oc(5_000_000 + i, 70_000 + i, generated=NOW - timedelta(minutes=i)) for i in range(120)
    ]
    bsale = FakeBsale(docs)
    store = FakeStore()
    _run_live(bsale, store, now=NOW)
    assert len(store.folios(3, 1)) == 120


def test_live_ignores_other_office_and_non_oc_types_in_oc_pass():
    bsale = FakeBsale(
        [
            _oc(1, 69882, generated=NOW, office=2),
            _oc(2, 69883, generated=NOW, doc_type=6),
            _oc(3, 69884, generated=NOW),
        ]
    )
    store = FakeStore()
    _run_live(bsale, store, now=NOW)
    assert store.folios(3, 1) == {69884}
    assert (3, 1, 6, 69883) in store.rows  # la factura entra por el pase de ventas
    assert not any(k[1] == 2 for k in store.rows)


# ---------------------------------------------------------------------------
# Salto seguro de documentos sin cambios
# ---------------------------------------------------------------------------


def _row_for(doc: dict[str, Any]) -> dict[str, Any]:
    row = document_dict_from_bsale(doc, company_id=3, default_office_id=1)
    row["_bsale_document"] = doc
    return row


def _cur_returning(local: tuple | None) -> MagicMock:
    cur = MagicMock()
    cur.fetchone.return_value = local
    return cur


def test_unchanged_detection_requires_same_source_amounts_and_details():
    doc = _oc(3921412, 69924, generated=NOW, total=218694)
    row = _row_for(doc)
    same = (
        "3921412",
        str(doc["generationDate"]),
        218694,
        doc["netAmount"],
        doc["taxAmount"],
        0,
        0,
        True,
    )
    assert sync_service._local_document_unchanged(_cur_returning(same), row) is True
    assert sync_service._local_document_unchanged(_cur_returning(None), row) is False
    assert sync_service._local_document_unchanged(_cur_returning(("999",) + same[1:]), row) is False
    changed_total = same[:2] + (1,) + same[3:]
    assert sync_service._local_document_unchanged(_cur_returning(changed_total), row) is False
    no_details = same[:7] + (False,)
    assert sync_service._local_document_unchanged(_cur_returning(no_details), row) is False


def test_process_row_skips_upsert_and_children_when_unchanged():
    doc = _oc(3921412, 69924, generated=NOW, total=218694)
    row = _row_for(doc)
    stats: dict[str, Any] = {"documents_processed": 0, "_skip_unchanged_documents": True}
    with patch.object(sync_service, "_local_document_unchanged", return_value=True), patch.object(
        sync_service, "upsert_documents"
    ) as up, patch.object(sync_service, "_refresh_document_children") as ch, patch.object(
        sync_service, "release_transaction"
    ):
        sync_service._process_one_pending_document_row(MagicMock(), MagicMock(), MagicMock(), row, stats)
    up.assert_not_called()
    ch.assert_not_called()
    assert stats["documents_unchanged_skipped"] == 1


def test_process_row_persists_when_changed_even_with_skip_flag():
    doc = _oc(3921412, 69924, generated=NOW, total=218694)
    row = _row_for(doc)
    stats: dict[str, Any] = {"documents_processed": 0, "_skip_unchanged_documents": True}
    with patch.object(sync_service, "_local_document_unchanged", return_value=False), patch.object(
        sync_service, "upsert_documents"
    ) as up, patch.object(sync_service, "_refresh_document_children") as ch, patch.object(
        sync_service, "release_transaction"
    ), patch.object(sync_service, "log_tx"), patch.object(sync_service, "log_order_sync_audit"):
        sync_service._process_one_pending_document_row(MagicMock(), MagicMock(), MagicMock(), row, stats)
    up.assert_called_once()
    ch.assert_called_once()
    assert stats["documents_processed"] == 1


# ---------------------------------------------------------------------------
# Reconciliación reciente (B)
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


def test_reconcile_uses_emission_full_days_and_recovers_gap_once():
    oc82 = _oc(4000082, 69882, generated=NOW - timedelta(days=1), emission=NOW - timedelta(days=1))
    bsale = FakeBsale([oc82], details={4000082: 12})
    store = FakeStore()
    out = _reconcile(bsale, store, days=3)
    params = [p for path, p in bsale.calls if path == "/documents.json"][0]
    assert "generationdaterange" not in params and "documenttypeid" not in params
    assert out["documents_filter"] == "emissiondaterange"
    assert out["range_to_epoch"] == int(NOW.timestamp())
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

    store.rows.clear()
    out = _reconcile(bsale, store)
    assert out["results"][0]["bsale_document_id"] == 4100082


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
    assert out["bsale_items"] == 2
    assert out["bsale_ignored_other_types"] == 1
    assert out["missing_folios"] == [69883]
    assert (3, 1, 6, 69882) not in store.rows


# ---------------------------------------------------------------------------
# Canario (C) — read-only
# ---------------------------------------------------------------------------


def test_canary_69924_reports_fields_and_is_visible_to_live_window():
    now = datetime(2026, 9, 30, 16, 11, tzinfo=UTC)
    oc = _oc(3921412, 69924, generated=datetime(2026, 9, 30, 15, 55, 11, tzinfo=UTC), total=218694)
    bsale = FakeBsale([oc], details={3921412: 6})
    rep = run_oc_folio_canary(
        bsale, folio=69924, company_id=3, office_id=1, local_loader=lambda f, ids: [], now=now
    )
    assert rep["found_in_bsale"] and not rep["found_locally"]
    assert rep["bsale_document_id"] == 3921412 and rep["details_count"] == 6
    diag = rep["diagnosis"]
    assert diag["emission_visible_to_live_window_now"] is True
    assert "live_sync_emission_window_blind" not in diag["reasons"]
    assert all(path != "/documents.json" or "generationdaterange" not in p for path, p in bsale.calls)


def test_emission_three_days_old_is_outside_live_but_inside_reconcile():
    old = _midnight(NOW - timedelta(days=2))
    assert not emission_visible_in_window(old, emission_day_window(NOW, days_back=1))
    assert emission_visible_in_window(old, emission_day_window(NOW, days_back=3))


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
