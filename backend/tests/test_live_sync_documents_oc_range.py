"""Batch ``--oc-from/--oc-to``: mismo canario y reparación puntual que ``--oc-number``."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from typing import Any
from unittest.mock import patch

import pytest

from backend.jobs import live_sync_documents as job
from backend.repositories.distribuidora.documents_repo import document_dict_from_bsale
from backend.services.distribuidora.recent_documents_reconcile_service import run_oc_folio_canary

UTC = timezone.utc
NOW = datetime(2026, 9, 30, 16, 30, tzinfo=UTC)
FOLIOS = list(range(69906, 69925))
LOCAL = {69906, 69907, 69910}
MISSING_IN_BSALE = 69915
OTHER_OFFICE = 69920
CANCELLED = 69921


def _oc(folio: int, *, office: int = 1, state: int = 0) -> dict[str, Any]:
    return {
        "id": 3_921_000 + folio - 69900,
        "number": folio,
        "emissionDate": int(datetime(2026, 9, 30, tzinfo=UTC).timestamp()),
        "generationDate": int(NOW.timestamp()) - (69925 - folio) * 60,
        "totalAmount": 100_000 + folio,
        "state": state,
        "commercialState": 0,
        "document_type": {"id": "33"},
        "office": {"id": str(office)},
        "client": {"id": "3411"},
    }


class FakeBsale:
    def __init__(self, docs: list[dict[str, Any]]):
        self.docs = docs
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def get(self, path: str, params: dict[str, Any] | None = None, **_kw) -> dict[str, Any]:
        params = dict(params or {})
        self.calls.append((path, params))
        if path.endswith("/details.json"):
            return {"items": [{"id": 1, "lineNumber": 1}, {"id": 2, "lineNumber": 2}]}
        if path.startswith("/clients/"):
            return {"company": "Cliente"}
        out = list(self.docs)
        if "officeid" in params:
            out = [d for d in out if int(d["office"]["id"]) == int(params["officeid"])]
        if "documenttypeid" in params:
            out = [d for d in out if int(d["document_type"]["id"]) == int(params["documenttypeid"])]
        if "number" in params:
            out = [d for d in out if int(d["number"]) == int(params["number"])]
        off = int(params.get("offset") or 0)
        return {"items": out[off : off + int(params.get("limit") or 50)]}


class FakeStore:
    def __init__(self) -> None:
        self.rows: dict[tuple[int, int, int, int], dict[str, Any]] = {}
        self.writes = 0

    def upsert(self, doc: dict[str, Any]) -> dict[str, Any]:
        row = document_dict_from_bsale(doc, company_id=3, default_office_id=1)
        key = (3, 1, 33, row["number"])
        if key in self.rows:
            row["document_id"] = self.rows[key]["document_id"]
        self.rows[key] = row
        self.writes += 1
        return row

    def local_matches(self, folio: int, _ids: list[int]) -> list[dict[str, Any]]:
        row = self.rows.get((3, 1, 33, folio))
        if not row:
            return []
        return [
            {
                "document_id": row["document_id"],
                "company_id": 3,
                "office_id": 1,
                "document_type_id": 33,
                "number": folio,
                "state": 0,
                "source_document_id": row["document_id"],
                "raw_bsale_id": row["document_id"],
                "matched_by": ["folio"],
            }
        ]


def _world() -> tuple[FakeBsale, FakeStore]:
    docs = []
    for f in FOLIOS:
        if f == MISSING_IN_BSALE:
            continue
        docs.append(_oc(f, office=2 if f == OTHER_OFFICE else 1, state=8888 if f == CANCELLED else 0))
    bsale = FakeBsale(docs)
    store = FakeStore()
    for d in docs:
        if d["number"] in LOCAL:
            store.upsert(d)
    store.writes = 0
    return bsale, store


def _run(bsale: FakeBsale, store: FakeStore, *, apply: bool, max_repairs: int = 50):
    def canary(folio: int) -> dict[str, Any]:
        return run_oc_folio_canary(
            bsale, folio=folio, company_id=3, office_id=1, local_loader=store.local_matches, now=NOW
        )

    def repair(folio: int) -> dict[str, Any]:
        before = canary(folio)
        if before["found_locally"]:
            return {"status": "already_present", "wrote": False}
        active = [d for d in bsale.docs if d["number"] == folio and d["office"]["id"] == "1"][0]
        row = store.upsert(active)
        return {
            "status": "synced",
            "wrote": True,
            "local_document_id": row["document_id"],
            "details_replaced": 2,
            "found_locally_after": canary(folio)["found_locally"],
        }

    return job.run_oc_range(
        FOLIOS,
        office_id=1,
        canary=canary,
        repair=repair if apply else None,
        apply=apply,
        max_repairs=max_repairs,
    )


def _actions(out: dict[str, Any]) -> dict[int, str]:
    return {i["folio"]: i["action"] for i in out["items"]}


def test_range_dry_run_classifies_each_folio_without_writes():
    bsale, store = _world()
    out = _run(bsale, store, apply=False)
    acts = _actions(out)
    assert out["folios_requested"] == 19
    assert {f for f, a in acts.items() if a == "already_exists"} == LOCAL
    assert acts[MISSING_IN_BSALE] == "not_found"
    assert acts[OTHER_OFFICE] == "not_eligible"
    assert acts[CANCELLED] == "not_eligible"
    expected_repair = set(FOLIOS) - LOCAL - {MISSING_IN_BSALE, OTHER_OFFICE, CANCELLED}
    assert {f for f, a in acts.items() if a == "would_repair"} == expected_repair
    assert out["would_repair"] == len(expected_repair) == 13
    assert out["already_local"] == 3 and out["not_found"] == 1 and out["not_eligible"] == 2
    assert out["found_in_bsale"] == 18 and out["repaired"] == 0 and out["errors"] == 0
    assert store.writes == 0


def test_range_uses_exact_number_lookup_never_date_ranges():
    bsale, store = _world()
    _run(bsale, store, apply=False)
    doc_queries = [p for path, p in bsale.calls if path == "/documents.json"]
    assert doc_queries
    for params in doc_queries:
        assert "number" in params and params["documenttypeid"] == 33
        assert "emissiondaterange" not in params and "generationdaterange" not in params


def test_apply_repairs_only_eligible_missing_once_and_is_idempotent():
    bsale, store = _world()
    out = _run(bsale, store, apply=True)
    assert out["repaired"] == 13 and out["errors"] == 0
    assert store.writes == 13
    acts = _actions(out)
    assert acts[MISSING_IN_BSALE] == "not_found"
    assert acts[OTHER_OFFICE] == "not_eligible" and acts[CANCELLED] == "not_eligible"
    assert (3, 1, 33, OTHER_OFFICE) not in store.rows
    assert (3, 1, 33, CANCELLED) not in store.rows

    again = _run(bsale, store, apply=True)
    assert again["repaired"] == 0 and again["already_local"] == 16
    assert store.writes == 13
    assert len(store.rows) == 16


def test_apply_respects_max_repairs():
    bsale, store = _world()
    out = _run(bsale, store, apply=True, max_repairs=5)
    assert out["repaired"] == 5 and out["skipped"] == 8
    assert _run(bsale, store, apply=True)["repaired"] == 8


def test_canary_error_counts_as_error_and_continues():
    def canary(folio: int) -> dict[str, Any]:
        if folio == 69908:
            raise RuntimeError("Bsale HTTP 500")
        return {"found_in_bsale": False, "found_locally": False}

    out = job.run_oc_range(
        FOLIOS, office_id=1, canary=canary, repair=None, apply=False, max_repairs=10
    )
    assert out["errors"] == 1 and out["not_found"] == 18


def test_run_oc_range_wires_same_canary_and_repair_per_folio():
    args = argparse.Namespace(
        company_id=3, office_id=1, oc_number=None, oc_from=69906, oc_to=69908, max_repairs=25
    )
    seen: list[int] = []

    def fake_canary(a, _token):
        seen.append(a.oc_number)
        return {
            "found_in_bsale": True,
            "found_locally": False,
            "active_source_selected": True,
            "document_type_id": 33,
            "bsale_office_id": 1,
        }

    with patch.object(job, "_run_canary", side_effect=fake_canary), patch.object(job, "_run_repair") as rep:
        out = job._run_oc_range(args, "T", apply=False)
    assert seen == [69906, 69907, 69908]
    rep.assert_not_called()
    assert out["would_repair"] == 3

    with patch.object(job, "_run_canary", side_effect=fake_canary), patch.object(
        job, "_run_repair", return_value={"status": "synced", "wrote": True, "found_locally_after": True}
    ) as rep:
        out = job._run_oc_range(args, "T", apply=True)
    assert [c.args[0].oc_number for c in rep.call_args_list] == [69906, 69907, 69908]
    assert out["repaired"] == 3


def test_job_range_cli_dispatch_and_validation():
    with patch.object(job, "live_sync_documents") as live, patch.object(
        job, "_run_oc_range", return_value={"errors": 0}
    ) as rng, patch("backend.utils.bsale_token_env.require_bsale_token", return_value="T"):
        rc = job.main(
            ["--company-id", "3", "--office-id", "1", "--oc-from", "69906", "--oc-to", "69924", "--dry-run"]
        )
    assert rc == 0
    assert rng.call_args.kwargs["apply"] is False
    live.assert_not_called()

    with patch("backend.utils.bsale_token_env.require_bsale_token", return_value="T"):
        with pytest.raises(SystemExit):
            job.main(["--oc-from", "69906"])
        with pytest.raises(SystemExit):
            job.main(["--oc-from", "69924", "--oc-to", "69906"])
        with pytest.raises(SystemExit):
            job.main(["--oc-from", "69906", "--oc-to", "69924", "--oc-number", "69910"])
        with pytest.raises(SystemExit):
            job.main(["--oc-from", "69906", "--oc-to", "69924", "--apply"])


def test_job_without_args_still_runs_live_only():
    with patch.object(job, "_main_live", return_value=0) as live, patch.object(job, "_main_tools") as tools:
        assert job.main([]) == 0
    live.assert_called_once()
    tools.assert_not_called()
