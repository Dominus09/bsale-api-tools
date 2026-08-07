"""Regresión: header sin details no es sync completa; reintento y peso tras details."""

from __future__ import annotations

from contextlib import nullcontext
from typing import Any
from unittest.mock import MagicMock, patch

from backend.services.distribuidora.oc_reconciliation_service import (
    needs_detail_sync,
    reconcile_one_oc,
)
from backend.services.distribuidora.oc_source_resolver import compute_oc_source_hash
from backend.services.distribuidora.sync_service import _refresh_document_children


LOCAL_ID = 3853320
SOURCE_ID = 3853408
FOLIO = 68701


def _bsale_doc(source_id: int = SOURCE_ID, *, total: int = 85880) -> dict[str, Any]:
    return {
        "id": source_id,
        "number": FOLIO,
        "state": 0,
        "commercialState": 0,
        "emissionDate": 1786100000,
        "generationDate": 1786100000,
        "modificationDate": 1786100100,
        "totalAmount": total,
        "netAmount": 70000,
        "taxAmount": 15880,
        "office": {"id": 1},
        "company": {"id": 3},
        "document_type": {"id": 33},
        "client": {"id": 1},
    }


DETAILS = [
    {
        "id": 9100001,
        "lineNumber": 0,
        "quantity": 2.0,
        "netUnitValue": 1000,
        "totalUnitValue": 1190,
        "netAmount": 2000,
        "taxAmount": 380,
        "totalAmount": 2380,
        "variant": {"id": 100, "code": "ABC", "description": "X"},
    }
]


class _Client:
    def __init__(self, details: list[dict] | None = None, doc: dict | None = None):
        self.details = details if details is not None else DETAILS
        self.doc = doc or _bsale_doc()
        self.gets: list[str] = []

    def get(self, path: str, params: dict | None = None, **kwargs):
        self.gets.append(path)
        if path.startswith("/documents.json") or path.endswith("documents.json"):
            return {"items": [self.doc], "count": 1}
        if "/details.json" in path:
            return {"items": self.details, "count": len(self.details)}
        if path.endswith("/attributes.json"):
            return {"items": []}
        if path.endswith("/references.json"):
            return {"items": []}
        if path.endswith("/sellers.json"):
            return {"items": []}
        if path.startswith("/documents/") and path.endswith(".json") is False:
            return self.doc
        # GET /documents/{id}.json variants
        if "/documents/" in path and path.count("/") >= 2:
            return self.doc
        return {"items": [self.doc]}


def test_needs_detail_sync_when_hash_would_match_but_local_empty():
    pg = {"total_amount": 85880, "number": FOLIO, "state": 0}
    assert (
        needs_detail_sync(
            pg_document=pg,
            pg_details=[],
            bsale_document=_bsale_doc(),
            bsale_details=DETAILS,
        )
        is True
    )


def test_needs_detail_sync_false_when_local_has_lines():
    assert (
        needs_detail_sync(
            pg_document={"total_amount": 85880},
            pg_details=[{"detail_id": 1}],
            bsale_document=_bsale_doc(),
            bsale_details=DETAILS,
        )
        is False
    )


def test_hash_match_empty_details_with_total_does_not_early_return():
    """source_hash de header+0 líneas NO marca already_in_sync si monto > 0."""
    digest = compute_oc_source_hash(_bsale_doc(), [])
    pg_document = {
        "document_id": LOCAL_ID,
        "number": FOLIO,
        "company_id": 3,
        "office_id": 1,
        "document_type_id": 33,
        "total_amount": 85880,
        "net_amount": 70000,
        "tax_amount": 15880,
        "state": 0,
        "commercial_state": 0,
        "raw_data": {"id": SOURCE_ID},
        "source_document_id": SOURCE_ID,
        "source_hash": digest,
        "invoice_link": {},
    }
    with (
        patch(
            "backend.services.distribuidora.oc_reconciliation_service._load_local_oc",
            return_value=(pg_document, []),
        ),
        patch(
            "backend.services.distribuidora.oc_reconciliation_service.discover_oc_sources",
            return_value={
                "folio": FOLIO,
                "active_document": _bsale_doc(),
                "documents": [],
            },
        ),
        patch(
            "backend.services.distribuidora.oc_reconciliation_service.fetch_all_document_details",
            return_value=[],
        ),
        patch(
            "backend.services.distribuidora.oc_reconciliation_service.fetch_document_attributes_payload",
            return_value={"items": []},
        ),
        patch(
            "backend.services.distribuidora.oc_reconciliation_service.fetch_document_references_payload",
            return_value={"items": []},
        ),
        patch(
            "backend.services.distribuidora.oc_reconciliation_service.load_local_observaciones",
            return_value=None,
        ),
        patch(
            "backend.services.distribuidora.oc_reconciliation_service._projected_weight",
            return_value={"peso_total_kg": None, "status": "unavailable"},
        ),
        patch(
            "backend.services.distribuidora.oc_reconciliation_service._oc_writer_locks",
            return_value=nullcontext(),
        ),
        patch(
            "backend.services.distribuidora.oc_reconciliation_service.get_connection"
        ) as get_conn,
        patch(
            "backend.services.distribuidora.oc_reconciliation_service.upsert_documents",
        ),
        patch(
            "backend.services.distribuidora.oc_reconciliation_service.replace_document_details",
        ) as replace_details,
        patch(
            "backend.services.distribuidora.oc_reconciliation_service.replace_document_attributes",
            return_value=0,
        ),
        patch(
            "backend.services.distribuidora.oc_reconciliation_service.replace_document_references",
            return_value=0,
        ),
        patch(
            "backend.services.distribuidora.oc_reconciliation_service._invalidate_affected_dispatch_plans",
            return_value=0,
        ),
        patch(
            "backend.services.distribuidora.oc_reconciliation_service._assert_source_schema",
        ),
        patch(
            "backend.services.distribuidora.oc_reconciliation_service._assert_plan_invalidation_schema",
        ),
        patch(
            "backend.services.distribuidora.oc_reconciliation_service.document_dict_from_bsale",
            return_value={"document_id": LOCAL_ID},
        ),
        patch(
            "backend.services.distribuidora.oc_reconciliation_service._mark_reconciliation_attempt",
        ) as mark_ok,
    ):
        conn = MagicMock()
        cur = MagicMock()
        conn.cursor.return_value = cur
        get_conn.return_value = conn
        report = reconcile_one_oc(
            _Client(details=[]),
            folio=FOLIO,
            local_document_id=LOCAL_ID,
            dry_run=False,
        )

    assert report["status"] == "header_ok_details_pending"
    assert report["needs_detail_sync"] is True
    assert report["source_hash_matches"] is True
    replace_details.assert_not_called()
    mark_ok.assert_not_called()


def test_missing_local_details_recovers_from_bsale_source():
    digest = compute_oc_source_hash(_bsale_doc(), DETAILS)
    pg_document = {
        "document_id": LOCAL_ID,
        "number": FOLIO,
        "company_id": 3,
        "office_id": 1,
        "document_type_id": 33,
        "total_amount": 85880,
        "net_amount": 70000,
        "tax_amount": 15880,
        "state": 0,
        "commercial_state": 0,
        "raw_data": {"id": SOURCE_ID},
        "source_document_id": SOURCE_ID,
        "source_hash": digest,
        "invoice_link": {},
    }
    captured: dict[str, Any] = {}

    def fake_replace(cur, doc_id, items, **kwargs):
        captured["replaced"] = (doc_id, len(items))
        return len(items)

    with (
        patch(
            "backend.services.distribuidora.oc_reconciliation_service._load_local_oc",
            return_value=(pg_document, []),
        ),
        patch(
            "backend.services.distribuidora.oc_reconciliation_service.discover_oc_sources",
            return_value={
                "folio": FOLIO,
                "active_document": _bsale_doc(),
                "documents": [],
            },
        ),
        patch(
            "backend.services.distribuidora.oc_reconciliation_service.fetch_all_document_details",
            return_value=DETAILS,
        ),
        patch(
            "backend.services.distribuidora.oc_reconciliation_service.fetch_document_attributes_payload",
            return_value={"items": []},
        ),
        patch(
            "backend.services.distribuidora.oc_reconciliation_service.fetch_document_references_payload",
            return_value={"items": []},
        ),
        patch(
            "backend.services.distribuidora.oc_reconciliation_service.load_local_observaciones",
            return_value=None,
        ),
        patch(
            "backend.services.distribuidora.oc_reconciliation_service._projected_weight",
            return_value={"peso_total_kg": 10.0, "status": "calculated"},
        ),
        patch(
            "backend.services.distribuidora.oc_reconciliation_service._oc_writer_locks",
            return_value=nullcontext(),
        ),
        patch(
            "backend.services.distribuidora.oc_reconciliation_service.get_connection"
        ) as get_conn,
        patch(
            "backend.services.distribuidora.oc_reconciliation_service.upsert_documents",
        ),
        patch(
            "backend.services.distribuidora.oc_reconciliation_service.replace_document_details",
            side_effect=fake_replace,
        ),
        patch(
            "backend.services.distribuidora.oc_reconciliation_service.replace_document_attributes",
            return_value=0,
        ),
        patch(
            "backend.services.distribuidora.oc_reconciliation_service.replace_document_references",
            return_value=0,
        ),
        patch(
            "backend.services.distribuidora.oc_reconciliation_service.recalculate_order_weight_in_transaction",
            return_value={"peso_total_kg": 10.0, "porcentaje_cobertura": 100.0},
        ),
        patch(
            "backend.services.distribuidora.oc_reconciliation_service._invalidate_affected_dispatch_plans",
            return_value=0,
        ),
        patch(
            "backend.services.distribuidora.oc_reconciliation_service._assert_source_schema",
        ),
        patch(
            "backend.services.distribuidora.oc_reconciliation_service._assert_plan_invalidation_schema",
        ),
        patch(
            "backend.services.distribuidora.oc_reconciliation_service.document_dict_from_bsale",
            return_value={"document_id": LOCAL_ID},
        ),
        patch(
            "backend.services.distribuidora.sync_related_service.sync_related_for_single_oc",
            return_value={"rows_inserted": 0},
        ),
        patch(
            "backend.services.distribuidora.oc_reconciliation_service.load_local_invoice_link_flags",
            return_value={},
        ),
    ):
        conn = MagicMock()
        cur = MagicMock()
        conn.cursor.return_value = cur
        get_conn.return_value = conn
        report = reconcile_one_oc(
            _Client(),
            folio=FOLIO,
            local_document_id=LOCAL_ID,
            dry_run=False,
        )

    assert report["status"] == "synced"
    assert report["wrote"] is True
    assert captured["replaced"] == (LOCAL_ID, 1)
    assert report["details_synced"] is True
    assert report["needs_detail_sync"] is False
    assert report.get("peso_despues_kg") == 10.0


def test_empty_bsale_details_with_total_marks_pending_not_complete():
    pg_document = {
        "document_id": LOCAL_ID,
        "number": FOLIO,
        "company_id": 3,
        "office_id": 1,
        "document_type_id": 33,
        "total_amount": 85880,
        "net_amount": 70000,
        "tax_amount": 15880,
        "state": 0,
        "commercial_state": 0,
        "raw_data": {"id": SOURCE_ID},
        "source_document_id": SOURCE_ID,
        "source_hash": None,
        "invoice_link": {},
    }
    with (
        patch(
            "backend.services.distribuidora.oc_reconciliation_service._load_local_oc",
            return_value=(pg_document, []),
        ),
        patch(
            "backend.services.distribuidora.oc_reconciliation_service.discover_oc_sources",
            return_value={
                "folio": FOLIO,
                "active_document": _bsale_doc(),
                "documents": [],
            },
        ),
        patch(
            "backend.services.distribuidora.oc_reconciliation_service.fetch_all_document_details",
            return_value=[],
        ),
        patch(
            "backend.services.distribuidora.oc_reconciliation_service.fetch_document_attributes_payload",
            return_value={"items": []},
        ),
        patch(
            "backend.services.distribuidora.oc_reconciliation_service.fetch_document_references_payload",
            return_value={"items": []},
        ),
        patch(
            "backend.services.distribuidora.oc_reconciliation_service.load_local_observaciones",
            return_value=None,
        ),
        patch(
            "backend.services.distribuidora.oc_reconciliation_service._projected_weight",
            return_value={"peso_total_kg": None, "status": "unavailable"},
        ),
        patch(
            "backend.services.distribuidora.oc_reconciliation_service._oc_writer_locks",
            return_value=nullcontext(),
        ),
        patch(
            "backend.services.distribuidora.oc_reconciliation_service.get_connection"
        ) as get_conn,
        patch(
            "backend.services.distribuidora.oc_reconciliation_service.upsert_documents",
        ),
        patch(
            "backend.services.distribuidora.oc_reconciliation_service.replace_document_details",
        ) as replace_details,
        patch(
            "backend.services.distribuidora.oc_reconciliation_service.replace_document_attributes",
            return_value=0,
        ),
        patch(
            "backend.services.distribuidora.oc_reconciliation_service.replace_document_references",
            return_value=0,
        ),
        patch(
            "backend.services.distribuidora.oc_reconciliation_service._invalidate_affected_dispatch_plans",
            return_value=0,
        ),
        patch(
            "backend.services.distribuidora.oc_reconciliation_service._assert_source_schema",
        ),
        patch(
            "backend.services.distribuidora.oc_reconciliation_service._assert_plan_invalidation_schema",
        ),
        patch(
            "backend.services.distribuidora.oc_reconciliation_service.document_dict_from_bsale",
            return_value={"document_id": LOCAL_ID},
        ),
    ):
        conn = MagicMock()
        cur = MagicMock()
        conn.cursor.return_value = cur
        get_conn.return_value = conn
        report = reconcile_one_oc(
            _Client(details=[]),
            folio=FOLIO,
            local_document_id=LOCAL_ID,
            dry_run=False,
        )

    assert report["status"] == "header_ok_details_pending"
    assert report["needs_detail_sync"] is True
    replace_details.assert_not_called()


def test_refresh_children_local_eq_source_writes_details_and_weight():
    captured: dict[str, Any] = {"gets": [], "replace": None, "weight": False}

    def fake_get(path: str, params: dict | None = None, **kwargs):
        captured["gets"].append(path)
        if path.endswith("/details.json"):
            return {"items": DETAILS, "count": 1}
        return {"items": []}

    with (
        patch("backend.services.distribuidora.sync_service.release_transaction"),
        patch("backend.services.distribuidora.sync_service.log_tx"),
        patch("backend.services.distribuidora.sync_service.safe_rollback"),
        patch(
            "backend.services.distribuidora.sync_service.replace_document_details",
            side_effect=lambda _c, doc_id, items: (
                captured.__setitem__("replace", (doc_id, len(items))),
                len(items),
            )[1],
        ),
        patch(
            "backend.services.distribuidora.sync_service.replace_document_attributes",
            return_value=0,
        ),
        patch(
            "backend.services.distribuidora.sync_service.replace_document_references",
            return_value=0,
        ),
        patch(
            "backend.services.distribuidora.sync_service.replace_document_sellers",
            return_value=0,
        ),
        patch(
            "backend.services.order_weight_service.recalculate_order_weight_in_transaction",
            side_effect=lambda *a, **k: captured.__setitem__("weight", True) or {},
        ),
    ):
        client = MagicMock()
        client.get.side_effect = fake_get
        stats: dict[str, Any] = {}
        _refresh_document_children(
            client,
            MagicMock(),
            MagicMock(),
            3853317,
            33,
            stats,
            raw_document=_bsale_doc(3853317, total=69000),
            folio=68700,
        )

    assert captured["replace"] == (3853317, 1)
    assert captured["weight"] is True
    assert stats.get("last_children_details_pending") is False
    assert any("/documents/3853317/details.json" in g for g in captured["gets"])


def test_refresh_children_local_ne_source_fetches_new_source():
    gets: list[str] = []

    def fake_get(path: str, params: dict | None = None, **kwargs):
        gets.append(path)
        if path.endswith("/details.json"):
            return {"items": DETAILS, "count": 1}
        return {"items": []}

    with (
        patch("backend.services.distribuidora.sync_service.release_transaction"),
        patch("backend.services.distribuidora.sync_service.log_tx"),
        patch("backend.services.distribuidora.sync_service.safe_rollback"),
        patch(
            "backend.services.distribuidora.sync_service.replace_document_details",
            return_value=1,
        ),
        patch(
            "backend.services.distribuidora.sync_service.replace_document_attributes",
            return_value=0,
        ),
        patch(
            "backend.services.distribuidora.sync_service.replace_document_references",
            return_value=0,
        ),
        patch(
            "backend.services.distribuidora.sync_service.replace_document_sellers",
            return_value=0,
        ),
        patch(
            "backend.services.order_weight_service.recalculate_order_weight_in_transaction",
            return_value={},
        ),
    ):
        client = MagicMock()
        client.get.side_effect = fake_get
        stats: dict[str, Any] = {}
        _refresh_document_children(
            client,
            MagicMock(),
            MagicMock(),
            LOCAL_ID,
            33,
            stats,
            raw_document=_bsale_doc(SOURCE_ID),
            folio=FOLIO,
        )

    assert any(f"/documents/{SOURCE_ID}/details.json" in g for g in gets)
    assert not any(f"/documents/{LOCAL_ID}/details.json" in g for g in gets)
    assert stats["last_children_ids_differ"] is True


def test_refresh_children_empty_details_does_not_replace_when_total_gt_0():
    with (
        patch("backend.services.distribuidora.sync_service.release_transaction"),
        patch("backend.services.distribuidora.sync_service.log_tx"),
        patch("backend.services.distribuidora.sync_service.safe_rollback"),
        patch(
            "backend.services.distribuidora.sync_service.replace_document_details",
        ) as replace_details,
        patch(
            "backend.services.distribuidora.sync_service.replace_document_attributes",
            return_value=0,
        ),
        patch(
            "backend.services.distribuidora.sync_service.replace_document_references",
            return_value=0,
        ),
        patch(
            "backend.services.distribuidora.sync_service.replace_document_sellers",
            return_value=0,
        ),
    ):
        client = MagicMock()
        client.get.side_effect = lambda path, params=None, **kw: {"items": []}
        stats: dict[str, Any] = {}
        _refresh_document_children(
            client,
            MagicMock(),
            MagicMock(),
            LOCAL_ID,
            33,
            stats,
            raw_document=_bsale_doc(),
            folio=FOLIO,
        )

    replace_details.assert_not_called()
    assert stats["last_children_details_pending"] is True
    assert stats["header_ok_details_pending"] == 1
