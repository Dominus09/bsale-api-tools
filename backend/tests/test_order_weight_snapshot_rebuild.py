"""Snapshot de peso: persistencia, estados, dry-run y recovery sin N+1."""

from __future__ import annotations

from contextlib import nullcontext
from typing import Any
from unittest.mock import MagicMock, patch

from backend.jobs.rebuild_order_weight_snapshots import rebuild_folios
from backend.jobs.repair_oc_missing_details_folios import _summarize_dry_run
from backend.services.distribuidora.oc_reconciliation_service import (
    _projected_weight,
    reconcile_one_oc,
)
from backend.services.distribuidora.oc_source_resolver import compute_oc_source_hash
from backend.services.order_weight_service import build_weight_payload
from backend.services.distribuidora.sync_service import _refresh_document_children


LOCAL_ID = 3853320
SOURCE_ID = 3853408
FOLIO = 68701


def _calc_result(
    *,
    peso: float,
    coverage: float,
    missing: int,
    total: int,
    status: str,
) -> dict[str, Any]:
    summary = {
        "peso_total_kg": peso,
        "productos_totales": total,
        "productos_con_peso": total - missing,
        "productos_sin_peso": missing,
        "productos_manuales": 0,
        "productos_estimados": 0,
        "porcentaje_cobertura": coverage,
    }
    lines = [{"detail_id": i + 1, "warnings": []} for i in range(total)]
    weight = build_weight_payload(summary, lines=lines)
    assert weight["status"] == status
    return {
        "document_id": LOCAL_ID,
        "oc": FOLIO,
        "company_id": 3,
        "office_id": 1,
        **summary,
        "total_weight": peso,
        "coverage_percent": coverage,
        "missing_products": missing,
        "peso_total_kg": peso,
        "weight": weight,
        "lines": lines,
    }


def test_build_weight_payload_calculated_100():
    payload = build_weight_payload(
        {
            "peso_total_kg": 37.353,
            "productos_totales": 6,
            "productos_sin_peso": 0,
        },
        lines=[{"warnings": []}] * 6,
    )
    assert payload["status"] == "calculated"
    assert payload["value_kg"] == 37.353


def test_build_weight_payload_partial_75():
    payload = build_weight_payload(
        {
            "peso_total_kg": 13.52,
            "productos_totales": 4,
            "productos_sin_peso": 1,
        },
        lines=[{"warnings": []}] * 4,
    )
    assert payload["status"] == "partial"
    assert payload["value_kg"] == 13.52


def test_build_weight_payload_partial_80():
    payload = build_weight_payload(
        {
            "peso_total_kg": 160.86,
            "productos_totales": 5,
            "productos_sin_peso": 1,
        },
        lines=[{"warnings": []}] * 5,
    )
    assert payload["status"] == "partial"
    assert payload["value_kg"] == 160.86


def test_projected_weight_uses_in_memory_calc_not_snapshot():
    calc = _calc_result(
        peso=37.353, coverage=100.0, missing=0, total=6, status="calculated"
    )
    with patch(
        "backend.services.distribuidora.oc_reconciliation_service.calculate_order_weight",
        return_value=calc,
    ):
        weight = _projected_weight(
            local_document_id=LOCAL_ID,
            pg_details=[{"detail_id": 1}] * 6,
            bsale_details=[{"id": 1, "quantity": 1, "variant": {"id": 1}}] * 6,
        )
    assert weight["peso_total_kg"] == 37.353
    assert weight["status"] == "calculated"
    assert weight["source"] == "local_details_recalc"


def test_dry_run_summary_shows_projected_weight_real():
    report = {
        "folio": FOLIO,
        "status": "dry_run_needs_weight_snapshot",
        "local_document_id": LOCAL_ID,
        "current_bsale_source_document_id": SOURCE_ID,
        "needs_detail_sync": False,
        "needs_weight_snapshot_sync": True,
        "source_hash_matches": True,
        "postgresql_details": [{"detail_id": 1}] * 6,
        "bsale_details": [{"detail_id": 1}] * 6,
        "diff": {"matches": True},
        "weight": {
            "peso_total_kg": 37.353,
            "status": "calculated",
            "productos_sin_peso": 0,
            "porcentaje_cobertura": 100.0,
        },
    }
    summary = _summarize_dry_run(report)
    assert summary["peso_projected_kg"] == 37.353
    assert summary["weight_status_projected"] == "calculated"


def test_dry_run_summary_partial_not_unavailable():
    report = {
        "folio": 68700,
        "status": "dry_run_needs_weight_snapshot",
        "postgresql_details": [1, 2, 3, 4],
        "bsale_details": [1, 2, 3, 4],
        "diff": {"matches": True},
        "weight": {
            "peso_total_kg": 13.52,
            "status": "partial",
            "productos_sin_peso": 1,
            "porcentaje_cobertura": 75.0,
        },
    }
    summary = _summarize_dry_run(report)
    assert summary["peso_projected_kg"] == 13.52
    assert summary["weight_status_projected"] == "partial"
    assert summary["missing_products"] == 1


def test_in_sync_but_missing_snapshot_rebuilds_on_apply():
    details = [
        {
            "id": 9100001,
            "lineNumber": 0,
            "quantity": 2.0,
            "totalAmount": 2380,
            "variant": {"id": 100},
        }
    ]
    doc = {
        "id": SOURCE_ID,
        "number": FOLIO,
        "state": 0,
        "commercialState": 0,
        "emissionDate": 1,
        "generationDate": 1,
        "totalAmount": 85880,
        "netAmount": 70000,
        "taxAmount": 15880,
        "office": {"id": 1},
        "company": {"id": 3},
        "document_type": {"id": 33},
    }
    digest = compute_oc_source_hash(doc, details)
    pg_document = {
        "document_id": LOCAL_ID,
        "number": FOLIO,
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
    pg_details = [
        {"detail_id": 9100001, "variant_id": 100, "quantity": 2.0, "total_amount": 2380}
    ]
    calc = _calc_result(
        peso=37.353, coverage=100.0, missing=0, total=1, status="calculated"
    )

    with (
        patch(
            "backend.services.distribuidora.oc_reconciliation_service._load_local_oc",
            return_value=(pg_document, pg_details),
        ),
        patch(
            "backend.services.distribuidora.oc_reconciliation_service.discover_oc_sources",
            return_value={"folio": FOLIO, "active_document": doc, "documents": []},
        ),
        patch(
            "backend.services.distribuidora.oc_reconciliation_service.fetch_all_document_details",
            return_value=details,
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
            "backend.services.distribuidora.oc_reconciliation_service._has_weight_snapshot",
            return_value=False,
        ),
        patch(
            "backend.services.distribuidora.oc_reconciliation_service.calculate_order_weight",
            return_value=calc,
        ),
        patch(
            "backend.services.distribuidora.oc_reconciliation_service.recalculate_order_weight",
            return_value=calc,
        ) as recalc,
        patch(
            "backend.services.distribuidora.oc_reconciliation_service._oc_writer_locks",
            return_value=nullcontext(),
        ),
        patch(
            "backend.services.distribuidora.oc_reconciliation_service._mark_reconciliation_attempt",
        ),
        patch(
            "backend.services.distribuidora.oc_reconciliation_service.get_connection",
        ),
        patch(
            "backend.services.distribuidora.oc_reconciliation_service.compare_oc_state",
            return_value={
                "matches": True,
                "attributes": {},
                "header": [],
                "lines": [],
            },
        ),
    ):
        report = reconcile_one_oc(
            MagicMock(),
            folio=FOLIO,
            local_document_id=LOCAL_ID,
            dry_run=False,
        )

    assert report["status"] == "synced_weight_snapshot"
    assert report["wrote"] is True
    assert report["peso_despues_kg"] == 37.353
    recalc.assert_called_once()
    assert recalc.call_args.kwargs.get("persist") is True


def test_rebuild_job_dry_run_does_not_persist():
    docs = [
        {
            "document_id": LOCAL_ID,
            "number": FOLIO,
            "line_count": 6,
            "old_peso_kg": None,
            "old_coverage": None,
            "old_missing": None,
        }
    ]
    calc = _calc_result(
        peso=37.353, coverage=100.0, missing=0, total=6, status="calculated"
    )
    with (
        patch(
            "backend.jobs.rebuild_order_weight_snapshots._load_docs_by_folios",
            return_value=docs,
        ),
        patch(
            "backend.jobs.rebuild_order_weight_snapshots.calculate_order_weight",
            return_value=calc,
        ) as calc_mock,
    ):
        result = rebuild_folios(folios=[FOLIO], dry_run=True)

    assert result["summaries"][0]["projected_weight"] == 37.353
    assert result["summaries"][0]["projected_status"] == "calculated"
    assert result["summaries"][0]["wrote"] is False
    assert calc_mock.call_args.kwargs.get("persist_cache") is False


def test_rebuild_job_apply_persists():
    docs = [
        {
            "document_id": LOCAL_ID,
            "number": FOLIO,
            "line_count": 6,
            "old_peso_kg": None,
            "old_coverage": None,
            "old_missing": None,
        }
    ]
    calc = _calc_result(
        peso=37.353, coverage=100.0, missing=0, total=6, status="calculated"
    )
    with (
        patch(
            "backend.jobs.rebuild_order_weight_snapshots._load_docs_by_folios",
            return_value=docs,
        ),
        patch(
            "backend.jobs.rebuild_order_weight_snapshots.calculate_order_weight",
            return_value=calc,
        ) as calc_mock,
    ):
        result = rebuild_folios(folios=[FOLIO], dry_run=False)

    assert result["summaries"][0]["wrote"] is True
    assert calc_mock.call_args.kwargs.get("persist_cache") is True


def test_refresh_children_persists_weight_after_details():
    details = [
        {
            "id": 1,
            "quantity": 1,
            "variant": {"id": 10, "code": "A", "description": "A"},
            "netUnitValue": 1,
            "totalUnitValue": 1,
            "netAmount": 1,
            "taxAmount": 0,
            "totalAmount": 1,
        }
    ]
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
        ) as weight_fn,
    ):
        client = MagicMock()
        client.get.side_effect = lambda path, params=None, **kw: (
            {"items": details, "count": 1}
            if str(path).endswith("/details.json")
            else {"items": []}
        )
        stats: dict[str, Any] = {}
        _refresh_document_children(
            client,
            MagicMock(),
            MagicMock(),
            LOCAL_ID,
            33,
            stats,
            raw_document={
                "id": SOURCE_ID,
                "number": FOLIO,
                "totalAmount": 85880,
            },
            folio=FOLIO,
        )

    weight_fn.assert_called_once()
    assert weight_fn.call_args.kwargs.get("persist") is True
    assert stats.get("order_weight_recalculated") == 1
