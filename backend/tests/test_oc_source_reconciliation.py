"""Regresión: segunda reemisión Bsale de una OC con PK local estable."""

from __future__ import annotations

from contextlib import nullcontext
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from backend.services.distribuidora.oc_reconciliation_service import (
    compare_oc_state,
    reconcile_one_oc,
    reconcile_recent_ocs,
)
from backend.services.distribuidora import oc_reconciliation_service as reconciliation
from backend.services.distribuidora.oc_source_resolver import (
    compute_oc_source_hash,
    discover_oc_sources,
    fetch_all_document_details,
    select_active_oc_source,
)

LOCAL_ID = 3832233
PREVIOUS_SOURCE_ID = 3832384
CURRENT_SOURCE_ID = 3833001
FOLIO = 68199


def _document(
    source_id: int,
    *,
    number: int,
    state: int,
    generation: int,
    total: int,
) -> dict:
    return {
        "id": source_id,
        "number": number,
        "state": state,
        "commercialState": 0,
        "emissionDate": 1784505600,
        "generationDate": generation,
        "totalAmount": total,
        "netAmount": 199328,
        "taxAmount": 37872,
        "office": {"id": 1},
        "document_type": {"id": 33},
    }


ACTIVE = _document(
    CURRENT_SOURCE_ID,
    number=FOLIO,
    state=0,
    generation=1784653800,
    total=237200,
)
OLD_LOCAL_SOURCE = _document(
    LOCAL_ID,
    number=0,
    state=8888,
    generation=1784570000,
    total=10990,
)
PREVIOUS_SOURCE = _document(
    PREVIOUS_SOURCE_ID,
    number=0,
    state=1,
    generation=1784584947,
    total=219800,
)
ACTIVE_DETAILS = [
    {
        "id": 9000001,
        "lineNumber": 0,
        "variant": {"id": 27383, "code": "68237149926080"},
        "quantity": 20.0,
        "netUnitValue": 9966.4,
        "totalUnitValue": 11860,
        "netAmount": 199328,
        "taxAmount": 37872,
        "totalAmount": 237200,
        "netDiscount": 0,
        "totalDiscount": 0,
        "discountPercentage": 0,
        "relatedDetailId": 8972297,
    }
]


class FakeBsaleClient:
    def get(self, path: str, params=None, **kwargs):
        if path == "/documents.json":
            assert params["number"] == FOLIO
            assert params["officeid"] == 1
            assert params["documenttypeid"] == 33
            return {"items": [ACTIVE], "count": 1}
        if path == f"/documents/{LOCAL_ID}.json":
            return OLD_LOCAL_SOURCE
        if path == f"/documents/{PREVIOUS_SOURCE_ID}.json":
            return PREVIOUS_SOURCE
        if path == f"/documents/{CURRENT_SOURCE_ID}/details.json":
            return {"items": ACTIVE_DETAILS, "count": 1}
        if path.startswith(f"/documents/{CURRENT_SOURCE_ID}/"):
            return {"items": []}
        raise AssertionError(f"GET inesperado: {path} params={params}")


def _old_pg_document(*, source_hash=None) -> dict:
    return {
        "document_id": LOCAL_ID,
        "number": FOLIO,
        "company_id": 3,
        "office_id": 1,
        "document_type_id": 33,
        "total_amount": 219800,
        "net_amount": 184706,
        "tax_amount": 35094,
        "state": 0,
        "commercial_state": 0,
        "raw_data": {"id": PREVIOUS_SOURCE_ID, "number": FOLIO},
        "source_document_id": PREVIOUS_SOURCE_ID,
        "source_hash": source_hash,
    }


OLD_DETAILS = [
    {
        "detail_id": 8972297,
        "variant_id": 27383,
        "quantity": 20.0,
        "total_amount": 219800.0,
    }
]


def test_discovery_lists_old_sources_and_selects_second_reissue():
    result = discover_oc_sources(
        FakeBsaleClient(),
        folio=FOLIO,
        known_source_ids=[LOCAL_ID, PREVIOUS_SOURCE_ID],
    )
    assert result["active_source_document_id"] == CURRENT_SOURCE_ID
    by_id = {item["id"]: item for item in result["documents"]}
    assert set(by_id) == {LOCAL_ID, PREVIOUS_SOURCE_ID, CURRENT_SOURCE_ID}
    assert by_id[LOCAL_ID]["eligible"] is False
    assert "folio_mismatch_or_zero" in by_id[LOCAL_ID]["discard_reasons"]
    assert "state_not_active" in by_id[LOCAL_ID]["discard_reasons"]
    assert by_id[PREVIOUS_SOURCE_ID]["eligible"] is False
    assert by_id[CURRENT_SOURCE_ID]["eligible"] is True


def test_dry_run_finds_new_source_and_reports_237200_diff_without_writes():
    with (
        patch(
            "backend.services.distribuidora.oc_reconciliation_service._load_local_oc",
            return_value=(_old_pg_document(), OLD_DETAILS),
        ),
        patch(
            "backend.services.distribuidora.oc_reconciliation_service.calculate_order_weight",
            return_value={
                "peso_total_kg": 300.0,
                "lines": [{"variant_id": 27383, "peso_unitario_kg": 15.0}],
            },
        ),
        patch(
            "backend.services.distribuidora.oc_reconciliation_service.get_connection"
        ) as get_connection,
    ):
        report = reconcile_one_oc(
            FakeBsaleClient(),
            folio=FOLIO,
            local_document_id=LOCAL_ID,
            dry_run=True,
        )

    assert report["status"] == "dry_run_needs_sync"
    assert report["wrote"] is False
    assert report["previous_source_document_id"] == PREVIOUS_SOURCE_ID
    assert report["current_bsale_source_document_id"] == CURRENT_SOURCE_ID
    assert report["bsale_document"]["totalAmount"] == 237200
    assert report["diff"]["postgresql_line_total"] == 219800
    assert report["diff"]["bsale_line_total"] == 237200
    assert report["weight"]["before_kg"] == 300
    assert report["weight"]["after_projected_kg"] == 300
    get_connection.assert_not_called()


def test_malformed_details_payload_aborts_instead_of_erasing_local_rows():
    class MalformedClient:
        def get(self, path, params=None):
            return {"unexpected": []}

    with pytest.raises(ValueError, match="details inválida"):
        fetch_all_document_details(MalformedClient(), CURRENT_SOURCE_ID)


def test_source_hash_normalizes_numeric_representations_and_line_order():
    second = {
        **ACTIVE_DETAILS[0],
        "id": 9000002,
        "lineNumber": 1,
        "quantity": 1,
        "totalAmount": 100,
    }
    as_strings = [
        {
            **second,
            "quantity": "1.000",
            "totalAmount": "100.0",
        },
        {
            **ACTIVE_DETAILS[0],
            "quantity": "20.000",
            "totalAmount": "237200.0",
        },
    ]
    assert compute_oc_source_hash(ACTIVE, [ACTIVE_DETAILS[0], second]) == (
        compute_oc_source_hash(ACTIVE, as_strings)
    )


def test_writer_lock_conflict_aborts_and_releases_acquired_locks():
    with patch.object(reconciliation, "get_connection") as get_connection:
        conn = get_connection.return_value
        cur = conn.cursor.return_value
        cur.fetchone.side_effect = [(True,), (False,)]
        with pytest.raises(RuntimeError, match="Otro sync"):
            with reconciliation._oc_writer_locks():
                raise AssertionError("no debe entrar")

    calls = [call.args[0] for call in cur.execute.call_args_list]
    assert "SELECT pg_advisory_unlock(%s)" in calls
    conn.close.assert_called_once()


def test_changed_oc_invalidates_only_non_dispatched_plans():
    cur = MagicMock()
    cur.rowcount = 2
    count = reconciliation._invalidate_affected_dispatch_plans(cur, LOCAL_ID)
    sql, params = cur.execute.call_args.args
    assert count == 2
    assert "p.status <> 'dispatched'" in sql
    assert "needs_recalculation = TRUE" in sql
    assert params[1] == LOCAL_ID


def test_next_sync_with_same_hash_and_data_is_idempotent():
    digest = compute_oc_source_hash(ACTIVE, ACTIVE_DETAILS)
    pg_document = {
        **_old_pg_document(source_hash=digest),
        "total_amount": 237200,
        "net_amount": 199328,
        "tax_amount": 37872,
        "raw_data": {"id": CURRENT_SOURCE_ID, "number": FOLIO},
        "source_document_id": CURRENT_SOURCE_ID,
    }
    pg_details = [
        {
            "detail_id": 9000001,
            "variant_id": 27383,
            "quantity": 20.0,
            "total_amount": 237200.0,
        }
    ]
    with (
        patch(
            "backend.services.distribuidora.oc_reconciliation_service._load_local_oc",
            return_value=(pg_document, pg_details),
        ),
        patch(
            "backend.services.distribuidora.oc_reconciliation_service.calculate_order_weight",
            return_value={
                "peso_total_kg": 300.0,
                "lines": [{"variant_id": 27383, "peso_unitario_kg": 15.0}],
            },
        ),
        patch(
            "backend.services.distribuidora.oc_reconciliation_service.get_connection"
        ) as get_connection,
        patch(
            "backend.services.distribuidora.oc_reconciliation_service._oc_writer_locks",
            return_value=nullcontext(),
        ),
        patch(
            "backend.services.distribuidora.oc_reconciliation_service._mark_reconciliation_attempt"
        ) as mark_attempt,
    ):
        report = reconcile_one_oc(
            FakeBsaleClient(),
            folio=FOLIO,
            local_document_id=LOCAL_ID,
            dry_run=False,
        )

    assert report["status"] == "already_in_sync"
    assert report["source_hash_matches"] is True
    assert report["diff"]["matches"] is True
    assert report["wrote"] is False
    assert report["metadata_updated"] is True
    mark_attempt.assert_called_once_with(LOCAL_ID, successful=True)
    get_connection.assert_not_called()


def test_compare_current_pg_and_bsale_matches():
    pg = {
        "number": FOLIO,
        "total_amount": 237200,
        "net_amount": 199328,
        "tax_amount": 37872,
        "state": 0,
        "commercial_state": 0,
    }
    pg_details = [
        {
            "detail_id": 9000001,
            "variant_id": 27383,
            "quantity": 20,
            "total_amount": 237200,
        }
    ]
    assert compare_oc_state(
        pg_document=pg,
        pg_details=pg_details,
        bsale_document=ACTIVE,
        bsale_details=ACTIVE_DETAILS,
    )["matches"] is True


def test_automatic_reconciliation_keeps_latest_source_on_next_run():
    """La ventana automática vuelve a elegir el source nuevo y hace zero writes."""
    inactive_same_folio = _document(
        PREVIOUS_SOURCE_ID,
        number=FOLIO,
        state=1,
        generation=1784584947,
        total=219800,
    )
    lock_conn = patch(
        "backend.services.distribuidora.oc_reconciliation_service.get_connection"
    )
    with lock_conn as get_connection:
        connection = get_connection.return_value
        cursor = connection.cursor.return_value
        cursor.fetchone.return_value = (True,)
        with (
            patch(
                "backend.services.distribuidora.oc_reconciliation_service._fetch_recent_oc_documents",
                return_value=[inactive_same_folio, ACTIVE],
            ),
            patch(
                "backend.services.distribuidora.oc_reconciliation_service.reconcile_one_oc",
                return_value={
                    "folio": FOLIO,
                    "status": "already_in_sync",
                    "wrote": False,
                },
            ) as reconcile,
            patch(
                "backend.services.distribuidora.oc_reconciliation_service._load_full_coverage_batch",
                return_value=([], None),
            ),
        ):
            stats = reconcile_recent_ocs(FakeBsaleClient(), window_days=30)

    assert stats["synced"] == 0
    assert stats["unchanged"] == 1
    selected = reconcile.call_args.kwargs["active_document"]
    assert selected["id"] == CURRENT_SOURCE_ID


def test_full_coverage_returns_open_oc_older_than_60_days():
    reviewed = datetime.now(timezone.utc) - timedelta(days=61)
    columns = (
        "document_id",
        "number",
        "emission_date",
        "last_reconciliation_at",
        "seconds_since_review",
        "max_seconds_since_review",
    )
    with patch.object(reconciliation, "get_connection") as get_connection:
        cur = get_connection.return_value.cursor.return_value
        cur.description = [(column,) for column in columns]
        cur.fetchall.return_value = [
            (
                LOCAL_ID,
                FOLIO,
                datetime.now(timezone.utc) - timedelta(days=90),
                reviewed,
                61 * 86400,
                61 * 86400,
            )
        ]
        rows, max_age = reconciliation._load_full_coverage_batch(limit=10)

    assert rows[0]["document_id"] == LOCAL_ID
    assert max_age == 61 * 86400
    sql = cur.execute.call_args.args[0]
    assert "ORDER BY last_reconciliation_at NULLS FIRST" in sql
    assert "invoice.document_type_id IN (1, 6)" in sql
    assert "AND invoice.state = 0" in sql
    assert "AND d.state = 0" in sql


def test_sixth_reissue_selected_after_five_previous_versions():
    versions = [
        _document(
            3833000 + index,
            number=FOLIO,
            state=1,
            generation=1784650000 + index,
            total=219800 + index,
        )
        for index in range(5)
    ]
    sixth = _document(
        3833005,
        number=FOLIO,
        state=0,
        generation=1784660000,
        total=237200,
    )
    active, evaluated = select_active_oc_source(
        [*versions, sixth],
        folio=FOLIO,
    )
    assert active["id"] == 3833005
    assert sum(1 for item in evaluated if item["eligible"]) == 1


def test_full_lane_detects_sixth_version_then_stops_after_invoice():
    candidate = {
        "document_id": LOCAL_ID,
        "number": FOLIO,
        "seconds_since_review": 60 * 86400,
    }
    with patch.object(reconciliation, "get_connection") as get_connection:
        connection = get_connection.return_value
        connection.cursor.return_value.fetchone.return_value = (True,)
        with (
            patch.object(reconciliation, "_fetch_recent_oc_documents", return_value=[]),
            patch.object(
                reconciliation,
                "_load_full_coverage_batch",
                return_value=([candidate], 60 * 86400),
            ),
            patch.object(
                reconciliation,
                "reconcile_one_oc",
                return_value={
                    "folio": FOLIO,
                    "local_document_id": LOCAL_ID,
                    "current_bsale_source_document_id": 3833005,
                    "source_changed": True,
                    "status": "synced",
                    "wrote": True,
                },
            ),
        ):
            changed = reconcile_recent_ocs(
                FakeBsaleClient(),
                full_coverage_limit=10,
            )

    assert changed["full_coverage_lane"]["reviewed"] == 1
    assert changed["new_versions_detected"] == 1
    assert changed["ocs_modified"] == 1

    # La consulta de elegibilidad devuelve vacío una vez existe factura definitiva.
    with patch.object(reconciliation, "get_connection") as get_connection:
        connection = get_connection.return_value
        connection.cursor.return_value.fetchone.return_value = (True,)
        with (
            patch.object(reconciliation, "_fetch_recent_oc_documents", return_value=[]),
            patch.object(
                reconciliation,
                "_load_full_coverage_batch",
                return_value=([], None),
            ),
        ):
            invoiced = reconcile_recent_ocs(
                FakeBsaleClient(),
                full_coverage_limit=10,
            )
    assert invoiced["full_coverage_lane"]["candidates"] == 0
    assert invoiced["ocs_reviewed"] == 0
