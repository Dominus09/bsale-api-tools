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
                return_value=([], None, {}),
            ),
        ):
            stats = reconcile_recent_ocs(FakeBsaleClient(), window_days=30)

    assert stats["synced"] == 0
    assert stats["unchanged"] == 1
    selected = reconcile.call_args.kwargs["active_document"]
    assert selected["id"] == CURRENT_SOURCE_ID


def test_full_coverage_returns_open_oc_older_than_60_days():
    reviewed = datetime.now(timezone.utc) - timedelta(days=61)
    historical_row = {
        "document_id": LOCAL_ID,
        "number": FOLIO,
        "emission_date": datetime.now(timezone.utc) - timedelta(days=90),
        "generation_date": datetime.now(timezone.utc) - timedelta(days=90),
        "last_reconciliation_at": reviewed,
        "seconds_since_review": 61 * 86400,
        "max_seconds_since_review": 61 * 86400,
    }
    with (
        patch.object(reconciliation, "get_connection"),
        patch.object(
            reconciliation,
            "_fetch_candidate_rows",
            side_effect=[[], [historical_row]],
        ),
    ):
        rows, max_age, meta = reconciliation._load_full_coverage_batch(limit=10)

    assert rows[0]["document_id"] == LOCAL_ID
    assert rows[0]["candidate_lane"] == "historical"
    assert max_age == 61 * 86400
    assert meta["historical_selected"] == 1
    assert meta["recent_selected"] == 0


def test_lane_slots_are_eighty_twenty_for_limit_100():
    assert reconciliation._allocate_lane_slots(100) == (80, 20)
    assert reconciliation._allocate_lane_slots(10) == (8, 2)
    assert reconciliation._allocate_lane_slots(1) == (1, 0)


def test_recent_oc_is_selected_despite_hundreds_of_historical_nulls():
    now = datetime.now(timezone.utc)
    historical_rows = [
        {
            "document_id": 600000 + index,
            "number": 65000 + index,
            "emission_date": now - timedelta(days=120),
            "generation_date": now - timedelta(days=120),
            "last_reconciliation_at": None,
            "seconds_since_review": 120 * 86400,
        }
        for index in range(250)
    ]
    recent_68199 = {
        "document_id": LOCAL_ID,
        "number": FOLIO,
        "emission_date": now - timedelta(days=2),
        "generation_date": now - timedelta(days=1),
        "last_reconciliation_at": now - timedelta(hours=3),
        "seconds_since_review": 3 * 3600,
    }
    selected = reconciliation._merge_lane_candidates(
        recent_rows=[recent_68199],
        historical_rows=historical_rows,
        recent_slots=80,
        historical_slots=20,
        total_limit=100,
    )
    assert len(selected) == 100
    assert selected[0]["document_id"] == LOCAL_ID
    assert selected[0]["number"] == FOLIO
    assert selected[0]["candidate_lane"] == "recent"
    assert sum(1 for row in selected if row["candidate_lane"] == "recent") == 1
    assert sum(1 for row in selected if row["candidate_lane"] == "historical") == 99
    assert all(row["candidate_lane"] == "historical" for row in selected[1:])
    assert len({row["document_id"] for row in selected}) == 100


def test_lane_quota_fill_and_no_duplicates():
    recent_rows = [
        {"document_id": 1000 + index, "number": 70000 + index}
        for index in range(80)
    ]
    historical_rows = [
        {"document_id": 2000 + index, "number": 65000 + index}
        for index in range(150)
    ]
    full = reconciliation._merge_lane_candidates(
        recent_rows=recent_rows,
        historical_rows=historical_rows,
        recent_slots=80,
        historical_slots=20,
        total_limit=100,
    )
    assert len(full) == 100
    assert sum(1 for row in full if row["candidate_lane"] == "recent") == 80
    assert sum(1 for row in full if row["candidate_lane"] == "historical") == 20
    assert len({row["document_id"] for row in full}) == 100

    sparse_recent = recent_rows[:30]
    topped = reconciliation._merge_lane_candidates(
        recent_rows=sparse_recent,
        historical_rows=historical_rows,
        recent_slots=80,
        historical_slots=20,
        total_limit=100,
    )
    assert len(topped) == 100  # 30 recent + 70 historical por overflow
    assert sum(1 for row in topped if row["candidate_lane"] == "recent") == 30
    assert sum(1 for row in topped if row["candidate_lane"] == "historical") == 70
    assert len({row["document_id"] for row in topped}) == 100

    # Sin recientes, el histórico puede consumir el cupo total vía overflow.
    only_hist = reconciliation._merge_lane_candidates(
        recent_rows=[],
        historical_rows=historical_rows,
        recent_slots=80,
        historical_slots=20,
        total_limit=100,
    )
    assert len(only_hist) == 100
    assert all(row["candidate_lane"] == "historical" for row in only_hist)


def test_candidate_sql_separates_recent_and_historical_lanes():
    recent_sql = " ".join(reconciliation._RECENT_CANDIDATES_SQL.split())
    historical_sql = " ".join(reconciliation._HISTORICAL_CANDIDATES_SQL.split())
    assert "d.number IS NOT NULL" in recent_sql
    assert "d.number > 0" in recent_sql
    assert "lane_date >= NOW() - make_interval(days => %s)" in recent_sql
    assert "generation_date DESC NULLS LAST" in recent_sql
    assert "last_reconciliation_at ASC NULLS FIRST" in recent_sql
    assert "document_id DESC" in recent_sql
    assert "lane_date < NOW() - make_interval(days => %s)" in historical_sql
    assert "last_reconciliation_at ASC NULLS FIRST" in historical_sql
    assert "invoice.document_type_id IN (1, 6)" in recent_sql
    assert "AND invoice.state = 0" in recent_sql
    assert "AND d.state = 0" in recent_sql


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
                return_value=([candidate], 60 * 86400, {}),
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
                return_value=([], None, {}),
            ),
        ):
            invoiced = reconcile_recent_ocs(
                FakeBsaleClient(),
                full_coverage_limit=10,
            )
    assert invoiced["full_coverage_lane"]["candidates"] == 0
    assert invoiced["ocs_reviewed"] == 0


def test_oc_68199_new_source_changes_discounts_and_amounts_not_quantity():
    previous_source_id = 3832987
    current_source_id = 3833128
    current_document = {
        **ACTIVE,
        "id": current_source_id,
        "totalAmount": 213480,
        "netAmount": 179395,
        "taxAmount": 34085,
        "generationDate": ACTIVE["generationDate"] + 600,
    }
    current_details = [
        {
            **ACTIVE_DETAILS[0],
            "id": 9000128,
            "quantity": 20.0,
            "netDiscount": 19933,
            "totalDiscount": 23720,
            "discountPercentage": 10.0,
            "netAmount": 179395,
            "taxAmount": 34085,
            "totalAmount": 213480,
        }
    ]

    class ReissuedClient:
        def get(self, path, params=None, **kwargs):
            if path == "/documents.json":
                return {"items": [current_document]}
            if path in {
                f"/documents/{LOCAL_ID}.json",
                f"/documents/{previous_source_id}.json",
            }:
                return {
                    **PREVIOUS_SOURCE,
                    "id": int(path.split("/")[-1].split(".")[0]),
                    "number": 0,
                    "state": 1,
                }
            if path == f"/documents/{current_source_id}/details.json":
                return {"items": current_details}
            raise AssertionError(path)

    pg_document = {
        **_old_pg_document(),
        "source_document_id": previous_source_id,
        "raw_data": {"id": previous_source_id, "number": FOLIO},
        "total_amount": 237200,
        "net_amount": 199328,
        "tax_amount": 37872,
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
        patch.object(
            reconciliation,
            "_load_local_oc",
            return_value=(pg_document, pg_details),
        ),
        patch.object(
            reconciliation,
            "calculate_order_weight",
            return_value={
                "peso_total_kg": 300.0,
                "lines": [{"variant_id": 27383, "peso_unitario_kg": 15.0}],
            },
        ),
    ):
        report = reconcile_one_oc(
            ReissuedClient(),
            folio=FOLIO,
            local_document_id=LOCAL_ID,
            dry_run=True,
        )

    assert report["status"] == "dry_run_needs_sync"
    assert report["previous_source_document_id"] == previous_source_id
    assert report["current_bsale_source_document_id"] == current_source_id
    assert report["source_changed"] is True
    assert report["diff"]["postgresql_quantity"] == 20
    assert report["diff"]["bsale_quantity"] == 20
    assert report["diff"]["postgresql_line_total"] == 237200
    assert report["diff"]["bsale_line_total"] == 213480


def test_batch_continues_after_failure_and_emits_mandatory_logs(caplog):
    candidates = [
        {"document_id": 1, "number": 100, "last_reconciliation_at": None},
        {"document_id": 2, "number": 101, "last_reconciliation_at": None},
        {"document_id": 3, "number": 102, "last_reconciliation_at": None},
    ]
    outcomes = [
        {
            "status": "synced",
            "wrote": True,
            "source_changed": True,
            "previous_source_document_id": 10,
            "current_bsale_source_document_id": 11,
            "details_replaced": 1,
        },
        {
            "status": "dry_run_in_sync",
            "wrote": False,
            "source_changed": False,
            "current_bsale_source_document_id": 20,
        },
        RuntimeError("Bsale unavailable"),
    ]
    caplog.set_level("INFO")
    with patch.object(reconciliation, "get_connection") as get_connection:
        get_connection.return_value.cursor.return_value.fetchone.return_value = (True,)
        with (
            patch.object(
                reconciliation,
                "_load_full_coverage_batch",
                return_value=(
                    candidates,
                    999,
                    {
                        "recent_slots": 2,
                        "historical_slots": 1,
                        "recent_candidates_loaded": 3,
                        "historical_candidates_loaded": 0,
                    },
                ),
            ),
            patch.object(
                reconciliation,
                "reconcile_one_oc",
                side_effect=outcomes,
            ),
        ):
            stats = reconciliation.reconcile_open_purchase_orders_batch(
                FakeBsaleClient(),
                execute=False,
                limit=3,
            )

    text = caplog.text
    for event in (
        "reconciliation_cycle_started",
        "oc_checked",
        "oc_source_changed",
        "oc_updated",
        "oc_unchanged",
        "oc_failed",
        "reconciliation_cycle_finished",
    ):
        assert event in text
    assert stats["ocs_checked"] == 3
    assert stats["ocs_updated"] == 1
    assert stats["ocs_unchanged"] == 1
    assert stats["errors"] == 1
    assert stats["checked"] == 3
    assert stats["updated"] == 1
    assert stats["unchanged"] == 1
    assert stats["skipped"] == 0
    assert stats["invalid_folios"] == 0
    assert "duration_seconds" in stats
    assert text.count("reconciliation_cycle_finished") == 1


def test_batch_skips_invalid_folios_and_processes_valid_candidate(caplog):
    candidates = [
        {"document_id": 11, "number": None, "last_reconciliation_at": None},
        {"document_id": 12, "number": 0, "last_reconciliation_at": None},
        {"document_id": 13, "number": "", "last_reconciliation_at": None},
        {"document_id": 14, "number": 68260, "last_reconciliation_at": None},
    ]
    valid_result = {
        "status": "dry_run_in_sync",
        "wrote": False,
        "source_changed": False,
        "current_bsale_source_document_id": 99,
        "local_document_id": 14,
        "folio": 68260,
    }
    caplog.set_level("INFO")
    with patch.object(reconciliation, "get_connection") as get_connection:
        get_connection.return_value.cursor.return_value.fetchone.return_value = (True,)
        with (
            patch.object(
                reconciliation,
                "_load_full_coverage_batch",
                return_value=(
                    candidates,
                    10,
                    {
                        "recent_slots": 8,
                        "historical_slots": 2,
                        "recent_candidates_loaded": 4,
                        "historical_candidates_loaded": 0,
                    },
                ),
            ),
            patch.object(
                reconciliation,
                "reconcile_one_oc",
                return_value=valid_result,
            ) as reconcile_one,
        ):
            stats = reconciliation.reconcile_open_purchase_orders_batch(
                FakeBsaleClient(),
                execute=False,
                limit=10,
            )

    assert reconcile_one.call_count == 1
    assert reconcile_one.call_args.kwargs["folio"] == 68260
    assert reconcile_one.call_args.kwargs["local_document_id"] == 14
    assert stats["status"] == "completed"
    assert stats["checked"] == 4
    assert stats["skipped"] == 3
    assert stats["invalid_folios"] == 3
    assert stats["unchanged"] == 1
    assert stats["updated"] == 0
    assert stats["errors"] == 0
    assert "duration_seconds" in stats

    skipped = [item for item in stats["results"] if item["status"] == "oc_skipped"]
    assert len(skipped) == 3
    assert {item["local_document_id"] for item in skipped} == {11, 12, 13}
    assert all(item["reason"] == "invalid_or_missing_folio" for item in skipped)

    text = caplog.text
    assert text.count("oc_skipped") == 3
    assert "reason=invalid_or_missing_folio" in text
    assert text.count("reconciliation_cycle_finished") == 1
    assert "reconciliation_cycle_failed" not in text


def test_candidate_sql_excludes_null_and_non_positive_folios():
    sql = " ".join(reconciliation._FULL_COVERAGE_CANDIDATES_SQL.split())
    assert "d.number IS NOT NULL" in sql
    assert "d.number > 0" in sql


def test_unexpected_batch_abort_does_not_emit_finished(caplog):
    candidates = [
        {"document_id": 1, "number": 100, "last_reconciliation_at": None},
    ]
    caplog.set_level("INFO")
    with patch.object(reconciliation, "get_connection") as get_connection:
        get_connection.return_value.cursor.return_value.fetchone.return_value = (True,)
        with (
            patch.object(
                reconciliation,
                "_load_full_coverage_batch",
                return_value=(candidates, 1, {}),
            ),
            patch.object(
                reconciliation,
                "reconcile_one_oc",
                side_effect=MemoryError("boom"),
            ),
            patch.object(
                reconciliation,
                "_is_global_reconciliation_error",
                return_value=True,
            ),
        ):
            with pytest.raises(MemoryError):
                reconciliation.reconcile_open_purchase_orders_batch(
                    FakeBsaleClient(),
                    execute=False,
                    limit=1,
                )

    assert "reconciliation_cycle_finished" not in caplog.text


def test_batch_skips_whole_cycle_on_active_writer_lock():
    candidate = {
        "document_id": LOCAL_ID,
        "number": FOLIO,
        "last_reconciliation_at": None,
    }
    with patch.object(reconciliation, "get_connection") as get_connection:
        get_connection.return_value.cursor.return_value.fetchone.return_value = (True,)
        with (
            patch.object(
                reconciliation,
                "_load_full_coverage_batch",
                return_value=([candidate], 100, {}),
            ),
            patch.object(
                reconciliation,
                "reconcile_one_oc",
                side_effect=reconciliation.ActiveSyncConflict("active sync"),
            ),
            patch.object(reconciliation, "_mark_reconciliation_attempt") as mark,
        ):
            result = reconciliation.reconcile_open_purchase_orders_batch(
                FakeBsaleClient(),
                execute=True,
                limit=10,
            )

    assert result["status"] == "skipped_due_to_active_sync"
    assert result["errors"] == 0
    mark.assert_not_called()
