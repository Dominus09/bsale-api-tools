"""Recuperación selectiva de OC ausentes (reconcile_bsale_ocs / missing-only)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from backend.jobs import reconcile_bsale_ocs as job
from backend.services.distribuidora import oc_reconciliation_service as reconciliation


def _oc(
    source_id: int,
    *,
    number: int,
    state: int = 0,
    total: int = 1000,
    office_id: int = 1,
    doc_type: int = 33,
    generation: int = 1_700_000_000,
) -> dict:
    return {
        "id": source_id,
        "number": number,
        "state": state,
        "commercialState": 0,
        "emissionDate": generation - 1000,
        "generationDate": generation,
        "totalAmount": total,
        "office": {"id": office_id},
        "document_type": {"id": doc_type},
        "company": {"id": 3},
    }


def test_missing_oc_is_detected_and_repaired_with_execute():
    active = _oc(3958410, number=70511, total=2570890)
    with (
        patch.object(
            reconciliation,
            "_fetch_recent_oc_documents_with_meta",
            return_value={
                "items": [active],
                "pages": 1,
                "api_calls": 1,
                "listing_complete": True,
            },
        ),
        patch.object(
            reconciliation,
            "_lookup_local_ocs_by_folios",
            return_value={},
        ),
        patch.object(reconciliation, "get_connection") as get_connection,
        patch.object(
            reconciliation,
            "reconcile_one_oc",
            return_value={
                "status": "synced",
                "wrote": True,
                "local_document_id": 3958410,
                "bsale_details_count": 3,
            },
        ) as reconcile,
    ):
        get_connection.return_value.cursor.return_value.fetchone.return_value = (True,)
        stats = reconciliation.recover_missing_purchase_orders(
            MagicMock(),
            window_days=7,
            dry_run=False,
            max_repairs=10,
        )

    assert stats["ocs_missing"] == 1
    assert stats["ocs_recovered"] == 1
    assert stats["wrote"] is True
    assert reconcile.call_count == 1
    assert reconcile.call_args.kwargs["folio"] == 70511
    assert reconcile.call_args.kwargs["dry_run"] is False


def test_existing_oc_triggers_zero_reconcile_calls():
    active = _oc(100, number=70001)
    with (
        patch.object(
            reconciliation,
            "_fetch_recent_oc_documents_with_meta",
            return_value={
                "items": [active],
                "pages": 1,
                "api_calls": 1,
                "listing_complete": True,
            },
        ),
        patch.object(
            reconciliation,
            "_lookup_local_ocs_by_folios",
            return_value={
                70001: {
                    "document_id": 100,
                    "state": 0,
                    "source_document_id": 100,
                }
            },
        ),
        patch.object(reconciliation, "reconcile_one_oc") as reconcile,
    ):
        stats = reconciliation.recover_missing_purchase_orders(
            MagicMock(),
            dry_run=True,
            max_repairs=10,
        )

    assert stats["ocs_existing_ok"] == 1
    assert stats["ocs_missing"] == 0
    reconcile.assert_not_called()
    assert stats["wrote"] is False


def test_version_diff_is_reported_without_repair():
    active = _oc(200, number=70002, generation=2)
    with (
        patch.object(
            reconciliation,
            "_fetch_recent_oc_documents_with_meta",
            return_value={
                "items": [active],
                "pages": 1,
                "api_calls": 1,
                "listing_complete": True,
            },
        ),
        patch.object(
            reconciliation,
            "_lookup_local_ocs_by_folios",
            return_value={
                70002: {
                    "document_id": 199,
                    "state": 0,
                    "source_document_id": 199,
                }
            },
        ),
        patch.object(reconciliation, "reconcile_one_oc") as reconcile,
    ):
        stats = reconciliation.recover_missing_purchase_orders(
            MagicMock(), dry_run=False, max_repairs=10
        )

    assert stats["ocs_version_diff"] == 1
    assert stats["version_diffs"][0]["action"] == "version_diff_defer_to_open_reconciler"
    reconcile.assert_not_called()


def test_ghost_state_8888_and_number_zero_are_discarded():
    ghost = _oc(1, number=0, state=8888)
    cancelled = _oc(2, number=70003, state=1)
    with (
        patch.object(
            reconciliation,
            "_fetch_recent_oc_documents_with_meta",
            return_value={
                "items": [ghost, cancelled],
                "pages": 1,
                "api_calls": 1,
                "listing_complete": True,
            },
        ),
        patch.object(reconciliation, "_lookup_local_ocs_by_folios", return_value={}),
        patch.object(reconciliation, "reconcile_one_oc") as reconcile,
    ):
        stats = reconciliation.recover_missing_purchase_orders(
            MagicMock(), dry_run=True, max_repairs=10
        )

    assert stats["bsale_active_folios"] == 0
    assert stats["ocs_missing"] == 0
    reconcile.assert_not_called()


def test_cancelled_without_active_version_not_inserted():
    cancelled = _oc(3, number=70004, state=8888)
    # number still set but state inactive → no active source
    with (
        patch.object(
            reconciliation,
            "_fetch_recent_oc_documents_with_meta",
            return_value={
                "items": [cancelled],
                "pages": 1,
                "api_calls": 1,
                "listing_complete": True,
            },
        ),
        patch.object(reconciliation, "_lookup_local_ocs_by_folios") as lookup,
        patch.object(reconciliation, "reconcile_one_oc") as reconcile,
    ):
        stats = reconciliation.recover_missing_purchase_orders(
            MagicMock(), dry_run=False, max_repairs=5
        )

    lookup.assert_called()
    assert stats["ocs_missing"] == 0
    reconcile.assert_not_called()


def test_incomplete_listing_fails_without_writes():
    with (
        patch.object(
            reconciliation,
            "_fetch_recent_oc_documents_with_meta",
            side_effect=reconciliation.IncompleteBsaleListingError("502"),
        ),
        patch.object(reconciliation, "reconcile_one_oc") as reconcile,
    ):
        stats = reconciliation.recover_missing_purchase_orders(
            MagicMock(), dry_run=False, max_repairs=10
        )

    assert stats["status"] == "failed"
    assert stats["wrote"] is False
    reconcile.assert_not_called()


def test_active_sync_conflict_defers_without_marking_recovered():
    active = _oc(10, number=70005)
    with (
        patch.object(
            reconciliation,
            "_fetch_recent_oc_documents_with_meta",
            return_value={
                "items": [active],
                "pages": 1,
                "api_calls": 1,
                "listing_complete": True,
            },
        ),
        patch.object(reconciliation, "_lookup_local_ocs_by_folios", return_value={}),
        patch.object(reconciliation, "get_connection") as get_connection,
        patch.object(
            reconciliation,
            "reconcile_one_oc",
            side_effect=reconciliation.ActiveSyncConflict("live"),
        ),
        patch.object(reconciliation, "_mark_reconciliation_attempt") as mark,
    ):
        get_connection.return_value.cursor.return_value.fetchone.return_value = (True,)
        stats = reconciliation.recover_missing_purchase_orders(
            MagicMock(), dry_run=False, max_repairs=10
        )

    assert stats["ocs_deferred_due_to_lock"] == 1
    assert stats["ocs_recovered"] == 0
    mark.assert_not_called()
    assert stats["results"][0]["status"] == "deferred_due_to_active_sync"


def test_recovery_after_lock_available():
    active = _oc(11, number=70006)

    def _side_effect(*_a, **_k):
        return {
            "status": "synced",
            "wrote": True,
            "local_document_id": 11,
            "bsale_details_count": 1,
        }

    with (
        patch.object(
            reconciliation,
            "_fetch_recent_oc_documents_with_meta",
            return_value={
                "items": [active],
                "pages": 1,
                "api_calls": 1,
                "listing_complete": True,
            },
        ),
        patch.object(reconciliation, "_lookup_local_ocs_by_folios", return_value={}),
        patch.object(reconciliation, "get_connection") as get_connection,
        patch.object(reconciliation, "reconcile_one_oc", side_effect=_side_effect),
    ):
        # Primer intento: lock ocupación → skipped; segundo: OK.
        cur = get_connection.return_value.cursor.return_value
        cur.fetchone.side_effect = [(False,), (True,)]
        blocked = reconciliation.recover_missing_purchase_orders(
            MagicMock(), dry_run=False, max_repairs=10
        )
        ok = reconciliation.recover_missing_purchase_orders(
            MagicMock(), dry_run=False, max_repairs=10
        )

    assert blocked["status"] == "skipped_due_to_active_sync"
    assert blocked["ocs_recovered"] == 0
    assert ok["ocs_recovered"] == 1


def test_max_repairs_limits_work():
    docs = [_oc(1000 + i, number=71000 + i) for i in range(5)]
    with (
        patch.object(
            reconciliation,
            "_fetch_recent_oc_documents_with_meta",
            return_value={
                "items": docs,
                "pages": 1,
                "api_calls": 1,
                "listing_complete": True,
            },
        ),
        patch.object(reconciliation, "_lookup_local_ocs_by_folios", return_value={}),
        patch.object(reconciliation, "get_connection") as get_connection,
        patch.object(
            reconciliation,
            "reconcile_one_oc",
            return_value={
                "status": "synced",
                "wrote": True,
                "local_document_id": 1,
                "bsale_details_count": 1,
            },
        ) as reconcile,
    ):
        get_connection.return_value.cursor.return_value.fetchone.return_value = (True,)
        stats = reconciliation.recover_missing_purchase_orders(
            MagicMock(), dry_run=False, max_repairs=2
        )

    assert stats["ocs_missing"] == 5
    assert reconcile.call_count == 2
    assert stats["ocs_pending_max_repairs"] == 3
    assert stats["ocs_recovered"] == 2


def test_repeated_run_is_idempotent_when_already_present():
    active = _oc(12, number=70007)
    with (
        patch.object(
            reconciliation,
            "_fetch_recent_oc_documents_with_meta",
            return_value={
                "items": [active],
                "pages": 1,
                "api_calls": 1,
                "listing_complete": True,
            },
        ),
        patch.object(
            reconciliation,
            "_lookup_local_ocs_by_folios",
            return_value={
                70007: {
                    "document_id": 12,
                    "state": 0,
                    "source_document_id": 12,
                }
            },
        ),
        patch.object(reconciliation, "reconcile_one_oc") as reconcile,
    ):
        first = reconciliation.recover_missing_purchase_orders(
            MagicMock(), dry_run=False, max_repairs=10
        )
        second = reconciliation.recover_missing_purchase_orders(
            MagicMock(), dry_run=False, max_repairs=10
        )

    assert first["ocs_missing"] == 0
    assert second["ocs_missing"] == 0
    reconcile.assert_not_called()


def test_dry_run_never_writes():
    active = _oc(13, number=70008)
    with (
        patch.object(
            reconciliation,
            "_fetch_recent_oc_documents_with_meta",
            return_value={
                "items": [active],
                "pages": 1,
                "api_calls": 1,
                "listing_complete": True,
            },
        ),
        patch.object(reconciliation, "_lookup_local_ocs_by_folios", return_value={}),
        patch.object(
            reconciliation,
            "reconcile_one_oc",
            return_value={
                "status": "dry_run_needs_sync",
                "wrote": False,
                "bsale_details_count": 2,
            },
        ) as reconcile,
        patch.object(reconciliation, "get_connection") as get_connection,
    ):
        stats = reconciliation.recover_missing_purchase_orders(
            MagicMock(), dry_run=True, max_repairs=10
        )

    get_connection.assert_not_called()  # dry-run no toma lock 013
    assert reconcile.call_args.kwargs["dry_run"] is True
    assert stats["wrote"] is False
    assert stats["ocs_recovered"] == 0
    assert stats["results"][0]["status"] == "missing_in_postgresql"


def test_reject_other_company_office_or_type():
    wrong_office = _oc(14, number=70009, office_id=2)
    wrong_type = _oc(15, number=70010, doc_type=1)
    with (
        patch.object(
            reconciliation,
            "_fetch_recent_oc_documents_with_meta",
            return_value={
                "items": [wrong_office, wrong_type],
                "pages": 1,
                "api_calls": 1,
                "listing_complete": True,
            },
        ),
        patch.object(reconciliation, "_lookup_local_ocs_by_folios", return_value={}),
        patch.object(reconciliation, "reconcile_one_oc") as reconcile,
    ):
        stats = reconciliation.recover_missing_purchase_orders(
            MagicMock(), dry_run=True, max_repairs=10
        )

    assert stats["bsale_active_folios"] == 0
    reconcile.assert_not_called()


def test_job_requires_execute_and_rejects_conflicting_flags():
    assert job.main(["--execute", "--dry-run"]) == 2


def test_job_dry_run_default_calls_recover(monkeypatch):
    monkeypatch.setenv("BSALE_TOKEN_SPA", "test-token-spa")
    with (
        patch.object(job, "BsaleClient"),
        patch.object(
            job,
            "recover_missing_purchase_orders",
            return_value={
                "status": "completed",
                "errors": 0,
                "ocs_missing": 0,
                "wrote": False,
            },
        ) as recover,
    ):
        rc = job.main(["--window-days", "7", "--max-repairs", "5"])

    assert rc == 0
    assert recover.call_args.kwargs["dry_run"] is True
    assert recover.call_args.kwargs["window_days"] == 7
    assert recover.call_args.kwargs["max_repairs"] == 5


def test_job_execute_sets_dry_run_false(monkeypatch):
    monkeypatch.setenv("BSALE_TOKEN_SPA", "test-token-spa")
    with (
        patch.object(job, "BsaleClient"),
        patch.object(
            job,
            "recover_missing_purchase_orders",
            return_value={"status": "completed", "errors": 0, "wrote": True},
        ) as recover,
    ):
        rc = job.main(["--execute", "--window-days", "7"])

    assert rc == 0
    assert recover.call_args.kwargs["dry_run"] is False


def test_reconcile_recent_ocs_does_not_mark_cursor_on_active_sync():
    candidate = {
        "document_id": 99,
        "number": 70011,
        "seconds_since_review": 10,
    }
    with patch.object(reconciliation, "get_connection") as get_connection:
        get_connection.return_value.cursor.return_value.fetchone.return_value = (True,)
        with (
            patch.object(reconciliation, "_fetch_recent_oc_documents", return_value=[]),
            patch.object(
                reconciliation,
                "_load_full_coverage_batch",
                return_value=([candidate], 10, {}),
            ),
            patch.object(
                reconciliation,
                "reconcile_one_oc",
                side_effect=reconciliation.ActiveSyncConflict("busy"),
            ),
            patch.object(reconciliation, "_mark_reconciliation_attempt") as mark,
        ):
            stats = reconciliation.reconcile_recent_ocs(
                MagicMock(), dry_run=False, full_coverage_limit=1
            )

    assert stats["ocs_deferred_due_to_lock"] == 1
    assert stats["results"][0]["status"] == "deferred_due_to_active_sync"
    mark.assert_not_called()


def test_fetch_strict_raises_on_invalid_page():
    client = MagicMock()
    client.get.return_value = {"items": None}
    with pytest.raises(reconciliation.IncompleteBsaleListingError):
        reconciliation._fetch_recent_oc_documents_with_meta(
            client, window_days=7, min_window_days=None, strict=True
        )
