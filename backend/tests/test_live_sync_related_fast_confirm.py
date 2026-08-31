"""Live related sync: fast confirmation, presupuesto runtime y singleton."""

from __future__ import annotations

import time
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from backend.services.distribuidora import sync_related_service as srs
from backend.services.distribuidora.oc_related_discovery_service import (
    DISCOVERY_MODE_FAST_CONFIRM,
    DISCOVERY_MODE_FULL,
    STOP_REASON_COMPLETED,
    STOP_REASON_RUNTIME_BUDGET,
    STOP_REASON_SKIPPED_ALREADY_RUNNING,
    discover_invoice_edges_for_oc,
    resolve_related_sync_max_runtime_sec,
)


def _invoice_item(related_id: int, *, doc_type: int = 1) -> dict:
    return {
        "id": related_id,
        "number": 2721119,
        "documentTypeId": doc_type,
        "generationDate": int(
            datetime(2026, 8, 27, 19, 44, 18, tzinfo=timezone.utc).timestamp()
        ),
    }


def _detail_ids(n: int) -> list[int]:
    return list(range(1, n + 1))


def _mock_discovery_stack(
    mock_oc_num,
    mock_source,
    mock_existing,
    mock_details,
    mock_fetch_items,
    mock_triples,
    *,
    n_details: int,
    invoice_at_detail: int | None,
    existing_confirmed: bool = False,
):
    mock_oc_num.return_value = (69174, 0)
    mock_source.return_value = (69174, 69174)
    if existing_confirmed:
        mock_existing.return_value = ({(1, 3883163)}, {(3883163, 1)})
    else:
        mock_existing.return_value = (set(), set())
    mock_details.return_value = (_detail_ids(n_details), 1)

    def _fetch_side_effect(_client, detail_id, **_kw):
        if invoice_at_detail is not None and int(detail_id) == invoice_at_detail:
            return ([_invoice_item(3883163)], 1)
        return ([], 1)

    mock_fetch_items.side_effect = _fetch_side_effect

    def _triples_side_effect(_client, _cur, detail_id, items, **_kw):
        if items:
            return ([(int(detail_id), 3883163, 1)], 0)
        return ([], 0)

    mock_triples.side_effect = _triples_side_effect


@patch(
    "backend.services.distribuidora.sync_related_service._documents_json_items_to_triples"
)
@patch(
    "backend.services.distribuidora.oc_related_discovery_service.fetch_relateddetailid_items"
)
@patch(
    "backend.services.distribuidora.sync_related_service._fetch_detail_ids_from_bsale_details"
)
@patch(
    "backend.services.distribuidora.oc_related_discovery_service.load_existing_invoice_relations_for_oc"
)
@patch(
    "backend.services.distribuidora.sync_related_service._bsale_source_id_from_pg"
)
@patch(
    "backend.services.distribuidora.oc_related_discovery_service._oc_number_and_state"
)
def test_fast_confirm_stops_after_first_invoice_detail(
    mock_oc_num,
    mock_source,
    mock_existing,
    mock_details,
    mock_fetch_items,
    mock_triples,
):
    _mock_discovery_stack(
        mock_oc_num,
        mock_source,
        mock_existing,
        mock_details,
        mock_fetch_items,
        mock_triples,
        n_details=50,
        invoice_at_detail=1,
    )

    res = discover_invoice_edges_for_oc(
        MagicMock(),
        MagicMock(),
        69174,
        office_id=1,
        throttle=0,
        discovery_mode=DISCOVERY_MODE_FAST_CONFIRM,
    )

    assert res["fast_confirmed"] is True
    assert res["details_queried"] == 1
    assert res["details_skipped_after_confirmation"] == 49
    assert res["api_calls_saved_by_early_exit"] == 49
    assert len(res["edges"]) == 1
    assert mock_fetch_items.call_count == 1


@patch(
    "backend.services.distribuidora.sync_related_service._documents_json_items_to_triples"
)
@patch(
    "backend.services.distribuidora.oc_related_discovery_service.fetch_relateddetailid_items"
)
@patch(
    "backend.services.distribuidora.sync_related_service._fetch_detail_ids_from_bsale_details"
)
@patch(
    "backend.services.distribuidora.oc_related_discovery_service.load_existing_invoice_relations_for_oc"
)
@patch(
    "backend.services.distribuidora.sync_related_service._bsale_source_id_from_pg"
)
@patch(
    "backend.services.distribuidora.oc_related_discovery_service._oc_number_and_state"
)
def test_fast_confirm_queries_until_invoice_on_detail_30(
    mock_oc_num,
    mock_source,
    mock_existing,
    mock_details,
    mock_fetch_items,
    mock_triples,
):
    _mock_discovery_stack(
        mock_oc_num,
        mock_source,
        mock_existing,
        mock_details,
        mock_fetch_items,
        mock_triples,
        n_details=50,
        invoice_at_detail=30,
    )

    res = discover_invoice_edges_for_oc(
        MagicMock(),
        MagicMock(),
        69174,
        office_id=1,
        throttle=0,
        discovery_mode=DISCOVERY_MODE_FAST_CONFIRM,
    )

    assert res["fast_confirmed"] is True
    assert res["details_queried"] == 30
    assert res["details_skipped_after_confirmation"] == 20
    assert mock_fetch_items.call_count == 30


@patch(
    "backend.services.distribuidora.sync_related_service._documents_json_items_to_triples"
)
@patch(
    "backend.services.distribuidora.oc_related_discovery_service.fetch_relateddetailid_items"
)
@patch(
    "backend.services.distribuidora.sync_related_service._fetch_detail_ids_from_bsale_details"
)
@patch(
    "backend.services.distribuidora.oc_related_discovery_service.load_existing_invoice_relations_for_oc"
)
@patch(
    "backend.services.distribuidora.sync_related_service._bsale_source_id_from_pg"
)
@patch(
    "backend.services.distribuidora.oc_related_discovery_service._oc_number_and_state"
)
def test_full_mode_queries_all_details(
    mock_oc_num,
    mock_source,
    mock_existing,
    mock_details,
    mock_fetch_items,
    mock_triples,
):
    _mock_discovery_stack(
        mock_oc_num,
        mock_source,
        mock_existing,
        mock_details,
        mock_fetch_items,
        mock_triples,
        n_details=50,
        invoice_at_detail=1,
    )

    res = discover_invoice_edges_for_oc(
        MagicMock(),
        MagicMock(),
        69174,
        office_id=1,
        throttle=0,
        discovery_mode=DISCOVERY_MODE_FULL,
    )

    assert res.get("fast_confirmed") is False
    assert res["details_queried"] == 50
    assert mock_fetch_items.call_count == 50


@patch(
    "backend.services.distribuidora.sync_related_service._documents_json_items_to_triples"
)
@patch(
    "backend.services.distribuidora.oc_related_discovery_service.fetch_relateddetailid_items"
)
@patch(
    "backend.services.distribuidora.sync_related_service._fetch_detail_ids_from_bsale_details"
)
@patch(
    "backend.services.distribuidora.oc_related_discovery_service.load_existing_invoice_relations_for_oc"
)
@patch(
    "backend.services.distribuidora.sync_related_service._bsale_source_id_from_pg"
)
@patch(
    "backend.services.distribuidora.oc_related_discovery_service._oc_number_and_state"
)
def test_already_confirmed_skips_bsale_calls(
    mock_oc_num,
    mock_source,
    mock_existing,
    mock_details,
    mock_fetch_items,
    mock_triples,
):
    _mock_discovery_stack(
        mock_oc_num,
        mock_source,
        mock_existing,
        mock_details,
        mock_fetch_items,
        mock_triples,
        n_details=50,
        invoice_at_detail=1,
        existing_confirmed=True,
    )

    res = discover_invoice_edges_for_oc(
        MagicMock(),
        MagicMock(),
        69174,
        office_id=1,
        throttle=0,
        discovery_mode=DISCOVERY_MODE_FAST_CONFIRM,
    )

    assert res["status"] == "existing"
    assert res["api_calls"] == 0
    assert res["early_exit"] is True
    mock_details.assert_not_called()
    mock_fetch_items.assert_not_called()


@patch.dict("os.environ", {}, clear=True)
def test_default_live_runtime_budget_is_240():
    assert resolve_related_sync_max_runtime_sec(True) == 240
    assert resolve_related_sync_max_runtime_sec(False) is None


@patch.dict("os.environ", {"RELATED_SYNC_MAX_RUNTIME_SEC": "90"})
def test_runtime_budget_env_override():
    assert resolve_related_sync_max_runtime_sec(True) == 90


def _pick_meta(pending: list[int], refresh: list[int] | None = None) -> dict:
    refresh = refresh or []
    merged = list(dict.fromkeys(pending + refresh))
    return {
        "pending_without_related": len(pending),
        "pending_total_in_window": len(pending),
        "pending_offset": 0,
        "recent_refresh_candidates": len(refresh),
        "merged_unique": len(merged),
        "pending_ids": pending,
        "refresh_ids": [x for x in refresh if x not in set(pending)],
    }


@patch(
    "backend.services.distribuidora.sync_related_service._process_one_oc_related_sync"
)
@patch(
    "backend.services.distribuidora.sync_related_service.create_bsale_client_for_related_discovery"
)
@patch(
    "backend.services.distribuidora.sync_related_service.emission_date_bounds_for_document_ids",
    return_value=(None, None),
)
@patch(
    "backend.services.distribuidora.sync_related_service._fetch_oc_document_ids_for_incremental"
)
@patch.dict("os.environ", {"BSALE_TOKEN": "tok"})
def test_live_mode_uses_fast_confirm_and_pending_first(
    mock_pick,
    _mock_bounds,
    mock_client_factory,
    mock_process,
):
    mock_pick.return_value = (
        [100, 200, 300],
        _pick_meta([100, 200], [300]),
    )
    mock_client_factory.return_value = MagicMock()

    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value = cur
    cur.fetchone.side_effect = [(True,), (None,)]

    with patch(
        "backend.services.distribuidora.sync_related_service.get_connection",
        return_value=conn,
    ), patch(
        "backend.services.distribuidora.sync_related_service.insert_sync_status_row"
    ), patch(
        "backend.services.distribuidora.sync_related_service._with_deadlock_retry",
        side_effect=lambda _c, _m, fn: fn(),
    ):
        stats = srs.sync_distribuidora_related_documents(
            strict_token=False,
            live_mode=True,
            max_runtime_sec=240,
        )

    assert stats["live_mode"] is True
    assert stats["discovery_mode"] == DISCOVERY_MODE_FAST_CONFIRM
    assert stats["ocs_pending_processed"] == 2
    assert stats["ocs_refresh_processed"] == 1
    assert stats["stop_reason"] == STOP_REASON_COMPLETED
    assert mock_process.call_count == 3
    for call in mock_process.call_args_list:
        assert call.kwargs["discovery_mode"] == DISCOVERY_MODE_FAST_CONFIRM


@patch(
    "backend.services.distribuidora.sync_related_service._process_one_oc_related_sync",
    side_effect=lambda **_kw: time.sleep(0.05),
)
@patch(
    "backend.services.distribuidora.sync_related_service.create_bsale_client_for_related_discovery"
)
@patch(
    "backend.services.distribuidora.sync_related_service.emission_date_bounds_for_document_ids",
    return_value=(None, None),
)
@patch(
    "backend.services.distribuidora.sync_related_service._fetch_oc_document_ids_for_incremental"
)
@patch.dict("os.environ", {"BSALE_TOKEN": "tok"})
def test_runtime_budget_stops_before_all_ocs(
    mock_pick,
    _mock_bounds,
    mock_client_factory,
    _mock_process,
):
    pending = list(range(1, 11))
    mock_pick.return_value = (pending, _pick_meta(pending))
    mock_client_factory.return_value = MagicMock()

    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value = cur
    cur.fetchone.side_effect = [(True,), (None,)]

    with patch(
        "backend.services.distribuidora.sync_related_service.get_connection",
        return_value=conn,
    ), patch(
        "backend.services.distribuidora.sync_related_service.insert_sync_status_row"
    ), patch(
        "backend.services.distribuidora.sync_related_service._with_deadlock_retry",
        side_effect=lambda _c, _m, fn: fn(),
    ):
        stats = srs.sync_distribuidora_related_documents(
            strict_token=False,
            live_mode=True,
            max_runtime_sec=0,
        )

    assert stats["stop_reason"] == STOP_REASON_RUNTIME_BUDGET
    assert stats["ocs_pending_processed"] == 0


@patch.dict("os.environ", {"BSALE_TOKEN": "tok"})
def test_advisory_lock_skips_second_run():
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value = cur
    cur.fetchone.return_value = (False,)

    with patch(
        "backend.services.distribuidora.sync_related_service.get_connection",
        return_value=conn,
    ):
        stats = srs.sync_distribuidora_related_documents(
            strict_token=False,
            live_mode=True,
        )

    assert stats["omitido_concurrencia"] is True
    assert stats["skipped_already_running"] is True
    assert stats["stop_reason"] == STOP_REASON_SKIPPED_ALREADY_RUNNING
