"""Paridad live/catchup y regresión OC facturada días después."""

from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from backend.services.distribuidora import sync_related_service as srs
from backend.services.distribuidora.oc_related_discovery_service import (
    CatchupApiError,
    compute_pending_rotation_offset,
    discover_confirmed_related_documents_for_oc,
    discover_invoice_edges_for_oc,
    fetch_pending_oc_ids_for_incremental,
    resolve_related_sync_lookback_days,
)
from backend.services.distribuidora.sync_related_service import (
    _fetch_oc_document_ids_for_incremental,
)


def _invoice_item(
    related_id: int,
    *,
    doc_type: int = 1,
    number: int = 2721119,
    generation_ts: int | None = None,
) -> dict:
    ts = generation_ts or int(
        datetime(2026, 8, 27, 19, 44, 18, tzinfo=timezone.utc).timestamp()
    )
    return {
        "id": related_id,
        "number": number,
        "documentTypeId": doc_type,
        "generationDate": ts,
    }


@patch.dict("os.environ", {}, clear=True)
def test_default_lookback_is_14_days():
    assert resolve_related_sync_lookback_days() == 14


@patch.dict("os.environ", {"RELATED_SYNC_LOOKBACK_DAYS": "21"})
def test_lookback_env_override():
    assert resolve_related_sync_lookback_days() == 21


@patch.dict("os.environ", {"LIVE_SYNC_RELATED_WINDOW_DAYS": "3"})
def test_legacy_live_window_env_still_works():
    assert resolve_related_sync_lookback_days() == 3


def test_rotation_offset_cycles_when_many_pending():
    # 1000 pending, limit 400 → 3 pages → offsets 0, 400, 800
    total = 1000
    limit = 400
    offsets = {
        compute_pending_rotation_offset(total, limit, slot_seconds=300, now_ts=0),
        compute_pending_rotation_offset(total, limit, slot_seconds=300, now_ts=300),
        compute_pending_rotation_offset(total, limit, slot_seconds=300, now_ts=600),
    }
    assert offsets == {0, 400, 800}


def test_rotation_offset_zero_when_few_pending():
    assert compute_pending_rotation_offset(50, 400) == 0


@patch(
    "backend.services.distribuidora.sync_related_service.count_pending_ocs_in_lookback",
    return_value=500,
)
@patch(
    "backend.services.distribuidora.sync_related_service.fetch_pending_oc_ids_for_incremental"
)
@patch(
    "backend.services.distribuidora.sync_related_service.fetch_recent_oc_ids_for_refresh"
)
@patch(
    "backend.services.distribuidora.sync_related_service.compute_pending_rotation_offset",
    return_value=400,
)
def test_incremental_fetch_passes_rotation_offset(
    mock_offset,
    mock_refresh,
    mock_pending,
    _mock_count,
):
    cur = MagicMock()
    mock_pending.return_value = [68995]
    mock_refresh.return_value = []
    ids, meta = _fetch_oc_document_ids_for_incremental(
        cur, lookback_days=14, limit_documents=250
    )
    assert 68995 in ids
    assert meta["pending_offset"] == 400
    mock_pending.assert_called_once()
    assert mock_pending.call_args.kwargs["pending_offset"] == 400


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
def test_oc_68995_boleta_discovered_after_delay(
    mock_oc_num,
    mock_source,
    mock_existing,
    mock_details,
    mock_fetch_items,
    mock_triples,
):
    """OC emitida 20/08, boleta 27/08: motor canónico la detecta."""
    mock_oc_num.return_value = (68995, 0)
    mock_source.return_value = (68995, 68995)
    mock_existing.return_value = (set(), set())
    mock_details.return_value = ([101, 102, 103], 1)
    mock_fetch_items.return_value = ([_invoice_item(2721119, doc_type=1)], 1)
    mock_triples.side_effect = [
        ([(101, 2721119, 1)], 0),
        ([(102, 2721119, 1)], 0),
        ([(103, 2721119, 1)], 0),
    ]

    res = discover_confirmed_related_documents_for_oc(
        MagicMock(), MagicMock(), 68995, office_id=1, throttle=0
    )
    assert res["would_confirm"] is True
    assert res["status"] == "would_insert"
    assert len(res["edges"]) == 3
    assert res["unique_related_documents"][0]["related_document_type"] == 1


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
def test_live_and_catchup_same_discovery_result(
    mock_oc_num,
    mock_source,
    mock_existing,
    mock_details,
    mock_fetch_items,
    mock_triples,
):
    mocks = (
        mock_oc_num,
        mock_source,
        mock_existing,
        mock_details,
        mock_fetch_items,
        mock_triples,
    )
    mock_oc_num.return_value = (68995, 0)
    mock_source.return_value = (68995, 68995)
    mock_existing.return_value = (set(), set())
    mock_details.return_value = ([10], 1)
    mock_fetch_items.return_value = ([_invoice_item(2721119)], 1)
    mock_triples.return_value = ([(10, 2721119, 1)], 0)

    live_res = discover_invoice_edges_for_oc(
        MagicMock(), MagicMock(), 68995, office_id=1, throttle=0
    )
    catchup_res = discover_confirmed_related_documents_for_oc(
        MagicMock(), MagicMock(), 68995, office_id=1, throttle=0
    )
    assert live_res["would_confirm"] == catchup_res["would_confirm"]
    assert live_res["edges"] == catchup_res["edges"]
    assert live_res["status"] == catchup_res["status"]


@patch(
    "backend.services.distribuidora.sync_related_service.discover_invoice_edges_for_oc"
)
@patch(
    "backend.services.distribuidora.sync_related_service.create_bsale_client_for_related_discovery"
)
@patch(
    "backend.services.distribuidora.sync_related_service.emission_date_bounds_for_document_ids",
    return_value=("2026-08-20T00:00:00+00:00", "2026-08-27T00:00:00+00:00"),
)
@patch(
    "backend.services.distribuidora.sync_related_service._fetch_oc_document_ids_for_incremental",
    return_value=([68995], {"pending_without_related": 1, "pending_total_in_window": 1, "pending_offset": 0, "recent_refresh_candidates": 0, "merged_unique": 1}),
)
@patch.dict("os.environ", {"BSALE_TOKEN": "tok"})
def test_sync_incremental_uses_canonical_discovery(
    _mock_pick,
    _mock_bounds,
    mock_client_factory,
    mock_discover,
):
    mock_client_factory.return_value = MagicMock()
    mock_discover.return_value = {
        "oc_document_id": 68995,
        "oc_number": 68995,
        "would_confirm": True,
        "status": "would_insert",
        "edges": [
            {
                "detail_id": 1,
                "related_document_id": 2721119,
                "related_document_type": 1,
                "classification": "would_insert_receipt",
            }
        ],
        "detail_ids_consulted": [1],
        "api_calls": 2,
    }
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value = cur
    cur.fetchone.side_effect = [
        (True,),  # advisory lock
        (None, None),  # unlock
    ]

    with patch("backend.services.distribuidora.sync_related_service.get_connection", return_value=conn), patch(
        "backend.services.distribuidora.sync_related_service.apply_discovered_invoice_edges",
        return_value=1,
    ) as mock_apply, patch(
        "backend.services.distribuidora.sync_related_service.insert_sync_status_row"
    ), patch(
        "backend.services.distribuidora.sync_related_service._with_deadlock_retry",
        side_effect=lambda _c, _m, fn: fn(),
    ):
        stats = srs.sync_distribuidora_related_documents(strict_token=False, lookback_days=14)

    mock_discover.assert_called()
    mock_apply.assert_called()
    assert stats["discovered"] == 1
    assert stats["rows_inserted"] == 1
    assert stats["candidate_window_days"] == 14


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
def test_no_relation_before_boleta_exists(
    mock_oc_num,
    mock_source,
    mock_existing,
    mock_details,
    mock_fetch_items,
):
    mock_oc_num.return_value = (68995, 0)
    mock_source.return_value = (68995, 68995)
    mock_existing.return_value = (set(), set())
    mock_details.return_value = ([10], 1)
    mock_fetch_items.return_value = ([], 1)

    res = discover_invoice_edges_for_oc(
        MagicMock(), MagicMock(), 68995, office_id=1, throttle=0
    )
    assert res["would_confirm"] is False
    assert res["status"] == "no_relation_found"


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
def test_429_not_classified_as_no_relation(
    mock_oc_num,
    mock_source,
    mock_existing,
    mock_details,
    mock_fetch_items,
):
    mock_oc_num.return_value = (1, 0)
    mock_source.return_value = (1, 1)
    mock_existing.return_value = (set(), set())
    mock_details.return_value = ([10], 1)
    mock_fetch_items.side_effect = CatchupApiError("429", rate_limited=True)

    res = discover_invoice_edges_for_oc(
        MagicMock(), MagicMock(), 1, office_id=1, throttle=0
    )
    assert res["status"] == "rate_limited"
    assert res["would_confirm"] is False


def test_lookback_3_excludes_oc_8_days_old():
    """Regresión: lookback 3 días excluye OC del 20/08 al 28/08 (causa original)."""
    emission = datetime(2026, 8, 20, 12, 0, tzinfo=timezone.utc)
    now = datetime(2026, 8, 28, 12, 0, tzinfo=timezone.utc)
    age_days = (now - emission).days
    assert age_days == 8
    assert age_days > resolve_related_sync_lookback_days(3)
    assert age_days <= resolve_related_sync_lookback_days(14)
