"""Tests: catchup OCâ†’boleta/factura (dry-run real, apply guardado, dedupe)."""

from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from backend.jobs.catchup_oc_invoice_relations import main as job_main
from backend.services.distribuidora.catchup_oc_invoice_relations_service import (
    CATCHUP_DEFAULT_MIN_INTERVAL_SEC,
    CATCHUP_INVOICE_TYPES,
    CATCHUP_MAX_429_RETRIES,
    CatchupApiError,
    PLAN_OC_CANARIES,
    _generation_date_iso,
    _plan_oc_entry,
    discover_invoice_edges_for_oc,
    fetch_relateddetailid_items,
    run_catchup_oc_invoice_relations,
)


def _invoice_item(
    related_id: int,
    *,
    doc_type: int = 6,
    number: int = 50001,
    generation_ts: int = 1720800000,
) -> dict:
    return {
        "id": related_id,
        "number": number,
        "documentTypeId": doc_type,
        "generationDate": generation_ts,
    }


def test_catchup_invoice_types_excludes_nc():
    assert CATCHUP_INVOICE_TYPES == frozenset({1, 6})
    assert 9 not in CATCHUP_INVOICE_TYPES


def test_generation_date_iso_uses_generation_not_emission():
    ts = int(datetime(2026, 7, 15, 12, 0, tzinfo=timezone.utc).timestamp())
    assert _generation_date_iso({"generationDate": ts}) == datetime.fromtimestamp(
        ts, tz=timezone.utc
    ).isoformat()
    assert _generation_date_iso({"emissionDate": ts}) is None


def test_fetch_relateddetailid_items_pagination():
    client = MagicMock()
    client.get.side_effect = [
        {"items": [{"id": 1, "documentTypeId": 6}] * 50},
        {"items": [{"id": 2, "documentTypeId": 6}]},
    ]
    items, calls = fetch_relateddetailid_items(
        client, 999, office_id=1, throttle=0, log_ctx="[test]"
    )
    assert len(items) == 51
    assert calls == 2
    assert client.get.call_count == 2


def test_fetch_relateddetailid_items_429_raises_rate_limited():
    client = MagicMock()
    client.get.side_effect = RuntimeError("Bsale HTTP 429: too many")
    with pytest.raises(CatchupApiError) as exc:
        fetch_relateddetailid_items(client, 1, office_id=1, throttle=0, log_ctx="[t]")
    assert exc.value.rate_limited is True


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
def test_discover_invoice_relation_factura(
    mock_oc_num,
    mock_source,
    mock_existing,
    mock_details,
    mock_fetch_items,
    mock_triples,
):
    mock_oc_num.return_value = (68933, 0)
    mock_source.return_value = (100, 68933)
    mock_existing.return_value = (set(), set())
    mock_details.return_value = ([10, 11], 1)
    mock_fetch_items.return_value = ([_invoice_item(50610, doc_type=6, number=50610)], 1)
    mock_triples.return_value = ([(10, 50610, 6), (11, 50610, 6)], 0)

    cur = MagicMock()
    res = discover_invoice_edges_for_oc(
        MagicMock(), cur, 3867897, office_id=1, throttle=0
    )

    assert res["confirmed_before"] is False
    assert res["would_confirm"] is True
    assert res["status"] == "would_insert"
    assert len(res["edges"]) == 4  # 2 detail_ids Ã— 2 triples por consulta Bsale
    assert len(res["unique_related_documents"]) == 1
    assert all(e["relation_source"] == "bsale_relateddetailid" for e in res["edges"])


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
def test_discover_boleta_and_existing(
    mock_oc_num,
    mock_source,
    mock_existing,
    mock_details,
    mock_fetch_items,
    mock_triples,
):
    mock_oc_num.return_value = (69074, 0)
    mock_source.return_value = (1, 69074)
    mock_existing.return_value = ({(10, 2719368)}, {(2719368, 1)})
    mock_details.return_value = ([10], 1)
    mock_fetch_items.return_value = (
        [_invoice_item(2719368, doc_type=1, number=2719368)],
        1,
    )
    mock_triples.return_value = ([(10, 2719368, 1)], 0)

    res = discover_invoice_edges_for_oc(
        MagicMock(), MagicMock(), 1, office_id=1, throttle=0
    )
    assert res["confirmed_before"] is True
    assert res["would_confirm"] is False
    assert res["edges"][0]["classification"] == "existing"
    assert res["status"] == "existing"


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
def test_discover_nc_not_materialized(
    mock_oc_num,
    mock_source,
    mock_existing,
    mock_details,
    mock_fetch_items,
    mock_triples,
):
    mock_oc_num.return_value = (1, 0)
    mock_source.return_value = (1, 1)
    mock_existing.return_value = (set(), set())
    mock_details.return_value = ([10], 1)
    mock_fetch_items.return_value = ([_invoice_item(99, doc_type=9)], 1)
    mock_triples.return_value = ([(10, 99, 9)], 0)

    res = discover_invoice_edges_for_oc(
        MagicMock(), MagicMock(), 1, office_id=1, throttle=0
    )
    assert res["edges"] == []
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
def test_discover_api_error_not_no_relation(
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
    assert res["rate_limited"] is True
    assert res["would_confirm"] is False
    assert res["edges"] == []


def test_plan_oc_entry_fields():
    oc_res = {
        "oc_number": 68933,
        "oc_document_id": 1,
        "confirmed_before": False,
        "would_confirm": True,
        "status": "would_insert",
        "api_error": None,
        "unique_related_documents": [
            {
                "related_number": 50610,
                "related_document_type": 6,
                "generation_date": "2026-07-12T00:00:00+00:00",
            }
        ],
    }
    entry = _plan_oc_entry(oc_res)
    assert entry["relation_source"] == "bsale_relateddetailid"
    assert entry["would_confirm"] is True
    assert entry["related_type"] == 6


@patch(
    "backend.services.distribuidora.catchup_oc_invoice_relations_service._insert_related_triples"
)
@patch(
    "backend.services.distribuidora.catchup_oc_invoice_relations_service.discover_invoice_edges_for_oc"
)
@patch(
    "backend.services.distribuidora.catchup_oc_invoice_relations_service.fetch_oc_document_ids_for_range"
)
@patch("backend.services.distribuidora.catchup_oc_invoice_relations_service.create_bsale_client_for_related_discovery")
@patch("backend.services.distribuidora.catchup_oc_invoice_relations_service.get_connection")
@patch.dict("os.environ", {"BSALE_TOKEN": "tok"})
def test_dry_run_does_not_insert(
    mock_conn_fn,
    mock_client_cls,
    mock_oc_ids,
    mock_discover,
    mock_insert,
):
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value = cur
    mock_conn_fn.return_value = conn
    mock_oc_ids.return_value = [100]
    mock_discover.return_value = {
        "oc_document_id": 100,
        "oc_number": 68933,
        "detail_ids_consulted": [1],
        "confirmed_before": False,
        "would_confirm": True,
        "status": "would_insert",
        "rate_limited": False,
        "edges": [
            {
                "detail_id": 1,
                "related_document_id": 50610,
                "related_document_type": 6,
                "classification": "would_insert_invoice",
            }
        ],
        "unique_related_documents": [],
    }

    report = run_catchup_oc_invoice_relations(
        start_date=date(2026, 7, 11),
        end_date=date(2026, 8, 25),
        dry_run=True,
    )

    assert report.dry_run is True
    mock_insert.assert_not_called()
    conn.rollback.assert_called_once()
    conn.close.assert_called_once()


@patch(
    "backend.services.distribuidora.catchup_oc_invoice_relations_service._insert_related_triples"
)
@patch(
    "backend.services.distribuidora.catchup_oc_invoice_relations_service.discover_invoice_edges_for_oc"
)
@patch(
    "backend.services.distribuidora.catchup_oc_invoice_relations_service.fetch_oc_document_ids_for_range"
)
@patch("backend.services.distribuidora.catchup_oc_invoice_relations_service.create_bsale_client_for_related_discovery")
@patch("backend.services.distribuidora.catchup_oc_invoice_relations_service.get_connection")
@patch.dict("os.environ", {"BSALE_TOKEN": "tok"})
def test_apply_inserts_verified_triples(
    mock_conn_fn,
    mock_client_cls,
    mock_oc_ids,
    mock_discover,
    mock_insert,
):
    conn = MagicMock()
    conn.cursor.return_value = MagicMock()
    mock_conn_fn.return_value = conn
    mock_oc_ids.return_value = [100]
    mock_discover.return_value = {
        "oc_document_id": 100,
        "oc_number": 1,
        "detail_ids_consulted": [1, 2],
        "confirmed_before": False,
        "would_confirm": True,
        "status": "would_insert",
        "rate_limited": False,
        "edges": [
            {
                "detail_id": 1,
                "related_document_id": 50610,
                "related_document_type": 6,
                "classification": "would_insert_invoice",
            },
            {
                "detail_id": 2,
                "related_document_id": 50610,
                "related_document_type": 6,
                "classification": "would_insert_invoice",
            },
        ],
        "unique_related_documents": [],
    }
    mock_insert.return_value = 2

    report = run_catchup_oc_invoice_relations(
        start_date=date(2026, 7, 11),
        end_date=date(2026, 8, 25),
        dry_run=False,
    )

    assert report.dry_run is False
    mock_insert.assert_called_once()
    triples = mock_insert.call_args[0][2]
    assert triples == [(1, 50610, 6), (2, 50610, 6)]
    assert report.invoice_links_inserted == 2
    conn.rollback.assert_not_called()


@patch(
    "backend.services.distribuidora.catchup_oc_invoice_relations_service.discover_invoice_edges_for_oc"
)
@patch(
    "backend.services.distribuidora.catchup_oc_invoice_relations_service.fetch_oc_document_ids_for_range"
)
@patch("backend.services.distribuidora.catchup_oc_invoice_relations_service.create_bsale_client_for_related_discovery")
@patch("backend.services.distribuidora.catchup_oc_invoice_relations_service.get_connection")
@patch.dict("os.environ", {"BSALE_TOKEN": "tok"})
def test_dedupe_summary_counts_one_invoice_per_oc(
    mock_conn_fn,
    mock_client_cls,
    mock_oc_ids,
    mock_discover,
):
    conn = MagicMock()
    conn.cursor.return_value = MagicMock()
    mock_conn_fn.return_value = conn
    mock_oc_ids.return_value = [100]
    mock_discover.return_value = {
        "oc_document_id": 100,
        "oc_number": 1,
        "detail_ids_consulted": [1, 2],
        "confirmed_before": False,
        "would_confirm": True,
        "status": "would_insert",
        "rate_limited": False,
        "edges": [
            {
                "detail_id": 1,
                "related_document_id": 50610,
                "related_document_type": 6,
                "classification": "would_insert_invoice",
            },
            {
                "detail_id": 2,
                "related_document_id": 50610,
                "related_document_type": 6,
                "classification": "would_insert_invoice",
            },
        ],
        "unique_related_documents": [],
    }

    report = run_catchup_oc_invoice_relations(
        start_date=date(2026, 7, 11),
        end_date=date(2026, 8, 25),
        dry_run=True,
    )

    assert report.invoice_links_would_insert == 1
    assert report.relations_discovered == 1
    assert len(mock_discover.return_value["edges"]) == 2


@patch(
    "backend.services.distribuidora.catchup_oc_invoice_relations_service.discover_invoice_edges_for_oc"
)
@patch(
    "backend.services.distribuidora.catchup_oc_invoice_relations_service.fetch_oc_document_ids_for_range"
)
@patch("backend.services.distribuidora.catchup_oc_invoice_relations_service.create_bsale_client_for_related_discovery")
@patch("backend.services.distribuidora.catchup_oc_invoice_relations_service.get_connection")
@patch.dict("os.environ", {"BSALE_TOKEN": "tok"})
def test_plan_oc_canaries_in_report(
    mock_conn_fn,
    mock_client_cls,
    mock_oc_ids,
    mock_discover,
):
    conn = MagicMock()
    conn.cursor.return_value = MagicMock()
    mock_conn_fn.return_value = conn
    canary = next(iter(PLAN_OC_CANARIES))
    mock_oc_ids.return_value = [1]
    mock_discover.return_value = {
        "oc_document_id": 1,
        "oc_number": canary,
        "detail_ids_consulted": [1],
        "confirmed_before": False,
        "would_confirm": False,
        "status": "no_relation_found",
        "rate_limited": False,
        "edges": [],
        "unique_related_documents": [],
        "api_error": None,
    }

    report = run_catchup_oc_invoice_relations(
        start_date=date(2026, 7, 11),
        end_date=date(2026, 8, 25),
        dry_run=True,
    )

    assert len(report.plan_oc_results) == 1
    assert report.plan_oc_results[0]["oc_number"] == canary
    assert report.plan_oc_results[0]["relation_source"] == "bsale_relateddetailid"


@patch("backend.jobs.catchup_oc_invoice_relations.run_catchup_oc_invoice_relations")
def test_job_apply_requires_double_confirmation(mock_run):
    rc = job_main(
        [
            "--start-date",
            "2026-07-11",
            "--end-date",
            "2026-08-25",
            "--apply",
        ]
    )
    assert rc == 2
    mock_run.assert_not_called()


@patch("backend.jobs.catchup_oc_invoice_relations.run_catchup_oc_invoice_relations")
def test_job_apply_with_both_flags(mock_run):
    from backend.services.distribuidora.catchup_oc_invoice_relations_service import (
        CatchupOcInvoiceReport,
    )

    mock_run.return_value = CatchupOcInvoiceReport(dry_run=False)
    rc = job_main(
        [
            "--start-date",
            "2026-07-11",
            "--end-date",
            "2026-08-25",
            "--apply",
            "--i-understand-writes",
        ]
    )
    assert rc == 0
    assert mock_run.call_args.kwargs["dry_run"] is False


@patch("backend.jobs.catchup_oc_invoice_relations.run_catchup_oc_invoice_relations")
def test_job_default_dry_run(mock_run):
    from backend.services.distribuidora.catchup_oc_invoice_relations_service import (
        CatchupOcInvoiceReport,
    )

    mock_run.return_value = CatchupOcInvoiceReport(dry_run=True)
    rc = job_main(["--start-date", "2026-07-11", "--end-date", "2026-08-25"])
    assert rc == 0
    assert mock_run.call_args.kwargs["dry_run"] is True


@patch(
    "backend.services.distribuidora.catchup_oc_invoice_relations_service._insert_related_triples"
)
@patch(
    "backend.services.distribuidora.catchup_oc_invoice_relations_service.discover_invoice_edges_for_oc"
)
@patch(
    "backend.services.distribuidora.catchup_oc_invoice_relations_service.fetch_oc_document_ids_for_range"
)
@patch("backend.services.distribuidora.catchup_oc_invoice_relations_service.create_bsale_client_for_related_discovery")
@patch("backend.services.distribuidora.catchup_oc_invoice_relations_service.get_connection")
@patch.dict("os.environ", {"BSALE_TOKEN": "tok"})
def test_apply_idempotent_on_conflict(
    mock_conn_fn,
    mock_client_cls,
    mock_oc_ids,
    mock_discover,
    mock_insert,
):
    """Segunda pasada: ON CONFLICT DO NOTHING â†’ 0 insertadas."""
    conn = MagicMock()
    conn.cursor.return_value = MagicMock()
    mock_conn_fn.return_value = conn
    mock_oc_ids.return_value = [100]
    mock_discover.return_value = {
        "oc_document_id": 100,
        "oc_number": 1,
        "detail_ids_consulted": [1],
        "confirmed_before": True,
        "would_confirm": True,
        "status": "would_insert",
        "rate_limited": False,
        "edges": [
            {
                "detail_id": 1,
                "related_document_id": 50610,
                "related_document_type": 6,
                "classification": "would_insert_invoice",
            }
        ],
        "unique_related_documents": [],
    }
    mock_insert.return_value = 0

    report = run_catchup_oc_invoice_relations(
        start_date=date(2026, 7, 11),
        end_date=date(2026, 8, 25),
        dry_run=False,
    )

    mock_insert.assert_called_once()
    assert report.invoice_links_inserted == 0


def test_catchup_rate_limit_defaults():
    assert CATCHUP_MAX_429_RETRIES == 5
    assert 0.75 <= CATCHUP_DEFAULT_MIN_INTERVAL_SEC <= 1.0


@patch(
    "backend.services.distribuidora.catchup_oc_invoice_relations_service.discover_invoice_edges_for_oc"
)
@patch(
    "backend.services.distribuidora.catchup_oc_invoice_relations_service.fetch_oc_document_ids_for_range"
)
@patch("backend.services.distribuidora.catchup_oc_invoice_relations_service.create_bsale_client_for_related_discovery")
@patch("backend.services.distribuidora.catchup_oc_invoice_relations_service.get_connection")
@patch.dict("os.environ", {"BSALE_TOKEN": "tok"})
def test_no_relation_found_counter(
    mock_conn_fn,
    mock_client_cls,
    mock_oc_ids,
    mock_discover,
):
    conn = MagicMock()
    conn.cursor.return_value = MagicMock()
    mock_conn_fn.return_value = conn
    mock_oc_ids.return_value = [100, 101]
    mock_discover.side_effect = [
        {
            "oc_document_id": 100,
            "oc_number": 1,
            "detail_ids_consulted": [1],
            "confirmed_before": False,
            "would_confirm": False,
            "status": "no_relation_found",
            "rate_limited": False,
            "edges": [],
            "unique_related_documents": [],
        },
        {
            "oc_document_id": 101,
            "oc_number": 2,
            "detail_ids_consulted": [2],
            "confirmed_before": False,
            "would_confirm": False,
            "status": "api_error",
            "rate_limited": False,
            "edges": [],
            "unique_related_documents": [],
        },
    ]

    report = run_catchup_oc_invoice_relations(
        start_date=date(2026, 7, 11),
        end_date=date(2026, 8, 25),
        dry_run=True,
    )

    assert report.ocs_without_relation == 1
    assert report.api_errors == 1
    assert report.ocs_completed == 2


@patch(
    "backend.services.distribuidora.catchup_oc_invoice_relations_service.discover_invoice_edges_for_oc"
)
@patch(
    "backend.services.distribuidora.catchup_oc_invoice_relations_service.fetch_oc_document_ids_for_range"
)
@patch("backend.services.distribuidora.catchup_oc_invoice_relations_service.create_bsale_client_for_related_discovery")
@patch("backend.services.distribuidora.catchup_oc_invoice_relations_service.get_connection")
@patch.dict("os.environ", {"BSALE_TOKEN": "tok"})
def test_rate_limited_oc_not_counted_as_no_relation(
    mock_conn_fn,
    mock_client_cls,
    mock_oc_ids,
    mock_discover,
):
    conn = MagicMock()
    conn.cursor.return_value = MagicMock()
    mock_conn_fn.return_value = conn
    mock_oc_ids.return_value = [100]
    mock_discover.return_value = {
        "oc_document_id": 100,
        "oc_number": 68933,
        "detail_ids_consulted": [1],
        "confirmed_before": False,
        "would_confirm": False,
        "status": "rate_limited",
        "rate_limited": True,
        "edges": [],
        "unique_related_documents": [],
        "api_error": "Bsale HTTP 429",
    }

    report = run_catchup_oc_invoice_relations(
        start_date=date(2026, 7, 11),
        end_date=date(2026, 8, 25),
        dry_run=True,
    )

    assert report.ocs_without_relation == 0
    assert report.ocs_rate_limited == 1
    assert report.rate_limited == 1
    assert report.ocs_completed == 1
    mock_client_cls.assert_called_once()
    assert mock_client_cls.call_args.args[0] == "tok"
    assert "rate_stats" in mock_client_cls.call_args.kwargs
    assert CATCHUP_MAX_429_RETRIES == 5
    assert CATCHUP_DEFAULT_MIN_INTERVAL_SEC == 0.75

@patch(
    "backend.services.distribuidora.catchup_oc_invoice_relations_service.discover_invoice_edges_for_oc"
)
@patch(
    "backend.services.distribuidora.catchup_oc_invoice_relations_service.fetch_oc_document_ids_for_range"
)
@patch("backend.services.distribuidora.catchup_oc_invoice_relations_service.create_bsale_client_for_related_discovery")
@patch("backend.services.distribuidora.catchup_oc_invoice_relations_service.get_connection")
@patch.dict("os.environ", {"BSALE_TOKEN": "tok"})
def test_no_relation_folios_lists_all(
    mock_conn_fn,
    mock_client_cls,
    mock_oc_ids,
    mock_discover,
):
    conn = MagicMock()
    conn.cursor.return_value = MagicMock()
    mock_conn_fn.return_value = conn
    mock_oc_ids.return_value = list(range(1, 10))
    mock_discover.side_effect = [
        {
            "oc_document_id": i,
            "oc_number": 68000 + i,
            "detail_ids_consulted": [],
            "confirmed_before": False,
            "would_confirm": False,
            "status": "no_relation_found",
            "rate_limited": False,
            "edges": [],
            "unique_related_documents": [],
        }
        for i in range(1, 10)
    ]

    report = run_catchup_oc_invoice_relations(
        start_date=date(2026, 8, 3),
        end_date=date(2026, 8, 15),
        dry_run=True,
    )

    assert report.ocs_without_relation == 9
    assert report.no_relation_folios == list(range(68001, 68010))
    assert len(report.samples.get("no_relation_examples") or []) <= 8


@patch(
    "backend.services.distribuidora.oc_related_discovery_service._oc_number_and_state"
)
@patch(
    "backend.services.distribuidora.oc_related_discovery_service.load_existing_invoice_relations_for_oc"
)
@patch(
    "backend.services.distribuidora.sync_related_service._bsale_source_id_from_pg"
)
def test_discover_cancelled_skips_bsale_and_not_no_relation(
    mock_source,
    mock_existing,
    mock_num_state,
):
    mock_num_state.return_value = (68544, 8888)
    mock_source.return_value = (1, 68544)
    mock_existing.return_value = (set(), set())
    client = MagicMock()
    res = discover_invoice_edges_for_oc(client, MagicMock(), 1, office_id=1, throttle=0)
    assert res["status"] == "cancelled"
    assert res["would_confirm"] is False
    client.get.assert_not_called()


# ---------------------------------------------------------------------------
# Modo canario --oc-number
# ---------------------------------------------------------------------------


def test_find_oc_by_number_filters_company_office_type():
    from backend.services.distribuidora.catchup_oc_invoice_relations_service import (
        DOC_TYPE_OC,
        find_oc_by_number,
    )

    cur = MagicMock()
    cur.fetchone.return_value = (
        40001,
        69087,
        datetime(2026, 8, 20, 12, 0, tzinfo=timezone.utc),
        datetime(2026, 8, 20, 12, 5, tzinfo=timezone.utc),
        0,
    )
    meta = find_oc_by_number(cur, oc_number=69087, company_id=3, office_id=1)
    assert meta is not None
    assert meta["oc_document_id"] == 40001
    assert meta["oc_number"] == 69087
    assert meta["state"] == 0
    assert meta["emission_date"] is not None
    sql = cur.execute.call_args[0][0]
    params = cur.execute.call_args[0][1]
    where_clause = sql.lower().split("where", 1)[1].split("order", 1)[0]
    assert "emission_date" not in where_clause
    assert params == (3, 1, DOC_TYPE_OC, 69087)


def test_find_oc_by_number_missing_returns_none():
    from backend.services.distribuidora.catchup_oc_invoice_relations_service import (
        find_oc_by_number,
    )

    cur = MagicMock()
    cur.fetchone.return_value = None
    assert find_oc_by_number(cur, oc_number=99999, company_id=3, office_id=1) is None


@patch(
    "backend.services.distribuidora.catchup_oc_invoice_relations_service.discover_invoice_edges_for_oc"
)
@patch(
    "backend.services.distribuidora.catchup_oc_invoice_relations_service.find_oc_by_number"
)
@patch(
    "backend.services.distribuidora.catchup_oc_invoice_relations_service.create_bsale_client_for_related_discovery"
)
@patch(
    "backend.services.distribuidora.catchup_oc_invoice_relations_service.get_connection"
)
@patch.dict("os.environ", {"BSALE_TOKEN": "tok"})
def test_canary_oc_found_with_relation(
    mock_conn_fn,
    mock_client_cls,
    mock_find,
    mock_discover,
):
    from backend.services.distribuidora.catchup_oc_invoice_relations_service import (
        run_catchup_oc_invoice_relations_by_oc_number,
    )

    conn = MagicMock()
    conn.cursor.return_value = MagicMock()
    mock_conn_fn.return_value = conn
    mock_find.return_value = {
        "oc_document_id": 40001,
        "oc_number": 69087,
        "emission_date": "2026-08-20T12:00:00+00:00",
        "generation_date": "2026-08-20T12:05:00+00:00",
        "state": 0,
    }
    mock_discover.return_value = {
        "oc_document_id": 40001,
        "oc_number": 69087,
        "detail_ids_consulted": [10, 11],
        "confirmed_before": False,
        "would_confirm": True,
        "status": "would_insert",
        "rate_limited": False,
        "edges": [
            {
                "detail_id": 10,
                "related_document_id": 2721119,
                "related_document_type": 1,
                "classification": "would_insert_receipt",
            }
        ],
        "unique_related_documents": [
            {
                "related_number": 2721119,
                "related_document_type": 1,
                "generation_date": "2026-08-27T19:44:18+00:00",
            }
        ],
    }

    report = run_catchup_oc_invoice_relations_by_oc_number(
        oc_number=69087, company_id=3, office_id=1, dry_run=True
    )

    assert report.mode == "by_oc_number"
    assert report.oc_scanned == 1
    assert report.dry_run is True
    c = report.canary
    assert c["oc_number"] == 69087
    assert c["oc_document_id"] == 40001
    assert c["emission_date"] == "2026-08-20T12:00:00+00:00"
    assert c["generation_date"] == "2026-08-20T12:05:00+00:00"
    assert c["state"] == 0
    assert c["details_scanned"] == 2
    assert c["confirmed_before"] is False
    assert c["status"] == "would_insert"
    assert c["would_confirm"] is True
    assert c["related_number"] == 2721119
    assert c["related_type"] == 1
    assert c["related_generation_date"] == "2026-08-27T19:44:18+00:00"
    assert c["unique_docs_count"] == 1
    assert c["edges_count"] == 1
    mock_find.assert_called_once()
    assert mock_find.call_args.kwargs["company_id"] == 3
    assert mock_find.call_args.kwargs["office_id"] == 1
    mock_discover.assert_called_once()
    assert mock_discover.call_args.args[2] == 40001


@patch(
    "backend.services.distribuidora.catchup_oc_invoice_relations_service.discover_invoice_edges_for_oc"
)
@patch(
    "backend.services.distribuidora.catchup_oc_invoice_relations_service.find_oc_by_number"
)
@patch(
    "backend.services.distribuidora.catchup_oc_invoice_relations_service.create_bsale_client_for_related_discovery"
)
@patch(
    "backend.services.distribuidora.catchup_oc_invoice_relations_service.get_connection"
)
@patch.dict("os.environ", {"BSALE_TOKEN": "tok"})
def test_canary_oc_found_no_relation(
    mock_conn_fn,
    mock_client_cls,
    mock_find,
    mock_discover,
):
    from backend.services.distribuidora.catchup_oc_invoice_relations_service import (
        run_catchup_oc_invoice_relations_by_oc_number,
    )

    conn = MagicMock()
    conn.cursor.return_value = MagicMock()
    mock_conn_fn.return_value = conn
    mock_find.return_value = {
        "oc_document_id": 40002,
        "oc_number": 69094,
        "emission_date": "2026-08-21T00:00:00+00:00",
        "generation_date": None,
        "state": 0,
    }
    mock_discover.return_value = {
        "oc_document_id": 40002,
        "oc_number": 69094,
        "detail_ids_consulted": [5],
        "confirmed_before": False,
        "would_confirm": False,
        "status": "no_relation_found",
        "rate_limited": False,
        "edges": [],
        "unique_related_documents": [],
    }

    report = run_catchup_oc_invoice_relations_by_oc_number(oc_number=69094, dry_run=True)
    assert report.canary["status"] == "no_relation_found"
    assert report.canary["would_confirm"] is False
    assert report.ocs_without_relation == 1
    assert report.canary["related_number"] is None


@patch(
    "backend.services.distribuidora.catchup_oc_invoice_relations_service.discover_invoice_edges_for_oc"
)
@patch(
    "backend.services.distribuidora.catchup_oc_invoice_relations_service.find_oc_by_number",
    return_value=None,
)
@patch(
    "backend.services.distribuidora.catchup_oc_invoice_relations_service.create_bsale_client_for_related_discovery"
)
@patch(
    "backend.services.distribuidora.catchup_oc_invoice_relations_service.get_connection"
)
@patch.dict("os.environ", {"BSALE_TOKEN": "tok"})
def test_canary_local_oc_not_found(
    mock_conn_fn,
    mock_client_cls,
    mock_find,
    mock_discover,
):
    from backend.services.distribuidora.catchup_oc_invoice_relations_service import (
        run_catchup_oc_invoice_relations_by_oc_number,
    )

    conn = MagicMock()
    conn.cursor.return_value = MagicMock()
    mock_conn_fn.return_value = conn

    report = run_catchup_oc_invoice_relations_by_oc_number(oc_number=69087, dry_run=True)
    assert report.canary["status"] == "local_oc_not_found"
    assert report.oc_scanned == 0
    assert report.canary["oc_document_id"] is None
    mock_discover.assert_not_called()
    mock_client_cls.assert_not_called()


@patch(
    "backend.services.distribuidora.catchup_oc_invoice_relations_service.fetch_oc_document_ids_for_range"
)
@patch(
    "backend.jobs.catchup_oc_invoice_relations.run_catchup_oc_invoice_relations_by_oc_number"
)
def test_job_cli_oc_number_skips_range(mock_by_number, mock_range):
    from backend.services.distribuidora.catchup_oc_invoice_relations_service import (
        CatchupOcInvoiceReport,
    )

    mock_by_number.return_value = CatchupOcInvoiceReport(
        dry_run=True,
        mode="by_oc_number",
        oc_number=69087,
        canary={"oc_number": 69087, "status": "no_relation_found"},
    )
    rc = job_main(
        [
            "--company-id",
            "3",
            "--office-id",
            "1",
            "--oc-number",
            "69087",
            "--dry-run",
        ]
    )
    assert rc == 0
    mock_by_number.assert_called_once()
    assert mock_by_number.call_args.kwargs["oc_number"] == 69087
    assert mock_by_number.call_args.kwargs["company_id"] == 3
    assert mock_by_number.call_args.kwargs["office_id"] == 1
    assert mock_by_number.call_args.kwargs["dry_run"] is True
    mock_range.assert_not_called()


def test_job_cli_requires_range_or_oc_number():
    rc = job_main(["--company-id", "3", "--office-id", "1"])
    assert rc == 2


@patch(
    "backend.services.distribuidora.catchup_oc_invoice_relations_service.discover_invoice_edges_for_oc"
)
@patch(
    "backend.services.distribuidora.catchup_oc_invoice_relations_service.find_oc_by_number"
)
@patch(
    "backend.services.distribuidora.catchup_oc_invoice_relations_service.create_bsale_client_for_related_discovery"
)
@patch(
    "backend.services.distribuidora.catchup_oc_invoice_relations_service.get_connection"
)
@patch.dict("os.environ", {"BSALE_TOKEN": "tok"})
def test_canary_uses_canonical_discover(
    mock_conn_fn,
    mock_client_cls,
    mock_find,
    mock_discover,
):
    """Paridad: canario llama discover_invoice_edges_for_oc (motor canónico)."""
    from backend.services.distribuidora.catchup_oc_invoice_relations_service import (
        run_catchup_oc_invoice_relations_by_oc_number,
    )

    conn = MagicMock()
    conn.cursor.return_value = MagicMock()
    mock_conn_fn.return_value = conn
    mock_find.return_value = {
        "oc_document_id": 55,
        "oc_number": 69095,
        "emission_date": None,
        "generation_date": None,
        "state": 0,
    }
    mock_discover.return_value = {
        "oc_document_id": 55,
        "oc_number": 69095,
        "detail_ids_consulted": [],
        "confirmed_before": False,
        "would_confirm": False,
        "status": "no_relation_found",
        "edges": [],
        "unique_related_documents": [],
    }
    run_catchup_oc_invoice_relations_by_oc_number(oc_number=69095, dry_run=True)
    assert mock_discover.called
    assert mock_discover.call_args.kwargs.get("office_id") == 1

