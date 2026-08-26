"""Tests: sync headers faltantes para document_related huérfanos (sin DB real)."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from backend.jobs.sync_missing_related_documents import main as job_main
from backend.repositories.distribuidora.document_related_repo import (
    ORPHAN_RELATED_DOCUMENT_TYPES,
)
from backend.services.distribuidora.oc_document_chain_resolver import (
    resolve_oc_operational_status_from_parts,
)
from backend.services.distribuidora.oc_operational_status import (
    BILLING_INVOICED,
    BILLING_INVOICED_FULL_CN,
    is_predespacho_pending_row,
)
from backend.services.distribuidora.sync_missing_related_documents_service import (
    CANARY_OC_68677,
    MissingRelatedApiError,
    bsale_document_type_id,
    fetch_bsale_document_by_id,
    run_sync_missing_related_documents,
    validate_bsale_against_candidate,
)


def _bsale_doc(
    doc_id: int,
    *,
    doc_type: int = 6,
    number: int = 50367,
    state: int = 0,
    generation_ts: int | None = None,
) -> dict:
    ts = generation_ts or int(datetime(2026, 8, 6, 12, 0, tzinfo=timezone.utc).timestamp())
    return {
        "id": doc_id,
        "number": number,
        "state": state,
        "generationDate": ts,
        "emissionDate": ts,
        "totalAmount": 310350,
        "document_type": {"id": doc_type},
        "company": {"id": 3},
        "office": {"id": 1},
        "client": {"id": 100},
    }


def _orphan_row(
    related_id: int = 3853417,
    related_type: int = 6,
) -> dict:
    return {
        "related_document_id": related_id,
        "related_document_type": related_type,
        "reference_count": 1,
        "oc_document_ids": [3852324],
        "oc_numbers": [68677],
        "origin_detail_ids": [9019600],
        "earliest_oc_emission": datetime(2026, 8, 6, tzinfo=timezone.utc),
    }


@pytest.fixture
def mock_conn():
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value = cur
    return conn, cur


def test_orphan_types_include_invoice_boleta_nc():
    assert ORPHAN_RELATED_DOCUMENT_TYPES == frozenset({1, 6, 9})


def test_canary_68677_constants():
    assert CANARY_OC_68677["oc_number"] == 68677
    assert CANARY_OC_68677["related_document_id"] == 3853417
    assert CANARY_OC_68677["related_document_type"] == 6
    assert CANARY_OC_68677["expected_bsale_number"] == 50367
    assert CANARY_OC_68677["expected_nc_number"] == 18408


def test_bsale_document_type_from_blob():
    assert bsale_document_type_id(_bsale_doc(1, doc_type=6)) == 6
    assert bsale_document_type_id({"documentTypeId": 9}) == 9


def test_validate_rejects_type_mismatch():
    ok, reason = validate_bsale_against_candidate(
        related_document_id=3853417,
        expected_type=6,
        blob=_bsale_doc(3853417, doc_type=1),
        company_id=3,
        office_id=1,
    )
    assert ok is False
    assert reason == "type_mismatch expected=6 bsale=1"


def test_validate_accepts_matching_invoice():
    ok, reason = validate_bsale_against_candidate(
        related_document_id=3853417,
        expected_type=6,
        blob=_bsale_doc(3853417, doc_type=6, number=50367),
        company_id=3,
        office_id=1,
    )
    assert ok is True
    assert reason is None


def test_fetch_bsale_document_by_id_uses_documents_path():
    client = MagicMock()
    client.get.return_value = _bsale_doc(3853417)
    doc = fetch_bsale_document_by_id(client, 3853417)
    assert doc["number"] == 50367
    client.get.assert_called_once_with("/documents/3853417.json")


def test_fetch_bsale_document_not_found():
    client = MagicMock()
    client.get.side_effect = RuntimeError("Bsale HTTP 404: not found")
    with pytest.raises(MissingRelatedApiError) as exc:
        fetch_bsale_document_by_id(client, 999)
    assert exc.value.not_found is True


def test_fetch_bsale_document_id_mismatch_is_ambiguous():
    client = MagicMock()
    client.get.return_value = _bsale_doc(111)
    with pytest.raises(MissingRelatedApiError, match="Ambigüedad"):
        fetch_bsale_document_by_id(client, 3853417)


@patch("backend.services.distribuidora.sync_missing_related_documents_service._bsale_token")
@patch("backend.services.distribuidora.sync_missing_related_documents_service.get_connection")
@patch(
    "backend.services.distribuidora.sync_missing_related_documents_service.fetch_orphan_related_document_candidates"
)
@patch(
    "backend.services.distribuidora.sync_missing_related_documents_service.document_header_exists"
)
@patch(
    "backend.services.distribuidora.sync_missing_related_documents_service.fetch_bsale_document_by_id"
)
@patch(
    "backend.services.distribuidora.sync_missing_related_documents_service.discover_nc_bsale_ids_for_invoice"
)
@patch(
    "backend.services.distribuidora.sync_missing_related_documents_service._materialize_cn_for_invoice"
)
def test_dry_run_does_not_persist(
    mock_cn,
    mock_discover_nc,
    mock_fetch_bsale,
    mock_header_exists,
    mock_fetch_orphans,
    mock_get_conn,
    mock_token,
    mock_conn,
):
    conn, cur = mock_conn
    mock_get_conn.return_value = conn
    mock_token.return_value = "tok"
    mock_fetch_orphans.return_value = [_orphan_row()]
    mock_header_exists.return_value = False
    mock_fetch_bsale.return_value = _bsale_doc(3853417, number=50367)
    mock_discover_nc.return_value = [{"nc_bsale_document_id": 99918408, "nc_number": 18408}]
    mock_cn.return_value = (0, 0)

    with patch(
        "backend.services.distribuidora.sync_missing_related_documents_service.BsaleClient"
    ):
        report = run_sync_missing_related_documents(dry_run=True, related_document_ids=[3853417])

    assert report.dry_run is True
    assert report.candidates == 1
    assert report.found_in_bsale == 1
    assert report.would_insert == 1
    assert report.headers_inserted == 0
    assert len(report.samples) == 1
    s = report.samples[0]
    assert s["would_insert"] is True
    assert s["bsale_number"] == 50367
    assert s["expected_related_document_type"] == 6


@pytest.mark.parametrize("doc_type,number", [(6, 50367), (1, 60001), (9, 18408)])
@patch("backend.services.distribuidora.sync_missing_related_documents_service._bsale_token")
@patch("backend.services.distribuidora.sync_missing_related_documents_service.get_connection")
@patch(
    "backend.services.distribuidora.sync_missing_related_documents_service.fetch_orphan_related_document_candidates"
)
@patch(
    "backend.services.distribuidora.sync_missing_related_documents_service.document_header_exists"
)
@patch(
    "backend.services.distribuidora.sync_missing_related_documents_service.fetch_bsale_document_by_id"
)
@patch(
    "backend.services.distribuidora.sync_missing_related_documents_service.discover_nc_bsale_ids_for_invoice"
)
@patch(
    "backend.services.distribuidora.sync_missing_related_documents_service._materialize_cn_for_invoice"
)
def test_dry_run_recoverable_document_types(
    mock_cn,
    mock_discover_nc,
    mock_fetch_bsale,
    mock_header_exists,
    mock_fetch_orphans,
    mock_get_conn,
    mock_token,
    doc_type,
    number,
    mock_conn,
):
    conn, _cur = mock_conn
    mock_get_conn.return_value = conn
    mock_token.return_value = "tok"
    mock_fetch_orphans.return_value = [_orphan_row(related_id=100, related_type=doc_type)]
    mock_header_exists.return_value = False
    mock_fetch_bsale.return_value = _bsale_doc(100, doc_type=doc_type, number=number)
    mock_discover_nc.return_value = []
    mock_cn.return_value = (0, 0)

    with patch(
        "backend.services.distribuidora.sync_missing_related_documents_service.BsaleClient"
    ):
        report = run_sync_missing_related_documents(dry_run=True)

    assert report.would_insert == 1
    assert report.samples[0]["bsale_document_type"] == doc_type
    assert report.samples[0]["bsale_number"] == number


@patch("backend.services.distribuidora.sync_missing_related_documents_service._bsale_token")
@patch("backend.services.distribuidora.sync_missing_related_documents_service.get_connection")
@patch(
    "backend.services.distribuidora.sync_missing_related_documents_service.fetch_orphan_related_document_candidates"
)
@patch(
    "backend.services.distribuidora.sync_missing_related_documents_service.document_header_exists"
)
@patch(
    "backend.services.distribuidora.sync_missing_related_documents_service.fetch_bsale_document_by_id"
)
def test_not_found_in_bsale(
    mock_fetch_bsale,
    mock_header_exists,
    mock_fetch_orphans,
    mock_get_conn,
    mock_token,
    mock_conn,
):
    conn, _cur = mock_conn
    mock_get_conn.return_value = conn
    mock_token.return_value = "tok"
    mock_fetch_orphans.return_value = [_orphan_row()]
    mock_header_exists.return_value = False
    mock_fetch_bsale.side_effect = MissingRelatedApiError("404", not_found=True)

    with patch(
        "backend.services.distribuidora.sync_missing_related_documents_service.BsaleClient"
    ):
        report = run_sync_missing_related_documents(dry_run=True)

    assert report.not_found == 1
    assert report.would_insert == 0
    assert report.samples[0]["not_found"] is True


@patch("backend.services.distribuidora.sync_missing_related_documents_service._bsale_token")
@patch("backend.services.distribuidora.sync_missing_related_documents_service.get_connection")
@patch(
    "backend.services.distribuidora.sync_missing_related_documents_service.fetch_orphan_related_document_candidates"
)
@patch(
    "backend.services.distribuidora.sync_missing_related_documents_service.document_header_exists"
)
@patch(
    "backend.services.distribuidora.sync_missing_related_documents_service.fetch_bsale_document_by_id"
)
def test_rate_limited_429(
    mock_fetch_bsale,
    mock_header_exists,
    mock_fetch_orphans,
    mock_get_conn,
    mock_token,
    mock_conn,
):
    conn, _cur = mock_conn
    mock_get_conn.return_value = conn
    mock_token.return_value = "tok"
    mock_fetch_orphans.return_value = [_orphan_row()]
    mock_header_exists.return_value = False
    mock_fetch_bsale.side_effect = MissingRelatedApiError(
        "Bsale HTTP 429: rate limit",
        rate_limited=True,
    )

    with patch(
        "backend.services.distribuidora.sync_missing_related_documents_service.BsaleClient"
    ):
        report = run_sync_missing_related_documents(dry_run=True)

    assert report.rate_limited == 1
    assert report.api_errors == 1
    assert report.samples[0]["rate_limited"] is True


@patch("backend.services.distribuidora.sync_missing_related_documents_service._bsale_token")
@patch("backend.services.distribuidora.sync_missing_related_documents_service.get_connection")
@patch(
    "backend.services.distribuidora.sync_missing_related_documents_service.fetch_orphan_related_document_candidates"
)
@patch(
    "backend.services.distribuidora.sync_missing_related_documents_service.document_header_exists"
)
@patch(
    "backend.services.distribuidora.sync_missing_related_documents_service.fetch_bsale_document_by_id"
)
def test_idempotency_already_present(
    mock_fetch_bsale,
    mock_header_exists,
    mock_fetch_orphans,
    mock_get_conn,
    mock_token,
    mock_conn,
):
    conn, _cur = mock_conn
    mock_get_conn.return_value = conn
    mock_token.return_value = "tok"
    mock_fetch_orphans.return_value = [_orphan_row()]
    mock_header_exists.return_value = True

    with patch(
        "backend.services.distribuidora.sync_missing_related_documents_service.BsaleClient"
    ):
        report = run_sync_missing_related_documents(dry_run=True)

    assert report.already_present == 1
    mock_fetch_bsale.assert_not_called()
    assert report.found_in_bsale == 0
    assert report.samples[0]["already_present"] is True


@patch("backend.services.distribuidora.sync_missing_related_documents_service._bsale_token")
@patch("backend.services.distribuidora.sync_missing_related_documents_service.get_connection")
@patch(
    "backend.services.distribuidora.sync_missing_related_documents_service.fetch_orphan_related_document_candidates"
)
@patch(
    "backend.services.distribuidora.sync_missing_related_documents_service.document_header_exists"
)
@patch(
    "backend.services.distribuidora.sync_missing_related_documents_service.fetch_bsale_document_by_id"
)
@patch(
    "backend.services.distribuidora.sync_missing_related_documents_service._persist_document_from_bsale"
)
@patch(
    "backend.services.distribuidora.sync_missing_related_documents_service.discover_nc_bsale_ids_for_invoice"
)
@patch(
    "backend.services.distribuidora.sync_missing_related_documents_service._materialize_cn_for_invoice"
)
def test_apply_persists_header(
    mock_cn,
    mock_discover_nc,
    mock_persist,
    mock_fetch_bsale,
    mock_header_exists,
    mock_fetch_orphans,
    mock_get_conn,
    mock_token,
    mock_conn,
):
    conn, _cur = mock_conn
    mock_get_conn.return_value = conn
    mock_token.return_value = "tok"
    mock_fetch_orphans.return_value = [_orphan_row()]
    mock_header_exists.return_value = False
    mock_fetch_bsale.return_value = _bsale_doc(3853417)
    mock_persist.return_value = 3853417
    mock_discover_nc.return_value = []
    mock_cn.return_value = (0, 0)

    with patch(
        "backend.services.distribuidora.sync_missing_related_documents_service.BsaleClient"
    ):
        report = run_sync_missing_related_documents(dry_run=False)

    mock_persist.assert_called_once()
    assert report.headers_inserted == 1


def test_job_apply_requires_double_confirmation():
    rc = job_main(["--apply"])
    assert rc == 2


def test_job_apply_with_confirmation_calls_service():
    with patch(
        "backend.jobs.sync_missing_related_documents.run_sync_missing_related_documents"
    ) as mock_run:
        mock_run.return_value = MagicMock(
            dry_run=False,
            candidates=0,
            found_in_bsale=0,
            would_insert=0,
            already_present=0,
            not_found=0,
            api_errors=0,
            rate_limited=0,
            headers_inserted=0,
            cn_links_would_materialize=0,
            cn_links_inserted=0,
            derived_nc_candidates=[],
            samples=[],
            errors=[],
            rate_stats={},
        )
        rc = job_main(["--apply", "--i-understand-writes"])
    assert rc == 0
    mock_run.assert_called_once()
    assert mock_run.call_args.kwargs["dry_run"] is False


def test_orphan_related_oc_stays_closed_without_header():
    """Regresión: header ausente no reabre OC si related tipo 1/6 existe."""
    st = resolve_oc_operational_status_from_parts(
        {"document_id": 3852324, "number": 68677, "state": 0},
        [
            {
                "from_document_id": 3852324,
                "to_document_id": 3853417,
                "to_number": None,
                "to_document_type_id": 6,
                "to_total_amount": None,
                "to_raw_data": {},
                "to_emission_date": None,
            }
        ],
    )
    assert st.billing_status == BILLING_INVOICED
    assert st.dispatch_closed is True
    assert st.planning_eligible is False
    assert is_predespacho_pending_row(
        billing_status=st.billing_status,
        planning_eligible=st.planning_eligible,
        dispatch_closed=st.dispatch_closed,
    ) is False


def test_full_chain_after_sync_closes_with_nc():
    """Cadena esperada OC→Factura→NC total cuando headers existen."""
    st = resolve_oc_operational_status_from_parts(
        {"document_id": 3852324, "number": 68677, "state": 0},
        [
            {
                "from_document_id": 3852324,
                "to_document_id": 3853417,
                "to_number": 50367,
                "to_document_type_id": 6,
                "to_total_amount": 310350,
                "to_raw_data": {},
                "to_emission_date": datetime(2026, 8, 6, tzinfo=timezone.utc),
            }
        ],
        credit_notes_by_invoice={
            3853417: [
                {
                    "document_id": 99918408,
                    "number": 18408,
                    "total_amount": 310350,
                    "raw_data": {},
                }
            ]
        },
    )
    assert st.billing_status == BILLING_INVOICED_FULL_CN
    assert st.dispatch_closed is True
    assert st.planning_eligible is False


@patch("backend.services.distribuidora.sync_missing_related_documents_service._bsale_token")
def test_no_token_returns_error(mock_token):
    mock_token.return_value = ""
    report = run_sync_missing_related_documents(dry_run=True)
    assert report.errors
    assert report.candidates == 0
