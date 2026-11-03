"""Auditoría read-only de correlativos OC (tipo 33)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from backend.services.distribuidora import oc_correlative_audit_service as audit


def _oc(
    source_id: int,
    *,
    number: int,
    state: int = 0,
    office_id: int = 1,
    company_id: int = 3,
    doc_type: int = 33,
    generation: int = 1_700_000_000,
    total: int = 1000,
) -> dict:
    return {
        "id": source_id,
        "number": number,
        "state": state,
        "commercialState": 0,
        "emissionDate": generation - 100,
        "generationDate": generation,
        "totalAmount": total,
        "office": {"id": office_id},
        "document_type": {"id": doc_type},
        "company": {"id": company_id},
    }


def _local(
    *folios: int,
    company_id: int = 3,
    office_id: int = 1,
    source_map: dict[int, int] | None = None,
) -> dict:
    by_folio = {}
    for folio in folios:
        source = (source_map or {}).get(folio, folio)
        by_folio[folio] = {
            "folio": folio,
            "document_id": folio,
            "state": 0,
            "source_document_id": source,
            "company_id": company_id,
            "office_id": office_id,
            "document_type_id": 33,
        }
    return {"by_folio": by_folio, "zero_number_rows": [], "row_count": len(by_folio)}


def test_consecutive_folios_have_no_gaps():
    gaps = audit.find_numeric_gaps({10, 11, 12}, from_folio=10, to_folio=12)
    assert gaps["gaps"] == []
    assert gaps["gap_count"] == 0
    assert gaps["present_count"] == 3


def test_simple_and_multiple_gaps():
    gaps = audit.find_numeric_gaps({100, 103, 104}, from_folio=100, to_folio=105)
    assert gaps["gaps"] == [101, 102, 105]
    assert gaps["gap_count"] == 3


def test_gap_cap_truncates_large_hole():
    gaps = audit.find_numeric_gaps(
        {1},
        from_folio=1,
        to_folio=20,
        max_gaps=3,
        max_range_span=50,
    )
    assert gaps["gaps"] == [2, 3, 4]
    assert gaps["truncated"] is True


def test_range_too_large_is_rejected():
    with pytest.raises(audit.CorrelativeRangeError):
        audit.validate_correlative_range(
            from_folio=1,
            to_folio=10_000,
            max_range_span=500,
        )


def test_audit_separates_company_sequences():
    def loader(**kwargs):
        assert kwargs["company_id"] == 3
        return _local(10, 12, company_id=3)

    report = audit.audit_oc_correlatives(
        None,
        company_id=3,
        office_id=1,
        from_folio=10,
        to_folio=12,
        investigate=False,
        local_loader=loader,
    )
    assert report["wrote"] is False
    assert report["counts"].get("ok") == 2
    assert report["counts"].get("gap_pending") == 1
    assert report["findings"][1]["folio"] == 11 or any(
        f["folio"] == 11 and f["status"] == "gap_pending" for f in report["findings"]
    )


def test_audit_separates_office_sequences():
    seen = {}

    def loader(**kwargs):
        seen["office_id"] = kwargs["office_id"]
        if kwargs["office_id"] == 1:
            return _local(50, 51, office_id=1)
        return _local(50, office_id=2)

    r1 = audit.audit_oc_correlatives(
        None,
        company_id=3,
        office_id=1,
        from_folio=50,
        to_folio=52,
        investigate=False,
        local_loader=loader,
    )
    r2 = audit.audit_oc_correlatives(
        None,
        company_id=3,
        office_id=2,
        from_folio=50,
        to_folio=52,
        investigate=False,
        local_loader=loader,
    )
    assert r1["office_id"] == 1
    assert r2["office_id"] == 2
    assert r1["counts"].get("gap_pending") == 1  # 52
    assert r2["counts"].get("gap_pending") == 2  # 51, 52


def test_cancelled_folio_classified_from_bsale():
    cancelled = _oc(9, number=0, state=8888)

    def search(*_a, folio, **_k):
        assert folio == 70500
        return [cancelled]

    result = audit.investigate_folio_in_bsale(
        MagicMock(),
        folio=70500,
        search_fn=search,
    )
    assert result["status"] == audit.STATUS_CANCELLED
    assert result["wrote"] is False


def test_version_diff_when_source_id_differs():
    active = _oc(3958571, number=70511)

    def search(*_a, folio, **_k):
        assert folio == 70511
        return [active]

    result = audit.investigate_folio_in_bsale(
        MagicMock(),
        folio=70511,
        local_row={
            "document_id": 3958410,
            "source_document_id": 3958410,
        },
        search_fn=search,
    )
    assert result["status"] == audit.STATUS_VERSION_DIFF
    assert result["bsale_source_document_id"] == 3958571
    assert result["local_document_id"] == 3958410


def test_bsale_502_is_verification_error_not_not_found():
    def search(*_a, folio, **_k):
        raise RuntimeError("Bsale HTTP 502 en /documents.json tras 5 intentos")

    result = audit.investigate_folio_in_bsale(
        MagicMock(),
        folio=70001,
        search_fn=search,
    )
    assert result["status"] == audit.STATUS_VERIFICATION_ERROR
    assert "502" in result["error"]


def test_bsale_unrelated_documents_are_rejected():
    foreign = _oc(1, number=999, office_id=9, company_id=2, doc_type=1)

    def search(*_a, folio, **_k):
        return [foreign]

    result = audit.investigate_folio_in_bsale(
        MagicMock(),
        folio=70001,
        company_id=3,
        office_id=1,
        search_fn=search,
    )
    assert result["status"] == audit.STATUS_VERIFICATION_ERROR
    assert result["error"] == "bsale_returned_unrelated_documents"


def test_missing_in_pg_when_active_found():
    active = _oc(111, number=70005)

    def search(*_a, folio, **_k):
        return [active]

    result = audit.investigate_folio_in_bsale(
        MagicMock(),
        folio=70005,
        search_fn=search,
    )
    assert result["status"] == audit.STATUS_MISSING_IN_PG
    assert result["bsale_source_document_id"] == 111


def test_not_found_after_valid_empty_query():
    def search(*_a, folio, **_k):
        return []

    result = audit.investigate_folio_in_bsale(
        MagicMock(),
        folio=70006,
        search_fn=search,
    )
    assert result["status"] == audit.STATUS_NOT_FOUND_IN_BSALE


def test_audit_investigate_rewrites_gaps_and_never_writes():
    active = _oc(222, number=11)

    def loader(**_k):
        return _local(10, 12)

    def search(*_a, folio, **_k):
        if folio == 11:
            return [active]
        return []

    report = audit.audit_oc_correlatives(
        MagicMock(),
        company_id=3,
        office_id=1,
        from_folio=10,
        to_folio=12,
        investigate=True,
        max_investigate=10,
        local_loader=loader,
        search_fn=search,
    )
    assert report["wrote"] is False
    assert all(f.get("wrote") is False for f in report["findings"])
    statuses = {f["folio"]: f["status"] for f in report["findings"]}
    assert statuses[11] == audit.STATUS_MISSING_IN_PG
    assert report["counts"].get("missing_in_pg") == 1


def test_reject_non_oc_document_type():
    with pytest.raises(audit.CorrelativeRangeError):
        audit.audit_oc_correlatives(
            None,
            from_folio=1,
            to_folio=2,
            document_type_id=1,
            investigate=False,
            local_loader=lambda **_k: _local(),
        )


def test_synthetic_audit_output_example():
    """Ejemplo de salida del auditor (datos sintéticos)."""

    def loader(**_k):
        data = _local(100, 101, 104, source_map={101: 9001})
        return data

    def search(*_a, folio, **_k):
        if folio == 102:
            return [_oc(5002, number=102)]
        if folio == 103:
            return [_oc(5003, number=0, state=8888)]
        if folio == 101:
            return [_oc(9002, number=101)]  # version distinta al source local 9001
        if folio == 100:
            return [_oc(100, number=100)]
        if folio == 104:
            return [_oc(104, number=104)]
        return []

    report = audit.audit_oc_correlatives(
        MagicMock(),
        company_id=3,
        office_id=1,
        from_folio=100,
        to_folio=104,
        investigate=True,
        max_investigate=20,
        local_loader=loader,
        search_fn=search,
    )
    assert report["mode"] == "correlative_audit_readonly"
    assert report["wrote"] is False
    assert report["company_id"] == 3
    assert report["office_id"] == 1
    by_folio = {f["folio"]: f["status"] for f in report["findings"]}
    assert by_folio[100] == audit.STATUS_OK
    assert by_folio[101] == audit.STATUS_VERSION_DIFF
    assert by_folio[102] == audit.STATUS_MISSING_IN_PG
    assert by_folio[103] == audit.STATUS_CANCELLED
    assert by_folio[104] == audit.STATUS_OK
    assert report["recoverable_count"] == 1
    # Forma estable para revisión humana
    assert "bsale_filters_documented" in report
    assert report["bsale_filters_documented"]["params"] == [
        "number",
        "documenttypeid",
        "officeid",
        "limit",
        "offset",
    ]


def test_tail_beyond_local_max_marked_as_gap_pending():
    """Si to_folio > máximo PG, los huecos finales quedan gap_pending/beyond_local_max."""

    def loader(**_k):
        return _local(200, 201)

    report = audit.audit_oc_correlatives(
        None,
        company_id=3,
        office_id=1,
        from_folio=200,
        to_folio=205,
        investigate=False,
        local_loader=loader,
        bounds_loader=lambda **_k: {"min_folio": 200, "max_folio": 201, "positive_count": 2},
    )
    assert report["to_folio_beyond_local_max"] is True
    assert report["gaps_beyond_local_max"] >= 1
    beyond = [f for f in report["findings"] if f.get("beyond_local_max")]
    assert {f["folio"] for f in beyond} == {202, 203, 204, 205}
    assert all(f["status"] == audit.STATUS_GAP_PENDING for f in beyond)
    assert report["wrote"] is False


def test_suggest_recent_range_anchors_to_local_max():
    suggested = audit.suggest_recent_folio_range(
        company_id=3,
        office_id=1,
        window=10,
        bounds_loader=lambda **_k: {
            "min_folio": 100,
            "max_folio": 70627,
            "positive_count": 1000,
            "company_id": 3,
            "office_id": 1,
            "document_type_id": 33,
        },
    )
    assert suggested["to_folio"] == 70627
    assert suggested["from_folio"] == 70618


def test_investigate_selected_respects_batch_cap_and_429():
    calls = []

    def search(*_a, folio, **_k):
        calls.append(folio)
        if folio == 2:
            raise RuntimeError("Bsale HTTP 429 Too Many Requests")
        return [_oc(folio, number=folio)]

    report = audit.investigate_selected_folios(
        MagicMock(),
        folios=[1, 2, 3, 4, 5],
        max_folios=3,
        local_loader=lambda **_k: _local(),
        search_fn=search,
    )
    assert report["investigated"] == 3
    assert report["truncated"] is True
    assert report["wrote"] is False
    statuses = {f["folio"]: f["status"] for f in report["findings"]}
    assert statuses[1] == audit.STATUS_MISSING_IN_PG
    assert statuses[2] == audit.STATUS_VERIFICATION_ERROR
    assert "429" in report["findings"][1]["error"]
