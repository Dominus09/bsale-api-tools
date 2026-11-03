"""Comparación tip correlativo PG vs Bsale (sonda number=, sin inventar máximo)."""

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
    generation: int = 1_700_000_000,
) -> dict:
    return {
        "id": source_id,
        "number": number,
        "state": state,
        "commercialState": 0,
        "emissionDate": generation - 100,
        "generationDate": generation,
        "totalAmount": 1000,
        "office": {"id": office_id},
        "document_type": {"id": 33},
        "company": {"id": company_id},
    }


def test_bsale_ahead_lists_pending_not_as_lost():
    # PG max=100; Bsale tiene 101 y 102 activos; 103+ not found.
    def search(_client, *, folio, **_k):
        if folio in (100, 101, 102):
            return [_oc(9000 + folio, number=folio)]
        return []

    report = audit.compare_bsale_pg_folio_tip(
        MagicMock(),
        company_id=3,
        office_id=1,
        max_tail_probe=10,
        stop_after_not_found=3,
        bounds_loader=lambda **_k: {
            "min_folio": 1,
            "max_folio": 100,
            "positive_count": 100,
        },
        search_fn=search,
    )
    assert report["wrote"] is False
    assert report["verification_status"] == audit.COMPARE_BSALE_AHEAD
    assert report["pg_max_folio"] == 100
    assert report["bsale_max_folio"] == 102
    assert report["pending_investigate_folios"] == [101, 102]
    assert report["pending_investigate_count"] == 2
    # No clasifica automáticamente como perdidas.
    assert "lost" not in str(report.get("notes") or {}).lower()


def test_equal_when_tip_matches_pg_max():
    def search(_client, *, folio, **_k):
        if folio == 200:
            return [_oc(9200, number=200)]
        return []

    report = audit.compare_bsale_pg_folio_tip(
        MagicMock(),
        company_id=3,
        office_id=1,
        max_tail_probe=5,
        stop_after_not_found=3,
        bounds_loader=lambda **_k: {"min_folio": 100, "max_folio": 200},
        search_fn=search,
    )
    assert report["verification_status"] == audit.COMPARE_EQUAL
    assert report["bsale_max_folio"] == 200
    assert report["pending_investigate_count"] == 0


def test_unverified_when_anchor_http_error():
    def search(_client, *, folio, **_k):
        raise RuntimeError("Bsale HTTP 502 persistente")

    report = audit.compare_bsale_pg_folio_tip(
        MagicMock(),
        company_id=3,
        office_id=1,
        bounds_loader=lambda **_k: {"min_folio": 1, "max_folio": 50},
        search_fn=search,
    )
    assert report["verification_status"] == audit.COMPARE_UNVERIFIED
    assert report["bsale_max_folio"] is None
    assert report["wrote"] is False


def test_cancelled_anchor_without_active_tip_is_unverified():
    def search(_client, *, folio, **_k):
        if folio == 80:
            return [_oc(880, number=0, state=8888)]
        return []

    report = audit.compare_bsale_pg_folio_tip(
        MagicMock(),
        company_id=3,
        office_id=1,
        max_tail_probe=3,
        stop_after_not_found=2,
        bounds_loader=lambda **_k: {"min_folio": 1, "max_folio": 80},
        search_fn=search,
    )
    assert report["anchor_status"] == audit.STATUS_CANCELLED
    assert report["verification_status"] == audit.COMPARE_UNVERIFIED
    assert report["bsale_max_folio"] is None


def test_verification_error_in_tail_does_not_end_as_not_found():
    calls = {"n": 0}

    def search(_client, *, folio, **_k):
        calls["n"] += 1
        if folio == 10:
            return [_oc(10, number=10)]
        if folio == 11:
            raise RuntimeError("Bsale HTTP 429: rate limit")
        if folio == 12:
            return [_oc(12, number=12)]
        return []

    report = audit.compare_bsale_pg_folio_tip(
        MagicMock(),
        company_id=3,
        office_id=1,
        max_tail_probe=6,
        stop_after_not_found=3,
        bounds_loader=lambda **_k: {"min_folio": 1, "max_folio": 10},
        search_fn=search,
    )
    assert report["bsale_max_folio"] == 12
    assert report["verification_status"] in (
        audit.COMPARE_BSALE_AHEAD,
        audit.COMPARE_PARTIAL,
    )
    assert any(
        f.get("status") == audit.STATUS_VERIFICATION_ERROR
        for f in report["probe_findings"]
    )


def test_no_local_folio_does_not_invent_bsale_max():
    report = audit.compare_bsale_pg_folio_tip(
        MagicMock(),
        company_id=3,
        office_id=1,
        bounds_loader=lambda **_k: {"min_folio": None, "max_folio": None},
        search_fn=lambda *_a, **_k: (_ for _ in ()).throw(AssertionError("no search")),
    )
    assert report["verification_status"] == audit.COMPARE_NO_LOCAL
    assert report["bsale_max_folio"] is None


def test_rejects_non_oc_document_type():
    with pytest.raises(audit.CorrelativeRangeError):
        audit.compare_bsale_pg_folio_tip(
            MagicMock(),
            document_type_id=1,
            bounds_loader=lambda **_k: {"max_folio": 1},
            search_fn=lambda *_a, **_k: [],
        )
