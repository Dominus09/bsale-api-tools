"""Tests unitarios del CLI diagnose_oc (sin red ni BD)."""

from __future__ import annotations

import pytest

from backend.jobs.diagnose_oc_bsale_vs_pg import (
    _assert_bsale_folio,
    _build_verdict,
    _detail_diffs,
    _get_raw_field,
    _header_diffs,
)


def test_missing_field_is_null_with_warning():
    warnings: list[str] = []
    val = _get_raw_field({"totalAmount": 10}, "number", warnings=warnings, context="t")
    assert val is None
    assert any("number" in w for w in warnings)


def test_present_zero_is_not_invented():
    warnings: list[str] = []
    val = _get_raw_field({"number": 0}, "number", warnings=warnings, context="t")
    assert val == 0


def test_assert_folio_aborts_on_number_zero():
    with pytest.raises(SystemExit, match="ABORT: folio Bsale inválido"):
        _assert_bsale_folio(
            bsale_document={"id": 3832233, "number": 0, "state": 8888, "totalAmount": 10990},
            document_id=3832233,
            expected_folio=68199,
            warnings=[],
        )


def test_assert_folio_ok():
    _assert_bsale_folio(
        bsale_document={"id": 3832233, "number": 68199, "state": 0, "totalAmount": 219800},
        document_id=3832233,
        expected_folio=68199,
        warnings=[],
        client=None,
        office_id=None,
    )


def test_verdict_not_stale_details_when_lines_match():
    header = [
        {"campo": "total_amount", "coincide": True},
        {"campo": "number", "coincide": True},
    ]
    details = {
        "lines_match": True,
        "field_mismatches": [],
        "only_in_bsale_detail_ids": [],
        "only_in_pg_detail_ids": [],
    }
    assert _build_verdict(header_diff=header, details_diff=details).startswith("FRESCO")


def test_verdict_stale_details_only_on_real_line_diff():
    header = [{"campo": "total_amount", "coincide": True}]
    details = {
        "lines_match": False,
        "field_mismatches": [
            {
                "detail_id": 1,
                "campo": "quantity",
                "bsale_actual": 20,
                "postgresql": 1,
                "coincide": False,
            }
        ],
        "only_in_bsale_detail_ids": [],
        "only_in_pg_detail_ids": [],
    }
    v = _build_verdict(header_diff=header, details_diff=details)
    assert v.startswith("STALE_DETAILS")


def test_detail_diffs_lines_match_despite_float_int():
    bsale = [
        {
            "id": 1,
            "quantity": 20.0,
            "totalAmount": 219800,
            "netAmount": 184706,
            "totalUnitValue": 10990,
            "netUnitValue": 9235,
            "totalDiscount": 0,
            "discountPercentage": 0.0,
            "variant": {"id": 27383},
        }
    ]
    pg = [
        {
            "detail_id": 1,
            "quantity": 20,
            "total_amount": 219800.0,
            "net_amount": 184706,
            "total_unit_value": 10990,
            "net_unit_value": 9235,
            "total_discount": 0,
            "discount_percentage": 0,
            "variant_id": 27383,
        }
    ]
    d = _detail_diffs(bsale, pg)
    assert d["lines_match"] is True
    assert d["field_mismatches"] == []
    assert _build_verdict(
        header_diff=[{"campo": "x", "coincide": True}],
        details_diff=d,
    ).startswith("FRESCO")


def test_header_warns_on_sentinel_state():
    warnings: list[str] = []
    diffs = _header_diffs(
        {"number": 0, "state": 8888, "totalAmount": 10990, "netAmount": 1, "taxAmount": 1},
        {
            "number": 68199,
            "state": 0,
            "total_amount": 219800,
            "net_amount": 1,
            "tax_amount": 1,
            "raw_data": {},
        },
        warnings=warnings,
    )
    assert any(r["campo"] == "number" and not r["coincide"] for r in diffs)
    assert any("8888" in w for w in warnings)
