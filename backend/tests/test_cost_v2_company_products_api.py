"""Tests E.7.3 — Costos V2 consolidado por empresa."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

import pytest

from backend.repositories.cost_v2_company_read_repo import CostV2CompanyReadRepository
from backend.schemas.cost_v2_company_read import (
    COST_CONTROL_OFFICE_IDS_BY_COMPANY,
    CostV2ReadValidationError,
    VISUAL_NO_CHANGE_ABS,
    coverage_label,
    decode_company_product_cursor,
    derive_business_statuses,
    derive_office_alignment_status,
    encode_company_product_cursor,
    validate_company_id_for_v2_company,
    validate_company_product_sort,
)
from backend.services.analytics.money import D
from backend.services.cost_v2_company_read_service import map_company_product_item


def test_company_id_whitelist():
    assert validate_company_id_for_v2_company(3) == 3
    with pytest.raises(CostV2ReadValidationError):
        validate_company_id_for_v2_company(99)


def test_coverage_and_alignment_semantics():
    assert coverage_label(with_v2=1, active=4) == "1 de 4 oficinas"
    assert derive_office_alignment_status(
        offices_with_current_cost=1, has_office_difference=False
    ) == "insufficient_coverage"
    assert derive_office_alignment_status(
        offices_with_current_cost=2, has_office_difference=False
    ) == "offices_aligned"
    assert derive_office_alignment_status(
        offices_with_current_cost=2, has_office_difference=True
    ) == "office_difference"
    statuses = derive_business_statuses(
        requires_review=True,
        has_office_difference=False,
        offices_with_v2_data=1,
        active_offices_count=4,
        offices_with_current_cost=1,
    )
    assert "requires_review" in statuses
    assert "insufficient_coverage" in statuses
    assert "partial_coverage" in statuses
    assert "offices_aligned" not in statuses


def test_company_cursor_roundtrip():
    tok = encode_company_product_cursor(
        sort="latest_reception",
        variant_id=42,
        admission_date=date(2026, 7, 31),
    )
    cur = decode_company_product_cursor(tok)
    assert cur["variant_id"] == 42
    assert cur["sort"] == "latest_reception"


def test_sort_validation():
    assert validate_company_product_sort(None) == "latest_reception"
    assert validate_company_product_sort("abs_change") == "abs_change"
    assert validate_company_product_sort("absolute_change") == "abs_change"
    assert validate_company_product_sort("product_name") == "product"
    with pytest.raises(CostV2ReadValidationError):
        validate_company_product_sort("impacto")
    with pytest.raises(CostV2ReadValidationError):
        validate_company_product_sort("DROP TABLE;--")


def test_map_company_product_distinct_change_fields():
    row = {
        "variant_id": 10,
        "company_id": 3,
        "barcode": "5000267013602",
        "product_name": "JOHNNIE WALKER",
        "variant_name": "Red Label",
        "current_history_id": 501,
        "current_cost": D("9798"),
        "current_cost_raw": D("9798"),
        "current_admission_date": date(2026, 7, 31),
        "current_office_id": 3,
        "current_office_name": "SUPERMERCADO LA QUILLOTANA",
        "current_document_number": 18358,
        "current_quality_status": "missing_taxes_in_gross",
        "current_warnings_json": [],
        "previous_distinct_history_id": 400,
        "previous_distinct_cost": D("9998"),
        "change_amount": D("-200"),
        "change_percent": D("-2.0"),
        "last_change_date": date(2026, 7, 16),
        "has_comparable_cost": True,
        "active_offices_count": 4,
        "offices_with_v2_data": 1,
        "offices_with_current_cost": 1,
        "has_office_difference": False,
        "requires_review": False,
        "last_reception_date": date(2026, 7, 31),
        "receptions_in_period": 3,
        "last_calculated_at": None,
        "latest_history_id": 501,
    }
    item = map_company_product_item(row)
    assert item["variant_id"] == 10
    assert item["current_cost"] == "9798"
    assert item["previous_distinct_cost"] == "9998"
    assert item["change_amount"] == "-200"
    assert item["coverage_label"] == "1 de 4 oficinas"
    assert item["office_alignment_status"] == "insufficient_coverage"
    assert item["has_office_difference"] is False
    assert "partial_coverage" in item["business_statuses"]
    # Impuestos no incluidos con costo calculable → no revisión automática
    assert item["requires_review"] is False


def test_visual_no_change_threshold():
    row = {
        "variant_id": 1,
        "current_cost": D("100"),
        "current_cost_raw": D("100"),
        "change_amount": D("0.3"),
        "change_percent": D("0.3"),
        "has_comparable_cost": True,
        "active_offices_count": 4,
        "offices_with_v2_data": 1,
        "offices_with_current_cost": 1,
        "has_office_difference": False,
        "requires_review": False,
        "current_warnings_json": [],
        "receptions_in_period": 0,
    }
    item = map_company_product_item(row)
    assert abs(D("0.3")) < VISUAL_NO_CHANGE_ABS
    assert item["visual_no_change"] is True


def test_outlier_requires_review_flag_from_sql_field():
    row = {
        "variant_id": 2,
        "current_cost": D("500"),
        "current_cost_raw": D("500"),
        "current_quality_status": "valid_gross",
        "current_warnings_json": ["suspicious_outlier"],
        "has_comparable_cost": False,
        "active_offices_count": 4,
        "offices_with_v2_data": 1,
        "offices_with_current_cost": 1,
        "has_office_difference": False,
        "requires_review": True,
        "receptions_in_period": 1,
    }
    item = map_company_product_item(row)
    assert "suspicious_outlier" in item["current_warnings"]
    assert item["requires_review"] is True


def test_list_company_products_sql_no_offset_and_distinct_prev():
    sqls: list[str] = []

    def executor(sql: str, params: tuple) -> list[dict]:
        sqls.append(sql)
        if "FROM bsale.offices" in sql:
            return [
                {"office_id": oid, "office_name": name}
                for oid, name in [
                    (1, "BODEGA CENTRAL"),
                    (3, "SUPERMERCADO LA QUILLOTANA"),
                    (4, "QUILLOTANA I"),
                    (5, "QUILLOTANA II"),
                ]
            ]
        return []

    repo = CostV2CompanyReadRepository(executor)
    repo.list_company_products(
        company_id=3,
        date_from=date(2026, 5, 1),
        date_to=date(2026, 7, 31),
        limit=50,
        sort="latest_reception",
    )
    joined = "\n".join(sqls)
    assert "OFFSET" not in joined.upper().replace("DATE_TO_EXCLUSIVE", "")
    assert "OFFSET" not in joined
    assert "IS DISTINCT FROM" in joined
    assert "corrected_gross_cost" in joined
    assert "variant_cost" not in joined.lower()
    assert "quantity" not in joined.lower() or "stored_quantity" not in joined
    # No office_id obligatorio en scope empresa (solo unnest de activas)
    assert "calc_ranked" in joined or "rn_calc" in joined


def test_control_offices_company_3():
    assert COST_CONTROL_OFFICE_IDS_BY_COMPANY[3] == (1, 3, 4, 5)


def test_summarize_sql_has_decision_kpis():
    sqls: list[str] = []

    def executor(sql: str, params: tuple) -> list[dict]:
        sqls.append(sql)
        if "FROM bsale.offices" in sql:
            return [
                {"office_id": 1, "office_name": "BODEGA CENTRAL"},
                {"office_id": 3, "office_name": "SUPERMERCADO"},
                {"office_id": 4, "office_name": "Q1"},
                {"office_id": 5, "office_name": "Q2"},
            ]
        if "relevant_changes" in sql:
            return [
                {
                    "total_products": 10,
                    "products_with_current_cost": 8,
                    "products_without_current_cost": 2,
                    "relevant_changes": 3,
                    "products_requiring_review": 4,
                    "products_with_outlier": 1,
                    "products_with_office_difference": 0,
                    "active_offices_count": 4,
                    "offices_with_v2_coverage": 1,
                    "latest_reception_date": date(2026, 7, 31),
                    "latest_sync_or_calculation_at": None,
                }
            ]
        return []

    repo = CostV2CompanyReadRepository(executor)
    s = repo.summarize_company_products(
        company_id=3,
        date_from=date(2026, 5, 1),
        date_to=date(2026, 7, 31),
        change_threshold_percent=D("10"),
    )
    assert s["relevant_changes"] == 3
    assert s["offices_with_v2_coverage"] == 1
    assert "AVG(" not in "\n".join(sqls).upper()
    assert "SUM(c.corrected" not in "\n".join(sqls).lower()


def test_history_sql_ordered_chronologically():
    sqls: list[str] = []

    def executor(sql: str, params: tuple) -> list[dict]:
        sqls.append(sql)
        return []

    repo = CostV2CompanyReadRepository(executor)
    repo.list_company_product_history(
        company_id=3,
        variant_id=10,
        date_from=date(2026, 1, 1),
        date_to=date(2026, 7, 31),
        office_id=3,
        limit=50,
    )
    joined = "\n".join(sqls).upper()
    assert "ORDER BY H.ADMISSION_DATE ASC" in joined
    assert "OFFSET" not in joined
