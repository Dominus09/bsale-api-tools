"""Tests E.7.1 — API productos V2 (última/penúltima recepción)."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

import pytest

from backend.schemas.cost_v2_read import (
    unit_change_amount,
    unit_change_percent,
    validate_change_threshold,
    validate_product_sort,
    CostV2ReadValidationError,
    encode_product_cursor,
    decode_product_cursor,
)
from backend.services.analytics.money import D
from backend.services.cost_v2_read_service import (
    _map_product_item,
    list_v2_products,
    summarize_v2_products,
)
from backend.repositories.cost_v2_read_repo import CostV2ReadRepository


def test_unit_change_decimal_null_safe():
    assert unit_change_amount(D("100"), D("80")) == D("20")
    assert unit_change_percent(D("100"), D("80")) == D("25")
    assert unit_change_amount(None, D("80")) is None
    assert unit_change_percent(D("100"), None) is None
    assert unit_change_percent(D("100"), D("0")) is None


def test_threshold_and_sort_validation():
    assert validate_change_threshold(None) == D("10")
    assert validate_product_sort(None) == "latest_reception"
    with pytest.raises(CostV2ReadValidationError):
        validate_product_sort("impacto")
    with pytest.raises(CostV2ReadValidationError):
        validate_change_threshold(-1)


def test_product_cursor_roundtrip():
    tok = encode_product_cursor(
        sort="latest_reception",
        variant_id=42,
        admission_date=date(2026, 7, 1),
    )
    cur = decode_product_cursor(tok)
    assert cur["variant_id"] == 42
    assert cur["sort"] == "latest_reception"
    assert cur["admission_date"] == date(2026, 7, 1)


def test_map_product_one_row_per_variant_fields():
    row = {
        "variant_id": 10,
        "company_id": 3,
        "office_id": 3,
        "barcode": "7803473005960",
        "product_name": "Mankeke",
        "variant_name": "Unidad",
        "latest_history_id": 23190,
        "latest_admission_date": date(2026, 6, 20),
        "latest_document_number": 55,
        "current_stored_cost_net": D("650"),
        "current_corrected_gross_cost": D("773.50"),
        "current_quality_status": "missing_taxes_in_gross",
        "current_warnings_json": ["suspicious_outlier"],
        "previous_history_id": 20000,
        "previous_admission_date": date(2026, 5, 1),
        "previous_corrected_gross_cost": D("700"),
        "unit_change_amount": D("73.50"),
        "unit_change_percent": D("10.5"),
        "receptions_count": 3,
        "last_calculated_at": None,
    }
    item = _map_product_item(row)
    assert item["variant_id"] == 10
    assert item["latest_history_id"] == 23190
    assert item["previous_corrected_gross_cost"] == "700"
    assert item["unit_change_percent"] == "10.5"
    assert "suspicious_outlier" in item["current_warnings"]
    assert item["needs_review"] is False


def test_list_products_sql_uses_row_number_not_offset():
    sqls: list[str] = []

    def executor(sql: str, params: tuple) -> list[dict]:
        sqls.append(sql)
        return []

    repo = CostV2ReadRepository(executor)
    repo.list_products(
        company_id=3,
        office_id=3,
        date_from=date(2026, 6, 1),
        date_to=date(2026, 7, 31),
        limit=10,
        sort="latest_reception",
    )
    joined = "\n".join(sqls).upper()
    assert "ROW_NUMBER()" in joined
    assert "PARTITION BY" in joined
    assert "OFFSET" not in joined
    assert "UNIT_CHANGE_PERCENT" in joined or "unit_change_percent" in "\n".join(sqls)


def test_summarize_products_has_decision_kpis():
    def executor(sql: str, params: tuple) -> list[dict]:
        if "total_products" in sql:
            return [
                {
                    "total_products": 100,
                    "products_with_current_cost": 90,
                    "products_without_calculable_cost": 10,
                    "products_incomplete_tax_context": 4,
                    "products_with_outlier": 2,
                    "products_with_increase": 20,
                    "products_with_decrease": 15,
                    "products_with_change_over_threshold": 8,
                    "products_needing_review": 12,
                    "products_missing_cost": 6,
                    "products_rounding_warning": 1,
                    "latest_reception_date": date(2026, 7, 31),
                    "latest_calculation_at": None,
                }
            ]
        return []

    repo = CostV2ReadRepository(executor)
    s = repo.summarize_products(
        company_id=3,
        office_id=3,
        date_from=date(2026, 6, 1),
        date_to=date(2026, 7, 31),
        change_threshold_percent=D("10"),
    )
    assert s["total_products"] == 100
    assert s["products_with_change_over_threshold"] == 8
    assert s["products_needing_review"] == 12


def test_service_list_products_wires_repo(monkeypatch):
    captured: dict[str, Any] = {}

    class FakeRepo:
        def list_products(self, **kwargs):
            captured.update(kwargs)
            return [
                {
                    "variant_id": 1,
                    "company_id": 3,
                    "office_id": 3,
                    "barcode": "x",
                    "product_name": "A",
                    "variant_name": "V",
                    "latest_history_id": 9,
                    "latest_admission_date": date(2026, 7, 1),
                    "latest_document_number": 1,
                    "current_stored_cost_net": D("10"),
                    "current_corrected_gross_cost": D("12"),
                    "current_quality_status": "valid_gross",
                    "current_warnings_json": [],
                    "previous_history_id": None,
                    "previous_admission_date": None,
                    "previous_corrected_gross_cost": None,
                    "unit_change_amount": None,
                    "unit_change_percent": None,
                    "receptions_count": 1,
                    "last_calculated_at": None,
                }
            ]

    def fake_with(fn, **_kw):
        return fn(FakeRepo())

    monkeypatch.setattr(
        "backend.services.cost_v2_read_service._with_repo",
        fake_with,
    )
    out = list_v2_products(
        company_id=3,
        office_id=3,
        date_from="2026-06-01",
        date_to="2026-07-31",
        only_needs_review=True,
        limit=20,
    )
    assert len(out["items"]) == 1
    assert out["items"][0]["unit_change_amount"] is None
    assert captured["only_needs_review"] is True
    assert out["page"]["has_more"] is False
