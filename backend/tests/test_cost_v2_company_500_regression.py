"""Regresión urgente: company-summary / company-products no deben 500.

Causa raíz histórica: `_with_repo` llamaba `open_readonly_connection()` sin
`get_connection` y `make_psycopg_executor()` sin `statement_timeout_seconds`.
"""

from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from pydantic import BaseModel, ConfigDict, Field

from backend.repositories.cost_v2_company_read_repo import CostV2CompanyReadRepository
from backend.schemas.cost_v2_company_read import (
    BUSINESS_STATUS_INSUFFICIENT_COVERAGE,
    validate_company_product_sort,
)
from backend.services.analytics.money import D
from backend.services import cost_v2_company_read_service as svc
from backend.services.cost_v2_company_read_service import map_company_product_item


class CompanyProductItemSchema(BaseModel):
    """Schema mínimo de respuesta para validar serialización Decimal/null/JSONB."""

    model_config = ConfigDict(extra="allow")

    variant_id: int
    current_cost: str | None = None
    previous_distinct_cost: str | None = None
    change_amount: str | None = None
    change_percent: str | None = None
    current_warnings: list[str] = Field(default_factory=list)
    current_office_name: str | None = None
    offices_with_v2_data: int
    has_office_difference: bool | None = None
    office_alignment_status: str
    business_statuses: list[str]


class CompanySummarySchema(BaseModel):
    model_config = ConfigDict(extra="allow")

    total_products: int
    products_with_office_difference: int | None = None
    offices_with_v2_coverage: int
    office_difference_comparable: bool


def _partial_coverage_row(**overrides: Any) -> dict[str, Any]:
    base = {
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
        "current_quality_status": "valid_gross",
        "current_warnings_json": ["suspicious_outlier"],
        "previous_distinct_history_id": None,
        "previous_distinct_cost": None,
        "change_amount": None,
        "change_percent": None,
        "last_change_date": None,
        "has_comparable_cost": False,
        "active_offices_count": 4,
        "offices_with_v2_data": 1,
        "offices_with_current_cost": 1,
        "has_office_difference": False,
        "requires_review": False,
        "last_reception_date": date(2026, 7, 31),
        "receptions_in_period": 1,
        "last_calculated_at": None,
        "latest_history_id": 501,
    }
    base.update(overrides)
    return base


def test_with_repo_passes_get_connection_and_timeout():
    """Regresión del TypeError que producía HTTP 500 en producción."""
    fake_conn = MagicMock()
    fake_repo_result = {"ok": True}

    with (
        patch.object(svc, "open_readonly_connection") as open_ro,
        patch.object(svc, "make_psycopg_executor") as make_ex,
        patch.object(svc, "CostV2CompanyReadRepository") as Repo,
    ):
        open_ro.return_value = fake_conn
        make_ex.return_value = lambda sql, params: []
        Repo.return_value = MagicMock()

        def _fn(repo):
            return fake_repo_result

        out = svc._with_repo(_fn)
        assert out == fake_repo_result
        open_ro.assert_called_once()
        args, kwargs = open_ro.call_args
        assert args or kwargs  # recibe get_connection posicional o kw
        # Primer arg posicional = get_connection callable
        assert callable(args[0] if args else kwargs.get("get_connection"))
        make_ex.assert_called_once()
        _, mex_kwargs = make_ex.call_args
        assert mex_kwargs.get("statement_timeout_seconds") == 20
        assert mex_kwargs.get("lock_timeout") == "3s"
        fake_conn.rollback.assert_called()
        fake_conn.close.assert_called()


def test_with_repo_broken_signature_reproduces_original_typeerror():
    """Documenta el fallo original si se omite get_connection."""
    from backend.services.analytics.validate_distribuidora_source import (
        open_readonly_connection,
    )

    with pytest.raises(TypeError, match="get_connection"):
        open_readonly_connection()  # type: ignore[call-arg]


def test_company_summary_single_office_coverage_no_raise():
    def _run(repo):
        return {
            "summary": {
                "total_products": 5,
                "products_with_current_cost": 5,
                "products_without_current_cost": 0,
                "relevant_changes": 1,
                "products_requiring_review": 0,
                "products_with_outlier": 0,
                "products_with_office_difference": None,
                "office_difference_comparable": False,
                "active_offices_count": 4,
                "offices_with_v2_coverage": 1,
                "coverage_label": "1 de 4 oficinas",
                "latest_reception_date": "2026-07-31",
                "latest_sync_or_calculation_at": None,
            },
            "meta": svc._meta(),
        }

    with patch.object(svc, "_with_repo", side_effect=lambda fn: _run(None)):
        out = svc.summarize_company_products(
            company_id=3,
            date_from=date(2026, 5, 5),
            date_to=date(2026, 8, 3),
            change_threshold_percent=10,
        )
    assert out["summary"]["offices_with_v2_coverage"] == 1
    assert out["summary"]["office_difference_comparable"] is False
    assert out["summary"]["products_with_office_difference"] is None
    CompanySummarySchema.model_validate(out["summary"])


def test_company_products_single_office_mapped():
    item = map_company_product_item(_partial_coverage_row())
    assert item["offices_with_v2_data"] == 1
    assert item["office_alignment_status"] == BUSINESS_STATUS_INSUFFICIENT_COVERAGE
    assert item["has_office_difference"] is False
    assert "partial_coverage" in item["business_statuses"]
    CompanyProductItemSchema.model_validate(item)
    # Serializa a JSON (Decimal ya convertido a str)
    json.dumps(item)


def test_date_to_after_last_reception_still_maps():
    item = map_company_product_item(
        _partial_coverage_row(
            last_reception_date=date(2026, 7, 31),
            current_admission_date=date(2026, 7, 31),
        )
    )
    assert item["last_reception_date"] == "2026-07-31"
    CompanyProductItemSchema.model_validate(item)


def test_product_without_previous_distinct_cost():
    item = map_company_product_item(_partial_coverage_row())
    assert item["previous_distinct_cost"] is None
    assert item["change_amount"] is None
    assert item["change_percent"] is None
    assert item["has_comparable_cost"] is False


def test_change_percent_zero_denominator_row():
    """Si previous=0, SQL deja change_percent NULL; mapper tolera null."""
    item = map_company_product_item(
        _partial_coverage_row(
            previous_distinct_history_id=99,
            previous_distinct_cost=D("0"),
            change_amount=D("100"),
            change_percent=None,
            has_comparable_cost=True,
        )
    )
    assert item["change_percent"] is None
    assert item["previous_distinct_cost"] == "0"


def test_offices_with_v2_data_one_no_office_diff():
    item = map_company_product_item(
        _partial_coverage_row(
            offices_with_v2_data=1,
            offices_with_current_cost=1,
            has_office_difference=True,  # SQL espurio; mapper lo anula
        )
    )
    assert item["has_office_difference"] is False
    assert item["office_alignment_status"] == "insufficient_coverage"


def test_sort_closed_list_aliases():
    assert validate_company_product_sort("latest_reception") == "latest_reception"
    assert validate_company_product_sort("pct_increase") == "pct_increase"
    assert validate_company_product_sort("absolute_change") == "abs_change"
    assert validate_company_product_sort("product_name") == "product"


def test_list_sql_sort_latest_and_pct_and_filters():
    sqls: list[str] = []

    def executor(sql: str, params: tuple) -> list[dict]:
        sqls.append(sql)
        if "FROM bsale.offices" in sql:
            return [
                {"office_id": 3, "office_name": "SUPERMERCADO LA QUILLOTANA"},
            ]
        return []

    repo = CostV2CompanyReadRepository(executor)
    for sort in ("latest_reception", "pct_increase"):
        sqls.clear()
        repo.list_company_products(
            company_id=3,
            date_from=date(2026, 5, 5),
            date_to=date(2026, 8, 3),
            limit=50,
            sort=sort,
            only_relevant_changes=True,
            min_abs_change_percent=D("10"),
            change_threshold_percent=D("10"),
        )
        joined = "\n".join(sqls)
        assert "ORDER BY" in joined
        assert "DROP" not in joined.upper() or "DROP" not in sort.upper()
        if sort == "latest_reception":
            assert "last_reception_date DESC" in joined
        if sort == "pct_increase":
            assert "change_percent DESC" in joined
        assert "ABS(p.change_percent)" in joined or "change_percent" in joined


def test_decimal_and_jsonb_warnings_serialization():
    item = map_company_product_item(
        _partial_coverage_row(
            current_cost=D("1234.56"),
            current_warnings_json='["suspicious_outlier", "missing_taxes_in_gross"]',
        )
    )
    assert item["current_cost"] == "1234.56"
    assert "suspicious_outlier" in item["current_warnings"]
    payload = json.loads(json.dumps(item))
    CompanyProductItemSchema.model_validate(payload)


def test_null_office_name_allowed():
    item = map_company_product_item(
        _partial_coverage_row(current_office_name=None)
    )
    assert item["current_office_name"] is None
    CompanyProductItemSchema.model_validate(item)


def test_summary_without_office_differences():
    def executor(sql: str, params: tuple) -> list[dict]:
        if "FROM bsale.offices" in sql:
            return [
                {"office_id": 1, "office_name": "BODEGA"},
                {"office_id": 3, "office_name": "SUPER"},
                {"office_id": 4, "office_name": "Q1"},
                {"office_id": 5, "office_name": "Q2"},
            ]
        if "relevant_changes" in sql:
            return [
                {
                    "total_products": 2,
                    "products_with_current_cost": 2,
                    "products_without_current_cost": 0,
                    "relevant_changes": 0,
                    "products_requiring_review": 0,
                    "products_with_outlier": 0,
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
        date_from=date(2026, 5, 5),
        date_to=date(2026, 8, 3),
        change_threshold_percent=D("10"),
    )
    assert s["products_with_office_difference"] == 0
    assert s["offices_with_v2_coverage"] == 1


def test_repo_assert_read_only_no_writes():
    from backend.services.analytics.validate_distribuidora_source import (
        assert_sql_is_read_only,
    )

    with pytest.raises(Exception):
        assert_sql_is_read_only("UPDATE analytics.cost_reception_calculated SET x=1")


def test_http_500_detail_contract_no_leak():
    """Router company endpoints: detail genérico, sin filtrar TypeError/SQL al cliente."""
    from pathlib import Path

    src = Path(__file__).resolve().parents[1] / "routers" / "cost_analytics.py"
    text = src.read_text(encoding="utf-8")
    assert "def _company_http_error" in text
    assert 'detail="Error interno al consultar Costos"' in text
    assert "log_company_endpoint_error" in text
    # Los handlers company usan _company_http_error, no str(exc) directo
    assert "raise _company_http_error(" in text
    assert 'endpoint="company-summary"' in text
    assert 'endpoint="company-products"' in text


def test_office_scoped_v2_with_repo_still_intact():
    """Endpoints anteriores: patrón _with_repo de office V2 intacto."""
    from backend.services import cost_v2_read_service as office_svc

    fake_conn = MagicMock()
    with (
        patch.object(office_svc, "open_readonly_connection") as open_ro,
        patch.object(office_svc, "make_psycopg_executor") as make_ex,
        patch.object(office_svc, "CostV2ReadRepository"),
    ):
        open_ro.return_value = fake_conn
        make_ex.return_value = lambda sql, params: []
        office_svc._with_repo(lambda repo: {"ok": True})
        assert open_ro.call_args.args
        assert make_ex.call_args.kwargs["statement_timeout_seconds"] == 20
