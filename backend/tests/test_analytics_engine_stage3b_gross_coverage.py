"""Tests Etapa 3B — job cobertura costo bruto (sin PostgreSQL)."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

import pytest

from backend.jobs import validate_analytics_gross_cost_coverage as job
from backend.services.analytics.cost_models import (
    HistoricalCostResolution,
    ReceptionCostCandidate,
    VariantCostSnapshot,
)
from backend.services.analytics.cost_repository import CostCandidateRepository
from backend.services.analytics.distribuidora_source import DistribuidoraDocumentSource
from backend.services.analytics.money import D
from backend.services.analytics.schemas import CostFallbackLevel, CostQualityStatus
from backend.services.analytics.validate_gross_cost_coverage import (
    AnalyticsValidationError,
    assert_sql_is_read_only,
    build_coverage_report,
    clamp_gross_validate_args,
    commercial_date_window,
    open_readonly_connection,
    run_gross_cost_validation,
)


class FakeConn:
    def __init__(self) -> None:
        self.readonly = False
        self.autocommit = True
        self.rolled_back = False
        self.closed = False
        self.executed: list[str] = []

    def set_session(self, *, readonly: bool = False, autocommit: bool = False) -> None:
        self.readonly = readonly
        self.autocommit = autocommit

    def cursor(self) -> Any:
        return FakeCursor(self)

    def rollback(self) -> None:
        self.rolled_back = True

    def close(self) -> None:
        self.closed = True


class FakeCursor:
    def __init__(self, conn: FakeConn) -> None:
        self.conn = conn
        self.description = None

    def execute(self, sql: str, params: tuple | None = None) -> None:
        self.conn.executed.append(sql)

    def fetchall(self) -> list:
        return []

    def close(self) -> None:
        return None


class RecordingExecutor:
    def __init__(
        self,
        *,
        headers: list[dict],
        lines: list[dict],
        receptions: list[dict] | None = None,
        snapshots: list[dict] | None = None,
    ) -> None:
        self.headers = headers
        self.lines = lines
        self.receptions = receptions or []
        self.snapshots = snapshots or []
        self.sqls: list[str] = []
        self.params: list[tuple] = []

    def __call__(self, sql: str, params: tuple) -> list[dict]:
        assert_sql_is_read_only(sql)
        self.sqls.append(sql)
        self.params.append(params)
        upper = sql.upper()
        if "COST_RECEPTION_HISTORY" in upper:
            return list(self.receptions)
        if "VARIANT_COST" in upper and "AVERAGE_COST" in upper:
            return list(self.snapshots)
        if "DOCUMENT_DETAILS" in upper:
            ids = set(params[2]) if len(params) >= 3 else set()
            return [r for r in self.lines if r["document_id"] in ids]
        return list(self.headers)


def _header(document_id: int = 1, *, total: str = "2000", net: str = "1680") -> dict:
    return {
        "document_id": document_id,
        "source_document_id": document_id + 1000,
        "document_type_id": 6,
        "number": 50000 + document_id,
        "company_id": 3,
        "office_id": 1,
        "emission_date": date(2026, 7, 20),
        "generation_date": date(2026, 7, 20),
        "client_id": 10,
        "seller_id": None,
        "seller_name": None,
        "net_amount": D(net),
        "tax_amount": D("320"),
        "total_amount": D(total),
        "state": 0,
        "commercial_state": 0,
    }


def _line(
    document_id: int = 1,
    *,
    detail_id: int = 10,
    variant_id: int = 100,
    qty: str = "1",
    net: str = "1680",
    total: str = "2000",
) -> dict:
    return {
        "detail_id": detail_id,
        "document_id": document_id,
        "variant_id": variant_id,
        "variant_code": "X",
        "quantity": D(qty),
        "net_amount": D(net),
        "tax_amount": None,
        "total_amount": D(total),
        "net_unit_value": None,
        "total_unit_value": None,
        "net_discount": None,
        "total_discount": None,
    }


def test_clamp_args_limits():
    args = clamp_gross_validate_args(
        company_id=3,
        office_id=1,
        days=99,
        document_limit=9999,
        statement_timeout_seconds=99,
        sample_limit=99,
    )
    assert args.days == 30
    assert args.document_limit == 500
    assert args.statement_timeout_seconds == 30
    assert args.sample_limit == 20
    with pytest.raises(AnalyticsValidationError):
        clamp_gross_validate_args(company_id=0, office_id=1)


def test_open_readonly_and_rollback_job(monkeypatch):
    fake = FakeConn()
    conn = open_readonly_connection(lambda: fake)
    assert conn.readonly is True

    monkeypatch.setattr(job, "open_readonly_connection", lambda g: fake)
    monkeypatch.setattr(
        job,
        "make_psycopg_executor",
        lambda *a, **k: RecordingExecutor(headers=[], lines=[]),
    )
    monkeypatch.setattr(
        job,
        "run_gross_cost_validation",
        lambda **kwargs: {"ok": True, "read_only": True},
    )
    code, payload = job.run_job(
        ["--company-id", "3", "--office-id", "1", "--days", "7"]
    )
    assert code == 0
    assert payload["ok"] is True
    assert fake.rolled_back is True
    assert fake.closed is True


def test_forbidden_sql_blocked():
    with pytest.raises(AnalyticsValidationError):
        assert_sql_is_read_only("UPDATE analytics.cost_reception_history SET x=1")
    with pytest.raises(AnalyticsValidationError):
        assert_sql_is_read_only("SELECT * FROM t FOR UPDATE")


def test_no_n_plus_one_in_simulated_repo():
    exe = RecordingExecutor(
        headers=[_header()],
        lines=[_line()],
        receptions=[
            {
                "id": 1,
                "variant_id": 100,
                "cost_net": D("1000"),
                "admission_date": date(2026, 7, 10),
                "reception_id": 9,
                "reception_detail_id": 1,
                "document_number": 1,
                "office_id": 1,
                "iva_amount": D("190"),
                "other_taxes": D("205"),
                "cost_bruto_erp": D("1395"),
            }
        ],
        snapshots=[],
    )
    args = clamp_gross_validate_args(company_id=3, office_id=1, days=7)
    report = run_gross_cost_validation(
        args=args,
        document_source=DistribuidoraDocumentSource(exe),
        cost_repository=CostCandidateRepository(exe),
        today=date(2026, 7, 22),
    )
    assert report["ok"] is True
    # Batch: docs + lines + receptions (+ optional snapshots). No per-line queries.
    assert len(exe.sqls) <= 4
    assert report["gross_cost_coverage"]["actual_purchase_gross"] == 1
    assert report["commercial_margin"]["calculable_lines"] == 1
    assert report["commercial_margin"]["gross_margin_pct"] == "30.25"


def test_coverage_weighted_and_json_shape():
    args = clamp_gross_validate_args(company_id=3, office_id=1, sample_limit=10)
    resolutions = [
        HistoricalCostResolution(
            detail_id=1,
            document_id=1,
            variant_id=10,
            commercial_date=date(2026, 7, 20),
            unit_cost=D("1000"),
            total_cost=D("1000"),
            cost_source="x",
            cost_date=date(2026, 7, 1),
            purchase_document_id=1,
            supplier_id=None,
            age_days_at_sale=19,
            fallback_level=CostFallbackLevel.RECEPTION_AT_SALE,
            is_estimated=False,
            quality_status=CostQualityStatus.HISTORICAL_REAL,
            resolution_reason="ok",
            historical_net_cost=D("1000"),
            historical_gross_cost=D("1395"),
            gross_sales=D("2000"),
            line_net_amount=D("1680"),
            gross_commercial_profit=D("605"),
            gross_cost_quality="actual_purchase_gross",
            tax_breakdown_quality="aggregated_other_taxes",
        ),
        HistoricalCostResolution(
            detail_id=2,
            document_id=2,
            variant_id=11,
            commercial_date=date(2026, 7, 20),
            unit_cost=None,
            total_cost=None,
            cost_source=None,
            cost_date=None,
            purchase_document_id=None,
            supplier_id=None,
            age_days_at_sale=None,
            fallback_level=CostFallbackLevel.MISSING,
            is_estimated=False,
            quality_status=CostQualityStatus.MISSING_COST,
            resolution_reason="missing",
            gross_sales=D("1000"),
            line_net_amount=D("840"),
            historical_gross_cost=None,
            gross_cost_quality="missing_gross_cost",
            tax_breakdown_quality="missing_breakdown",
        ),
    ]
    report = build_coverage_report(
        args=args,
        date_from=date(2026, 7, 16),
        date_to=date(2026, 7, 22),
        documents_loaded=2,
        resolutions=resolutions,
        duration_ms=12.5,
    )
    assert report["ok"] is True
    assert report["read_only"] is True
    assert report["lines_loaded"] == 2
    assert report["unique_variants"] == 2
    assert report["commercial_margin"]["calculable_lines"] == 1
    assert report["commercial_margin"]["uncalculable_lines"] == 1
    assert report["commercial_margin"]["line_coverage_pct"] == "50.00"
    # ponderado: 2000/(2000+1000)=66.67
    assert report["commercial_margin"]["gross_sales_coverage_pct"] == "66.67"
    assert report["net_cost_coverage"]["missing"] == 1
    assert report["gross_cost_coverage"]["actual_purchase_gross"] == 1
    assert report["gross_cost_coverage"]["missing"] == 1
    assert report["tax_breakdown"]["aggregated_other_taxes"] == 1
    assert "samples" in report
    assert len(report["samples"]) >= 1


def test_commercial_date_window():
    d0, d1 = commercial_date_window(7, today=date(2026, 7, 22))
    assert d1 == date(2026, 7, 22)
    assert d0 == date(2026, 7, 16)
