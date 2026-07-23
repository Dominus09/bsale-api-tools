"""Tests Etapa 2A — adaptador Distribuidora con executor falso (sin PG)."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

import pytest

from backend.services.analytics.document_models import (
    AnalyticsDocumentKind,
    LineNetMethod,
    ReconciliationStatus,
    resolve_commercial_date,
)
from backend.services.analytics.distribuidora_source import (
    CONFIRMED_DOCUMENT_TYPES,
    DistribuidoraDocumentSource,
    build_headers_query,
    build_lines_query,
    row_to_header,
    row_to_line,
)
from backend.services.analytics.line_net import allocate_line_nets
from backend.services.analytics.money import D
from backend.services.analytics.reconciliation import reconcile_header_vs_lines


class FakeExecutor:
    def __init__(self, rows_by_keyword: dict[str, list[dict]] | None = None):
        self.calls: list[tuple[str, tuple]] = []
        self._rows = rows_by_keyword or {}

    def __call__(self, sql: str, params: tuple) -> list[dict]:
        self.calls.append((sql, params))
        if "document_details" in sql:
            return list(self._rows.get("lines", []))
        return list(self._rows.get("headers", []))


def _header_row(**overrides):
    base = {
        "document_id": 1001,
        "source_document_id": 9001,
        "document_type_id": 6,
        "number": 50001,
        "company_id": 3,
        "office_id": 1,
        "emission_date": date(2026, 7, 20),
        "generation_date": date(2026, 7, 19),
        "client_id": 10,
        "seller_id": 80,
        "seller_name": "Cristopher Saldivia",
        "net_amount": Decimal("100.0000"),
        "tax_amount": Decimal("19.0000"),
        "total_amount": Decimal("119.0000"),
        "state": 0,
        "commercial_state": 0,
    }
    base.update(overrides)
    return base


def _line_row(**overrides):
    base = {
        "detail_id": 1,
        "document_id": 1001,
        "variant_id": 50,
        "variant_code": "ABC",
        "quantity": Decimal("2"),
        "net_amount": Decimal("50.0000"),
        "tax_amount": Decimal("9.5000"),
        "total_amount": Decimal("59.5000"),
        "net_unit_value": Decimal("25.0000"),
        "total_unit_value": Decimal("29.7500"),
        "net_discount": None,
        "total_discount": None,
    }
    base.update(overrides)
    return base


def test_sql_headers_requires_scope_filters():
    sql, params = build_headers_query(
        company_id=3,
        office_id=1,
        date_from=date(2026, 7, 1),
        date_to=date(2026, 7, 22),
        page=1,
        page_size=20,
    )
    assert "d.company_id = %s" in sql
    assert "d.office_id = %s" in sql
    assert "COALESCE(d.emission_date, d.generation_date)::date >= %s" in sql
    assert "COALESCE(d.emission_date, d.generation_date)::date <= %s" in sql
    assert "COALESCE(d.state, 0) = 0" in sql
    assert "LIMIT %s OFFSET %s" in sql
    assert "SELECT *" not in sql
    assert params[0] == 3
    assert params[1] == 1
    assert params[2] == date(2026, 7, 1)
    assert params[3] == date(2026, 7, 22)


def test_sql_rejects_missing_dates():
    with pytest.raises(ValueError):
        build_headers_query(
            company_id=3,
            office_id=1,
            date_from=None,  # type: ignore[arg-type]
            date_to=date(2026, 7, 22),
        )


def test_active_document_included_annulled_excluded_via_sql_flag():
    sql_active, _ = build_headers_query(
        company_id=3,
        office_id=1,
        date_from=date(2026, 7, 1),
        date_to=date(2026, 7, 22),
        active_only=True,
    )
    assert "COALESCE(d.state, 0) = 0" in sql_active
    sql_all, _ = build_headers_query(
        company_id=3,
        office_id=1,
        date_from=date(2026, 7, 1),
        date_to=date(2026, 7, 22),
        active_only=False,
    )
    assert "COALESCE(d.state, 0) = 0" not in sql_all


def test_factura_boleta_nc_classification():
    assert row_to_header(_header_row(document_type_id=6)).kind == AnalyticsDocumentKind.SALE
    assert row_to_header(_header_row(document_type_id=1)).kind == AnalyticsDocumentKind.SALE
    assert (
        row_to_header(_header_row(document_type_id=9)).kind
        == AnalyticsDocumentKind.CREDIT_NOTE
    )
    assert (
        row_to_header(_header_row(document_type_id=33)).kind
        == AnalyticsDocumentKind.UNSUPPORTED
    )
    assert CONFIRMED_DOCUMENT_TYPES == (1, 6, 9)


def test_commercial_date_emission_and_fallback():
    assert resolve_commercial_date(date(2026, 7, 20), date(2026, 7, 19)) == date(
        2026, 7, 20
    )
    assert resolve_commercial_date(None, date(2026, 7, 19)) == date(2026, 7, 19)
    assert resolve_commercial_date(
        datetime(2026, 7, 20, 15, 0, 0), None
    ) == date(2026, 7, 20)
    h = row_to_header(_header_row(emission_date=None, generation_date=date(2026, 7, 18)))
    assert h.commercial_date == date(2026, 7, 18)


def test_amounts_are_decimal():
    h = row_to_header(_header_row())
    assert isinstance(h.net_amount, Decimal)
    assert isinstance(h.total_amount, Decimal)
    line = row_to_line(_line_row())
    assert isinstance(line.line_total_amount, Decimal)


def test_pagination_and_deterministic_order():
    sql, params = build_headers_query(
        company_id=3,
        office_id=1,
        date_from=date(2026, 7, 1),
        date_to=date(2026, 7, 22),
        page=2,
        page_size=10,
    )
    assert "ORDER BY COALESCE(d.emission_date, d.generation_date) ASC NULLS LAST" in sql
    assert "d.document_id ASC" in sql
    assert params[-2:] == (10, 10)  # LIMIT 10 OFFSET 10

    lines_sql, lines_params = build_lines_query(
        company_id=3,
        office_id=1,
        date_from=date(2026, 7, 1),
        date_to=date(2026, 7, 22),
        page=1,
        page_size=5,
    )
    assert "ORDER BY dd.document_id ASC, dd.detail_id ASC" in lines_sql
    assert lines_params[-2:] == (5, 0)


def test_fetch_via_fake_executor():
    executor = FakeExecutor(
        {
            "headers": [
                _header_row(document_id=1, state=0),
                _header_row(document_id=2, document_type_id=9, number=9),
            ]
        }
    )
    src = DistribuidoraDocumentSource(executor)
    docs = src.fetch_documents(
        company_id=3,
        office_id=1,
        date_from=date(2026, 7, 1),
        date_to=date(2026, 7, 22),
        page_size=20,
    )
    assert len(docs) == 2
    assert docs[0].is_active is True
    assert docs[1].kind == AnalyticsDocumentKind.CREDIT_NOTE
    assert len(executor.calls) == 1
    assert "distribuidora.documents" in executor.calls[0][0]


def test_reconciliation_matched_rounding_mismatch_missing():
    matched = reconcile_header_vs_lines(
        document_id=1,
        header_total_amount=D("100"),
        header_net_amount=D("84"),
        line_totals=[D("60"), D("40")],
        line_nets=[D("50"), D("34")],
        quantities=[D("1"), D("1")],
    )
    assert matched.reconciliation_status == ReconciliationStatus.MATCHED

    rounding = reconcile_header_vs_lines(
        document_id=2,
        header_total_amount=D("100"),
        header_net_amount=D("84"),
        line_totals=[D("60"), D("39.50")],
        line_nets=[D("50"), D("34")],
        quantities=[D("1"), D("1")],
    )
    assert rounding.reconciliation_status == ReconciliationStatus.ROUNDING_DIFFERENCE
    assert abs(rounding.difference_total or 0) <= D("1")

    mismatch = reconcile_header_vs_lines(
        document_id=3,
        header_total_amount=D("100"),
        header_net_amount=D("84"),
        line_totals=[D("60"), D("30")],
        line_nets=[D("50"), D("34")],
        quantities=[D("1"), D("1")],
    )
    assert mismatch.reconciliation_status == ReconciliationStatus.MISMATCH

    missing = reconcile_header_vs_lines(
        document_id=4,
        header_total_amount=D("100"),
        header_net_amount=D("84"),
        line_totals=[],
        line_nets=[],
        quantities=[],
    )
    assert missing.reconciliation_status == ReconciliationStatus.MISSING_LINES


def test_allocate_proportional_sums_to_header_net():
    allocated, method = allocate_line_nets(
        header_net_amount=D("100"),
        line_nets=[None, None],
        line_totals=[D("60"), D("40")],
    )
    assert method == LineNetMethod.ALLOCATED_FROM_HEADER
    assert sum(allocated, D("0")) == D("100.0000")
    assert allocated[0] == D("60.0000")
    assert allocated[1] == D("40.0000")


def test_allocate_explicit_when_line_nets_present():
    allocated, method = allocate_line_nets(
        header_net_amount=D("100"),
        line_nets=[D("55"), D("45")],
        line_totals=[D("60"), D("40")],
    )
    assert method == LineNetMethod.EXPLICIT_LINE_NET
    assert allocated == [D("55.0000"), D("45.0000")]


def test_allocate_unavailable_without_header_net():
    allocated, method = allocate_line_nets(
        header_net_amount=None,
        line_nets=[None],
        line_totals=[D("10")],
    )
    assert method == LineNetMethod.UNAVAILABLE
    assert allocated == [None]


def test_source_reconcile_uses_allocation():
    header = row_to_header(_header_row(net_amount=D("100"), total_amount=D("119")))
    lines = [
        row_to_line(
            _line_row(
                detail_id=1,
                net_amount=None,
                total_amount=D("71.4000"),
            )
        ),
        row_to_line(
            _line_row(
                detail_id=2,
                net_amount=None,
                total_amount=D("47.6000"),
            )
        ),
    ]
    src = DistribuidoraDocumentSource(FakeExecutor())
    enriched = src.enrich_lines_with_header_net(header, lines)
    assert enriched[0].net_method == LineNetMethod.ALLOCATED_FROM_HEADER
    assert sum((ln.allocated_net_amount or D("0")) for ln in enriched) == D("100.0000")
    result = src.reconcile_document(header, lines)
    assert result.reconciliation_status == ReconciliationStatus.MATCHED
