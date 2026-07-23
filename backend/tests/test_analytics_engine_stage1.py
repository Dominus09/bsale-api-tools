"""Tests unitarios puros — Etapa 1 motor analítico (sin DB)."""

from __future__ import annotations

from decimal import Decimal

import pytest

from backend.services.analytics.document_source import (
    DOC_TYPE_BOLETA,
    DOC_TYPE_CREDIT_NOTE,
    DOC_TYPE_FACTURA,
    UnboundDocumentSource,
    classify_document_type,
)
from backend.services.analytics.formulas import (
    compute_gross_profit,
    compute_margin_pct,
    compute_markup_pct,
    line_economics,
)
from backend.services.analytics.money import D, quantize_money
from backend.services.analytics.quality import aggregate_line_quality
from backend.services.analytics.schemas import (
    CostQualityStatus,
    DataQualityStatus,
)


def test_canonical_margin_and_markup_100_80():
    """Precio neto 100, costo 80 → utilidad 20, margen 20%, markup 25%."""
    net_sales = D("100")
    cost = D("80")
    profit = compute_gross_profit(net_sales, cost)
    assert profit == D("20.0000")
    assert compute_margin_pct(net_sales, profit) == D("20.0000")
    assert compute_markup_pct(cost, profit) == D("25.0000")

    eco = line_economics(net_sales=net_sales, historical_cost=cost)
    assert eco.gross_profit == D("20.0000")
    assert eco.gross_margin_pct == D("20.0000")
    assert eco.markup_pct == D("25.0000")
    assert eco.cost_quality == CostQualityStatus.HISTORICAL_REAL


def test_missing_cost_does_not_become_zero():
    eco = line_economics(
        net_sales=D("100"),
        historical_cost=None,
        cost_quality=CostQualityStatus.MISSING_COST,
    )
    assert eco.historical_cost is None
    assert eco.gross_profit is None
    assert eco.gross_margin_pct is None
    assert eco.markup_pct is None

    # Aunque pasen 0, MISSING_COST fuerza None (no margen 100%).
    eco_zero = line_economics(
        net_sales=D("100"),
        historical_cost=D("0"),
        cost_quality=CostQualityStatus.MISSING_COST,
    )
    assert eco_zero.gross_profit is None
    assert eco_zero.gross_margin_pct is None
    assert eco_zero.markup_pct is None


def test_zero_net_sales_margin_is_none():
    profit = compute_gross_profit(D("0"), D("10"))
    assert profit == D("-10.0000")
    assert compute_margin_pct(D("0"), profit) is None


def test_zero_cost_markup_is_none_but_margin_ok():
    """Costo 0 conocido (no missing): markup indefinido; margen sí si hay venta."""
    eco = line_economics(
        net_sales=D("100"),
        historical_cost=D("0"),
        cost_quality=CostQualityStatus.HISTORICAL_REAL,
    )
    assert eco.gross_profit == D("100.0000")
    assert eco.gross_margin_pct == D("100.0000")
    assert eco.markup_pct is None


def test_partial_quality_when_some_lines_missing_cost():
    statuses = [
        CostQualityStatus.HISTORICAL_REAL,
        CostQualityStatus.MISSING_COST,
        CostQualityStatus.HISTORICAL_REAL,
    ]
    q = aggregate_line_quality(statuses, source_scope="test")
    assert q.total_lines == 3
    assert q.costed_lines == 2
    assert q.missing_cost_lines == 1
    assert q.estimated_cost_lines == 0
    assert q.cost_coverage_pct == D("66.6667")
    assert q.quality_status == DataQualityStatus.PARTIAL


def test_complete_and_estimated_quality():
    complete = aggregate_line_quality(
        [CostQualityStatus.HISTORICAL_REAL, CostQualityStatus.HISTORICAL_REAL]
    )
    assert complete.quality_status == DataQualityStatus.COMPLETE
    assert complete.cost_coverage_pct == D("100.0000")

    estimated = aggregate_line_quality(
        [
            CostQualityStatus.AVERAGE_COST_FALLBACK,
            CostQualityStatus.CURRENT_COST_FALLBACK,
        ]
    )
    assert estimated.quality_status == DataQualityStatus.ESTIMATED
    assert estimated.estimated_cost_lines == 2


def test_conflicting_quality_from_header_mismatch():
    q = aggregate_line_quality(
        [CostQualityStatus.HISTORICAL_REAL],
        header_line_mismatch_docs=1,
    )
    assert q.quality_status == DataQualityStatus.CONFLICTING


def test_money_helpers_use_decimal_not_float():
    value = quantize_money(D("10.12345"))
    assert isinstance(value, Decimal)
    assert not isinstance(value, float)
    assert value == D("10.1235")

    with pytest.raises(TypeError):
        D(None)


def test_document_type_classification():
    assert classify_document_type(DOC_TYPE_BOLETA) == "sale"
    assert classify_document_type(DOC_TYPE_FACTURA) == "sale"
    assert classify_document_type(DOC_TYPE_CREDIT_NOTE) == "credit_note"
    assert classify_document_type(33) == "unsupported"


def test_unbound_document_source_raises():
    src = UnboundDocumentSource()
    with pytest.raises(NotImplementedError):
        src.list_headers(
            company_id=3,
            office_id=1,
            date_from=__import__("datetime").date(2026, 7, 1),
            date_to=__import__("datetime").date(2026, 7, 23),
        )
