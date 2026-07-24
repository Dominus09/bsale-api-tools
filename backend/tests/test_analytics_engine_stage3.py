"""Tests Etapa 3 — resolvedor de costo histórico (sin PostgreSQL)."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from backend.services.analytics.cost_models import (
    LineCostInput,
    ReceptionCostCandidate,
    VariantCostSnapshot,
)
from backend.services.analytics.cost_repository import (
    build_reception_costs_query,
    build_variant_cost_snapshots_query,
)
from backend.services.analytics.historical_costs import (
    HistoricalCostResolver,
    resolve_unit_cost_from_candidates,
    select_reception_for_date,
)
from backend.services.analytics.money import D
from backend.services.analytics.schemas import CostFallbackLevel, CostQualityStatus


def _line(
    *,
    document_id: int = 1,
    detail_id: int = 10,
    variant_id: int | None = 100,
    commercial_date: date = date(2026, 7, 20),
    quantity: str = "1",
    net: str = "100",
) -> LineCostInput:
    return LineCostInput(
        document_id=document_id,
        detail_id=detail_id,
        variant_id=variant_id,
        commercial_date=commercial_date,
        quantity=D(quantity),
        line_net_amount=D(net),
    )


def _reception(
    *,
    id: int,
    variant_id: int = 100,
    cost: str,
    cost_date: date,
    reception_id: int = 500,
) -> ReceptionCostCandidate:
    return ReceptionCostCandidate(
        id=id,
        variant_id=variant_id,
        cost_net=D(cost),
        cost_date=cost_date,
        reception_id=reception_id,
        reception_detail_id=id,
        document_number=9000 + id,
        office_id=1,
    )


def test_exact_historical_before_sale():
    line = _line(commercial_date=date(2026, 7, 20), net="100", quantity="1")
    receptions = [
        _reception(id=1, cost="80", cost_date=date(2026, 7, 10)),
        _reception(id=2, cost="90", cost_date=date(2026, 7, 25)),  # posterior: ignorar
    ]
    snap = VariantCostSnapshot(
        variant_id=100,
        average_cost_net=D("50"),
        last_update=date(2026, 7, 22),
        cost_source="cost_receptions_sync",
    )
    result = resolve_unit_cost_from_candidates(
        line, receptions=receptions, snapshot=snap
    )
    assert result.quality_status == CostQualityStatus.HISTORICAL_REAL
    assert result.unit_cost == D("80.0000")
    assert result.total_cost == D("80.0000")
    assert result.gross_profit == D("20.0000")
    assert result.gross_margin_pct == D("20.0000")
    assert result.markup_pct == D("25.0000")
    assert result.cost_date == date(2026, 7, 10)
    assert result.age_days_at_sale == 10
    assert result.is_estimated is False


def test_cost_after_sale_not_used():
    line = _line(commercial_date=date(2026, 7, 20))
    receptions = [_reception(id=1, cost="70", cost_date=date(2026, 7, 21))]
    result = resolve_unit_cost_from_candidates(
        line,
        receptions=receptions,
        snapshot=VariantCostSnapshot(100, D("55"), date(2026, 7, 1), "x"),
    )
    assert result.quality_status == CostQualityStatus.AVERAGE_COST_FALLBACK
    assert result.unit_cost == D("55.0000")


def test_multiple_costs_takes_most_recent_before_sale():
    line = _line(commercial_date=date(2026, 7, 20))
    receptions = [
        _reception(id=1, cost="70", cost_date=date(2026, 6, 1)),
        _reception(id=2, cost="80", cost_date=date(2026, 7, 15)),
        _reception(id=3, cost="75", cost_date=date(2026, 7, 1)),
    ]
    winner, _, _ = select_reception_for_date(
        receptions, commercial_date=date(2026, 7, 20)
    )
    assert winner is not None
    assert winner.id == 2
    assert winner.cost_net == D("80")


def test_conflicting_same_date_different_costs():
    line = _line(commercial_date=date(2026, 7, 20))
    receptions = [
        _reception(id=1, cost="80", cost_date=date(2026, 7, 10)),
        _reception(id=2, cost="82", cost_date=date(2026, 7, 10)),
    ]
    result = resolve_unit_cost_from_candidates(
        line, receptions=receptions, snapshot=None
    )
    assert result.quality_status == CostQualityStatus.CONFLICTING_COST
    assert result.unit_cost is None
    assert result.gross_profit is None
    assert result.gross_margin_pct is None
    assert result.markup_pct is None
    assert set(result.conflicting_source_ids) == {1, 2}


def test_average_cost_fallback():
    line = _line()
    result = resolve_unit_cost_from_candidates(
        line,
        receptions=[],
        snapshot=VariantCostSnapshot(100, D("40"), date(2026, 7, 1), "avg"),
    )
    assert result.quality_status == CostQualityStatus.AVERAGE_COST_FALLBACK
    assert result.fallback_level == CostFallbackLevel.AVERAGE_COST
    assert result.is_estimated is True
    assert result.unit_cost == D("40.0000")


def test_missing_cost_and_zero_average():
    missing = resolve_unit_cost_from_candidates(
        _line(), receptions=[], snapshot=None
    )
    assert missing.quality_status == CostQualityStatus.MISSING_COST
    assert missing.unit_cost is None
    assert missing.gross_profit is None

    zero = resolve_unit_cost_from_candidates(
        _line(),
        receptions=[],
        snapshot=VariantCostSnapshot(100, D("0"), date(2026, 7, 1), "x"),
    )
    assert zero.quality_status == CostQualityStatus.MISSING_COST


def test_decimal_quantity_and_zero_cost_no_markup():
    line = _line(quantity="2.5", net="100")
    result = resolve_unit_cost_from_candidates(
        line,
        receptions=[_reception(id=1, cost="10", cost_date=date(2026, 7, 1))],
        snapshot=None,
    )
    assert result.total_cost == D("25.0000")
    assert result.gross_profit == D("75.0000")

    zero_cost_line = _line(net="100", quantity="1")
    # Costo histórico real 0: utilidad = venta; markup None
    from backend.services.analytics.formulas import line_economics

    eco = line_economics(
        net_sales=D("100"),
        historical_cost=D("0"),
        cost_quality=CostQualityStatus.HISTORICAL_REAL,
    )
    assert eco.gross_margin_pct == D("100.0000")
    assert eco.markup_pct is None
    assert zero_cost_line.line_net_amount == D("100")


def test_canonical_100_80_margin_markup():
    result = resolve_unit_cost_from_candidates(
        _line(net="100", quantity="1"),
        receptions=[_reception(id=1, cost="80", cost_date=date(2026, 7, 1))],
        snapshot=VariantCostSnapshot(100, D("999"), date(2026, 7, 22), "later"),
    )
    assert result.gross_profit == D("20.0000")
    assert result.gross_margin_pct == D("20.0000")
    assert result.markup_pct == D("25.0000")


def test_historical_stable_when_current_changes():
    line = _line(commercial_date=date(2026, 7, 20), net="100")
    receptions = [_reception(id=1, cost="80", cost_date=date(2026, 7, 5))]
    first = resolve_unit_cost_from_candidates(
        line,
        receptions=receptions,
        snapshot=VariantCostSnapshot(100, D("80"), date(2026, 7, 5), "x"),
    )
    second = resolve_unit_cost_from_candidates(
        line,
        receptions=receptions,
        snapshot=VariantCostSnapshot(100, D("120"), date(2026, 7, 22), "x"),
    )
    assert first.unit_cost == second.unit_cost == D("80.0000")
    assert first.gross_profit == second.gross_profit == D("20.0000")


def test_sql_builders_have_filters():
    sql, params = build_reception_costs_query(
        company_id=3,
        variant_ids=[100, 200],
        on_or_before=date(2026, 7, 20),
    )
    assert "company_id = %s" in sql
    assert "variant_id = ANY(%s)" in sql
    assert "admission_date::date <= %s" in sql
    assert "SELECT *" not in sql
    assert params[0] == 3

    sql2, params2 = build_variant_cost_snapshots_query(
        company_id=3, variant_ids=[100]
    )
    assert "bsale.variant_cost" in sql2
    assert params2[0] == 3


def test_resolver_batch_with_fake_repo():
    class FakeRepo:
        def fetch_reception_candidates(self, **kwargs):
            return [
                _reception(id=1, variant_id=100, cost="80", cost_date=date(2026, 7, 1)),
                _reception(id=2, variant_id=200, cost="30", cost_date=date(2026, 7, 1)),
            ]

        def fetch_variant_snapshots(self, **kwargs):
            return {}

    resolver = HistoricalCostResolver(FakeRepo())  # type: ignore[arg-type]
    results = resolver.resolve_lines(
        [
            _line(detail_id=1, variant_id=100, net="100"),
            _line(detail_id=2, variant_id=200, net="50", quantity="1"),
            _line(detail_id=3, variant_id=999, net="10"),
        ],
        company_id=3,
    )
    assert results[0].quality_status == CostQualityStatus.HISTORICAL_REAL
    assert results[1].unit_cost == D("30.0000")
    assert results[2].quality_status == CostQualityStatus.MISSING_COST
