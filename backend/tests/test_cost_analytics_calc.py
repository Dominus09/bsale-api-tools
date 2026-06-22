"""Tests cálculos Analítica → Costos."""

from decimal import Decimal

from backend.utils.cost_analytics_calc import (
    alert_semaphore,
    branch_spread_pct,
    classify_cost_alert,
    cost_gross_from_net,
    make_unique_key,
    parse_tax_factor,
    split_erp_cost,
    variation_pct,
)


def test_parse_tax_factor_defaults():
    assert parse_tax_factor(None) == Decimal("1")
    assert parse_tax_factor(0) == Decimal("1")
    assert parse_tax_factor("1.19") == Decimal("1.19")


def test_cost_gross_from_net():
    assert cost_gross_from_net(1000, 1.19) == Decimal("1190.0000")


def test_split_erp_cost_with_iva():
    iva, other, bruto = split_erp_cost(1000, tax_factor=1.19, iva_rate=19)
    assert bruto == Decimal("1190.0000")
    assert iva == Decimal("190.0000")
    assert other == Decimal("0.0000")


def test_variation_pct():
    assert variation_pct(110, 100) == 10.0
    assert variation_pct(100, None) is None


def test_branch_spread():
    assert branch_spread_pct(850, 920) == 8.24


def test_classify_alerts_cross_branch():
    kinds = classify_cost_alert(
        has_history=True,
        has_cost_row=True,
        average_cost=100,
        cost_net=100,
        variation_pct=5,
        cross_branch_spread=12,
    )
    assert "cross_branch_diff" in kinds


def test_semaphore():
    assert alert_semaphore(["variation_20"]) == "red"
    assert alert_semaphore(["variation_10"]) == "yellow"
    assert alert_semaphore([]) == "green"


def test_unique_key():
    assert make_unique_key(3, 999) == "3_999"
