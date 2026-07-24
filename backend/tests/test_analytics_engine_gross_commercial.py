"""Tests corrección comercial bruta (IVA + ILA aditivos) — sin PostgreSQL.

Casos A–E obligatorios + partial sin perfil tributario + NC por componente.
Redondeo: dinero MONEY_QUANT 0.0001 HALF_UP; % comercial 0.01 HALF_UP.
"""

from __future__ import annotations

from decimal import Decimal

from backend.services.analytics.formulas import (
    apply_taxes_to_net_cost,
    commercial_line_economics,
    compute_commercial_margin_pct,
    compute_commercial_markup_pct,
    compute_commercial_profit,
    reverse_commercial_line,
)
from backend.services.analytics.money import D, quantize_commercial_pct, quantize_money
from backend.services.analytics.schemas import CostQualityStatus
from backend.services.analytics.tax_models import (
    TaxCategory,
    TaxProfile,
    TaxQualityStatus,
    classify_tax_category,
    resolve_tax_profile,
)


def _profile(
    iva: str,
    ila: str,
    *,
    category: TaxCategory = TaxCategory.UNKNOWN,
    quality: TaxQualityStatus = TaxQualityStatus.HISTORICAL_TAX_PROFILE,
) -> TaxProfile:
    return TaxProfile(
        iva_rate_pct=D(iva),
        ila_rate_pct=D(ila),
        tax_category=category,
        tax_source="fixture",
        quality_status=quality,
    )


def test_case_a_iva_only_gross_cost():
    """Costo neto 1000, IVA 19%, ILA 0% → bruto 1190."""
    bd = apply_taxes_to_net_cost(D("1000"), _profile("19", "0", category=TaxCategory.STANDARD_IVA_ONLY))
    assert bd.cost_iva == D("190.0000")
    assert bd.cost_ila == D("0.0000")
    assert bd.historical_gross_cost == D("1190.0000")
    assert bd.tax_category == TaxCategory.STANDARD_IVA_ONLY


def test_case_b_beer_gross_cost():
    """Cerveza: neto 1000, IVA 19%, ILA 20.5% → bruto 1395."""
    bd = apply_taxes_to_net_cost(
        D("1000"),
        _profile("19", "20.5", category=TaxCategory.ILA_BEER_WINE),
    )
    assert bd.cost_iva == D("190.0000")
    assert bd.cost_ila == D("205.0000")
    assert bd.historical_gross_cost == D("1395.0000")
    # NO compuesto: 1000 * 1.19 * 1.205 = 1433.95 ≠ 1395
    compound = quantize_money(D("1000") * D("1.19") * D("1.205"))
    assert compound == D("1433.9500")
    assert bd.historical_gross_cost != compound


def test_case_c_spirits_gross_cost():
    """Destilado: neto 1000, IVA 19%, ILA 31.5% → bruto 1505."""
    bd = apply_taxes_to_net_cost(
        D("1000"),
        _profile("19", "31.5", category=TaxCategory.ILA_SPIRITS),
    )
    assert bd.historical_gross_cost == D("1505.0000")


def test_case_d_ila_10_gross_cost():
    """Bebida ILA 10%: neto 1000, IVA 19%, ILA 10% → bruto 1290."""
    bd = apply_taxes_to_net_cost(
        D("1000"),
        _profile("19", "10", category=TaxCategory.ILA_NON_ALCOHOLIC),
    )
    assert bd.cost_iva == D("190.0000")
    assert bd.cost_ila == D("100.0000")
    assert bd.historical_gross_cost == D("1290.0000")


def test_case_e_commercial_profit_margin_markup():
    """Venta bruta 2000, costo bruto 1395 → utilidad 605, margen 30.25%, markup 43.37%."""
    gross_sales = D("2000")
    gross_cost = D("1395")
    profit = compute_commercial_profit(gross_sales, gross_cost)
    assert profit == D("605.0000")
    margin = compute_commercial_margin_pct(gross_sales, profit)
    markup = compute_commercial_markup_pct(gross_cost, profit)
    assert margin == D("30.25")
    assert markup == D("43.37")
    assert quantize_commercial_pct(D("43.36917562724014336917562724")) == D("43.37")

    cel = commercial_line_economics(
        gross_sales=gross_sales,
        net_sales=D("1680.6723"),  # irrelevante para métricas brutas default
        historical_net_unit_cost=D("1000"),
        quantity=D("1"),
        tax_profile=_profile("19", "20.5", category=TaxCategory.ILA_BEER_WINE),
    )
    assert cel.historical_gross_cost == D("1395.0000")
    assert cel.gross_commercial_profit == D("605.0000")
    assert cel.gross_commercial_margin_pct == D("30.25")
    assert cel.gross_commercial_markup_pct == D("43.37")
    # Preferir total Bsale: no reconstruir venta bruta
    assert cel.gross_sales == D("2000.0000")


def test_missing_tax_profile_keeps_net_clears_gross():
    profile = TaxProfile(
        iva_rate_pct=None,
        ila_rate_pct=None,
        quality_status=TaxQualityStatus.MISSING_TAX_PROFILE,
    )
    bd = apply_taxes_to_net_cost(D("1000"), profile)
    assert bd.historical_net_cost == D("1000.0000")
    assert bd.historical_gross_cost is None
    assert bd.cost_iva is None
    assert bd.cost_ila is None

    cel = commercial_line_economics(
        gross_sales=D("2000"),
        net_sales=D("1680"),
        historical_net_unit_cost=D("1000"),
        quantity=D("1"),
        tax_profile=profile,
    )
    assert cel.historical_net_cost == D("1000.0000")
    assert cel.historical_gross_cost is None
    assert cel.gross_commercial_profit is None
    assert cel.gross_commercial_margin_pct is None
    assert cel.tax_quality_status == TaxQualityStatus.MISSING_TAX_PROFILE.value


def test_iva_only_without_confirmed_ila_is_not_assumed_zero():
    """Solo IVA conocido sin ILA confirmado → no inventar ILA=0."""
    incomplete = TaxProfile(
        iva_rate_pct=D("19"),
        ila_rate_pct=None,
        quality_status=TaxQualityStatus.HISTORICAL_TAX_PROFILE,
    )
    bd = apply_taxes_to_net_cost(D("1000"), incomplete)
    assert bd.historical_gross_cost is None
    assert bd.tax_quality_status == TaxQualityStatus.MISSING_TAX_PROFILE

    resolved = resolve_tax_profile(
        historical_profile=TaxProfile(
            iva_rate_pct=D("19"),
            ila_rate_pct=None,
            quality_status=TaxQualityStatus.HISTORICAL_TAX_PROFILE,
        )
    )
    assert resolved.quality_status == TaxQualityStatus.MISSING_TAX_PROFILE


def test_purchase_line_tax_priority():
    resolved = resolve_tax_profile(
        purchase_net_cost=D("1000"),
        purchase_iva_amount=D("190"),
        purchase_ila_amount=D("205"),
        historical_profile=_profile("19", "0"),
        current_profile=_profile("19", "0"),
    )
    assert resolved.quality_status == TaxQualityStatus.ACTUAL_PURCHASE_TAX
    assert classify_tax_category(
        iva_rate_pct=resolved.iva_rate_pct, ila_rate_pct=resolved.ila_rate_pct
    ) == TaxCategory.ILA_BEER_WINE


def test_credit_note_reverses_components_separately():
    cel = commercial_line_economics(
        gross_sales=D("2000"),
        net_sales=D("1433.6918"),
        historical_net_unit_cost=D("1000"),
        quantity=D("1"),
        tax_profile=_profile("19", "20.5"),
    )
    rev = reverse_commercial_line(cel)
    assert rev.net_sales == quantize_money(-cel.net_sales)
    assert rev.iva_sales == quantize_money(-cel.iva_sales)  # type: ignore[operator]
    assert rev.ila_sales == quantize_money(-cel.ila_sales)  # type: ignore[operator]
    assert rev.gross_sales == quantize_money(-cel.gross_sales)
    assert rev.historical_net_cost == quantize_money(-cel.historical_net_cost)  # type: ignore[operator]
    assert rev.cost_iva == quantize_money(-cel.cost_iva)  # type: ignore[operator]
    assert rev.cost_ila == quantize_money(-cel.cost_ila)  # type: ignore[operator]
    assert rev.historical_gross_cost == quantize_money(-cel.historical_gross_cost)  # type: ignore[operator]
    assert rev.gross_commercial_profit == quantize_money(-cel.gross_commercial_profit)  # type: ignore[operator]


def test_net_metrics_remain_available_as_secondary():
    cel = commercial_line_economics(
        gross_sales=D("1190"),
        net_sales=D("1000"),
        historical_net_unit_cost=D("800"),
        quantity=D("1"),
        tax_profile=_profile("19", "0", category=TaxCategory.STANDARD_IVA_ONLY),
        cost_quality=CostQualityStatus.HISTORICAL_REAL,
    )
    assert cel.net_gross_profit == D("200.0000")
    assert cel.net_margin_pct == D("20.0000")
    assert cel.net_markup_pct == D("25.0000")
    assert cel.gross_commercial_profit == D("238.0000")  # 1190 - 952
    assert cel.historical_gross_cost == D("952.0000")
