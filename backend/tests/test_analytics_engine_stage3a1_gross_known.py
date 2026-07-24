"""Tests ajuste 3A.1 — costo bruto conocido + dos calidades."""

from __future__ import annotations

from backend.services.analytics.formulas import (
    commercial_line_economics,
    compute_commercial_margin_pct,
    compute_commercial_markup_pct,
    compute_commercial_profit,
)
from backend.services.analytics.money import D
from backend.services.analytics.tax_models import (
    GrossCostQuality,
    PurchaseTaxAmounts,
    TaxBreakdownQuality,
    TaxProfile,
    TaxQualityStatus,
    resolve_gross_cost,
)


def _hist_profile(iva: str = "19", ila: str = "20.5") -> TaxProfile:
    return TaxProfile(
        iva_rate_pct=D(iva),
        ila_rate_pct=D(ila),
        quality_status=TaxQualityStatus.HISTORICAL_TAX_PROFILE,
        tax_source="fixture_hist",
    )


def test_actual_purchase_gross_exact_split():
    bd = resolve_gross_cost(
        historical_net_cost=D("1000"),
        purchase=PurchaseTaxAmounts(
            net_cost=D("1000"),
            cost_bruto_erp=D("1395"),
            iva_amount=D("190"),
            ila_amount=D("205"),
        ),
    )
    assert bd.historical_gross_cost == D("1395.0000")
    assert bd.gross_cost_quality == GrossCostQuality.ACTUAL_PURCHASE_GROSS
    assert bd.tax_breakdown_quality == TaxBreakdownQuality.EXACT_IVA_ILA_SPLIT
    assert bd.cost_iva == D("190.0000")
    assert bd.cost_ila == D("205.0000")


def test_actual_purchase_gross_aggregated_other_taxes():
    bd = resolve_gross_cost(
        historical_net_cost=D("1000"),
        purchase=PurchaseTaxAmounts(
            net_cost=D("1000"),
            cost_bruto_erp=D("1395"),
            iva_amount=D("190"),
            other_taxes=D("205"),
        ),
    )
    assert bd.historical_gross_cost == D("1395.0000")
    assert bd.gross_cost_quality == GrossCostQuality.ACTUAL_PURCHASE_GROSS
    assert bd.tax_breakdown_quality == TaxBreakdownQuality.AGGREGATED_OTHER_TAXES
    assert bd.cost_ila is None  # no afirmar ILA
    assert bd.unclassified_tax_amount == D("205.0000")
    assert bd.total_tax_amount == D("395.0000")


def test_gross_calculable_without_ila_rate_when_bruto_known():
    bd = resolve_gross_cost(
        historical_net_cost=D("1000"),
        purchase=PurchaseTaxAmounts(
            net_cost=D("1000"),
            cost_bruto_erp=D("1395"),
        ),
        historical_profile=TaxProfile(
            iva_rate_pct=D("19"),
            ila_rate_pct=None,
            quality_status=TaxQualityStatus.HISTORICAL_TAX_PROFILE,
        ),
    )
    assert bd.historical_gross_cost == D("1395.0000")
    assert bd.gross_cost_quality == GrossCostQuality.ACTUAL_PURCHASE_GROSS
    assert bd.tax_breakdown_quality == TaxBreakdownQuality.PARTIAL_BREAKDOWN


def test_reconstructed_from_historical_rates():
    bd = resolve_gross_cost(
        historical_net_cost=D("1000"),
        historical_profile=_hist_profile("19", "20.5"),
    )
    assert bd.historical_gross_cost == D("1395.0000")
    assert bd.gross_cost_quality == GrossCostQuality.HISTORICAL_TAX_PROFILE
    assert bd.tax_breakdown_quality == TaxBreakdownQuality.RECONSTRUCTED_FROM_RATES


def test_missing_gross_cost():
    bd = resolve_gross_cost(historical_net_cost=D("1000"))
    assert bd.historical_gross_cost is None
    assert bd.gross_cost_quality == GrossCostQuality.MISSING_GROSS_COST
    assert bd.tax_breakdown_quality == TaxBreakdownQuality.MISSING_BREAKDOWN


def test_partial_breakdown_does_not_block_commercial_margin():
    cel = commercial_line_economics(
        gross_sales=D("2000"),
        net_sales=D("1680"),
        historical_net_unit_cost=D("1000"),
        quantity=D("1"),
        purchase=PurchaseTaxAmounts(
            net_cost=D("1000"),
            cost_bruto_erp=D("1395"),
            iva_amount=D("190"),
            other_taxes=D("205"),
        ),
    )
    assert cel.historical_gross_cost == D("1395.0000")
    assert cel.gross_commercial_profit == D("605.0000")
    assert cel.gross_commercial_margin_pct == D("30.25")
    assert cel.gross_commercial_markup_pct == D("43.37")
    assert cel.gross_cost_quality == GrossCostQuality.ACTUAL_PURCHASE_GROSS.value
    assert cel.tax_breakdown_quality == TaxBreakdownQuality.AGGREGATED_OTHER_TAXES.value


def test_case_net_1000_bruto_erp_1395_qualities():
    bd = resolve_gross_cost(
        historical_net_cost=D("1000"),
        purchase=PurchaseTaxAmounts(
            net_cost=D("1000"),
            cost_bruto_erp=D("1395"),
            iva_amount=D("190"),
            other_taxes=D("205"),
        ),
    )
    assert bd.historical_gross_cost == D("1395.0000")
    assert bd.gross_cost_quality == GrossCostQuality.ACTUAL_PURCHASE_GROSS
    assert bd.tax_breakdown_quality == TaxBreakdownQuality.AGGREGATED_OTHER_TAXES


def test_case_sale_2000_real_gross_cost_1395_metrics():
    profit = compute_commercial_profit(D("2000"), D("1395"))
    assert profit == D("605.0000")
    assert compute_commercial_margin_pct(D("2000"), profit) == D("30.25")
    assert compute_commercial_markup_pct(D("1395"), profit) == D("43.37")


def test_reconstructed_from_iva_and_other_taxes_without_bruto():
    bd = resolve_gross_cost(
        historical_net_cost=D("1000"),
        purchase=PurchaseTaxAmounts(
            net_cost=D("1000"),
            iva_amount=D("190"),
            other_taxes=D("205"),
        ),
    )
    assert bd.historical_gross_cost == D("1395.0000")
    assert bd.gross_cost_quality == GrossCostQuality.RECONSTRUCTED_FROM_ACTUAL_TAXES
    assert bd.tax_breakdown_quality == TaxBreakdownQuality.AGGREGATED_OTHER_TAXES


def test_conflicting_purchase_gross():
    bd = resolve_gross_cost(
        historical_net_cost=D("1000"),
        purchase=PurchaseTaxAmounts(
            net_cost=D("1000"),
            cost_bruto_erp=D("1395"),
            conflicting=True,
        ),
    )
    assert bd.historical_gross_cost is None
    assert bd.gross_cost_quality == GrossCostQuality.CONFLICTING_GROSS_COST
