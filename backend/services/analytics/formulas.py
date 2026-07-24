"""Fórmulas financieras canónicas.

Vista predeterminada Quillotana = COMERCIAL BRUTA.
Vista secundaria = NETA.

Costo bruto:
  1) cost_bruto_erp autoritativo (aunque desglose ILA sea agregado)
  2) net + iva_amount + other_taxes
  3) net + tasas (aditivo; NO compuesto)
"""

from __future__ import annotations

from decimal import Decimal

from backend.services.analytics.money import (
    ZERO,
    quantize_commercial_pct,
    quantize_money,
    quantize_pct,
)
from backend.services.analytics.schemas import (
    CommercialLineEconomics,
    CostQualityStatus,
    LineEconomics,
)
from backend.services.analytics.tax_models import (
    CostTaxBreakdown,
    GrossCostQuality,
    PurchaseTaxAmounts,
    TaxBreakdownQuality,
    TaxCategory,
    TaxProfile,
    TaxQualityStatus,
    TaxResolutionMethod,
    apply_taxes_to_net_cost,
    resolve_gross_cost,
)


def compute_gross_profit(
    net_sales: Decimal,
    historical_cost: Decimal | None,
) -> Decimal | None:
    """Utilidad neta = venta neta − costo neto histórico. None si falta costo."""
    if historical_cost is None:
        return None
    return quantize_money(net_sales - historical_cost)


def compute_margin_pct(
    net_sales: Decimal,
    gross_profit: Decimal | None,
) -> Decimal | None:
    """Margen neto = utilidad_neta / venta_neta × 100."""
    if gross_profit is None:
        return None
    if net_sales == ZERO:
        return None
    return quantize_pct((gross_profit / net_sales) * Decimal("100"))


def compute_markup_pct(
    historical_cost: Decimal | None,
    gross_profit: Decimal | None,
) -> Decimal | None:
    """Markup neto = utilidad_neta / costo_neto × 100."""
    if gross_profit is None or historical_cost is None:
        return None
    if historical_cost <= ZERO:
        return None
    return quantize_pct((gross_profit / historical_cost) * Decimal("100"))


def line_economics(
    *,
    net_sales: Decimal,
    historical_cost: Decimal | None,
    cost_quality: CostQualityStatus = CostQualityStatus.HISTORICAL_REAL,
) -> LineEconomics:
    """Economía NETA (vista secundaria). Compatibilidad Etapa 1."""
    effective_cost: Decimal | None
    if cost_quality == CostQualityStatus.MISSING_COST:
        effective_cost = None
    else:
        effective_cost = historical_cost

    profit = compute_gross_profit(net_sales, effective_cost)
    return LineEconomics(
        net_sales=quantize_money(net_sales),
        historical_cost=(
            None if effective_cost is None else quantize_money(effective_cost)
        ),
        gross_profit=profit,
        gross_margin_pct=compute_margin_pct(net_sales, profit),
        markup_pct=compute_markup_pct(effective_cost, profit),
        cost_quality=cost_quality,
    )


def apply_taxes_to_net_sales(
    net_sales: Decimal,
    profile: TaxProfile,
) -> tuple[Decimal | None, Decimal | None, Decimal | None]:
    """Desglose IVA/ILA de venta sobre neto (aditivo). None si perfil incompleto."""
    if (
        profile.quality_status
        in (
            TaxQualityStatus.MISSING_TAX_PROFILE,
            TaxQualityStatus.CONFLICTING_TAX_PROFILE,
        )
        or profile.iva_rate_pct is None
        or profile.ila_rate_pct is None
    ):
        return None, None, None
    net = quantize_money(net_sales)
    iva = quantize_money(net * profile.iva_rate_pct / Decimal("100"))
    ila = quantize_money(net * profile.ila_rate_pct / Decimal("100"))
    return iva, ila, quantize_money(net + iva + ila)


def compute_commercial_profit(
    gross_sales: Decimal,
    historical_gross_cost: Decimal | None,
) -> Decimal | None:
    if historical_gross_cost is None:
        return None
    return quantize_money(gross_sales - historical_gross_cost)


def compute_commercial_margin_pct(
    gross_sales: Decimal,
    commercial_profit: Decimal | None,
) -> Decimal | None:
    if commercial_profit is None or gross_sales == ZERO:
        return None
    return quantize_commercial_pct((commercial_profit / gross_sales) * Decimal("100"))


def compute_commercial_markup_pct(
    historical_gross_cost: Decimal | None,
    commercial_profit: Decimal | None,
) -> Decimal | None:
    if commercial_profit is None or historical_gross_cost is None:
        return None
    if historical_gross_cost <= ZERO:
        return None
    return quantize_commercial_pct(
        (commercial_profit / historical_gross_cost) * Decimal("100")
    )


def commercial_line_economics(
    *,
    gross_sales: Decimal,
    net_sales: Decimal,
    historical_net_unit_cost: Decimal | None,
    quantity: Decimal,
    tax_profile: TaxProfile | None = None,
    purchase: PurchaseTaxAmounts | None = None,
    historical_tax_profile: TaxProfile | None = None,
    current_tax_profile: TaxProfile | None = None,
    cost_quality: CostQualityStatus = CostQualityStatus.HISTORICAL_REAL,
    prefer_bsale_gross_sales: bool = True,
    cost_breakdown: CostTaxBreakdown | None = None,
) -> CommercialLineEconomics:
    """Economía completa: métricas brutas (default UI) + netas (secundarias).

    ``gross_sales`` = ``line.total_amount`` autoritativo cuando
    ``prefer_bsale_gross_sales``; el desglose IVA/ILA de venta es trazabilidad
    adicional, no requisito para margen bruto.
    """
    qty = quantity
    net = quantize_money(net_sales)
    gross = quantize_money(gross_sales)

    profile = tax_profile or TaxProfile(
        iva_rate_pct=None,
        ila_rate_pct=None,
        quality_status=TaxQualityStatus.MISSING_TAX_PROFILE,
    )
    iva_sales, ila_sales, rebuilt_gross = apply_taxes_to_net_sales(net, profile)
    if not prefer_bsale_gross_sales and rebuilt_gross is not None:
        gross = rebuilt_gross

    net_eco = line_economics(
        net_sales=net,
        historical_cost=(
            None
            if historical_net_unit_cost is None
            or cost_quality == CostQualityStatus.MISSING_COST
            else quantize_money(qty * historical_net_unit_cost)
        ),
        cost_quality=cost_quality,
    )

    unit_net = (
        None
        if historical_net_unit_cost is None
        or cost_quality == CostQualityStatus.MISSING_COST
        else quantize_money(historical_net_unit_cost)
    )
    net_cost = net_eco.historical_cost

    if cost_breakdown is not None:
        tax_bd = cost_breakdown
    elif net_cost is None:
        tax_bd = CostTaxBreakdown(
            historical_net_cost=ZERO,
            cost_iva=None,
            cost_ila=None,
            historical_gross_cost=None,
            iva_rate_pct=profile.iva_rate_pct,
            ila_rate_pct=profile.ila_rate_pct,
            tax_category=TaxCategory.UNKNOWN,
            tax_resolution_method=TaxResolutionMethod.UNAVAILABLE,
            tax_quality_status=TaxQualityStatus.MISSING_TAX_PROFILE,
            tax_source=profile.tax_source,
            gross_cost_quality=GrossCostQuality.MISSING_GROSS_COST,
            tax_breakdown_quality=TaxBreakdownQuality.MISSING_BREAKDOWN,
        )
    else:
        # purchase.net_cost debe alinearse con el neto de línea resuelto.
        purchase_aligned = purchase
        if purchase is not None and purchase.net_cost != net_cost:
            purchase_aligned = PurchaseTaxAmounts(
                net_cost=net_cost,
                cost_bruto_erp=(
                    None
                    if purchase.cost_bruto_erp is None or historical_net_unit_cost is None
                    else quantize_money(
                        purchase.cost_bruto_erp
                        * (net_cost / purchase.net_cost)
                    )
                    if purchase.net_cost > ZERO
                    else purchase.cost_bruto_erp
                ),
                iva_amount=(
                    None
                    if purchase.iva_amount is None or purchase.net_cost <= ZERO
                    else quantize_money(
                        purchase.iva_amount * (net_cost / purchase.net_cost)
                    )
                ),
                other_taxes=(
                    None
                    if purchase.other_taxes is None or purchase.net_cost <= ZERO
                    else quantize_money(
                        purchase.other_taxes * (net_cost / purchase.net_cost)
                    )
                ),
                ila_amount=(
                    None
                    if purchase.ila_amount is None or purchase.net_cost <= ZERO
                    else quantize_money(
                        purchase.ila_amount * (net_cost / purchase.net_cost)
                    )
                ),
                conflicting=purchase.conflicting,
            )
        elif purchase is not None:
            purchase_aligned = PurchaseTaxAmounts(
                net_cost=net_cost,
                cost_bruto_erp=purchase.cost_bruto_erp,
                iva_amount=purchase.iva_amount,
                other_taxes=purchase.other_taxes,
                ila_amount=purchase.ila_amount,
                conflicting=purchase.conflicting,
            )
        tax_bd = resolve_gross_cost(
            historical_net_cost=net_cost,
            purchase=purchase_aligned,
            historical_profile=historical_tax_profile or profile,
            current_profile=current_tax_profile,
        )

    if tax_bd.historical_gross_cost is not None and qty != ZERO:
        unit_gross = quantize_money(tax_bd.historical_gross_cost / qty)
    else:
        unit_gross = None

    commercial_profit = compute_commercial_profit(gross, tax_bd.historical_gross_cost)
    return CommercialLineEconomics(
        net_sales=net,
        iva_sales=iva_sales,
        ila_sales=ila_sales,
        gross_sales=gross,
        historical_net_unit_cost=unit_net,
        historical_net_cost=net_cost,
        cost_iva=tax_bd.cost_iva,
        cost_ila=tax_bd.cost_ila,
        historical_gross_unit_cost=unit_gross,
        historical_gross_cost=tax_bd.historical_gross_cost,
        net_gross_profit=net_eco.gross_profit,
        gross_commercial_profit=commercial_profit,
        net_margin_pct=net_eco.gross_margin_pct,
        gross_commercial_margin_pct=compute_commercial_margin_pct(
            gross, commercial_profit
        ),
        net_markup_pct=net_eco.markup_pct,
        gross_commercial_markup_pct=compute_commercial_markup_pct(
            tax_bd.historical_gross_cost, commercial_profit
        ),
        cost_quality=cost_quality,
        tax_resolution_method=tax_bd.tax_resolution_method.value,
        tax_quality_status=tax_bd.tax_quality_status.value,
        tax_category=tax_bd.tax_category.value,
        tax_source=tax_bd.tax_source,
        gross_cost_quality=tax_bd.gross_cost_quality.value,
        tax_breakdown_quality=tax_bd.tax_breakdown_quality.value,
        total_tax_amount=tax_bd.total_tax_amount,
        unclassified_tax_amount=tax_bd.unclassified_tax_amount,
    )


def reverse_commercial_line(
    line: CommercialLineEconomics,
) -> CommercialLineEconomics:
    """Nota de crédito: revierte montos; desglose tributario es trazabilidad."""

    def _neg(v: Decimal | None) -> Decimal | None:
        return None if v is None else quantize_money(-v)

    return CommercialLineEconomics(
        net_sales=quantize_money(-line.net_sales),
        iva_sales=_neg(line.iva_sales),
        ila_sales=_neg(line.ila_sales),
        gross_sales=quantize_money(-line.gross_sales),
        historical_net_unit_cost=line.historical_net_unit_cost,
        historical_net_cost=_neg(line.historical_net_cost),
        cost_iva=_neg(line.cost_iva),
        cost_ila=_neg(line.cost_ila),
        historical_gross_unit_cost=line.historical_gross_unit_cost,
        historical_gross_cost=_neg(line.historical_gross_cost),
        net_gross_profit=_neg(line.net_gross_profit),
        gross_commercial_profit=_neg(line.gross_commercial_profit),
        net_margin_pct=line.net_margin_pct,
        gross_commercial_margin_pct=line.gross_commercial_margin_pct,
        net_markup_pct=line.net_markup_pct,
        gross_commercial_markup_pct=line.gross_commercial_markup_pct,
        cost_quality=line.cost_quality,
        tax_resolution_method=line.tax_resolution_method,
        tax_quality_status=line.tax_quality_status,
        tax_category=line.tax_category,
        tax_source=line.tax_source,
        gross_cost_quality=line.gross_cost_quality,
        tax_breakdown_quality=line.tax_breakdown_quality,
        total_tax_amount=_neg(line.total_tax_amount),
        unclassified_tax_amount=_neg(line.unclassified_tax_amount),
    )


# Re-export para callers que importaban desde formulas.
__all__ = [
    "apply_taxes_to_net_cost",
    "apply_taxes_to_net_sales",
    "commercial_line_economics",
    "compute_commercial_margin_pct",
    "compute_commercial_markup_pct",
    "compute_commercial_profit",
    "compute_gross_profit",
    "compute_margin_pct",
    "compute_markup_pct",
    "line_economics",
    "resolve_gross_cost",
    "reverse_commercial_line",
]
