"""Perfiles tributarios y resolución de costo bruto comercial.

Dos dimensiones de calidad (Etapa 3A.1):

A) gross_cost_quality — ¿tenemos el monto bruto?
B) tax_breakdown_quality — ¿podemos separar IVA / ILA?

``cost_bruto_erp`` de ``analytics.cost_reception_history`` es autoritativo
para el bruto aunque ``other_taxes`` no separe ILA con certeza.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from enum import Enum

from backend.services.analytics.money import ZERO, quantize_money


class TaxCategory(str, Enum):
    STANDARD_IVA_ONLY = "standard_iva_only"
    ILA_NON_ALCOHOLIC = "ila_non_alcoholic"
    ILA_HIGH_SUGAR = "ila_high_sugar"
    ILA_BEER_WINE = "ila_beer_wine"
    ILA_SPIRITS = "ila_spirits"
    EXEMPT_OR_ZERO = "exempt_or_zero"
    UNKNOWN = "unknown"


class TaxQualityStatus(str, Enum):
    """Legacy / agregado; preferir gross_cost_quality + tax_breakdown_quality."""

    ACTUAL_PURCHASE_TAX = "actual_purchase_tax"
    HISTORICAL_TAX_PROFILE = "historical_tax_profile"
    CURRENT_TAX_PROFILE_FALLBACK = "current_tax_profile_fallback"
    MISSING_TAX_PROFILE = "missing_tax_profile"
    CONFLICTING_TAX_PROFILE = "conflicting_tax_profile"


class TaxResolutionMethod(str, Enum):
    PURCHASE_GROSS_ERP = "purchase_gross_erp"
    PURCHASE_LINE_AMOUNTS = "purchase_line_amounts"
    HISTORICAL_PROFILE_RATES = "historical_profile_rates"
    CURRENT_PROFILE_RATES = "current_profile_rates"
    UNAVAILABLE = "unavailable"


class GrossCostQuality(str, Enum):
    ACTUAL_PURCHASE_GROSS = "actual_purchase_gross"
    RECONSTRUCTED_FROM_ACTUAL_TAXES = "reconstructed_from_actual_taxes"
    HISTORICAL_TAX_PROFILE = "historical_tax_profile"
    CURRENT_TAX_PROFILE_FALLBACK = "current_tax_profile_fallback"
    MISSING_GROSS_COST = "missing_gross_cost"
    CONFLICTING_GROSS_COST = "conflicting_gross_cost"


class TaxBreakdownQuality(str, Enum):
    EXACT_IVA_ILA_SPLIT = "exact_iva_ila_split"
    AGGREGATED_OTHER_TAXES = "aggregated_other_taxes"
    RECONSTRUCTED_FROM_RATES = "reconstructed_from_rates"
    PARTIAL_BREAKDOWN = "partial_breakdown"
    MISSING_BREAKDOWN = "missing_breakdown"
    CONFLICTING_BREAKDOWN = "conflicting_breakdown"


@dataclass(frozen=True, slots=True)
class TaxProfile:
    """Tasas en porcentaje (19 = 19%). None = desconocido (no asumir 0)."""

    iva_rate_pct: Decimal | None
    ila_rate_pct: Decimal | None
    tax_category: TaxCategory = TaxCategory.UNKNOWN
    tax_effective_date: date | None = None
    tax_source: str | None = None
    quality_status: TaxQualityStatus = TaxQualityStatus.MISSING_TAX_PROFILE


@dataclass(frozen=True, slots=True)
class PurchaseTaxAmounts:
    """Montos de compra/recepción (unitarios o de línea, misma base que net_cost)."""

    net_cost: Decimal
    cost_bruto_erp: Decimal | None = None
    iva_amount: Decimal | None = None
    other_taxes: Decimal | None = None
    ila_amount: Decimal | None = None  # solo si la fuente separa ILA con certeza
    conflicting: bool = False


@dataclass(frozen=True, slots=True)
class CostTaxBreakdown:
    historical_net_cost: Decimal
    cost_iva: Decimal | None
    cost_ila: Decimal | None
    historical_gross_cost: Decimal | None
    iva_rate_pct: Decimal | None
    ila_rate_pct: Decimal | None
    tax_category: TaxCategory
    tax_resolution_method: TaxResolutionMethod
    tax_quality_status: TaxQualityStatus
    tax_source: str | None = None
    gross_cost_quality: GrossCostQuality = GrossCostQuality.MISSING_GROSS_COST
    tax_breakdown_quality: TaxBreakdownQuality = TaxBreakdownQuality.MISSING_BREAKDOWN
    total_tax_amount: Decimal | None = None
    unclassified_tax_amount: Decimal | None = None


def classify_tax_category(
    *,
    iva_rate_pct: Decimal | None,
    ila_rate_pct: Decimal | None,
) -> TaxCategory:
    """Clasificación heurística por tasas conocidas (solo etiquetado; no fija tasas)."""
    if iva_rate_pct is None and ila_rate_pct is None:
        return TaxCategory.UNKNOWN
    iva = iva_rate_pct if iva_rate_pct is not None else Decimal("0")
    ila = ila_rate_pct if ila_rate_pct is not None else Decimal("0")
    if iva == 0 and ila == 0:
        return TaxCategory.EXEMPT_OR_ZERO
    if ila == 0:
        return TaxCategory.STANDARD_IVA_ONLY
    if ila == Decimal("10") or ila == Decimal("10.0"):
        return TaxCategory.ILA_NON_ALCOHOLIC
    if ila == Decimal("18") or ila == Decimal("18.0"):
        return TaxCategory.ILA_HIGH_SUGAR
    if ila == Decimal("20.5") or ila == Decimal("20.50"):
        return TaxCategory.ILA_BEER_WINE
    if ila == Decimal("31.5") or ila == Decimal("31.50"):
        return TaxCategory.ILA_SPIRITS
    return TaxCategory.UNKNOWN


def resolve_tax_profile(
    *,
    purchase_iva_amount: Decimal | None = None,
    purchase_ila_amount: Decimal | None = None,
    purchase_net_cost: Decimal | None = None,
    historical_profile: TaxProfile | None = None,
    current_profile: TaxProfile | None = None,
) -> TaxProfile:
    """Prioridad de tasas: impuestos de compra exactos → histórico → actual → missing."""
    if (
        purchase_net_cost is not None
        and purchase_net_cost > 0
        and purchase_iva_amount is not None
        and purchase_ila_amount is not None
    ):
        iva_pct = (purchase_iva_amount / purchase_net_cost) * Decimal("100")
        ila_pct = (purchase_ila_amount / purchase_net_cost) * Decimal("100")
        return TaxProfile(
            iva_rate_pct=iva_pct,
            ila_rate_pct=ila_pct,
            tax_category=classify_tax_category(
                iva_rate_pct=iva_pct, ila_rate_pct=ila_pct
            ),
            tax_source="purchase_line_amounts",
            quality_status=TaxQualityStatus.ACTUAL_PURCHASE_TAX,
        )

    if (
        historical_profile is not None
        and historical_profile.iva_rate_pct is not None
        and historical_profile.ila_rate_pct is not None
    ):
        return TaxProfile(
            iva_rate_pct=historical_profile.iva_rate_pct,
            ila_rate_pct=historical_profile.ila_rate_pct,
            tax_category=historical_profile.tax_category
            if historical_profile.tax_category != TaxCategory.UNKNOWN
            else classify_tax_category(
                iva_rate_pct=historical_profile.iva_rate_pct,
                ila_rate_pct=historical_profile.ila_rate_pct,
            ),
            tax_effective_date=historical_profile.tax_effective_date,
            tax_source=historical_profile.tax_source or "historical_tax_profile",
            quality_status=TaxQualityStatus.HISTORICAL_TAX_PROFILE,
        )

    if (
        current_profile is not None
        and current_profile.iva_rate_pct is not None
        and current_profile.ila_rate_pct is not None
    ):
        return TaxProfile(
            iva_rate_pct=current_profile.iva_rate_pct,
            ila_rate_pct=current_profile.ila_rate_pct,
            tax_category=current_profile.tax_category
            if current_profile.tax_category != TaxCategory.UNKNOWN
            else classify_tax_category(
                iva_rate_pct=current_profile.iva_rate_pct,
                ila_rate_pct=current_profile.ila_rate_pct,
            ),
            tax_effective_date=current_profile.tax_effective_date,
            tax_source=current_profile.tax_source or "current_tax_profile",
            quality_status=TaxQualityStatus.CURRENT_TAX_PROFILE_FALLBACK,
        )

    return TaxProfile(
        iva_rate_pct=None,
        ila_rate_pct=None,
        tax_category=TaxCategory.UNKNOWN,
        tax_source=None,
        quality_status=TaxQualityStatus.MISSING_TAX_PROFILE,
    )


def _missing_breakdown(
    net: Decimal,
    *,
    gross_quality: GrossCostQuality = GrossCostQuality.MISSING_GROSS_COST,
    breakdown_quality: TaxBreakdownQuality = TaxBreakdownQuality.MISSING_BREAKDOWN,
    tax_quality: TaxQualityStatus = TaxQualityStatus.MISSING_TAX_PROFILE,
    method: TaxResolutionMethod = TaxResolutionMethod.UNAVAILABLE,
    source: str | None = None,
    iva_rate: Decimal | None = None,
    ila_rate: Decimal | None = None,
) -> CostTaxBreakdown:
    return CostTaxBreakdown(
        historical_net_cost=net,
        cost_iva=None,
        cost_ila=None,
        historical_gross_cost=None,
        iva_rate_pct=iva_rate,
        ila_rate_pct=ila_rate,
        tax_category=TaxCategory.UNKNOWN,
        tax_resolution_method=method,
        tax_quality_status=tax_quality,
        tax_source=source,
        gross_cost_quality=gross_quality,
        tax_breakdown_quality=breakdown_quality,
        total_tax_amount=None,
        unclassified_tax_amount=None,
    )


def _from_rates(
    net: Decimal,
    profile: TaxProfile,
    *,
    gross_quality: GrossCostQuality,
) -> CostTaxBreakdown:
    if profile.iva_rate_pct is None or profile.ila_rate_pct is None:
        return _missing_breakdown(
            net,
            iva_rate=profile.iva_rate_pct,
            ila_rate=profile.ila_rate_pct,
            source=profile.tax_source,
        )
    iva = quantize_money(net * profile.iva_rate_pct / Decimal("100"))
    ila = quantize_money(net * profile.ila_rate_pct / Decimal("100"))
    gross = quantize_money(net + iva + ila)
    method = (
        TaxResolutionMethod.HISTORICAL_PROFILE_RATES
        if gross_quality == GrossCostQuality.HISTORICAL_TAX_PROFILE
        else TaxResolutionMethod.CURRENT_PROFILE_RATES
    )
    legacy = (
        TaxQualityStatus.HISTORICAL_TAX_PROFILE
        if gross_quality == GrossCostQuality.HISTORICAL_TAX_PROFILE
        else TaxQualityStatus.CURRENT_TAX_PROFILE_FALLBACK
    )
    category = (
        profile.tax_category
        if profile.tax_category != TaxCategory.UNKNOWN
        else classify_tax_category(
            iva_rate_pct=profile.iva_rate_pct,
            ila_rate_pct=profile.ila_rate_pct,
        )
    )
    return CostTaxBreakdown(
        historical_net_cost=net,
        cost_iva=iva,
        cost_ila=ila,
        historical_gross_cost=gross,
        iva_rate_pct=profile.iva_rate_pct,
        ila_rate_pct=profile.ila_rate_pct,
        tax_category=category,
        tax_resolution_method=method,
        tax_quality_status=legacy,
        tax_source=profile.tax_source,
        gross_cost_quality=gross_quality,
        tax_breakdown_quality=TaxBreakdownQuality.RECONSTRUCTED_FROM_RATES,
        total_tax_amount=quantize_money(iva + ila),
        unclassified_tax_amount=None,
    )


def resolve_gross_cost(
    *,
    historical_net_cost: Decimal,
    purchase: PurchaseTaxAmounts | None = None,
    historical_profile: TaxProfile | None = None,
    current_profile: TaxProfile | None = None,
) -> CostTaxBreakdown:
    """Prioridad bruto:

    1. cost_bruto_erp real
    2. net + iva_amount + other_taxes reales
    3. net + perfil histórico (ambas tasas)
    4. net + perfil actual (ambas tasas)
    5. missing
    """
    net = quantize_money(historical_net_cost)

    if purchase is not None and purchase.conflicting:
        return _missing_breakdown(
            net,
            gross_quality=GrossCostQuality.CONFLICTING_GROSS_COST,
            breakdown_quality=TaxBreakdownQuality.CONFLICTING_BREAKDOWN,
            tax_quality=TaxQualityStatus.CONFLICTING_TAX_PROFILE,
            source="purchase_conflict",
        )

    if purchase is not None:
        bruto = purchase.cost_bruto_erp
        if bruto is not None and bruto > ZERO:
            gross = quantize_money(bruto)
            total_tax = quantize_money(gross - net)
            iva = (
                quantize_money(purchase.iva_amount)
                if purchase.iva_amount is not None
                else None
            )
            if purchase.ila_amount is not None and iva is not None:
                ila = quantize_money(purchase.ila_amount)
                return CostTaxBreakdown(
                    historical_net_cost=net,
                    cost_iva=iva,
                    cost_ila=ila,
                    historical_gross_cost=gross,
                    iva_rate_pct=None,
                    ila_rate_pct=None,
                    tax_category=TaxCategory.UNKNOWN,
                    tax_resolution_method=TaxResolutionMethod.PURCHASE_GROSS_ERP,
                    tax_quality_status=TaxQualityStatus.ACTUAL_PURCHASE_TAX,
                    tax_source="cost_bruto_erp+iva+ila",
                    gross_cost_quality=GrossCostQuality.ACTUAL_PURCHASE_GROSS,
                    tax_breakdown_quality=TaxBreakdownQuality.EXACT_IVA_ILA_SPLIT,
                    total_tax_amount=total_tax,
                    unclassified_tax_amount=None,
                )
            unclassified: Decimal | None = None
            breakdown = TaxBreakdownQuality.PARTIAL_BREAKDOWN
            if iva is not None:
                unclassified = quantize_money(total_tax - iva)
                breakdown = TaxBreakdownQuality.AGGREGATED_OTHER_TAXES
            elif purchase.other_taxes is not None:
                unclassified = quantize_money(purchase.other_taxes)
                breakdown = TaxBreakdownQuality.AGGREGATED_OTHER_TAXES
            return CostTaxBreakdown(
                historical_net_cost=net,
                cost_iva=iva,
                cost_ila=None,  # no afirmar ILA
                historical_gross_cost=gross,
                iva_rate_pct=None,
                ila_rate_pct=None,
                tax_category=TaxCategory.UNKNOWN,
                tax_resolution_method=TaxResolutionMethod.PURCHASE_GROSS_ERP,
                tax_quality_status=TaxQualityStatus.ACTUAL_PURCHASE_TAX,
                tax_source="cost_bruto_erp",
                gross_cost_quality=GrossCostQuality.ACTUAL_PURCHASE_GROSS,
                tax_breakdown_quality=breakdown,
                total_tax_amount=total_tax,
                unclassified_tax_amount=unclassified,
            )

        # Sin bruto ERP: reconstruir con impuestos reales de compra.
        if purchase.iva_amount is not None and purchase.other_taxes is not None:
            iva = quantize_money(purchase.iva_amount)
            other = quantize_money(purchase.other_taxes)
            gross = quantize_money(net + iva + other)
            return CostTaxBreakdown(
                historical_net_cost=net,
                cost_iva=iva,
                cost_ila=None,
                historical_gross_cost=gross,
                iva_rate_pct=None,
                ila_rate_pct=None,
                tax_category=TaxCategory.UNKNOWN,
                tax_resolution_method=TaxResolutionMethod.PURCHASE_LINE_AMOUNTS,
                tax_quality_status=TaxQualityStatus.ACTUAL_PURCHASE_TAX,
                tax_source="iva_amount+other_taxes",
                gross_cost_quality=GrossCostQuality.RECONSTRUCTED_FROM_ACTUAL_TAXES,
                tax_breakdown_quality=TaxBreakdownQuality.AGGREGATED_OTHER_TAXES,
                total_tax_amount=quantize_money(iva + other),
                unclassified_tax_amount=other,
            )
        if (
            purchase.iva_amount is not None
            and purchase.ila_amount is not None
        ):
            iva = quantize_money(purchase.iva_amount)
            ila = quantize_money(purchase.ila_amount)
            gross = quantize_money(net + iva + ila)
            return CostTaxBreakdown(
                historical_net_cost=net,
                cost_iva=iva,
                cost_ila=ila,
                historical_gross_cost=gross,
                iva_rate_pct=None,
                ila_rate_pct=None,
                tax_category=TaxCategory.UNKNOWN,
                tax_resolution_method=TaxResolutionMethod.PURCHASE_LINE_AMOUNTS,
                tax_quality_status=TaxQualityStatus.ACTUAL_PURCHASE_TAX,
                tax_source="iva_amount+ila_amount",
                gross_cost_quality=GrossCostQuality.RECONSTRUCTED_FROM_ACTUAL_TAXES,
                tax_breakdown_quality=TaxBreakdownQuality.EXACT_IVA_ILA_SPLIT,
                total_tax_amount=quantize_money(iva + ila),
                unclassified_tax_amount=None,
            )

    hist = resolve_tax_profile(historical_profile=historical_profile)
    if hist.quality_status == TaxQualityStatus.HISTORICAL_TAX_PROFILE:
        return _from_rates(
            net, hist, gross_quality=GrossCostQuality.HISTORICAL_TAX_PROFILE
        )

    cur = resolve_tax_profile(current_profile=current_profile)
    if cur.quality_status == TaxQualityStatus.CURRENT_TAX_PROFILE_FALLBACK:
        return _from_rates(
            net, cur, gross_quality=GrossCostQuality.CURRENT_TAX_PROFILE_FALLBACK
        )

    return _missing_breakdown(net)


def apply_taxes_to_net_cost(
    historical_net_cost: Decimal,
    profile: TaxProfile,
) -> CostTaxBreakdown:
    """Compat: reconstruye bruto solo desde tasas (sin cost_bruto_erp)."""
    if profile.quality_status == TaxQualityStatus.CONFLICTING_TAX_PROFILE:
        return _missing_breakdown(
            quantize_money(historical_net_cost),
            gross_quality=GrossCostQuality.CONFLICTING_GROSS_COST,
            breakdown_quality=TaxBreakdownQuality.CONFLICTING_BREAKDOWN,
            tax_quality=TaxQualityStatus.CONFLICTING_TAX_PROFILE,
            iva_rate=profile.iva_rate_pct,
            ila_rate=profile.ila_rate_pct,
            source=profile.tax_source,
        )
    if profile.quality_status == TaxQualityStatus.MISSING_TAX_PROFILE:
        return _missing_breakdown(
            quantize_money(historical_net_cost),
            source=profile.tax_source,
        )
    if profile.iva_rate_pct is None or profile.ila_rate_pct is None:
        return _missing_breakdown(
            quantize_money(historical_net_cost),
            iva_rate=profile.iva_rate_pct,
            ila_rate=profile.ila_rate_pct,
            source=profile.tax_source,
        )
    gross_q = (
        GrossCostQuality.HISTORICAL_TAX_PROFILE
        if profile.quality_status == TaxQualityStatus.HISTORICAL_TAX_PROFILE
        else GrossCostQuality.CURRENT_TAX_PROFILE_FALLBACK
        if profile.quality_status == TaxQualityStatus.CURRENT_TAX_PROFILE_FALLBACK
        else GrossCostQuality.HISTORICAL_TAX_PROFILE
    )
    if profile.quality_status == TaxQualityStatus.ACTUAL_PURCHASE_TAX:
        # Tasas derivadas de montos de compra → tratar como reconstrucción por tasas.
        net = quantize_money(historical_net_cost)
        iva = quantize_money(net * profile.iva_rate_pct / Decimal("100"))
        ila = quantize_money(net * profile.ila_rate_pct / Decimal("100"))
        gross = quantize_money(net + iva + ila)
        return CostTaxBreakdown(
            historical_net_cost=net,
            cost_iva=iva,
            cost_ila=ila,
            historical_gross_cost=gross,
            iva_rate_pct=profile.iva_rate_pct,
            ila_rate_pct=profile.ila_rate_pct,
            tax_category=classify_tax_category(
                iva_rate_pct=profile.iva_rate_pct,
                ila_rate_pct=profile.ila_rate_pct,
            ),
            tax_resolution_method=TaxResolutionMethod.PURCHASE_LINE_AMOUNTS,
            tax_quality_status=TaxQualityStatus.ACTUAL_PURCHASE_TAX,
            tax_source=profile.tax_source,
            gross_cost_quality=GrossCostQuality.RECONSTRUCTED_FROM_ACTUAL_TAXES,
            tax_breakdown_quality=TaxBreakdownQuality.RECONSTRUCTED_FROM_RATES,
            total_tax_amount=quantize_money(iva + ila),
            unclassified_tax_amount=None,
        )
    return _from_rates(
        quantize_money(historical_net_cost),
        profile,
        gross_quality=gross_q,
    )
