"""Control de precios por lista: fórmulas Decimal (sin ventas).

actual_markup_pct = (price - cost) / cost * 100  → cumplimiento
gross_margin_pct  = (price - cost) / price * 100 → informativo
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal
from enum import Enum

from backend.services.analytics.money import (
    ZERO,
    quantize_commercial_pct,
    quantize_money,
)


class PriceControlStatus(str, Enum):
    BELOW_MINIMUM = "below_minimum"
    WITHIN_POLICY = "within_policy"
    ABOVE_MAXIMUM = "above_maximum"
    MISSING_RULE = "missing_rule"
    MISSING_COST = "missing_cost"
    MISSING_PRICE = "missing_price"
    STALE_COST = "stale_cost"
    CONFLICTING_COST = "conflicting_cost"
    COST_OUTLIER = "cost_outlier"


def actual_markup_pct(
    gross_price: Decimal | None,
    reference_gross_cost: Decimal | None,
) -> Decimal | None:
    if gross_price is None or reference_gross_cost is None:
        return None
    if reference_gross_cost <= ZERO:
        return None
    return quantize_commercial_pct(
        (gross_price - reference_gross_cost) / reference_gross_cost * Decimal("100")
    )


def gross_margin_pct(
    gross_price: Decimal | None,
    reference_gross_cost: Decimal | None,
) -> Decimal | None:
    if gross_price is None or reference_gross_cost is None:
        return None
    if gross_price <= ZERO:
        return None
    return quantize_commercial_pct(
        (gross_price - reference_gross_cost) / gross_price * Decimal("100")
    )


def recommended_gross_price(
    reference_gross_cost: Decimal | None,
    markup_pct: Decimal | None,
) -> Decimal | None:
    if reference_gross_cost is None or markup_pct is None:
        return None
    if reference_gross_cost <= ZERO:
        return None
    # Redondeo monetario a peso entero (HALF_UP vía quantize 1)
    raw = reference_gross_cost * (Decimal("1") + markup_pct / Decimal("100"))
    return raw.quantize(Decimal("1"), rounding=ROUND_HALF_UP)


def policy_compliance_status(
    *,
    markup_pct: Decimal | None,
    min_markup_pct: Decimal | None,
    max_markup_pct: Decimal | None,
    has_rule: bool,
) -> PriceControlStatus | None:
    """Cumplimiento puro por recargo; None si no se puede evaluar."""
    if not has_rule or min_markup_pct is None:
        return PriceControlStatus.MISSING_RULE
    if markup_pct is None:
        return None
    if markup_pct < min_markup_pct:
        return PriceControlStatus.BELOW_MINIMUM
    if max_markup_pct is not None and max_markup_pct > ZERO and markup_pct > max_markup_pct:
        return PriceControlStatus.ABOVE_MAXIMUM
    return PriceControlStatus.WITHIN_POLICY


def resolve_display_status(
    *,
    gross_price: Decimal | None,
    reference_gross_cost: Decimal | None,
    min_markup_pct: Decimal | None,
    max_markup_pct: Decimal | None,
    has_rule: bool,
    is_stale: bool = False,
    is_conflicting: bool = False,
    is_outlier: bool = False,
) -> tuple[PriceControlStatus, PriceControlStatus | None]:
    """Retorna (estado UI prioritario, cumplimiento de política si aplica).

    Prioridad UI:
    1 missing_price 2 missing_cost 3 conflicting 4 outlier
    5 missing_rule 6 stale 7 below/within/above
    """
    price_ok = gross_price is not None and gross_price > ZERO
    cost_ok = reference_gross_cost is not None and reference_gross_cost > ZERO

    if not price_ok:
        return PriceControlStatus.MISSING_PRICE, None
    if not cost_ok:
        return PriceControlStatus.MISSING_COST, None
    if is_conflicting:
        return PriceControlStatus.CONFLICTING_COST, None

    markup = actual_markup_pct(gross_price, reference_gross_cost)
    compliance = policy_compliance_status(
        markup_pct=markup,
        min_markup_pct=min_markup_pct,
        max_markup_pct=max_markup_pct,
        has_rule=has_rule,
    )

    if is_outlier:
        return PriceControlStatus.COST_OUTLIER, compliance
    if compliance == PriceControlStatus.MISSING_RULE:
        return PriceControlStatus.MISSING_RULE, compliance
    if is_stale:
        return PriceControlStatus.STALE_COST, compliance
    assert compliance is not None
    return compliance, compliance


@dataclass(frozen=True, slots=True)
class PriceListControlMetrics:
    gross_price: Decimal | None
    reference_gross_cost: Decimal | None
    actual_markup_pct: Decimal | None
    gross_margin_pct: Decimal | None
    min_markup_pct: Decimal | None
    max_markup_pct: Decimal | None
    minimum_recommended_gross_price: Decimal | None
    maximum_recommended_gross_price: Decimal | None
    price_adjustment_to_minimum: Decimal | None
    price_diff_vs_cost: Decimal | None
    status: PriceControlStatus
    policy_compliance: PriceControlStatus | None


def compute_price_list_control_row(
    *,
    gross_price: Decimal | None,
    reference_gross_cost: Decimal | None,
    min_markup_pct: Decimal | None,
    max_markup_pct: Decimal | None,
    has_rule: bool,
    is_stale: bool = False,
    is_conflicting: bool = False,
    is_outlier: bool = False,
) -> PriceListControlMetrics:
    markup = actual_markup_pct(gross_price, reference_gross_cost)
    margin = gross_margin_pct(gross_price, reference_gross_cost)
    min_rec = recommended_gross_price(reference_gross_cost, min_markup_pct)
    max_rec = recommended_gross_price(reference_gross_cost, max_markup_pct)
    adj = None
    if min_rec is not None and gross_price is not None:
        adj = quantize_money(min_rec - gross_price)
    diff = None
    if gross_price is not None and reference_gross_cost is not None:
        diff = quantize_money(gross_price - reference_gross_cost)
    status, compliance = resolve_display_status(
        gross_price=gross_price,
        reference_gross_cost=reference_gross_cost,
        min_markup_pct=min_markup_pct,
        max_markup_pct=max_markup_pct,
        has_rule=has_rule,
        is_stale=is_stale,
        is_conflicting=is_conflicting,
        is_outlier=is_outlier,
    )
    return PriceListControlMetrics(
        gross_price=None if gross_price is None else quantize_money(gross_price),
        reference_gross_cost=(
            None if reference_gross_cost is None else quantize_money(reference_gross_cost)
        ),
        actual_markup_pct=markup,
        gross_margin_pct=margin,
        min_markup_pct=min_markup_pct,
        max_markup_pct=max_markup_pct,
        minimum_recommended_gross_price=min_rec,
        maximum_recommended_gross_price=max_rec,
        price_adjustment_to_minimum=adj,
        price_diff_vs_cost=diff,
        status=status,
        policy_compliance=compliance,
    )
