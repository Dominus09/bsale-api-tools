"""Costo bruto máximo válido y vigente (para /margins y auditoría /costos).

No corrige costos automáticamente. No usa ventas ni documentos comerciales.

Prioridad de monto bruto por candidato:
1. cost_bruto_erp > 0
2. net + iva_amount + other_taxes (reconstruido)
3. (fallback externo) variant_cost — lo aporta el caller

Excluye por defecto tipos de recepción no válidos para pricing:
ajuste / devolución / NC.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from statistics import median

from backend.services.analytics.money import ZERO, quantize_money
from backend.services.analytics.tax_models import GrossCostQuality

INVALID_RECEPTION_TYPES = frozenset(
    {
        "recepcion_ajuste",
        "recepcion_devolucion",
        "recepcion_nc",
    }
)

STALE_DAYS_DEFAULT = 90
OUTLIER_FACTOR = Decimal("3")


@dataclass(frozen=True, slots=True)
class GrossCostCandidate:
    """Candidato unitario o de línea (misma base)."""

    gross_cost: Decimal | None
    net_cost: Decimal | None = None
    iva_amount: Decimal | None = None
    other_taxes: Decimal | None = None
    cost_date: date | None = None
    cost_source: str = "cost_reception_history"
    reception_type: str | None = None
    reception_id: int | None = None
    variant_id: int | None = None


@dataclass(frozen=True, slots=True)
class MaxValidGrossCostResolution:
    gross_cost: Decimal | None
    cost_date: date | None
    cost_source: str | None
    cost_age_days: int | None
    gross_cost_quality: str
    is_outlier: bool
    is_stale: bool
    resolution_reason: str
    min_gross_among_valid: Decimal | None = None
    max_gross_among_valid: Decimal | None = None
    previous_gross: Decimal | None = None
    candidate_count: int = 0
    excluded_count: int = 0


def _effective_gross(c: GrossCostCandidate) -> Decimal | None:
    if c.gross_cost is not None and c.gross_cost > ZERO:
        return quantize_money(c.gross_cost)
    if (
        c.net_cost is not None
        and c.net_cost > ZERO
        and c.iva_amount is not None
        and c.other_taxes is not None
    ):
        return quantize_money(c.net_cost + c.iva_amount + c.other_taxes)
    return None


def _quality_for(c: GrossCostCandidate, gross: Decimal) -> str:
    if c.gross_cost is not None and c.gross_cost > ZERO:
        return GrossCostQuality.ACTUAL_PURCHASE_GROSS.value
    if c.cost_source.startswith("variant_cost"):
        return GrossCostQuality.CURRENT_TAX_PROFILE_FALLBACK.value
    return GrossCostQuality.RECONSTRUCTED_FROM_ACTUAL_TAXES.value


def resolve_max_valid_gross_cost(
    candidates: list[GrossCostCandidate],
    *,
    as_of: date,
    stale_days: int = STALE_DAYS_DEFAULT,
    fallback: GrossCostCandidate | None = None,
) -> MaxValidGrossCostResolution:
    """Elige el costo bruto máximo entre candidatos válidos (sin auto-corregir)."""
    excluded = 0
    usable: list[tuple[GrossCostCandidate, Decimal]] = []
    for c in candidates:
        rtype = (c.reception_type or "").strip().lower()
        if rtype in INVALID_RECEPTION_TYPES:
            excluded += 1
            continue
        g = _effective_gross(c)
        if g is None:
            excluded += 1
            continue
        usable.append((c, g))

    if not usable and fallback is not None:
        g = _effective_gross(fallback)
        if g is not None:
            age = (
                (as_of - fallback.cost_date).days
                if fallback.cost_date is not None
                else None
            )
            return MaxValidGrossCostResolution(
                gross_cost=g,
                cost_date=fallback.cost_date,
                cost_source=fallback.cost_source,
                cost_age_days=age,
                gross_cost_quality=_quality_for(fallback, g),
                is_outlier=False,
                is_stale=age is not None and age > stale_days,
                resolution_reason="variant_cost_fallback",
                min_gross_among_valid=g,
                max_gross_among_valid=g,
                previous_gross=None,
                candidate_count=0,
                excluded_count=excluded,
            )

    if not usable:
        return MaxValidGrossCostResolution(
            gross_cost=None,
            cost_date=None,
            cost_source=None,
            cost_age_days=None,
            gross_cost_quality=GrossCostQuality.MISSING_GROSS_COST.value,
            is_outlier=False,
            is_stale=False,
            resolution_reason="no_valid_gross_candidates",
            candidate_count=0,
            excluded_count=excluded,
        )

    # Orden temporal para previous; máximo para pricing.
    by_date = sorted(
        usable,
        key=lambda pair: (
            pair[0].cost_date or date.min,
            pair[1],
        ),
    )
    grosses = [g for _, g in usable]
    min_g = min(grosses)
    max_g = max(grosses)
    winner_c, winner_g = max(
        usable,
        key=lambda pair: (
            pair[1],
            pair[0].cost_date or date.min,
        ),
    )

    med = Decimal(str(median([float(x) for x in grosses]))) if len(grosses) >= 3 else None
    is_outlier = bool(
        med is not None and med > ZERO and winner_g > med * OUTLIER_FACTOR
    )

    # previous = penúltimo por fecha (auditoría), no el max.
    previous = by_date[-2][1] if len(by_date) >= 2 else None
    age = (as_of - winner_c.cost_date).days if winner_c.cost_date is not None else None
    stale = age is not None and age > stale_days

    reason = "max_valid_purchase_gross"
    if is_outlier:
        reason = "max_valid_purchase_gross_flagged_outlier"
    if stale:
        reason = f"{reason}_stale"

    return MaxValidGrossCostResolution(
        gross_cost=winner_g,
        cost_date=winner_c.cost_date,
        cost_source=winner_c.cost_source,
        cost_age_days=age,
        gross_cost_quality=_quality_for(winner_c, winner_g),
        is_outlier=is_outlier,
        is_stale=stale,
        resolution_reason=reason,
        min_gross_among_valid=min_g,
        max_gross_among_valid=max_g,
        previous_gross=previous,
        candidate_count=len(usable),
        excluded_count=excluded,
    )
