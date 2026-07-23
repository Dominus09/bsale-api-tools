"""Fórmulas financieras canónicas (margen real ≠ markup).

Regla explícita (precio neto 100, costo histórico 80):
  utilidad = 20
  margen real = 20%
  markup = 25%

missing_cost → no se usa 0; utilidad/margen/markup quedan en None.
"""

from __future__ import annotations

from decimal import Decimal

from backend.services.analytics.money import ZERO, quantize_money, quantize_pct
from backend.services.analytics.schemas import CostQualityStatus, LineEconomics


def compute_gross_profit(
    net_sales: Decimal,
    historical_cost: Decimal | None,
) -> Decimal | None:
    """Utilidad bruta = venta neta − costo histórico. None si falta costo."""
    if historical_cost is None:
        return None
    return quantize_money(net_sales - historical_cost)


def compute_margin_pct(
    net_sales: Decimal,
    gross_profit: Decimal | None,
) -> Decimal | None:
    """Margen real = utilidad / venta_neta × 100. None si venta=0 o sin utilidad."""
    if gross_profit is None:
        return None
    if net_sales == ZERO:
        return None
    return quantize_pct((gross_profit / net_sales) * Decimal("100"))


def compute_markup_pct(
    historical_cost: Decimal | None,
    gross_profit: Decimal | None,
) -> Decimal | None:
    """Markup = utilidad / costo × 100. None si costo≤0 o sin utilidad."""
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
    """Aplica el contrato canónico a una línea.

    Si cost_quality es MISSING_COST, historical_cost se ignora aunque venga 0.
    """
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
