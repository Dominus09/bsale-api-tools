"""Cálculos de costo bruto ERP y variación (sin alterar márgenes existentes)."""

from __future__ import annotations

from decimal import Decimal
from typing import Any


def parse_tax_factor(raw: Any) -> Decimal:
    try:
        v = Decimal(str(raw or "1"))
    except Exception:
        return Decimal("1")
    if v <= 0:
        return Decimal("1")
    return v


def cost_gross_from_net(cost_net: float | Decimal, tax_factor: float | Decimal) -> Decimal:
    net = Decimal(str(cost_net or 0))
    tf = parse_tax_factor(tax_factor)
    return (net * tf).quantize(Decimal("0.0001"))


def split_erp_cost(
    cost_net: float | Decimal,
    *,
    tax_factor: float | Decimal = 1,
    iva_rate: float | Decimal | None = None,
) -> tuple[Decimal, Decimal, Decimal]:
    """
    Costo Neto Bsale + IVA + otros impuestos = Costo Bruto ERP.
    Si no hay iva_rate, todo el delta neto→bruto va a other_taxes.
    """
    net = Decimal(str(cost_net or 0))
    bruto = cost_gross_from_net(net, tax_factor)
    if iva_rate is not None:
        try:
            iva = (net * Decimal(str(iva_rate)) / Decimal("100")).quantize(Decimal("0.0001"))
        except Exception:
            iva = Decimal("0")
    else:
        iva = Decimal("0")
    other = (bruto - net - iva).quantize(Decimal("0.0001"))
    if other < 0:
        other = Decimal("0")
        iva = (bruto - net).quantize(Decimal("0.0001"))
    return iva, other, bruto


def variation_pct(current: float | Decimal, previous: float | Decimal | None) -> float | None:
    if previous is None:
        return None
    prev = float(previous)
    cur = float(current)
    if prev <= 0:
        return None
    return round(((cur - prev) / prev) * 100.0, 2)


def branch_spread_pct(min_cost: float, max_cost: float) -> float | None:
    if min_cost <= 0 or max_cost <= 0:
        return None
    lo = min(min_cost, max_cost)
    hi = max(min_cost, max_cost)
    if lo <= 0:
        return None
    return round(((hi - lo) / lo) * 100.0, 2)


def classify_cost_alert(
    *,
    has_history: bool,
    has_cost_row: bool,
    average_cost: float | None,
    cost_net: float | None,
    variation_pct: float | None,
    cross_branch_spread: float | None = None,
    suspicious_reception: bool = False,
) -> list[str]:
    alerts: list[str] = []
    if not has_history:
        alerts.append("no_history")
    if not has_cost_row:
        alerts.append("missing_cost")
    elif average_cost is not None and float(average_cost) == 0:
        alerts.append("zero_cost")
    if cost_net is not None and float(cost_net) == 0 and has_history:
        alerts.append("zero_cost")
    if variation_pct is not None:
        av = abs(float(variation_pct))
        if av >= 20:
            alerts.append("variation_20")
        elif av >= 10:
            alerts.append("variation_10")
        if av >= 50:
            alerts.append("anomalous_cost")
    if cross_branch_spread is not None and float(cross_branch_spread) >= 10:
        alerts.append("cross_branch_diff")
    if suspicious_reception:
        alerts.append("suspicious_reception")
    return alerts


def alert_semaphore(alert_types: list[str]) -> str:
    red = {
        "zero_cost",
        "anomalous_cost",
        "variation_20",
        "suspicious_reception",
    }
    yellow = {
        "variation_10",
        "cross_branch_diff",
        "no_history",
        "missing_cost",
    }
    if any(t in red for t in alert_types):
        return "red"
    if any(t in yellow for t in alert_types):
        return "yellow"
    return "green"


def make_unique_key(company_id: int, reception_detail_id: int) -> str:
    return f"{company_id}_{reception_detail_id}"
