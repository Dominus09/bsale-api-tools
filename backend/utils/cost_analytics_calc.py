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
        v = float(variation_pct)
        if v <= -10:
            alerts.append("cost_decrease_10")
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


def _normalize_text_blob(*parts: Any) -> str:
    import unicodedata

    raw = " ".join(str(p or "") for p in parts)
    lowered = raw.lower()
    return "".join(
        c
        for c in unicodedata.normalize("NFD", lowered)
        if unicodedata.category(c) != "Mn"
    )


def classify_reception_type(
    document: str | None,
    note: str | None,
    document_number: int | str | None = None,
) -> str:
    """
    Clasifica recepción según document / note / documentNumber.
    Valores: recepcion_normal, recepcion_ajuste, recepcion_devolucion, recepcion_nc.
    """
    blob = _normalize_text_blob(document, note, document_number)
    nc_markers = (
        "nota credito",
        "nota de credito",
        " nc ",
        "nc ",
        " nc",
        "abono",
    )
    devolucion_markers = ("devolucion", "devolución")
    ajuste_markers = (
        "ajuste",
        "ajuste inventario",
        "correccion",
        "correccion recepcion",
        "corrección",
    )
    if any(m.replace("ó", "o") in blob for m in nc_markers):
        return "recepcion_nc"
    if any(m.replace("ó", "o") in blob for m in devolucion_markers):
        return "recepcion_devolucion"
    if any(m.replace("ó", "o") in blob for m in ajuste_markers):
        return "recepcion_ajuste"
    return "recepcion_normal"


RECEPTION_TYPE_LABELS: dict[str, str] = {
    "recepcion_normal": "Recepción normal",
    "recepcion_ajuste": "Recepción ajuste",
    "recepcion_devolucion": "Recepción devolución",
    "recepcion_nc": "Recepción NC",
}


def spread_semaphore(spread_pct: float | None) -> str:
    """Semáforo variación interna entre sucursales: 0-3 verde, 3-10 amarillo, >10 rojo."""
    if spread_pct is None:
        return "green"
    v = abs(float(spread_pct))
    if v > 10:
        return "red"
    if v > 3:
        return "yellow"
    return "green"


def classify_purchase_opportunity(
    current_cost: float | None,
    avg_historical: float | None,
    *,
    threshold_pct: float = 3.0,
) -> tuple[str | None, float | None]:
    """
    Retorna (estado, variacion_pct).
    estado: oportunidad_compra | riesgo_comercial | None
    """
    if current_cost is None or avg_historical is None:
        return None, None
    cur = float(current_cost)
    avg = float(avg_historical)
    if cur <= 0 or avg <= 0:
        return None, None
    var_pct = round(((cur - avg) / avg) * 100.0, 2)
    if var_pct <= -threshold_pct:
        return "oportunidad_compra", var_pct
    if var_pct >= threshold_pct:
        return "riesgo_comercial", var_pct
    return None, var_pct


OPPORTUNITY_LABELS: dict[str, str] = {
    "oportunidad_compra": "Oportunidad de compra",
    "riesgo_comercial": "Costo elevado",
}


ALERT_ACTIONS: dict[str, tuple[str, str]] = {
    "variation_20": ("red", "Revisar precio de venta"),
    "variation_10": ("yellow", "Vigilar margen y precio"),
    "anomalous_cost": ("red", "Revisar costo anómalo"),
    "zero_cost": ("red", "Revisar recepción con costo cero"),
    "suspicious_reception": ("yellow", "Revisar recepción"),
    "cross_branch_diff": ("red", "Revisar diferencia entre sucursales"),
    "cost_decrease_10": ("green", "Oportunidad de compra"),
    "missing_cost": ("yellow", "Completar costo del producto"),
    "no_history": ("yellow", "Sin historial de costos"),
    "oportunidad_compra": ("green", "Oportunidad de compra"),
    "riesgo_comercial": ("red", "Revisar precio de venta"),
}


COMMERCIAL_SCORE_LABELS: dict[str, str] = {
    "excelente": "Excelente",
    "vigilar": "Vigilar",
    "revisar": "Revisar",
}


WATCHLIST_STATUS_LABELS: dict[str, str] = {
    "mejorando": "Mejorando",
    "estable": "Estable",
    "revisar": "Revisar",
}


def watchlist_status(variation_pct_90d: float | None) -> tuple[str, str]:
    """Estado watchlist: mejorando | estable | revisar."""
    if variation_pct_90d is None:
        return "estable", "yellow"
    v = float(variation_pct_90d)
    if v <= -3:
        return "mejorando", "green"
    if v >= 3:
        return "revisar", "red"
    return "estable", "yellow"


def commercial_score(
    *,
    variation_pct_90d: float | None = None,
    branch_spread_pct: float | None = None,
    reception_count_90d: int = 0,
    movement_qty_90d: float = 0,
    anomalous: bool = False,
    zero_cost: bool = False,
) -> tuple[str, str]:
    """
    Score comercial: excelente | vigilar | revisar.
    Retorna (score_key, semaphore).
    """
    risk = 0
    if zero_cost:
        risk += 4
    if anomalous:
        risk += 3
    if variation_pct_90d is not None:
        v = abs(float(variation_pct_90d))
        if v >= 20:
            risk += 3
        elif v >= 10:
            risk += 2
        elif v >= 5:
            risk += 1
    if branch_spread_pct is not None:
        s = float(branch_spread_pct)
        if s > 10:
            risk += 3
        elif s > 3:
            risk += 1
    if reception_count_90d < 2:
        risk += 1
    if movement_qty_90d <= 0:
        risk += 1
    if risk >= 5:
        return "revisar", "red"
    if risk >= 2:
        return "vigilar", "yellow"
    return "excelente", "green"


def alert_to_action(alert_type: str) -> dict[str, str]:
    sem, action = ALERT_ACTIONS.get(alert_type, ("yellow", "Revisar producto"))
    return {"alert_type": alert_type, "semaphore": sem, "action": action}
