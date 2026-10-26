"""Stock global por empresa+variante para Analítica V2 (solo lectura).

STOCK_GLOBAL = SUM(quantity) sobre ``bsale_raw.stocks`` filtrado por company_id,
agrupado por variant_id (todas las oficinas de la empresa). No inventa ceros:
sin fila → estado ``sin_sincronizar`` (total None).

Filtro operativo (Fase 2.4):
- ``with`` (default): stock_global > 0
- ``without``: sin fila, cero o negativo (sin stock positivo)
- ``all``: universo completo
"""

from __future__ import annotations

from decimal import ROUND_FLOOR, Decimal
from typing import Any, Mapping

from backend.services.analytics_v2.cost_signals import to_decimal

STOCK_TOTALS_SQL = """
SELECT variant_id, sum(quantity) AS stock_total, count(*) AS stock_offices,
       max(api_fetched_at) AS stock_fetched_at
FROM bsale_raw.stocks
WHERE company_id = %s
GROUP BY variant_id
"""

STOCK_FILTER_WITH = "with"
STOCK_FILTER_WITHOUT = "without"
STOCK_FILTER_ALL = "all"
DEFAULT_STOCK_FILTER = STOCK_FILTER_WITH
STOCK_FILTERS = frozenset({STOCK_FILTER_WITH, STOCK_FILTER_WITHOUT, STOCK_FILTER_ALL})

STOCK_STATE_CON = "con_stock"
STOCK_STATE_CERO = "cero"
STOCK_STATE_NEG = "negativo"
STOCK_STATE_SIN = "sin_sincronizar"

# Universo «sin stock positivo»: cero, negativo o sin registro.
WITHOUT_STATES = frozenset({STOCK_STATE_CERO, STOCK_STATE_NEG, STOCK_STATE_SIN})

STOCK_FILTER_LABELS = {
    STOCK_FILTER_WITH: "CON STOCK",
    STOCK_FILTER_WITHOUT: "SIN STOCK",
    STOCK_FILTER_ALL: "TODOS",
}

STOCK_CRITERION = (
    "STOCK_GLOBAL = SUM(quantity) por company_id+variant_id en bsale_raw.stocks "
    "(todas las oficinas de la empresa; saldo real, incluye negativos)"
)


def normalize_stock_filter(value: str | None, *, default: str = DEFAULT_STOCK_FILTER) -> str:
    """Normaliza el filtro; None o vacío → default operativo (CON STOCK)."""
    if value is None or value == "":
        return default
    if value not in STOCK_FILTERS:
        raise ValueError(f"stock_filter inválido: {value}")
    return value


def stock_state_from_total(total: Decimal | None) -> str:
    if total is None:
        return STOCK_STATE_SIN
    if total > 0:
        return STOCK_STATE_CON
    if total < 0:
        return STOCK_STATE_NEG
    return STOCK_STATE_CERO


def stock_payload(total: Decimal | None, *, offices: int = 0) -> dict[str, Any]:
    """Contrato de stock para filas analíticas (total None = sin sincronizar)."""
    return {
        "total": None if total is None else str(total),
        "state": stock_state_from_total(total),
        "offices": int(offices or 0),
    }


def matches_stock_filter(
    *,
    state: str | None = None,
    total: Decimal | None | object = ...,
    stock_filter: str,
) -> bool:
    """True si la variante entra en el universo del filtro (antes de paginar)."""
    filt = normalize_stock_filter(stock_filter)
    if filt == STOCK_FILTER_ALL:
        return True
    if state is None:
        if total is ...:
            state = STOCK_STATE_SIN
        else:
            state = stock_state_from_total(to_decimal(total) if total is not None else None)
    if filt == STOCK_FILTER_WITH:
        return state == STOCK_STATE_CON
    return state in WITHOUT_STATES


def stock_filter_sql_clause(alias: str = "st", *, stock_filter: str) -> str | None:
    """Cláusula SQL sobre ``alias.stock_total`` (LEFT JOIN de totales). None = sin filtro."""
    filt = normalize_stock_filter(stock_filter)
    if filt == STOCK_FILTER_ALL:
        return None
    col = f"{alias}.stock_total"
    if filt == STOCK_FILTER_WITH:
        return f"{col} > 0"
    # Sin stock positivo: NULL (sin fila), 0 o negativo.
    return f"({col} IS NULL OR {col} <= 0)"


def index_stock_totals(rows: list[Mapping[str, Any]]) -> dict[int, dict[str, Any]]:
    """Mapa variant_id → {total: Decimal|None, offices: int}."""
    out: dict[int, dict[str, Any]] = {}
    for r in rows:
        vid = int(r["variant_id"])
        out[vid] = {
            "total": to_decimal(r.get("stock_total")),
            "offices": int(r.get("stock_offices") or 0),
        }
    return out


def resolve_stock_entry(
    stocks: Mapping[int, Mapping[str, Any]],
    variant_id: int,
) -> dict[str, Any]:
    entry = stocks.get(int(variant_id))
    if not entry:
        return stock_payload(None, offices=0)
    total = entry.get("total")
    if total is not None and not isinstance(total, Decimal):
        total = to_decimal(total)
    return stock_payload(total, offices=int(entry.get("offices") or 0))


def pack_availability_from_components(
    component_qtys: Mapping[int, Decimal],
    stocks: Mapping[int, Mapping[str, Any]],
) -> dict[str, Any]:
    """Disponibilidad derivada de componentes (packs virtuales sin fila propia).

    Sin registro de stock en un componente → 0 unidades aportadas (no inventa stock).
    """
    if not component_qtys:
        return {
            **stock_payload(None, offices=0),
            "derived": True,
            "scope": "global_componentes",
            "label": "Unidades armables globales (por componentes; no es stock de una sucursal)",
            "available_units": None,
            "note": "pack_sin_componentes_cuantificables",
        }
    units: list[Decimal] = []
    offices = 0
    any_synced = False
    for vid, qty in component_qtys.items():
        if qty is None or qty <= 0:
            continue
        entry = stocks.get(int(vid))
        if entry is None or entry.get("total") is None:
            units.append(Decimal("0"))
            continue
        any_synced = True
        offices = max(offices, int(entry.get("offices") or 0))
        total = entry["total"]
        if not isinstance(total, Decimal):
            total = to_decimal(total) or Decimal("0")
        units.append(total / qty)
    if not units:
        return {
            **stock_payload(None, offices=0),
            "derived": True,
            "scope": "global_componentes",
            "label": "Unidades armables globales (por componentes; no es stock de una sucursal)",
            "available_units": None,
            "note": "pack_sin_componentes_cuantificables",
        }
    available = min(units)
    floored = available.to_integral_value(rounding=ROUND_FLOOR)
    if not any_synced and floored == 0:
        payload = stock_payload(None, offices=0)
    else:
        payload = stock_payload(floored, offices=offices)
    return {
        **payload,
        "derived": True,
        "scope": "global_componentes",
        "label": "Unidades armables globales (por componentes; no es stock de una sucursal)",
        "available_units": str(floored),
        "note": "disponibilidad_por_componentes",
    }


def excel_stock_banner(*, stock_filter: str, cut_at: str | None = None) -> list[str]:
    """Líneas de encabezado para Excel (filtro + criterio + corte)."""
    filt = normalize_stock_filter(stock_filter)
    lines = [
        f"FILTRO DE STOCK: {STOCK_FILTER_LABELS[filt]}",
        f"Criterio: {STOCK_CRITERION}",
    ]
    if cut_at:
        lines.append(f"Fecha/hora de corte: {cut_at}")
    return lines
