"""Stock operativo vs stock total Bsale para Analítica V2 (solo lectura).

STOCK_TOTAL_BSALE = SUM(quantity) por company_id+variant_id en todas las oficinas.
STOCK_OPERATIVO = SUM(quantity) solo en oficinas operativas configuradas por empresa
(``COST_CONTROL_OFFICE_IDS_BY_COMPANY``). Si la empresa no tiene mapa, operativo = total.

Filtro comercial (default CON STOCK):
- ``with``: stock_operativo > 0
- ``without``: sin fila, operativo <= 0 (incluye solo auxiliares)
- ``all``: universo completo

No escribe bsale_raw ni altera inventarios físicos/contables.
"""

from __future__ import annotations

from decimal import ROUND_FLOOR, Decimal
from typing import Any, Mapping, Sequence

from backend.schemas.cost_v2_company_read import COST_CONTROL_OFFICE_IDS_BY_COMPANY
from backend.services.analytics_v2.cost_signals import to_decimal

# Fuente única: mismos IDs que selectores de control de costos.
OPERATIONAL_OFFICE_IDS_BY_COMPANY: dict[int, tuple[int, ...]] = COST_CONTROL_OFFICE_IDS_BY_COMPANY

# Totales sin acotar oficinas (referencia auditable / empresas sin mapa operativo).
STOCK_TOTALS_SQL = """
SELECT variant_id,
       sum(quantity) AS stock_total,
       sum(quantity) AS stock_total_bsale,
       count(*) AS stock_offices,
       count(*) AS stock_offices_operational,
       max(api_fetched_at) AS stock_fetched_at
FROM bsale_raw.stocks
WHERE company_id = %s
GROUP BY variant_id
""".strip()

# Dual: total Bsale + operativo (ARRAY de office_id).
STOCK_TOTALS_OPERATIONAL_SQL = """
SELECT variant_id,
       sum(quantity) AS stock_total_bsale,
       COALESCE(sum(quantity) FILTER (WHERE office_id = ANY(%s)), 0) AS stock_total,
       count(*) AS stock_offices,
       count(*) FILTER (WHERE office_id = ANY(%s)) AS stock_offices_operational,
       max(api_fetched_at) AS stock_fetched_at
FROM bsale_raw.stocks
WHERE company_id = %s
GROUP BY variant_id
""".strip()

STOCK_FILTER_WITH = "with"
STOCK_FILTER_WITHOUT = "without"
STOCK_FILTER_ALL = "all"
DEFAULT_STOCK_FILTER = STOCK_FILTER_WITH
STOCK_FILTERS = frozenset({STOCK_FILTER_WITH, STOCK_FILTER_WITHOUT, STOCK_FILTER_ALL})

STOCK_STATE_CON = "con_stock"
STOCK_STATE_CERO = "cero"
STOCK_STATE_NEG = "negativo"
STOCK_STATE_SIN = "sin_sincronizar"

WITHOUT_STATES = frozenset({STOCK_STATE_CERO, STOCK_STATE_NEG, STOCK_STATE_SIN})

STOCK_FILTER_LABELS = {
    STOCK_FILTER_WITH: "CON STOCK OPERATIVO",
    STOCK_FILTER_WITHOUT: "SIN STOCK OPERATIVO",
    STOCK_FILTER_ALL: "TODOS",
}

STOCK_CRITERION = (
    "STOCK_OPERATIVO = SUM(quantity) por company_id+variant_id en oficinas operativas "
    "configuradas (empresa 3: 1,3,4,5). STOCK_TOTAL_BSALE = suma de todas las oficinas. "
    "Saldo algebraico (incluye negativos). Sin fila → sin_sincronizar."
)

STOCK_SCOPE_BANNER = "ALCANCE: STOCK OPERATIVO — OFICINAS HABILITADAS"
AUXILIARY_ONLY_LEGEND = "Sin stock operativo · Stock en bodegas auxiliares"

_STOCK_FILTER_ALIASES = {
    "with": STOCK_FILTER_WITH,
    "con": STOCK_FILTER_WITH,
    "con_stock": STOCK_FILTER_WITH,
    "con-stock": STOCK_FILTER_WITH,
    "positivo": STOCK_FILTER_WITH,
    "without": STOCK_FILTER_WITHOUT,
    "sin": STOCK_FILTER_WITHOUT,
    "sin_stock": STOCK_FILTER_WITHOUT,
    "sin-stock": STOCK_FILTER_WITHOUT,
    "all": STOCK_FILTER_ALL,
    "todos": STOCK_FILTER_ALL,
    "todas": STOCK_FILTER_ALL,
}


def operational_office_ids(company_id: int) -> tuple[int, ...] | None:
    """Oficinas operativas de la empresa, o None si aún no hay mapa (comportamiento = total)."""
    return OPERATIONAL_OFFICE_IDS_BY_COMPANY.get(int(company_id))


def has_operational_office_scope(company_id: int) -> bool:
    return operational_office_ids(company_id) is not None


def stock_totals_query(company_id: int) -> tuple[str, tuple[Any, ...]]:
    """SQL + params para totales por variante (operativo + total Bsale)."""
    offices = operational_office_ids(company_id)
    if offices is None:
        return STOCK_TOTALS_SQL, (int(company_id),)
    arr = list(offices)
    return STOCK_TOTALS_OPERATIONAL_SQL, (arr, arr, int(company_id))


def stock_join_subquery(company_id: int, *, alias: str = "st") -> tuple[str, list[Any]]:
    """LEFT JOIN subquery: ``alias.stock_total`` = operativo; ``stock_total_bsale`` = total.

    Parámetros se anteponen al WHERE del caller (mismo patrón que catálogo).
    """
    offices = operational_office_ids(company_id)
    if offices is None:
        sql = (
            f"LEFT JOIN (SELECT variant_id, sum(quantity) AS stock_total, "
            f"sum(quantity) AS stock_total_bsale, "
            f"max(api_fetched_at) AS stock_fetched_at, count(*) AS stock_offices "
            f"FROM bsale_raw.stocks WHERE company_id = %s GROUP BY variant_id) {alias} "
            f"ON {alias}.variant_id = v.bsale_id"
        )
        return sql, [int(company_id)]
    arr = list(offices)
    sql = (
        f"LEFT JOIN (SELECT variant_id, "
        f"sum(quantity) AS stock_total_bsale, "
        f"COALESCE(sum(quantity) FILTER (WHERE office_id = ANY(%s)), 0) AS stock_total, "
        f"max(api_fetched_at) AS stock_fetched_at, count(*) AS stock_offices "
        f"FROM bsale_raw.stocks WHERE company_id = %s GROUP BY variant_id) {alias} "
        f"ON {alias}.variant_id = v.bsale_id"
    )
    return sql, [arr, int(company_id)]


def stock_cte_sql(company_id: int, *, alias: str = "st") -> tuple[str, list[Any]]:
    """CTE ``alias`` con stock_total (operativo) y stock_total_bsale."""
    offices = operational_office_ids(company_id)
    if offices is None:
        sql = (
            f"{alias} AS ("
            f" SELECT variant_id, sum(quantity) AS stock_total, sum(quantity) AS stock_total_bsale"
            f" FROM bsale_raw.stocks WHERE company_id = %s GROUP BY variant_id)"
        )
        return sql, [int(company_id)]
    arr = list(offices)
    sql = (
        f"{alias} AS ("
        f" SELECT variant_id, sum(quantity) AS stock_total_bsale,"
        f" COALESCE(sum(quantity) FILTER (WHERE office_id = ANY(%s)), 0) AS stock_total"
        f" FROM bsale_raw.stocks WHERE company_id = %s GROUP BY variant_id)"
    )
    return sql, [arr, int(company_id)]


def normalize_stock_filter(value: str | None, *, default: str = DEFAULT_STOCK_FILTER) -> str:
    """Normaliza el filtro; None o vacío → default operativo (CON STOCK)."""
    if value is None or value == "":
        return default
    key = str(value).strip().lower().replace(" ", "_")
    mapped = _STOCK_FILTER_ALIASES.get(key)
    if mapped is not None:
        return mapped
    if value in STOCK_FILTERS:
        return value
    raise ValueError(
        f"stock_filter inválido: {value!r}. Use with|without|all "
        f"(o con_stock|sin_stock|todos)."
    )


def stock_state_from_total(total: Decimal | None) -> str:
    if total is None:
        return STOCK_STATE_SIN
    if total > 0:
        return STOCK_STATE_CON
    if total < 0:
        return STOCK_STATE_NEG
    return STOCK_STATE_CERO


def stock_payload(
    total: Decimal | None,
    *,
    offices: int = 0,
    total_bsale: Decimal | None | object = ...,
    scope_configured: bool = False,
) -> dict[str, Any]:
    """Contrato de stock: ``total`` = operativo (filtro); ``total_bsale`` = todas las oficinas."""
    bsale: Decimal | None
    if total_bsale is ...:
        bsale = total
    elif total_bsale is None:
        bsale = None
    elif isinstance(total_bsale, Decimal):
        bsale = total_bsale
    else:
        bsale = to_decimal(total_bsale)

    state = stock_state_from_total(total)
    auxiliary_only = (
        scope_configured
        and total is not None
        and total <= 0
        and bsale is not None
        and bsale > 0
    )
    out: dict[str, Any] = {
        "total": None if total is None else str(total),
        "state": state,
        "offices": int(offices or 0),
        "total_bsale": None if bsale is None else str(bsale),
        "scope": "operativo" if scope_configured else "total_bsale",
        "auxiliary_only": auxiliary_only,
    }
    if auxiliary_only:
        out["legend"] = AUXILIARY_ONLY_LEGEND
    return out


def matches_stock_filter(
    *,
    state: str | None = None,
    total: Decimal | None | object = ...,
    stock_filter: str,
) -> bool:
    """True si la variante entra en el universo del filtro (antes de paginar).

    ``total`` / ``state`` deben referirse al stock **operativo**.
    """
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
    """Cláusula SQL sobre ``alias.stock_total`` (columna operativa del JOIN/CTE)."""
    filt = normalize_stock_filter(stock_filter)
    if filt == STOCK_FILTER_ALL:
        return None
    col = f"{alias}.stock_total"
    if filt == STOCK_FILTER_WITH:
        return f"{col} > 0"
    return f"({col} IS NULL OR {col} <= 0)"


def index_stock_totals(
    rows: list[Mapping[str, Any]],
    *,
    company_id: int | None = None,
) -> dict[int, dict[str, Any]]:
    """Mapa variant_id → totales (``total`` = operativo para filtros)."""
    configured = has_operational_office_scope(company_id) if company_id is not None else False
    out: dict[int, dict[str, Any]] = {}
    for r in rows:
        vid = int(r["variant_id"])
        has_bsale_col = "stock_total_bsale" in r
        total_bsale = to_decimal(r.get("stock_total_bsale")) if has_bsale_col else to_decimal(r.get("stock_total"))
        operational = to_decimal(r.get("stock_total"))
        if configured and operational is None and total_bsale is not None:
            operational = Decimal("0")
        if not configured and not has_bsale_col:
            total_bsale = operational
        out[vid] = {
            "total": operational,
            "total_bsale": total_bsale,
            "offices": int(r.get("stock_offices") or 0),
            "offices_operational": int(
                r.get("stock_offices_operational")
                if r.get("stock_offices_operational") is not None
                else (r.get("stock_offices") or 0)
            ),
            "scope_configured": configured,
        }
    return out


def resolve_stock_entry(
    stocks: Mapping[int, Mapping[str, Any]],
    variant_id: int,
) -> dict[str, Any]:
    entry = stocks.get(int(variant_id))
    if not entry:
        return stock_payload(None, offices=0, total_bsale=None, scope_configured=False)
    total = entry.get("total")
    if total is not None and not isinstance(total, Decimal):
        total = to_decimal(total)
    bsale = entry.get("total_bsale", total)
    if bsale is not None and not isinstance(bsale, Decimal):
        bsale = to_decimal(bsale)
    return stock_payload(
        total,
        offices=int(entry.get("offices_operational") or entry.get("offices") or 0),
        total_bsale=bsale,
        scope_configured=bool(entry.get("scope_configured")),
    )


def reconcile_stock_scopes(stocks: Mapping[int, Mapping[str, Any]]) -> dict[str, Any]:
    """Resumen de conciliación operativo vs total Bsale (unidades heterogéneas ≠ valorización)."""
    units_op = Decimal("0")
    units_bsale = Decimal("0")
    var_pos_op = 0
    var_pos_bsale = 0
    var_aux_only = 0
    for entry in stocks.values():
        op = entry.get("total")
        bs = entry.get("total_bsale", op)
        if not isinstance(op, Decimal) and op is not None:
            op = to_decimal(op)
        if not isinstance(bs, Decimal) and bs is not None:
            bs = to_decimal(bs)
        if op is not None:
            units_op += op
            if op > 0:
                var_pos_op += 1
        if bs is not None:
            units_bsale += bs
            if bs > 0:
                var_pos_bsale += 1
        if (
            op is not None
            and op <= 0
            and bs is not None
            and bs > 0
        ):
            var_aux_only += 1
    return {
        "variants_indexed": len(stocks),
        "variants_positive_operational": var_pos_op,
        "variants_positive_total_bsale": var_pos_bsale,
        "variants_auxiliary_only_positive": var_aux_only,
        "units_operational": str(units_op),
        "units_total_bsale": str(units_bsale),
        "units_delta": str(units_bsale - units_op),
        "note": "Suma de unidades de productos heterogéneos; no es valorización económica.",
    }


def pack_availability_from_components(
    component_qtys: Mapping[int, Decimal],
    stocks: Mapping[int, Mapping[str, Any]],
) -> dict[str, Any]:
    """Disponibilidad armable con stock **operativo** de componentes (no auxiliares)."""
    if not component_qtys:
        return {
            **stock_payload(None, offices=0),
            "derived": True,
            "scope": "operativo_componentes",
            "label": (
                "Unidades armables (componentes en oficinas operativas; "
                "no es stock inmediato de una sucursal)"
            ),
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
        offices = max(offices, int(entry.get("offices_operational") or entry.get("offices") or 0))
        total = entry["total"]
        if not isinstance(total, Decimal):
            total = to_decimal(total) or Decimal("0")
        units.append(total / qty)
    if not units:
        return {
            **stock_payload(None, offices=0),
            "derived": True,
            "scope": "operativo_componentes",
            "label": (
                "Unidades armables (componentes en oficinas operativas; "
                "no es stock inmediato de una sucursal)"
            ),
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
        "scope": "operativo_componentes",
        "label": (
            "Unidades armables (componentes en oficinas operativas; "
            "no es stock inmediato de una sucursal)"
        ),
        "available_units": str(floored),
        "note": "disponibilidad_por_componentes_operativos",
    }


def excel_stock_banner(
    *,
    stock_filter: str,
    cut_at: str | None = None,
    company_id: int | None = None,
) -> list[str]:
    """Líneas de encabezado para Excel (alcance operativo + filtro + criterio)."""
    filt = normalize_stock_filter(stock_filter)
    lines = [STOCK_SCOPE_BANNER]
    offices = operational_office_ids(company_id) if company_id is not None else None
    if offices is not None:
        lines.append(f"Oficinas operativas: {', '.join(str(o) for o in offices)}")
    elif company_id is not None:
        lines.append("Sin mapa operativo: stock operativo = stock total Bsale")
    lines.append(f"FILTRO DE STOCK: {STOCK_FILTER_LABELS[filt]}")
    lines.append(f"Criterio: {STOCK_CRITERION}")
    if cut_at:
        lines.append(f"Fecha/hora de corte: {cut_at}")
    return lines


def office_ids_for_display(company_id: int) -> Sequence[int] | None:
    return operational_office_ids(company_id)
