"""Rentabilidad de packs Bsale: costo por componentes × precio vigente por lista (solo lectura).

Compone Control de Costos V2 (costo del pack desde ``pack_details``) y Márgenes V2 (precio, regla,
promoción y estado por lista). Es rentabilidad estimada actual: no usa ventas históricas.
Los problemas detectados son señales para revisión; nunca cambian precios ni costos.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Callable

from backend.services.analytics_v2.contracts import (
    DASHBOARD_TOP_N,
    DEFAULT_COST_BASIS,
    PACK_LIST_PRICE_RATIO_ALERT,
    CostBasis,
    MarginComplianceStatus as S,
    MarginRisk,
    PackCostStatus,
    money,
    paginate,
)
from backend.services.analytics_v2.cost_control_service import CostControlV2Service
from backend.services.analytics_v2.margins_service import (
    MarginsNotFound,
    MarginsV2Service,
    MarginsValidationError,
    run_margins,
)
from backend.services.analytics_v2.movement_classification import normalize_label
from backend.services.analytics_v2.pack_engine import (
    ISSUE_INACTIVE_DETAIL,
    WARN_COST_INCREASE,
    WARN_INACTIVE_COMPONENT,
    WARN_REVIEW_COST,
    WARN_STALE_COST,
)

PACK_SORTS = frozenset({"status", "markup_asc", "markup_desc", "product"})
PACK_STATES = frozenset({"active", "inactive", "all"})


class PackProblem:
    PRECIO_BAJO_COSTO = "precio_bajo_costo"
    BAJO_POLITICA = "bajo_politica"
    PACK_NO_CALCULABLE = "pack_no_calculable"
    COMPONENTES_FALTANTES = "componentes_faltantes"
    COMPONENTES_SIN_COSTO = "componentes_sin_costo"
    COMPONENTE_COSTO_DESACTUALIZADO = WARN_STALE_COST
    COMPONENTE_COSTO_REQUIERE_REVISION = WARN_REVIEW_COST
    COMPONENTE_ALZA_DE_COSTO = WARN_COST_INCREASE
    COMPONENTE_INACTIVO = WARN_INACTIVE_COMPONENT
    DETALLE_INACTIVO_EXCLUIDO = ISSUE_INACTIVE_DETAIL
    DIFERENCIA_EXTREMA_ENTRE_LISTAS = "diferencia_extrema_entre_listas"


ALL_PROBLEMS = tuple(v for k, v in vars(PackProblem).items() if k.isupper())
_PACK_WARNING_PROBLEMS = frozenset(
    {WARN_STALE_COST, WARN_REVIEW_COST, WARN_COST_INCREASE, WARN_INACTIVE_COMPONENT, ISSUE_INACTIVE_DETAIL}
)

_STATUS_ORDER = {
    S.BAJO_POLITICA.value: 0,
    S.PACK_NO_CALCULABLE.value: 1,
    S.REQUIERE_REVISION_TRIBUTARIA.value: 1,
    S.SIN_REGLA.value: 2,
    S.SIN_COSTO.value: 3,
    S.SIN_PRECIO.value: 4,
    S.SOBRE_POLITICA.value: 5,
    S.REMATE_VIGENTE.value: 6,
    S.PROMOCION_VIGENTE.value: 7,
    S.DENTRO_POLITICA.value: 8,
}


def list_price_spread(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Precio bruto vigente por lista y razón máximo/mínimo; solo precios válidos (sin problemas de precio)."""
    prices = [
        (r["price_list"]["id"], Decimal(r["price"]["gross"]))
        for r in rows
        if r["price"]["issue"] is None and r["price"]["gross"] is not None
    ]
    if len(prices) < 2:
        return None
    low = min(p for _, p in prices)
    high = max(p for _, p in prices)
    ratio = high / low if low > 0 else None
    return {
        "prices": {str(lid): money(p) for lid, p in sorted(prices)},
        "min": money(low),
        "max": money(high),
        "ratio": money(ratio.quantize(Decimal("0.01"))) if ratio is not None else None,
        "alert": ratio is not None and ratio >= PACK_LIST_PRICE_RATIO_ALERT,
        "threshold_ratio": money(PACK_LIST_PRICE_RATIO_ALERT),
        "note": "verificar_en_bsale_raw_antes_de_clasificar_como_anomalia",
    }


def _row_problems(row: dict[str, Any], pack: dict[str, Any], spread_alert: bool) -> list[str]:
    out: list[str] = []
    if MarginRisk.VENTA_BAJO_COSTO.value in row["risks"]:
        out.append(PackProblem.PRECIO_BAJO_COSTO)
    if row["status"] == S.BAJO_POLITICA.value:
        out.append(PackProblem.BAJO_POLITICA)
    if row["status"] == S.PACK_NO_CALCULABLE.value:
        out.append(PackProblem.PACK_NO_CALCULABLE)
    if pack["missing_variant_ids"]:
        out.append(PackProblem.COMPONENTES_FALTANTES)
    if pack["missing_cost_variant_ids"]:
        out.append(PackProblem.COMPONENTES_SIN_COSTO)
    out.extend(w for w in pack["warnings"] if w in _PACK_WARNING_PROBLEMS)
    if spread_alert:
        out.append(PackProblem.DIFERENCIA_EXTREMA_ENTRE_LISTAS)
    return list(dict.fromkeys(out))


class PacksV2Service:
    def __init__(self, margins: MarginsV2Service, costs: CostControlV2Service) -> None:
        self._margins = margins
        self._costs = costs

    # ---------- datos ----------

    def _packs(self, cost_basis: str) -> tuple[list[dict[str, Any]], dict[int, dict[str, Any]], dict[str, Any]]:
        """(filas pack × lista, ítems de costo pack por variante, contexto con la dispersión de precios por pack)."""
        items, _ = self._costs.sku_items()
        packs = {i["variant_id"]: i for i in items if i["is_pack"]}
        rows, info = self._margins.evaluated_rows(cost_basis=cost_basis)
        by_variant: dict[int, list[dict[str, Any]]] = {}
        for r in rows:
            if r["variant_id"] in packs:
                by_variant.setdefault(r["variant_id"], []).append(r)
        out: list[dict[str, Any]] = []
        spreads: dict[int, dict[str, Any] | None] = {}
        for vid, pack_rows in by_variant.items():
            spread = spreads[vid] = list_price_spread(pack_rows)
            for r in pack_rows:
                out.append(self._row(r, packs[vid], bool(spread and spread["alert"])))
        return out, packs, {**info, "spreads": spreads}

    @staticmethod
    def _state(item: dict[str, Any]) -> str:
        return "activo" if item["variant_state"] == "activa" and item["_sort"]["product_active"] else "inactivo"

    def _row(self, r: dict[str, Any], item: dict[str, Any], spread_alert: bool) -> dict[str, Any]:
        pack = r["pack"] or {}
        problems = _row_problems(r, pack, spread_alert)
        return {
            "variant_id": r["variant_id"],
            "product_id": r["product_id"],
            "code": r["code"],
            # barcode de la variante del pack (bsale_raw.variants.bar_code vía márgenes); no usar SKU.
            "barcode": r.get("barcode"),
            "product_name": r["product_name"],
            "variant_name": r["variant_name"],
            "product_type": r.get("product_type") or {"id": None, "name": None},
            "pack_state": self._state(item),
            "pack_status": pack.get("status"),
            "components_count": pack.get("components_count", 0),
            "price_list": r["price_list"],
            "price_gross": r["price"]["gross"],
            "price_issue": r["price"]["issue"],
            "cost_net_total": pack.get("net_total"),
            "cost_gross_total": pack.get("gross_total"),
            "markup_pct": r["markup_pct"],
            "margin_on_sale_pct": r["margin_on_sale_pct"],
            "margin_status": r["status"],
            "qualifiers": r["qualifiers"],
            "risks": r["risks"],
            "reliability": r["costs"]["reliability"],
            "validity": r["validity"],
            "rule": r["rule"],
            "promotion_check": r["promotion"]["check"],
            "missing_cost_variant_ids": pack.get("missing_cost_variant_ids", []),
            "missing_variant_ids": pack.get("missing_variant_ids", []),
            "problems": problems,
            "updated_at": {
                "price_fetched_at": r["price"]["fetched_at"],
                "composition_fetched_at": (item.get("pack") or {}).get("composition_fetched_at"),
            },
            "_markup": r["_markup"],
            "_search": r["_search"],
        }

    # ---------- consultas ----------

    def list_rows(
        self,
        *,
        q: str | None = None,
        price_list_id: int | None = None,
        pack_status: str | None = None,
        margin_status: str | None = None,
        problem: str | None = None,
        pack_state: str = "active",
        cost_basis: str = DEFAULT_COST_BASIS.value,
        sort: str = "status",
        page: int = 1,
        page_size: int = 50,
    ) -> dict[str, Any]:
        if pack_state not in PACK_STATES:
            raise MarginsValidationError(f"pack_state inválido: {pack_state}")
        if pack_status is not None and pack_status not in {s.value for s in PackCostStatus}:
            raise MarginsValidationError(f"pack_status inválido: {pack_status}")
        if margin_status is not None and margin_status not in {s.value for s in S}:
            raise MarginsValidationError(f"margin_status inválido: {margin_status}")
        if problem is not None and problem not in ALL_PROBLEMS:
            raise MarginsValidationError(f"problem inválido: {problem}")
        if sort not in PACK_SORTS:
            raise MarginsValidationError(f"sort inválido: {sort}")
        rows, _, info = self._packs(cost_basis)
        if price_list_id is not None and price_list_id not in {l["id"] for l in info["price_lists"]}:
            raise MarginsValidationError("La lista no está en el alcance comercial evaluado")
        needle = normalize_label(q) if q else ""
        wanted_state = {"active": "activo", "inactive": "inactivo"}.get(pack_state)

        def keep(r: dict[str, Any]) -> bool:
            if wanted_state is not None and r["pack_state"] != wanted_state:
                return False
            if price_list_id is not None and r["price_list"]["id"] != price_list_id:
                return False
            if pack_status is not None and r["pack_status"] != pack_status:
                return False
            if margin_status is not None and r["margin_status"] != margin_status:
                return False
            if problem is not None and problem not in r["problems"]:
                return False
            if needle and needle not in r["_search"]:
                return False
            return True

        filtered = sorted((r for r in rows if keep(r)), key=_sort_key(sort))
        result = paginate(filtered, page=page, page_size=page_size)
        result["items"] = [_public(r) for r in result["items"]]
        result["sort"] = sort
        result["cost_basis"] = info["cost_basis"]
        result["price_lists"] = info["price_lists"]
        result["meta"] = {**info["meta"], "historical_profitability": "no_implementada"}
        return result

    def detail(self, variant_id: int, *, cost_basis: str = DEFAULT_COST_BASIS.value) -> dict[str, Any]:
        rows, packs, info = self._packs(cost_basis)
        item = packs.get(variant_id)
        if item is None:
            raise MarginsNotFound("Pack inexistente en la empresa")
        mine = sorted((r for r in rows if r["variant_id"] == variant_id), key=lambda r: r["price_list"]["id"])
        basis_pack = self._costs.pack_costs(CostBasis(info["cost_basis"]))[variant_id]
        pack_json = {**basis_pack.to_json(lines=True), "composition_fetched_at": (item.get("pack") or {}).get("composition_fetched_at")}
        spread = info["spreads"].get(variant_id)
        return {
            "variant_id": variant_id,
            "product_id": item["product_id"],
            "code": item["code"],
            "barcode": item.get("barcode"),
            "product_name": item["product_name"],
            "variant_name": item["variant_name"],
            "product_type": item.get("product_type") or {"id": None, "name": None},
            "pack_state": self._state(item),
            "pack": pack_json,
            "pack_sku_own": {
                "bsale_average_cost": item["bsale_average_cost"],
                "tax_ids": item["_sort"]["tax_ids"],
                "used_for_cost": False,
            },
            "rows": [_public(r) for r in mine],
            "list_price_spread": spread,
            "problems": sorted({p for r in mine for p in r["problems"]}),
            "cost_basis": info["cost_basis"],
            "meta": {**info["meta"], "historical_profitability": "no_implementada"},
        }

    def summary(self, *, cost_basis: str = DEFAULT_COST_BASIS.value, top: int = DASHBOARD_TOP_N) -> dict[str, Any]:
        rows, packs, info = self._packs(cost_basis)
        active_ids = {vid for vid, item in packs.items() if self._state(item) == "activo"}
        active_rows = [r for r in rows if r["variant_id"] in active_ids]
        by_pack_status = {s.value: 0 for s in PackCostStatus}
        for vid in active_ids:
            pack = packs[vid]["pack"] or {}
            by_pack_status[pack.get("status") or PackCostStatus.SIN_COMPOSICION.value] += 1
        by_margin = {s.value: 0 for s in S}
        problems: dict[str, set[int]] = {p: set() for p in ALL_PROBLEMS}
        for r in active_rows:
            by_margin[r["margin_status"]] += 1
            for p in r["problems"]:
                problems[p].add(r["variant_id"])
        calculable = by_pack_status[PackCostStatus.CALCULADO.value]
        total = len(active_ids)
        below_cost = sorted(
            (r for r in active_rows if PackProblem.PRECIO_BAJO_COSTO in r["problems"]),
            key=_sort_key("markup_asc"),
        )
        below_policy = sorted(
            (r for r in active_rows if r["margin_status"] == S.BAJO_POLITICA.value), key=_sort_key("markup_asc")
        )
        fetched = [
            (packs[vid].get("pack") or {}).get("composition_fetched_at")
            for vid in active_ids
            if (packs[vid].get("pack") or {}).get("composition_fetched_at")
        ]
        return {
            "packs_total": len(packs),
            "packs_active": total,
            "packs_active_calculable": calculable,
            "packs_active_pending": total - calculable,
            "coverage_pct": money((Decimal(calculable) * 100 / Decimal(total)).quantize(Decimal("0.1"))) if total else None,
            "by_pack_status": by_pack_status,
            "combinations_by_margin_status": by_margin,
            "packs_by_problem": {p: len(ids) for p, ids in problems.items()},
            "top_below_cost": [_public(r) for r in below_cost[:top]],
            "top_below_policy": [_public(r) for r in below_policy[:top]],
            "composition_fetched": {"min": min(fetched) if fetched else None, "max": max(fetched) if fetched else None},
            "cost_basis": info["cost_basis"],
            "price_lists": info["price_lists"],
            "meta": {**info["meta"], "historical_profitability": "no_implementada"},
        }


def run_packs(user: dict[str, Any], company_id: int, fn: Callable[[PacksV2Service], Any]) -> Any:
    """Misma sesión read-only y validación de empresa que Márgenes V2."""
    return run_margins(user, company_id, lambda margins: fn(PacksV2Service(margins, margins.costs)))


def _public(r: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in r.items() if not k.startswith("_")}


def _sort_key(sort: str) -> Callable[[dict[str, Any]], Any]:
    def tie(r: dict[str, Any]) -> tuple[Any, ...]:
        return (normalize_label(r.get("product_name") or ""), r["variant_id"], r["price_list"]["id"])

    if sort == "product":
        return tie
    if sort in ("markup_asc", "markup_desc"):
        sign = Decimal(1) if sort == "markup_asc" else Decimal(-1)
        return lambda r: (r["_markup"] is None, sign * (r["_markup"] or 0), *tie(r))
    return lambda r: (_STATUS_ORDER.get(r["margin_status"], 99), *tie(r))
