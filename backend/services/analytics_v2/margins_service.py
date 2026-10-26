"""Márgenes V2: SKU × lista evaluada, con costos de Control de Costos V2 y reglas comerciales.

No escribe nada: ni precios, ni costos, ni reglas. Filtros, orden y paginación en backend.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Callable

from backend.db import get_connection

logger = logging.getLogger(__name__)
from backend.services.analytics.validate_distribuidora_source import (
    make_psycopg_executor,
    open_readonly_connection,
)
from backend.services.analytics_v2.access import CompanyScope, resolve_company_scope
from backend.services.analytics_v2.commercial_scope import (
    CommercialScopeNotConfigured,
    PriceListScope,
    policy_review_status,
    resolve_price_list_scope,
    rule_in_commercial_scope,
)
from backend.services.analytics_v2.contracts import (
    CONTRACT_VERSION,
    DASHBOARD_TOP_N,
    DATA_SOURCE,
    DEFAULT_COST_BASIS,
    NET_COST_VERIFIED,
    TAX_CONFIG_VERIFIED,
    POLICIES_REVIEWED,
    PROMO_PRICE_MAX_AGE_DAYS,
    SEVERE_MARGIN_RISKS,
    CostBasis,
    CostReliability,
    DataClass,
    MarginComplianceStatus,
    MarginRisk,
    PriceListScopeStatus,
    RuleIssue,
    iso,
    local_today,
    money,
    paginate,
)
from backend.services.analytics_v2.cost_control_repo import CostControlV2Repository
from backend.services.analytics_v2.cost_control_service import CostControlV2Service
from backend.services.analytics_v2.cost_signals import to_decimal
from backend.services.analytics_v2.margin_engine import CostFacts, PriceFact, RuleFact, evaluate, rule_issues
from backend.services.analytics_v2.margins_repo import MarginsV2Repository
from backend.services.analytics_v2.movement_classification import normalize_label
from backend.services.analytics_v2.promotion_exceptions import (
    PromotionExceptionSource,
    UnavailablePromotionSource,
    resolve_exception,
)
from backend.services.analytics_v2.promotion_source import CampaignExceptionSource
from backend.services.analytics_v2.promotions_repo import PromotionsV2Repository
from backend.services.analytics_v2.stock_scope import (
    DEFAULT_STOCK_FILTER,
    STOCK_FILTERS,
    matches_stock_filter,
    normalize_stock_filter,
    resolve_stock_entry,
)

MARGINS_CACHE_TTL_SECONDS = 120
MARGIN_SORTS = frozenset({"status", "markup_asc", "markup_desc", "gap_asc", "product"})
VARIANT_STATES = frozenset({"active", "inactive", "all"})

_STATUS_ORDER = {
    MarginComplianceStatus.BAJO_POLITICA.value: 0,
    MarginComplianceStatus.REQUIERE_REVISION_TRIBUTARIA.value: 1,
    MarginComplianceStatus.COSTO_NO_VERIFICADO.value: 1,
    MarginComplianceStatus.SIN_REGLA.value: 2,
    MarginComplianceStatus.SIN_COSTO.value: 3,
    MarginComplianceStatus.PACK_NO_CALCULABLE.value: 3,
    MarginComplianceStatus.SIN_PRECIO.value: 4,
    MarginComplianceStatus.SOBRE_POLITICA.value: 5,
    MarginComplianceStatus.REMATE_VIGENTE.value: 6,
    MarginComplianceStatus.PROMOCION_VIGENTE.value: 7,
    MarginComplianceStatus.DENTRO_POLITICA.value: 8,
}


class MarginsValidationError(ValueError):
    pass


class MarginsNotFound(LookupError):
    pass


def _stock_public(stocks: dict[int, dict[str, Any]], variant_id: int) -> dict[str, Any]:
    """Bloque `stock`: SUM(quantity) global por variante; sin fila → sin sincronizar."""
    return resolve_stock_entry(stocks, variant_id)


@dataclass(slots=True)
class MarginsContext:
    scope: PriceListScope
    prices: dict[tuple[int, int], PriceFact]
    rules: list[RuleFact]
    generated_at: datetime
    # Stock físico total por variante: {variant_id: {"total": Decimal, "offices": int}}
    stocks: dict[int, dict[str, Any]] = field(default_factory=dict)
    rows_cache: dict[tuple[str, str], tuple[list[dict[str, Any]], datetime]] = field(default_factory=dict)


_cache: dict[int, tuple[float, MarginsContext]] = {}
_cache_lock = threading.Lock()


def clear_cache() -> None:
    with _cache_lock:
        _cache.clear()


def _rule_fact(row: dict[str, Any]) -> RuleFact:
    min_m = to_decimal(row.get("min_margin"))
    max_m = to_decimal(row.get("max_margin"))
    active = row.get("active") is not False
    return RuleFact(
        rule_id=int(row["id"]),
        price_list_id=int(row["price_list_id"]) if row.get("price_list_id") is not None else -1,
        product_type_id=int(row["product_type_id"]) if row.get("product_type_id") is not None else None,
        min_markup=min_m,
        max_markup=max_m,
        active=active,
        rule_key=row.get("rule_key"),
        price_list_name=row.get("price_list_name"),
        product_type_name=row.get("product_type_name"),
        review_status=policy_review_status(POLICIES_REVIEWED),
        issues=rule_issues(min_m, max_m, row.get("active")),
    )


class MarginsV2Service:
    def __init__(
        self,
        repo: MarginsV2Repository,
        costs: CostControlV2Service,
        scope: CompanyScope,
        *,
        today: date | None = None,
        use_cache: bool = True,
        promotions: PromotionExceptionSource | None = None,
    ) -> None:
        self._repo = repo
        self._costs = costs
        self._scope = scope
        self._today = today or local_today()
        self._use_cache = use_cache
        self._promotions = promotions or UnavailablePromotionSource()

    @property
    def costs(self) -> CostControlV2Service:
        return self._costs

    # ---------- contexto ----------

    def _load_context(self) -> MarginsContext:
        company_id = self._scope.company_id
        if self._use_cache:
            with _cache_lock:
                hit = _cache.get(company_id)
            if hit and time.monotonic() - hit[0] < MARGINS_CACHE_TTL_SECONDS:
                return hit[1]
        try:
            scope = resolve_price_list_scope(company_id, self._repo.price_lists(company_id=company_id))
        except CommercialScopeNotConfigured as exc:
            raise MarginsValidationError(str(exc)) from exc
        evaluated = sorted(scope.evaluated_ids)
        prices = {
            (int(r["price_list_id"]), int(r["variant_id"])): PriceFact(
                gross=to_decimal(r.get("variant_value_with_taxes")),
                net=to_decimal(r.get("variant_value")),
                missing=r.get("missing_since") is not None,
                fetched_at=r.get("api_fetched_at"),
            )
            for r in self._repo.variant_prices(company_id=company_id, price_list_ids=evaluated)
        }
        # Stock total por variante (solo lectura, misma definición que el
        # Catálogo Maestro). Es un dato complementario: si su lectura falla,
        # Márgenes no cae — el mapa queda vacío y todo se presenta «sin
        # sincronizar», nunca como cero inventado. El fallo queda REGISTRADO
        # en el log del backend para diagnóstico (empresa y causa).
        stock_rows: list[dict[str, Any]] = []
        try:
            stock_rows = self._repo.stock_totals(company_id=company_id)
        except Exception:
            logger.warning(
                "margins: no se pudo leer el stock total (bsale_raw.stocks) "
                "para company_id=%s; se presenta como «sin sincronizar»",
                company_id,
                exc_info=True,
            )
            stock_rows = []
        stocks = {
            int(r["variant_id"]): {
                "total": to_decimal(r.get("stock_total")),
                "offices": int(r.get("stock_offices") or 0),
            }
            for r in stock_rows
        }
        ctx = MarginsContext(
            scope=scope,
            prices=prices,
            rules=[_rule_fact(r) for r in self._repo.margin_rules(company_id=company_id)],
            generated_at=datetime.now().astimezone(),
            stocks=stocks,
        )
        if self._use_cache:
            with _cache_lock:
                _cache[company_id] = (time.monotonic(), ctx)
        return ctx

    def _meta(self, ctx: MarginsContext, cost_generated_at: datetime | None = None) -> dict[str, Any]:
        return {
            "contract_version": CONTRACT_VERSION,
            "data_source": DATA_SOURCE,
            "company_id": self._scope.company_id,
            "company_name": self._scope.company_name,
            "generated_at": iso(ctx.generated_at),
            "costs_generated_at": iso(cost_generated_at),
            "policies_reviewed": POLICIES_REVIEWED,
            "net_cost_verified": NET_COST_VERIFIED,
            "tax_config_verified": TAX_CONFIG_VERIFIED,
            "promotions_integrated": bool(self._promotions.available),
            "evaluated_price_list_ids": sorted(ctx.scope.evaluated_ids),
            "scope_discrepancies": len(ctx.scope.discrepancies),
        }

    @staticmethod
    def _rule_index(rules: list[RuleFact]) -> dict[tuple[int, int | None], RuleFact]:
        return {(r.price_list_id, r.product_type_id): r for r in rules}

    # ---------- filas ----------

    def _rows(self, ctx: MarginsContext, cost_basis: CostBasis, variant_state: str) -> tuple[list[dict[str, Any]], datetime]:
        key = (cost_basis.value, variant_state)
        if key not in ctx.rows_cache:
            ctx.rows_cache[key] = self._build_rows(ctx, cost_basis, variant_state)
        return ctx.rows_cache[key]

    def _build_rows(self, ctx: MarginsContext, cost_basis: CostBasis, variant_state: str) -> tuple[list[dict[str, Any]], datetime]:
        items, cost_generated_at = self._costs.sku_items()
        pack_costs = self._costs.pack_costs(cost_basis) if any(i["is_pack"] for i in items) else {}
        index = self._rule_index(ctx.rules)
        exceptions = list(self._promotions.exceptions_for(self._scope.company_id)) if self._promotions.available else []
        lists = {e.price_list_id: e for e in ctx.scope.entries}
        rows: list[dict[str, Any]] = []
        for item in items:
            if variant_state != "all" and item["variant_state"] != ("activa" if variant_state == "active" else "inactiva"):
                continue
            s = item["_sort"]
            cost = CostFacts(
                average_net=s["average_cost"],
                last_comparable_net=s["lc_cost"],
                reliability=CostReliability(item["reliability"]["status"]),
                tax_profile=s["tax_profile"],
                anomaly_codes=tuple(a["code"] for a in item["anomalies"]),
                pack=pack_costs.get(item["variant_id"]) if item["is_pack"] else None,
            )
            type_id = item["product_type"]["id"]
            for list_id in sorted(ctx.scope.evaluated_ids):
                price = ctx.prices.get((list_id, item["variant_id"]))
                rule, match = None, None
                if type_id is not None and (list_id, int(type_id)) in index:
                    rule, match = index[(list_id, int(type_id))], "tipo_producto"
                elif (list_id, None) in index:
                    rule, match = index[(list_id, None)], "general_lista"
                promotion = resolve_exception(
                    source_available=bool(self._promotions.available),
                    candidates=exceptions,
                    company_id=self._scope.company_id,
                    variant_id=item["variant_id"],
                    price_list_id=list_id,
                    bsale_gross_price=price.gross if price and not price.missing else None,
                    today=self._today,
                    bsale_price_fetched_at=price.fetched_at if price else None,
                    max_price_age_days=PROMO_PRICE_MAX_AGE_DAYS,
                )
                result = evaluate(
                    price=price,
                    cost=cost,
                    rule=rule,
                    rule_match=match,
                    cost_basis=cost_basis,
                    promotion=promotion,
                )
                entry = lists[list_id]
                result.update(
                    {
                        "variant_id": item["variant_id"],
                        "product_id": item["product_id"],
                        "code": item["code"],
                        "barcode": item["barcode"],
                        "product_name": item["product_name"],
                        "variant_name": item["variant_name"],
                        "variant_state": item["variant_state"],
                        "product_type": item["product_type"],
                        "is_pack": item["is_pack"],
                        "price_list": {
                            "id": list_id,
                            "name": entry.name,
                            "label": entry.visible_name if entry else None,
                        },
                        # Stock por variante (no por fila): el mismo valor se
                        # repite en cada lista sin multiplicar existencias.
                        "stock": _stock_public(ctx.stocks, item["variant_id"]),
                        "_search": s["search"],
                    }
                )
                rows.append(result)
        return rows, cost_generated_at

    @staticmethod
    def _public(row: dict[str, Any]) -> dict[str, Any]:
        return {k: v for k, v in row.items() if not k.startswith("_")}

    def _validate_common(self, cost_basis: str, variant_state: str) -> CostBasis:
        if cost_basis not in {c.value for c in CostBasis}:
            raise MarginsValidationError(f"cost_basis inválido: {cost_basis}")
        if variant_state not in VARIANT_STATES:
            raise MarginsValidationError(f"variant_state inválido: {variant_state}")
        return CostBasis(cost_basis)

    def list_rows(
        self,
        *,
        q: str | None = None,
        price_list_id: int | None = None,
        product_type_id: int | None = None,
        status: str | None = None,
        qualifier: str | None = None,
        reliability: str | None = None,
        stock_filter: str | None = None,
        cost_basis: str = DEFAULT_COST_BASIS.value,
        variant_state: str = "active",
        sort: str = "status",
        page: int = 1,
        page_size: int = 50,
    ) -> dict[str, Any]:
        basis = self._validate_common(cost_basis, variant_state)
        statuses = {s.value for s in MarginComplianceStatus}
        if status is not None and status not in statuses:
            raise MarginsValidationError(f"status inválido: {status}")
        if qualifier is not None and qualifier not in statuses:
            raise MarginsValidationError(f"qualifier inválido: {qualifier}")
        if reliability is not None and reliability not in {r.value for r in CostReliability}:
            raise MarginsValidationError(f"reliability inválido: {reliability}")
        try:
            stock_scope = normalize_stock_filter(stock_filter)
        except ValueError as exc:
            raise MarginsValidationError(str(exc)) from exc
        if sort not in MARGIN_SORTS:
            raise MarginsValidationError(f"sort inválido: {sort}")
        ctx = self._load_context()
        if price_list_id is not None and price_list_id not in ctx.scope.evaluated_ids:
            raise MarginsValidationError("La lista no está en el alcance comercial evaluado")
        rows, cost_generated_at = self._rows(ctx, basis, variant_state)
        needle = normalize_label(q) if q else ""

        def keep(r: dict[str, Any]) -> bool:
            if price_list_id is not None and r["price_list"]["id"] != price_list_id:
                return False
            if product_type_id is not None and r["product_type"]["id"] != product_type_id:
                return False
            if status is not None and r["status"] != status:
                return False
            if qualifier is not None and qualifier not in r["qualifiers"]:
                return False
            if reliability is not None and r["costs"]["reliability"] != reliability:
                return False
            # Filtro ANTES de paginar (total, tabla, exports). Sin fila = SIN STOCK.
            if not matches_stock_filter(state=(r.get("stock") or {}).get("state"), stock_filter=stock_scope):
                return False
            if needle and needle not in r["_search"]:
                return False
            return True

        filtered = [r for r in rows if keep(r)]
        filtered.sort(key=_sort_key(sort))
        result = paginate(filtered, page=page, page_size=page_size)
        result["items"] = [self._public(r) for r in result["items"]]
        result["sort"] = sort
        result["cost_basis"] = basis.value
        result["stock_filter"] = stock_scope
        result["meta"] = {
            **self._meta(ctx, cost_generated_at),
            "stock_filter": stock_scope,
            "stock_filter_default": DEFAULT_STOCK_FILTER,
            "stock_criterion": "SUM(quantity) global por company_id+variant_id",
        }
        return result

    def summary(self, *, cost_basis: str = DEFAULT_COST_BASIS.value) -> dict[str, Any]:
        basis = self._validate_common(cost_basis, "active")
        ctx = self._load_context()
        rows, cost_generated_at = self._rows(ctx, basis, "active")
        statuses = [s.value for s in MarginComplianceStatus]
        by_list: dict[int, dict[str, Any]] = {}
        for list_id in sorted(ctx.scope.evaluated_ids):
            entry = ctx.scope.entry(list_id)
            by_list[list_id] = {
                "price_list_id": list_id,
                "name": entry.name if entry else None,
                "label": entry.visible_name if entry else None,
                "rows": 0,
                "by_status": {s: 0 for s in statuses},
                "by_qualifier": {},
                "confirmed": 0,
                "provisional": 0,
            }
        totals = {s: 0 for s in statuses}
        qualifiers: dict[str, int] = {}
        for r in rows:
            bucket = by_list[r["price_list"]["id"]]
            bucket["rows"] += 1
            bucket["by_status"][r["status"]] += 1
            totals[r["status"]] += 1
            for q in r["qualifiers"]:
                bucket["by_qualifier"][q] = bucket["by_qualifier"].get(q, 0) + 1
                qualifiers[q] = qualifiers.get(q, 0) + 1
            if r["confirmation"] == "confirmado":
                bucket["confirmed"] += 1
            if r["validity"]["status"] == "provisional":
                bucket["provisional"] += 1
        policy = self._policy_counts(ctx, rows)
        return {
            "rows_total": len(rows),
            "by_status": totals,
            "by_qualifier": qualifiers,
            "by_price_list": list(by_list.values()),
            "policy": policy,
            "scope": ctx.scope.to_json(),
            "cost_basis": basis.value,
            "meta": self._meta(ctx, cost_generated_at),
        }

    def _missing_rules(self, ctx: MarginsContext, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        missing: dict[tuple[int, Any], dict[str, Any]] = {}
        for r in rows:
            if r["rule"] is not None or r["price"]["issue"] is not None:
                continue
            key = (r["price_list"]["id"], r["product_type"]["id"])
            bucket = missing.setdefault(
                key,
                {
                    "price_list_id": key[0],
                    "price_list_name": r["price_list"]["name"],
                    "product_type_id": key[1],
                    "product_type_name": r["product_type"]["name"],
                    "priced_active_skus": 0,
                },
            )
            bucket["priced_active_skus"] += 1
        return sorted(missing.values(), key=lambda m: (m["price_list_id"], m["product_type_id"] is None, m["product_type_id"] or 0))

    def _policy_counts(self, ctx: MarginsContext, rows: list[dict[str, Any]]) -> dict[str, Any]:
        in_scope = [r for r in ctx.rules if rule_in_commercial_scope(r.price_list_id, ctx.scope)]
        return {
            "rules_total": len(ctx.rules),
            "rules_in_scope": len(in_scope),
            "rules_out_of_scope": len(ctx.rules) - len(in_scope),
            "rules_pending_review": sum(1 for r in in_scope if r.review_status.value == "pendiente_revision"),
            "rules_max_zero_in_scope": sum(1 for r in in_scope if RuleIssue.MAXIMO_CERO in r.issues),
            "rules_with_issues_in_scope": sum(1 for r in in_scope if r.issues),
            "missing_rule_combinations": len(self._missing_rules(ctx, rows)),
        }

    def overview(self, *, cost_basis: str = DEFAULT_COST_BASIS.value, top: int = DASHBOARD_TOP_N) -> dict[str, Any]:
        """Indicadores para el Dashboard V2: combinaciones SKU × lista y SKUs únicos (variantes activas)."""
        basis = self._validate_common(cost_basis, "active")
        ctx = self._load_context()
        rows, cost_generated_at = self._rows(ctx, basis, "active")
        S = MarginComplianceStatus
        excepted = {S.PROMOCION_VIGENTE.value, S.REMATE_VIGENTE.value}

        def counter() -> dict[str, Any]:
            return {"combinations": 0, "_skus": set()}

        def add(bucket: dict[str, Any], row: dict[str, Any]) -> None:
            bucket["combinations"] += 1
            bucket["_skus"].add(row["variant_id"])

        by_status = {s.value: counter() for s in S}
        by_class = {c.value: counter() for c in DataClass}
        by_risk = {r.value: counter() for r in MarginRisk}
        alerts, pending_verification, exceptions, exception_risks = counter(), counter(), counter(), counter()
        lists: dict[int, dict[str, Any]] = {}
        for list_id in sorted(ctx.scope.evaluated_ids):
            entry = ctx.scope.entry(list_id)
            fetched = [p.fetched_at for (lid, _), p in ctx.prices.items() if lid == list_id and not p.missing and p.fetched_at]
            lists[list_id] = {
                "price_list_id": list_id,
                "name": entry.name if entry else None,
                "label": entry.visible_name if entry else None,
                "combinations": 0,
                "priced": 0,
                "alerts": 0,
                "within": 0,
                "above": 0,
                "exceptions": 0,
                "unavailable": 0,
                "tax_review": 0,
                "prices_fetched_min": iso(min(fetched)) if fetched else None,
                "prices_fetched_max": iso(max(fetched)) if fetched else None,
            }
        all_skus: set[int] = set()
        for r in rows:
            all_skus.add(r["variant_id"])
            add(by_status[r["status"]], r)
            klass = data_class(r)
            add(by_class[klass.value], r)
            for risk in r["risks"]:
                add(by_risk[risk], r)
            bucket = lists[r["price_list"]["id"]]
            bucket["combinations"] += 1
            if r["price"]["issue"] is None:
                bucket["priced"] += 1
            if klass is DataClass.NO_DISPONIBLE:
                bucket["unavailable"] += 1
            if r["status"] == S.REQUIERE_REVISION_TRIBUTARIA.value:
                bucket["tax_review"] += 1
            if r["status"] == S.BAJO_POLITICA.value:
                add(alerts, r)
                bucket["alerts"] += 1
                if S.PROMOCION_PENDIENTE_VERIFICACION.value in r["qualifiers"]:
                    add(pending_verification, r)
            elif r["status"] == S.DENTRO_POLITICA.value:
                bucket["within"] += 1
            elif r["status"] == S.SOBRE_POLITICA.value:
                bucket["above"] += 1
            elif r["status"] in excepted:
                add(exceptions, r)
                bucket["exceptions"] += 1
                if _severe(r):
                    add(exception_risks, r)

        def done(bucket: dict[str, Any]) -> dict[str, Any]:
            return {"combinations": bucket["combinations"], "skus": len(bucket["_skus"])}

        alert_rows = sorted((r for r in rows if r["status"] == S.BAJO_POLITICA.value), key=_sort_key("gap_asc"))
        risky_exceptions = sorted(
            (r for r in rows if r["status"] in excepted and _severe(r)),
            key=lambda r: (MarginRisk.VENTA_BAJO_COSTO.value not in r["risks"], *_sort_key("product")(r)),
        )
        return {
            "cost_basis": basis.value,
            "totals": {"combinations": len(rows), "skus": len(all_skus), "price_lists": len(lists)},
            "by_status": {k: done(v) for k, v in by_status.items()},
            "by_data_class": {k: done(v) for k, v in by_class.items()},
            "by_risk": {k: done(v) for k, v in by_risk.items()},
            "alerts": done(alerts),
            "alerts_pending_promotion_verification": done(pending_verification),
            "promotion_exceptions": done(exceptions),
            "promotion_exceptions_with_risk": done(exception_risks),
            "by_price_list": list(lists.values()),
            "top_alerts": [_compact(r) for r in alert_rows[:top]],
            "top_exception_risks": [_compact(r) for r in risky_exceptions[:top]],
            "policy": self._policy_counts(ctx, rows),
            "scope": {
                "evaluated": [
                    {"price_list_id": e.price_list_id, "name": e.name, "label": e.visible_name}
                    for e in ctx.scope.entries
                    if e.status is PriceListScopeStatus.EVALUADA
                ],
                "excluded": [
                    {"price_list_id": e.price_list_id, "name": e.name, "status": e.status.value}
                    for e in ctx.scope.entries
                    if e.status is not PriceListScopeStatus.EVALUADA
                ],
                "discrepancies": len(ctx.scope.discrepancies),
            },
            "meta": self._meta(ctx, cost_generated_at),
        }

    def policy_review(self, *, only_issues: bool = False) -> dict[str, Any]:
        ctx = self._load_context()
        rows, cost_generated_at = self._rows(ctx, DEFAULT_COST_BASIS, "active")
        out = []
        for r in ctx.rules:
            entry = ctx.scope.entry(r.price_list_id)
            in_scope = rule_in_commercial_scope(r.price_list_id, ctx.scope)
            if only_issues and in_scope and not r.issues and r.review_status.value == "validada":
                continue
            out.append(
                {
                    "rule_id": r.rule_id,
                    "rule_key": r.rule_key,
                    "price_list_id": r.price_list_id,
                    "price_list_name": r.price_list_name,
                    "price_list_scope": entry.status.value if entry else PriceListScopeStatus.NO_VIGENTE.value,
                    "in_commercial_scope": in_scope,
                    "product_type_id": r.product_type_id,
                    "product_type_name": r.product_type_name,
                    "min_markup_pct": money(r.min_markup),
                    "max_markup_pct": money(r.max_markup),
                    "active": r.active,
                    "review_status": r.review_status.value,
                    "issues": [i.value for i in r.issues],
                    "alerts_enabled": in_scope,
                }
            )
        return {
            "rules": out,
            "missing_rules": self._missing_rules(ctx, rows),
            "policy": self._policy_counts(ctx, rows),
            "scope": ctx.scope.to_json(),
            "meta": self._meta(ctx, cost_generated_at),
        }

    def scope(self) -> dict[str, Any]:
        ctx = self._load_context()
        return {"scope": ctx.scope.to_json(), "meta": self._meta(ctx)}

    def filters(self) -> dict[str, Any]:
        ctx = self._load_context()
        return {
            "price_lists": [
                {"id": e.price_list_id, "name": e.name, "label": e.visible_name}
                for e in ctx.scope.entries
                if e.status is PriceListScopeStatus.EVALUADA
            ],
            "product_types": [
                {"id": int(r["product_type_id"]), "name": r.get("name"), "state": r.get("state")}
                for r in self._costs.product_types()
            ],
            "statuses": [s.value for s in MarginComplianceStatus],
            "cost_bases": [c.value for c in CostBasis],
            "reliability_statuses": [r.value for r in CostReliability],
            "sorts": sorted(MARGIN_SORTS),
            "meta": self._meta(ctx),
        }

    def evaluated_rows(self, *, cost_basis: str = DEFAULT_COST_BASIS.value) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        """Filas internas SKU × lista (todas las variantes; solo lectura) para otros motores V2 de la misma empresa."""
        basis = self._validate_common(cost_basis, "all")
        ctx = self._load_context()
        rows, cost_generated_at = self._rows(ctx, basis, "all")
        lists = [
            {"id": e.price_list_id, "name": e.name, "label": e.visible_name}
            for e in ctx.scope.entries
            if e.status is PriceListScopeStatus.EVALUADA
        ]
        return rows, {"meta": self._meta(ctx, cost_generated_at), "price_lists": lists, "cost_basis": basis.value}

    def product(self, variant_id: int, *, cost_basis: str = DEFAULT_COST_BASIS.value) -> dict[str, Any]:
        basis = self._validate_common(cost_basis, "all")
        ctx = self._load_context()
        rows, cost_generated_at = self._rows(ctx, basis, "all")
        mine = [self._public(r) for r in rows if r["variant_id"] == variant_id]
        if not mine:
            raise MarginsNotFound("SKU inexistente en la empresa")
        return {"variant_id": variant_id, "rows": mine, "cost_basis": basis.value, "meta": self._meta(ctx, cost_generated_at)}


_UNAVAILABLE_STATUSES = frozenset(
    {
        MarginComplianceStatus.SIN_PRECIO.value,
        MarginComplianceStatus.SIN_COSTO.value,
        MarginComplianceStatus.SIN_REGLA.value,
        MarginComplianceStatus.COSTO_NO_VERIFICADO.value,
        MarginComplianceStatus.REQUIERE_REVISION_TRIBUTARIA.value,
        MarginComplianceStatus.PACK_NO_CALCULABLE.value,
    }
)


def data_class(row: dict[str, Any]) -> DataClass:
    """No disponible: sin precio, costo o regla comparable. Confirmado: sin calificadores y política/costo validados."""
    if row["status"] in _UNAVAILABLE_STATUSES:
        return DataClass.NO_DISPONIBLE
    if row["confirmation"] == "confirmado" and row["validity"]["status"] == "validado":
        return DataClass.CONFIRMADO
    return DataClass.PROVISIONAL


def _severe(row: dict[str, Any]) -> bool:
    return any(risk in SEVERE_MARGIN_RISKS for risk in row["risks"])


def _compact(r: dict[str, Any]) -> dict[str, Any]:
    return {
        "variant_id": r["variant_id"],
        "code": r["code"],
        "barcode": r.get("barcode"),
        "product_name": r["product_name"],
        "variant_name": r["variant_name"],
        "product_type": r.get("product_type") or {"id": None, "name": None},
        "price_list": r["price_list"],
        "status": r["status"],
        "data_class": data_class(r).value,
        "qualifiers": r["qualifiers"],
        "risks": r["risks"],
        "price_gross": r["price"]["gross"],
        "cost_reference_net": r["costs"]["reference"]["net"],
        "cost_reference_gross": r["costs"]["reference"]["gross"],
        "tax_status": r["costs"]["reference"]["tax_status"],
        "reliability": r["costs"]["reliability"],
        "markup_pct": r["markup_pct"],
        "min_markup_pct": r["rule"]["min_markup_pct"] if r["rule"] else None,
        "gap_to_min_pct": r["gap_to_min_pct"],
        "promotion_check": r["promotion"]["check"],
        "campaign_name": (r["promotion"].get("exception") or {}).get("campaign_name"),
    }


def _sort_key(sort: str) -> Callable[[dict[str, Any]], Any]:
    def name(r: dict[str, Any]) -> str:
        return normalize_label(r.get("product_name") or "")

    def tie(r: dict[str, Any]) -> tuple[Any, ...]:
        return (name(r), r["variant_id"], r["price_list"]["id"])

    if sort == "product":
        return tie
    if sort in ("markup_asc", "markup_desc"):
        sign = Decimal(1) if sort == "markup_asc" else Decimal(-1)
        return lambda r: (r["_markup"] is None, sign * (r["_markup"] or 0), *tie(r))
    if sort == "gap_asc":
        return lambda r: (r["gap_to_min_pct"] is None, Decimal(r["gap_to_min_pct"] or 0), *tie(r))
    return lambda r: (_STATUS_ORDER.get(r["status"], 99), *tie(r))


def run_margins(
    user: dict[str, Any],
    company_id: int,
    fn: Callable[[MarginsV2Service], Any],
    *,
    get_conn: Callable[[], Any] = get_connection,
) -> Any:
    """Sesión read-only, empresa validada contra el usuario; costos y promociones de la misma empresa."""
    conn = open_readonly_connection(get_conn)
    try:
        executor = make_psycopg_executor(conn, statement_timeout_seconds=25, lock_timeout="3s")
        scope = resolve_company_scope(user, company_id, executor)
        costs = CostControlV2Service(CostControlV2Repository(executor), scope)
        today = local_today()
        promotions = CampaignExceptionSource(PromotionsV2Repository(executor), today=today)
        return fn(MarginsV2Service(MarginsV2Repository(executor), costs, scope, today=today, promotions=promotions))
    finally:
        try:
            conn.rollback()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass
