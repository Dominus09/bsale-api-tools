"""Control de Costos V2: contratos de respuesta sobre BSALE RAW (solo lectura).

Distingue siempre:
- costo promedio Bsale (variant_costs.average_cost, original, nunca recalculado);
- costo de recepción (stock_reception_details.cost, un movimiento concreto);
- costo bruto comercial: promedio Bsale × factor tributario RAW (``tax_profile``), provisional
  mientras la configuración tributaria no se contraste con documentos.
"""

from __future__ import annotations

import statistics
import threading
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Callable

from backend.db import get_connection
from backend.services.analytics.validate_distribuidora_source import (
    make_psycopg_executor,
    open_readonly_connection,
)
from backend.services.analytics_v2.access import CompanyScope, resolve_company_scope
from backend.services.analytics_v2.contracts import (
    CONTRACT_VERSION,
    DASHBOARD_ALERT_DAYS,
    DASHBOARD_TOP_N,
    DATA_SOURCE,
    DECREASE_ALERT_PCT,
    EXTREME_QUANTITY_FACTOR,
    HISTORY_SERIES_MAX_POINTS,
    HISTORY_START,
    INCREASE_ALERT_PCT,
    NET_COST_VERIFIED,
    TAX_CONFIG_VERIFIED,
    CONFIRMED_TAX_ROLES,
    VERIFIED_EXEMPT_PRODUCT_IDS,
    AnomalyCode,
    CostReliability,
    GrossCostStatus,
    MovementClass,
    Severity,
    TaxProfileStatus,
    TaxValidation,
    iso,
    money,
    paginate,
)
from backend.services.analytics_v2.cost_control_repo import CostControlV2Repository
from backend.services.analytics_v2.cost_signals import (
    Anomaly,
    LineFacts,
    average_divergence,
    change_pct,
    is_comparable_line,
    line_anomalies,
    sku_reliability,
    to_decimal,
)
from backend.services.analytics_v2.movement_classification import normalize_label
from backend.services.analytics_v2.tax_profile import (
    TaxEntry,
    build_tax_catalog,
    commercial_gross_cost,
    resolve_tax_profile,
)

FACTS_CACHE_TTL_SECONDS = 120

PRODUCT_SORTS = frozenset(
    {"product", "change_pct_desc", "change_pct_asc", "last_date_desc", "average_cost_desc", "reliability"}
)
VARIANT_STATES = frozenset({"active", "inactive", "all"})

_RELIABILITY_ORDER = {
    CostReliability.REQUIERE_REVISION: 0,
    CostReliability.SIN_COSTO: 1,
    CostReliability.SOLO_PROMEDIO_BSALE: 2,
    CostReliability.DESACTUALIZADO: 3,
    CostReliability.CONSISTENTE: 4,
}
_SEVERITY_ORDER = {Severity.ALTA: 0, Severity.MEDIA: 1, Severity.BAJA: 2}


class CostControlValidationError(ValueError):
    pass


class CostControlNotFound(LookupError):
    pass


@dataclass(slots=True)
class CompanyContext:
    facts: list[dict[str, Any]]
    tax_catalog: dict[int, TaxEntry]
    offices: dict[int, str]
    generated_at: datetime
    verified_exempt: frozenset[int] = frozenset()
    # Ítems ya evaluados por fecha (la confiabilidad depende de "hoy"); solo lectura para los consumidores.
    items_cache: dict[date, list[dict[str, Any]]] = field(default_factory=dict)


_cache: dict[int, tuple[float, CompanyContext]] = {}
_cache_lock = threading.Lock()


def clear_cache() -> None:
    with _cache_lock:
        _cache.clear()


def _variant_state_label(state: Any) -> str:
    if state is None:
        return "desconocido"
    return "activa" if int(state) == 0 else "inactiva"


def _line_json(row: dict[str, Any], prefix: str, offices: dict[int, str]) -> dict[str, Any] | None:
    if row.get(f"{prefix}reception_id") is None:
        return None
    office_id = row.get(f"{prefix}office_id")
    return {
        "reception_id": int(row[f"{prefix}reception_id"]),
        "detail_id": int(row[f"{prefix}detail_id"]),
        "admission_date": iso(row.get(f"{prefix}admission_date")),
        "office_id": office_id,
        "office_name": offices.get(int(office_id)) if office_id is not None else None,
        "document": row.get(f"{prefix}document"),
        "document_number": row.get(f"{prefix}document_number"),
        "movement_class": row.get(f"{prefix}movement_class"),
        "cost": money(to_decimal(row.get(f"{prefix}cost"))),
        "quantity": money(to_decimal(row.get(f"{prefix}quantity"))),
    }


class CostControlV2Service:
    def __init__(
        self,
        repo: CostControlV2Repository,
        scope: CompanyScope,
        *,
        today: date | None = None,
        use_cache: bool = True,
    ) -> None:
        self._repo = repo
        self._scope = scope
        self._today = today or date.today()
        self._use_cache = use_cache

    # ---------- contexto por empresa ----------

    def _load_context(self) -> CompanyContext:
        company_id = self._scope.company_id
        if self._use_cache:
            with _cache_lock:
                hit = _cache.get(company_id)
            if hit and time.monotonic() - hit[0] < FACTS_CACHE_TTL_SECONDS:
                return hit[1]
        ctx = CompanyContext(
            facts=self._repo.variant_facts(company_id=company_id, history_start=HISTORY_START),
            tax_catalog=build_tax_catalog(
                self._repo.tax_catalog(company_id=company_id), CONFIRMED_TAX_ROLES.get(company_id)
            ),
            offices={
                int(r["office_id"]): str(r.get("name") or f"Oficina {r['office_id']}")
                for r in self._repo.offices(company_id=company_id)
            },
            generated_at=datetime.now(timezone.utc),
            verified_exempt=VERIFIED_EXEMPT_PRODUCT_IDS.get(company_id, frozenset()),
        )
        if self._use_cache:
            with _cache_lock:
                _cache[company_id] = (time.monotonic(), ctx)
        return ctx

    def _meta(self, ctx: CompanyContext | None = None) -> dict[str, Any]:
        return {
            "contract_version": CONTRACT_VERSION,
            "data_source": DATA_SOURCE,
            "company_id": self._scope.company_id,
            "company_name": self._scope.company_name,
            "history_start": HISTORY_START.isoformat(),
            "net_cost_verified": NET_COST_VERIFIED,
            "tax_config_verified": TAX_CONFIG_VERIFIED,
            "generated_at": iso(ctx.generated_at) if ctx else iso(datetime.now(timezone.utc)),
        }

    # ---------- SKU ----------

    def _build_item(self, row: dict[str, Any], ctx: CompanyContext) -> dict[str, Any]:
        average_cost = to_decimal(row.get("average_cost"))
        lc_cost = to_decimal(row.get("lc_cost"))
        pc_cost = to_decimal(row.get("pc_cost"))
        median_qty = to_decimal(row.get("median_comparable_quantity"))
        lc_movement = row.get("lc_movement_class")

        anomalies: list[Anomaly] = []
        if lc_movement is not None:
            anomalies = line_anomalies(
                LineFacts(
                    movement_class=MovementClass(lc_movement),
                    cost=lc_cost,
                    quantity=to_decimal(row.get("lc_quantity")),
                    details_complete=row.get("lc_details_complete"),
                    previous_comparable_cost=pc_cost,
                    median_comparable_quantity=median_qty,
                    cost_modified=bool(row.get("lc_cost_modified")),
                    quantity_modified=bool(row.get("lc_quantity_modified")),
                    variant_modified=bool(row.get("lc_variant_modified")),
                )
            )
        divergence = average_divergence(lc_cost, average_cost)
        if divergence:
            anomalies.append(divergence)

        lc_date = row.get("lc_admission_date")
        reliability, reliability_reason = sku_reliability(
            average_cost=average_cost,
            last_comparable_cost=lc_cost,
            last_comparable_date=lc_date,
            last_comparable_anomalies=anomalies,
            today=self._today,
        )

        tax_ids = row.get("tax_ids")
        product_id = row.get("product_id")
        profile = resolve_tax_profile(
            [int(t) for t in tax_ids] if tax_ids is not None else None,
            ctx.tax_catalog,
            verified_exempt=product_id is not None and int(product_id) in ctx.verified_exempt,
            config_stale=bool(row.get("tax_config_stale")),
        )
        commercial = commercial_gross_cost(average_cost, profile)
        reception_base = lc_cost if lc_cost is not None else average_cost
        reception_gross = commercial_gross_cost(reception_base, profile)

        amount = (lc_cost - pc_cost) if lc_cost is not None and pc_cost is not None else None
        pct = change_pct(lc_cost, pc_cost)
        return {
            "variant_id": int(row["variant_id"]),
            "product_id": row.get("product_id"),
            "code": row.get("code"),
            "barcode": row.get("bar_code"),
            "product_name": row.get("product_name"),
            "variant_name": row.get("variant_name"),
            "product_type": {"id": row.get("product_type_id"), "name": row.get("product_type_name")},
            "variant_state": _variant_state_label(row.get("variant_state")),
            "bsale_average_cost": {
                "value": money(average_cost),
                "fetched_at": iso(row.get("average_cost_fetched_at")),
                "source": "bsale_raw.variant_costs",
            },
            "last_reception": _line_json(row, "last_", ctx.offices),
            "last_comparable": _line_json(row, "lc_", ctx.offices),
            "previous_comparable": _line_json(row, "pc_", ctx.offices),
            "change": {"amount": money(amount), "pct": money(pct)} if amount is not None else None,
            "tax_profile": profile.to_json(),
            "commercial_gross_cost": {**commercial.to_json(), "base": "promedio_bsale"},
            "gross_cost_preliminary": {
                "value": money(reception_gross.gross),
                "status": reception_gross.status.value,
                "base": "ultimo_costo_comparable" if lc_cost is not None else "promedio_bsale",
            },
            "reliability": {"status": reliability.value, "reason": reliability_reason},
            "anomalies": [a.to_json() for a in anomalies],
            "counts": {
                "lines_total": int(row.get("lines_total") or 0),
                "lines_comparable": int(row.get("lines_comparable") or 0),
                "receptions_total": int(row.get("receptions_total") or 0),
                "first_admission_date": iso(row.get("first_admission_date")),
            },
            "_sort": {
                "pct": pct,
                "last_date": row.get("last_admission_date"),
                "average_cost": average_cost,
                "reliability": reliability,
                "lc_office_id": row.get("lc_office_id"),
                "lc_cost": lc_cost,
                "tax_profile": profile,
                "commercial_gross": commercial,
                "tax_ids": sorted({int(t) for t in tax_ids}) if tax_ids is not None else None,
                "search": normalize_label(
                    " ".join(
                        str(x)
                        for x in (row.get("code"), row.get("bar_code"), row.get("product_name"), row.get("variant_name"))
                        if x
                    )
                ),
            },
        }

    def _all_items(self, ctx: CompanyContext) -> list[dict[str, Any]]:
        items = ctx.items_cache.get(self._today)
        if items is None:
            items = [self._build_item(row, ctx) for row in ctx.facts]
            ctx.items_cache.clear()
            ctx.items_cache[self._today] = items
        return items

    def sku_items(self) -> tuple[list[dict[str, Any]], datetime]:
        """Hechos de costo por SKU (con ``_sort`` interno) para otros motores V2 de la misma empresa."""
        ctx = self._load_context()
        return self._all_items(ctx), ctx.generated_at

    def product_types(self) -> list[dict[str, Any]]:
        return self._repo.product_types(company_id=self._scope.company_id)

    @staticmethod
    def _public(item: dict[str, Any]) -> dict[str, Any]:
        return {k: v for k, v in item.items() if k != "_sort"}

    def list_products(
        self,
        *,
        q: str | None = None,
        product_type_id: int | None = None,
        variant_state: str = "active",
        reliability: str | None = None,
        tax_status: str | None = None,
        last_movement_class: str | None = None,
        anomaly_code: str | None = None,
        office_id: int | None = None,
        min_abs_change_pct: Decimal | None = None,
        sort: str = "reliability",
        page: int = 1,
        page_size: int = 50,
    ) -> dict[str, Any]:
        if sort not in PRODUCT_SORTS:
            raise CostControlValidationError(f"sort inválido: {sort}")
        if variant_state not in VARIANT_STATES:
            raise CostControlValidationError(f"variant_state inválido: {variant_state}")
        if reliability is not None and reliability not in {r.value for r in CostReliability}:
            raise CostControlValidationError(f"reliability inválido: {reliability}")
        if last_movement_class is not None and last_movement_class not in {m.value for m in MovementClass}:
            raise CostControlValidationError(f"movement_class inválido: {last_movement_class}")
        if anomaly_code is not None and anomaly_code not in {a.value for a in AnomalyCode}:
            raise CostControlValidationError(f"anomaly_code inválido: {anomaly_code}")
        if tax_status is not None and tax_status not in {t.value for t in TaxProfileStatus}:
            raise CostControlValidationError(f"tax_status inválido: {tax_status}")

        ctx = self._load_context()
        items = self._all_items(ctx)
        needle = normalize_label(q) if q else ""

        def keep(item: dict[str, Any]) -> bool:
            s = item["_sort"]
            if variant_state != "all" and item["variant_state"] != ("activa" if variant_state == "active" else "inactiva"):
                return False
            if needle and needle not in s["search"]:
                return False
            if product_type_id is not None and item["product_type"]["id"] != product_type_id:
                return False
            if reliability is not None and item["reliability"]["status"] != reliability:
                return False
            if tax_status is not None and item["tax_profile"]["status"] != tax_status:
                return False
            if last_movement_class is not None and (
                item["last_reception"] is None or item["last_reception"]["movement_class"] != last_movement_class
            ):
                return False
            if anomaly_code is not None and not any(a["code"] == anomaly_code for a in item["anomalies"]):
                return False
            if office_id is not None and s["lc_office_id"] != office_id:
                return False
            if min_abs_change_pct is not None and (s["pct"] is None or abs(s["pct"]) < min_abs_change_pct):
                return False
            return True

        filtered = [i for i in items if keep(i)]
        filtered.sort(key=_sort_key(sort))
        result = paginate([self._public(i) for i in filtered], page=page, page_size=page_size)
        result["sort"] = sort
        result["meta"] = self._meta(ctx)
        return result

    def get_product(self, variant_id: int) -> dict[str, Any]:
        ctx = self._load_context()
        for row in ctx.facts:
            if int(row["variant_id"]) == variant_id:
                item = self._public(self._build_item(row, ctx))
                item["meta"] = self._meta(ctx)
                return item
        raise CostControlNotFound("SKU inexistente en la empresa")

    # ---------- historial ----------

    def product_history(
        self,
        variant_id: int,
        *,
        office_id: int | None = None,
        date_from: date | None = None,
        date_to: date | None = None,
        page: int = 1,
        page_size: int = 50,
    ) -> dict[str, Any]:
        ctx = self._load_context()
        if not any(int(r["variant_id"]) == variant_id for r in ctx.facts):
            raise CostControlNotFound("SKU inexistente en la empresa")
        rows = self._repo.variant_lines(
            company_id=self._scope.company_id, variant_id=variant_id, history_start=HISTORY_START
        )
        comparable_quantities = [
            to_decimal(r["quantity"]) for r in rows if r.get("comparable") and r.get("quantity") is not None
        ]
        median_qty = (
            Decimal(str(statistics.median(comparable_quantities))) if comparable_quantities else None
        )

        lines: list[dict[str, Any]] = []
        series: list[dict[str, Any]] = []
        previous_comparable: Decimal | None = None
        for r in rows:
            movement = MovementClass(r["movement_class"])
            cost = to_decimal(r.get("cost"))
            quantity = to_decimal(r.get("quantity"))
            comparable = is_comparable_line(movement, cost, quantity, r.get("details_complete"))
            anomalies = line_anomalies(
                LineFacts(
                    movement_class=movement,
                    cost=cost,
                    quantity=quantity,
                    details_complete=r.get("details_complete"),
                    previous_comparable_cost=previous_comparable if comparable else None,
                    median_comparable_quantity=median_qty,
                    cost_modified=bool(r.get("cost_modified")),
                    quantity_modified=bool(r.get("quantity_modified")),
                    variant_modified=bool(r.get("variant_modified")),
                )
            )
            office = r.get("office_id")
            line = {
                "reception_id": int(r["reception_id"]),
                "detail_id": int(r["detail_id"]),
                "admission_date": iso(r.get("admission_date")),
                "office_id": office,
                "office_name": ctx.offices.get(int(office)) if office is not None else None,
                "document": r.get("document"),
                "document_number": r.get("document_number"),
                "internal_dispatch_id": r.get("internal_dispatch_id"),
                "movement_class": movement.value,
                "comparable": comparable,
                "cost": money(cost),
                "quantity": money(quantity),
                "previous_comparable_cost": money(previous_comparable) if comparable else None,
                "change_pct": money(change_pct(cost, previous_comparable)) if comparable else None,
                "anomalies": [a.to_json() for a in anomalies],
            }
            if comparable:
                previous_comparable = cost
            in_range = (
                (office_id is None or office == office_id)
                and (date_from is None or (r.get("admission_date") and r["admission_date"] >= date_from))
                and (date_to is None or (r.get("admission_date") and r["admission_date"] <= date_to))
            )
            if not in_range:
                continue
            lines.append(line)
            if comparable:
                series.append(
                    {
                        "date": line["admission_date"],
                        "cost": line["cost"],
                        "office_id": office,
                        "reception_id": line["reception_id"],
                    }
                )
        lines.reverse()
        result = paginate(lines, page=page, page_size=page_size)
        result["series"] = series[-HISTORY_SERIES_MAX_POINTS:]
        result["series_truncated"] = len(series) > HISTORY_SERIES_MAX_POINTS
        result["variant_id"] = variant_id
        result["meta"] = self._meta(ctx)
        return result

    # ---------- alertas ----------

    def list_alerts(
        self,
        *,
        date_from: date,
        date_to: date,
        anomaly_code: str | None = None,
        severity: str | None = None,
        office_id: int | None = None,
        page: int = 1,
        page_size: int = 50,
    ) -> dict[str, Any]:
        if date_from > date_to:
            raise CostControlValidationError("date_from posterior a date_to")
        if date_from < HISTORY_START:
            raise CostControlValidationError(f"date_from anterior al historial ({HISTORY_START})")
        if anomaly_code is not None and anomaly_code not in {a.value for a in AnomalyCode}:
            raise CostControlValidationError(f"anomaly_code inválido: {anomaly_code}")
        if severity is not None and severity not in {s.value for s in Severity}:
            raise CostControlValidationError(f"severity inválido: {severity}")
        ctx = self._load_context()
        rows = self._repo.alert_candidates(
            company_id=self._scope.company_id,
            history_start=HISTORY_START,
            date_from=date_from,
            date_to=date_to,
            extreme_quantity_factor=EXTREME_QUANTITY_FACTOR,
            increase_pct=INCREASE_ALERT_PCT,
            decrease_pct=DECREASE_ALERT_PCT,
        )
        alerts: list[dict[str, Any]] = []
        counts: dict[str, int] = {}
        for r in rows:
            if office_id is not None and r.get("office_id") != office_id:
                continue
            movement = MovementClass(r["movement_class"])
            cost = to_decimal(r.get("cost"))
            previous = to_decimal(r.get("previous_comparable_cost"))
            found = line_anomalies(
                LineFacts(
                    movement_class=movement,
                    cost=cost,
                    quantity=to_decimal(r.get("quantity")),
                    details_complete=r.get("details_complete"),
                    previous_comparable_cost=previous,
                    median_comparable_quantity=to_decimal(r.get("median_comparable_quantity")),
                    cost_modified=bool(r.get("cost_modified")),
                    quantity_modified=bool(r.get("quantity_modified")),
                    variant_modified=bool(r.get("variant_modified")),
                )
            )
            for a in found:
                counts[a.code.value] = counts.get(a.code.value, 0) + 1
            if anomaly_code is not None:
                found = [a for a in found if a.code.value == anomaly_code]
            if severity is not None:
                found = [a for a in found if a.severity.value == severity]
            if not found:
                continue
            office = r.get("office_id")
            alerts.append(
                {
                    "variant_id": r.get("variant_id"),
                    "code": r.get("code"),
                    "barcode": r.get("bar_code"),
                    "product_name": r.get("product_name"),
                    "variant_name": r.get("variant_name"),
                    "reception_id": int(r["reception_id"]),
                    "detail_id": int(r["detail_id"]),
                    "admission_date": iso(r.get("admission_date")),
                    "office_id": office,
                    "office_name": ctx.offices.get(int(office)) if office is not None else None,
                    "document": r.get("document"),
                    "document_number": r.get("document_number"),
                    "movement_class": movement.value,
                    "cost": money(cost),
                    "quantity": money(to_decimal(r.get("quantity"))),
                    "previous_comparable_cost": money(previous),
                    "change_pct": money(change_pct(cost, previous)),
                    "max_severity": min((a.severity for a in found), key=_SEVERITY_ORDER.__getitem__).value,
                    "anomalies": [a.to_json() for a in found],
                }
            )
        result = paginate(alerts, page=page, page_size=page_size)
        result["counts_by_code"] = counts
        result["date_from"] = date_from.isoformat()
        result["date_to"] = date_to.isoformat()
        result["meta"] = self._meta(ctx)
        return result

    # ---------- recepción ----------

    def get_reception(self, reception_id: int) -> dict[str, Any]:
        company_id = self._scope.company_id
        header = self._repo.reception_header(company_id=company_id, reception_id=reception_id)
        if header is None:
            raise CostControlNotFound("Recepción inexistente en la empresa")
        ctx = self._load_context()
        office = header.get("office_id")
        lines = self._repo.reception_lines(company_id=company_id, reception_id=reception_id)
        events = self._repo.reception_events(company_id=company_id, reception_id=reception_id)
        return {
            "reception_id": int(header["reception_id"]),
            "admission_date": iso(header.get("admission_date")),
            "office_id": office,
            "office_name": ctx.offices.get(int(office)) if office is not None else None,
            "document": header.get("document"),
            "document_number": header.get("document_number"),
            "internal_dispatch_id": header.get("internal_dispatch_id"),
            "movement_class": header.get("movement_class"),
            "details_complete": header.get("details_complete"),
            "details_count": header.get("details_count"),
            "missing_since": iso(header.get("missing_since")),
            "first_seen_at": iso(header.get("first_seen_at")),
            "last_changed_at": iso(header.get("last_changed_at")),
            "api_fetched_at": iso(header.get("api_fetched_at")),
            "lines": [
                {
                    "detail_id": int(l["detail_id"]),
                    "variant_id": l.get("variant_id"),
                    "code": l.get("code"),
                    "barcode": l.get("bar_code"),
                    "product_name": l.get("product_name"),
                    "variant_name": l.get("variant_name"),
                    "quantity": money(to_decimal(l.get("quantity"))),
                    "cost": money(to_decimal(l.get("cost"))),
                    "missing_since": iso(l.get("missing_since")),
                    "last_changed_at": iso(l.get("last_changed_at")),
                }
                for l in lines
            ],
            "events": [
                {
                    "event_kind": e.get("event_kind"),
                    "detected_at": iso(e.get("detected_at")),
                    "detected_by": e.get("detected_by"),
                    "header_changed": e.get("header_changed"),
                    "details_changed": e.get("details_changed"),
                    "added_lines": e.get("added_lines"),
                    "removed_lines": e.get("removed_lines"),
                    "cost_changed_detail_ids": list(e.get("cost_changed_detail_ids") or []),
                    "quantity_changed_detail_ids": list(e.get("quantity_changed_detail_ids") or []),
                    "variant_changed_detail_ids": list(e.get("variant_changed_detail_ids") or []),
                }
                for e in events
            ],
            "meta": self._meta(ctx),
        }

    # ---------- resumen y metadatos ----------

    def summary(self) -> dict[str, Any]:
        ctx = self._load_context()
        items = self._all_items(ctx)
        active = [i for i in items if i["variant_state"] == "activa"]
        reliability_counts = {r.value: 0 for r in CostReliability}
        anomaly_skus: dict[str, int] = {}
        tax_counts: dict[str, int] = {}
        for item in active:
            reliability_counts[item["reliability"]["status"]] += 1
            for code in {a["code"] for a in item["anomalies"]}:
                anomaly_skus[code] = anomaly_skus.get(code, 0) + 1
            tax_status = item["tax_profile"]["status"]
            tax_counts[tax_status] = tax_counts.get(tax_status, 0) + 1
        movements = self._repo.movement_summary(
            company_id=self._scope.company_id, history_start=HISTORY_START
        )
        return {
            "skus_total": len(items),
            "skus_active": len(active),
            "skus_active_with_bsale_average": sum(
                1
                for i in active
                if i["_sort"]["average_cost"] is not None and i["_sort"]["average_cost"] > 0
            ),
            "skus_active_with_comparable_reception": sum(1 for i in active if i["last_comparable"]),
            "skus_active_by_reliability": reliability_counts,
            "skus_active_by_anomaly": anomaly_skus,
            "skus_active_by_tax_profile": tax_counts,
            "movements": [
                {
                    "movement_class": m.get("movement_class"),
                    "lines": int(m.get("lines") or 0),
                    "receptions": int(m.get("receptions") or 0),
                    "comparable_lines": int(m.get("comparable_lines") or 0),
                    "zero_cost_lines": int(m.get("zero_cost_lines") or 0),
                    "null_cost_lines": int(m.get("null_cost_lines") or 0),
                    "first_admission_date": iso(m.get("first_admission_date")),
                    "last_admission_date": iso(m.get("last_admission_date")),
                }
                for m in movements
            ],
            "meta": self._meta(ctx),
        }

    def overview(self, *, top: int = DASHBOARD_TOP_N, alert_days: int = DASHBOARD_ALERT_DAYS) -> dict[str, Any]:
        """Indicadores de costos para el Dashboard V2 (SKUs activos; mismas reglas que ``summary``)."""
        ctx = self._load_context()
        active = [i for i in self._all_items(ctx) if i["variant_state"] == "activa"]
        reliability = {r.value: 0 for r in CostReliability}
        with_high_anomaly = 0
        for item in active:
            reliability[item["reliability"]["status"]] += 1
            if any(a["severity"] == Severity.ALTA.value for a in item["anomalies"]):
                with_high_anomaly += 1
        increases = sorted(
            (i for i in active if i["_sort"]["pct"] is not None and i["_sort"]["pct"] >= INCREASE_ALERT_PCT),
            key=lambda i: (-i["_sort"]["pct"], i["variant_id"]),
        )
        fetched = [r["average_cost_fetched_at"] for r in ctx.facts if r.get("average_cost_fetched_at") is not None]
        last_dates = [i["_sort"]["last_date"] for i in active if i["_sort"]["last_date"] is not None]
        date_to = self._today
        date_from = max(HISTORY_START, date_to - timedelta(days=alert_days))
        alerts = self.list_alerts(date_from=date_from, date_to=date_to, severity=Severity.ALTA.value, page=1, page_size=top)
        return {
            "skus_active": len(active),
            "skus_by_reliability": reliability,
            "skus_reliable": reliability[CostReliability.CONSISTENTE.value],
            "skus_with_high_anomaly": with_high_anomaly,
            "relevant_increases": {
                "threshold_pct": money(INCREASE_ALERT_PCT),
                "total": len(increases),
                "top": [
                    {
                        "variant_id": i["variant_id"],
                        "code": i["code"],
                        "product_name": i["product_name"],
                        "variant_name": i["variant_name"],
                        "change_pct": i["change"]["pct"] if i["change"] else None,
                        "last_comparable": i["last_comparable"],
                        "previous_comparable": i["previous_comparable"],
                        "reliability": i["reliability"]["status"],
                    }
                    for i in increases[:top]
                ],
            },
            "high_alerts": {
                "date_from": alerts["date_from"],
                "date_to": alerts["date_to"],
                "total": alerts["total"],
                "counts_by_code": alerts["counts_by_code"],
                "top": alerts["items"],
            },
            "data_dates": {
                "average_cost_fetched_min": iso(min(fetched)) if fetched else None,
                "average_cost_fetched_max": iso(max(fetched)) if fetched else None,
                "last_reception_date": iso(max(last_dates)) if last_dates else None,
            },
            "net_cost_verified": NET_COST_VERIFIED,
            "meta": self._meta(ctx),
        }

    def tax_coverage(self, *, examples: int = 5) -> dict[str, Any]:
        """Cobertura tributaria de SKUs activos: mismos perfiles y bruto que usan Costos y Márgenes."""
        ctx = self._load_context()
        active = [i for i in self._all_items(ctx) if i["variant_state"] == "activa"]
        by_status = {s.value: 0 for s in TaxProfileStatus}
        by_validation = {v.value: 0 for v in TaxValidation}
        by_gross = {g.value: 0 for g in GrossCostStatus}
        identified = 0
        products: set[int] = set()
        combos: dict[tuple[int, ...] | None, dict[str, Any]] = {}
        anomalies: dict[str, dict[str, Any]] = {}
        for item in active:
            profile = item["_sort"]["tax_profile"]
            gross = item["_sort"]["commercial_gross"]
            by_status[profile.status.value] += 1
            by_validation[profile.validation.value] += 1
            by_gross[gross.status.value] += 1
            if profile.identified:
                identified += 1
            if item["product_id"] is not None:
                products.add(int(item["product_id"]))
            ids = item["_sort"]["tax_ids"]
            key = tuple(ids) if ids is not None else None
            combo = combos.setdefault(
                key,
                {
                    "tax_ids": list(key) if key is not None else None,
                    "taxes": [{"tax_id": t.tax_id, "name": t.name, "kind": t.role} for t in profile.taxes],
                    "status": profile.status.value,
                    "validation": profile.validation.value,
                    "total_rate_pct": money(profile.total_rate_pct),
                    "skus": 0,
                    "products": set(),
                },
            )
            combo["skus"] += 1
            if item["product_id"] is not None:
                combo["products"].add(int(item["product_id"]))
            for issue in profile.issues:
                bucket = anomalies.setdefault(issue, {"code": issue, "skus": 0, "products": set(), "examples": []})
                bucket["skus"] += 1
                if item["product_id"] is not None:
                    bucket["products"].add(int(item["product_id"]))
                if len(bucket["examples"]) < examples:
                    bucket["examples"].append(
                        {
                            "variant_id": item["variant_id"],
                            "product_id": item["product_id"],
                            "code": item["code"],
                            "product_name": item["product_name"],
                            "tax_ids": list(key) if key is not None else None,
                            "tax_status": profile.status.value,
                        }
                    )
        total = len(active)
        calculable = by_gross[GrossCostStatus.PRELIMINAR_NO_VERIFICADO.value]
        usage: dict[int, int] = {}
        for key, combo in combos.items():
            for tax_id in key or ():
                usage[tax_id] = usage.get(tax_id, 0) + combo["skus"]

        def pct(n: int) -> str | None:
            return money((Decimal(n) * 100 / Decimal(total)).quantize(Decimal("0.1"))) if total else None

        return {
            "skus_active": total,
            "products_active": len(products),
            "skus_taxes_identified": identified,
            "skus_gross_calculable": calculable,
            "skus_without_cost": by_gross[GrossCostStatus.SIN_COSTO_BASE.value],
            "skus_tax_review": by_validation[TaxValidation.REQUIERE_REVISION_TRIBUTARIA.value],
            "skus_without_tax_config": by_validation[TaxValidation.SIN_CONFIGURACION.value],
            "coverage_pct": {"taxes_identified": pct(identified), "gross_calculable": pct(calculable)},
            "by_tax_status": by_status,
            "by_validation": by_validation,
            "by_gross_status": by_gross,
            "combinations": sorted(
                ({**c, "products": len(c["products"])} for c in combos.values()),
                key=lambda c: (-c["skus"], str(c["tax_ids"])),
            ),
            "anomalies": sorted(
                ({**a, "products": len(a["products"])} for a in anomalies.values()),
                key=lambda a: (-a["skus"], a["code"]),
            ),
            "catalog": [
                {**entry.to_json(), "skus_using": usage.get(tax_id, 0)}
                for tax_id, entry in sorted(ctx.tax_catalog.items())
            ],
            "cost_base": "promedio_bsale",
            "tax_config_verified": TAX_CONFIG_VERIFIED,
            "meta": self._meta(ctx),
        }

    def filters(self) -> dict[str, Any]:
        company_id = self._scope.company_id
        return {
            "product_types": [
                {"id": int(r["product_type_id"]), "name": r.get("name"), "state": r.get("state")}
                for r in self._repo.product_types(company_id=company_id)
            ],
            "offices": [
                {
                    "id": int(r["office_id"]),
                    "name": r.get("name"),
                    "state": r.get("state"),
                    "is_virtual": r.get("is_virtual"),
                }
                for r in self._repo.offices(company_id=company_id)
            ],
            "movement_classes": [m.value for m in MovementClass],
            "reliability_statuses": [r.value for r in CostReliability],
            "anomaly_codes": [a.value for a in AnomalyCode],
            "meta": self._meta(),
        }

    def freshness(self) -> dict[str, Any]:
        rows = self._repo.sync_state(company_id=self._scope.company_id)
        return {
            "resources": [
                {
                    "resource": r.get("resource"),
                    "scope": r.get("scope"),
                    "status": r.get("status"),
                    "last_success_at": iso(r.get("last_success_at")),
                    "last_attempt_at": iso(r.get("last_attempt_at")),
                    "last_full_reconcile_at": iso(r.get("last_full_reconcile_at")),
                }
                for r in rows
            ],
            "meta": self._meta(),
        }


def _sort_key(sort: str) -> Callable[[dict[str, Any]], Any]:
    def name(item: dict[str, Any]) -> str:
        return normalize_label(item.get("product_name") or "")

    if sort == "product":
        return lambda i: (name(i), i["variant_id"])
    if sort == "change_pct_desc":
        return lambda i: (i["_sort"]["pct"] is None, -(i["_sort"]["pct"] or 0), i["variant_id"])
    if sort == "change_pct_asc":
        return lambda i: (i["_sort"]["pct"] is None, i["_sort"]["pct"] or 0, i["variant_id"])
    if sort == "last_date_desc":
        return lambda i: (
            i["_sort"]["last_date"] is None,
            -(i["_sort"]["last_date"].toordinal() if i["_sort"]["last_date"] else 0),
            i["variant_id"],
        )
    if sort == "average_cost_desc":
        return lambda i: (
            i["_sort"]["average_cost"] is None,
            -(i["_sort"]["average_cost"] or 0),
            i["variant_id"],
        )
    return lambda i: (_RELIABILITY_ORDER[i["_sort"]["reliability"]], name(i), i["variant_id"])


def run_cost_control(
    user: dict[str, Any],
    company_id: int,
    fn: Callable[[CostControlV2Service], Any],
    *,
    get_conn: Callable[[], Any] = get_connection,
) -> Any:
    """Abre una sesión read-only, valida la empresa del usuario y ejecuta ``fn``."""
    conn = open_readonly_connection(get_conn)
    try:
        executor = make_psycopg_executor(conn, statement_timeout_seconds=25, lock_timeout="3s")
        scope = resolve_company_scope(user, company_id, executor)
        service = CostControlV2Service(CostControlV2Repository(executor), scope)
        return fn(service)
    finally:
        try:
            conn.rollback()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass
