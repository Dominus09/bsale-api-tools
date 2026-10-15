"""Dashboard V2: compone Control de Costos, Márgenes y Promociones V2 en una sola respuesta.

No tiene fórmulas propias: cada indicador lo calcula su motor (``overview`` / ``summary``). Aquí
solo se decide qué bloques ve el usuario, se aíslan los fallos por bloque y se clasifica la
frescura de BSALE RAW. Solo lectura.
"""

from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from backend.auth.permissions import resolve_executive_permissions
from backend.db import get_connection
from backend.services.bsale_raw.core.registry import POINT_STATE_SCOPE
from backend.services.analytics.validate_distribuidora_source import (
    AnalyticsValidationError,
    make_psycopg_executor,
    open_readonly_connection,
)
from backend.services.analytics_v2.access import CompanyScope, promotion_capabilities, resolve_company_scope
from backend.services.analytics_v2.contracts import (
    CONTRACT_VERSION,
    DASHBOARD_CACHE_TTL_SECONDS,
    DASHBOARD_RAW_RESOURCES,
    DATA_SOURCE,
    DEFAULT_COST_BASIS,
    NET_COST_VERIFIED,
    POLICIES_REVIEWED,
    RAW_FRESHNESS_STALE_HOURS,
    TAX_CONFIG_VERIFIED,
    CostBasis,
    DataClass,
    FreshnessStatus,
    local_today,
)
from backend.services.analytics_v2.cost_control_repo import CostControlV2Repository
from backend.services.analytics_v2.cost_control_service import CostControlV2Service
from backend.services.analytics_v2.margins_repo import MarginsV2Repository
from backend.services.analytics_v2.margins_service import MarginsV2Service, MarginsValidationError
from backend.services.analytics_v2.packs_service import PacksV2Service
from backend.services.analytics_v2.promotion_campaigns import CampaignValidationError
from backend.services.analytics_v2.promotion_source import CampaignExceptionSource
from backend.services.analytics_v2.promotions_repo import PromotionsV2Repository
from backend.services.analytics_v2.promotions_service import PromotionsV2Service

logger = logging.getLogger(__name__)

BLOCK_OK = "ok"
BLOCK_FORBIDDEN = "sin_permiso"
BLOCK_UNAVAILABLE = "no_disponible"

_FRESHNESS_ORDER = {
    FreshnessStatus.CON_ERROR: 0,
    FreshnessStatus.SIN_DATOS: 1,
    FreshnessStatus.ATRASADA: 2,
    FreshnessStatus.AL_DIA: 3,
}

_cache: dict[tuple[Any, ...], tuple[float, dict[str, Any]]] = {}
_cache_lock = threading.Lock()


def clear_cache() -> None:
    with _cache_lock:
        _cache.clear()


def dashboard_capabilities(user: dict[str, Any] | None) -> dict[str, bool]:
    executive = resolve_executive_permissions(user)
    return {
        "costs": bool(executive.get("costs")),
        "margins": bool(executive.get("margins")),
        "promotions": bool(promotion_capabilities(user).get("view")),
    }


def _parse_ts(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        ts = value
    else:
        try:
            ts = datetime.fromisoformat(str(value))
        except ValueError:
            return None
    return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)


def classify_freshness(
    status: str | None, last_success_at: Any, now: datetime, *, stale_hours: int = RAW_FRESHNESS_STALE_HOURS
) -> FreshnessStatus:
    if (status or "").upper() == "FAILED":
        return FreshnessStatus.CON_ERROR
    success = _parse_ts(last_success_at)
    if success is None:
        return FreshnessStatus.SIN_DATOS
    if now - success > timedelta(hours=stale_hours):
        return FreshnessStatus.ATRASADA
    return FreshnessStatus.AL_DIA


def summarize_freshness(resources: list[dict[str, Any]], now: datetime) -> dict[str, Any]:
    """Peor estado por recurso (un recurso puede tener varios scopes); recursos esperados sin filas = sin datos.

    El scope ``point`` (sincronización puntual de variantes o recepciones) no mide la frescura del
    recurso cuando existen barridos periódicos, por lo que solo cuenta si es el único scope.
    """
    periodic = {r.get("resource") for r in resources if r.get("scope") != POINT_STATE_SCOPE}
    per_resource: dict[str, dict[str, Any]] = {}
    for r in resources:
        name = r.get("resource")
        if name not in DASHBOARD_RAW_RESOURCES:
            continue
        if r.get("scope") == POINT_STATE_SCOPE and name in periodic:
            continue
        status = classify_freshness(r.get("status"), r.get("last_success_at"), now)
        success = _parse_ts(r.get("last_success_at"))
        current = per_resource.get(name)
        if current is None:
            per_resource[name] = {"status": status, "oldest_success": success, "last_attempt_at": r.get("last_attempt_at"), "raw_status": r.get("status"), "scopes": 1}
            continue
        current["scopes"] += 1
        if _FRESHNESS_ORDER[status] < _FRESHNESS_ORDER[current["status"]]:
            current["status"] = status
            current["raw_status"] = r.get("status")
        if success is not None and (current["oldest_success"] is None or success < current["oldest_success"]):
            current["oldest_success"] = success
    out = []
    for name in DASHBOARD_RAW_RESOURCES:
        info = per_resource.get(name)
        if info is None:
            out.append({"resource": name, "status": FreshnessStatus.SIN_DATOS.value, "last_success_at": None, "last_attempt_at": None, "raw_status": None, "scopes": 0})
            continue
        out.append(
            {
                "resource": name,
                "status": info["status"].value,
                "last_success_at": info["oldest_success"].isoformat() if info["oldest_success"] else None,
                "last_attempt_at": info["last_attempt_at"],
                "raw_status": info["raw_status"],
                "scopes": info["scopes"],
            }
        )
    overall = min((FreshnessStatus(o["status"]) for o in out), key=_FRESHNESS_ORDER.__getitem__)
    successes = [_parse_ts(o["last_success_at"]) for o in out if o["last_success_at"]]
    return {
        "overall": overall.value,
        "stale_after_hours": RAW_FRESHNESS_STALE_HOURS,
        "oldest_success_at": min(successes).isoformat() if successes else None,
        "resources": out,
    }


class DashboardV2Service:
    def __init__(
        self,
        *,
        scope: CompanyScope,
        capabilities: dict[str, bool],
        costs: CostControlV2Service,
        margins: MarginsV2Service,
        promotions: PromotionsV2Service,
        reset: Callable[[], None] | None = None,
        now: datetime | None = None,
    ) -> None:
        self._scope = scope
        self._caps = capabilities
        self._costs = costs
        self._margins = margins
        self._promotions = promotions
        self._reset = reset
        self._now = now or datetime.now(timezone.utc)

    def _block(self, name: str, allowed: bool, fn: Callable[[], Any]) -> dict[str, Any]:
        if not allowed:
            return {"status": BLOCK_FORBIDDEN, "reason": "sin_permiso", "elapsed_ms": 0, "data": None}
        start = time.perf_counter()
        reason: str | None = None
        try:
            data = fn()
        except AnalyticsValidationError as exc:
            reason = "tiempo_agotado" if getattr(exc, "error_type", None) == "statement_timeout" else "error_de_datos"
        except (MarginsValidationError, CampaignValidationError):
            reason = "sin_alcance_comercial"
        except Exception:
            logger.exception("[DASHBOARD_V2] bloque=%s company_id=%s", name, self._scope.company_id)
            reason = "error_interno"
        elapsed = int((time.perf_counter() - start) * 1000)
        if reason is None:
            return {"status": BLOCK_OK, "reason": None, "elapsed_ms": elapsed, "data": data}
        if self._reset is not None:
            try:
                self._reset()
            except Exception:
                pass
        return {"status": BLOCK_UNAVAILABLE, "reason": reason, "elapsed_ms": elapsed, "data": None}

    def _promotions_data(self) -> dict[str, Any]:
        summary = self._promotions.summary()
        return {
            "month": summary["month"],
            "tables_ready": summary["meta"]["tables_ready"],
            "kpis": summary["kpis"],
            "verification_live": summary["verification_live"],
            "legacy": summary["legacy"],
            "writes_bsale": False,
        }

    def _packs_data(self, cost_basis: str) -> dict[str, Any]:
        summary = PacksV2Service(self._margins, self._costs).summary(cost_basis=cost_basis, top=5)
        keys = (
            "packs_active",
            "packs_active_calculable",
            "packs_active_pending",
            "coverage_pct",
            "by_pack_status",
            "packs_by_problem",
            "top_below_cost",
            "top_below_policy",
        )
        return {k: summary[k] for k in keys}

    def build(self, *, cost_basis: str) -> dict[str, Any]:
        freshness = self._block("freshness", True, lambda: summarize_freshness(self._costs.freshness()["resources"], self._now))
        margins = self._block("margins", self._caps["margins"], lambda: self._margins.overview(cost_basis=cost_basis))
        costs = self._block("costs", self._caps["costs"], self._costs.overview)
        taxes = self._block("taxes", self._caps["margins"] or self._caps["costs"], self._costs.tax_coverage)
        promotions = self._block("promotions", self._caps["promotions"], self._promotions_data)
        packs = self._block("packs", self._caps["margins"], lambda: self._packs_data(cost_basis))
        reliability = self._reliability(freshness, margins, costs, promotions, taxes)
        if packs["status"] == BLOCK_OK and packs["data"]["packs_active_pending"]:
            reliability["warnings"].append("packs_con_costo_pendiente")
        elif packs["status"] == BLOCK_UNAVAILABLE:
            reliability["warnings"].append("bloque_packs_no_disponible")
        return {
            "today": local_today().isoformat(),
            "cost_basis": cost_basis,
            "permissions": dict(self._caps),
            "reliability": reliability,
            "freshness": freshness,
            "margins": margins,
            "costs": costs,
            "taxes": taxes,
            "promotions": promotions,
            "packs": packs,
            "meta": {
                "contract_version": CONTRACT_VERSION,
                "data_source": DATA_SOURCE,
                "company_id": self._scope.company_id,
                "company_name": self._scope.company_name,
                "generated_at": self._now.isoformat(),
                "cached": False,
                "writes_bsale": False,
            },
        }

    @staticmethod
    def _reliability(*blocks: dict[str, Any]) -> dict[str, Any]:
        freshness, margins, costs, promotions, taxes = blocks
        warnings: list[str] = []
        overall = freshness["data"]["overall"] if freshness["status"] == BLOCK_OK else FreshnessStatus.SIN_DATOS.value
        if overall != FreshnessStatus.AL_DIA.value:
            warnings.append(f"sincronizacion_{overall}")
        if not POLICIES_REVIEWED:
            warnings.append("politica_margenes_pendiente_revision")
        if not NET_COST_VERIFIED:
            warnings.append("costo_bruto_preliminar")
        if not TAX_CONFIG_VERIFIED:
            warnings.append("configuracion_tributaria_no_contrastada")
        if taxes["status"] == BLOCK_OK and taxes["data"]["skus_tax_review"]:
            warnings.append("productos_requieren_revision_tributaria")
        if margins["status"] == BLOCK_OK and not margins["data"]["meta"]["promotions_integrated"]:
            warnings.append("promociones_no_integradas")
        for name, block in (("margenes", margins), ("costos", costs), ("impuestos", taxes), ("promociones", promotions)):
            if block["status"] == BLOCK_UNAVAILABLE:
                warnings.append(f"bloque_{name}_no_disponible")
        if margins["status"] != BLOCK_OK:
            level = DataClass.NO_DISPONIBLE
        elif warnings:
            level = DataClass.PROVISIONAL
        else:
            level = DataClass.CONFIRMADO
        return {
            "level": level.value,
            "raw_freshness": overall,
            "policies_reviewed": POLICIES_REVIEWED,
            "net_cost_verified": NET_COST_VERIFIED,
            "tax_config_verified": TAX_CONFIG_VERIFIED,
            "warnings": warnings,
        }


def _cached(key: tuple[Any, ...]) -> dict[str, Any] | None:
    with _cache_lock:
        hit = _cache.get(key)
    if hit and time.monotonic() - hit[0] < DASHBOARD_CACHE_TTL_SECONDS:
        payload = dict(hit[1])
        payload["meta"] = {**hit[1]["meta"], "cached": True}
        return payload
    return None


def run_dashboard(
    user: dict[str, Any],
    company_id: int,
    *,
    cost_basis: str = DEFAULT_COST_BASIS.value,
    refresh: bool = False,
    get_conn: Callable[[], Any] = get_connection,
) -> dict[str, Any]:
    """Una sesión read-only; la empresa se valida siempre, también cuando la respuesta sale de caché."""
    if cost_basis not in {c.value for c in CostBasis}:
        raise MarginsValidationError(f"cost_basis inválido: {cost_basis}")
    caps = dashboard_capabilities(user)
    conn = open_readonly_connection(get_conn)
    try:
        executor = make_psycopg_executor(conn, statement_timeout_seconds=25, lock_timeout="3s")
        scope = resolve_company_scope(user, company_id, executor)
        today = local_today()
        key = (scope.company_id, cost_basis, tuple(sorted(caps.items())), today)
        if not refresh:
            hit = _cached(key)
            if hit is not None:
                return hit
        start = time.perf_counter()
        costs = CostControlV2Service(CostControlV2Repository(executor), scope)
        margins = MarginsV2Service(
            MarginsV2Repository(executor),
            costs,
            scope,
            today=today,
            promotions=CampaignExceptionSource(PromotionsV2Repository(executor), today=today),
        )
        promotions = PromotionsV2Service(PromotionsV2Repository(executor), scope, user=user, today=today)

        def reset() -> None:
            conn.rollback()

        payload = DashboardV2Service(
            scope=scope, capabilities=caps, costs=costs, margins=margins, promotions=promotions, reset=reset
        ).build(cost_basis=cost_basis)
        payload["meta"]["elapsed_ms"] = int((time.perf_counter() - start) * 1000)
        if not any(payload[b]["status"] == BLOCK_UNAVAILABLE for b in ("freshness", "margins", "costs", "taxes", "promotions", "packs")):
            with _cache_lock:
                _cache[key] = (time.monotonic(), payload)
        return payload
    finally:
        try:
            conn.rollback()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass
