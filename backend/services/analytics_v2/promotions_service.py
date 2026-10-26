"""Promociones V2: ofertas del mes, remates, verificación contra Bsale e historial comercial.

Registro comercial administrado: nunca modifica precios en Bsale ni BSALE RAW, y registrar una
campaña no prueba que su precio se aplicó (eso lo dice la verificación contra RAW). Cada cambio
queda en un historial append-only. Los precios normales y el stock de referencia se leen en backend
desde RAW; del navegador solo se acepta el precio promocional declarado.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Mapping

from backend.db import get_connection
from backend.services.analytics.validate_distribuidora_source import (
    make_psycopg_executor,
    make_psycopg_rw_executor,
    open_readonly_connection,
    open_readwrite_connection,
)
from backend.services.analytics_v2.access import CompanyScope, promotion_capabilities, resolve_company_scope
from backend.services.analytics_v2.commercial_scope import (
    CommercialScopeNotConfigured,
    PriceListScope,
    resolve_price_list_scope,
)
from backend.services.analytics_v2.contracts import (
    BUSINESS_TIMEZONE,
    CONTRACT_VERSION,
    DATA_SOURCE,
    PROMO_PRICE_MAX_AGE_DAYS,
    CampaignApproval as A,
    CampaignKind,
    CampaignTimeline as T,
    ClearanceReason,
    PriceListScopeStatus,
    PriceVerification as V,
    PromotionCheck,
    iso,
    local_today,
    money,
    paginate,
)
from backend.services.analytics_v2.movement_classification import normalize_label
from backend.services.analytics_v2.promotion_campaigns import (
    LEGACY_MAPPEABLE,
    REASON_REQUIRED,
    TRANSITION_EVENTS,
    BsalePrice,
    CampaignConflict,
    CampaignNotFound,
    CampaignValidationError,
    approval_after_change,
    assert_editable,
    campaign_item_exception,
    campaign_timeline,
    clean_text,
    effective_end,
    legacy_exception,
    legacy_mapping_status,
    measurement_windows,
    month_bounds,
    next_approval,
    overlaps,
    price_verification,
    validate_dates,
    validate_promo_price,
)
from backend.services.analytics_v2.promotion_exceptions import PromotionException, resolve_exception
from backend.services.analytics_v2.promotions_repo import PromotionsV2Repository

SECTIONS = ("vigentes", "remates", "proximas", "pendientes", "historial")
UPCOMING_HORIZON_DAYS = 180
SEARCH_LIMIT = 20

# Estados de excepción de un ítem que no está en condiciones de participar en márgenes hoy.
CHECK_NO_VIGENTE = "no_vigente"
CHECK_NO_PARTICIPA = "no_participa"

_PARTICIPATING = frozenset({A.APROBADA, A.PENDIENTE_APROBACION})


class PromotionsNotReady(RuntimeError):
    """Tablas V2 no migradas: las lecturas muestran solo legacy y las escrituras se rechazan (503)."""


class PromotionsForbidden(PermissionError):
    pass


ACTION_PERMISSION = {"submit": "edit", "approve": "approve", "reject": "approve", "cancel": "cancel"}


@dataclass(frozen=True)
class Actor:
    email: str
    role: str | None


def actor_from(user: Mapping[str, Any] | None) -> Actor:
    email = str((user or {}).get("email") or "").strip().lower()
    role = (user or {}).get("role")
    return Actor(email=email or "desconocido", role=str(role).strip().lower() if role else None)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _business_tz() -> Any:
    try:
        from zoneinfo import ZoneInfo

        return ZoneInfo(BUSINESS_TIMEZONE)
    except Exception:
        return timezone.utc


def _dec(value: Any) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _discount_pct(normal: Decimal | None, promo: Decimal | None) -> str | None:
    if normal is None or promo is None or normal <= 0:
        return None
    return money(((normal - promo) / normal * 100).quantize(Decimal("0.01")))


def _parse_date(value: Any, label: str) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError) as exc:
        raise CampaignValidationError(f"{label} inválida") from exc


def _parse_int(value: Any, label: str) -> int:
    if isinstance(value, bool):
        raise CampaignValidationError(f"{label} inválido")
    try:
        out = int(value)
    except (TypeError, ValueError) as exc:
        raise CampaignValidationError(f"{label} inválido") from exc
    if out < 1:
        raise CampaignValidationError(f"{label} inválido")
    return out


def _parse_price(value: Any) -> Decimal:
    if isinstance(value, (bool, float)) or value is None:
        raise CampaignValidationError("El precio promocional debe enviarse como texto o entero exacto")
    try:
        return validate_promo_price(Decimal(str(value).strip()))
    except InvalidOperation as exc:
        raise CampaignValidationError("Precio promocional inválido") from exc


def _parse_kind(value: Any) -> CampaignKind:
    try:
        return CampaignKind(str(value).strip().lower())
    except ValueError as exc:
        raise CampaignValidationError("kind debe ser oferta o remate") from exc


def _parse_clearance(value: Any, kind: CampaignKind) -> str | None:
    if kind is CampaignKind.OFERTA:
        if value not in (None, ""):
            raise CampaignValidationError("El motivo de remate solo aplica a campañas de remate")
        return None
    try:
        return ClearanceReason(str(value or "").strip().lower()).value
    except ValueError as exc:
        raise CampaignValidationError("Un remate exige motivo: vencimiento, descontinuado, dano, sobrestock u otro") from exc


class PromotionsV2Service:
    def __init__(
        self,
        repo: PromotionsV2Repository,
        scope: CompanyScope,
        *,
        user: Mapping[str, Any] | None = None,
        today: date | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._repo = repo
        self._scope = scope
        self._today = today or local_today()
        self._clock = clock or _now
        self._caps = promotion_capabilities(dict(user) if user else None)
        self._actor = actor_from(user)
        self._ready: bool | None = None
        self._lists: PriceListScope | None = None

    # ------------------------------------------------------------------ infraestructura

    @property
    def company_id(self) -> int:
        return self._scope.company_id

    def _tables_ready(self) -> bool:
        if self._ready is None:
            self._ready = self._repo.tables_ready()
        return self._ready

    def _require_ready(self) -> None:
        if not self._tables_ready():
            raise PromotionsNotReady("Promociones V2 aún no tiene sus tablas migradas en esta base")

    def _require_cap(self, action: str) -> None:
        """Defensa adicional al permiso del router: el servicio tampoco escribe sin el permiso de la acción."""
        if not self._caps.get(action):
            raise PromotionsForbidden(f"Su rol no permite '{action}' en Promociones V2")

    def _price_scope(self) -> PriceListScope:
        if self._lists is None:
            try:
                self._lists = resolve_price_list_scope(self.company_id, self._repo.price_lists(company_id=self.company_id))
            except CommercialScopeNotConfigured as exc:
                raise CampaignValidationError(str(exc)) from exc
        return self._lists

    def _meta(self) -> dict[str, Any]:
        lists = self._price_scope()
        return {
            "contract_version": CONTRACT_VERSION,
            "data_source": DATA_SOURCE,
            "company_id": self.company_id,
            "company_name": self._scope.company_name,
            "today": iso(self._today),
            "tables_ready": self._tables_ready(),
            "capabilities": self._caps,
            "evaluated_price_list_ids": sorted(lists.evaluated_ids),
            "price_max_age_days": PROMO_PRICE_MAX_AGE_DAYS,
            "writes_bsale": False,
            "sales_measurement": "no_implementado",
        }

    def _list_info(self, price_list_id: int) -> dict[str, Any]:
        entry = self._price_scope().entry(int(price_list_id))
        return {
            "id": int(price_list_id),
            "name": entry.name if entry else None,
            "label": entry.visible_name if entry else None,
            "in_scope": bool(entry and entry.status is PriceListScopeStatus.EVALUADA),
        }

    def _actions(self, approval: A, timeline: T) -> list[str]:
        closed = approval is A.CANCELADA or timeline is T.FINALIZADA
        out: list[str] = []
        if not closed and self._caps.get("edit"):
            out.append("edit")
            if approval in (A.BORRADOR, A.RECHAZADA):
                out.append("submit")
        if not closed and self._caps.get("approve") and approval is A.PENDIENTE_APROBACION:
            out.extend(["approve", "reject"])
        if not closed and self._caps.get("cancel"):
            out.append("cancel")
        return out

    # ------------------------------------------------------------------ contexto de lectura

    def _candidates(self) -> list[PromotionException]:
        """Mismos candidatos que usa Márgenes V2 para el día de hoy (V2 + legacy mapeable)."""
        out: list[PromotionException] = []
        if self._tables_ready():
            for row in self._repo.covering_items(company_id=self.company_id, date_from=self._today, date_to=self._today):
                exc = campaign_item_exception(row)
                if exc is not None:
                    out.append(exc)
        scope = self._price_scope()
        for row in self._repo.legacy_snapshots(company_id=self.company_id):
            status, variant_id, list_id = legacy_mapping_status(row, scope)
            if status == LEGACY_MAPPEABLE and variant_id is not None and list_id is not None:
                exc = legacy_exception(row, self.company_id, variant_id, list_id)
                if exc is not None and exc.is_active_on(self._today):
                    out.append(exc)
        return out

    def _load(self, date_from: date, date_to: date) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        if not self._tables_ready():
            return [], []
        lo, hi = min(date_from, self._today), max(date_to, self._today)
        campaigns = self._repo.campaigns_in_range(company_id=self.company_id, date_from=lo, date_to=hi)
        by_id = {int(c["id"]): c for c in campaigns}
        items = self._repo.items(company_id=self.company_id, campaign_ids=sorted(by_id))
        prices = self._prices_for(items)
        candidates = self._candidates()
        views = [self._item_view(by_id[int(i["campaign_id"])], i, prices, candidates) for i in items]
        return [self._campaign_view(c, [v for v in views if v["campaign_id"] == int(c["id"])]) for c in campaigns], views

    def _prices_for(self, items: list[dict[str, Any]]) -> dict[tuple[int, int], BsalePrice]:
        variants = sorted({int(i["variant_id"]) for i in items})
        lists = sorted({int(i["price_list_id"]) for i in items})
        rows = self._repo.raw_prices(company_id=self.company_id, variant_ids=variants, price_list_ids=lists)
        return {
            (int(r["price_list_id"]), int(r["variant_id"])): BsalePrice(
                gross=_dec(r.get("variant_value_with_taxes")),
                missing=r.get("missing_since") is not None,
                fetched_at=r.get("api_fetched_at"),
            )
            for r in rows
        }

    def _item_view(
        self,
        campaign: Mapping[str, Any],
        item: Mapping[str, Any],
        prices: Mapping[tuple[int, int], BsalePrice],
        candidates: list[PromotionException],
    ) -> dict[str, Any]:
        approval = A(campaign["approval_status"])
        timeline = campaign_timeline(approval, campaign["starts_on"], campaign["ends_on"], self._today)
        variant_id, list_id = int(item["variant_id"]), int(item["price_list_id"])
        promo = _dec(item["promo_gross_price"])
        normal = _dec(item.get("normal_gross_price"))
        bsale = prices.get((list_id, variant_id))
        active = item.get("item_status") == "activo"
        verification = (
            price_verification(timeline=timeline, promo_gross_price=promo, bsale=bsale, today=self._today)
            if active and promo is not None
            else V.NO_APLICA
        )
        exception: dict[str, Any] | None = None
        if not active or approval not in _PARTICIPATING:
            check = CHECK_NO_PARTICIPA
        elif timeline is not T.VIGENTE:
            check = CHECK_NO_VIGENTE
        else:
            gross = bsale.gross if bsale and not bsale.missing else None
            res = resolve_exception(
                source_available=True,
                candidates=candidates,
                company_id=self.company_id,
                variant_id=variant_id,
                price_list_id=list_id,
                bsale_gross_price=gross,
                today=self._today,
                bsale_price_fetched_at=bsale.fetched_at if bsale else None,
                max_price_age_days=PROMO_PRICE_MAX_AGE_DAYS,
            )
            check = res.check.value
            exception = res.to_json()
            exception["applies_to_this_item"] = bool(res.exception and res.exception.item_id == int(item["id"]))
        return {
            "item_id": int(item["id"]),
            "campaign_id": int(campaign["id"]),
            "campaign_name": campaign["name"],
            "kind": campaign["kind"],
            "approval_status": approval.value,
            "timeline": timeline.value,
            "starts_on": iso(campaign["starts_on"]),
            "ends_on": iso(campaign["ends_on"]),
            "variant_id": variant_id,
            "product_id": item.get("product_id"),
            "sku_code": item.get("sku_code"),
            # Preferir barcode RAW vigente; snapshot del ítem solo si Bsale no informa.
            "barcode": item.get("raw_barcode") or item.get("barcode"),
            "product_name": item.get("product_name"),
            "variant_name": item.get("variant_name"),
            "product_type": {
                "id": item.get("product_type_id"),
                "name": item.get("product_type_name"),
            },
            "price_list": self._list_info(list_id),
            "normal_gross_price": money(normal),
            "normal_price_fetched_at": iso(item.get("normal_price_fetched_at")),
            "promo_gross_price": money(promo),
            "discount_pct": _discount_pct(normal, promo),
            "bsale_current_gross": money(bsale.gross) if bsale and not bsale.missing else None,
            "bsale_fetched_at": iso(bsale.fetched_at) if bsale else None,
            "verification": verification.value,
            "exception_check": check,
            "exception": exception,
            "clearance_reason": item.get("clearance_reason"),
            "reference_stock": money(_dec(item.get("reference_stock"))),
            "reference_stock_fetched_at": iso(item.get("reference_stock_fetched_at")),
            "notes": item.get("notes"),
            "item_status": item.get("item_status"),
            "legacy_snapshot_id": item.get("legacy_snapshot_id"),
            "created_by": item.get("created_by"),
            "created_at": iso(item.get("created_at")),
            "updated_by": item.get("updated_by"),
            "updated_at": iso(item.get("updated_at")),
            "removed_by": item.get("removed_by"),
            "removed_at": iso(item.get("removed_at")),
            "version": item.get("version"),
            "_search": normalize_label(
                " ".join(str(x or "") for x in (item.get("product_name"), item.get("sku_code"), item.get("barcode"), campaign["name"]))
            ),
        }

    def _campaign_view(self, campaign: Mapping[str, Any], items: list[dict[str, Any]]) -> dict[str, Any]:
        approval = A(campaign["approval_status"])
        timeline = campaign_timeline(approval, campaign["starts_on"], campaign["ends_on"], self._today)
        active = [i for i in items if i["item_status"] == "activo"]
        return {
            "id": int(campaign["id"]),
            "kind": campaign["kind"],
            "name": campaign["name"],
            "starts_on": iso(campaign["starts_on"]),
            "ends_on": iso(campaign["ends_on"]),
            "effective_end_on": iso(effective_end(campaign["ends_on"], campaign.get("cancelled_at"))),
            "approval_status": approval.value,
            "timeline": timeline.value,
            "responsible": campaign.get("responsible"),
            "notes": campaign.get("notes"),
            "created_by": campaign.get("created_by"),
            "created_at": iso(campaign.get("created_at")),
            "updated_by": campaign.get("updated_by"),
            "updated_at": iso(campaign.get("updated_at")),
            "submitted_by": campaign.get("submitted_by"),
            "submitted_at": iso(campaign.get("submitted_at")),
            "approved_by": campaign.get("approved_by"),
            "approved_at": iso(campaign.get("approved_at")),
            "rejected_by": campaign.get("rejected_by"),
            "rejected_at": iso(campaign.get("rejected_at")),
            "cancelled_by": campaign.get("cancelled_by"),
            "cancelled_at": iso(campaign.get("cancelled_at")),
            "status_reason": campaign.get("status_reason"),
            "legacy_promotion_id": campaign.get("legacy_promotion_id"),
            "version": campaign.get("version"),
            "items_active": len(active),
            "products": len({i["variant_id"] for i in active}),
            "price_list_ids": sorted({i["price_list"]["id"] for i in active}),
            "verification": dict(Counter(i["verification"] for i in active)),
            "exceptions": dict(Counter(i["exception_check"] for i in active)),
            "allowed_actions": self._actions(approval, timeline),
            "_search": normalize_label(f"{campaign['name']} " + " ".join(i["_search"] for i in active)),
        }

    @staticmethod
    def _public(row: Mapping[str, Any]) -> dict[str, Any]:
        return {k: v for k, v in row.items() if not k.startswith("_")}

    # ------------------------------------------------------------------ lecturas

    def filters(self) -> dict[str, Any]:
        lists = self._price_scope()
        return {
            "price_lists": [
                {"id": e.price_list_id, "name": e.name, "label": e.visible_name}
                for e in lists.entries
                if e.status is PriceListScopeStatus.EVALUADA
            ],
            "kinds": [k.value for k in CampaignKind],
            "approval_statuses": [a.value for a in A],
            "timelines": [t.value for t in T],
            "clearance_reasons": [r.value for r in ClearanceReason],
            "verifications": [v.value for v in V],
            "exception_checks": [c.value for c in PromotionCheck] + [CHECK_NO_VIGENTE, CHECK_NO_PARTICIPA],
            "sections": list(SECTIONS),
            "meta": self._meta(),
        }

    def summary(self, *, month: str | None = None) -> dict[str, Any]:
        first, last, label = month_bounds(month, self._today)
        campaigns, items = self._load(first, max(last, self._today + timedelta(days=UPCOMING_HORIZON_DAYS)))
        month_campaigns = [c for c in campaigns if overlaps(date.fromisoformat(c["starts_on"]), date.fromisoformat(c["ends_on"]), first, last)]
        live = [i for i in items if i["item_status"] == "activo" and i["timeline"] == T.VIGENTE.value]
        approved_live = [i for i in live if i["approval_status"] == A.APROBADA.value]
        participating = [i for i in live if i["approval_status"] in {a.value for a in _PARTICIPATING}]
        kpis = {
            "campaigns_vigentes": len({c["id"] for c in campaigns if c["timeline"] == T.VIGENTE.value and c["approval_status"] == A.APROBADA.value}),
            "products_on_offer": len({i["variant_id"] for i in approved_live if i["kind"] == CampaignKind.OFERTA.value}),
            "products_on_clearance": len({i["variant_id"] for i in approved_live if i["kind"] == CampaignKind.REMATE.value}),
            "campaigns_finished_month": sum(
                1
                for c in month_campaigns
                if c["timeline"] in (T.FINALIZADA.value, T.CANCELADA.value) and first <= date.fromisoformat(c["effective_end_on"]) <= last
            ),
            "exceptions_verified": sum(1 for i in participating if i["exception_check"] == PromotionCheck.EXCEPCION_VERIFICADA.value),
            "exceptions_pending": sum(1 for i in participating if i["exception_check"] != PromotionCheck.EXCEPCION_VERIFICADA.value),
            "campaigns_pending_approval": sum(1 for c in campaigns if c["approval_status"] == A.PENDIENTE_APROBACION.value and c["timeline"] != T.FINALIZADA.value),
            "campaigns_upcoming": sum(1 for c in campaigns if c["timeline"] == T.PROGRAMADA.value),
            "items_expired_not_reverted": sum(
                1 for i in items if i["item_status"] == "activo" and i["verification"] == V.VENCIDA_SIN_REVERTIR.value
            ),
        }
        return {
            "month": label,
            "month_from": iso(first),
            "month_to": iso(last),
            "kpis": kpis,
            "month_by_kind": dict(Counter(c["kind"] for c in month_campaigns)),
            "month_by_approval": dict(Counter(c["approval_status"] for c in month_campaigns)),
            "month_by_timeline": dict(Counter(c["timeline"] for c in month_campaigns)),
            "verification_live": dict(Counter(i["verification"] for i in participating)),
            "legacy": self._legacy_counts(),
            "meta": self._meta(),
        }

    def _legacy_counts(self) -> dict[str, Any]:
        rows = self._repo.legacy_promotions(company_id=self.company_id)
        return {"promotions": len(rows), "active_flag": sum(1 for r in rows if r.get("activa"))}

    def _validate_list_filter(self, price_list_id: int | None) -> None:
        if price_list_id is not None and price_list_id not in self._price_scope().evaluated_ids:
            raise CampaignValidationError("La lista no está en el alcance comercial vigente")

    def list_campaigns(
        self,
        *,
        month: str | None = None,
        kind: str | None = None,
        approval: str | None = None,
        timeline: str | None = None,
        price_list_id: int | None = None,
        q: str | None = None,
        page: int = 1,
        page_size: int = 50,
    ) -> dict[str, Any]:
        first, last, label = month_bounds(month, self._today)
        self._validate_enum(kind, CampaignKind, "kind")
        self._validate_enum(approval, A, "approval")
        self._validate_enum(timeline, T, "timeline")
        self._validate_list_filter(price_list_id)
        campaigns, _ = self._load(first, last)
        needle = normalize_label(q) if q else ""
        rows = [
            c
            for c in campaigns
            if overlaps(date.fromisoformat(c["starts_on"]), date.fromisoformat(c["ends_on"]), first, last)
            and (kind is None or c["kind"] == kind)
            and (approval is None or c["approval_status"] == approval)
            and (timeline is None or c["timeline"] == timeline)
            and (price_list_id is None or price_list_id in c["price_list_ids"])
            and (not needle or needle in c["_search"])
        ]
        result = paginate(rows, page=page, page_size=page_size)
        result["items"] = [self._public(c) for c in result["items"]]
        result["month"] = label
        result["meta"] = self._meta()
        return result

    @staticmethod
    def _validate_enum(value: str | None, enum: Any, label: str) -> None:
        if value is not None and value not in {e.value for e in enum}:
            raise CampaignValidationError(f"{label} inválido: {value}")

    @staticmethod
    def _in_section(i: Mapping[str, Any], section: str) -> bool:
        active = i["item_status"] == "activo"
        approval, timeline = i["approval_status"], i["timeline"]
        if section == "vigentes":
            return active and timeline == T.VIGENTE.value and approval == A.APROBADA.value and i["kind"] == CampaignKind.OFERTA.value
        if section == "remates":
            return (
                active
                and i["kind"] == CampaignKind.REMATE.value
                and timeline in (T.VIGENTE.value, T.PROGRAMADA.value)
                and approval in (A.BORRADOR.value, A.PENDIENTE_APROBACION.value, A.APROBADA.value)
            )
        if section == "proximas":
            return active and timeline == T.PROGRAMADA.value and approval not in (A.CANCELADA.value, A.RECHAZADA.value)
        if section == "pendientes":
            if not active or timeline in (T.FINALIZADA.value, T.CANCELADA.value):
                return False
            if approval == A.PENDIENTE_APROBACION.value:
                return True
            return approval == A.APROBADA.value and timeline == T.VIGENTE.value and i["exception_check"] != PromotionCheck.EXCEPCION_VERIFICADA.value
        return True

    def list_items(
        self,
        *,
        section: str = "vigentes",
        month: str | None = None,
        kind: str | None = None,
        approval: str | None = None,
        price_list_id: int | None = None,
        q: str | None = None,
        page: int = 1,
        page_size: int = 50,
    ) -> dict[str, Any]:
        if section not in SECTIONS:
            raise CampaignValidationError(f"section inválida: {section}")
        self._validate_enum(kind, CampaignKind, "kind")
        self._validate_enum(approval, A, "approval")
        self._validate_list_filter(price_list_id)
        first, last, label = month_bounds(month, self._today)
        if section in ("vigentes", "remates", "pendientes"):
            lo, hi = self._today, self._today
        elif section == "proximas":
            lo, hi = self._today + timedelta(days=1), self._today + timedelta(days=UPCOMING_HORIZON_DAYS)
        else:
            lo, hi = first, last
        if section in ("remates",):
            hi = self._today + timedelta(days=UPCOMING_HORIZON_DAYS)
        _, items = self._load(lo, hi)
        needle = normalize_label(q) if q else ""
        rows = [
            i
            for i in items
            if overlaps(date.fromisoformat(i["starts_on"]), date.fromisoformat(i["ends_on"]), lo, hi)
            and self._in_section(i, section)
            and (kind is None or i["kind"] == kind)
            and (approval is None or i["approval_status"] == approval)
            and (price_list_id is None or i["price_list"]["id"] == price_list_id)
            and (not needle or needle in i["_search"])
        ]
        rows.sort(key=lambda i: (i["starts_on"], normalize_label(i["product_name"] or ""), i["price_list"]["id"], i["item_id"]))
        result = paginate(rows, page=page, page_size=page_size)
        result["items"] = [self._public(i) for i in result["items"]]
        result["section"] = section
        result["month"] = label
        result["meta"] = self._meta()
        return result

    def campaign_detail(self, campaign_id: int) -> dict[str, Any]:
        self._require_ready()
        campaign = self._repo.campaign(company_id=self.company_id, campaign_id=campaign_id)
        if campaign is None:
            raise CampaignNotFound("Campaña inexistente en la empresa")
        items = self._repo.items(company_id=self.company_id, campaign_ids=[campaign_id])
        prices = self._prices_for(items)
        candidates = self._candidates()
        views = [self._item_view(campaign, i, prices, candidates) for i in items]
        out = self._public(self._campaign_view(campaign, views))
        out["items"] = [self._public(v) for v in views]
        out["history"] = [self._history_view(h) for h in self._repo.campaign_history(company_id=self.company_id, campaign_id=campaign_id)]
        out["measurement"] = measurement_windows(campaign["starts_on"], campaign["ends_on"], campaign.get("cancelled_at"), self._today)
        out["meta"] = self._meta()
        return out

    @staticmethod
    def _history_view(h: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "id": h["id"],
            "campaign_id": h["campaign_id"],
            "campaign_name": h.get("campaign_name"),
            "kind": h.get("kind"),
            "item_id": h.get("item_id"),
            "product_name": h.get("product_name"),
            "price_list_id": h.get("price_list_id"),
            "event": h["event"],
            "previous_status": h.get("previous_status"),
            "new_status": h.get("new_status"),
            "previous_values": h.get("previous_values"),
            "new_values": h.get("new_values"),
            "reason": h.get("reason"),
            "actor": h.get("actor"),
            "actor_role": h.get("actor_role"),
            "occurred_at": iso(h.get("occurred_at")),
        }

    def history(self, *, month: str | None = None, page: int = 1, page_size: int = 50) -> dict[str, Any]:
        first, last, label = month_bounds(month, self._today)
        if not self._tables_ready():
            return {"items": [], "page": page, "page_size": page_size, "total": 0, "total_pages": 0, "month": label, "meta": self._meta()}
        tz = _business_tz()
        since = datetime.combine(first, time.min, tzinfo=tz)
        until = datetime.combine(last + timedelta(days=1), time.min, tzinfo=tz)
        rows = self._repo.company_history(
            company_id=self.company_id, since=since, until=until, limit=page_size, offset=(page - 1) * page_size
        )
        total = int(rows[0]["total_rows"]) if rows else 0
        return {
            "items": [self._history_view(r) for r in rows],
            "page": page,
            "page_size": page_size,
            "total": total,
            "total_pages": (total + page_size - 1) // page_size if total else 0,
            "month": label,
            "meta": self._meta(),
        }

    def legacy(self) -> dict[str, Any]:
        """Promociones legacy (app.promotions*) en solo lectura: se conservan y nunca otorgan excepciones."""
        scope = self._price_scope()
        imported = self._repo.legacy_imported(company_id=self.company_id) if self._tables_ready() else {}
        snapshots: dict[int, list[dict[str, Any]]] = {}
        for row in self._repo.legacy_snapshots(company_id=self.company_id):
            status, variant_id, list_id = legacy_mapping_status(row, scope)
            regular, sale = _dec(row.get("regular_price")), _dec(row.get("sale_price"))
            snapshots.setdefault(int(row["promotion_id"]), []).append(
                {
                    "snapshot_id": row["snapshot_id"],
                    "barcode": row.get("barcode"),
                    "price_list_text": row.get("price_list"),
                    "regular_price": money(regular),
                    "sale_price": money(sale),
                    "discount_pct": _discount_pct(regular, sale),
                    "generated_at": iso(row.get("fecha_generado")),
                    "mapping_status": status,
                    "variant_id": variant_id,
                    "price_list_id": list_id,
                }
            )
        promotions = []
        for p in self._repo.legacy_promotions(company_id=self.company_id):
            pid = int(p["promotion_id"])
            promotions.append(
                {
                    "promotion_id": pid,
                    "tipo": p.get("tipo"),
                    "canal": p.get("canal"),
                    "starts_on": iso(p.get("fecha_inicio")),
                    "ends_on": iso(p.get("fecha_fin")),
                    "active_flag": bool(p.get("activa")),
                    "legacy_status": self._legacy_status(p),
                    "created_at": iso(p.get("created_at")),
                    "items": int(p.get("items") or 0),
                    "snapshots": snapshots.get(pid, []),
                    "imported_campaign_id": imported.get(pid),
                    "exception_policy": "nunca_otorga_excepcion",
                }
            )
        return {"promotions": promotions, "total": len(promotions), "meta": self._meta()}

    def _legacy_status(self, p: Mapping[str, Any]) -> str:
        if not p.get("activa"):
            return "inactiva"
        if self._today < p["fecha_inicio"]:
            return "programada"
        if self._today > p["fecha_fin"]:
            return "vencida"
        return "activa"

    def search_products(self, *, q: str) -> dict[str, Any]:
        text = (q or "").strip()
        if len(text) < 2:
            raise CampaignValidationError("Ingrese al menos 2 caracteres para buscar")
        rows = self._repo.search_products(company_id=self.company_id, q=text[:60], limit=SEARCH_LIMIT)
        lists = sorted(self._price_scope().evaluated_ids)
        prices = self._repo.raw_prices(
            company_id=self.company_id, variant_ids=[int(r["variant_id"]) for r in rows], price_list_ids=lists
        )
        by_variant: dict[int, list[dict[str, Any]]] = {}
        for p in prices:
            by_variant.setdefault(int(p["variant_id"]), []).append(
                {
                    "price_list": self._list_info(int(p["price_list_id"])),
                    "gross": money(_dec(p.get("variant_value_with_taxes"))) if p.get("missing_since") is None else None,
                    "fetched_at": iso(p.get("api_fetched_at")),
                }
            )
        return {
            "items": [
                {
                    "variant_id": int(r["variant_id"]),
                    "product_id": r.get("product_id"),
                    "code": r.get("code"),
                    "barcode": r.get("bar_code"),
                    "product_name": r.get("product_name"),
                    "variant_name": r.get("description"),
                    "product_type": {
                        "id": r.get("product_type_id"),
                        "name": r.get("product_type_name"),
                    },
                    "variant_active": r.get("state") == 0,
                    "prices": sorted(by_variant.get(int(r["variant_id"]), []), key=lambda x: x["price_list"]["id"]),
                }
                for r in rows
            ],
            "meta": self._meta(),
        }

    # ------------------------------------------------------------------ escrituras

    def _campaign_or_404(self, campaign_id: int) -> dict[str, Any]:
        campaign = self._repo.campaign(company_id=self.company_id, campaign_id=campaign_id)
        if campaign is None:
            raise CampaignNotFound("Campaña inexistente en la empresa")
        return campaign

    def _state(self, campaign: Mapping[str, Any]) -> tuple[A, T]:
        approval = A(campaign["approval_status"])
        return approval, campaign_timeline(approval, campaign["starts_on"], campaign["ends_on"], self._today)

    @staticmethod
    def _expected_version(body: Mapping[str, Any]) -> int:
        return _parse_int(body.get("version"), "version")

    def _history(
        self,
        campaign_id: int,
        event: str,
        *,
        item_id: int | None = None,
        previous_status: str | None = None,
        new_status: str | None = None,
        previous_values: Mapping[str, Any] | None = None,
        new_values: Mapping[str, Any] | None = None,
        reason: str | None = None,
    ) -> None:
        self._repo.insert_history(
            company_id=self.company_id,
            campaign_id=campaign_id,
            item_id=item_id,
            event=event,
            previous_status=previous_status,
            new_status=new_status,
            previous_values=previous_values,
            new_values=new_values,
            reason=reason,
            actor=self._actor.email,
            actor_role=self._actor.role,
        )

    def _save(self, campaign: Mapping[str, Any], expected_version: int, changes: Mapping[str, Any]) -> dict[str, Any]:
        values = {**campaign, **changes}
        saved = self._repo.save_campaign(
            company_id=self.company_id,
            campaign_id=int(campaign["id"]),
            expected_version=expected_version,
            values=values,
            actor=self._actor.email,
        )
        if saved is None:
            raise CampaignConflict("La campaña fue modificada por otro usuario; recargue antes de continuar")
        return saved

    def _revoke_if_needed(self, campaign: Mapping[str, Any], new_approval: A, reason: str) -> dict[str, Any]:
        """Cambios materiales en una campaña aprobada la devuelven a aprobación (queda en historial)."""
        old = A(campaign["approval_status"])
        if new_approval is old:
            return {}
        self._history(
            int(campaign["id"]),
            "aprobacion_revocada_por_cambio" if old is A.APROBADA else "campania_editada",
            previous_status=old.value,
            new_status=new_approval.value,
            previous_values={"approved_by": campaign.get("approved_by"), "approved_at": campaign.get("approved_at")},
            reason=reason,
        )
        changes: dict[str, Any] = {"approval_status": new_approval.value}
        if old is A.APROBADA:
            changes.update({"approved_by": None, "approved_at": None})
        return changes

    def create_campaign(self, body: Mapping[str, Any]) -> dict[str, Any]:
        self._require_cap("create")
        self._require_ready()
        kind = _parse_kind(body.get("kind"))
        name = clean_text(body.get("name"), max_len=120, required=True, label="Nombre")
        starts_on = _parse_date(body.get("starts_on"), "Fecha de inicio")
        ends_on = _parse_date(body.get("ends_on"), "Fecha de término")
        validate_dates(starts_on, ends_on)
        if ends_on < self._today:
            raise CampaignValidationError("No se registran campañas ya finalizadas")
        responsible = clean_text(
            body.get("responsible"), max_len=120, required=kind is CampaignKind.REMATE, label="Responsable del remate"
        )
        notes = clean_text(body.get("notes"), max_len=1000, label="Observaciones")
        row = self._repo.insert_campaign(
            company_id=self.company_id,
            values={"kind": kind.value, "name": name, "starts_on": starts_on, "ends_on": ends_on, "responsible": responsible, "notes": notes},
            actor=self._actor.email,
        )
        self._history(
            int(row["id"]),
            "campania_creada",
            new_status=A.BORRADOR.value,
            new_values={"kind": kind.value, "name": name, "starts_on": starts_on, "ends_on": ends_on, "responsible": responsible, "notes": notes},
        )
        return self.campaign_detail(int(row["id"]))

    def update_campaign(self, campaign_id: int, body: Mapping[str, Any]) -> dict[str, Any]:
        self._require_cap("edit")
        self._require_ready()
        expected = self._expected_version(body)
        campaign = self._campaign_or_404(campaign_id)
        approval, timeline = self._state(campaign)
        assert_editable(approval, timeline)
        kind = CampaignKind(campaign["kind"])
        new = {
            "name": clean_text(body.get("name", campaign["name"]), max_len=120, required=True, label="Nombre"),
            "starts_on": _parse_date(body.get("starts_on", campaign["starts_on"]), "Fecha de inicio"),
            "ends_on": _parse_date(body.get("ends_on", campaign["ends_on"]), "Fecha de término"),
            "responsible": clean_text(
                body.get("responsible", campaign.get("responsible")),
                max_len=120,
                required=kind is CampaignKind.REMATE,
                label="Responsable del remate",
            ),
            "notes": clean_text(body.get("notes", campaign.get("notes")), max_len=1000, label="Observaciones"),
        }
        validate_dates(new["starts_on"], new["ends_on"])
        if new["ends_on"] < self._today:
            raise CampaignValidationError("Para terminar antes una campaña use Cancelar")
        changed = {k: (campaign.get(k), v) for k, v in new.items() if campaign.get(k) != v}
        if not changed:
            return self.campaign_detail(campaign_id)
        material = bool({"starts_on", "ends_on"} & set(changed))
        changes: dict[str, Any] = dict(new)
        if material:
            changes.update(self._revoke_if_needed(campaign, approval_after_change(approval), "cambio_de_fechas"))
        self._save(campaign, expected, changes)
        self._history(
            campaign_id,
            "campania_editada",
            previous_values={k: old for k, (old, _) in changed.items()},
            new_values={k: v for k, (_, v) in changed.items()},
        )
        return self.campaign_detail(campaign_id)

    def _overlap_warnings(self, campaign: Mapping[str, Any], pairs: set[tuple[int, int]]) -> list[dict[str, Any]]:
        rows = self._repo.covering_items(company_id=self.company_id, date_from=campaign["starts_on"], date_to=campaign["ends_on"])
        return [
            {
                "campaign_id": int(r["campaign_id"]),
                "campaign_name": r.get("campaign_name"),
                "approval_status": r.get("approval_status"),
                "item_id": int(r["item_id"]),
                "variant_id": int(r["variant_id"]),
                "price_list_id": int(r["price_list_id"]),
                "promo_gross_price": money(_dec(r.get("promo_gross_price"))),
                "starts_on": iso(r.get("starts_on")),
                "ends_on": iso(r.get("ends_on")),
            }
            for r in rows
            if int(r["campaign_id"]) != int(campaign["id"]) and (int(r["variant_id"]), int(r["price_list_id"])) in pairs
        ]

    def add_item(self, campaign_id: int, body: Mapping[str, Any]) -> dict[str, Any]:
        self._require_cap("edit")
        self._require_ready()
        expected = self._expected_version(body)
        campaign = self._campaign_or_404(campaign_id)
        approval, timeline = self._state(campaign)
        assert_editable(approval, timeline)
        kind = CampaignKind(campaign["kind"])
        variant_id = _parse_int(body.get("variant_id"), "variant_id")
        price_list_id = _parse_int(body.get("price_list_id"), "price_list_id")
        if price_list_id not in self._price_scope().evaluated_ids:
            raise CampaignValidationError("La lista no está en el alcance comercial vigente (retirada o no autorizada)")
        promo = _parse_price(body.get("promo_gross_price"))
        clearance = _parse_clearance(body.get("clearance_reason"), kind)
        notes = clean_text(body.get("notes"), max_len=1000, label="Observaciones")
        variant = self._repo.raw_variant(company_id=self.company_id, variant_id=variant_id)
        if variant is None or variant.get("missing_since") is not None:
            raise CampaignValidationError("La variante no existe en Bsale para esta empresa")
        existing = self._repo.items(company_id=self.company_id, campaign_ids=[campaign_id])
        if any(
            i["item_status"] == "activo" and int(i["variant_id"]) == variant_id and int(i["price_list_id"]) == price_list_id
            for i in existing
        ):
            raise CampaignConflict("El producto ya está en esta campaña para esa lista")

        price_rows = self._repo.raw_prices(company_id=self.company_id, variant_ids=[variant_id], price_list_ids=[price_list_id])
        price_row = price_rows[0] if price_rows else None
        normal = _dec(price_row.get("variant_value_with_taxes")) if price_row and price_row.get("missing_since") is None else None
        stock = self._repo.raw_stock(company_id=self.company_id, variant_id=variant_id) if kind is CampaignKind.REMATE else None
        warnings: list[dict[str, Any]] = []
        if normal is None:
            warnings.append({"code": "sin_precio_bsale", "detail": "Sin precio vigente en BSALE RAW para esa lista"})
        elif promo >= normal:
            warnings.append({"code": "precio_promocional_no_menor_al_normal", "detail": "El precio declarado no es menor al precio Bsale actual"})
        overlaps_found = self._overlap_warnings(campaign, {(variant_id, price_list_id)})
        if overlaps_found:
            warnings.append({"code": "superposicion_con_otra_campania", "detail": overlaps_found})

        changes = self._revoke_if_needed(campaign, approval_after_change(approval), "producto_agregado")
        self._save(campaign, expected, changes)
        values = {
            "variant_id": variant_id,
            "product_id": variant.get("product_id"),
            "price_list_id": price_list_id,
            "sku_code": variant.get("code"),
            "barcode": variant.get("bar_code"),
            "product_name": variant.get("product_name"),
            "variant_name": variant.get("description"),
            "normal_gross_price": normal,
            "normal_price_fetched_at": price_row.get("api_fetched_at") if price_row else None,
            "promo_gross_price": promo,
            "clearance_reason": clearance,
            "reference_stock": _dec(stock.get("quantity_available")) if stock else None,
            "reference_stock_fetched_at": stock.get("fetched_at") if stock else None,
            "notes": notes,
        }
        item = self._repo.insert_item(company_id=self.company_id, campaign_id=campaign_id, values=values, actor=self._actor.email)
        self._history(
            campaign_id,
            "item_agregado",
            item_id=int(item["id"]),
            new_values={k: values[k] for k in ("variant_id", "price_list_id", "normal_gross_price", "promo_gross_price", "clearance_reason", "reference_stock", "notes")},
        )
        return {"campaign": self.campaign_detail(campaign_id), "warnings": warnings}

    def _item_or_404(self, campaign_id: int, item_id: int) -> dict[str, Any]:
        item = self._repo.item(company_id=self.company_id, campaign_id=campaign_id, item_id=item_id)
        if item is None:
            raise CampaignNotFound("Producto inexistente en la campaña")
        return item

    def _save_item(self, campaign_id: int, item: Mapping[str, Any], changes: Mapping[str, Any]) -> dict[str, Any]:
        saved = self._repo.save_item(
            company_id=self.company_id,
            campaign_id=campaign_id,
            item_id=int(item["id"]),
            expected_version=int(item["version"]),
            values={**item, **changes},
            actor=self._actor.email,
        )
        if saved is None:
            raise CampaignConflict("El producto fue modificado por otro usuario; recargue antes de continuar")
        return saved

    def update_item(self, campaign_id: int, item_id: int, body: Mapping[str, Any]) -> dict[str, Any]:
        self._require_cap("edit")
        self._require_ready()
        expected = self._expected_version(body)
        campaign = self._campaign_or_404(campaign_id)
        approval, timeline = self._state(campaign)
        assert_editable(approval, timeline)
        item = self._item_or_404(campaign_id, item_id)
        if item["item_status"] != "activo":
            raise CampaignConflict("El producto fue retirado de la campaña")
        kind = CampaignKind(campaign["kind"])
        new = {
            "promo_gross_price": _parse_price(body["promo_gross_price"]) if "promo_gross_price" in body else _dec(item["promo_gross_price"]),
            "clearance_reason": _parse_clearance(body.get("clearance_reason", item.get("clearance_reason")), kind),
            "notes": clean_text(body.get("notes", item.get("notes")), max_len=1000, label="Observaciones"),
        }
        old = {"promo_gross_price": _dec(item["promo_gross_price"]), "clearance_reason": item.get("clearance_reason"), "notes": item.get("notes")}
        changed = {k for k in new if new[k] != old[k]}
        if not changed:
            return {"campaign": self.campaign_detail(campaign_id), "warnings": []}
        price_changed = "promo_gross_price" in changed
        changes = self._revoke_if_needed(campaign, approval_after_change(approval), "precio_promocional_cambiado") if price_changed or "clearance_reason" in changed else {}
        self._save(campaign, expected, changes)
        self._save_item(campaign_id, item, new)
        self._history(
            campaign_id,
            "item_precio_cambiado" if price_changed else "item_editado",
            item_id=item_id,
            previous_values={k: old[k] for k in changed} | {"normal_gross_price_registro": _dec(item.get("normal_gross_price"))},
            new_values={k: new[k] for k in changed},
            reason=clean_text(body.get("reason"), max_len=500, label="Motivo"),
        )
        return {"campaign": self.campaign_detail(campaign_id), "warnings": []}

    def remove_item(self, campaign_id: int, item_id: int, body: Mapping[str, Any]) -> dict[str, Any]:
        self._require_cap("edit")
        self._require_ready()
        expected = self._expected_version(body)
        reason = clean_text(body.get("reason"), max_len=500, required=True, label="Motivo")
        campaign = self._campaign_or_404(campaign_id)
        approval, timeline = self._state(campaign)
        assert_editable(approval, timeline)
        item = self._item_or_404(campaign_id, item_id)
        if item["item_status"] != "activo":
            raise CampaignConflict("El producto ya fue retirado")
        changes = self._revoke_if_needed(campaign, approval_after_change(approval), "producto_retirado")
        self._save(campaign, expected, changes)
        self._save_item(campaign_id, item, {"item_status": "retirado", "removed_by": self._actor.email, "removed_at": self._clock()})
        self._history(
            campaign_id,
            "item_retirado",
            item_id=item_id,
            previous_values={"item_status": "activo", "promo_gross_price": _dec(item["promo_gross_price"])},
            new_values={"item_status": "retirado"},
            reason=reason,
        )
        return {"campaign": self.campaign_detail(campaign_id), "warnings": []}

    def transition(self, campaign_id: int, action: str, body: Mapping[str, Any]) -> dict[str, Any]:
        if action not in ACTION_PERMISSION:
            raise CampaignValidationError(f"acción inválida: {action}")
        self._require_cap(ACTION_PERMISSION[action])
        self._require_ready()
        expected = self._expected_version(body)
        campaign = self._campaign_or_404(campaign_id)
        approval, timeline = self._state(campaign)
        target = next_approval(action, approval, timeline)
        reason = clean_text(body.get("reason"), max_len=500, required=action in REASON_REQUIRED, label="Motivo")
        items = [i for i in self._repo.items(company_id=self.company_id, campaign_ids=[campaign_id]) if i["item_status"] == "activo"]
        if action in ("submit", "approve") and not items:
            raise CampaignConflict("La campaña no tiene productos")
        extra: dict[str, Any] = {}
        if action == "approve":
            out_of_scope = sorted({int(i["price_list_id"]) for i in items} - set(self._price_scope().evaluated_ids))
            if out_of_scope:
                raise CampaignConflict(f"Listas fuera del alcance comercial vigente: {out_of_scope}")
            approved_overlaps = [
                o
                for o in self._overlap_warnings(campaign, {(int(i["variant_id"]), int(i["price_list_id"])) for i in items})
                if o["approval_status"] == A.APROBADA.value
            ]
            if approved_overlaps:
                raise CampaignConflict(
                    "Productos ya incluidos en otra campaña aprobada en las mismas fechas: "
                    + ", ".join(f"campaña {o['campaign_id']} variante {o['variant_id']} lista {o['price_list_id']}" for o in approved_overlaps[:10])
                )
            extra["self_approved"] = campaign.get("created_by") == self._actor.email
        now = self._clock()
        changes: dict[str, Any] = {"approval_status": target.value, "status_reason": reason}
        if action == "submit":
            changes.update({"submitted_by": self._actor.email, "submitted_at": now})
        elif action == "approve":
            changes.update({"approved_by": self._actor.email, "approved_at": now})
        elif action == "reject":
            changes.update({"rejected_by": self._actor.email, "rejected_at": now})
        elif action == "cancel":
            changes.update({"cancelled_by": self._actor.email, "cancelled_at": now})
        self._save(campaign, expected, changes)
        self._history(
            campaign_id,
            TRANSITION_EVENTS[action],
            previous_status=approval.value,
            new_status=target.value,
            new_values={"items_active": len(items), **extra},
            reason=reason,
        )
        return self.campaign_detail(campaign_id)

    def import_legacy(self, promotion_id: int) -> dict[str, Any]:
        """Crea un borrador V2 desde una promoción legacy sin tocarla. Lo no mapeable se informa y no se importa."""
        self._require_cap("create")
        self._require_ready()
        if promotion_id in self._repo.legacy_imported(company_id=self.company_id):
            raise CampaignConflict("Esta promoción legacy ya fue importada")
        rows = [r for r in self._repo.legacy_snapshots(company_id=self.company_id) if int(r["promotion_id"]) == promotion_id]
        if not rows:
            raise CampaignNotFound("Promoción legacy sin precios congelados en esta empresa")
        head = rows[0]
        kind = CampaignKind.REMATE if (head.get("tipo") or "").lower() == "remate" else CampaignKind.OFERTA
        campaign = self._repo.insert_campaign(
            company_id=self.company_id,
            values={
                "kind": kind.value,
                "name": f"Promoción legacy #{promotion_id} ({head.get('tipo')}/{head.get('canal')})",
                "starts_on": head["fecha_inicio"],
                "ends_on": head["fecha_fin"],
                "responsible": self._actor.email if kind is CampaignKind.REMATE else None,
                "notes": "Importada desde app.promotions; la promoción legacy se conserva sin cambios.",
                "legacy_promotion_id": promotion_id,
            },
            actor=self._actor.email,
        )
        cid = int(campaign["id"])
        scope = self._price_scope()
        imported, skipped = [], []
        for row in rows:
            status, variant_id, list_id = legacy_mapping_status(row, scope)
            sale = _dec(row.get("sale_price"))
            if status != LEGACY_MAPPEABLE or variant_id is None or list_id is None:
                skipped.append({"snapshot_id": row["snapshot_id"], "reason": status})
                continue
            try:
                promo = validate_promo_price(sale) if sale is not None else None
            except CampaignValidationError:
                promo = None
            if promo is None:
                skipped.append({"snapshot_id": row["snapshot_id"], "reason": "precio_legacy_invalido"})
                continue
            if (variant_id, list_id) in {(i["variant_id"], i["price_list_id"]) for i in imported}:
                skipped.append({"snapshot_id": row["snapshot_id"], "reason": "duplicado"})
                continue
            variant = self._repo.raw_variant(company_id=self.company_id, variant_id=variant_id) or {}
            price_rows = self._repo.raw_prices(company_id=self.company_id, variant_ids=[variant_id], price_list_ids=[list_id])
            pr = price_rows[0] if price_rows else None
            item = self._repo.insert_item(
                company_id=self.company_id,
                campaign_id=cid,
                values={
                    "variant_id": variant_id,
                    "product_id": variant.get("product_id"),
                    "price_list_id": list_id,
                    "sku_code": variant.get("code"),
                    "barcode": variant.get("bar_code") or row.get("barcode"),
                    "product_name": variant.get("product_name"),
                    "variant_name": variant.get("description"),
                    "normal_gross_price": _dec(pr.get("variant_value_with_taxes")) if pr and pr.get("missing_since") is None else None,
                    "normal_price_fetched_at": pr.get("api_fetched_at") if pr else None,
                    "promo_gross_price": promo,
                    "clearance_reason": ClearanceReason.OTRO.value if kind is CampaignKind.REMATE else None,
                    "notes": f"Legacy: precio normal congelado {money(_dec(row.get('regular_price')))}",
                    "legacy_snapshot_id": int(row["snapshot_id"]),
                },
                actor=self._actor.email,
            )
            imported.append({"item_id": int(item["id"]), "variant_id": variant_id, "price_list_id": list_id, "snapshot_id": row["snapshot_id"]})
        self._history(
            cid,
            "importada_legacy",
            new_status=A.BORRADOR.value,
            new_values={"legacy_promotion_id": promotion_id, "imported": imported, "skipped": skipped},
        )
        detail = self.campaign_detail(cid)
        return {"campaign": detail, "imported": len(imported), "skipped": skipped}


# ---------------------------------------------------------------------- runners


def run_promotions_read(
    user: dict[str, Any],
    company_id: int,
    fn: Callable[[PromotionsV2Service], Any],
    *,
    get_conn: Callable[[], Any] = get_connection,
) -> Any:
    conn = open_readonly_connection(get_conn)
    try:
        executor = make_psycopg_executor(conn, statement_timeout_seconds=20, lock_timeout="3s")
        scope = resolve_company_scope(user, company_id, executor)
        return fn(PromotionsV2Service(PromotionsV2Repository(executor), scope, user=user))
    finally:
        try:
            conn.rollback()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


def run_promotions_write(
    user: dict[str, Any],
    company_id: int,
    fn: Callable[[PromotionsV2Service], Any],
    *,
    get_conn: Callable[[], Any] = get_connection,
) -> Any:
    """Una transacción por acción: cambios + historial se confirman juntos o no se confirma nada."""
    conn = open_readwrite_connection(get_conn)
    try:
        executor = make_psycopg_rw_executor(conn, statement_timeout_seconds=15, lock_timeout="5s")
        scope = resolve_company_scope(user, company_id, executor)
        result = fn(PromotionsV2Service(PromotionsV2Repository(executor), scope, user=user))
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        try:
            conn.close()
        except Exception:
            pass
    from backend.services.analytics_v2.dashboard_service import clear_cache as clear_dashboard_cache
    from backend.services.analytics_v2.margins_service import clear_cache

    clear_cache()
    clear_dashboard_cache()
    return result
