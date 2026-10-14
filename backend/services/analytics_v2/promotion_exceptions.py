"""Contrato de excepciones por promoción o remate entre Promociones V2 y Márgenes V2.

Una excepción solo reemplaza la alerta normal de bajo margen si, a la fecha evaluada:
1. corresponde a la misma empresa, 2. a la misma variante y 3. a la lista evaluada;
4. está aprobada; 5. está vigente; 6. su precio coincide con el precio bruto vigente en BSALE RAW
(sincronizado recientemente) y 7. no hay otra promoción activa en conflicto.

Si algo falla no hay excepción silenciosa: el resultado informa el motivo. Las promociones legacy
(app.promotions) nunca otorgan excepciones; solo se señalan como pendientes de verificación.
Al vencer la campaña deja de aplicar sola porque la vigencia se evalúa contra la fecha de consulta.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any, Iterable, Protocol

from backend.services.analytics_v2.contracts import MarginComplianceStatus, PromotionCheck, iso, money


class ExceptionKind(StrEnum):
    PROMOCION = "promocion"
    REMATE = "remate"


class ApprovalStatus(StrEnum):
    PENDIENTE = "pendiente"
    APROBADA = "aprobada"
    RECHAZADA = "rechazada"


class ExceptionOrigin(StrEnum):
    V2 = "promociones_v2"
    LEGACY = "promociones_legacy"


@dataclass(frozen=True)
class PromotionException:
    company_id: int
    variant_id: int
    price_list_id: int
    kind: ExceptionKind
    starts_on: date
    ends_on: date
    applicable_gross_price: Decimal
    approval: ApprovalStatus
    reference: str | None = None
    origin: ExceptionOrigin = ExceptionOrigin.V2
    campaign_id: int | None = None
    item_id: int | None = None
    campaign_name: str | None = None
    approved_by: str | None = None
    approved_at: datetime | None = None

    def is_active_on(self, day: date) -> bool:
        return self.starts_on <= day <= self.ends_on

    def to_json(self) -> dict[str, Any]:
        return {
            "kind": self.kind.value,
            "origin": self.origin.value,
            "campaign_id": self.campaign_id,
            "item_id": self.item_id,
            "campaign_name": self.campaign_name,
            "starts_on": iso(self.starts_on),
            "ends_on": iso(self.ends_on),
            "applicable_gross_price": money(self.applicable_gross_price),
            "approval": self.approval.value,
            "approved_by": self.approved_by,
            "approved_at": iso(self.approved_at),
            "reference": self.reference,
        }


class PromotionExceptionSource(Protocol):
    available: bool

    def exceptions_for(self, company_id: int) -> Iterable[PromotionException]: ...


class UnavailablePromotionSource:
    """Sin fuente integrada (p. ej. tablas V2 aún no migradas): no concede excepciones."""

    available = False

    def exceptions_for(self, company_id: int) -> Iterable[PromotionException]:
        return ()


@dataclass(frozen=True)
class ExceptionResolution:
    check: PromotionCheck
    exception: PromotionException | None = None
    conflicts: tuple[PromotionException, ...] = ()
    reason: str | None = None

    @property
    def verified_status(self) -> MarginComplianceStatus | None:
        if self.check is not PromotionCheck.EXCEPCION_VERIFICADA or self.exception is None:
            return None
        if self.exception.kind is ExceptionKind.REMATE:
            return MarginComplianceStatus.REMATE_VIGENTE
        return MarginComplianceStatus.PROMOCION_VIGENTE

    @property
    def pending_verification(self) -> bool:
        return self.check in PENDING_CHECKS

    def to_json(self) -> dict[str, Any]:
        return {
            "check": self.check.value,
            "reason": self.reason,
            "exception": self.exception.to_json() if self.exception else None,
            "conflicts": [c.to_json() for c in self.conflicts],
        }


# Checks que mantienen una alerta de bajo margen marcada como "promoción por verificar".
PENDING_CHECKS = frozenset(
    {
        PromotionCheck.FUENTE_NO_INTEGRADA,
        PromotionCheck.EXCEPCION_NO_VERIFICADA,
        PromotionCheck.EXCEPCION_PENDIENTE_APROBACION,
        PromotionCheck.CONFLICTO_PROMOCIONES,
        PromotionCheck.PRECIO_NO_VERIFICABLE,
        PromotionCheck.PROMOCION_LEGACY_NO_VERIFICABLE,
    }
)

NOT_INTEGRATED = ExceptionResolution(PromotionCheck.FUENTE_NO_INTEGRADA, reason="promociones_no_integradas")


def _price_age_days(fetched_at: datetime | None, today: date) -> int | None:
    if fetched_at is None:
        return None
    return (today - fetched_at.date()).days


def resolve_exception(
    *,
    source_available: bool,
    candidates: Iterable[PromotionException],
    company_id: int,
    variant_id: int,
    price_list_id: int,
    bsale_gross_price: Decimal | None,
    today: date,
    bsale_price_fetched_at: datetime | None = None,
    max_price_age_days: int | None = None,
) -> ExceptionResolution:
    """``max_price_age_days`` activa la exigencia de precio RAW reciente (Márgenes V2 siempre la usa)."""
    if not source_available:
        return NOT_INTEGRATED
    same_target = [
        e
        for e in candidates
        if e.company_id == company_id
        and e.variant_id == variant_id
        and e.price_list_id == price_list_id
        and e.is_active_on(today)
    ]
    v2 = [e for e in same_target if e.origin is ExceptionOrigin.V2]
    legacy = [e for e in same_target if e.origin is ExceptionOrigin.LEGACY]
    approved = [e for e in v2 if e.approval is ApprovalStatus.APROBADA]
    pending = [e for e in v2 if e.approval is ApprovalStatus.PENDIENTE]

    if not approved:
        if pending:
            return ExceptionResolution(
                PromotionCheck.EXCEPCION_PENDIENTE_APROBACION, pending[0], tuple(pending[1:]), "campania_sin_aprobar"
            )
        if legacy:
            return ExceptionResolution(
                PromotionCheck.PROMOCION_LEGACY_NO_VERIFICABLE, legacy[0], tuple(legacy[1:]), "promocion_legacy"
            )
        return ExceptionResolution(PromotionCheck.SIN_EXCEPCION)

    chosen = max(approved, key=lambda e: (e.starts_on, e.ends_on, e.item_id or 0))
    conflicts = tuple(
        e
        for e in same_target
        if e is not chosen
        and e.approval in (ApprovalStatus.APROBADA, ApprovalStatus.PENDIENTE)
        and (e.origin is ExceptionOrigin.V2 or e.applicable_gross_price != chosen.applicable_gross_price)
    )
    if conflicts:
        return ExceptionResolution(PromotionCheck.CONFLICTO_PROMOCIONES, chosen, conflicts, "promociones_superpuestas")
    if bsale_gross_price is None:
        return ExceptionResolution(PromotionCheck.PRECIO_NO_VERIFICABLE, chosen, reason="sin_precio_bsale")
    if max_price_age_days is not None:
        age = _price_age_days(bsale_price_fetched_at, today)
        if age is None or age > max_price_age_days:
            return ExceptionResolution(PromotionCheck.PRECIO_NO_VERIFICABLE, chosen, reason="precio_bsale_desactualizado")
    if bsale_gross_price != chosen.applicable_gross_price:
        return ExceptionResolution(PromotionCheck.EXCEPCION_NO_VERIFICADA, chosen, reason="precio_distinto_al_vigente")
    return ExceptionResolution(PromotionCheck.EXCEPCION_VERIFICADA, chosen, reason="precio_coincide_bsale")
