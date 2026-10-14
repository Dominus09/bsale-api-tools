"""Dominio puro de Promociones V2: vigencia, transiciones, verificación contra Bsale y legacy.

Registrar una oferta o remate no cambia precios en Bsale ni prueba que el precio se aplicó: la
verificación compara el precio declarado con BSALE RAW. La vigencia se calcula contra la fecha de
consulta, por lo que una campaña deja de aplicar sola al vencer (sin cron).
"""

from __future__ import annotations

import calendar
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any, Mapping

from backend.services.analytics_v2.commercial_scope import PriceListScope
from backend.services.analytics_v2.contracts import (
    PROMO_PRICE_MAX_AGE_DAYS,
    CampaignApproval as A,
    business_date,
    CampaignKind,
    CampaignTimeline as T,
    PriceVerification as V,
    iso,
)
from backend.services.analytics_v2.promotion_exceptions import (
    ApprovalStatus,
    ExceptionKind,
    ExceptionOrigin,
    PromotionException,
)


class CampaignValidationError(ValueError):
    pass


class CampaignConflict(RuntimeError):
    pass


class CampaignNotFound(LookupError):
    pass


# ---------------------------------------------------------------- vigencia


def effective_end(ends_on: date, cancelled_at: datetime | date | None) -> date:
    """Último día real de la campaña: la cancelación la corta (el día de cancelación ya no aplica)."""
    if cancelled_at is None:
        return ends_on
    return min(ends_on, business_date(cancelled_at) - timedelta(days=1))


def campaign_timeline(approval: A, starts_on: date, ends_on: date, today: date) -> T:
    if approval is A.CANCELADA:
        return T.CANCELADA
    if today < starts_on:
        return T.PROGRAMADA
    if today > ends_on:
        return T.FINALIZADA
    return T.VIGENTE


# ---------------------------------------------------------------- transiciones

TRANSITIONS: dict[str, tuple[frozenset[A], A]] = {
    "submit": (frozenset({A.BORRADOR, A.RECHAZADA}), A.PENDIENTE_APROBACION),
    "approve": (frozenset({A.PENDIENTE_APROBACION}), A.APROBADA),
    "reject": (frozenset({A.PENDIENTE_APROBACION}), A.RECHAZADA),
    "cancel": (frozenset({A.BORRADOR, A.PENDIENTE_APROBACION, A.APROBADA, A.RECHAZADA}), A.CANCELADA),
}

TRANSITION_EVENTS = {
    "submit": "enviada_aprobacion",
    "approve": "aprobada",
    "reject": "rechazada",
    "cancel": "cancelada",
}

REASON_REQUIRED = frozenset({"reject", "cancel"})


def next_approval(action: str, current: A, timeline: T) -> A:
    if action not in TRANSITIONS:
        raise CampaignValidationError(f"acción inválida: {action}")
    allowed, target = TRANSITIONS[action]
    if current not in allowed:
        raise CampaignConflict(f"No se puede '{action}' una campaña en estado {current.value}")
    if timeline is T.FINALIZADA:
        raise CampaignConflict("La campaña ya finalizó; solo queda como historial")
    return target


def assert_editable(approval: A, timeline: T) -> None:
    if approval is A.CANCELADA:
        raise CampaignConflict("Campaña cancelada: no se puede modificar")
    if timeline is T.FINALIZADA:
        raise CampaignConflict("La campaña ya finalizó; solo queda como historial")


def approval_after_change(approval: A) -> A:
    """Cambiar precios, fechas o productos de una campaña aprobada exige aprobarla de nuevo."""
    if approval is A.APROBADA:
        return A.PENDIENTE_APROBACION
    if approval is A.RECHAZADA:
        return A.BORRADOR
    return approval


# ---------------------------------------------------------------- validación de entrada

MIN_PROMO_PRICE = Decimal("10")
MAX_PROMO_PRICE = Decimal("100000000")
MAX_CAMPAIGN_DAYS = 366


def validate_dates(starts_on: date, ends_on: date) -> None:
    if ends_on < starts_on:
        raise CampaignValidationError("La fecha de término no puede ser anterior al inicio")
    if (ends_on - starts_on).days + 1 > MAX_CAMPAIGN_DAYS:
        raise CampaignValidationError("Una campaña no puede durar más de un año")


def validate_promo_price(value: Decimal) -> Decimal:
    if value is None or not value.is_finite():
        raise CampaignValidationError("Precio promocional inválido")
    if value < MIN_PROMO_PRICE or value > MAX_PROMO_PRICE:
        raise CampaignValidationError("Precio promocional fuera de rango")
    if value != value.quantize(Decimal("0.01")):
        raise CampaignValidationError("El precio promocional admite como máximo 2 decimales")
    return value.quantize(Decimal("0.01"))


def clean_text(value: str | None, *, max_len: int, required: bool = False, label: str = "texto") -> str | None:
    text = (value or "").strip()
    if not text:
        if required:
            raise CampaignValidationError(f"{label} es obligatorio")
        return None
    if len(text) > max_len:
        raise CampaignValidationError(f"{label} supera {max_len} caracteres")
    return text


_MONTH_RE = re.compile(r"^(\d{4})-(\d{2})$")


def month_bounds(month: str | None, today: date) -> tuple[date, date, str]:
    if not month:
        y, m = today.year, today.month
    else:
        match = _MONTH_RE.match(month.strip())
        if not match:
            raise CampaignValidationError("month debe tener formato AAAA-MM")
        y, m = int(match.group(1)), int(match.group(2))
        if not (2020 <= y <= 2100 and 1 <= m <= 12):
            raise CampaignValidationError("month fuera de rango")
    first = date(y, m, 1)
    last = date(y, m, calendar.monthrange(y, m)[1])
    return first, last, f"{y:04d}-{m:02d}"


def overlaps(starts_on: date, ends_on: date, first: date, last: date) -> bool:
    return starts_on <= last and ends_on >= first


# ---------------------------------------------------------------- verificación contra Bsale


@dataclass(frozen=True)
class BsalePrice:
    gross: Decimal | None
    missing: bool
    fetched_at: datetime | None


def price_verification(
    *,
    timeline: T,
    promo_gross_price: Decimal,
    bsale: BsalePrice | None,
    today: date,
    max_age_days: int = PROMO_PRICE_MAX_AGE_DAYS,
) -> V:
    """Solo informa si el precio declarado está en Bsale; nunca lo da por aplicado sin evidencia."""
    gross = bsale.gross if bsale and not bsale.missing else None
    if timeline is T.CANCELADA:
        return V.NO_APLICA
    if timeline is T.FINALIZADA:
        if gross is not None and gross == promo_gross_price:
            return V.VENCIDA_SIN_REVERTIR
        return V.NO_APLICA
    if gross is None:
        return V.SIN_PRECIO_BSALE
    if bsale.fetched_at is None or (today - bsale.fetched_at.date()).days > max_age_days:
        return V.PRECIO_BSALE_DESACTUALIZADO
    return V.COINCIDE_BSALE if gross == promo_gross_price else V.NO_COINCIDE_BSALE


# ---------------------------------------------------------------- contrato con Márgenes V2

_APPROVAL_TO_EXCEPTION = {
    A.APROBADA: ApprovalStatus.APROBADA,
    A.PENDIENTE_APROBACION: ApprovalStatus.PENDIENTE,
}


def campaign_item_exception(row: Mapping[str, Any]) -> PromotionException | None:
    """Ítem activo de una campaña aprobada o pendiente → candidato de excepción; el resto no participa."""
    approval = A(row["approval_status"])
    if approval not in _APPROVAL_TO_EXCEPTION or row.get("item_status", "activo") != "activo":
        return None
    kind = ExceptionKind.REMATE if row["kind"] == CampaignKind.REMATE.value else ExceptionKind.PROMOCION
    return PromotionException(
        company_id=int(row["company_id"]),
        variant_id=int(row["variant_id"]),
        price_list_id=int(row["price_list_id"]),
        kind=kind,
        starts_on=row["starts_on"],
        ends_on=row["ends_on"],
        applicable_gross_price=Decimal(row["promo_gross_price"]),
        approval=_APPROVAL_TO_EXCEPTION[approval],
        reference=f"campania:{row['campaign_id']}/item:{row['item_id']}",
        origin=ExceptionOrigin.V2,
        campaign_id=int(row["campaign_id"]),
        item_id=int(row["item_id"]),
        campaign_name=row.get("campaign_name"),
        approved_by=row.get("approved_by"),
        approved_at=row.get("approved_at"),
    )


# ---------------------------------------------------------------- compatibilidad legacy

LEGACY_MAPPEABLE = "mapeable"


def _ids(value: Any) -> list[int]:
    if not value:
        return []
    return [int(v) for v in value]


def legacy_mapping_status(row: Mapping[str, Any], scope: PriceListScope) -> tuple[str, int | None, int | None]:
    """barcode → variante RAW y nombre de lista → lista RAW; si no es único o está fuera de alcance, no se mapea."""
    variants = _ids(row.get("variant_ids"))
    lists = _ids(row.get("price_list_ids"))
    if not variants:
        return "variante_no_encontrada", None, None
    if len(variants) > 1:
        return "variante_ambigua", None, None
    if not lists:
        return "lista_no_encontrada", variants[0], None
    if len(lists) > 1:
        return "lista_ambigua", variants[0], None
    if lists[0] not in scope.evaluated_ids:
        return "lista_fuera_de_alcance", variants[0], lists[0]
    return LEGACY_MAPPEABLE, variants[0], lists[0]


def legacy_exception(row: Mapping[str, Any], company_id: int, variant_id: int, price_list_id: int) -> PromotionException | None:
    """Promoción legacy activa y mapeable: se informa en márgenes como no verificable, nunca la concede."""
    sale = row.get("sale_price")
    if not row.get("activa") or sale is None:
        return None
    kind = ExceptionKind.REMATE if (row.get("tipo") or "").lower() == "remate" else ExceptionKind.PROMOCION
    return PromotionException(
        company_id=company_id,
        variant_id=variant_id,
        price_list_id=price_list_id,
        kind=kind,
        starts_on=row["fecha_inicio"],
        ends_on=row["fecha_fin"],
        applicable_gross_price=Decimal(sale),
        approval=ApprovalStatus.PENDIENTE,
        reference=f"legacy_promotion:{row['promotion_id']}/snapshot:{row['snapshot_id']}",
        origin=ExceptionOrigin.LEGACY,
        campaign_id=None,
        item_id=None,
        campaign_name=f"Promoción legacy #{row['promotion_id']}",
    )


# ---------------------------------------------------------------- medición futura de ventas

MEASUREMENT_STATUS = "no_implementado"


def measurement_windows(starts_on: date, ends_on: date, cancelled_at: datetime | date | None, today: date) -> dict[str, Any]:
    """Ventanas antes/durante/después de igual duración para el futuro análisis de ventas (sin cifras)."""
    real_end = effective_end(ends_on, cancelled_at)
    ran = real_end >= starts_on
    planned_days = (ends_on - starts_on).days + 1
    real_days = (real_end - starts_on).days + 1 if ran else 0
    length = max(real_days, 1)
    during_end = min(real_end, today) if ran else None
    return {
        "status": MEASUREMENT_STATUS,
        "planned_days": planned_days,
        "real_days": real_days,
        "real_end_on": iso(real_end) if ran else None,
        "before": {"from": iso(starts_on - timedelta(days=length)), "to": iso(starts_on - timedelta(days=1))},
        "during": {"from": iso(starts_on), "to": iso(during_end)} if ran and today >= starts_on else None,
        "after": {"from": iso(real_end + timedelta(days=1)), "to": iso(real_end + timedelta(days=length))}
        if ran and today > real_end
        else None,
        "metrics": None,
    }
