"""Estado operativo canónico de una OC: billing vs despacho (sin mezclar)."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any

# --- billing_status ---
BILLING_PENDING = "pending"
BILLING_INVOICED = "invoiced"
BILLING_INVOICED_PARTIAL_CN = "invoiced_with_partial_credit_note"
BILLING_INVOICED_FULL_CN = "invoiced_with_full_credit_note"
BILLING_INVOICED_CN_UNSPECIFIED = "invoiced_with_credit_note"
BILLING_CANCELLED = "cancelled"
BILLING_PROBABLE = "probable"

# --- dispatch_status ---
DISPATCH_PENDING = "pending"
DISPATCH_PLANNED = "planned"
DISPATCH_COMPLETED = "completed"
DISPATCH_CLOSED = "closed"
DISPATCH_EXCLUDED = "excluded"

# --- fulfillment_status (contexto de una planificación) ---
FULFILL_PENDING = "pending"
FULFILL_BY_PICKING = "fulfilled_by_picking"
FULFILL_BY_INVOICE = "fulfilled_by_invoice"
FULFILL_EXCLUDED_PREEXISTING = "excluded_preexisting_invoice"
FULFILL_EXCLUDED_CANCELLED = "excluded_cancelled_order"
FULFILL_UNRESOLVED = "unresolved"

EXCLUDED_REASON_ALREADY_INVOICED = "already_invoiced_before_planning"
EXCLUDED_REASON_CANCELLED_ORDER = "cancelled_order"

# Precedencia canónica: CANCELLED > CONFIRMED INVOICE > PROBABLE > PENDING
FULFILL_EXCLUDED_STATUSES = frozenset(
    {FULFILL_EXCLUDED_PREEXISTING, FULFILL_EXCLUDED_CANCELLED}
)

INVOICE_DOC_TYPES = frozenset({1, 6})
CREDIT_NOTE_DOC_TYPE = 9
# Tipos intermedios observados / esperados en cadenas Bsale (picking, guías, etc.).
INTERMEDIATE_DOC_TYPES = frozenset({5, 8, 10, 12, 14, 15, 16, 17, 18, 19, 20, 21, 22})

BILLING_LABELS_ES = {
    BILLING_PENDING: "Pendiente por facturar",
    BILLING_INVOICED: "Facturada",
    BILLING_INVOICED_PARTIAL_CN: "Facturada con NC parcial",
    BILLING_INVOICED_FULL_CN: "Facturada con NC total",
    BILLING_INVOICED_CN_UNSPECIFIED: "Facturada con nota de crédito",
    BILLING_CANCELLED: "Anulada",
    BILLING_PROBABLE: "Probable",
}

FULFILL_LABELS_ES = {
    FULFILL_PENDING: "Pendiente",
    FULFILL_BY_PICKING: "Completada",
    FULFILL_BY_INVOICE: "Completada",
    FULFILL_EXCLUDED_PREEXISTING: "Excluida: ya estaba facturada",
    FULFILL_EXCLUDED_CANCELLED: "Excluida: orden anulada",
    FULFILL_UNRESOLVED: "Revisión necesaria",
}


@dataclass(frozen=True)
class LinkedDocument:
    document_id: int
    number: int | None
    document_type_id: int
    total_amount: Decimal | None = None
    issued_at: datetime | None = None
    path: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, Any]:
        return {
            "document_id": self.document_id,
            "number": self.number,
            "document_type_id": self.document_type_id,
            "total_amount": float(self.total_amount)
            if self.total_amount is not None
            else None,
            "issued_at": self.issued_at.isoformat() if self.issued_at else None,
            "path": list(self.path),
        }


@dataclass
class OcDocumentChain:
    oc_document_id: int
    oc_number: int | None = None
    oc_state: int = 0
    confirmed_invoices: list[LinkedDocument] = field(default_factory=list)
    probable_invoices: list[LinkedDocument] = field(default_factory=list)
    credit_notes: list[LinkedDocument] = field(default_factory=list)
    pickings: list[LinkedDocument] = field(default_factory=list)
    intermediates: list[LinkedDocument] = field(default_factory=list)
    relation_paths: list[tuple[str, ...]] = field(default_factory=list)
    evidence_source: str = "none"
    probable_score: float | None = None

    @property
    def confirmed_invoice_ids(self) -> list[int]:
        return [d.document_id for d in self.confirmed_invoices]

    @property
    def confirmed_invoice_numbers(self) -> list[int | None]:
        return [d.number for d in self.confirmed_invoices]

    @property
    def credit_note_ids(self) -> list[int]:
        return [d.document_id for d in self.credit_notes]

    @property
    def credit_note_numbers(self) -> list[int | None]:
        return [d.number for d in self.credit_notes]

    @property
    def picking_ids(self) -> list[int]:
        return [d.document_id for d in self.pickings]


@dataclass
class OcOperationalStatus:
    oc_document_id: int
    billing_status: str
    dispatch_status: str
    planning_eligible: bool
    dispatch_closed: bool
    fulfillment_status: str | None = None
    excluded_reason: str | None = None
    completion_source: str | None = None
    evidence_source: str = "none"
    billing_label_es: str = ""
    fulfillment_label_es: str | None = None
    confirmed_invoice: dict[str, Any] | None = None
    credit_notes: list[dict[str, Any]] = field(default_factory=list)
    relation_path: list[str] = field(default_factory=list)
    chain: OcDocumentChain | None = None

    def as_dict(self) -> dict[str, Any]:
        d = asdict(self)
        if self.chain is not None:
            d["chain"] = {
                "oc_document_id": self.chain.oc_document_id,
                "oc_number": self.chain.oc_number,
                "confirmed_invoice_ids": self.chain.confirmed_invoice_ids,
                "confirmed_invoice_numbers": self.chain.confirmed_invoice_numbers,
                "credit_note_ids": self.chain.credit_note_ids,
                "credit_note_numbers": self.chain.credit_note_numbers,
                "picking_ids": self.chain.picking_ids,
                "relation_paths": [list(p) for p in self.chain.relation_paths],
                "evidence_source": self.chain.evidence_source,
                "probable_score": self.chain.probable_score,
            }
        return d


def _dec(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def classify_credit_note_coverage(
    invoice_total: Decimal | None,
    credit_note_totals: list[Decimal | None],
    *,
    credit_note_states: list[int] | None = None,
) -> str | None:
    """
    Devuelve parcial / total / neutro, o None si no hay NC vigentes.

    - NC anulada (state != 0) se ignora.
    - Sin montos fiables → ``invoiced_with_credit_note`` (no inventar parcial/total).
    """
    if not credit_note_totals:
        return None
    states = credit_note_states or [0] * len(credit_note_totals)
    active_amounts: list[Decimal] = []
    any_active = False
    for i, amt in enumerate(credit_note_totals):
        st = int(states[i]) if i < len(states) else 0
        if st != 0:
            continue
        any_active = True
        if amt is not None:
            active_amounts.append(amt)
    if not any_active:
        return None
    if not active_amounts or invoice_total is None or invoice_total <= 0:
        return BILLING_INVOICED_CN_UNSPECIFIED
    cn_sum = sum(active_amounts, Decimal("0"))
    if cn_sum >= invoice_total * Decimal("0.99"):
        return BILLING_INVOICED_FULL_CN
    return BILLING_INVOICED_PARTIAL_CN


def derive_billing_status(chain: OcDocumentChain) -> str:
    """
    Precedencia canónica:
    CANCELLED > CONFIRMED INVOICE > PROBABLE > PENDING.

    Si ``documents.state != 0`` (Bsale anulación, p.ej. 8888), cancelled gana
    incluso si existen related 1/6 o probable — no inventar semántica híbrida.
    """
    if int(chain.oc_state or 0) != 0:
        return BILLING_CANCELLED
    if chain.confirmed_invoices:
        inv = chain.confirmed_invoices[0]
        # states no viajan en LinkedDocument; se asume state=0 (solo se cargan NC activas).
        cn_kind = classify_credit_note_coverage(
            inv.total_amount,
            [c.total_amount for c in chain.credit_notes],
        )
        if cn_kind:
            return cn_kind
        return BILLING_INVOICED
    if chain.probable_invoices and (chain.probable_score or 0) >= 60:
        return BILLING_PROBABLE
    return BILLING_PENDING


def derive_dispatch_flags(billing_status: str) -> tuple[bool, bool, str]:
    """
    Returns: planning_eligible, dispatch_closed, base_dispatch_status.

    Regla crítica: factura confirmada (con o sin NC) cierra despacho y
    NO vuelve a pending por nota de crédito.
    OC anulada: planning_eligible=false, dispatch_closed=true (no requiere factura).
    """
    if billing_status == BILLING_CANCELLED:
        return False, True, DISPATCH_EXCLUDED
    if billing_status in (
        BILLING_INVOICED,
        BILLING_INVOICED_PARTIAL_CN,
        BILLING_INVOICED_FULL_CN,
        BILLING_INVOICED_CN_UNSPECIFIED,
    ):
        return False, True, DISPATCH_CLOSED
    if billing_status == BILLING_PROBABLE:
        # Probable no cierra ni admite como facturada confirmada.
        return True, False, DISPATCH_PENDING
    return True, False, DISPATCH_PENDING


def earliest_confirmed_invoice_issued_at(chain: OcDocumentChain) -> datetime | None:
    times = [d.issued_at for d in chain.confirmed_invoices if d.issued_at]
    return min(times) if times else None


def is_fulfillment_excluded(fulfillment_status: str | None) -> bool:
    return fulfillment_status in FULFILL_EXCLUDED_STATUSES


def resolve_plan_fulfillment(
    chain: OcDocumentChain,
    *,
    planned_at: datetime | None,
    has_picking: bool = False,
    force_excluded: bool = False,
) -> tuple[str, str | None, str | None, str]:
    """
    fulfillment_status, excluded_reason, completion_source, dispatch_status
    en contexto de una planificación.
    """
    billing = derive_billing_status(chain)
    eligible, closed, base_dispatch = derive_dispatch_flags(billing)

    # Anulada: fuera del denominador; no cuenta como pending ni como fulfilled_by_invoice.
    if billing == BILLING_CANCELLED:
        return (
            FULFILL_EXCLUDED_CANCELLED,
            EXCLUDED_REASON_CANCELLED_ORDER,
            None,
            DISPATCH_EXCLUDED,
        )

    if force_excluded or (
        closed
        and planned_at is not None
        and _invoice_predates_planning(chain, planned_at)
    ):
        return (
            FULFILL_EXCLUDED_PREEXISTING,
            EXCLUDED_REASON_ALREADY_INVOICED,
            None,
            DISPATCH_EXCLUDED,
        )

    if has_picking and closed:
        return FULFILL_BY_PICKING, None, "picking", DISPATCH_COMPLETED

    if closed and planned_at is not None:
        issued = earliest_confirmed_invoice_issued_at(chain)
        if issued is not None and issued >= planned_at:
            return FULFILL_BY_INVOICE, None, "invoice", DISPATCH_COMPLETED
        if issued is None and chain.confirmed_invoices:
            # Cadena confirmada sin timestamp usable: tratar como cumplimiento
            # documental (no exigir picking extra).
            return FULFILL_BY_INVOICE, None, "invoice", DISPATCH_COMPLETED

    if closed:
        # Facturada fuera de contexto temporal claro → cerrada, no pending.
        return FULFILL_BY_INVOICE, None, "invoice", DISPATCH_CLOSED

    if billing == BILLING_PROBABLE:
        return FULFILL_UNRESOLVED, None, None, DISPATCH_PLANNED if planned_at else DISPATCH_PENDING

    if planned_at is not None:
        return FULFILL_PENDING, None, None, DISPATCH_PLANNED
    return FULFILL_PENDING, None, None, base_dispatch


def _invoice_predates_planning(chain: OcDocumentChain, planned_at: datetime) -> bool:
    issued = earliest_confirmed_invoice_issued_at(chain)
    if issued is None:
        return False
    return issued < planned_at


def build_operational_status(
    chain: OcDocumentChain,
    *,
    planned_at: datetime | None = None,
    has_picking: bool = False,
    in_plan: bool = False,
) -> OcOperationalStatus:
    billing = derive_billing_status(chain)
    eligible, closed, base_dispatch = derive_dispatch_flags(billing)

    fulfillment = None
    excluded_reason = None
    completion_source = None
    dispatch_status = base_dispatch

    if in_plan or planned_at is not None:
        fulfillment, excluded_reason, completion_source, dispatch_status = (
            resolve_plan_fulfillment(
                chain,
                planned_at=planned_at,
                has_picking=has_picking,
            )
        )
        if is_fulfillment_excluded(fulfillment):
            eligible = False
            closed = True

    confirmed = (
        chain.confirmed_invoices[0].as_dict()
        if chain.confirmed_invoices and billing != BILLING_CANCELLED
        else None
    )
    path: list[str] = []
    if chain.relation_paths:
        path = list(chain.relation_paths[0])

    return OcOperationalStatus(
        oc_document_id=chain.oc_document_id,
        billing_status=billing,
        dispatch_status=dispatch_status,
        planning_eligible=eligible,
        dispatch_closed=closed,
        fulfillment_status=fulfillment,
        excluded_reason=excluded_reason,
        completion_source=completion_source,
        evidence_source=chain.evidence_source,
        billing_label_es=BILLING_LABELS_ES.get(billing, billing),
        fulfillment_label_es=(
            FULFILL_LABELS_ES.get(fulfillment) if fulfillment else None
        ),
        confirmed_invoice=confirmed,
        credit_notes=[c.as_dict() for c in chain.credit_notes],
        relation_path=path,
        chain=chain,
    )


def plan_progress_counts(statuses: list[OcOperationalStatus]) -> dict[str, Any]:
    """completion_percent = completed / eligible_planned (excluded fuera del denom)."""
    eligible = [
        s
        for s in statuses
        if not is_fulfillment_excluded(s.fulfillment_status)
    ]
    excluded = [
        s
        for s in statuses
        if is_fulfillment_excluded(s.fulfillment_status)
    ]
    completed = [
        s
        for s in eligible
        if s.fulfillment_status
        in (FULFILL_BY_INVOICE, FULFILL_BY_PICKING)
    ]
    pending = [
        s
        for s in eligible
        if s.fulfillment_status == FULFILL_PENDING
    ]
    unresolved = [
        s
        for s in eligible
        if s.fulfillment_status == FULFILL_UNRESOLVED
    ]
    denom = len(eligible)
    pct = (100.0 * len(completed) / denom) if denom else 0.0
    return {
        "eligible_planned_orders": denom,
        "completed_orders": len(completed),
        "pending_orders": len(pending),
        "unresolved_orders": len(unresolved),
        "excluded_orders": len(excluded),
        "completion_percent": round(pct, 2),
        "margin_blocked": len(pending) > 0 or len(unresolved) > 0,
        "fully_complete": denom > 0 and len(pending) == 0 and len(unresolved) == 0,
    }


def blocks_planning_admission(status: OcOperationalStatus) -> bool:
    return (not status.planning_eligible) or status.dispatch_closed


def is_predespacho_pending_row(
    *,
    billing_status: str | None,
    planning_eligible: bool | None = None,
    dispatch_closed: bool | None = None,
) -> bool:
    """
    Una fila solo puede figurar como «Pendiente por facturar» en Pre-despacho si
    es elegible y no está cerrada. Facturada (+ NC) y anulada quedan fuera.
    """
    billing = billing_status or BILLING_PENDING
    if billing == BILLING_CANCELLED:
        return False
    if billing in (
        BILLING_INVOICED,
        BILLING_INVOICED_PARTIAL_CN,
        BILLING_INVOICED_FULL_CN,
        BILLING_INVOICED_CN_UNSPECIFIED,
    ):
        return False
    if dispatch_closed is True:
        return False
    if planning_eligible is False:
        return False
    return billing in (BILLING_PENDING, BILLING_PROBABLE)


ADMISSION_BLOCK_MESSAGE = (
    "Esta orden ya fue facturada y no corresponde a una nueva planificación."
)

ADMISSION_BLOCK_MESSAGE_CANCELLED = (
    "Esta orden está anulada y no corresponde a una nueva planificación."
)


def admission_block_message(status: OcOperationalStatus) -> str:
    if status.billing_status == BILLING_CANCELLED:
        return ADMISSION_BLOCK_MESSAGE_CANCELLED
    return ADMISSION_BLOCK_MESSAGE
