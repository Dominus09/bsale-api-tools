"""Modelos internos del adaptador documental (Etapa 2A)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import Enum


class AnalyticsDocumentKind(str, Enum):
    SALE = "sale"
    CREDIT_NOTE = "credit_note"
    UNSUPPORTED = "unsupported"


class LineNetMethod(str, Enum):
    EXPLICIT_LINE_NET = "explicit_line_net"
    ALLOCATED_FROM_HEADER = "allocated_from_header"
    UNAVAILABLE = "unavailable"


class ReconciliationStatus(str, Enum):
    MATCHED = "matched"
    ROUNDING_DIFFERENCE = "rounding_difference"
    MISMATCH = "mismatch"
    MISSING_LINES = "missing_lines"


# Tolerancia monetaria explícita (CLP) para redondeo encabezado vs líneas.
HEADER_LINE_TOLERANCE = Decimal("1.0000")


@dataclass(frozen=True, slots=True)
class AnalyticsDocumentHeader:
    document_id: int
    source_document_id: int | None
    document_type_id: int
    number: int | None
    company_id: int
    office_id: int
    emission_date: date | None
    generation_date: date | None
    commercial_date: date
    client_id: int | None
    # client_name: no existe en distribuidora.documents (001_schema); opcional futuro.
    client_name: str | None
    seller_id: int | None
    raw_seller_name: str | None
    net_amount: Decimal | None
    tax_amount: Decimal | None
    total_amount: Decimal | None
    state: int
    commercial_state: int | None
    kind: AnalyticsDocumentKind
    is_active: bool


@dataclass(frozen=True, slots=True)
class AnalyticsDocumentLine:
    detail_id: int
    document_id: int
    variant_id: int | None
    # barcode: no confirmado en document_details; se usa variant_code si existe.
    barcode: str | None
    variant_code: str | None
    quantity: Decimal | None
    line_net_amount: Decimal | None
    line_tax_amount: Decimal | None
    line_total_amount: Decimal | None
    unit_price: Decimal | None
    discount_amount: Decimal | None
    allocated_net_amount: Decimal | None
    net_method: LineNetMethod


@dataclass(frozen=True, slots=True)
class DocumentReconciliationResult:
    document_id: int
    header_total_amount: Decimal | None
    lines_total_amount: Decimal | None
    difference_total: Decimal | None
    header_net_amount: Decimal | None
    lines_net_amount: Decimal | None
    difference_net: Decimal | None
    line_count: int
    quantity_total: Decimal | None
    reconciliation_status: ReconciliationStatus
    tolerance: Decimal = HEADER_LINE_TOLERANCE


def resolve_commercial_date(
    emission_date: date | datetime | None,
    generation_date: date | datetime | None,
) -> date | None:
    """Fecha comercial: emission_date, si falta generation_date."""

    def _as_date(value: date | datetime | None) -> date | None:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.date()
        return value

    return _as_date(emission_date) or _as_date(generation_date)


def classify_document_kind(document_type_id: int) -> AnalyticsDocumentKind:
    from backend.services.analytics.document_source import (
        CREDIT_NOTE_DOCUMENT_TYPES,
        SALE_DOCUMENT_TYPES,
    )

    if document_type_id in SALE_DOCUMENT_TYPES:
        return AnalyticsDocumentKind.SALE
    if document_type_id in CREDIT_NOTE_DOCUMENT_TYPES:
        return AnalyticsDocumentKind.CREDIT_NOTE
    return AnalyticsDocumentKind.UNSUPPORTED
