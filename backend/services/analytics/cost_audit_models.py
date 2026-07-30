"""Modelos y tolerancias del auditor de calidad de costos (read-only).

No modifica sync ni datos. Las fórmulas aquí son de auditoría independiente:
no reutilizan split_erp_cost del sync (objeto auditado).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any

from backend.services.analytics.money import ZERO, optional_decimal
from backend.services.analytics.validate_distribuidora_source import (
    AnalyticsValidationError,
)

MAX_DAYS = 365
MAX_LIMIT = 50000  # tope de filas clasificadas en modo detalle
MAX_POPULATION_SCAN = 200000  # tope de seguridad para --summary-only
MAX_SAMPLE_LIMIT = 100
MAX_TIMEOUT_SECONDS = 30
MAX_PAGE_SIZE = 2000
MAX_PAGES = 100
DEFAULT_DAYS = 90
DEFAULT_LIMIT = 5000  # solo detalle; summary-only lo ignora para agregados
DEFAULT_SAMPLE_LIMIT = 20
DEFAULT_TIMEOUT_SECONDS = 20
DEFAULT_LOCK_TIMEOUT = "3s"
DEFAULT_PAGE_SIZE = 500
DEFAULT_MAX_PAGES = 20


class CostAuditFlag(str, Enum):
    # A) Consistencia de componentes almacenados
    STORED_COMPONENTS_MATCH = "stored_components_match"
    STORED_COMPONENTS_ROUNDING = "stored_components_rounding"
    STORED_COMPONENTS_MISMATCH = "stored_components_mismatch"
    # B) Consistencia vs perfil tributario esperado
    EXPECTED_TAX_MATCH = "expected_tax_match"
    EXPECTED_TAX_ROUNDING = "expected_tax_rounding"
    EXPECTED_TAX_MISMATCH = "expected_tax_mismatch"
    EXPECTED_TAX_UNAVAILABLE = "expected_tax_unavailable"
    # Señales técnicas adicionales
    PROBABLE_MISSING_TAXES = "probable_missing_taxes"
    PROBABLE_IVA_DUPLICATED = "probable_iva_duplicated"
    PROBABLE_SPECIFIC_TAX_DUPLICATED = "probable_specific_tax_duplicated"
    PROBABLE_TAX_FACTOR_DUPLICATED = "probable_tax_factor_duplicated"
    QUANTITY_MISMATCH = "quantity_mismatch"
    UNIT_TOTAL_MISMATCH = "unit_total_mismatch"
    DUPLICATE_RECEPTION = "duplicate_reception"
    DUPLICATE_VARIANT_LINK = "duplicate_variant_link"
    VARIANT_BARCODE_MISMATCH = "variant_barcode_mismatch"
    MISSING_TAX_CONTEXT = "missing_tax_context"
    TAX_IDS_NOT_CONSUMED = "tax_ids_not_consumed"
    MISSING_NET_COST = "missing_net_cost"
    MISSING_GROSS_COST = "missing_gross_cost"
    ZERO_COST = "zero_cost"
    NEGATIVE_COST = "negative_cost"
    SUSPICIOUS_OUTLIER = "suspicious_outlier"
    STALE_SNAPSHOT = "stale_snapshot"
    SOURCE_CONFLICT = "source_conflict"


class EffectiveQualityStatus(str, Enum):
    """Estado único prioritario para análisis / UI futura."""

    VALID_GROSS = "valid_gross"
    MISSING_TAXES_IN_GROSS = "missing_taxes_in_gross"
    DUPLICATED_TAXES_IN_GROSS = "duplicated_taxes_in_gross"
    INCOMPLETE_TAX_CONTEXT = "incomplete_tax_context"
    GROSS_COMPONENT_MISMATCH = "gross_component_mismatch"
    SUSPICIOUS_OUTLIER = "suspicious_outlier"
    MISSING_COST = "missing_cost"


# Prioridad: menor índice = más grave / gana
EFFECTIVE_STATUS_PRIORITY: tuple[str, ...] = (
    EffectiveQualityStatus.MISSING_COST.value,
    EffectiveQualityStatus.GROSS_COMPONENT_MISMATCH.value,
    EffectiveQualityStatus.DUPLICATED_TAXES_IN_GROSS.value,
    EffectiveQualityStatus.MISSING_TAXES_IN_GROSS.value,
    EffectiveQualityStatus.INCOMPLETE_TAX_CONTEXT.value,
    EffectiveQualityStatus.SUSPICIOUS_OUTLIER.value,
    EffectiveQualityStatus.VALID_GROSS.value,
)


QUALITY_COUNTER_KEYS: tuple[str, ...] = (
    CostAuditFlag.STORED_COMPONENTS_MATCH.value,
    CostAuditFlag.STORED_COMPONENTS_ROUNDING.value,
    CostAuditFlag.STORED_COMPONENTS_MISMATCH.value,
    CostAuditFlag.EXPECTED_TAX_MATCH.value,
    CostAuditFlag.EXPECTED_TAX_ROUNDING.value,
    CostAuditFlag.EXPECTED_TAX_MISMATCH.value,
    CostAuditFlag.EXPECTED_TAX_UNAVAILABLE.value,
    CostAuditFlag.PROBABLE_MISSING_TAXES.value,
    CostAuditFlag.PROBABLE_IVA_DUPLICATED.value,
    CostAuditFlag.PROBABLE_SPECIFIC_TAX_DUPLICATED.value,
    CostAuditFlag.UNIT_TOTAL_MISMATCH.value,
    CostAuditFlag.DUPLICATE_RECEPTION.value,
    CostAuditFlag.VARIANT_BARCODE_MISMATCH.value,
    CostAuditFlag.MISSING_TAX_CONTEXT.value,
    CostAuditFlag.TAX_IDS_NOT_CONSUMED.value,
    CostAuditFlag.ZERO_COST.value,
    CostAuditFlag.NEGATIVE_COST.value,
    CostAuditFlag.SUSPICIOUS_OUTLIER.value,
    CostAuditFlag.STALE_SNAPSHOT.value,
)

EFFECTIVE_COUNTER_KEYS: tuple[str, ...] = EFFECTIVE_STATUS_PRIORITY

# Alias de compatibilidad: tests antiguos no deben usar exact_match
EXACT_MATCH_REMOVED = True



@dataclass(frozen=True, slots=True)
class CostAuditTolerances:
    """Criterios centralizados y configurables."""

    money_rounding: Decimal = Decimal("0.01")
    money_exact: Decimal = Decimal("0.0001")
    pct_soft: Decimal = Decimal("0.5")  # % sobre |cost_net|
    stale_snapshot_days: int = 90
    outlier_factor: Decimal = Decimal("3")
    min_candidates_for_outlier: int = 3
    duplicate_iva_tolerance: Decimal = Decimal("0.02")  # fracción relativa
    unit_total_rel_tolerance: Decimal = Decimal("0.02")


DEFAULT_TOLERANCES = CostAuditTolerances()


def normalize_barcode(raw: str | None) -> str | None:
    """Normaliza barcode de filtro: trim; no interpreta variant_code."""
    if raw is None:
        return None
    cleaned = str(raw).strip()
    return cleaned or None


@dataclass(frozen=True, slots=True)
class BarcodeResolution:
    """Resultado de barcode → variant_id(s), alineado con /costos."""

    requested_barcode: str | None
    normalized_barcode: str | None
    catalog_matches: int
    resolved_variant_ids: tuple[int, ...]
    resolution_source: str | None
    duplicate_mapping: bool
    history_rows_found: int
    barcode_not_found: bool = False
    no_reception_history: bool = False
    warnings: tuple[str, ...] = ()
    match_details: tuple[dict[str, Any], ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "requested_barcode": self.requested_barcode,
            "normalized_barcode": self.normalized_barcode,
            "catalog_matches": self.catalog_matches,
            "resolved_variant_ids": list(self.resolved_variant_ids),
            "resolution_source": self.resolution_source,
            "duplicate_mapping": self.duplicate_mapping,
            "history_rows_found": self.history_rows_found,
            "barcode_not_found": self.barcode_not_found,
            "no_reception_history": self.no_reception_history,
            "warnings": list(self.warnings),
            "match_details": list(self.match_details),
        }


@dataclass(frozen=True, slots=True)
class CostAuditArgs:
    company_id: int
    office_id: int | None = None
    days: int = DEFAULT_DAYS
    limit: int = DEFAULT_LIMIT
    sample_limit: int = DEFAULT_SAMPLE_LIMIT
    statement_timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS
    variant_id: int | None = None
    barcode: str | None = None
    source_document_id: int | None = None
    lock_timeout: str = DEFAULT_LOCK_TIMEOUT
    tolerances: CostAuditTolerances = DEFAULT_TOLERANCES
    page_size: int = DEFAULT_PAGE_SIZE
    max_pages: int = DEFAULT_MAX_PAGES
    summary_only: bool = False


def clamp_cost_audit_args(
    *,
    company_id: int,
    office_id: int | None = None,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    sample_limit: int = DEFAULT_SAMPLE_LIMIT,
    statement_timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    variant_id: int | None = None,
    barcode: str | None = None,
    source_document_id: int | None = None,
    tolerances: CostAuditTolerances | None = None,
    page_size: int = DEFAULT_PAGE_SIZE,
    max_pages: int = DEFAULT_MAX_PAGES,
    summary_only: bool = False,
) -> CostAuditArgs:
    if int(company_id) <= 0:
        raise AnalyticsValidationError(
            "company_id is required and must be > 0",
            error_type="invalid_args",
        )
    office: int | None
    if office_id is None:
        office = None
    else:
        office = int(office_id)
        if office <= 0:
            raise AnalyticsValidationError(
                "office_id must be > 0 when provided",
                error_type="invalid_args",
            )
    page_size_i = max(1, min(int(page_size), MAX_PAGE_SIZE))
    max_pages_i = max(1, min(int(max_pages), MAX_PAGES))
    summary = bool(summary_only)
    # limit = tope de detalle únicamente; summary-only no lo usa para agregados
    detail_cap = max(1, min(int(limit), MAX_LIMIT))
    if not summary:
        detail_cap = min(detail_cap, page_size_i * max_pages_i)
    return CostAuditArgs(
        company_id=int(company_id),
        office_id=office,
        days=max(1, min(int(days), MAX_DAYS)),
        limit=detail_cap,
        sample_limit=max(1, min(int(sample_limit), MAX_SAMPLE_LIMIT)),
        statement_timeout_seconds=max(
            1, min(int(statement_timeout_seconds), MAX_TIMEOUT_SECONDS)
        ),
        variant_id=int(variant_id) if variant_id is not None else None,
        barcode=normalize_barcode(barcode),
        source_document_id=(
            int(source_document_id) if source_document_id is not None else None
        ),
        tolerances=tolerances or DEFAULT_TOLERANCES,
        page_size=page_size_i,
        max_pages=max_pages_i,
        summary_only=summary,
    )


@dataclass(frozen=True, slots=True)
class TaxCatalogEntry:
    tax_id: int
    name: str | None
    percentage: Decimal | None


@dataclass(frozen=True, slots=True)
class CostAuditRawRow:
    """Fila cruda cargada en batch (history + joins)."""

    history_id: int
    unique_key: str | None
    reception_id: int | None
    reception_detail_id: int | None
    source_document_id: int | None  # document_number o reception_id (no hay col canónica)
    variant_id: int
    product_id: int | None
    product_name: str | None
    variant_name: str | None
    barcode: str | None
    variant_code: str | None
    catalog_barcode: str | None
    admission_date: date | datetime | None
    quantity: Decimal | None
    cost_net: Decimal | None  # None preservado (no forzar 0)
    iva_amount: Decimal | None
    other_taxes: Decimal | None
    cost_bruto_erp: Decimal | None
    average_cost: Decimal | None
    reception_type: str | None
    office_id: int | None
    # variant_cost snapshot
    variant_cost_net: Decimal | None
    variant_cost_gross: Decimal | None
    vc_iva_rate: Decimal | None
    vc_tax_factor: Decimal | None
    specific_taxes: Any
    cost_source: str | None
    last_update: date | datetime | None
    # product tax context
    product_tax_factor: Decimal | None
    tax_ids_json: Any
    products_taxes: Any
    has_products_taxes_column: bool
    has_tax_ids_json: bool
    has_product_tax_factor: bool


@dataclass(slots=True)
class CostAuditRowResult:
    raw: CostAuditRawRow
    expected_gross_from_amounts: Decimal | None
    expected_gross_from_rates: Decimal | None
    expected_iva_from_rate: Decimal | None
    expected_specific_tax_from_rate: Decimal | None
    gross_difference_amounts: Decimal | None
    gross_difference_rates: Decimal | None
    tax_factor_used: Decimal | None
    iva_rate_used: Decimal | None
    specific_tax_rate_used: Decimal | None
    stored_components_status: str | None = None
    expected_tax_status: str | None = None
    corrected_gross_cost: Decimal | None = None
    stored_gross_cost: Decimal | None = None
    gross_understatement_amount: Decimal | None = None
    # % explícitos (no usar nombre ambiguo gross_understatement_pct)
    tax_rate_on_net_pct: Decimal | None = None
    gross_understatement_vs_corrected_pct: Decimal | None = None
    effective_quality_status: str | None = None
    tax_resolution: dict[str, Any] | None = None
    flags: list[str] = field(default_factory=list)
    probable_cause: str | None = None

    def to_sample_dict(self) -> dict[str, Any]:
        r = self.raw

        def _s(v: Decimal | None) -> str | None:
            return None if v is None else str(v)

        adm = r.admission_date
        if isinstance(adm, datetime):
            adm_s = adm.date().isoformat()
        elif isinstance(adm, date):
            adm_s = adm.isoformat()
        else:
            adm_s = None
        return {
            "history_id": r.history_id,
            "source_document_id": r.source_document_id,
            "variant_id": r.variant_id,
            "product_name": r.product_name,
            "variant_name": r.variant_name,
            "barcode": r.barcode or r.catalog_barcode,
            "variant_code": r.variant_code,
            "admission_date": adm_s,
            "quantity": _s(r.quantity),
            "cost_net": _s(r.cost_net),
            "iva_amount": _s(r.iva_amount),
            "other_taxes": _s(r.other_taxes),
            "cost_bruto_erp": _s(r.cost_bruto_erp),
            "stored_gross_cost": _s(self.stored_gross_cost),
            "expected_gross_from_amounts": _s(self.expected_gross_from_amounts),
            "expected_gross_from_rates": _s(self.expected_gross_from_rates),
            "expected_iva_amount": _s(self.expected_iva_from_rate),
            "expected_specific_tax_amount": _s(self.expected_specific_tax_from_rate),
            "corrected_gross_cost": _s(self.corrected_gross_cost),
            "gross_understatement_amount": _s(self.gross_understatement_amount),
            "tax_rate_on_net_pct": _s(self.tax_rate_on_net_pct),
            "gross_understatement_vs_corrected_pct": _s(
                self.gross_understatement_vs_corrected_pct
            ),
            "difference_stored_components": _s(self.gross_difference_amounts),
            "difference_expected_tax": _s(self.gross_difference_rates),
            "stored_components_status": self.stored_components_status,
            "expected_tax_status": self.expected_tax_status,
            "effective_quality_status": self.effective_quality_status,
            "tax_resolution": self.tax_resolution,
            "average_cost": _s(r.average_cost),
            "variant_cost_net": _s(r.variant_cost_net),
            "variant_cost_gross": _s(r.variant_cost_gross),
            "tax_factor": _s(self.tax_factor_used),
            "iva_rate": _s(self.iva_rate_used),
            "specific_taxes": r.specific_taxes,
            "tax_ids_json": r.tax_ids_json,
            "products_taxes": r.products_taxes,
            "flags": list(self.flags),
            "probable_cause": self.probable_cause,
        }


def coerce_optional_decimal(value: Any) -> Decimal | None:
    """Preserva None; no convierte silenciosamente a cero."""
    if value is None:
        return None
    return optional_decimal(value)


def rate_to_fraction(rate: Decimal | None) -> Decimal | None:
    """Convierte tasa almacenada a fracción.

    Convención confirmada en sync: iva_rate en puntos (19 = 19%).
    Si rate <= 1 se interpreta como fracción ya normalizada.
    """
    if rate is None:
        return None
    if rate > Decimal("1"):
        return rate / Decimal("100")
    return rate


def abs_decimal(value: Decimal) -> Decimal:
    return value if value >= ZERO else -value
