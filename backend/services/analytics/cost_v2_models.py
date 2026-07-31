"""Modelos inmutables del motor de cálculo de costos V2 (sin I/O)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Literal

CALCULATION_VERSION = "cost-v2.0.0"

TaxContextSource = Literal[
    "reception_payload",
    "historical_product_tax",
    "current_product_tax",
    "bsale_taxes",
    "canonical_fallback",
    "unresolved",
]

TaxResolutionQuality = Literal[
    "direct_reception",
    "historical_catalog",
    "current_catalog",
    "canonical_fallback",
    "unresolved",
]

EffectiveQualityStatus = Literal[
    "missing_cost",
    "gross_component_mismatch",
    "duplicated_taxes_in_gross",
    "missing_taxes_in_gross",
    "incomplete_tax_context",
    "valid_gross",
]

ALLOWED_CONTEXT_SOURCES: frozenset[str] = frozenset(
    {
        "reception_payload",
        "historical_product_tax",
        "current_product_tax",
        "bsale_taxes",
        "canonical_fallback",
        "unresolved",
    }
)

ALLOWED_RESOLUTION_QUALITIES: frozenset[str] = frozenset(
    {
        "direct_reception",
        "historical_catalog",
        "current_catalog",
        "canonical_fallback",
        "unresolved",
    }
)

TaxIdsSource = Literal[
    "reception_payload",
    "historical_product_tax",
    "current_product_tax",
    "unresolved",
]

TaxRatesSource = Literal[
    "reception_payload",
    "bsale_taxes",
    "canonical_fallback",
    "unresolved",
]

ALLOWED_TAX_IDS_SOURCES: frozenset[str] = frozenset(
    {
        "reception_payload",
        "historical_product_tax",
        "current_product_tax",
        "unresolved",
    }
)

ALLOWED_TAX_RATES_SOURCES: frozenset[str] = frozenset(
    {
        "reception_payload",
        "bsale_taxes",
        "canonical_fallback",
        "unresolved",
    }
)


@dataclass(frozen=True, slots=True)
class CostReceptionInput:
    """Fila almacenada (history) sin coercer NULL→0."""

    history_id: int
    company_id: int
    office_id: int | None
    variant_id: int
    admission_date: date | datetime | None
    stored_cost_net: Decimal | None
    stored_quantity: Decimal | None
    stored_iva_amount: Decimal | None
    stored_other_taxes: Decimal | None
    stored_gross_cost: Decimal | None
    reception_tax_ids: tuple[int, ...] = ()
    catalog_tax_ids: tuple[int, ...] = ()
    source_history_created_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class TaxRateEntry:
    tax_id: int
    name: str | None
    rate: Decimal
    category: str
    source: str


@dataclass(frozen=True, slots=True)
class TaxContextInput:
    tax_ids: tuple[int, ...]
    taxes: tuple[TaxRateEntry, ...]
    # DEPRECATED: prefer tax_ids_source + tax_rates_source (compat temporal).
    context_source: TaxContextSource
    context_as_of: datetime | None
    context_is_historical: bool
    resolution_quality: TaxResolutionQuality
    tax_ids_source: TaxIdsSource = "unresolved"
    tax_rates_source: TaxRatesSource = "unresolved"


@dataclass(frozen=True, slots=True)
class AdditionalTaxAmount:
    tax_id: int
    name: str | None
    rate: Decimal
    category: str
    amount: Decimal
    source: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "tax_id": self.tax_id,
            "name": self.name,
            "rate": str(self.rate),
            "category": self.category,
            "amount": str(self.amount),
            "source": self.source,
        }


@dataclass(frozen=True, slots=True)
class CostReceptionCalculation:
    """Resultado listo para persistir (sin calculation_batch_id)."""

    history_id: int
    company_id: int
    office_id: int | None
    variant_id: int
    admission_date: date | None
    calculation_version: str
    stored_cost_net: Decimal | None
    stored_quantity: Decimal | None
    stored_iva_amount: Decimal | None
    stored_other_taxes: Decimal | None
    stored_gross_cost: Decimal | None
    reception_tax_ids: tuple[int, ...]
    catalog_tax_ids: tuple[int, ...]
    resolved_tax_ids: tuple[int, ...]
    iva_tax_id: int | None
    iva_rate: Decimal | None
    calculated_iva_amount: Decimal | None
    additional_taxes: tuple[AdditionalTaxAmount, ...]
    additional_tax_rate_total: Decimal | None
    additional_tax_amount_total: Decimal | None
    total_tax_rate: Decimal | None
    corrected_gross_cost: Decimal | None
    gross_difference_amount: Decimal | None
    tax_rate_on_net_pct: Decimal | None
    gross_understatement_vs_corrected_pct: Decimal | None
    # DEPRECATED: prefer tax_ids_source + tax_rates_source (compat temporal).
    tax_context_source: str
    tax_ids_source: str
    tax_rates_source: str
    tax_context_as_of: datetime | None
    tax_context_is_historical: bool | None
    tax_resolution_quality: str
    effective_quality_status: str
    warnings: tuple[str, ...]
    source_history_created_at: datetime | None
    source_history_fingerprint: str
    tax_context_fingerprint: str

    def additional_taxes_json(self) -> list[dict[str, Any]]:
        return [t.to_dict() for t in self.additional_taxes]


@dataclass(frozen=True, slots=True)
class CostV2Tolerances:
    money_exact: Decimal = Decimal("0.0001")
    money_rounding: Decimal = Decimal("0.01")
    duplicate_rel: Decimal = Decimal("0.02")
