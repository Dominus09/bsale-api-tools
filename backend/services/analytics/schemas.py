"""Contratos canónicos del motor analítico.

Vista predeterminada Quillotana = COMERCIAL BRUTA (IVA + ILA incluidos).
Vista secundaria = NETA.

Definiciones financieras (referencia):

- gross_sales: line.total_amount (preferido) o document.total_amount.
- net_sales: monto neto de línea / prorrateo reconciliable.
- historical_net_cost: historical_net_unit_cost × quantity (Bsale costo base).
- historical_gross_cost: net + cost_iva + cost_ila (aditivo; no compuesto).
- gross_commercial_profit: gross_sales − historical_gross_cost (UI default).
- net metrics (gross_profit / gross_margin_pct / markup_pct): vista secundaria.

Política de Márgenes = target/control; no altera margen real.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any


class CostQualityStatus(str, Enum):
    """Calidad del costo resuelto para una línea."""

    HISTORICAL_REAL = "historical_real"
    AVERAGE_COST_FALLBACK = "average_cost_fallback"
    CURRENT_COST_FALLBACK = "current_cost_fallback"
    MISSING_COST = "missing_cost"
    CONFLICTING_COST = "conflicting_cost"


class CostFallbackLevel(int, Enum):
    """Nivel de fallback del resolvedor de costo (menor = mejor)."""

    RECEPTION_AT_SALE = 1
    HISTORICAL_BEFORE_SALE = 2
    AVERAGE_COST = 3
    CURRENT_COST = 4
    MISSING = 5


class DataQualityStatus(str, Enum):
    """Calidad agregada de un resultado analítico."""

    COMPLETE = "complete"
    PARTIAL = "partial"
    ESTIMATED = "estimated"
    UNAVAILABLE = "unavailable"
    CONFLICTING = "conflicting"


class DocumentRole(str, Enum):
    SALE = "sale"
    CREDIT_NOTE = "credit_note"
    OTHER = "other"


@dataclass(frozen=True, slots=True)
class AnalyticsScope:
    """Alcance de consulta; office_id opcional para futuros carriles retail."""

    company_id: int
    office_id: int | None = None
    date_from: date | None = None
    date_to: date | None = None
    source_adapter: str = "distribuidora_live"


@dataclass(frozen=True, slots=True)
class DataQuality:
    total_lines: int = 0
    costed_lines: int = 0
    missing_cost_lines: int = 0
    estimated_cost_lines: int = 0
    cost_coverage_pct: Decimal | None = None
    unmatched_credit_notes: int = 0
    header_line_mismatch_docs: int = 0
    data_freshness_at: datetime | None = None
    source_scope: str = ""
    quality_status: DataQualityStatus = DataQualityStatus.UNAVAILABLE

    def to_dict(self) -> dict[str, Any]:
        return {
            "total_lines": self.total_lines,
            "costed_lines": self.costed_lines,
            "missing_cost_lines": self.missing_cost_lines,
            "estimated_cost_lines": self.estimated_cost_lines,
            "cost_coverage_pct": self.cost_coverage_pct,
            "unmatched_credit_notes": self.unmatched_credit_notes,
            "header_line_mismatch_docs": self.header_line_mismatch_docs,
            "data_freshness_at": self.data_freshness_at,
            "source_scope": self.source_scope,
            "quality_status": self.quality_status.value,
        }


@dataclass(frozen=True, slots=True)
class DocumentHeader:
    """Encabezado normalizado (adaptador → motor)."""

    document_id: int
    company_id: int
    office_id: int
    document_type_id: int
    role: DocumentRole
    emission_date: date
    state: int
    net_amount: Decimal | None
    tax_amount: Decimal | None
    total_amount: Decimal | None
    client_id: int | None = None
    seller_id: int | None = None
    seller_name: str | None = None
    source_document_id: int | None = None
    is_active: bool = True


@dataclass(frozen=True, slots=True)
class DocumentLine:
    """Línea normalizada (adaptador → motor)."""

    document_id: int
    detail_id: int | None
    variant_id: int | None
    product_id: int | None
    quantity: Decimal
    line_net_amount: Decimal | None
    line_total_amount: Decimal | None
    company_id: int
    office_id: int
    emission_date: date
    document_type_id: int
    role: DocumentRole
    seller_id: int | None = None
    client_id: int | None = None


@dataclass(frozen=True, slots=True)
class ResolvedCost:
    """Resultado del resolvedor de costo (Etapa 2+; contrato ya fijo)."""

    unit_cost: Decimal | None
    total_cost: Decimal | None
    cost_source: str | None
    cost_date: date | None
    age_days_at_sale: int | None
    purchase_document_id: int | None
    supplier_id: int | None
    quality_status: CostQualityStatus
    is_estimated: bool
    fallback_level: CostFallbackLevel


@dataclass(frozen=True, slots=True)
class LineEconomics:
    """Economía NETA de una línea (vista secundaria; compat Etapa 1)."""

    net_sales: Decimal
    historical_cost: Decimal | None
    gross_profit: Decimal | None
    gross_margin_pct: Decimal | None
    markup_pct: Decimal | None
    cost_quality: CostQualityStatus


@dataclass(frozen=True, slots=True)
class CommercialLineEconomics:
    """Economía comercial completa (bruta predeterminada + neta secundaria)."""

    net_sales: Decimal
    iva_sales: Decimal | None
    ila_sales: Decimal | None
    gross_sales: Decimal
    historical_net_unit_cost: Decimal | None
    historical_net_cost: Decimal | None
    cost_iva: Decimal | None
    cost_ila: Decimal | None
    historical_gross_unit_cost: Decimal | None
    historical_gross_cost: Decimal | None
    net_gross_profit: Decimal | None
    gross_commercial_profit: Decimal | None
    net_margin_pct: Decimal | None
    gross_commercial_margin_pct: Decimal | None
    net_markup_pct: Decimal | None
    gross_commercial_markup_pct: Decimal | None
    cost_quality: CostQualityStatus
    tax_resolution_method: str
    tax_quality_status: str
    tax_category: str | None = None
    tax_source: str | None = None
    gross_cost_quality: str | None = None
    tax_breakdown_quality: str | None = None
    total_tax_amount: Decimal | None = None
    unclassified_tax_amount: Decimal | None = None


@dataclass(frozen=True, slots=True)
class AnalyticsLine:
    document_id: int
    detail_id: int | None
    emission_date: date
    client_id: int | None
    seller_id: int | None
    product_id: int | None
    variant_id: int | None
    net_quantity: Decimal
    # --- neto (secundario; nombres Etapa 1 conservados) ---
    net_sales: Decimal
    historical_cost: Decimal | None
    gross_profit: Decimal | None
    gross_margin_pct: Decimal | None
    markup_pct: Decimal | None
    related_credit_note_id: int | None
    cost_quality: CostQualityStatus
    data_quality: DataQualityStatus
    # --- bruto comercial (predeterminado UI) ---
    iva_sales: Decimal | None = None
    ila_sales: Decimal | None = None
    gross_sales: Decimal | None = None
    historical_net_unit_cost: Decimal | None = None
    historical_net_cost: Decimal | None = None
    cost_iva: Decimal | None = None
    cost_ila: Decimal | None = None
    historical_gross_unit_cost: Decimal | None = None
    historical_gross_cost: Decimal | None = None
    net_gross_profit: Decimal | None = None
    gross_commercial_profit: Decimal | None = None
    net_margin_pct: Decimal | None = None
    gross_commercial_margin_pct: Decimal | None = None
    net_markup_pct: Decimal | None = None
    gross_commercial_markup_pct: Decimal | None = None
    tax_resolution_method: str | None = None
    tax_quality_status: str | None = None
    tax_category: str | None = None
    tax_source: str | None = None
    gross_cost_quality: str | None = None
    tax_breakdown_quality: str | None = None
    total_tax_amount: Decimal | None = None
    unclassified_tax_amount: Decimal | None = None


@dataclass(frozen=True, slots=True)
class AnalyticsSummary:
    gross_sales: Decimal
    net_sales_before_credit_notes: Decimal
    credit_notes_net: Decimal
    net_sales: Decimal
    historical_cost: Decimal | None
    gross_profit: Decimal | None
    gross_margin_pct: Decimal | None
    markup_pct: Decimal | None
    document_count: int
    customer_count: int
    net_units: Decimal
    data_quality: DataQuality
    extra: dict[str, Any] = field(default_factory=dict)
    # Agregados brutos comerciales (cuando el pipeline los rellene)
    historical_gross_cost: Decimal | None = None
    gross_commercial_profit: Decimal | None = None
    gross_commercial_margin_pct: Decimal | None = None
    gross_commercial_markup_pct: Decimal | None = None
    cost_iva: Decimal | None = None
    cost_ila: Decimal | None = None
    iva_sales: Decimal | None = None
    ila_sales: Decimal | None = None
