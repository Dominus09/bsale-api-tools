"""Modelos del resolvedor de costo histórico (Etapa 3).

Fuentes auditadas en el repositorio (sin consultar PG):

1) analytics.cost_reception_history (038_cost_analytics_receptions.sql)
   - variant_id: variant_id
   - fecha efectiva: admission_date
   - costo neto: cost_net
   - UOM: implícita (misma que Bsale recepción); no hay columna UOM
   - proveedor: NO confirmado en esta tabla → supplier_id siempre None
   - documento origen: reception_id / document_number / reception_detail_id
   - confiabilidad: alta (evento de compra/recepción)

2) bsale.variant_cost (038 + sync cost_receptions)
   - variant_id: variant_id (+ company_id)
   - fecha: last_update (snapshot actual, no histórico de venta)
   - costo neto: average_cost_net
   - proveedor: no
   - confiabilidad: media (averageCost Bsale / fallback)

3) bsale.variant_cost_history
   - aparece en inventarios previos de PG pero SIN CREATE TABLE versionado
     en el repo → NO se usa en Etapa 3 (incierto).

4) products_master.supplier_id
   - existe para productos, no liga costo por fecha de venta → no usado aquí.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal

from backend.services.analytics.schemas import CostFallbackLevel, CostQualityStatus
from backend.services.analytics.tax_models import TaxProfile


@dataclass(frozen=True, slots=True)
class ReceptionCostCandidate:
    """Fila normalizada de analytics.cost_reception_history."""

    id: int
    variant_id: int
    cost_net: Decimal
    cost_date: date
    reception_id: int | None
    reception_detail_id: int | None
    document_number: int | None
    office_id: int | None
    iva_amount: Decimal | None = None
    other_taxes: Decimal | None = None
    cost_bruto_erp: Decimal | None = None


@dataclass(frozen=True, slots=True)
class VariantCostSnapshot:
    """Snapshot actual bsale.variant_cost (averageCost)."""

    variant_id: int
    average_cost_net: Decimal | None
    last_update: date | None
    cost_source: str | None


@dataclass(frozen=True, slots=True)
class HistoricalCostResolution:
    detail_id: int | None
    document_id: int
    variant_id: int | None
    commercial_date: date
    unit_cost: Decimal | None  # = historical_net_unit_cost
    total_cost: Decimal | None  # = historical_net_cost
    cost_source: str | None
    cost_date: date | None
    purchase_document_id: int | None
    supplier_id: int | None
    age_days_at_sale: int | None
    fallback_level: CostFallbackLevel
    is_estimated: bool
    quality_status: CostQualityStatus
    resolution_reason: str
    conflicting_source_ids: tuple[int, ...] = ()
    # Economía neta (secundaria; nombres Etapa 3)
    gross_profit: Decimal | None = None
    gross_margin_pct: Decimal | None = None
    markup_pct: Decimal | None = None
    quantity: Decimal | None = None
    line_net_amount: Decimal | None = None
    line_total_amount: Decimal | None = None
    # Desglose tributario + economía bruta comercial
    historical_net_unit_cost: Decimal | None = None
    historical_net_cost: Decimal | None = None
    cost_iva: Decimal | None = None
    cost_ila: Decimal | None = None
    historical_gross_unit_cost: Decimal | None = None
    historical_gross_cost: Decimal | None = None
    iva_sales: Decimal | None = None
    ila_sales: Decimal | None = None
    gross_sales: Decimal | None = None
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
    # Montos unitarios de la recepción ganadora (si aplica)
    reception_iva_amount: Decimal | None = None
    reception_other_taxes: Decimal | None = None
    reception_cost_bruto_erp: Decimal | None = None


@dataclass(frozen=True, slots=True)
class LineCostInput:
    """Entrada mínima para resolver + calcular economía de una línea."""

    document_id: int
    detail_id: int | None
    variant_id: int | None
    commercial_date: date
    quantity: Decimal | None
    line_net_amount: Decimal | None
    line_total_amount: Decimal | None = None
    tax_profile: TaxProfile | None = None
