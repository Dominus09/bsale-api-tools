"""Motor analítico común — contratos y fórmulas.

Vista predeterminada: comercial BRUTA (IVA + ILA / cost_bruto_erp).
Vista secundaria: NETA.
"""

from __future__ import annotations

from backend.services.analytics.formulas import (
    apply_taxes_to_net_cost,
    commercial_line_economics,
    compute_commercial_margin_pct,
    compute_commercial_markup_pct,
    compute_commercial_profit,
    compute_gross_profit,
    compute_markup_pct,
    compute_margin_pct,
    line_economics,
    resolve_gross_cost,
    reverse_commercial_line,
)
from backend.services.analytics.money import D, quantize_commercial_pct, quantize_money, quantize_pct
from backend.services.analytics.quality import aggregate_line_quality
from backend.services.analytics.schemas import (
    AnalyticsLine,
    AnalyticsScope,
    AnalyticsSummary,
    CommercialLineEconomics,
    CostFallbackLevel,
    CostQualityStatus,
    DataQuality,
    DataQualityStatus,
    DocumentHeader,
    DocumentLine,
)
from backend.services.analytics.tax_models import (
    GrossCostQuality,
    PurchaseTaxAmounts,
    TaxBreakdownQuality,
    TaxCategory,
    TaxProfile,
    TaxQualityStatus,
    TaxResolutionMethod,
    classify_tax_category,
    resolve_tax_profile,
)

__all__ = [
    "AnalyticsLine",
    "AnalyticsScope",
    "AnalyticsSummary",
    "CommercialLineEconomics",
    "CostFallbackLevel",
    "CostQualityStatus",
    "D",
    "DataQuality",
    "DataQualityStatus",
    "DocumentHeader",
    "DocumentLine",
    "GrossCostQuality",
    "PurchaseTaxAmounts",
    "TaxBreakdownQuality",
    "TaxCategory",
    "TaxProfile",
    "TaxQualityStatus",
    "TaxResolutionMethod",
    "aggregate_line_quality",
    "apply_taxes_to_net_cost",
    "classify_tax_category",
    "commercial_line_economics",
    "compute_commercial_margin_pct",
    "compute_commercial_markup_pct",
    "compute_commercial_profit",
    "compute_gross_profit",
    "compute_margin_pct",
    "compute_markup_pct",
    "line_economics",
    "quantize_commercial_pct",
    "quantize_money",
    "quantize_pct",
    "resolve_gross_cost",
    "resolve_tax_profile",
    "reverse_commercial_line",
]
