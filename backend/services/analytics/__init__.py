"""Motor analítico común — contratos y fórmulas (Fase 1 / Etapa 1).

No conecta consultas pesadas ni endpoints. Los módulos frontend
(/costos, /margins, comercial, NC) consumirán este paquete en etapas
posteriores.
"""

from __future__ import annotations

from backend.services.analytics.formulas import (
    compute_gross_profit,
    compute_markup_pct,
    compute_margin_pct,
    line_economics,
)
from backend.services.analytics.money import D, quantize_money, quantize_pct
from backend.services.analytics.quality import aggregate_line_quality
from backend.services.analytics.schemas import (
    AnalyticsLine,
    AnalyticsScope,
    AnalyticsSummary,
    CostFallbackLevel,
    CostQualityStatus,
    DataQuality,
    DataQualityStatus,
    DocumentHeader,
    DocumentLine,
)

__all__ = [
    "AnalyticsLine",
    "AnalyticsScope",
    "AnalyticsSummary",
    "CostFallbackLevel",
    "CostQualityStatus",
    "D",
    "DataQuality",
    "DataQualityStatus",
    "DocumentHeader",
    "DocumentLine",
    "aggregate_line_quality",
    "compute_gross_profit",
    "compute_margin_pct",
    "compute_markup_pct",
    "line_economics",
    "quantize_money",
    "quantize_pct",
]
