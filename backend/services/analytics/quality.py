"""Agregación de calidad de dato (sin I/O)."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from backend.services.analytics.money import ZERO, quantize_pct
from backend.services.analytics.schemas import (
    CostQualityStatus,
    DataQuality,
    DataQualityStatus,
)


_ESTIMATED = {
    CostQualityStatus.AVERAGE_COST_FALLBACK,
    CostQualityStatus.CURRENT_COST_FALLBACK,
}

_COSTED = {
    CostQualityStatus.HISTORICAL_REAL,
    CostQualityStatus.AVERAGE_COST_FALLBACK,
    CostQualityStatus.CURRENT_COST_FALLBACK,
}


def aggregate_line_quality(
    cost_statuses: list[CostQualityStatus],
    *,
    unmatched_credit_notes: int = 0,
    header_line_mismatch_docs: int = 0,
    data_freshness_at: datetime | None = None,
    source_scope: str = "",
) -> DataQuality:
    """Resume calidad a partir de estados de costo por línea."""
    total = len(cost_statuses)
    missing = sum(1 for s in cost_statuses if s == CostQualityStatus.MISSING_COST)
    estimated = sum(1 for s in cost_statuses if s in _ESTIMATED)
    costed = sum(1 for s in cost_statuses if s in _COSTED)
    conflicting = any(s == CostQualityStatus.CONFLICTING_COST for s in cost_statuses)

    coverage: Decimal | None
    if total == 0:
        coverage = None
        status = DataQualityStatus.UNAVAILABLE
    else:
        coverage = quantize_pct(Decimal(costed) / Decimal(total) * Decimal("100"))
        if conflicting or header_line_mismatch_docs > 0:
            status = DataQualityStatus.CONFLICTING
        elif missing == total:
            status = DataQualityStatus.UNAVAILABLE
        elif missing == 0 and estimated == 0:
            status = DataQualityStatus.COMPLETE
        elif costed > 0 and estimated > 0 and missing == 0:
            status = DataQualityStatus.ESTIMATED
        else:
            status = DataQualityStatus.PARTIAL

    return DataQuality(
        total_lines=total,
        costed_lines=costed,
        missing_cost_lines=missing,
        estimated_cost_lines=estimated,
        cost_coverage_pct=coverage,
        unmatched_credit_notes=unmatched_credit_notes,
        header_line_mismatch_docs=header_line_mismatch_docs,
        data_freshness_at=data_freshness_at,
        source_scope=source_scope,
        quality_status=status,
    )


def empty_quality(*, source_scope: str = "") -> DataQuality:
    return DataQuality(
        total_lines=0,
        costed_lines=0,
        missing_cost_lines=0,
        estimated_cost_lines=0,
        cost_coverage_pct=None,
        unmatched_credit_notes=0,
        header_line_mismatch_docs=0,
        data_freshness_at=None,
        source_scope=source_scope,
        quality_status=DataQualityStatus.UNAVAILABLE,
    )


# Re-export for callers that expect ZERO nearby
__all__ = [
    "ZERO",
    "aggregate_line_quality",
    "empty_quality",
]
