"""Validación read-only cobertura costo bruto comercial (Etapa 3B).

Sin DDL/DML. Executor inyectable; el job CLI abre PG solo fuera de tests.
"""

from __future__ import annotations

import time
from collections import Counter
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from backend.services.analytics.cost_models import LineCostInput
from backend.services.analytics.cost_repository import CostCandidateRepository
from backend.services.analytics.distribuidora_source import (
    DOC_TYPE_BOLETA,
    DOC_TYPE_FACTURA,
    DistribuidoraDocumentSource,
)
from backend.services.analytics.historical_costs import HistoricalCostResolver
from backend.services.analytics.money import ZERO, quantize_commercial_pct, quantize_money
from backend.services.analytics.schemas import CostQualityStatus
from backend.services.analytics.tax_models import GrossCostQuality, TaxBreakdownQuality
from backend.services.analytics.validate_distribuidora_source import (
    AnalyticsValidationError,
    assert_sql_is_read_only,
    make_psycopg_executor,
    open_readonly_connection,
)

MAX_DAYS = 30
MAX_DOCUMENT_LIMIT = 500
MAX_TIMEOUT_SECONDS = 30
MAX_SAMPLE_LIMIT = 20
DEFAULT_DAYS = 7
DEFAULT_DOCUMENT_LIMIT = 200
DEFAULT_TIMEOUT_SECONDS = 20
DEFAULT_SAMPLE_LIMIT = 10
DEFAULT_LOCK_TIMEOUT = "3s"

SALE_DOC_TYPES = (DOC_TYPE_BOLETA, DOC_TYPE_FACTURA)

_SAMPLE_PRIORITY = (
    GrossCostQuality.MISSING_GROSS_COST.value,
    GrossCostQuality.CONFLICTING_GROSS_COST.value,
    TaxBreakdownQuality.AGGREGATED_OTHER_TAXES.value,
    GrossCostQuality.CURRENT_TAX_PROFILE_FALLBACK.value,
)


@dataclass(frozen=True, slots=True)
class GrossValidateArgs:
    company_id: int
    office_id: int
    days: int = DEFAULT_DAYS
    document_limit: int = DEFAULT_DOCUMENT_LIMIT
    statement_timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS
    sample_limit: int = DEFAULT_SAMPLE_LIMIT


def clamp_gross_validate_args(
    *,
    company_id: int,
    office_id: int,
    days: int = DEFAULT_DAYS,
    document_limit: int = DEFAULT_DOCUMENT_LIMIT,
    statement_timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    sample_limit: int = DEFAULT_SAMPLE_LIMIT,
) -> GrossValidateArgs:
    if int(company_id) <= 0 or int(office_id) <= 0:
        raise AnalyticsValidationError(
            "company_id and office_id are required and must be > 0",
            error_type="invalid_args",
        )
    return GrossValidateArgs(
        company_id=int(company_id),
        office_id=int(office_id),
        days=max(1, min(int(days), MAX_DAYS)),
        document_limit=max(1, min(int(document_limit), MAX_DOCUMENT_LIMIT)),
        statement_timeout_seconds=max(
            1, min(int(statement_timeout_seconds), MAX_TIMEOUT_SECONDS)
        ),
        sample_limit=max(1, min(int(sample_limit), MAX_SAMPLE_LIMIT)),
    )


def commercial_date_window(days: int, *, today: date | None = None) -> tuple[date, date]:
    if today is None:
        try:
            from zoneinfo import ZoneInfo

            today = datetime.now(ZoneInfo("America/Santiago")).date()
        except Exception:
            today = datetime.now(timezone.utc).date()
    days_i = max(1, min(int(days), MAX_DAYS))
    date_to = today
    date_from = date_to - timedelta(days=days_i - 1)
    return date_from, date_to


def _net_bucket(status: CostQualityStatus) -> str:
    if status == CostQualityStatus.HISTORICAL_REAL:
        return "historical_reception"
    if status == CostQualityStatus.AVERAGE_COST_FALLBACK:
        return "average_cost_fallback"
    if status == CostQualityStatus.CURRENT_COST_FALLBACK:
        return "current_cost_fallback"
    if status == CostQualityStatus.CONFLICTING_COST:
        return "conflicting"
    return "missing"


def _gross_bucket(value: str | None) -> str:
    mapping = {
        GrossCostQuality.ACTUAL_PURCHASE_GROSS.value: "actual_purchase_gross",
        GrossCostQuality.RECONSTRUCTED_FROM_ACTUAL_TAXES.value: "reconstructed_from_actual_taxes",
        GrossCostQuality.HISTORICAL_TAX_PROFILE.value: "historical_tax_profile",
        GrossCostQuality.CURRENT_TAX_PROFILE_FALLBACK.value: "current_tax_profile_fallback",
        GrossCostQuality.CONFLICTING_GROSS_COST.value: "conflicting",
        GrossCostQuality.MISSING_GROSS_COST.value: "missing",
    }
    if value is None:
        return "missing"
    return mapping.get(value, "missing")


def _tax_bucket(value: str | None) -> str:
    mapping = {
        TaxBreakdownQuality.EXACT_IVA_ILA_SPLIT.value: "exact_iva_ila_split",
        TaxBreakdownQuality.AGGREGATED_OTHER_TAXES.value: "aggregated_other_taxes",
        TaxBreakdownQuality.RECONSTRUCTED_FROM_RATES.value: "reconstructed_from_rates",
        TaxBreakdownQuality.PARTIAL_BREAKDOWN.value: "partial_breakdown",
        TaxBreakdownQuality.CONFLICTING_BREAKDOWN.value: "conflicting",
        TaxBreakdownQuality.MISSING_BREAKDOWN.value: "missing",
    }
    if value is None:
        return "missing"
    return mapping.get(value, "missing")


def _sample_rank(resolution: Any) -> tuple[int, int]:
    gq = resolution.gross_cost_quality or GrossCostQuality.MISSING_GROSS_COST.value
    tq = resolution.tax_breakdown_quality or TaxBreakdownQuality.MISSING_BREAKDOWN.value
    for i, key in enumerate(_SAMPLE_PRIORITY):
        if gq == key or tq == key:
            return (i, resolution.document_id or 0)
    return (len(_SAMPLE_PRIORITY), resolution.document_id or 0)


def build_coverage_report(
    *,
    args: GrossValidateArgs,
    date_from: date,
    date_to: date,
    documents_loaded: int,
    resolutions: Sequence[Any],
    duration_ms: float,
) -> dict[str, Any]:
    net_cov = Counter()
    gross_cov = Counter()
    tax_cov = Counter()

    sales_gross = ZERO
    sales_net = ZERO
    known_gross_cost = ZERO
    known_gross_profit = ZERO
    calculable_sales_gross = ZERO
    calculable_lines = 0
    uncalculable_lines = 0
    variants: set[int] = set()
    sample_candidates: list[Any] = []

    for res in resolutions:
        if res.variant_id is not None:
            variants.add(int(res.variant_id))
        net_cov[_net_bucket(res.quality_status)] += 1
        gross_cov[_gross_bucket(res.gross_cost_quality)] += 1
        tax_cov[_tax_bucket(res.tax_breakdown_quality)] += 1

        g_sales = res.gross_sales if res.gross_sales is not None else ZERO
        n_sales = res.line_net_amount if res.line_net_amount is not None else ZERO
        sales_gross += g_sales
        sales_net += n_sales

        if res.historical_gross_cost is not None and res.gross_sales is not None:
            calculable_lines += 1
            calculable_sales_gross += res.gross_sales
            known_gross_cost += res.historical_gross_cost
            if res.gross_commercial_profit is not None:
                known_gross_profit += res.gross_commercial_profit
        else:
            uncalculable_lines += 1
            sample_candidates.append(res)

    # Completar samples con prioridad aunque sean calculables (aggregated etc.).
    sample_candidates.extend(
        r
        for r in resolutions
        if (r.gross_cost_quality or "") in {
            GrossCostQuality.MISSING_GROSS_COST.value,
            GrossCostQuality.CONFLICTING_GROSS_COST.value,
            GrossCostQuality.CURRENT_TAX_PROFILE_FALLBACK.value,
        }
        or (r.tax_breakdown_quality or "")
        == TaxBreakdownQuality.AGGREGATED_OTHER_TAXES.value
    )
    # unique by (document_id, detail_id)
    seen: set[tuple[Any, Any]] = set()
    unique_samples: list[Any] = []
    for r in sorted(sample_candidates, key=_sample_rank):
        key = (r.document_id, r.detail_id)
        if key in seen:
            continue
        seen.add(key)
        unique_samples.append(r)
        if len(unique_samples) >= args.sample_limit:
            break

    total_lines = len(resolutions)
    line_cov = (
        quantize_commercial_pct(Decimal(calculable_lines) / Decimal(total_lines) * 100)
        if total_lines
        else None
    )
    sales_cov = (
        quantize_commercial_pct(calculable_sales_gross / sales_gross * 100)
        if sales_gross > ZERO
        else None
    )
    margin = (
        quantize_commercial_pct(known_gross_profit / calculable_sales_gross * 100)
        if calculable_sales_gross > ZERO and calculable_lines > 0
        else None
    )

    samples = [
        {
            "document_id": r.document_id,
            "detail_id": r.detail_id,
            "variant_id": r.variant_id,
            "commercial_date": r.commercial_date.isoformat(),
            "gross_sales": str(r.gross_sales) if r.gross_sales is not None else None,
            "net_cost": str(r.historical_net_cost) if r.historical_net_cost is not None else None,
            "gross_cost": (
                str(r.historical_gross_cost) if r.historical_gross_cost is not None else None
            ),
            "net_cost_quality": r.quality_status.value,
            "gross_cost_quality": r.gross_cost_quality,
            "tax_breakdown_quality": r.tax_breakdown_quality,
            "resolution_reason": r.resolution_reason,
        }
        for r in unique_samples
    ]

    return {
        "ok": True,
        "read_only": True,
        "scope": {
            "company_id": args.company_id,
            "office_id": args.office_id,
            "date_from": date_from.isoformat(),
            "date_to": date_to.isoformat(),
        },
        "documents_loaded": documents_loaded,
        "lines_loaded": total_lines,
        "unique_variants": len(variants),
        "sales": {
            "gross": str(quantize_money(sales_gross)),
            "net": str(quantize_money(sales_net)),
        },
        "net_cost_coverage": {
            "historical_reception": net_cov["historical_reception"],
            "average_cost_fallback": net_cov["average_cost_fallback"],
            "current_cost_fallback": net_cov["current_cost_fallback"],
            "missing": net_cov["missing"],
            "conflicting": net_cov["conflicting"],
        },
        "gross_cost_coverage": {
            "actual_purchase_gross": gross_cov["actual_purchase_gross"],
            "reconstructed_from_actual_taxes": gross_cov["reconstructed_from_actual_taxes"],
            "historical_tax_profile": gross_cov["historical_tax_profile"],
            "current_tax_profile_fallback": gross_cov["current_tax_profile_fallback"],
            "missing": gross_cov["missing"],
            "conflicting": gross_cov["conflicting"],
        },
        "tax_breakdown": {
            "exact_iva_ila_split": tax_cov["exact_iva_ila_split"],
            "aggregated_other_taxes": tax_cov["aggregated_other_taxes"],
            "reconstructed_from_rates": tax_cov["reconstructed_from_rates"],
            "partial_breakdown": tax_cov["partial_breakdown"],
            "missing": tax_cov["missing"],
            "conflicting": tax_cov["conflicting"],
        },
        "commercial_margin": {
            "calculable_lines": calculable_lines,
            "uncalculable_lines": uncalculable_lines,
            "line_coverage_pct": str(line_cov) if line_cov is not None else None,
            "gross_sales_coverage_pct": str(sales_cov) if sales_cov is not None else None,
            "known_gross_cost": str(quantize_money(known_gross_cost)),
            "known_gross_profit": str(quantize_money(known_gross_profit)),
            "gross_margin_pct": str(margin) if margin is not None else None,
        },
        "samples": samples,
        "duration_ms": round(duration_ms, 1),
    }


def run_gross_cost_validation(
    *,
    args: GrossValidateArgs,
    document_source: DistribuidoraDocumentSource,
    cost_repository: CostCandidateRepository,
    today: date | None = None,
) -> dict[str, Any]:
    t0 = time.perf_counter()
    date_from, date_to = commercial_date_window(args.days, today=today)
    headers = document_source.fetch_documents(
        company_id=args.company_id,
        office_id=args.office_id,
        date_from=date_from,
        date_to=date_to,
        document_type_ids=SALE_DOC_TYPES,
        active_only=True,
        page=1,
        page_size=args.document_limit,
    )
    if len(headers) > args.document_limit:
        headers = headers[: args.document_limit]
    doc_ids = [h.document_id for h in headers]
    lines = document_source.fetch_lines_for_documents(
        company_id=args.company_id,
        office_id=args.office_id,
        document_ids=doc_ids,
        max_lines=max(args.document_limit * 200, 500),
    )
    allowed = set(doc_ids)
    lines = [ln for ln in lines if ln.document_id in allowed]

    # Enriquecer net allocation una vez por documento (batch ya cargado).
    by_doc: dict[int, list] = {}
    for ln in lines:
        by_doc.setdefault(ln.document_id, []).append(ln)

    cost_inputs: list[LineCostInput] = []
    for header in headers:
        enriched = document_source.enrich_lines_with_header_net(
            header, by_doc.get(header.document_id, [])
        )
        for ln in enriched:
            cost_inputs.append(
                LineCostInput(
                    document_id=ln.document_id,
                    detail_id=ln.detail_id,
                    variant_id=ln.variant_id,
                    commercial_date=header.commercial_date,
                    quantity=ln.quantity,
                    line_net_amount=ln.allocated_net_amount,
                    line_total_amount=ln.line_total_amount,
                )
            )

    resolver = HistoricalCostResolver(cost_repository)
    resolutions = resolver.resolve_lines(cost_inputs, company_id=args.company_id)
    duration_ms = (time.perf_counter() - t0) * 1000.0
    return build_coverage_report(
        args=args,
        date_from=date_from,
        date_to=date_to,
        documents_loaded=len(headers),
        resolutions=resolutions,
        duration_ms=duration_ms,
    )


# Re-exports útiles para el job / tests
__all__ = [
    "AnalyticsValidationError",
    "GrossValidateArgs",
    "assert_sql_is_read_only",
    "clamp_gross_validate_args",
    "commercial_date_window",
    "make_psycopg_executor",
    "open_readonly_connection",
    "run_gross_cost_validation",
    "build_coverage_report",
    "DEFAULT_LOCK_TIMEOUT",
]
