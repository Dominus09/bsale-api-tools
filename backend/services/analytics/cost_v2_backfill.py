"""Orquestación dry-run del backfill Costos V2 (solo lectura)."""

from __future__ import annotations

import time
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from statistics import median
from typing import Any

from backend.repositories.cost_v2_backfill_repo import CostV2BackfillRepository
from backend.services.analytics.cost_tax_resolution import TAX_ID_FALLBACK
from backend.services.analytics.cost_v2_calculator import (
    CALCULATION_VERSION,
    build_tax_context_from_ids,
    calculate_cost_reception,
    source_history_fingerprint,
)
from backend.services.analytics.cost_v2_models import (
    ALLOWED_CONTEXT_SOURCES,
    ALLOWED_RESOLUTION_QUALITIES,
    ALLOWED_TAX_IDS_SOURCES,
    ALLOWED_TAX_RATES_SOURCES,
    CostReceptionCalculation,
    CostReceptionInput,
)
from backend.services.analytics.money import ZERO
from backend.services.analytics.validate_distribuidora_source import (
    AnalyticsValidationError,
)

QUALITY_KEYS = (
    "missing_cost",
    "gross_component_mismatch",
    "duplicated_taxes_in_gross",
    "missing_taxes_in_gross",
    "incomplete_tax_context",
    "valid_gross",
)

SAMPLE_PRIORITY = (
    "missing_cost",
    "gross_component_mismatch",
    "duplicated_taxes_in_gross",
    "incomplete_tax_context",
    "missing_taxes_in_gross",
    "valid_gross",
)

MAX_BATCH_SIZE = 2000
DEFAULT_BATCH_SIZE = 500
MAX_SAMPLE_LIMIT = 100
DEFAULT_SAMPLE_LIMIT = 20
MAX_TIMEOUT = 30
MAX_APPLY_ROWS_CAP = 100
DEFAULT_MAX_APPLY_ROWS = 100

# Outlier (orquestador): mediana robusta por variant_id dentro del scope.
OUTLIER_FACTOR = Decimal("3")
MIN_OUTLIER_CANDIDATES = 3

# Vocabulario de warnings del dry-run V2 (no inventar alerts nuevas aquí).
# missing_document_number: NO está en este vocabulario todavía; document_number NULL
# se preserva sin warning hasta que se defina formalmente.
KNOWN_WARNINGS = frozenset(
    {
        "suspicious_outlier",
        "tax_ids_not_consumed",
        "variant_barcode_mismatch",
        "source_conflict",
        "reception_tax_context_unavailable",
        "stored_components_rounding",
    }
)


@dataclass(frozen=True, slots=True)
class CostV2BackfillArgs:
    company_id: int
    office_id: int | None
    date_from: date
    date_to: date
    dry_run: bool
    batch_size: int
    sample_limit: int
    statement_timeout_seconds: int
    calculation_version: str
    history_id: int | None = None
    variant_id: int | None = None
    barcode: str | None = None
    document_number: int | None = None
    apply: bool = False
    confirm_history_id: int | None = None
    apply_scope: bool = False
    confirm_row_count: int | None = None
    max_apply_rows: int = DEFAULT_MAX_APPLY_ROWS


def clamp_backfill_args(
    *,
    company_id: int,
    office_id: int | None = None,
    date_from: date,
    date_to: date,
    dry_run: bool = True,
    batch_size: int = DEFAULT_BATCH_SIZE,
    sample_limit: int = DEFAULT_SAMPLE_LIMIT,
    statement_timeout_seconds: int = 20,
    calculation_version: str = CALCULATION_VERSION,
    history_id: int | None = None,
    variant_id: int | None = None,
    barcode: str | None = None,
    document_number: int | None = None,
    apply: bool = False,
    confirm_history_id: int | None = None,
    apply_scope: bool = False,
    confirm_row_count: int | None = None,
    max_apply_rows: int = DEFAULT_MAX_APPLY_ROWS,
) -> CostV2BackfillArgs:
    if int(company_id) <= 0:
        raise AnalyticsValidationError(
            "company_id is required and must be > 0", error_type="invalid_args"
        )
    if date_to < date_from:
        raise AnalyticsValidationError(
            "date_to must be >= date_from", error_type="invalid_args"
        )
    version = str(calculation_version or "").strip()
    if not version:
        raise AnalyticsValidationError(
            "calculation_version no puede estar vacía",
            error_type="invalid_args",
        )

    if apply and dry_run:
        raise AnalyticsValidationError(
            "--dry-run y --apply no pueden usarse juntos",
            error_type="apply_dry_run_conflict",
        )

    if apply_scope and not apply:
        raise AnalyticsValidationError(
            "--apply-scope requiere --apply",
            error_type="apply_scope_requires_apply",
        )

    if apply and apply_scope:
        if history_id is not None or confirm_history_id is not None:
            raise AnalyticsValidationError(
                "Apply scope no puede combinarse con --history-id",
                error_type="apply_scope_history_forbidden",
            )
        if office_id is None:
            raise AnalyticsValidationError(
                "Apply scope requiere --office-id",
                error_type="apply_scope_office_required",
            )
        if document_number is not None:
            raise AnalyticsValidationError(
                "Apply scope no permite --document-number",
                error_type="apply_scope_forbidden",
            )
        has_barcode = bool(barcode and str(barcode).strip())
        has_variant = variant_id is not None
        if has_barcode == has_variant:
            raise AnalyticsValidationError(
                "Apply scope requiere --barcode o --variant-id, pero no ambos",
                error_type="apply_scope_selector_required",
            )
        if confirm_row_count is None:
            raise AnalyticsValidationError(
                "Apply scope requiere --confirm-row-count",
                error_type="apply_scope_confirm_required",
            )
        if int(confirm_row_count) <= 0:
            raise AnalyticsValidationError(
                "confirm-row-count debe ser > 0",
                error_type="apply_scope_confirm_invalid",
            )
        max_rows = int(max_apply_rows)
        if max_rows > MAX_APPLY_ROWS_CAP:
            raise AnalyticsValidationError(
                "Apply scope excede --max-apply-rows",
                error_type="apply_scope_max_rows",
                details={"max_apply_rows": max_rows, "cap": MAX_APPLY_ROWS_CAP},
            )
        if max_rows <= 0:
            raise AnalyticsValidationError(
                "max-apply-rows debe ser > 0",
                error_type="apply_scope_max_rows",
            )
        return CostV2BackfillArgs(
            company_id=int(company_id),
            office_id=int(office_id),
            date_from=date_from,
            date_to=date_to,
            dry_run=False,
            batch_size=max(1, min(int(batch_size), MAX_BATCH_SIZE)),
            sample_limit=max(1, min(int(sample_limit), MAX_SAMPLE_LIMIT)),
            statement_timeout_seconds=max(
                1, min(int(statement_timeout_seconds), MAX_TIMEOUT)
            ),
            calculation_version=version,
            history_id=None,
            variant_id=int(variant_id) if has_variant else None,
            barcode=(barcode.strip() if has_barcode else None),
            document_number=None,
            apply=True,
            confirm_history_id=None,
            apply_scope=True,
            confirm_row_count=int(confirm_row_count),
            max_apply_rows=max_rows,
        )

    if apply:
        if (
            history_id is None
            or confirm_history_id is None
            or int(history_id) != int(confirm_history_id)
        ):
            raise AnalyticsValidationError(
                "Apply canario requiere --history-id y --confirm-history-id iguales",
                error_type="apply_canary_confirmation_required",
            )
        if barcode:
            raise AnalyticsValidationError(
                "Apply canario no permite --barcode",
                error_type="apply_canary_scope_forbidden",
            )
        if variant_id is not None:
            raise AnalyticsValidationError(
                "Apply canario no permite --variant-id",
                error_type="apply_canary_scope_forbidden",
            )
        if document_number is not None:
            raise AnalyticsValidationError(
                "Apply canario no permite --document-number",
                error_type="apply_canary_scope_forbidden",
            )
        return CostV2BackfillArgs(
            company_id=int(company_id),
            office_id=int(office_id) if office_id is not None else None,
            date_from=date_from,
            date_to=date_to,
            dry_run=False,
            batch_size=max(1, min(int(batch_size), MAX_BATCH_SIZE)),
            sample_limit=max(1, min(int(sample_limit), MAX_SAMPLE_LIMIT)),
            statement_timeout_seconds=max(
                1, min(int(statement_timeout_seconds), MAX_TIMEOUT)
            ),
            calculation_version=version,
            history_id=int(history_id),
            variant_id=None,
            barcode=None,
            document_number=None,
            apply=True,
            confirm_history_id=int(confirm_history_id),
            apply_scope=False,
            confirm_row_count=None,
            max_apply_rows=DEFAULT_MAX_APPLY_ROWS,
        )

    if not dry_run:
        raise AnalyticsValidationError(
            "Solo dry-run está habilitado sin --apply (use --dry-run)",
            error_type="dry_run_required",
        )
    return CostV2BackfillArgs(
        company_id=int(company_id),
        office_id=int(office_id) if office_id is not None else None,
        date_from=date_from,
        date_to=date_to,
        dry_run=True,
        batch_size=max(1, min(int(batch_size), MAX_BATCH_SIZE)),
        sample_limit=max(1, min(int(sample_limit), MAX_SAMPLE_LIMIT)),
        statement_timeout_seconds=max(1, min(int(statement_timeout_seconds), MAX_TIMEOUT)),
        calculation_version=version,
        history_id=int(history_id) if history_id is not None else None,
        variant_id=int(variant_id) if variant_id is not None else None,
        barcode=(barcode.strip() if barcode else None) or None,
        document_number=int(document_number) if document_number is not None else None,
        apply=False,
        confirm_history_id=None,
        apply_scope=False,
        confirm_row_count=None,
        max_apply_rows=DEFAULT_MAX_APPLY_ROWS,
    )


def _abs(value: Decimal) -> Decimal:
    return value if value >= ZERO else -value


def build_variant_net_outlier_stats(
    net_rows: list[dict[str, Any]],
) -> dict[int, tuple[Decimal, int]]:
    """Mediana y conteo de cost_net > 0 por variant_id (batch, sin N+1)."""
    by_variant: dict[int, list[Decimal]] = defaultdict(list)
    for row in net_rows:
        vid = int(row["variant_id"])
        net = row.get("cost_net")
        if net is None:
            continue
        net_d = net if isinstance(net, Decimal) else Decimal(str(net))
        if net_d > ZERO:
            by_variant[vid].append(net_d)
    out: dict[int, tuple[Decimal, int]] = {}
    for vid, values in by_variant.items():
        med = Decimal(str(median([float(v) for v in values])))
        out[vid] = (med, len(values))
    return out


def is_suspicious_net_outlier(
    cost_net: Decimal | None,
    *,
    variant_median: Decimal | None,
    variant_count: int,
    factor: Decimal = OUTLIER_FACTOR,
    min_candidates: int = MIN_OUTLIER_CANDIDATES,
) -> bool:
    """Alerta externa: no corrige ni excluye; no cambia effective_quality_status."""
    if cost_net is None or cost_net <= ZERO:
        return False
    if variant_median is None or variant_median <= ZERO:
        return False
    if variant_count < min_candidates:
        return False
    return cost_net > variant_median * factor


def _sample_rank(
    calc: CostReceptionCalculation, meta: dict[str, Any]
) -> tuple[int, Decimal, int]:
    status = calc.effective_quality_status
    for i, key in enumerate(SAMPLE_PRIORITY):
        if status == key:
            diff = _abs(calc.gross_difference_amount or ZERO)
            return (i, -diff, calc.history_id)
    return (len(SAMPLE_PRIORITY), ZERO, calc.history_id)


def _sample_dict(calc: CostReceptionCalculation, meta: dict[str, Any]) -> dict[str, Any]:
    def _s(v: Decimal | None) -> str | None:
        return None if v is None else str(v)

    adm = calc.admission_date
    return {
        "history_id": calc.history_id,
        "product_name": meta.get("product_name"),
        "variant_name": meta.get("variant_name"),
        "barcode": meta.get("barcode"),
        "variant_id": calc.variant_id,
        "admission_date": adm.isoformat() if adm else None,
        "document_number": meta.get("document_number"),
        "stored_cost_net": _s(calc.stored_cost_net),
        "stored_iva_amount": _s(calc.stored_iva_amount),
        "stored_other_taxes": _s(calc.stored_other_taxes),
        "stored_gross_cost": _s(calc.stored_gross_cost),
        "catalog_tax_ids": list(calc.catalog_tax_ids),
        "resolved_tax_ids": list(calc.resolved_tax_ids),
        "calculated_iva_amount": _s(calc.calculated_iva_amount),
        "additional_tax_amount_total": _s(calc.additional_tax_amount_total),
        "total_tax_rate": _s(calc.total_tax_rate),
        "corrected_gross_cost": _s(calc.corrected_gross_cost),
        "gross_difference_amount": _s(calc.gross_difference_amount),
        "tax_rate_on_net_pct": _s(calc.tax_rate_on_net_pct),
        "gross_understatement_vs_corrected_pct": _s(
            calc.gross_understatement_vs_corrected_pct
        ),
        "tax_context_source": calc.tax_context_source,
        "tax_ids_source": calc.tax_ids_source,
        "tax_rates_source": calc.tax_rates_source,
        "tax_context_is_historical": calc.tax_context_is_historical,
        "tax_resolution_quality": calc.tax_resolution_quality,
        "effective_quality_status": calc.effective_quality_status,
        "warnings": list(calc.warnings),
        "source_history_fingerprint": calc.source_history_fingerprint,
        "tax_context_fingerprint": calc.tax_context_fingerprint,
        "calculation_result_fingerprint": calc.calculation_result_fingerprint,
    }


def run_cost_v2_backfill_dry_run(
    *,
    args: CostV2BackfillArgs,
    repository: CostV2BackfillRepository,
) -> dict[str, Any]:
    t0 = time.perf_counter()

    variant_ids: list[int] | None = None
    if args.barcode:
        variant_ids = repository.resolve_barcode_variant_ids(
            company_id=args.company_id, barcode=args.barcode
        )
        if args.variant_id is not None:
            variant_ids = [v for v in variant_ids if v == args.variant_id]
    elif args.variant_id is not None:
        variant_ids = [args.variant_id]

    population = repository.count_population(
        company_id=args.company_id,
        office_id=args.office_id,
        date_from=args.date_from,
        date_to=args.date_to,
        variant_ids=variant_ids,
        history_id=args.history_id,
        document_number=args.document_number,
    )

    # Variants del scope de salida → baseline independiente de filtros de salida.
    scope_variant_ids = repository.fetch_scope_variant_ids(
        company_id=args.company_id,
        office_id=args.office_id,
        date_from=args.date_from,
        date_to=args.date_to,
        variant_ids=variant_ids,
        history_id=args.history_id,
        document_number=args.document_number,
    )
    # Caché por corrida: una sola consulta batch (sin N+1, sin date/history/doc).
    net_rows = repository.fetch_outlier_baseline_cost_nets(
        company_id=args.company_id,
        office_id=args.office_id,
        variant_ids=scope_variant_ids,
    )
    outlier_stats = build_variant_net_outlier_stats(net_rows)

    results_count = {k: 0 for k in QUALITY_KEYS}
    tax_res_count = {"current_catalog": 0, "canonical_fallback": 0, "unresolved": 0}
    warning_count = {
        "suspicious_outlier": 0,
        "tax_ids_not_consumed": 0,
        "variant_barcode_mismatch": 0,
        "source_conflict": 0,
    }
    unknown_tax_ids: set[int] = set()
    diffs: list[Decimal] = []
    candidates: list[tuple[tuple[int, Decimal, int], dict[str, Any]]] = []

    after_id = 0
    batches = 0
    processed = 0
    all_tax_ids: set[int] = set()
    tax_catalog: dict[int, Any] = {}

    while True:
        batch = repository.fetch_history_batch(
            company_id=args.company_id,
            office_id=args.office_id,
            date_from=args.date_from,
            date_to=args.date_to,
            after_id=after_id,
            batch_size=args.batch_size,
            variant_ids=variant_ids,
            history_id=args.history_id,
            document_number=args.document_number,
        )
        if not batch:
            break
        batches += 1

        batch_ids: list[int] = []
        for row in batch:
            batch_ids.extend(row.get("catalog_tax_ids") or [])
        missing = [i for i in set(batch_ids) if i not in tax_catalog]
        if missing:
            fetched = repository.fetch_taxes_for_ids(
                company_id=args.company_id, tax_ids=missing
            )
            tax_catalog.update(fetched)

        for row in batch:
            catalog_ids = tuple(row.get("catalog_tax_ids") or [])
            all_tax_ids.update(catalog_ids)

            adm = row.get("admission_date")
            if isinstance(adm, datetime):
                adm_d = adm.date()
            else:
                adm_d = adm

            inp = CostReceptionInput(
                history_id=int(row["history_id"]),
                company_id=int(row["company_id"]),
                office_id=row.get("office_id"),
                variant_id=int(row["variant_id"]),
                admission_date=adm_d,
                stored_cost_net=row.get("cost_net"),
                stored_quantity=row.get("quantity"),
                stored_iva_amount=row.get("iva_amount"),
                stored_other_taxes=row.get("other_taxes"),
                stored_gross_cost=row.get("cost_bruto_erp"),
                reception_tax_ids=(),  # sin evidencia directa del payload
                catalog_tax_ids=catalog_ids,
                source_history_created_at=row.get("created_at"),
            )

            tax_ids_source = "current_product_tax" if catalog_ids else "unresolved"
            ctx = build_tax_context_from_ids(
                list(catalog_ids),
                tax_catalog=tax_catalog,
                tax_ids_source=tax_ids_source,
                context_is_historical=False,
                cost_net=inp.stored_cost_net,
            )

            for tid in catalog_ids:
                if tid not in tax_catalog and tid not in TAX_ID_FALLBACK:
                    unknown_tax_ids.add(tid)

            external_warnings: list[str] = []
            stats = outlier_stats.get(int(row["variant_id"]))
            if stats is not None:
                med, count = stats
                if is_suspicious_net_outlier(
                    inp.stored_cost_net,
                    variant_median=med,
                    variant_count=count,
                ):
                    external_warnings.append("suspicious_outlier")

            calc = calculate_cost_reception(
                inp,
                ctx,
                external_warnings=external_warnings,
                calculation_version=args.calculation_version,
            )
            processed += 1
            status = calc.effective_quality_status
            if status in results_count:
                results_count[status] += 1
            else:
                results_count["incomplete_tax_context"] += 1

            rq = calc.tax_resolution_quality
            if rq in tax_res_count:
                tax_res_count[rq] += 1
            else:
                tax_res_count["unresolved"] += 1

            for w in calc.warnings:
                if w in warning_count:
                    warning_count[w] += 1

            if calc.gross_difference_amount is not None and calc.gross_difference_amount != ZERO:
                diffs.append(calc.gross_difference_amount)

            sample = _sample_dict(calc, row)
            candidates.append((_sample_rank(calc, row), sample))

        after_id = int(batch[-1]["history_id"])
        if len(batch) < args.batch_size:
            break

    candidates.sort(key=lambda x: x[0])
    samples = [c[1] for c in candidates[: args.sample_limit]]

    unit_sum = sum(diffs, ZERO) if diffs else ZERO
    duration_ms = (time.perf_counter() - t0) * 1000.0

    status_sum = sum(results_count.values())
    if status_sum != processed:
        raise AnalyticsValidationError(
            f"suma de estados ({status_sum}) != rows_processed ({processed})",
            error_type="invariant_violation",
        )

    return {
        "ok": True,
        "mode": "dry-run",
        "read_only": True,
        "calculation_version": args.calculation_version,
        "scope": {
            "company_id": args.company_id,
            "office_id": args.office_id,
            "date_from": args.date_from.isoformat(),
            "date_to": args.date_to.isoformat(),
            "history_id": args.history_id,
            "variant_id": args.variant_id,
            "barcode": args.barcode,
            "document_number": args.document_number,
            "batch_size": args.batch_size,
            "sample_limit": args.sample_limit,
        },
        "population": {
            "rows_found": population["rows_found"],
            "rows_processed": processed,
            "batches": batches,
            "unique_variants": population["unique_variants"],
            "unique_documents": population["unique_documents"],
            "min_admission_date": population["min_admission_date"],
            "max_admission_date": population["max_admission_date"],
        },
        "results": {
            "would_insert": processed,
            "would_update_same_version": 0,
            "note": (
                "would_update_same_version=0 porque cost_reception_calculated "
                "aún no existe (migración 047 no ejecutada). "
                "Tras migrar, se comparará por (history_id, calculation_version)."
            ),
            **results_count,
        },
        "tax_resolution": {
            **tax_res_count,
            "unknown_tax_ids": sorted(unknown_tax_ids),
            "note": (
                "tax_ids_source (p.ej. current_product_tax) es ortogonal a "
                "tax_rates_source (bsale_taxes | canonical_fallback). "
                "tax_context_source es legacy/deprecado. "
                "context_is_historical=false; current_catalog no implica "
                "impuesto vigente en admission_date"
            ),
        },
        "differences": {
            "unit_difference_sum": str(unit_sum) if diffs else "0.00",
            "average_per_row": (
                str(unit_sum / Decimal(len(diffs))) if diffs else "0.00"
            ),
            "maximum_per_row": str(max(diffs)) if diffs else "0.00",
            "rows_with_difference": len(diffs),
            "warning": "No representa impacto total de compras",
        },
        "warnings": warning_count,
        "samples": samples,
        "duration_ms": round(duration_ms, 2),
        "limitations": [
            "Dry-run: no escribe cost_reception_calculated",
            "Apply canario: solo con --history-id + --confirm-history-id iguales (1 fila)",
            "No se usa quantity para totales ponderados",
            "No se consulta bsale.variant_cost",
            "reception_tax_ids vacío (sin evidencia de payload de línea)",
            "No se usa products.taxes[0] ni split_erp_cost",
            "tax_context_source es legacy; preferir tax_ids_source + tax_rates_source",
            (
                "document_number NULL se preserva; missing_document_number "
                "aún no forma parte del vocabulario de warnings V2"
            ),
            (
                f"outlier: mediana de cost_net por variant_id sobre historial disponible "
                f"(company_id+office_id+variants del scope); ignora date/history/barcode/"
                f"document del resultado; factor={OUTLIER_FACTOR}, min_n={MIN_OUTLIER_CANDIDATES}; "
                "warning analítico (no clasificación as-of admission_date)"
            ),
        ],
    }


def _dec_equal(a: Decimal | None, b: Decimal | None) -> bool:
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    if not isinstance(a, Decimal) or not isinstance(b, Decimal):
        raise TypeError("comparación monetaria requiere Decimal")
    return a == b


def _json_list_equal(a: Any, b: Any) -> bool:
    la = list(a or [])
    lb = list(b or [])
    return la == lb


def validate_calculation_before_persist(calc: CostReceptionCalculation) -> None:
    if not calc.history_id:
        raise AnalyticsValidationError(
            "history_id ausente", error_type="persist_validation"
        )
    if not calc.calculation_version:
        raise AnalyticsValidationError(
            "calculation_version ausente", error_type="persist_validation"
        )
    if not calc.source_history_fingerprint:
        raise AnalyticsValidationError(
            "source_history_fingerprint vacío", error_type="persist_validation"
        )
    if not calc.tax_context_fingerprint:
        raise AnalyticsValidationError(
            "tax_context_fingerprint vacío", error_type="persist_validation"
        )
    if not calc.calculation_result_fingerprint:
        raise AnalyticsValidationError(
            "calculation_result_fingerprint vacío", error_type="persist_validation"
        )
    if calc.effective_quality_status not in QUALITY_KEYS:
        raise AnalyticsValidationError(
            f"effective_quality_status inválido: {calc.effective_quality_status}",
            error_type="persist_validation",
        )
    if calc.tax_ids_source not in ALLOWED_TAX_IDS_SOURCES:
        raise AnalyticsValidationError(
            f"tax_ids_source inválido: {calc.tax_ids_source}",
            error_type="persist_validation",
        )
    if calc.tax_rates_source not in ALLOWED_TAX_RATES_SOURCES:
        raise AnalyticsValidationError(
            f"tax_rates_source inválido: {calc.tax_rates_source}",
            error_type="persist_validation",
        )
    if calc.tax_resolution_quality not in ALLOWED_RESOLUTION_QUALITIES:
        raise AnalyticsValidationError(
            f"tax_resolution_quality inválido: {calc.tax_resolution_quality}",
            error_type="persist_validation",
        )
    if calc.tax_context_source not in ALLOWED_CONTEXT_SOURCES:
        raise AnalyticsValidationError(
            f"tax_context_source inválido: {calc.tax_context_source}",
            error_type="persist_validation",
        )
    # warnings serializables
    for w in calc.warnings:
        if not isinstance(w, str):
            raise AnalyticsValidationError(
                "warnings deben ser str", error_type="persist_validation"
            )
    for v in (
        calc.stored_cost_net,
        calc.calculated_iva_amount,
        calc.corrected_gross_cost,
        calc.iva_rate,
        calc.total_tax_rate,
    ):
        if v is not None and not isinstance(v, Decimal):
            raise AnalyticsValidationError(
                "montos deben permanecer Decimal hasta SQL",
                error_type="persist_validation",
            )


def _assert_readback_matches(
    calc: CostReceptionCalculation,
    row: dict[str, Any],
    *,
    expect_batch_id: str | None,
) -> None:
    checks = [
        (int(row["history_id"]), calc.history_id, "history_id"),
        (str(row["calculation_version"]), calc.calculation_version, "calculation_version"),
        (row.get("tax_ids_source"), calc.tax_ids_source, "tax_ids_source"),
        (row.get("tax_rates_source"), calc.tax_rates_source, "tax_rates_source"),
        (
            row.get("tax_resolution_quality"),
            calc.tax_resolution_quality,
            "tax_resolution_quality",
        ),
        (
            row.get("effective_quality_status"),
            calc.effective_quality_status,
            "effective_quality_status",
        ),
        (
            row.get("source_history_fingerprint"),
            calc.source_history_fingerprint,
            "source_history_fingerprint",
        ),
        (
            row.get("tax_context_fingerprint"),
            calc.tax_context_fingerprint,
            "tax_context_fingerprint",
        ),
        (
            row.get("calculation_result_fingerprint"),
            calc.calculation_result_fingerprint,
            "calculation_result_fingerprint",
        ),
    ]
    for got, expected, name in checks:
        if got != expected:
            raise AnalyticsValidationError(
                f"readback mismatch {name}: {got!r} != {expected!r}",
                error_type="readback_mismatch",
            )
    if expect_batch_id is not None:
        if str(row.get("calculation_batch_id")) != str(expect_batch_id):
            raise AnalyticsValidationError(
                "readback mismatch calculation_batch_id",
                error_type="readback_mismatch",
            )
    money_pairs = [
        ("stored_cost_net", calc.stored_cost_net),
        ("calculated_iva_amount", calc.calculated_iva_amount),
        ("additional_tax_amount_total", calc.additional_tax_amount_total),
        ("total_tax_rate", calc.total_tax_rate),
        ("corrected_gross_cost", calc.corrected_gross_cost),
        ("gross_difference_amount", calc.gross_difference_amount),
        ("tax_rate_on_net_pct", calc.tax_rate_on_net_pct),
        (
            "gross_understatement_vs_corrected_pct",
            calc.gross_understatement_vs_corrected_pct,
        ),
    ]
    for key, expected in money_pairs:
        got = row.get(key)
        if got is not None and not isinstance(got, Decimal):
            raise AnalyticsValidationError(
                f"readback {key} no es Decimal",
                error_type="readback_mismatch",
            )
        if not _dec_equal(got, expected):
            raise AnalyticsValidationError(
                f"readback mismatch {key}: {got!r} != {expected!r}",
                error_type="readback_mismatch",
            )
    resolved = row.get("resolved_tax_ids_json") or []
    if sorted(int(x) for x in resolved) != sorted(calc.resolved_tax_ids):
        raise AnalyticsValidationError(
            "readback mismatch resolved_tax_ids_json",
            error_type="readback_mismatch",
        )
    warnings = row.get("warnings_json") or []
    if sorted(str(x) for x in warnings) != sorted(calc.warnings):
        raise AnalyticsValidationError(
            "readback mismatch warnings_json",
            error_type="readback_mismatch",
        )


def _calculate_single_row(
    *,
    row: dict[str, Any],
    tax_catalog: dict[int, Any],
    outlier_stats: dict[int, tuple[Decimal, int]],
    calculation_version: str,
) -> CostReceptionCalculation:
    catalog_ids = tuple(row.get("catalog_tax_ids") or [])
    adm = row.get("admission_date")
    if isinstance(adm, datetime):
        adm_d = adm.date()
    else:
        adm_d = adm
    inp = CostReceptionInput(
        history_id=int(row["history_id"]),
        company_id=int(row["company_id"]),
        office_id=row.get("office_id"),
        variant_id=int(row["variant_id"]),
        admission_date=adm_d,
        stored_cost_net=row.get("cost_net"),
        stored_quantity=row.get("quantity"),
        stored_iva_amount=row.get("iva_amount"),
        stored_other_taxes=row.get("other_taxes"),
        stored_gross_cost=row.get("cost_bruto_erp"),
        reception_tax_ids=(),
        catalog_tax_ids=catalog_ids,
        source_history_created_at=row.get("created_at"),
    )
    tax_ids_source = "current_product_tax" if catalog_ids else "unresolved"
    ctx = build_tax_context_from_ids(
        list(catalog_ids),
        tax_catalog=tax_catalog,
        tax_ids_source=tax_ids_source,
        context_is_historical=False,
        cost_net=inp.stored_cost_net,
    )
    external_warnings: list[str] = []
    stats = outlier_stats.get(int(row["variant_id"]))
    if stats is not None:
        med, count = stats
        if is_suspicious_net_outlier(
            inp.stored_cost_net,
            variant_median=med,
            variant_count=count,
        ):
            external_warnings.append("suspicious_outlier")
    return calculate_cost_reception(
        inp,
        ctx,
        external_warnings=external_warnings,
        calculation_version=calculation_version,
    )


def run_cost_v2_canary_apply(
    *,
    args: CostV2BackfillArgs,
    repository: CostV2BackfillRepository,
    commit_fn: Any,
    rollback_fn: Any,
) -> dict[str, Any]:
    """Persistencia canaria de exactamente una fila. Commit solo tras verificaciones."""
    if not args.apply or args.dry_run:
        raise AnalyticsValidationError(
            "run_cost_v2_canary_apply requiere apply=True",
            error_type="invalid_args",
        )
    batch_id = str(uuid.uuid4())
    try:
        if not repository.calculated_table_exists():
            raise AnalyticsValidationError(
                "Tabla analytics.cost_reception_calculated no existe",
                error_type="destination_table_missing",
            )

        population = repository.count_population(
            company_id=args.company_id,
            office_id=args.office_id,
            date_from=args.date_from,
            date_to=args.date_to,
            history_id=args.history_id,
        )
        if int(population["rows_found"]) != 1:
            raise AnalyticsValidationError(
                "Apply canario requiere exactamente una fila",
                error_type="apply_canary_row_count",
                details={"rows_found": population["rows_found"]},
            )

        batch = repository.fetch_history_batch(
            company_id=args.company_id,
            office_id=args.office_id,
            date_from=args.date_from,
            date_to=args.date_to,
            after_id=0,
            batch_size=2,
            history_id=args.history_id,
        )
        if len(batch) != 1:
            raise AnalyticsValidationError(
                "Apply canario requiere exactamente una fila",
                error_type="apply_canary_row_count",
                details={"rows_fetched": len(batch)},
            )
        row = batch[0]

        scope_variant_ids = [int(row["variant_id"])]
        net_rows = repository.fetch_outlier_baseline_cost_nets(
            company_id=args.company_id,
            office_id=args.office_id,
            variant_ids=scope_variant_ids,
        )
        outlier_stats = build_variant_net_outlier_stats(net_rows)

        catalog_ids = list(row.get("catalog_tax_ids") or [])
        tax_catalog = repository.fetch_taxes_for_ids(
            company_id=args.company_id, tax_ids=catalog_ids
        )
        calc = _calculate_single_row(
            row=row,
            tax_catalog=tax_catalog,
            outlier_stats=outlier_stats,
            calculation_version=args.calculation_version,
        )
        validate_calculation_before_persist(calc)

        existing = repository.get_existing_calculation(
            history_id=calc.history_id,
            calculation_version=calc.calculation_version,
        )
        persistence = {"inserted": 0, "updated": 0, "unchanged": 0}
        expect_batch: str | None = batch_id

        if existing is not None and (
            existing.get("calculation_result_fingerprint")
            == calc.calculation_result_fingerprint
            and existing.get("source_history_fingerprint")
            == calc.source_history_fingerprint
            and existing.get("tax_context_fingerprint") == calc.tax_context_fingerprint
        ):
            persistence["unchanged"] = 1
            expect_batch = str(existing.get("calculation_batch_id"))
            stored = existing
        else:
            upsert = repository.persist_calculation(
                calc=calc, calculation_batch_id=batch_id
            )
            if bool(upsert.get("was_inserted")):
                persistence["inserted"] = 1
            else:
                persistence["updated"] = 1
            stored = None

        readback = repository.read_calculation(
            history_id=calc.history_id,
            calculation_version=calc.calculation_version,
        )
        if readback is None:
            raise AnalyticsValidationError(
                "readback tabla vacío tras persistencia",
                error_type="readback_mismatch",
            )
        if persistence["unchanged"]:
            # no debió cambiar batch ni calculated_at
            if str(readback.get("calculation_batch_id")) != str(
                existing.get("calculation_batch_id")
            ):
                raise AnalyticsValidationError(
                    "unchanged alteró calculation_batch_id",
                    error_type="readback_mismatch",
                )
            if readback.get("calculated_at") != existing.get("calculated_at"):
                raise AnalyticsValidationError(
                    "unchanged alteró calculated_at",
                    error_type="readback_mismatch",
                )
        _assert_readback_matches(calc, readback, expect_batch_id=expect_batch)

        latest_rows = repository.read_latest_view(history_id=calc.history_id)
        if len(latest_rows) != 1:
            raise AnalyticsValidationError(
                "latest view no devolvió exactamente 1 fila",
                error_type="latest_view_mismatch",
            )
        _assert_readback_matches(
            calc, latest_rows[0], expect_batch_id=expect_batch
        )
        if latest_rows[0].get("calculation_version") != calc.calculation_version:
            raise AnalyticsValidationError(
                "latest view no coincide con calculation_version",
                error_type="latest_view_mismatch",
            )

        src = repository.verify_source_fingerprint_inputs(history_id=calc.history_id)
        if src is None:
            raise AnalyticsValidationError(
                "history fuente no encontrada en verificación",
                error_type="source_fingerprint_changed",
            )
        adm = src.get("admission_date")
        if isinstance(adm, datetime):
            adm_d = adm.date()
        else:
            adm_d = adm
        src_inp = CostReceptionInput(
            history_id=int(src["history_id"]),
            company_id=int(src["company_id"]),
            office_id=src.get("office_id"),
            variant_id=int(src["variant_id"]),
            admission_date=adm_d,
            stored_cost_net=src.get("cost_net"),
            stored_quantity=src.get("quantity"),
            stored_iva_amount=src.get("iva_amount"),
            stored_other_taxes=src.get("other_taxes"),
            stored_gross_cost=src.get("cost_bruto_erp"),
            reception_tax_ids=(),
            catalog_tax_ids=tuple(src.get("catalog_tax_ids") or []),
            source_history_created_at=src.get("created_at"),
        )
        if source_history_fingerprint(src_inp) != calc.source_history_fingerprint:
            raise AnalyticsValidationError(
                "source_history_fingerprint cambió antes del commit",
                error_type="source_fingerprint_changed",
            )

        if sum(persistence.values()) != 1:
            raise AnalyticsValidationError(
                "persistence counters inválidos",
                error_type="invariant_violation",
            )

        commit_fn()
        return {
            "ok": True,
            "mode": "apply-canary",
            "committed": True,
            "calculation_version": calc.calculation_version,
            "run_batch_id": batch_id,
            "calculation_batch_id": expect_batch if persistence["unchanged"] else batch_id,
            "scope": {
                "history_id": calc.history_id,
                "rows_found": 1,
                "rows_processed": 1,
            },
            "persistence": persistence,
            "verification": {
                "table_readback_ok": True,
                "latest_view_ok": True,
                "source_fingerprint_unchanged": True,
                "result_fingerprint_match": True,
            },
            "result": {
                "stored_cost_net": (
                    None if calc.stored_cost_net is None else str(calc.stored_cost_net)
                ),
                "corrected_gross_cost": (
                    None
                    if calc.corrected_gross_cost is None
                    else str(calc.corrected_gross_cost)
                ),
                "calculated_iva_amount": (
                    None
                    if calc.calculated_iva_amount is None
                    else str(calc.calculated_iva_amount)
                ),
                "effective_quality_status": calc.effective_quality_status,
                "warnings": list(calc.warnings),
            },
        }
    except Exception:
        try:
            rollback_fn()
        except Exception:
            pass
        raise


def _fingerprints_unchanged(existing: dict[str, Any], calc: CostReceptionCalculation) -> bool:
    return (
        existing.get("calculation_result_fingerprint")
        == calc.calculation_result_fingerprint
        and existing.get("source_history_fingerprint")
        == calc.source_history_fingerprint
        and existing.get("tax_context_fingerprint") == calc.tax_context_fingerprint
    )


def _fetch_all_scope_rows(
    *,
    repository: CostV2BackfillRepository,
    args: CostV2BackfillArgs,
    variant_ids: list[int],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    after_id = 0
    while True:
        batch = repository.fetch_history_batch(
            company_id=args.company_id,
            office_id=args.office_id,
            date_from=args.date_from,
            date_to=args.date_to,
            after_id=after_id,
            batch_size=args.batch_size,
            variant_ids=variant_ids,
        )
        if not batch:
            break
        rows.extend(batch)
        after_id = int(batch[-1]["history_id"])
        if len(batch) < args.batch_size:
            break
    rows.sort(key=lambda r: int(r["history_id"]))
    return rows


def _verify_source_fp(
    repository: CostV2BackfillRepository, calc: CostReceptionCalculation
) -> None:
    src = repository.verify_source_fingerprint_inputs(history_id=calc.history_id)
    if src is None:
        raise AnalyticsValidationError(
            "history fuente no encontrada en verificación",
            error_type="source_fingerprint_changed",
        )
    adm = src.get("admission_date")
    if isinstance(adm, datetime):
        adm_d = adm.date()
    else:
        adm_d = adm
    src_inp = CostReceptionInput(
        history_id=int(src["history_id"]),
        company_id=int(src["company_id"]),
        office_id=src.get("office_id"),
        variant_id=int(src["variant_id"]),
        admission_date=adm_d,
        stored_cost_net=src.get("cost_net"),
        stored_quantity=src.get("quantity"),
        stored_iva_amount=src.get("iva_amount"),
        stored_other_taxes=src.get("other_taxes"),
        stored_gross_cost=src.get("cost_bruto_erp"),
        reception_tax_ids=(),
        catalog_tax_ids=tuple(src.get("catalog_tax_ids") or []),
        source_history_created_at=src.get("created_at"),
    )
    if source_history_fingerprint(src_inp) != calc.source_history_fingerprint:
        raise AnalyticsValidationError(
            "source_history_fingerprint cambió antes del commit",
            error_type="source_fingerprint_changed",
        )


def run_cost_v2_scope_apply(
    *,
    args: CostV2BackfillArgs,
    repository: CostV2BackfillRepository,
    commit_fn: Any,
    rollback_fn: Any,
) -> dict[str, Any]:
    """Persistencia atómica de un lote acotado (barcode/variant + confirm-row-count)."""
    if not args.apply or not args.apply_scope or args.dry_run:
        raise AnalyticsValidationError(
            "run_cost_v2_scope_apply requiere apply-scope",
            error_type="invalid_args",
        )
    run_batch_id = str(uuid.uuid4())
    try:
        if not repository.calculated_table_exists():
            raise AnalyticsValidationError(
                "Tabla analytics.cost_reception_calculated no existe",
                error_type="destination_table_missing",
            )
        if not repository.calculated_latest_view_exists():
            raise AnalyticsValidationError(
                "Vista analytics.v_cost_reception_calculated_latest no existe",
                error_type="destination_view_missing",
            )

        variant_ids: list[int]
        if args.barcode:
            variant_ids = repository.resolve_barcode_variant_ids(
                company_id=args.company_id, barcode=args.barcode
            )
            if len(variant_ids) != 1:
                raise AnalyticsValidationError(
                    "Apply scope: barcode debe resolver exactamente una variante",
                    error_type="apply_scope_barcode_ambiguous",
                    details={"variants": variant_ids},
                )
        else:
            variant_ids = [int(args.variant_id)]  # type: ignore[arg-type]

        rows = _fetch_all_scope_rows(
            repository=repository, args=args, variant_ids=variant_ids
        )
        n = len(rows)
        if n == 0:
            raise AnalyticsValidationError(
                "Cantidad real no coincide con --confirm-row-count",
                error_type="apply_scope_row_count",
                details={"rows_found": 0, "confirm_row_count": args.confirm_row_count},
            )
        if n != int(args.confirm_row_count or -1):
            raise AnalyticsValidationError(
                "Cantidad real no coincide con --confirm-row-count",
                error_type="apply_scope_row_count",
                details={"rows_found": n, "confirm_row_count": args.confirm_row_count},
            )
        if n > int(args.max_apply_rows):
            raise AnalyticsValidationError(
                "Apply scope excede --max-apply-rows",
                error_type="apply_scope_max_rows",
                details={"rows_found": n, "max_apply_rows": args.max_apply_rows},
            )

        history_ids = [int(r["history_id"]) for r in rows]
        if len(set(history_ids)) != len(history_ids):
            raise AnalyticsValidationError(
                "history_id duplicados en el scope",
                error_type="apply_scope_duplicate_history",
            )
        # Ya ordenado ASC por _fetch_all_scope_rows
        assert history_ids == sorted(history_ids)

        outlier_stats = build_variant_net_outlier_stats(
            repository.fetch_outlier_baseline_cost_nets(
                company_id=args.company_id,
                office_id=args.office_id,
                variant_ids=variant_ids,
            )
        )

        all_tax_ids: list[int] = []
        for r in rows:
            all_tax_ids.extend(r.get("catalog_tax_ids") or [])
        tax_catalog = repository.fetch_taxes_for_ids(
            company_id=args.company_id, tax_ids=all_tax_ids
        )

        calcs: list[CostReceptionCalculation] = []
        for r in rows:
            calc = _calculate_single_row(
                row=r,
                tax_catalog=tax_catalog,
                outlier_stats=outlier_stats,
                calculation_version=args.calculation_version,
            )
            validate_calculation_before_persist(calc)
            calcs.append(calc)

        persistence = {"inserted": 0, "updated": 0, "unchanged": 0}
        prior_meta: dict[int, dict[str, Any]] = {}
        expect_batch_by_hid: dict[int, str] = {}

        for calc in calcs:
            existing = repository.get_existing_calculation(
                history_id=calc.history_id,
                calculation_version=calc.calculation_version,
            )
            if existing is not None and _fingerprints_unchanged(existing, calc):
                persistence["unchanged"] += 1
                prior_meta[calc.history_id] = {
                    "calculation_batch_id": str(existing.get("calculation_batch_id")),
                    "calculated_at": existing.get("calculated_at"),
                }
                expect_batch_by_hid[calc.history_id] = str(
                    existing.get("calculation_batch_id")
                )
            else:
                upsert = repository.persist_calculation(
                    calc=calc, calculation_batch_id=run_batch_id
                )
                if bool(upsert.get("was_inserted")):
                    persistence["inserted"] += 1
                else:
                    persistence["updated"] += 1
                expect_batch_by_hid[calc.history_id] = run_batch_id

        # Verificación post-escritura
        for calc in calcs:
            readback = repository.read_calculation(
                history_id=calc.history_id,
                calculation_version=calc.calculation_version,
            )
            if readback is None:
                raise AnalyticsValidationError(
                    "readback tabla vacío tras persistencia",
                    error_type="readback_mismatch",
                )
            if calc.history_id in prior_meta:
                meta = prior_meta[calc.history_id]
                if str(readback.get("calculation_batch_id")) != meta["calculation_batch_id"]:
                    raise AnalyticsValidationError(
                        "unchanged alteró calculation_batch_id",
                        error_type="readback_mismatch",
                    )
                if readback.get("calculated_at") != meta["calculated_at"]:
                    raise AnalyticsValidationError(
                        "unchanged alteró calculated_at",
                        error_type="readback_mismatch",
                    )
            _assert_readback_matches(
                calc,
                readback,
                expect_batch_id=expect_batch_by_hid[calc.history_id],
            )

            latest_rows = repository.read_latest_view(history_id=calc.history_id)
            if len(latest_rows) != 1:
                raise AnalyticsValidationError(
                    "latest view no devolvió exactamente 1 fila",
                    error_type="latest_view_mismatch",
                )
            _assert_readback_matches(
                calc,
                latest_rows[0],
                expect_batch_id=expect_batch_by_hid[calc.history_id],
            )
            _verify_source_fp(repository, calc)

        if sum(persistence.values()) != len(calcs):
            raise AnalyticsValidationError(
                "persistence counters != rows_processed",
                error_type="invariant_violation",
            )

        results_count = {k: 0 for k in QUALITY_KEYS}
        warning_count: dict[str, int] = defaultdict(int)
        for calc in calcs:
            if calc.effective_quality_status in results_count:
                results_count[calc.effective_quality_status] += 1
            for w in calc.warnings:
                warning_count[w] += 1

        commit_fn()
        return {
            "ok": True,
            "mode": "apply-scope-canary",
            "committed": True,
            "calculation_version": args.calculation_version,
            "run_batch_id": run_batch_id,
            "scope": {
                "company_id": args.company_id,
                "office_id": args.office_id,
                "barcode": args.barcode,
                "variant_id": args.variant_id,
                "rows_found": n,
                "rows_processed": n,
                "unique_variants": len(set(variant_ids)),
            },
            "persistence": dict(persistence),
            "verification": {
                "table_readback_ok": True,
                "latest_view_ok": True,
                "source_fingerprints_unchanged": True,
                "result_fingerprints_match": True,
                "scope_count_ok": True,
                "unchanged_metadata_preserved": True,
            },
            "results": {
                **results_count,
                "warnings": dict(warning_count),
            },
        }
    except Exception:
        try:
            rollback_fn()
        except Exception:
            pass
        raise
