"""Auditoría de calidad de costos (lógica pura + orquestación).

Fórmulas independientes del sync (no llama split_erp_cost / cost_gross_from_net).
"""

from __future__ import annotations

import time
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from statistics import median
from typing import Any

from backend.repositories.cost_data_audit_repo import CostDataAuditRepository, _tax_id_list
from backend.services.analytics.cost_audit_models import (
    QUALITY_COUNTER_KEYS,
    BarcodeResolution,
    CostAuditArgs,
    CostAuditFlag,
    CostAuditRawRow,
    CostAuditRowResult,
    CostAuditTolerances,
    TaxCatalogEntry,
    abs_decimal,
    clamp_cost_audit_args,
    rate_to_fraction,
)
from backend.services.analytics.money import ZERO
from backend.services.analytics.validate_distribuidora_source import (
    AnalyticsValidationError,
    assert_sql_is_read_only,
    make_psycopg_executor,
    open_readonly_connection,
)

__all__ = [
    "AnalyticsValidationError",
    "assert_sql_is_read_only",
    "clamp_cost_audit_args",
    "classify_cost_audit_row",
    "make_psycopg_executor",
    "open_readonly_connection",
    "run_cost_data_audit",
    "audit_date_window",
]


SAMPLE_PRIORITY: tuple[str, ...] = (
    CostAuditFlag.PROBABLE_MISSING_TAXES.value,
    CostAuditFlag.PROBABLE_IVA_DUPLICATED.value,
    CostAuditFlag.PROBABLE_SPECIFIC_TAX_DUPLICATED.value,
    CostAuditFlag.PROBABLE_TAX_FACTOR_DUPLICATED.value,
    CostAuditFlag.GROSS_MISMATCH.value,
    CostAuditFlag.UNIT_TOTAL_MISMATCH.value,
    CostAuditFlag.VARIANT_BARCODE_MISMATCH.value,
    CostAuditFlag.DUPLICATE_RECEPTION.value,
    CostAuditFlag.TAX_IDS_NOT_CONSUMED.value,
    CostAuditFlag.NEGATIVE_COST.value,
    CostAuditFlag.SUSPICIOUS_OUTLIER.value,
)


def audit_date_window(days: int, *, today: date | None = None) -> tuple[date, date]:
    if today is None:
        try:
            from zoneinfo import ZoneInfo

            today = datetime.now(ZoneInfo("America/Santiago")).date()
        except Exception:
            today = datetime.now(timezone.utc).date()
    date_to = today
    date_from = date_to - timedelta(days=max(1, int(days)) - 1)
    return date_from, date_to


def _near(a: Decimal, b: Decimal, tol: Decimal) -> bool:
    return abs_decimal(a - b) <= tol


def _rel_near(a: Decimal, b: Decimal, rel: Decimal) -> bool:
    if b == ZERO:
        return a == ZERO
    return abs_decimal(a - b) / abs_decimal(b) <= rel


def _extract_rates_from_products_taxes(
    products_taxes: Any,
) -> tuple[Decimal | None, Decimal | None]:
    if not isinstance(products_taxes, list) or not products_taxes:
        return None, None
    rates: list[Decimal] = []
    for item in products_taxes:
        if not isinstance(item, dict):
            continue
        raw = item.get("percentage")
        if raw is None:
            raw = item.get("rate")
        if raw is None:
            continue
        try:
            rates.append(Decimal(str(raw)))
        except Exception:
            continue
    if not rates:
        return None, None
    iva = rates[0]
    specific = sum(rates[1:], ZERO) if len(rates) > 1 else ZERO
    return iva, (specific if specific > ZERO else None)


def _rates_from_tax_ids(
    tax_ids_json: Any,
    tax_catalog: dict[int, TaxCatalogEntry],
) -> tuple[Decimal | None, Decimal | None]:
    ids = _tax_id_list(tax_ids_json)
    if not ids:
        return None, None
    rates: list[Decimal] = []
    for tid in ids:
        entry = tax_catalog.get(tid)
        if entry is None or entry.percentage is None:
            continue
        rates.append(entry.percentage)
    if not rates:
        return None, None
    iva = rates[0]
    specific = sum(rates[1:], ZERO) if len(rates) > 1 else ZERO
    return iva, (specific if specific > ZERO else None)


def _resolve_tax_inputs(
    raw: CostAuditRawRow,
    tax_catalog: dict[int, TaxCatalogEntry],
) -> tuple[Decimal | None, Decimal | None, Decimal | None, bool, bool]:
    """(tax_factor, iva_rate puntos, specific_rate puntos, rates_reliable, missing_context)."""
    tax_factor = raw.vc_tax_factor if raw.vc_tax_factor is not None else raw.product_tax_factor
    iva_rate = raw.vc_iva_rate
    specific_rate: Decimal | None = None

    pt_iva, pt_spec = _extract_rates_from_products_taxes(raw.products_taxes)
    if iva_rate is None:
        iva_rate = pt_iva
    if specific_rate is None:
        specific_rate = pt_spec

    id_iva, id_spec = _rates_from_tax_ids(raw.tax_ids_json, tax_catalog)
    if iva_rate is None:
        iva_rate = id_iva
    if specific_rate is None:
        specific_rate = id_spec

    has_any_tax_signal = bool(
        (tax_factor is not None and tax_factor > Decimal("1"))
        or (iva_rate is not None and iva_rate > ZERO)
        or (specific_rate is not None and specific_rate > ZERO)
        or (isinstance(raw.products_taxes, list) and len(raw.products_taxes) > 0)
        or bool(_tax_id_list(raw.tax_ids_json))
    )
    rates_reliable = iva_rate is not None or (
        tax_factor is not None and tax_factor > Decimal("1")
    )
    missing_context = not has_any_tax_signal
    return tax_factor, iva_rate, specific_rate, rates_reliable, missing_context


def _has_associated_taxes(raw: CostAuditRawRow, tax_factor: Decimal | None, iva_rate: Decimal | None) -> bool:
    if tax_factor is not None and tax_factor > Decimal("1"):
        return True
    if iva_rate is not None and iva_rate > ZERO:
        return True
    if isinstance(raw.products_taxes, list) and len(raw.products_taxes) > 0:
        return True
    if _tax_id_list(raw.tax_ids_json):
        return True
    return False


def _probable_causes(flags: list[str]) -> str | None:
    if not flags:
        return None
    mapping = {
        CostAuditFlag.PROBABLE_MISSING_TAXES.value: (
            "cost_bruto_erp ≈ cost_net pese a señales tributarias (posible fallback sin taxes)"
        ),
        CostAuditFlag.PROBABLE_IVA_DUPLICATED.value: (
            "bruto compatible con IVA aplicado dos veces sobre el neto"
        ),
        CostAuditFlag.PROBABLE_SPECIFIC_TAX_DUPLICATED.value: (
            "bruto compatible con impuesto específico duplicado"
        ),
        CostAuditFlag.PROBABLE_TAX_FACTOR_DUPLICATED.value: (
            "bruto compatible con tax_factor aplicado dos veces"
        ),
        CostAuditFlag.UNIT_TOTAL_MISMATCH.value: (
            "cost_net parece total de línea (≈ unitario × quantity) o mezcla unitario/total"
        ),
        CostAuditFlag.TAX_IDS_NOT_CONSUMED.value: (
            "tax_ids_json tiene impuestos pero products.taxes ausente/vacío"
        ),
        CostAuditFlag.GROSS_MISMATCH.value: (
            "cost_bruto_erp no cuadra con cost_net + iva_amount + other_taxes"
        ),
        CostAuditFlag.VARIANT_BARCODE_MISMATCH.value: (
            "barcode del historial no coincide con catálogo de la variante"
        ),
        CostAuditFlag.DUPLICATE_RECEPTION.value: "posible recepción/detalle duplicado",
        CostAuditFlag.STALE_SNAPSHOT.value: (
            "variant_cost.last_update desactualizado vs última recepción"
        ),
        CostAuditFlag.SOURCE_CONFLICT.value: (
            "average_cost de history difiere del snapshot variant_cost"
        ),
        CostAuditFlag.SUSPICIOUS_OUTLIER.value: (
            "costo bruto atípico vs mediana de la variante (solo alerta)"
        ),
        CostAuditFlag.MISSING_TAX_CONTEXT.value: "sin tax_factor/iva/taxes/tax_ids",
        CostAuditFlag.ZERO_COST.value: "costo neto o bruto en cero",
        CostAuditFlag.NEGATIVE_COST.value: "costo neto o bruto negativo",
    }
    for flag in SAMPLE_PRIORITY:
        if flag in flags:
            return mapping.get(flag)
    for flag in flags:
        if flag in mapping:
            return mapping[flag]
    return flags[0]


def classify_cost_audit_row(
    raw: CostAuditRawRow,
    *,
    tax_catalog: dict[int, TaxCatalogEntry] | None = None,
    tolerances: CostAuditTolerances | None = None,
    duplicate_unique_keys: set[str] | None = None,
    duplicate_detail_keys: set[tuple[int, int]] | None = None,
    variant_gross_values: list[Decimal] | None = None,
    as_of: date | None = None,
    latest_admission_by_variant: dict[int, date] | None = None,
) -> CostAuditRowResult:
    """Clasifica una fila. No muta `raw`. Permite múltiples flags."""
    tol = tolerances or CostAuditTolerances()
    catalog = tax_catalog or {}
    flags: list[str] = []

    tax_factor, iva_rate, specific_rate, rates_reliable, missing_context = _resolve_tax_inputs(
        raw, catalog
    )

    cost_net = raw.cost_net
    iva_amount = raw.iva_amount
    other_taxes = raw.other_taxes
    bruto = raw.cost_bruto_erp

    expected_from_amounts: Decimal | None = None
    diff_amounts: Decimal | None = None
    if cost_net is not None:
        expected_from_amounts = (
            cost_net
            + (iva_amount if iva_amount is not None else ZERO)
            + (other_taxes if other_taxes is not None else ZERO)
        )
        if bruto is not None:
            diff_amounts = bruto - expected_from_amounts

    expected_iva: Decimal | None = None
    expected_specific: Decimal | None = None
    expected_from_rates: Decimal | None = None
    diff_rates: Decimal | None = None

    iva_frac = rate_to_fraction(iva_rate)
    spec_frac = rate_to_fraction(specific_rate)

    if cost_net is not None and rates_reliable:
        if iva_frac is not None:
            expected_iva = cost_net * iva_frac
        if spec_frac is not None:
            expected_specific = cost_net * spec_frac
        elif tax_factor is not None and iva_frac is not None and tax_factor > Decimal("1"):
            # residual teórico del factor menos IVA (solo diagnóstico)
            residual = tax_factor - Decimal("1") - iva_frac
            if residual > ZERO:
                expected_specific = cost_net * residual
                specific_rate = residual * Decimal("100")
        if expected_iva is not None or expected_specific is not None:
            expected_from_rates = (
                cost_net
                + (expected_iva or ZERO)
                + (expected_specific or ZERO)
            )
        elif tax_factor is not None:
            expected_from_rates = cost_net * tax_factor
        if expected_from_rates is not None and bruto is not None:
            diff_rates = bruto - expected_from_rates

    # --- flags estructurales ---
    if cost_net is None:
        flags.append(CostAuditFlag.MISSING_NET_COST.value)
    if bruto is None:
        flags.append(CostAuditFlag.MISSING_GROSS_COST.value)

    if cost_net is not None and cost_net == ZERO:
        flags.append(CostAuditFlag.ZERO_COST.value)
    if bruto is not None and bruto == ZERO:
        if CostAuditFlag.ZERO_COST.value not in flags:
            flags.append(CostAuditFlag.ZERO_COST.value)

    if (cost_net is not None and cost_net < ZERO) or (bruto is not None and bruto < ZERO):
        flags.append(CostAuditFlag.NEGATIVE_COST.value)

    if raw.quantity is not None and raw.quantity == ZERO:
        flags.append(CostAuditFlag.QUANTITY_MISMATCH.value)

    if missing_context:
        flags.append(CostAuditFlag.MISSING_TAX_CONTEXT.value)

    tax_ids = _tax_id_list(raw.tax_ids_json)
    products_taxes_empty = not (
        isinstance(raw.products_taxes, list) and len(raw.products_taxes) > 0
    )
    if tax_ids and (not raw.has_products_taxes_column or products_taxes_empty):
        flags.append(CostAuditFlag.TAX_IDS_NOT_CONSUMED.value)

    # Identidad montos
    if cost_net is not None and bruto is not None and expected_from_amounts is not None and diff_amounts is not None:
        ad = abs_decimal(diff_amounts)
        if ad <= tol.money_exact:
            flags.append(CostAuditFlag.EXACT_MATCH.value)
        elif ad <= tol.money_rounding:
            flags.append(CostAuditFlag.ROUNDING_DIFFERENCE.value)
        else:
            soft = False
            if cost_net != ZERO and (ad / abs_decimal(cost_net) * Decimal("100")) <= tol.pct_soft:
                soft = True
            if soft:
                flags.append(CostAuditFlag.ROUNDING_DIFFERENCE.value)
            else:
                flags.append(CostAuditFlag.GROSS_MISMATCH.value)

    # Bruto == neto con impuestos asociados
    if (
        cost_net is not None
        and bruto is not None
        and _near(bruto, cost_net, tol.money_rounding)
        and _has_associated_taxes(raw, tax_factor, iva_rate)
    ):
        flags.append(CostAuditFlag.PROBABLE_MISSING_TAXES.value)

    # Tax mismatch vs tasas
    if (
        expected_from_rates is not None
        and bruto is not None
        and diff_rates is not None
        and abs_decimal(diff_rates) > tol.money_rounding
    ):
        if CostAuditFlag.TAX_MISMATCH.value not in flags:
            flags.append(CostAuditFlag.TAX_MISMATCH.value)

    # Duplicaciones probables
    if cost_net is not None and bruto is not None and cost_net > ZERO:
        if iva_frac is not None and iva_frac > ZERO:
            double_iva = cost_net * (Decimal("1") + iva_frac) * (Decimal("1") + iva_frac)
            additive_double = cost_net + (cost_net * iva_frac * Decimal("2"))
            if _rel_near(bruto, double_iva, tol.duplicate_iva_tolerance) or _rel_near(
                bruto, additive_double, tol.duplicate_iva_tolerance
            ):
                # Evitar falso positivo cuando tax_factor legitimo ≈ (1+iva)^2
                single = cost_net * (Decimal("1") + iva_frac)
                if not _rel_near(bruto, single, tol.duplicate_iva_tolerance):
                    flags.append(CostAuditFlag.PROBABLE_IVA_DUPLICATED.value)

        if spec_frac is not None and spec_frac > ZERO and iva_frac is not None:
            once = cost_net * (Decimal("1") + iva_frac + spec_frac)
            twice_spec = cost_net * (Decimal("1") + iva_frac + spec_frac * Decimal("2"))
            if _rel_near(bruto, twice_spec, tol.duplicate_iva_tolerance) and not _rel_near(
                bruto, once, tol.duplicate_iva_tolerance
            ):
                flags.append(CostAuditFlag.PROBABLE_SPECIFIC_TAX_DUPLICATED.value)

        if tax_factor is not None and tax_factor > Decimal("1"):
            once_tf = cost_net * tax_factor
            twice_tf = cost_net * tax_factor * tax_factor
            if _rel_near(bruto, twice_tf, tol.duplicate_iva_tolerance) and not _rel_near(
                bruto, once_tf, tol.duplicate_iva_tolerance
            ):
                flags.append(CostAuditFlag.PROBABLE_TAX_FACTOR_DUPLICATED.value)

    # Unitario vs total
    if (
        cost_net is not None
        and raw.quantity is not None
        and raw.quantity > Decimal("1")
        and raw.average_cost is not None
        and raw.average_cost > ZERO
    ):
        as_total = raw.average_cost * raw.quantity
        if _rel_near(cost_net, as_total, tol.unit_total_rel_tolerance):
            flags.append(CostAuditFlag.UNIT_TOTAL_MISMATCH.value)
    if (
        cost_net is not None
        and raw.quantity is not None
        and raw.quantity > Decimal("1")
        and raw.variant_cost_net is not None
        and raw.variant_cost_net > ZERO
    ):
        as_total2 = raw.variant_cost_net * raw.quantity
        if _rel_near(cost_net, as_total2, tol.unit_total_rel_tolerance):
            if CostAuditFlag.UNIT_TOTAL_MISMATCH.value not in flags:
                flags.append(CostAuditFlag.UNIT_TOTAL_MISMATCH.value)

    # Barcode mismatch
    hist_bc = (raw.barcode or "").strip()
    cat_bc = (raw.catalog_barcode or "").strip()
    if hist_bc and cat_bc and hist_bc != cat_bc:
        flags.append(CostAuditFlag.VARIANT_BARCODE_MISMATCH.value)

    # Duplicados
    if raw.unique_key and duplicate_unique_keys and raw.unique_key in duplicate_unique_keys:
        flags.append(CostAuditFlag.DUPLICATE_RECEPTION.value)
    if (
        raw.reception_detail_id is not None
        and duplicate_detail_keys
        and (raw.variant_id, raw.reception_detail_id) in duplicate_detail_keys
    ):
        if CostAuditFlag.DUPLICATE_RECEPTION.value not in flags:
            flags.append(CostAuditFlag.DUPLICATE_RECEPTION.value)
        flags.append(CostAuditFlag.DUPLICATE_VARIANT_LINK.value)

    # Stale snapshot
    as_of_d = as_of or date.today()
    last_adm = None
    if latest_admission_by_variant and raw.variant_id in latest_admission_by_variant:
        last_adm = latest_admission_by_variant[raw.variant_id]
    lu = raw.last_update
    lu_d = lu.date() if isinstance(lu, datetime) else lu if isinstance(lu, date) else None
    if lu_d is not None:
        age = (as_of_d - lu_d).days
        if age > tol.stale_snapshot_days:
            flags.append(CostAuditFlag.STALE_SNAPSHOT.value)
        if last_adm is not None and lu_d < last_adm and (last_adm - lu_d).days > 1:
            if CostAuditFlag.STALE_SNAPSHOT.value not in flags:
                flags.append(CostAuditFlag.STALE_SNAPSHOT.value)

    # Source conflict history average vs snapshot
    if (
        raw.average_cost is not None
        and raw.variant_cost_net is not None
        and raw.average_cost > ZERO
        and raw.variant_cost_net > ZERO
        and not _rel_near(raw.average_cost, raw.variant_cost_net, Decimal("0.05"))
    ):
        flags.append(CostAuditFlag.SOURCE_CONFLICT.value)

    # Outlier (alerta; no auto-error solo por ser alto)
    if (
        bruto is not None
        and bruto > ZERO
        and variant_gross_values
        and len(variant_gross_values) >= tol.min_candidates_for_outlier
    ):
        med = Decimal(str(median([float(x) for x in variant_gross_values])))
        if med > ZERO and bruto > med * tol.outlier_factor:
            flags.append(CostAuditFlag.SUSPICIOUS_OUTLIER.value)

    # Si no hay problemas y no marcamos exact/rounding, y no hay missing → exact si cuadra
    problem_flags = {
        CostAuditFlag.GROSS_MISMATCH.value,
        CostAuditFlag.TAX_MISMATCH.value,
        CostAuditFlag.PROBABLE_MISSING_TAXES.value,
        CostAuditFlag.PROBABLE_IVA_DUPLICATED.value,
        CostAuditFlag.PROBABLE_SPECIFIC_TAX_DUPLICATED.value,
        CostAuditFlag.PROBABLE_TAX_FACTOR_DUPLICATED.value,
        CostAuditFlag.UNIT_TOTAL_MISMATCH.value,
        CostAuditFlag.MISSING_NET_COST.value,
        CostAuditFlag.MISSING_GROSS_COST.value,
        CostAuditFlag.ZERO_COST.value,
        CostAuditFlag.NEGATIVE_COST.value,
    }
    if not any(f in problem_flags for f in flags):
        if CostAuditFlag.EXACT_MATCH.value not in flags and CostAuditFlag.ROUNDING_DIFFERENCE.value not in flags:
            if cost_net is not None and bruto is not None:
                flags.append(CostAuditFlag.EXACT_MATCH.value)

    return CostAuditRowResult(
        raw=raw,
        expected_gross_from_amounts=expected_from_amounts,
        expected_gross_from_rates=expected_from_rates,
        expected_iva_from_rate=expected_iva,
        expected_specific_tax_from_rate=expected_specific,
        gross_difference_amounts=diff_amounts,
        gross_difference_rates=diff_rates,
        tax_factor_used=tax_factor,
        iva_rate_used=iva_rate,
        specific_tax_rate_used=specific_rate,
        flags=flags,
        probable_cause=_probable_causes(flags),
    )


def _sample_rank(result: CostAuditRowResult) -> tuple[int, Decimal, int]:
    for i, key in enumerate(SAMPLE_PRIORITY):
        if key in result.flags:
            diff = abs_decimal(result.gross_difference_amounts or ZERO)
            return (i, -diff, result.raw.history_id)
    diff = abs_decimal(result.gross_difference_amounts or ZERO)
    return (len(SAMPLE_PRIORITY), -diff, result.raw.history_id)


def _is_problematic(result: CostAuditRowResult) -> bool:
    benign = {
        CostAuditFlag.EXACT_MATCH.value,
        CostAuditFlag.ROUNDING_DIFFERENCE.value,
    }
    return any(f not in benign for f in result.flags)


def build_cost_audit_report(
    *,
    args: CostAuditArgs,
    date_from: date,
    date_to: date,
    results: list[CostAuditRowResult],
    tax_context_stats: dict[str, int],
    duration_ms: float,
    barcode_resolution: Any | None = None,
) -> dict[str, Any]:
    quality = {k: 0 for k in QUALITY_COUNTER_KEYS}
    variants: set[int] = set()
    documents: set[int] = set()
    abs_diffs: list[Decimal] = []
    pct_diffs: list[Decimal] = []
    dates: list[date] = []

    for res in results:
        variants.add(res.raw.variant_id)
        if res.raw.source_document_id is not None:
            documents.add(res.raw.source_document_id)
        adm = res.raw.admission_date
        if isinstance(adm, datetime):
            dates.append(adm.date())
        elif isinstance(adm, date):
            dates.append(adm)
        counted: set[str] = set()
        for f in res.flags:
            if f in quality and f not in counted:
                quality[f] += 1
                counted.add(f)
        if res.gross_difference_amounts is not None:
            ad = abs_decimal(res.gross_difference_amounts)
            abs_diffs.append(ad)
            if res.raw.cost_net and res.raw.cost_net != ZERO:
                pct_diffs.append(ad / abs_decimal(res.raw.cost_net) * Decimal("100"))

    samples = sorted(
        [r for r in results if _is_problematic(r)],
        key=_sample_rank,
    )[: args.sample_limit]

    avg_abs = (
        str(sum(abs_diffs) / Decimal(len(abs_diffs))) if abs_diffs else None
    )
    max_abs = str(max(abs_diffs)) if abs_diffs else None
    avg_pct = (
        str(sum(pct_diffs) / Decimal(len(pct_diffs))) if pct_diffs else None
    )

    return {
        "ok": True,
        "read_only": True,
        "scope": {
            "company_id": args.company_id,
            "office_id": args.office_id,
            "date_from": date_from.isoformat(),
            "date_to": date_to.isoformat(),
            "variant_id": args.variant_id,
            "barcode": args.barcode,
            "source_document_id": args.source_document_id,
            "limit": args.limit,
        },
        "rows_analyzed": len(results),
        "unique_variants": len(variants),
        "unique_documents": len(documents),
        "date_range_found": {
            "min": min(dates).isoformat() if dates else None,
            "max": max(dates).isoformat() if dates else None,
        },
        "quality": quality,
        "tax_context": tax_context_stats,
        "differences": {
            "average_absolute": avg_abs,
            "maximum_absolute": max_abs,
            "average_percentage": avg_pct,
        },
        "samples": [s.to_sample_dict() for s in samples],
        "duration_ms": round(duration_ms, 2),
        "barcode_resolution": (
            barcode_resolution.to_dict()
            if barcode_resolution is not None and hasattr(barcode_resolution, "to_dict")
            else barcode_resolution
        ),
        "tolerances": {
            "money_rounding": str(args.tolerances.money_rounding),
            "money_exact": str(args.tolerances.money_exact),
            "pct_soft": str(args.tolerances.pct_soft),
            "stale_snapshot_days": args.tolerances.stale_snapshot_days,
            "outlier_factor": str(args.tolerances.outlier_factor),
        },
        "limitations": [
            "source_document_id no es columna canónica; se usa document_number o reception_id",
            "cost_bruto_erp del sync es sintético (net×tax_factor), no bruto de documento Bsale",
            "iva_rate interpretado en puntos porcentuales si > 1",
            "net_unit_value/total_unit_value no existen en cost_reception_history",
            "outlier es alerta estadística, no corrección",
        ],
    }


def run_cost_data_audit(
    *,
    args: CostAuditArgs,
    repository: CostDataAuditRepository,
    today: date | None = None,
) -> dict[str, Any]:
    t0 = time.perf_counter()
    date_from, date_to = audit_date_window(args.days, today=today)

    barcode_resolution = None
    variant_ids_filter: list[int] | None = None
    if args.barcode:
        barcode_resolution = repository.resolve_barcode_to_variant_ids(
            company_id=args.company_id,
            barcode=args.barcode,
        )
        variant_ids_filter = list(barcode_resolution.resolved_variant_ids)
        # Si además hay --variant-id, intersecar
        if args.variant_id is not None:
            variant_ids_filter = [v for v in variant_ids_filter if v == args.variant_id]

    raw_rows = repository.fetch_history_rows(
        args,
        date_from=date_from,
        date_to=date_to,
        variant_ids=variant_ids_filter,
    )

    if barcode_resolution is not None:
        warnings = list(barcode_resolution.warnings)
        if (
            barcode_resolution.resolved_variant_ids
            and not raw_rows
            and not barcode_resolution.barcode_not_found
        ):
            if "no_reception_history" not in warnings:
                # Puede ser filtro office/fecha; distinguir
                if barcode_resolution.no_reception_history:
                    pass
                else:
                    warnings.append("no_history_in_scope")
        barcode_resolution = BarcodeResolution(
            requested_barcode=barcode_resolution.requested_barcode,
            normalized_barcode=barcode_resolution.normalized_barcode,
            catalog_matches=barcode_resolution.catalog_matches,
            resolved_variant_ids=barcode_resolution.resolved_variant_ids,
            resolution_source=barcode_resolution.resolution_source,
            duplicate_mapping=barcode_resolution.duplicate_mapping,
            history_rows_found=len(raw_rows),
            barcode_not_found=barcode_resolution.barcode_not_found,
            no_reception_history=barcode_resolution.no_reception_history
            or (
                bool(barcode_resolution.resolved_variant_ids)
                and barcode_resolution.resolution_source
                == "bsale.variants.bar_code"
            ),
            warnings=tuple(dict.fromkeys(warnings)),
            match_details=barcode_resolution.match_details,
        )

    tax_ids = CostDataAuditRepository.collect_tax_ids(raw_rows)
    tax_catalog = repository.fetch_taxes_for_ids(
        company_id=args.company_id,
        tax_ids=tax_ids,
    )

    # Duplicados dentro del batch
    uk_counts: Counter[str] = Counter()
    detail_counts: Counter[tuple[int, int]] = Counter()
    for r in raw_rows:
        if r.unique_key:
            uk_counts[r.unique_key] += 1
        if r.reception_detail_id is not None:
            detail_counts[(r.variant_id, r.reception_detail_id)] += 1
    dup_uks = {k for k, c in uk_counts.items() if c > 1}
    dup_details = {k for k, c in detail_counts.items() if c > 1}

    gross_by_variant: dict[int, list[Decimal]] = defaultdict(list)
    latest_adm: dict[int, date] = {}
    for r in raw_rows:
        if r.cost_bruto_erp is not None and r.cost_bruto_erp > ZERO:
            gross_by_variant[r.variant_id].append(r.cost_bruto_erp)
        adm = r.admission_date
        adm_d = adm.date() if isinstance(adm, datetime) else adm if isinstance(adm, date) else None
        if adm_d is not None:
            prev = latest_adm.get(r.variant_id)
            if prev is None or adm_d > prev:
                latest_adm[r.variant_id] = adm_d

    as_of = today or date_to
    results: list[CostAuditRowResult] = []
    tax_stats = {
        "with_tax_ids_json": 0,
        "with_products_taxes": 0,
        "with_tax_factor": 0,
        "tax_ids_without_products_taxes": 0,
    }
    for r in raw_rows:
        if _tax_id_list(r.tax_ids_json):
            tax_stats["with_tax_ids_json"] += 1
        if isinstance(r.products_taxes, list) and r.products_taxes:
            tax_stats["with_products_taxes"] += 1
        elif _tax_id_list(r.tax_ids_json) and (
            not r.has_products_taxes_column
            or not (isinstance(r.products_taxes, list) and r.products_taxes)
        ):
            tax_stats["tax_ids_without_products_taxes"] += 1
        tf = r.vc_tax_factor if r.vc_tax_factor is not None else r.product_tax_factor
        if tf is not None and tf > Decimal("1"):
            tax_stats["with_tax_factor"] += 1

        results.append(
            classify_cost_audit_row(
                r,
                tax_catalog=tax_catalog,
                tolerances=args.tolerances,
                duplicate_unique_keys=dup_uks,
                duplicate_detail_keys=dup_details,
                variant_gross_values=gross_by_variant.get(r.variant_id),
                as_of=as_of,
                latest_admission_by_variant=latest_adm,
            )
        )

    duration_ms = (time.perf_counter() - t0) * 1000.0
    return build_cost_audit_report(
        args=args,
        date_from=date_from,
        date_to=date_to,
        results=results,
        tax_context_stats=tax_stats,
        duration_ms=duration_ms,
        barcode_resolution=barcode_resolution,
    )
