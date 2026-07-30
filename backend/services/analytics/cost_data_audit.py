"""Auditoría de calidad de costos (lógica pura + orquestación).

Fórmulas independientes del sync (no llama split_erp_cost / cost_gross_from_net).
"""

from __future__ import annotations

import time
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from statistics import median
from typing import Any

from backend.repositories.cost_data_audit_repo import CostDataAuditRepository, _tax_id_list
from backend.services.analytics.cost_audit_models import (
    EFFECTIVE_COUNTER_KEYS,
    EFFECTIVE_STATUS_PRIORITY,
    QUALITY_COUNTER_KEYS,
    BarcodeResolution,
    CostAuditArgs,
    CostAuditFlag,
    CostAuditRawRow,
    CostAuditRowResult,
    CostAuditTolerances,
    EffectiveQualityStatus,
    TaxCatalogEntry,
    abs_decimal,
    clamp_cost_audit_args,
    rate_to_fraction,
)
from backend.services.analytics.cost_tax_resolution import (
    ResolvedSpecificTax,
    TaxResolution,
    resolve_taxes_from_ids,
    tax_ids_fingerprint,
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
    EffectiveQualityStatus.MISSING_TAXES_IN_GROSS.value,
    EffectiveQualityStatus.DUPLICATED_TAXES_IN_GROSS.value,
    EffectiveQualityStatus.GROSS_COMPONENT_MISMATCH.value,
    CostAuditFlag.PROBABLE_IVA_DUPLICATED.value,
    CostAuditFlag.PROBABLE_MISSING_TAXES.value,
    CostAuditFlag.EXPECTED_TAX_MISMATCH.value,
    CostAuditFlag.STORED_COMPONENTS_MISMATCH.value,
    CostAuditFlag.UNIT_TOTAL_MISMATCH.value,
    CostAuditFlag.VARIANT_BARCODE_MISMATCH.value,
    CostAuditFlag.DUPLICATE_RECEPTION.value,
    CostAuditFlag.TAX_IDS_NOT_CONSUMED.value,
    CostAuditFlag.NEGATIVE_COST.value,
    CostAuditFlag.SUSPICIOUS_OUTLIER.value,
)

COMMERCIAL_QUANT = Decimal("0.01")


def _commercial(value: Decimal) -> Decimal:
    return value.quantize(COMMERCIAL_QUANT, rounding=ROUND_HALF_UP)


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


def _products_taxes_to_ids(products_taxes: Any) -> list[int] | None:
    """Extrae tax_ids de products.taxes si traen id; None si no hay ids."""
    if not isinstance(products_taxes, list) or not products_taxes:
        return None
    ids: list[int] = []
    saw_id = False
    for item in products_taxes:
        if not isinstance(item, dict):
            continue
        raw_id = item.get("id")
        if raw_id is None:
            raw_id = item.get("tax_id")
        if raw_id is None:
            continue
        saw_id = True
        try:
            ids.append(int(raw_id))
        except (TypeError, ValueError):
            continue
    return ids if saw_id else None


def _enrich_resolution_with_tax_factor(
    resolution: TaxResolution,
    tax_factor: Decimal | None,
) -> TaxResolution:
    """Si tax_factor implica tasa específica adicional no cubierta por tax_ids, sumarla."""
    if tax_factor is None or tax_factor <= Decimal("1"):
        return resolution
    if resolution.total_tax_rate is None and resolution.iva_rate is None:
        return resolution
    known = (resolution.iva_rate or ZERO) + (resolution.specific_tax_total_rate or ZERO)
    implied = (tax_factor - Decimal("1")) * Decimal("100")
    residual = implied - known
    if residual <= Decimal("0.05"):
        return resolution
    from backend.services.analytics.cost_tax_resolution import ResolvedSpecificTax

    extras = list(resolution.specific_taxes)
    extras.append(
        ResolvedSpecificTax(
            tax_id=-1,
            name="tax_factor_residual",
            rate=residual,
            category="specific_other",
        )
    )
    spec_total = (resolution.specific_tax_total_rate or ZERO) + residual
    total = (resolution.iva_rate or ZERO) + spec_total
    quality = resolution.tax_resolution_quality
    if quality == "unavailable":
        quality = "partial"
    return TaxResolution(
        iva_tax_id=resolution.iva_tax_id,
        iva_rate=resolution.iva_rate,
        specific_taxes=tuple(extras),
        specific_tax_total_rate=spec_total,
        total_tax_rate=total,
        tax_resolution_source=(
            f"{resolution.tax_resolution_source}+tax_factor"
            if resolution.tax_resolution_source
            else "tax_factor"
        ),
        tax_resolution_quality=quality,
        unresolved_tax_ids=resolution.unresolved_tax_ids,
    )


def _resolve_tax_profile(
    raw: CostAuditRawRow,
    tax_catalog: dict[int, TaxCatalogEntry],
) -> tuple[TaxResolution, Decimal | None]:
    """Resuelve perfil por identidad (tax_id). Nunca asume taxes[0]=IVA.

    Retorna (TaxResolution, tax_factor_almacenado).
    Prioridad: tax_ids_json → products.taxes con ids → vc_iva_rate / tax_factor.
    """
    tax_factor = raw.vc_tax_factor if raw.vc_tax_factor is not None else raw.product_tax_factor
    cost_net = raw.cost_net

    tax_ids = _tax_id_list(raw.tax_ids_json)
    if tax_ids:
        res = resolve_taxes_from_ids(
            tax_ids, tax_catalog=tax_catalog, cost_net=cost_net
        )
        return _enrich_resolution_with_tax_factor(res, tax_factor), tax_factor

    pt_ids = _products_taxes_to_ids(raw.products_taxes)
    if pt_ids:
        res = resolve_taxes_from_ids(
            pt_ids, tax_catalog=tax_catalog, cost_net=cost_net
        )
        return _enrich_resolution_with_tax_factor(res, tax_factor), tax_factor

    # Sin tax_ids: usar tasas snapshot (vc) — no inventar orden desde percentages
    iva_rate = raw.vc_iva_rate
    specific_rate: Decimal | None = None
    if isinstance(raw.specific_taxes, list) and raw.specific_taxes:
        total_spec = ZERO
        for item in raw.specific_taxes:
            if not isinstance(item, dict):
                continue
            raw_pct = item.get("percentage")
            if raw_pct is None:
                raw_pct = item.get("rate")
            if raw_pct is None:
                continue
            try:
                pct = Decimal(str(raw_pct))
            except Exception:
                continue
            name = str(item.get("name") or "").lower()
            if name.startswith("iva") or (iva_rate is not None and pct == iva_rate):
                continue
            total_spec += pct
        if total_spec > ZERO:
            specific_rate = total_spec

    if iva_rate is not None or (tax_factor is not None and tax_factor > Decimal("1")):
        if iva_rate is None and tax_factor is not None and tax_factor > Decimal("1"):
            total = (tax_factor - Decimal("1")) * Decimal("100")
            return (
                TaxResolution(
                    iva_tax_id=None,
                    iva_rate=None,
                    specific_taxes=(),
                    specific_tax_total_rate=None,
                    total_tax_rate=total,
                    tax_resolution_source="tax_factor_only",
                    tax_resolution_quality="partial",
                ),
                tax_factor,
            )
        spec = specific_rate or ZERO
        if (
            specific_rate is None
            and tax_factor is not None
            and tax_factor > Decimal("1")
            and iva_rate is not None
        ):
            iva_frac = rate_to_fraction(iva_rate) or ZERO
            residual = tax_factor - Decimal("1") - iva_frac
            if residual > ZERO:
                spec = residual * Decimal("100")
        total = (iva_rate or ZERO) + (spec if spec else ZERO)
        return (
            TaxResolution(
                iva_tax_id=1 if iva_rate is not None else None,
                iva_rate=iva_rate,
                specific_taxes=(),
                specific_tax_total_rate=spec if spec else ZERO,
                total_tax_rate=total,
                tax_resolution_source="variant_cost_rates",
                tax_resolution_quality="resolved" if iva_rate is not None else "partial",
            ),
            tax_factor,
        )

    return (
        TaxResolution(
            iva_tax_id=None,
            iva_rate=None,
            specific_taxes=(),
            specific_tax_total_rate=None,
            total_tax_rate=None,
            tax_resolution_source=None,
            tax_resolution_quality="unavailable",
        ),
        tax_factor,
    )


def _has_associated_taxes(
    raw: CostAuditRawRow,
    tax_factor: Decimal | None,
    resolution: TaxResolution,
) -> bool:
    if tax_factor is not None and tax_factor > Decimal("1"):
        return True
    if resolution.iva_rate is not None and resolution.iva_rate > ZERO:
        return True
    if resolution.specific_tax_total_rate and resolution.specific_tax_total_rate > ZERO:
        return True
    if isinstance(raw.products_taxes, list) and len(raw.products_taxes) > 0:
        return True
    if _tax_id_list(raw.tax_ids_json):
        return True
    return False


def _probable_causes(flags: list[str], effective: str | None) -> str | None:
    if effective == EffectiveQualityStatus.MISSING_TAXES_IN_GROSS.value:
        return (
            "componentes almacenados coherentes entre sí, pero el bruto omite impuestos "
            "esperados según iva_rate/tax_ids (p.ej. IVA 19% no aplicado)"
        )
    if effective == EffectiveQualityStatus.DUPLICATED_TAXES_IN_GROSS.value:
        return "bruto compatible con impuestos aplicados más de una vez"
    if effective == EffectiveQualityStatus.GROSS_COMPONENT_MISMATCH.value:
        return "cost_bruto_erp no cuadra con cost_net + iva_amount + other_taxes almacenados"
    if effective == EffectiveQualityStatus.INCOMPLETE_TAX_CONTEXT.value:
        return "hay señales tributarias incompletas (tax_ids sin rates/products.taxes)"
    if effective == EffectiveQualityStatus.MISSING_COST.value:
        return "falta costo neto o bruto, o es cero/negativo"
    if effective == EffectiveQualityStatus.SUSPICIOUS_OUTLIER.value:
        return "costo bruto atípico vs mediana de la variante (alerta)"
    if effective == EffectiveQualityStatus.VALID_GROSS.value:
        return "bruto alineado con componentes y perfil tributario esperado"
    mapping = {
        CostAuditFlag.PROBABLE_MISSING_TAXES.value: (
            "cost_bruto_erp ≈ cost_net pese a señales tributarias"
        ),
        CostAuditFlag.TAX_IDS_NOT_CONSUMED.value: (
            "tax_ids_json tiene impuestos pero products.taxes ausente/vacío"
        ),
        CostAuditFlag.VARIANT_BARCODE_MISMATCH.value: (
            "barcode del historial no coincide con catálogo de la variante"
        ),
        CostAuditFlag.DUPLICATE_RECEPTION.value: "posible recepción/detalle duplicado",
        CostAuditFlag.STALE_SNAPSHOT.value: "variant_cost desactualizado vs recepción",
        CostAuditFlag.SOURCE_CONFLICT.value: "average_cost history vs variant_cost conflictivo",
    }
    for flag in SAMPLE_PRIORITY:
        if flag in flags and flag in mapping:
            return mapping[flag]
    for flag in flags:
        if flag in mapping:
            return mapping[flag]
    return flags[0] if flags else None


def resolve_effective_quality_status(
    flags: list[str],
    *,
    tax_resolution_quality: str | None = None,
) -> str:
    """Un solo estado efectivo.

    ``expected_tax_unavailable`` / ``missing_tax_context`` → incomplete_tax_context.
    Nunca ``valid_gross`` sin perfil tributario resuelto y bruto esperado alineado.
    """
    candidates: list[str] = []
    if (
        CostAuditFlag.MISSING_NET_COST.value in flags
        or CostAuditFlag.MISSING_GROSS_COST.value in flags
        or CostAuditFlag.ZERO_COST.value in flags
        or CostAuditFlag.NEGATIVE_COST.value in flags
    ):
        candidates.append(EffectiveQualityStatus.MISSING_COST.value)
    if CostAuditFlag.STORED_COMPONENTS_MISMATCH.value in flags:
        candidates.append(EffectiveQualityStatus.GROSS_COMPONENT_MISMATCH.value)
    if (
        CostAuditFlag.PROBABLE_IVA_DUPLICATED.value in flags
        or CostAuditFlag.PROBABLE_SPECIFIC_TAX_DUPLICATED.value in flags
        or CostAuditFlag.PROBABLE_TAX_FACTOR_DUPLICATED.value in flags
    ):
        candidates.append(EffectiveQualityStatus.DUPLICATED_TAXES_IN_GROSS.value)
    if (
        CostAuditFlag.PROBABLE_MISSING_TAXES.value in flags
        or CostAuditFlag.EXPECTED_TAX_MISMATCH.value in flags
    ):
        # Sin perfil esperado no afirmar "missing taxes in gross"
        if CostAuditFlag.EXPECTED_TAX_UNAVAILABLE.value not in flags:
            candidates.append(EffectiveQualityStatus.MISSING_TAXES_IN_GROSS.value)

    unavailable = CostAuditFlag.EXPECTED_TAX_UNAVAILABLE.value in flags
    missing_ctx = CostAuditFlag.MISSING_TAX_CONTEXT.value in flags
    if unavailable or missing_ctx or tax_resolution_quality in (None, "unavailable"):
        candidates.append(EffectiveQualityStatus.INCOMPLETE_TAX_CONTEXT.value)

    if CostAuditFlag.SUSPICIOUS_OUTLIER.value in flags:
        candidates.append(EffectiveQualityStatus.SUSPICIOUS_OUTLIER.value)

    if not candidates:
        profile_ok = tax_resolution_quality == "resolved"
        stored_ok = (
            CostAuditFlag.STORED_COMPONENTS_MATCH.value in flags
            or CostAuditFlag.STORED_COMPONENTS_ROUNDING.value in flags
        )
        expected_ok = (
            CostAuditFlag.EXPECTED_TAX_MATCH.value in flags
            or CostAuditFlag.EXPECTED_TAX_ROUNDING.value in flags
        )
        if (
            profile_ok
            and stored_ok
            and expected_ok
            and CostAuditFlag.MISSING_NET_COST.value not in flags
            and CostAuditFlag.MISSING_GROSS_COST.value not in flags
        ):
            return EffectiveQualityStatus.VALID_GROSS.value
        return EffectiveQualityStatus.INCOMPLETE_TAX_CONTEXT.value

    for status in EFFECTIVE_STATUS_PRIORITY:
        if status in candidates:
            return status
    return candidates[0]


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
    """Clasifica una fila. No muta `raw`. Permite múltiples flags técnicos."""
    tol = tolerances or CostAuditTolerances()
    catalog = tax_catalog or {}
    flags: list[str] = []

    resolution, tax_factor = _resolve_tax_profile(raw, catalog)
    # Completar amounts en specifics
    if raw.cost_net is not None and resolution.specific_taxes:
        resolution = TaxResolution(
            iva_tax_id=resolution.iva_tax_id,
            iva_rate=resolution.iva_rate,
            specific_taxes=resolution.specifics_with_amounts(raw.cost_net),
            specific_tax_total_rate=resolution.specific_tax_total_rate,
            total_tax_rate=resolution.total_tax_rate,
            tax_resolution_source=resolution.tax_resolution_source,
            tax_resolution_quality=resolution.tax_resolution_quality,
            unresolved_tax_ids=resolution.unresolved_tax_ids,
        )

    iva_rate = resolution.iva_rate
    specific_rate = resolution.specific_tax_total_rate
    rates_reliable = resolution.tax_resolution_quality in ("resolved", "partial") and (
        resolution.total_tax_rate is not None
    )
    missing_context = resolution.tax_resolution_quality == "unavailable" and not (
        (tax_factor is not None and tax_factor > Decimal("1"))
        or bool(_tax_id_list(raw.tax_ids_json))
        or (isinstance(raw.products_taxes, list) and len(raw.products_taxes) > 0)
    )

    cost_net = raw.cost_net
    iva_amount = raw.iva_amount
    other_taxes = raw.other_taxes
    bruto = raw.cost_bruto_erp
    stored_gross = bruto

    # --- A) stored components ---
    expected_from_amounts: Decimal | None = None
    diff_amounts: Decimal | None = None
    stored_status: str | None = None
    if cost_net is not None:
        expected_from_amounts = _commercial(
            cost_net
            + (iva_amount if iva_amount is not None else ZERO)
            + (other_taxes if other_taxes is not None else ZERO)
        )
        if bruto is not None:
            diff_amounts = _commercial(bruto - expected_from_amounts)
            ad = abs_decimal(diff_amounts)
            if ad <= tol.money_exact:
                stored_status = CostAuditFlag.STORED_COMPONENTS_MATCH.value
            elif ad <= tol.money_rounding:
                stored_status = CostAuditFlag.STORED_COMPONENTS_ROUNDING.value
            else:
                soft = (
                    cost_net != ZERO
                    and (ad / abs_decimal(cost_net) * Decimal("100")) <= tol.pct_soft
                )
                stored_status = (
                    CostAuditFlag.STORED_COMPONENTS_ROUNDING.value
                    if soft
                    else CostAuditFlag.STORED_COMPONENTS_MISMATCH.value
                )
            flags.append(stored_status)

    # --- B) expected tax profile (por identidad) ---
    expected_iva: Decimal | None = None
    expected_specific: Decimal | None = None
    expected_from_rates: Decimal | None = None
    diff_rates: Decimal | None = None
    expected_tax_status: str | None = None

    iva_frac = rate_to_fraction(iva_rate)
    spec_frac = rate_to_fraction(specific_rate) if specific_rate else None

    if cost_net is not None and rates_reliable and resolution.total_tax_rate is not None:
        expected_iva = resolution.expected_iva_amount(cost_net)
        expected_specific = resolution.expected_specific_amount(cost_net)
        expected_from_rates = resolution.expected_gross(cost_net)

    if expected_from_rates is None:
        expected_tax_status = CostAuditFlag.EXPECTED_TAX_UNAVAILABLE.value
        flags.append(expected_tax_status)
    elif bruto is not None:
        diff_rates = _commercial(bruto - expected_from_rates)
        ad_r = abs_decimal(diff_rates)
        if ad_r <= tol.money_exact:
            expected_tax_status = CostAuditFlag.EXPECTED_TAX_MATCH.value
        elif ad_r <= tol.money_rounding:
            expected_tax_status = CostAuditFlag.EXPECTED_TAX_ROUNDING.value
        else:
            expected_tax_status = CostAuditFlag.EXPECTED_TAX_MISMATCH.value
        flags.append(expected_tax_status)

    corrected_gross = expected_from_rates
    understatement: Decimal | None = None
    understatement_vs_corrected: Decimal | None = None
    tax_rate_on_net: Decimal | None = None
    if cost_net is not None and corrected_gross is not None:
        total_tax = _commercial(corrected_gross - cost_net)
        if cost_net != ZERO:
            tax_rate_on_net = _commercial(total_tax / cost_net * Decimal("100"))
    if corrected_gross is not None and stored_gross is not None:
        gap = _commercial(corrected_gross - stored_gross)
        if gap > ZERO:
            understatement = gap
            if corrected_gross > ZERO:
                understatement_vs_corrected = _commercial(
                    gap / corrected_gross * Decimal("100")
                )

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

    if (
        cost_net is not None
        and bruto is not None
        and _near(bruto, cost_net, tol.money_rounding)
        and _has_associated_taxes(raw, tax_factor, resolution)
    ):
        flags.append(CostAuditFlag.PROBABLE_MISSING_TAXES.value)

    if cost_net is not None and bruto is not None and cost_net > ZERO:
        if iva_frac is not None and iva_frac > ZERO:
            double_iva = cost_net * (Decimal("1") + iva_frac) * (Decimal("1") + iva_frac)
            additive_double = cost_net + (cost_net * iva_frac * Decimal("2"))
            if _rel_near(bruto, double_iva, tol.duplicate_iva_tolerance) or _rel_near(
                bruto, additive_double, tol.duplicate_iva_tolerance
            ):
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

    hist_bc = (raw.barcode or "").strip()
    cat_bc = (raw.catalog_barcode or "").strip()
    if hist_bc and cat_bc and hist_bc != cat_bc:
        flags.append(CostAuditFlag.VARIANT_BARCODE_MISMATCH.value)

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

    if (
        raw.average_cost is not None
        and raw.variant_cost_net is not None
        and raw.average_cost > ZERO
        and raw.variant_cost_net > ZERO
        and not _rel_near(raw.average_cost, raw.variant_cost_net, Decimal("0.05"))
    ):
        flags.append(CostAuditFlag.SOURCE_CONFLICT.value)

    if (
        bruto is not None
        and bruto > ZERO
        and variant_gross_values
        and len(variant_gross_values) >= tol.min_candidates_for_outlier
    ):
        med = Decimal(str(median([float(x) for x in variant_gross_values])))
        if med > ZERO and bruto > med * tol.outlier_factor:
            flags.append(CostAuditFlag.SUSPICIOUS_OUTLIER.value)

    effective = resolve_effective_quality_status(
        flags, tax_resolution_quality=resolution.tax_resolution_quality
    )

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
        stored_components_status=stored_status,
        expected_tax_status=expected_tax_status,
        corrected_gross_cost=corrected_gross,
        stored_gross_cost=stored_gross,
        gross_understatement_amount=understatement,
        tax_rate_on_net_pct=tax_rate_on_net,
        gross_understatement_vs_corrected_pct=understatement_vs_corrected,
        effective_quality_status=effective,
        tax_resolution=resolution.to_dict(),
        flags=flags,
        probable_cause=_probable_causes(flags, effective),
    )


def _sample_rank(result: CostAuditRowResult) -> tuple[int, Decimal, int]:
    eff = result.effective_quality_status or ""
    for i, key in enumerate(SAMPLE_PRIORITY):
        if key == eff or key in result.flags:
            diff = abs_decimal(
                result.gross_understatement_amount
                or result.gross_difference_rates
                or result.gross_difference_amounts
                or ZERO
            )
            return (i, -diff, result.raw.history_id)
    diff = abs_decimal(result.gross_understatement_amount or ZERO)
    return (len(SAMPLE_PRIORITY), -diff, result.raw.history_id)


def _is_problematic(result: CostAuditRowResult) -> bool:
    return result.effective_quality_status != EffectiveQualityStatus.VALID_GROSS.value


def _with_denominator(counts: dict[str, int], denominator: int) -> dict[str, Any]:
    out: dict[str, Any] = {"denominator": int(denominator)}
    out.update(counts)
    return out


def _build_coverage(
    *,
    date_from: date,
    date_to: date,
    requested_days: int,
    population: dict[str, Any],
    history_meta: dict[str, Any] | None,
) -> dict[str, Any]:
    actual_min_s = population.get("min_admission_date")
    actual_max_s = population.get("max_admission_date")
    actual_min = date.fromisoformat(actual_min_s) if actual_min_s else None
    actual_max = date.fromisoformat(actual_max_s) if actual_max_s else None
    covered_days = (
        (actual_max - actual_min).days + 1 if actual_min and actual_max else 0
    )
    missing_before = (
        (actual_min - date_from).days if actual_min and actual_min > date_from else 0
    )
    missing_after = (
        (date_to - actual_max).days if actual_max and actual_max < date_to else 0
    )
    hm = history_meta or {}
    return {
        "requested_date_from": date_from.isoformat(),
        "requested_date_to": date_to.isoformat(),
        "actual_min_admission_date": actual_min_s,
        "actual_max_admission_date": actual_max_s,
        "requested_days": int(requested_days),
        "covered_days": covered_days,
        "missing_days_before": max(0, missing_before),
        "missing_days_after": max(0, missing_after),
        "coverage_is_complete": bool(
            actual_min
            and actual_max
            and missing_before <= 0
            and missing_after <= 0
            and int(population.get("rows_in_scope") or 0) > 0
        ),
        "history_earliest_admission_date_global": hm.get("earliest_admission_date"),
        "history_latest_admission_date_global": hm.get("latest_admission_date"),
        "history_earliest_created_at": hm.get("earliest_created_at"),
        "history_latest_created_at": hm.get("latest_created_at"),
        "history_total_rows_company": hm.get("total_rows"),
        "initial_sync_lookback_days_default": 90,
        "initial_sync_lookback_env": "COST_SYNC_INITIAL_DAYS",
        "checkpoint_table": "analytics.cost_sync_state",
        "checkpoint_field": "last_admission_ts",
        "backfill_full_history": False,
        "backfill_note": (
            "El primer sync usa lookback INITIAL_LOOKBACK_DAYS (default 90), "
            "no un backfill histórico completo a 365+ días. "
            "admission_date ≠ created_at ni last_run_at."
        ),
    }


def _build_sync_freshness(
    *,
    sync_meta: dict[str, Any] | None,
    history_meta: dict[str, Any] | None,
    latest_admission_date: str | None,
    as_of: date,
) -> dict[str, Any]:
    sm = sync_meta or {}
    hm = history_meta or {}
    last_run = sm.get("last_run_at")
    last_status = sm.get("last_status")
    last_run_s = None
    if last_run is not None:
        last_run_s = last_run.isoformat() if hasattr(last_run, "isoformat") else str(last_run)

    status = "unknown"
    if last_status == "error":
        status = "failed"
    elif last_status == "ok" and last_run_s:
        # Frescura del JOB, no de admission_date
        try:
            if isinstance(last_run, datetime):
                run_d = last_run.date()
            else:
                run_d = date.fromisoformat(str(last_run)[:10])
            age = (as_of - run_d).days
            status = "stale" if age > 7 else "current"
        except Exception:
            status = "unknown"
    elif not sm.get("row_found") and not sm.get("last_run_at"):
        status = "unknown"

    return {
        "last_successful_sync_at": last_run_s if last_status == "ok" else None,
        "last_source_query_at": last_run_s,  # no hay campo separado; last_run_at es el proxy
        "last_insert_or_update_at": hm.get("latest_created_at"),
        "latest_admission_date": latest_admission_date,
        "sync_status": status,
        "last_status_raw": last_status,
        "checkpoint_last_admission_ts": sm.get("last_admission_ts"),
        "notes": [
            "last_run_at = timestamp de ejecución del job sync (analytics.cost_sync_state)",
            "last_admission_ts = watermark de admission del origen (unix); no es created_at",
            "created_at en history = momento de INSERT local",
            "admission_date antigua NO implica sync fallido por sí sola",
        ],
    }


def _build_tax_dictionary(
    *,
    tax_catalog: dict[int, TaxCatalogEntry],
    occurrence_counts: dict[int, int],
    observed_ids: set[int],
) -> tuple[list[dict[str, Any]], list[int]]:
    from backend.services.analytics.cost_tax_resolution import (
        DOCUMENTED_TAX_IDS,
        TAX_ID_FALLBACK,
        TAX_ID_NOTES,
        _category_for,
        _is_iva_entry,
    )

    ids = set(DOCUMENTED_TAX_IDS) | set(observed_ids) | set(occurrence_counts.keys())
    unknown: list[int] = []
    dictionary: list[dict[str, Any]] = []
    for tax_id in sorted(ids):
        entry = tax_catalog.get(tax_id)
        rate: Decimal | None = None
        name: str | None = None
        source = "unresolved"
        if entry is not None and entry.percentage is not None:
            rate = entry.percentage
            name = entry.name
            source = "bsale.taxes"
        elif tax_id in TAX_ID_FALLBACK:
            name, rate, _cat = TAX_ID_FALLBACK[tax_id]
            source = "fallback"
        else:
            unknown.append(tax_id)
        is_iva = _is_iva_entry(tax_id, name=name, rate=rate) if rate is not None else (
            tax_id == 1
        )
        category = (
            _category_for(tax_id, rate, is_iva=is_iva) if rate is not None else "unknown"
        )
        dictionary.append(
            {
                "tax_id": tax_id,
                "name": name,
                "rate": None if rate is None else str(rate),
                "category": category,
                "source": source,
                "rows_in_scope": int(occurrence_counts.get(tax_id, 0)),
                "note": TAX_ID_NOTES.get(
                    tax_id,
                    "ID observado sin nota canónica en repo; sin tasa segura no se corrige.",
                ),
            }
        )
    return dictionary, sorted(set(unknown))


def _quantity_semantics() -> dict[str, Any]:
    return {
        "status": "unknown",
        "safe_for_weighted_totals": False,
        "description": (
            "quantity se copia desde reception detail Bsale (line.quantity) y cost_net "
            "desde line.cost en sync_cost_receptions. El repo de analítica multiplica "
            "cost_net*quantity en algunos totales de UI, pero el sync no valida unidad "
            "(unidad/caja/kg), SEC, units_per_box ni productos pesables. "
            "Por eso el auditor no pondera understatement por quantity."
        ),
        "source_fields": {
            "quantity": "Bsale reception detail → analytics.cost_reception_history.quantity",
            "cost_net": "Bsale reception detail cost → cost_reception_history.cost_net",
        },
        "sec_interaction": "no validado en sync_cost_receptions",
        "weighable_products": "no discriminados en el sync de costos",
        "boxes_vs_units": "no confirmado a nivel de fila de recepción",
    }


def build_cost_audit_report(
    *,
    args: CostAuditArgs,
    date_from: date,
    date_to: date,
    results: list[CostAuditRowResult],
    tax_context_stats: dict[str, int],
    duration_ms: float,
    barcode_resolution: Any | None = None,
    population: dict[str, Any] | None = None,
    freshness: dict[str, Any] | None = None,
    tax_combination_counts: dict[str, int] | None = None,
    coverage: dict[str, Any] | None = None,
    sync_freshness: dict[str, Any] | None = None,
    tax_dictionary: list[dict[str, Any]] | None = None,
    unknown_tax_ids: list[int] | None = None,
    unresolved_tax_rows: int = 0,
) -> dict[str, Any]:
    quality = {k: 0 for k in QUALITY_COUNTER_KEYS}
    effective_quality = {k: 0 for k in EFFECTIVE_COUNTER_KEYS}
    variants: set[int] = set()
    documents: set[int] = set()
    stored_abs: list[Decimal] = []
    tax_abs: list[Decimal] = []
    tax_pct: list[Decimal] = []
    dates: list[date] = []
    combo_counts: Counter[str] = Counter()
    under_amounts: list[Decimal] = []

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
        if res.effective_quality_status and res.effective_quality_status in effective_quality:
            effective_quality[res.effective_quality_status] += 1
        fp = tax_ids_fingerprint(res.raw.tax_ids_json)
        combo_counts[fp or "(none)"] += 1
        if res.gross_difference_amounts is not None:
            stored_abs.append(abs_decimal(res.gross_difference_amounts))
        if res.gross_difference_rates is not None:
            ad = abs_decimal(res.gross_difference_rates)
            tax_abs.append(ad)
            if res.raw.cost_net and res.raw.cost_net != ZERO:
                tax_pct.append(ad / abs_decimal(res.raw.cost_net) * Decimal("100"))
        elif res.gross_understatement_amount is not None:
            tax_abs.append(abs_decimal(res.gross_understatement_amount))
        if res.gross_understatement_amount is not None:
            under_amounts.append(res.gross_understatement_amount)

    samples: list[dict[str, Any]] = []
    if not args.summary_only:
        samples = [
            s.to_sample_dict()
            for s in sorted(
                [r for r in results if _is_problematic(r)],
                key=_sample_rank,
            )[: args.sample_limit]
        ]

    pop = population or {}
    rows_in_scope = int(pop.get("rows_in_scope") or len(results))
    rows_scanned = len(results)
    is_full = rows_scanned >= rows_in_scope

    denom = rows_scanned  # stats below are always over classified rows
    stats_scope = (
        "full_population"
        if args.summary_only and is_full
        else ("full_population" if is_full else "detail_scan")
    )

    unit_understatement = {
        "sum_across_reception_rows": (
            str(sum(under_amounts)) if under_amounts else None
        ),
        "average_per_row": (
            str(sum(under_amounts) / Decimal(len(under_amounts)))
            if under_amounts
            else None
        ),
        "maximum_per_row": str(max(under_amounts)) if under_amounts else None,
        "rows": len(under_amounts),
        "denominator": denom,
        "scope": stats_scope,
        "warning": "No representa impacto total de compras",
    }
    quantity_weighted = {
        "sum": None,
        "rows_included": 0,
        "rows_excluded": denom,
        "quality": "unavailable",
        "denominator": denom,
        "reason": "quantity_semantics.safe_for_weighted_totals=false",
    }

    tax_ctx = dict(tax_context_stats)
    tax_ctx["denominator"] = denom
    tax_ctx["scope"] = stats_scope

    differences = {
        "denominator": denom,
        "scope": stats_scope,
        "stored_components": {
            "average_absolute": (
                str(sum(stored_abs) / Decimal(len(stored_abs))) if stored_abs else None
            ),
            "maximum_absolute": str(max(stored_abs)) if stored_abs else None,
            "rows": len(stored_abs),
        },
        "expected_tax_profile": {
            "average_absolute": (
                str(sum(tax_abs) / Decimal(len(tax_abs))) if tax_abs else None
            ),
            "maximum_absolute": str(max(tax_abs)) if tax_abs else None,
            "average_percentage": (
                str(sum(tax_pct) / Decimal(len(tax_pct))) if tax_pct else None
            ),
            "rows": len(tax_abs),
        },
    }

    return {
        "ok": True,
        "read_only": True,
        "summary_only": args.summary_only,
        "scope": {
            "company_id": args.company_id,
            "office_id": args.office_id,
            "date_from": date_from.isoformat(),
            "date_to": date_to.isoformat(),
            "variant_id": args.variant_id,
            "barcode": args.barcode,
            "source_document_id": args.source_document_id,
            "limit": args.limit,
            "limit_applies_to": "detail_scan_only",
            "page_size": args.page_size,
            "max_pages": args.max_pages,
            "sample_limit": args.sample_limit,
            "summary_only_ignores_limit": True,
        },
        "population": {
            "rows_in_scope": rows_in_scope,
            "rows_scanned_for_detail": rows_scanned,
            "is_full_detail_scan": is_full,
            "unique_variants": int(pop.get("unique_variants") or len(variants)),
            "unique_documents": int(pop.get("unique_documents") or len(documents)),
            "min_admission_date": pop.get("min_admission_date")
            or (min(dates).isoformat() if dates else None),
            "max_admission_date": pop.get("max_admission_date")
            or (max(dates).isoformat() if dates else None),
            "note": (
                "rows_in_scope = COUNT SQL sin LIMIT. "
                "Con --summary-only se clasifica toda la población (limit ignorado). "
                "En modo detalle, limit/page-size/max-pages acotan el scan."
            ),
        },
        "coverage": coverage or {},
        "freshness": freshness
        or {
            "latest_admission_date": None,
            "days_since_latest_admission": None,
            "is_stale": None,
            "admission_date_meaning": (
                "Fecha de admisión/recepción (no es ejecución del sync)"
            ),
        },
        "sync_freshness": sync_freshness or {},
        "quantity_semantics": _quantity_semantics(),
        "rows_analyzed": rows_scanned,
        "unique_variants": int(pop.get("unique_variants") or len(variants)),
        "unique_documents": int(pop.get("unique_documents") or len(documents)),
        "date_range_found": {
            "min": pop.get("min_admission_date")
            or (min(dates).isoformat() if dates else None),
            "max": pop.get("max_admission_date")
            or (max(dates).isoformat() if dates else None),
        },
        "quality": _with_denominator(quality, denom),
        "effective_quality": _with_denominator(effective_quality, denom),
        "tax_combinations": {
            "denominator": rows_in_scope,
            "scope": "population_sql",
            "counts": tax_combination_counts or dict(combo_counts),
        },
        "tax_dictionary": tax_dictionary or [],
        "unknown_tax_ids": unknown_tax_ids or [],
        "unresolved_tax_rows": unresolved_tax_rows,
        "unit_understatement": unit_understatement,
        "quantity_weighted_understatement": quantity_weighted,
        "tax_context": tax_ctx,
        "differences": differences,
        "samples": samples,
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
            "sync_job_stale_days": 7,
        },
        "limitations": [
            "source_document_id no es columna canónica; se usa document_number o reception_id",
            "cost_bruto_erp del sync es sintético (net×tax_factor), no bruto de documento Bsale",
            "iva_rate interpretado en puntos porcentuales si > 1",
            "stored_components_match no implica bruto fiscalmente correcto",
            "outlier es alerta estadística, no corrección",
            "IVA se identifica por tax_id/tipo, nunca por posición en tax_ids_json",
            "sample-limit no limita population.rows_in_scope",
            "limit solo aplica al detail scan; summary-only lo ignora",
            "admission_date ≠ last_run_at del sync; ver sync_freshness",
            "unit_understatement no es impacto total de compras",
            "quantity no se usa para totales ponderados sin semántica confirmada",
            "tax_id 4–7 sin fallback seguro: sin bsale.taxes.percentage → unresolved",
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
        if args.variant_id is not None:
            variant_ids_filter = [v for v in variant_ids_filter if v == args.variant_id]

    population_row = repository.fetch_population_summary(
        args,
        date_from=date_from,
        date_to=date_to,
        variant_ids=variant_ids_filter,
    )
    tax_combos = repository.fetch_tax_combination_counts(
        args,
        date_from=date_from,
        date_to=date_to,
        variant_ids=variant_ids_filter,
    )
    tax_id_counts = repository.fetch_tax_id_occurrence_counts(
        args,
        date_from=date_from,
        date_to=date_to,
        variant_ids=variant_ids_filter,
    )
    sync_meta = repository.fetch_sync_meta(company_id=args.company_id)
    history_meta = repository.fetch_history_coverage_meta(company_id=args.company_id)

    raw_rows = repository.fetch_history_rows_paged(
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
    for combo in tax_combos:
        for part in str(combo).split(","):
            part = part.strip()
            if part.isdigit():
                tax_ids.append(int(part))
    tax_ids.extend(tax_id_counts.keys())
    tax_ids.extend(range(1, 9))
    tax_catalog = repository.fetch_taxes_for_ids(
        company_id=args.company_id,
        tax_ids=tax_ids,
    )

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
    unresolved_tax_rows = 0
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

        classified = classify_cost_audit_row(
            r,
            tax_catalog=tax_catalog,
            tolerances=args.tolerances,
            duplicate_unique_keys=dup_uks,
            duplicate_detail_keys=dup_details,
            variant_gross_values=gross_by_variant.get(r.variant_id),
            as_of=as_of,
            latest_admission_by_variant=latest_adm,
        )
        tr = classified.tax_resolution or {}
        if tr.get("unresolved_tax_ids") or tr.get("tax_resolution_quality") in (
            "unavailable",
            "partial",
        ):
            # partial con IVA+específicos conocidos no cuenta como unresolved row
            if tr.get("unresolved_tax_ids"):
                unresolved_tax_rows += 1
            elif tr.get("tax_resolution_quality") == "unavailable":
                unresolved_tax_rows += 1
        results.append(classified)

    latest_adm_pop = population_row.get("max_admission_date")
    latest_d: date | None = None
    if isinstance(latest_adm_pop, str):
        latest_d = date.fromisoformat(latest_adm_pop)
    elif isinstance(latest_adm_pop, date):
        latest_d = latest_adm_pop
    days_since = (as_of - latest_d).days if latest_d is not None else None
    freshness = {
        "latest_admission_date": latest_d.isoformat() if latest_d else None,
        "days_since_latest_admission": days_since,
        # Solo informa edad de admission; NO decide sync fallido
        "admission_data_gap_days": days_since,
        "is_stale": None,
        "note": (
            "No usar admission_date sola para afirmar sync stale/failed; "
            "ver sync_freshness.sync_status"
        ),
        "admission_date_meaning": (
            "Máxima admission_date en cost_reception_history (recepción/admisión)"
        ),
        "variant_cost_last_update_meaning": (
            "bsale.variant_cost.last_update = frescura del snapshot por variante"
        ),
        "as_of": as_of.isoformat(),
    }

    observed_ids: set[int] = set(tax_id_counts.keys())
    for combo in tax_combos:
        for part in str(combo).split(","):
            if part.strip().isdigit():
                observed_ids.add(int(part.strip()))
    tax_dictionary, unknown_ids = _build_tax_dictionary(
        tax_catalog=tax_catalog,
        occurrence_counts=tax_id_counts,
        observed_ids=observed_ids,
    )

    coverage = _build_coverage(
        date_from=date_from,
        date_to=date_to,
        requested_days=args.days,
        population=population_row,
        history_meta=history_meta,
    )
    sync_freshness = _build_sync_freshness(
        sync_meta=sync_meta,
        history_meta=history_meta,
        latest_admission_date=latest_d.isoformat() if latest_d else None,
        as_of=as_of,
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
        population=population_row,
        freshness=freshness,
        tax_combination_counts=tax_combos,
        coverage=coverage,
        sync_freshness=sync_freshness,
        tax_dictionary=tax_dictionary,
        unknown_tax_ids=unknown_ids,
        unresolved_tax_rows=unresolved_tax_rows,
    )
