"""Motor puro de cálculo de costos V2 (Decimal only, sin I/O, sin split_erp_cost)."""

from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal
from typing import Any, Iterable, Sequence

from backend.services.analytics.cost_audit_models import TaxCatalogEntry
from backend.services.analytics.cost_tax_resolution import (
    IVA_ADVANCE_TAX_IDS,
    IVA_TAX_IDS,
    resolve_taxes_from_ids,
)
from backend.services.analytics.cost_v2_models import (
    ALLOWED_CONTEXT_SOURCES,
    ALLOWED_RESOLUTION_QUALITIES,
    ALLOWED_TAX_IDS_SOURCES,
    ALLOWED_TAX_RATES_SOURCES,
    CALCULATION_VERSION,
    AdditionalTaxAmount,
    CostReceptionCalculation,
    CostReceptionInput,
    CostV2Tolerances,
    TaxContextInput,
    TaxRateEntry,
)
from backend.services.analytics.money import ZERO

COMMERCIAL = Decimal("0.01")
STORAGE = Decimal("0.0001")

__all__ = [
    "CALCULATION_VERSION",
    "build_tax_context_from_ids",
    "calculate_cost_reception",
    "calculation_result_fingerprint",
    "fingerprint_sha256",
    "source_history_fingerprint",
    "tax_context_fingerprint",
]


def _commercial(value: Decimal) -> Decimal:
    return value.quantize(COMMERCIAL, rounding=ROUND_HALF_UP)


def _storage(value: Decimal) -> Decimal:
    return value.quantize(STORAGE, rounding=ROUND_HALF_UP)


def _abs(value: Decimal) -> Decimal:
    return value if value >= ZERO else -value


def _near(a: Decimal, b: Decimal, tol: Decimal) -> bool:
    return _abs(a - b) <= tol


def _rel_near(a: Decimal, b: Decimal, rel: Decimal) -> bool:
    if b == ZERO:
        return a == ZERO
    return _abs(a - b) / _abs(b) <= rel


def _dec_str(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(value, "f")


def _admission_date(value: date | datetime | None) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    return value


def fingerprint_sha256(payload: Any) -> str:
    """Serialización JSON canónica → SHA-256 hex."""
    blob = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def source_history_fingerprint(row: CostReceptionInput) -> str:
    adm = _admission_date(row.admission_date)
    return fingerprint_sha256(
        {
            "history_id": row.history_id,
            "company_id": row.company_id,
            "office_id": row.office_id,
            "variant_id": row.variant_id,
            "admission_date": adm.isoformat() if adm else None,
            "stored_cost_net": _dec_str(row.stored_cost_net),
            "stored_quantity": _dec_str(row.stored_quantity),
            "stored_iva_amount": _dec_str(row.stored_iva_amount),
            "stored_other_taxes": _dec_str(row.stored_other_taxes),
            "stored_gross_cost": _dec_str(row.stored_gross_cost),
            "reception_tax_ids": sorted(set(row.reception_tax_ids)),
            "catalog_tax_ids": sorted(set(row.catalog_tax_ids)),
        }
    )


def tax_context_fingerprint(ctx: TaxContextInput) -> str:
    taxes_payload = [
        {
            "tax_id": t.tax_id,
            "rate": _dec_str(t.rate),
            "category": t.category,
            "source": t.source,
            "name": t.name,
        }
        for t in sorted(ctx.taxes, key=lambda x: x.tax_id)
    ]
    as_of = ctx.context_as_of
    return fingerprint_sha256(
        {
            "tax_ids": sorted(set(ctx.tax_ids)),
            "taxes": taxes_payload,
            "context_source": ctx.context_source,
            "tax_ids_source": ctx.tax_ids_source,
            "tax_rates_source": ctx.tax_rates_source,
            "context_as_of": as_of.isoformat() if as_of else None,
            "context_is_historical": ctx.context_is_historical,
            "resolution_quality": ctx.resolution_quality,
        }
    )


def calculation_result_fingerprint(calc: CostReceptionCalculation) -> str:
    """SHA-256 del resultado V2. Orden de warnings/taxes/ids no altera el hash.

    NULL se serializa como null JSON (distinto de cero).
    """
    adds = [
        {
            "tax_id": t.tax_id,
            "name": t.name,
            "rate": _dec_str(t.rate),
            "category": t.category,
            "amount": _dec_str(t.amount),
            "source": t.source,
        }
        for t in sorted(
            calc.additional_taxes,
            key=lambda x: (x.tax_id, x.category or "", x.source or "", x.name or ""),
        )
    ]
    return fingerprint_sha256(
        {
            "calculation_version": calc.calculation_version,
            "resolved_tax_ids": sorted(set(calc.resolved_tax_ids)),
            "iva_tax_id": calc.iva_tax_id,
            "iva_rate": _dec_str(calc.iva_rate),
            "calculated_iva_amount": _dec_str(calc.calculated_iva_amount),
            "additional_taxes": adds,
            "additional_tax_rate_total": _dec_str(calc.additional_tax_rate_total),
            "additional_tax_amount_total": _dec_str(calc.additional_tax_amount_total),
            "total_tax_rate": _dec_str(calc.total_tax_rate),
            "corrected_gross_cost": _dec_str(calc.corrected_gross_cost),
            "gross_difference_amount": _dec_str(calc.gross_difference_amount),
            "tax_rate_on_net_pct": _dec_str(calc.tax_rate_on_net_pct),
            "gross_understatement_vs_corrected_pct": _dec_str(
                calc.gross_understatement_vs_corrected_pct
            ),
            "effective_quality_status": calc.effective_quality_status,
            "warnings": sorted(set(calc.warnings)),
            "tax_ids_source": calc.tax_ids_source,
            "tax_rates_source": calc.tax_rates_source,
            "tax_resolution_quality": calc.tax_resolution_quality,
            "source_history_fingerprint": calc.source_history_fingerprint,
            "tax_context_fingerprint": calc.tax_context_fingerprint,
        }
    )


def _with_result_fingerprint(calc: CostReceptionCalculation) -> CostReceptionCalculation:
    return replace(calc, calculation_result_fingerprint=calculation_result_fingerprint(calc))


def build_tax_context_from_ids(
    tax_ids: Sequence[int],
    *,
    tax_catalog: dict[int, TaxCatalogEntry] | None = None,
    context_source: str = "bsale_taxes",
    tax_ids_source: str = "unresolved",
    context_as_of: datetime | None = None,
    context_is_historical: bool = False,
    cost_net: Decimal | None = None,
) -> TaxContextInput:
    """Helper: resuelve por identidad vía cost_tax_resolution.

    tax_ids_source y tax_rates_source son ortogonales: los IDs pueden venir del
    producto actual mientras las tasas salen de bsale.taxes o fallback canónico.
    context_source queda como campo legacy (preferir tax_rates_source).
    """
    if context_source not in ALLOWED_CONTEXT_SOURCES:
        raise ValueError(f"invalid context_source: {context_source}")
    if tax_ids_source not in ALLOWED_TAX_IDS_SOURCES:
        raise ValueError(f"invalid tax_ids_source: {tax_ids_source}")
    ids = tuple(sorted({int(x) for x in tax_ids}))
    resolution = resolve_taxes_from_ids(
        list(ids), tax_catalog=tax_catalog, cost_net=cost_net
    )
    taxes: list[TaxRateEntry] = []
    if resolution.iva_tax_id is not None and resolution.iva_rate is not None:
        taxes.append(
            TaxRateEntry(
                tax_id=resolution.iva_tax_id,
                name="IVA",
                rate=resolution.iva_rate,
                category="iva",
                source=resolution.tax_resolution_source or context_source,
            )
        )
    for spec in resolution.specific_taxes:
        taxes.append(
            TaxRateEntry(
                tax_id=spec.tax_id,
                name=spec.name,
                rate=spec.rate,
                category=spec.category,
                source=resolution.tax_resolution_source or context_source,
            )
        )

    if resolution.tax_resolution_quality == "unavailable" or (
        resolution.unresolved_tax_ids and not taxes
    ):
        quality = "unresolved"
        rates_source = "unresolved"
    elif resolution.unresolved_tax_ids:
        quality = "unresolved"
        rates_source = (
            "canonical_fallback"
            if resolution.tax_resolution_source == "canonical_fallback"
            else ("bsale_taxes" if taxes else "unresolved")
        )
    elif resolution.tax_resolution_source == "canonical_fallback":
        quality = "canonical_fallback"
        rates_source = "canonical_fallback"
    elif context_is_historical:
        quality = "historical_catalog"
        rates_source = (
            "reception_payload"
            if context_source == "reception_payload"
            else "bsale_taxes"
        )
    elif context_source == "reception_payload":
        quality = "direct_reception"
        rates_source = "reception_payload"
    else:
        quality = "current_catalog"
        rates_source = "bsale_taxes"

    # Legacy: tax_context_source ≈ fuente de tasas (deprecado).
    if rates_source == "bsale_taxes":
        legacy_source = "bsale_taxes"
    elif rates_source == "canonical_fallback":
        legacy_source = "canonical_fallback"
    elif rates_source == "reception_payload":
        legacy_source = "reception_payload"
    else:
        legacy_source = "unresolved"

    if quality not in ALLOWED_RESOLUTION_QUALITIES:
        quality = "unresolved"
    if rates_source not in ALLOWED_TAX_RATES_SOURCES:
        rates_source = "unresolved"
    if legacy_source not in ALLOWED_CONTEXT_SOURCES:
        legacy_source = "unresolved"

    ids_source = tax_ids_source if ids else "unresolved"
    if ids_source not in ALLOWED_TAX_IDS_SOURCES:
        ids_source = "unresolved"

    return TaxContextInput(
        tax_ids=ids,
        taxes=tuple(taxes),
        context_source=legacy_source,  # type: ignore[arg-type]
        context_as_of=context_as_of,
        context_is_historical=context_is_historical,
        resolution_quality=quality,  # type: ignore[arg-type]
        tax_ids_source=ids_source,  # type: ignore[arg-type]
        tax_rates_source=rates_source,  # type: ignore[arg-type]
    )


def _split_iva_and_additional(
    taxes: Sequence[TaxRateEntry],
) -> tuple[TaxRateEntry | None, list[TaxRateEntry]]:
    """Separa IVA principal de adicionales (incl. iva_advance 6/7)."""
    iva: TaxRateEntry | None = None
    additional: list[TaxRateEntry] = []
    for t in sorted(taxes, key=lambda x: x.tax_id):
        is_advance = (
            t.tax_id in IVA_ADVANCE_TAX_IDS or t.category == "iva_advance"
        )
        is_principal_iva = (not is_advance) and (
            t.tax_id in IVA_TAX_IDS or t.category == "iva"
        )
        if is_principal_iva:
            if iva is None or t.rate > iva.rate:
                iva = t
        else:
            additional.append(t)
    return iva, additional


def _profile_resolved(ctx: TaxContextInput) -> bool:
    if ctx.resolution_quality == "unresolved":
        return False
    if ctx.context_source == "unresolved":
        return False
    if not ctx.taxes:
        return False
    known = {t.tax_id for t in ctx.taxes}
    for tid in ctx.tax_ids:
        if tid not in known:
            return False
    return True


def _context_identity_fields(
    tax_context: TaxContextInput,
) -> tuple[tuple[int, ...], int | None, Decimal | None]:
    """IDs/tasas de identidad del contexto (sin montos calculados)."""
    resolved_ids = tuple(sorted({t.tax_id for t in tax_context.taxes}))
    iva_entry, _additional = _split_iva_and_additional(tax_context.taxes)
    iva_tax_id = iva_entry.tax_id if iva_entry is not None else None
    iva_rate = iva_entry.rate if iva_entry is not None else None
    return resolved_ids, iva_tax_id, iva_rate


def calculate_cost_reception(
    row: CostReceptionInput,
    tax_context: TaxContextInput,
    *,
    external_warnings: Iterable[str] = (),
    tolerances: CostV2Tolerances | None = None,
    calculation_version: str = CALCULATION_VERSION,
) -> CostReceptionCalculation:
    """Transforma history almacenado + contexto tributario → resultado V2."""
    tol = tolerances or CostV2Tolerances()
    warnings: list[str] = []
    for w in external_warnings:
        if w and w not in warnings:
            warnings.append(str(w))

    if tax_context.context_source not in ALLOWED_CONTEXT_SOURCES:
        raise ValueError(f"invalid context_source: {tax_context.context_source}")
    if tax_context.resolution_quality not in ALLOWED_RESOLUTION_QUALITIES:
        raise ValueError(f"invalid resolution_quality: {tax_context.resolution_quality}")
    if tax_context.tax_ids_source not in ALLOWED_TAX_IDS_SOURCES:
        raise ValueError(f"invalid tax_ids_source: {tax_context.tax_ids_source}")
    if tax_context.tax_rates_source not in ALLOWED_TAX_RATES_SOURCES:
        raise ValueError(f"invalid tax_rates_source: {tax_context.tax_rates_source}")

    hist_fp = source_history_fingerprint(row)
    tax_fp = tax_context_fingerprint(tax_context)
    adm = _admission_date(row.admission_date)

    net = row.stored_cost_net
    bruto = row.stored_gross_cost

    if row.reception_tax_ids and tax_context.tax_ids_source != "reception_payload":
        if "tax_ids_not_consumed" not in warnings:
            warnings.append("tax_ids_not_consumed")
    if not tax_context.tax_ids and not tax_context.taxes:
        if "reception_tax_context_unavailable" not in warnings:
            warnings.append("reception_tax_context_unavailable")

    # Conservar identidad tributaria aunque el neto no permita montos.
    resolved_ids, ctx_iva_tax_id, ctx_iva_rate = _context_identity_fields(tax_context)

    if net is None or net <= ZERO:
        return _with_result_fingerprint(
            CostReceptionCalculation(
                history_id=row.history_id,
                company_id=row.company_id,
                office_id=row.office_id,
                variant_id=row.variant_id,
                admission_date=adm,
                calculation_version=calculation_version,
                stored_cost_net=net,
                stored_quantity=row.stored_quantity,
                stored_iva_amount=row.stored_iva_amount,
                stored_other_taxes=row.stored_other_taxes,
                stored_gross_cost=bruto,
                reception_tax_ids=tuple(row.reception_tax_ids),
                catalog_tax_ids=tuple(row.catalog_tax_ids),
                resolved_tax_ids=resolved_ids,
                iva_tax_id=ctx_iva_tax_id,
                iva_rate=ctx_iva_rate,
                calculated_iva_amount=None,
                additional_taxes=(),
                additional_tax_rate_total=None,
                additional_tax_amount_total=None,
                total_tax_rate=None,
                corrected_gross_cost=None,
                gross_difference_amount=None,
                tax_rate_on_net_pct=None,
                gross_understatement_vs_corrected_pct=None,
                tax_context_source=tax_context.context_source,
                tax_ids_source=tax_context.tax_ids_source,
                tax_rates_source=tax_context.tax_rates_source,
                tax_context_as_of=tax_context.context_as_of,
                tax_context_is_historical=tax_context.context_is_historical,
                tax_resolution_quality=tax_context.resolution_quality,
                effective_quality_status="missing_cost",
                warnings=tuple(warnings),
                source_history_created_at=row.source_history_created_at,
                source_history_fingerprint=hist_fp,
                tax_context_fingerprint=tax_fp,
                calculation_result_fingerprint="",
            )
        )

    resolved = _profile_resolved(tax_context)
    iva_entry, additional_entries = _split_iva_and_additional(tax_context.taxes)

    iva_amount: Decimal | None = None
    iva_rate: Decimal | None = None
    iva_tax_id: int | None = None
    add_amounts: list[AdditionalTaxAmount] = []
    add_rate_total: Decimal | None = None
    add_amount_total: Decimal | None = None
    total_rate: Decimal | None = None
    corrected: Decimal | None = None
    # resolved_ids ya calculado arriba desde el contexto

    if resolved:
        if iva_entry is not None:
            iva_tax_id = iva_entry.tax_id
            iva_rate = iva_entry.rate
            iva_amount = _commercial(net * iva_rate / Decimal("100"))
        else:
            iva_amount = _commercial(ZERO)
            iva_rate = ZERO

        add_rate = ZERO
        add_amt = ZERO
        for t in additional_entries:
            amt = _commercial(net * t.rate / Decimal("100"))
            add_rate += t.rate
            add_amt += amt
            add_amounts.append(
                AdditionalTaxAmount(
                    tax_id=t.tax_id,
                    name=t.name,
                    rate=t.rate,
                    category=t.category,
                    amount=amt,
                    source=t.source,
                )
            )
        add_rate_total = _commercial(add_rate) if additional_entries else ZERO
        add_amount_total = _commercial(add_amt) if additional_entries else ZERO
        total_rate = _commercial((iva_rate or ZERO) + (add_rate_total or ZERO))
        corrected = _commercial(net + (iva_amount or ZERO) + (add_amount_total or ZERO))

    component_status: str | None = None
    if (
        row.stored_iva_amount is not None
        and row.stored_other_taxes is not None
        and bruto is not None
    ):
        expected_components = _commercial(
            net + row.stored_iva_amount + row.stored_other_taxes
        )
        diff_c = _commercial(bruto - expected_components)
        ad = _abs(diff_c)
        if ad <= tol.money_exact:
            component_status = "match"
        elif ad <= tol.money_rounding:
            component_status = "rounding"
            if "stored_components_rounding" not in warnings:
                warnings.append("stored_components_rounding")
        else:
            component_status = "mismatch"

    gross_diff: Decimal | None = None
    tax_rate_on_net: Decimal | None = None
    under_vs_corr: Decimal | None = None
    if corrected is not None:
        total_tax = _commercial(corrected - net)
        tax_rate_on_net = _commercial(total_tax / net * Decimal("100"))
        if bruto is not None:
            gross_diff = _commercial(corrected - bruto)
            if corrected > ZERO and gross_diff > ZERO:
                under_vs_corr = _commercial(gross_diff / corrected * Decimal("100"))

    duplicated = False
    if corrected is not None and bruto is not None and total_rate is not None and net > ZERO:
        frac = total_rate / Decimal("100")
        once = net * (Decimal("1") + frac)
        twice_mult = net * (Decimal("1") + frac) * (Decimal("1") + frac)
        twice_add = net + (net * frac * Decimal("2"))
        if (
            _rel_near(bruto, twice_mult, tol.duplicate_rel)
            or _rel_near(bruto, twice_add, tol.duplicate_rel)
        ) and not _rel_near(bruto, once, tol.duplicate_rel):
            duplicated = True

    if component_status == "mismatch":
        status = "gross_component_mismatch"
    elif duplicated:
        status = "duplicated_taxes_in_gross"
    elif not resolved:
        status = "incomplete_tax_context"
        status = "incomplete_tax_context"
        corrected = None
        gross_diff = None
        tax_rate_on_net = None
        under_vs_corr = None
        iva_amount = None
        iva_rate = None
        iva_tax_id = None
        add_amount_total = None
        add_rate_total = None
        total_rate = None
        add_amounts = []
        # resolved_ids se conserva desde el contexto (trazabilidad).
    elif bruto is not None and corrected is not None and _near(bruto, net, tol.money_rounding):
        if corrected > bruto + tol.money_rounding:
            status = "missing_taxes_in_gross"
        elif _near(bruto, corrected, tol.money_rounding):
            status = "valid_gross"
        else:
            status = "missing_taxes_in_gross"
    elif bruto is not None and corrected is not None and _near(bruto, corrected, tol.money_rounding):
        status = "valid_gross"
    elif bruto is not None and corrected is not None and bruto + tol.money_rounding < corrected:
        status = "missing_taxes_in_gross"
    else:
        status = "incomplete_tax_context" if not resolved else "missing_taxes_in_gross"

    if corrected is None:
        gross_diff = None
        tax_rate_on_net = None
        under_vs_corr = None

    return _with_result_fingerprint(
        CostReceptionCalculation(
            history_id=row.history_id,
            company_id=row.company_id,
            office_id=row.office_id,
            variant_id=row.variant_id,
            admission_date=adm,
            calculation_version=calculation_version,
            stored_cost_net=_storage(net),
            stored_quantity=(
                _storage(row.stored_quantity) if row.stored_quantity is not None else None
            ),
            stored_iva_amount=(
                _storage(row.stored_iva_amount) if row.stored_iva_amount is not None else None
            ),
            stored_other_taxes=(
                _storage(row.stored_other_taxes) if row.stored_other_taxes is not None else None
            ),
            stored_gross_cost=_storage(bruto) if bruto is not None else None,
            reception_tax_ids=tuple(row.reception_tax_ids),
            catalog_tax_ids=tuple(row.catalog_tax_ids),
            resolved_tax_ids=resolved_ids,
            iva_tax_id=iva_tax_id if resolved else None,
            iva_rate=iva_rate if resolved else None,
            calculated_iva_amount=iva_amount if resolved else None,
            additional_taxes=tuple(add_amounts) if resolved else (),
            additional_tax_rate_total=add_rate_total if resolved else None,
            additional_tax_amount_total=add_amount_total if resolved else None,
            total_tax_rate=total_rate if resolved else None,
            corrected_gross_cost=corrected,
            gross_difference_amount=gross_diff,
            tax_rate_on_net_pct=tax_rate_on_net,
            gross_understatement_vs_corrected_pct=under_vs_corr,
            tax_context_source=tax_context.context_source,
            tax_ids_source=tax_context.tax_ids_source,
            tax_rates_source=tax_context.tax_rates_source,
            tax_context_as_of=tax_context.context_as_of,
            tax_context_is_historical=tax_context.context_is_historical,
            tax_resolution_quality=(
                tax_context.resolution_quality if resolved else "unresolved"
            ),
            effective_quality_status=status,
            warnings=tuple(warnings),
            source_history_created_at=row.source_history_created_at,
            source_history_fingerprint=hist_fp,
            tax_context_fingerprint=tax_fp,
            calculation_result_fingerprint="",
        )
    )
