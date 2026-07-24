"""Resolvedor canónico de costo histórico por línea (Etapa 3 + 3A.1).

Prioridad neto: recepción ≤ fecha → average_cost → missing.
Prioridad bruto: cost_bruto_erp → impuestos reales → tasas → missing.
El bruto real no se bloquea por falta de ila_rate.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date
from decimal import Decimal

from backend.services.analytics.cost_models import (
    HistoricalCostResolution,
    LineCostInput,
    ReceptionCostCandidate,
    VariantCostSnapshot,
)
from backend.services.analytics.cost_repository import CostCandidateRepository
from backend.services.analytics.formulas import commercial_line_economics, line_economics
from backend.services.analytics.money import ZERO, quantize_money
from backend.services.analytics.schemas import CostFallbackLevel, CostQualityStatus
from backend.services.analytics.tax_models import PurchaseTaxAmounts, TaxProfile, TaxQualityStatus


def _empty_commercial_fields() -> dict:
    return {
        "historical_net_unit_cost": None,
        "historical_net_cost": None,
        "cost_iva": None,
        "cost_ila": None,
        "historical_gross_unit_cost": None,
        "historical_gross_cost": None,
        "iva_sales": None,
        "ila_sales": None,
        "gross_sales": None,
        "net_gross_profit": None,
        "gross_commercial_profit": None,
        "net_margin_pct": None,
        "gross_commercial_margin_pct": None,
        "net_markup_pct": None,
        "gross_commercial_markup_pct": None,
        "tax_resolution_method": None,
        "tax_quality_status": None,
        "tax_category": None,
        "tax_source": None,
        "gross_cost_quality": None,
        "tax_breakdown_quality": None,
        "total_tax_amount": None,
        "unclassified_tax_amount": None,
        "reception_iva_amount": None,
        "reception_other_taxes": None,
        "reception_cost_bruto_erp": None,
    }


def _purchase_from_base(base: HistoricalCostResolution) -> PurchaseTaxAmounts | None:
    if base.unit_cost is None:
        return None
    if (
        base.reception_cost_bruto_erp is None
        and base.reception_iva_amount is None
        and base.reception_other_taxes is None
    ):
        return None
    return PurchaseTaxAmounts(
        net_cost=quantize_money(base.unit_cost),
        cost_bruto_erp=base.reception_cost_bruto_erp,
        iva_amount=base.reception_iva_amount,
        other_taxes=base.reception_other_taxes,
        conflicting=base.quality_status == CostQualityStatus.CONFLICTING_COST,
    )


def _missing(
    line: LineCostInput,
    *,
    reason: str,
) -> HistoricalCostResolution:
    return HistoricalCostResolution(
        detail_id=line.detail_id,
        document_id=line.document_id,
        variant_id=line.variant_id,
        commercial_date=line.commercial_date,
        unit_cost=None,
        total_cost=None,
        cost_source=None,
        cost_date=None,
        purchase_document_id=None,
        supplier_id=None,
        age_days_at_sale=None,
        fallback_level=CostFallbackLevel.MISSING,
        is_estimated=False,
        quality_status=CostQualityStatus.MISSING_COST,
        resolution_reason=reason,
        quantity=line.quantity,
        line_net_amount=line.line_net_amount,
        line_total_amount=line.line_total_amount,
        gross_profit=None,
        gross_margin_pct=None,
        markup_pct=None,
        **_empty_commercial_fields(),
    )


def _with_economics(
    base: HistoricalCostResolution,
    *,
    quantity: Decimal | None,
    line_net_amount: Decimal | None,
    line_total_amount: Decimal | None = None,
    tax_profile: TaxProfile | None = None,
) -> HistoricalCostResolution:
    recv_fields = {
        "reception_iva_amount": base.reception_iva_amount,
        "reception_other_taxes": base.reception_other_taxes,
        "reception_cost_bruto_erp": base.reception_cost_bruto_erp,
    }
    if (
        base.quality_status
        in (CostQualityStatus.MISSING_COST, CostQualityStatus.CONFLICTING_COST)
        or base.unit_cost is None
        or quantity is None
        or line_net_amount is None
    ):
        empty = _empty_commercial_fields()
        empty.update(recv_fields)
        return HistoricalCostResolution(
            detail_id=base.detail_id,
            document_id=base.document_id,
            variant_id=base.variant_id,
            commercial_date=base.commercial_date,
            unit_cost=base.unit_cost,
            total_cost=None,
            cost_source=base.cost_source,
            cost_date=base.cost_date,
            purchase_document_id=base.purchase_document_id,
            supplier_id=base.supplier_id,
            age_days_at_sale=base.age_days_at_sale,
            fallback_level=base.fallback_level,
            is_estimated=base.is_estimated,
            quality_status=base.quality_status,
            resolution_reason=base.resolution_reason,
            conflicting_source_ids=base.conflicting_source_ids,
            quantity=quantity,
            line_net_amount=line_net_amount,
            line_total_amount=line_total_amount,
            gross_profit=None,
            gross_margin_pct=None,
            markup_pct=None,
            **empty,
        )

    total_cost = quantize_money(quantity * base.unit_cost)
    eco = line_economics(
        net_sales=line_net_amount,
        historical_cost=total_cost,
        cost_quality=base.quality_status,
    )

    commercial_kwargs = _empty_commercial_fields()
    commercial_kwargs.update(recv_fields)
    commercial_kwargs["historical_net_unit_cost"] = quantize_money(base.unit_cost)
    commercial_kwargs["historical_net_cost"] = total_cost
    commercial_kwargs["net_gross_profit"] = eco.gross_profit
    commercial_kwargs["net_margin_pct"] = eco.gross_margin_pct
    commercial_kwargs["net_markup_pct"] = eco.markup_pct

    profile = tax_profile or TaxProfile(
        iva_rate_pct=None,
        ila_rate_pct=None,
        quality_status=TaxQualityStatus.MISSING_TAX_PROFILE,
    )
    purchase = _purchase_from_base(base)

    if line_total_amount is None:
        commercial_kwargs["gross_sales"] = None
    else:
        # Escalar montos unitarios de recepción a costo de línea.
        purchase_line: PurchaseTaxAmounts | None = None
        if purchase is not None:
            purchase_line = PurchaseTaxAmounts(
                net_cost=total_cost,
                cost_bruto_erp=(
                    None
                    if purchase.cost_bruto_erp is None
                    else quantize_money(purchase.cost_bruto_erp * quantity)
                ),
                iva_amount=(
                    None
                    if purchase.iva_amount is None
                    else quantize_money(purchase.iva_amount * quantity)
                ),
                other_taxes=(
                    None
                    if purchase.other_taxes is None
                    else quantize_money(purchase.other_taxes * quantity)
                ),
                conflicting=purchase.conflicting,
            )
        cel = commercial_line_economics(
            gross_sales=quantize_money(line_total_amount),
            net_sales=line_net_amount,
            historical_net_unit_cost=base.unit_cost,
            quantity=quantity,
            tax_profile=profile,
            purchase=purchase_line,
            historical_tax_profile=profile,
            cost_quality=base.quality_status,
            prefer_bsale_gross_sales=True,
        )
        commercial_kwargs.update(
            {
                "historical_net_unit_cost": cel.historical_net_unit_cost,
                "historical_net_cost": cel.historical_net_cost,
                "cost_iva": cel.cost_iva,
                "cost_ila": cel.cost_ila,
                "historical_gross_unit_cost": cel.historical_gross_unit_cost,
                "historical_gross_cost": cel.historical_gross_cost,
                "iva_sales": cel.iva_sales,
                "ila_sales": cel.ila_sales,
                "gross_sales": cel.gross_sales,
                "net_gross_profit": cel.net_gross_profit,
                "gross_commercial_profit": cel.gross_commercial_profit,
                "net_margin_pct": cel.net_margin_pct,
                "gross_commercial_margin_pct": cel.gross_commercial_margin_pct,
                "net_markup_pct": cel.net_markup_pct,
                "gross_commercial_markup_pct": cel.gross_commercial_markup_pct,
                "tax_resolution_method": cel.tax_resolution_method,
                "tax_quality_status": cel.tax_quality_status,
                "tax_category": cel.tax_category,
                "tax_source": cel.tax_source,
                "gross_cost_quality": cel.gross_cost_quality,
                "tax_breakdown_quality": cel.tax_breakdown_quality,
                "total_tax_amount": cel.total_tax_amount,
                "unclassified_tax_amount": cel.unclassified_tax_amount,
            }
        )

    return HistoricalCostResolution(
        detail_id=base.detail_id,
        document_id=base.document_id,
        variant_id=base.variant_id,
        commercial_date=base.commercial_date,
        unit_cost=base.unit_cost,
        total_cost=total_cost,
        cost_source=base.cost_source,
        cost_date=base.cost_date,
        purchase_document_id=base.purchase_document_id,
        supplier_id=base.supplier_id,
        age_days_at_sale=base.age_days_at_sale,
        fallback_level=base.fallback_level,
        is_estimated=base.is_estimated,
        quality_status=base.quality_status,
        resolution_reason=base.resolution_reason,
        conflicting_source_ids=base.conflicting_source_ids,
        quantity=quantity,
        line_net_amount=line_net_amount,
        line_total_amount=line_total_amount,
        gross_profit=eco.gross_profit,
        gross_margin_pct=eco.gross_margin_pct,
        markup_pct=eco.markup_pct,
        **commercial_kwargs,
    )


def select_reception_for_date(
    candidates: list[ReceptionCostCandidate],
    *,
    commercial_date: date,
) -> tuple[ReceptionCostCandidate | None, tuple[int, ...], str | None]:
    eligible = [c for c in candidates if c.cost_date <= commercial_date]
    if not eligible:
        return None, (), None

    best_date = max(c.cost_date for c in eligible)
    at_best = [c for c in eligible if c.cost_date == best_date]
    distinct_costs = {quantize_money(c.cost_net) for c in at_best}
    if len(distinct_costs) > 1:
        ids = tuple(sorted(c.id for c in at_best))
        return None, ids, "multiple_reception_costs_same_date"

    winner = max(at_best, key=lambda c: c.id)
    return winner, (), None


def resolve_unit_cost_from_candidates(
    line: LineCostInput,
    *,
    receptions: list[ReceptionCostCandidate],
    snapshot: VariantCostSnapshot | None,
) -> HistoricalCostResolution:
    eco_kw = {
        "quantity": line.quantity,
        "line_net_amount": line.line_net_amount,
        "line_total_amount": line.line_total_amount,
        "tax_profile": line.tax_profile,
    }
    if line.variant_id is None:
        return _missing(line, reason="line_without_variant_id")

    variant_receptions = [c for c in receptions if c.variant_id == line.variant_id]
    winner, conflict_ids, conflict_reason = select_reception_for_date(
        variant_receptions,
        commercial_date=line.commercial_date,
    )
    if conflict_reason:
        base = HistoricalCostResolution(
            detail_id=line.detail_id,
            document_id=line.document_id,
            variant_id=line.variant_id,
            commercial_date=line.commercial_date,
            unit_cost=None,
            total_cost=None,
            cost_source="cost_reception_history",
            cost_date=None,
            purchase_document_id=None,
            supplier_id=None,
            age_days_at_sale=None,
            fallback_level=CostFallbackLevel.RECEPTION_AT_SALE,
            is_estimated=False,
            quality_status=CostQualityStatus.CONFLICTING_COST,
            resolution_reason=conflict_reason,
            conflicting_source_ids=conflict_ids,
            quantity=line.quantity,
            line_net_amount=line.line_net_amount,
            line_total_amount=line.line_total_amount,
            gross_cost_quality="conflicting_gross_cost",
            tax_breakdown_quality="conflicting_breakdown",
        )
        return _with_economics(base, **eco_kw)

    if winner is not None:
        age = (line.commercial_date - winner.cost_date).days
        base = HistoricalCostResolution(
            detail_id=line.detail_id,
            document_id=line.document_id,
            variant_id=line.variant_id,
            commercial_date=line.commercial_date,
            unit_cost=quantize_money(winner.cost_net),
            total_cost=None,
            cost_source="cost_reception_history",
            cost_date=winner.cost_date,
            purchase_document_id=winner.reception_id,
            supplier_id=None,
            age_days_at_sale=age,
            fallback_level=CostFallbackLevel.RECEPTION_AT_SALE,
            is_estimated=False,
            quality_status=CostQualityStatus.HISTORICAL_REAL,
            resolution_reason="reception_on_or_before_sale",
            quantity=line.quantity,
            line_net_amount=line.line_net_amount,
            line_total_amount=line.line_total_amount,
            reception_iva_amount=winner.iva_amount,
            reception_other_taxes=winner.other_taxes,
            reception_cost_bruto_erp=winner.cost_bruto_erp,
        )
        return _with_economics(base, **eco_kw)

    avg = snapshot.average_cost_net if snapshot is not None else None
    if avg is not None and avg > ZERO:
        base = HistoricalCostResolution(
            detail_id=line.detail_id,
            document_id=line.document_id,
            variant_id=line.variant_id,
            commercial_date=line.commercial_date,
            unit_cost=quantize_money(avg),
            total_cost=None,
            cost_source=snapshot.cost_source or "variant_cost.average_cost_net",
            cost_date=snapshot.last_update if snapshot else None,
            purchase_document_id=None,
            supplier_id=None,
            age_days_at_sale=(
                (line.commercial_date - snapshot.last_update).days
                if snapshot and snapshot.last_update
                else None
            ),
            fallback_level=CostFallbackLevel.AVERAGE_COST,
            is_estimated=True,
            quality_status=CostQualityStatus.AVERAGE_COST_FALLBACK,
            resolution_reason="average_cost_fallback",
            quantity=line.quantity,
            line_net_amount=line.line_net_amount,
            line_total_amount=line.line_total_amount,
        )
        return _with_economics(base, **eco_kw)

    if avg is not None and avg == ZERO:
        return _missing(line, reason="variant_cost_zero_treated_as_missing")

    return _missing(line, reason="no_reception_and_no_average_cost")


class HistoricalCostResolver:
    def __init__(self, repository: CostCandidateRepository) -> None:
        self._repo = repository

    def resolve_lines(
        self,
        lines: list[LineCostInput],
        *,
        company_id: int,
    ) -> list[HistoricalCostResolution]:
        if not lines:
            return []
        variant_ids = sorted({int(ln.variant_id) for ln in lines if ln.variant_id is not None})
        max_date = max(ln.commercial_date for ln in lines)
        receptions = self._repo.fetch_reception_candidates(
            company_id=company_id,
            variant_ids=variant_ids,
            on_or_before=max_date,
        )
        snapshots = self._repo.fetch_variant_snapshots(
            company_id=company_id,
            variant_ids=variant_ids,
        )
        by_variant: dict[int, list[ReceptionCostCandidate]] = defaultdict(list)
        for cand in receptions:
            by_variant[cand.variant_id].append(cand)

        out: list[HistoricalCostResolution] = []
        for line in lines:
            vid = line.variant_id
            out.append(
                resolve_unit_cost_from_candidates(
                    line,
                    receptions=by_variant.get(int(vid), []) if vid is not None else [],
                    snapshot=snapshots.get(int(vid)) if vid is not None else None,
                )
            )
        return out
