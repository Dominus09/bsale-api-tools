"""Reconciliación encabezado vs suma de líneas (Etapa 2A)."""

from __future__ import annotations

from decimal import Decimal

from backend.services.analytics.document_models import (
    HEADER_LINE_TOLERANCE,
    DocumentReconciliationResult,
    ReconciliationStatus,
)
from backend.services.analytics.money import ZERO, optional_decimal, quantize_money


def reconcile_header_vs_lines(
    *,
    document_id: int,
    header_total_amount: Decimal | None,
    header_net_amount: Decimal | None,
    line_totals: list[Decimal | None],
    line_nets: list[Decimal | None],
    quantities: list[Decimal | None],
    tolerance: Decimal = HEADER_LINE_TOLERANCE,
) -> DocumentReconciliationResult:
    line_count = len(line_totals)
    if line_count == 0:
        return DocumentReconciliationResult(
            document_id=document_id,
            header_total_amount=_q_opt(header_total_amount),
            lines_total_amount=None,
            difference_total=None,
            header_net_amount=_q_opt(header_net_amount),
            lines_net_amount=None,
            difference_net=None,
            line_count=0,
            quantity_total=None,
            reconciliation_status=ReconciliationStatus.MISSING_LINES,
            tolerance=tolerance,
        )

    lines_total = _sum_optional(line_totals)
    lines_net = _sum_optional(line_nets)
    qty_total = _sum_optional(quantities)

    diff_total = _diff(header_total_amount, lines_total)
    diff_net = _diff(header_net_amount, lines_net)

    status = _status(diff_total, diff_net, tolerance)

    return DocumentReconciliationResult(
        document_id=document_id,
        header_total_amount=_q_opt(header_total_amount),
        lines_total_amount=lines_total,
        difference_total=diff_total,
        header_net_amount=_q_opt(header_net_amount),
        lines_net_amount=lines_net,
        difference_net=diff_net,
        line_count=line_count,
        quantity_total=qty_total,
        reconciliation_status=status,
        tolerance=tolerance,
    )


def _q_opt(value: Decimal | None) -> Decimal | None:
    if value is None:
        return None
    return quantize_money(value)


def _sum_optional(values: list[Decimal | None]) -> Decimal | None:
    present = [optional_decimal(v) for v in values if v is not None]
    if not present:
        # Todas None → None (no cero silencioso).
        if values and all(v is None for v in values):
            return None
        return ZERO
    return quantize_money(sum(present, ZERO))


def _diff(header: Decimal | None, lines: Decimal | None) -> Decimal | None:
    if header is None or lines is None:
        return None
    return quantize_money(header - lines)


def _status(
    diff_total: Decimal | None,
    diff_net: Decimal | None,
    tolerance: Decimal,
) -> ReconciliationStatus:
    # Si no hay total comparable, usar neto; si ninguno, mismatch.
    primary = diff_total if diff_total is not None else diff_net
    if primary is None:
        return ReconciliationStatus.MISMATCH
    abs_diff = abs(primary)
    if abs_diff == ZERO:
        # Verificar neto si existe
        if diff_net is not None and abs(diff_net) > tolerance:
            return ReconciliationStatus.MISMATCH
        if diff_net is not None and abs(diff_net) > ZERO:
            return ReconciliationStatus.ROUNDING_DIFFERENCE
        return ReconciliationStatus.MATCHED
    if abs_diff <= tolerance:
        return ReconciliationStatus.ROUNDING_DIFFERENCE
    return ReconciliationStatus.MISMATCH
