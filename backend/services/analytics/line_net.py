"""Asignación determinística de net_amount de línea (Etapa 2A)."""

from __future__ import annotations

from decimal import Decimal

from backend.services.analytics.document_models import LineNetMethod
from backend.services.analytics.money import ZERO, optional_decimal, quantize_money


def allocate_line_nets(
    *,
    header_net_amount: Decimal | None,
    line_nets: list[Decimal | None],
    line_totals: list[Decimal | None],
) -> tuple[list[Decimal | None], LineNetMethod]:
    """Devuelve netos de línea y el método usado.

    - Si todas las líneas traen net_amount → explicit_line_net.
    - Si falta algún neto de línea y hay header_net → prorrateo por total_amount.
    - Si no hay base → unavailable (None, sin inventar ceros silenciosos).

    El último tramo absorbe el residuo para que Σ allocated == header_net
    (salvo header_net None).
    """
    n = len(line_nets)
    if n == 0:
        return [], LineNetMethod.UNAVAILABLE

    if all(v is not None for v in line_nets):
        return [quantize_money(v) for v in line_nets if v is not None], LineNetMethod.EXPLICIT_LINE_NET

    if header_net_amount is None:
        return [None] * n, LineNetMethod.UNAVAILABLE

    header_net = quantize_money(header_net_amount)
    weights: list[Decimal] = []
    for total in line_totals:
        t = optional_decimal(total)
        weights.append(abs(t) if t is not None else ZERO)

    weight_sum = sum(weights, ZERO)
    if weight_sum <= ZERO:
        # Sin pesos: reparte en partes iguales.
        equal = quantize_money(header_net / Decimal(n))
        allocated = [equal] * (n - 1)
        allocated.append(quantize_money(header_net - equal * (n - 1)))
        return allocated, LineNetMethod.ALLOCATED_FROM_HEADER

    allocated: list[Decimal | None] = []
    running = ZERO
    for index, weight in enumerate(weights):
        if index == n - 1:
            allocated.append(quantize_money(header_net - running))
        else:
            share = quantize_money(header_net * (weight / weight_sum))
            allocated.append(share)
            running += share
    return allocated, LineNetMethod.ALLOCATED_FROM_HEADER
