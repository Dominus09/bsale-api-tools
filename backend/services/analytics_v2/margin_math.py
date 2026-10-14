"""Fórmulas de Márgenes V2 (Decimal, sin división por cero, sin redondeos intermedios).

- Recargo % (política)          = (Precio bruto / Costo bruto − 1) × 100
- Margen sobre venta % (info)    = (Precio bruto − Costo bruto) / Precio bruto × 100
- Precio objetivo mínimo         = Costo bruto × (1 + mínimo / 100), al peso superior
- Precio máximo de política      = Costo bruto × (1 + máximo / 100), al peso inferior
"""

from __future__ import annotations

from decimal import ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_UP, Decimal

HUNDRED = Decimal("100")
PCT_QUANT = Decimal("0.01")
PESO = Decimal("1")


def _positive(value: Decimal | None) -> bool:
    return value is not None and value > 0


def markup_pct(price: Decimal | None, cost: Decimal | None) -> Decimal | None:
    if price is None or not _positive(cost):
        return None
    return (price / cost - 1) * HUNDRED


def margin_on_sale_pct(price: Decimal | None, cost: Decimal | None) -> Decimal | None:
    if cost is None or not _positive(price):
        return None
    return (price - cost) / price * HUNDRED


def target_price(cost: Decimal | None, markup: Decimal | None) -> Decimal | None:
    if not _positive(cost) or markup is None:
        return None
    return (cost * (1 + markup / HUNDRED)).quantize(PESO, rounding=ROUND_CEILING)


def ceiling_price(cost: Decimal | None, markup: Decimal | None) -> Decimal | None:
    if not _positive(cost) or not _positive(markup):
        return None
    return (cost * (1 + markup / HUNDRED)).quantize(PESO, rounding=ROUND_FLOOR)


def round_pct(value: Decimal | None) -> Decimal | None:
    return None if value is None else value.quantize(PCT_QUANT, rounding=ROUND_HALF_UP)
