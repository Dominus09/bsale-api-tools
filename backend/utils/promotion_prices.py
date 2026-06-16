"""Lógica pura de precios promocionales (snapshot congelado)."""

from __future__ import annotations

from decimal import Decimal


def calc_sale_price(
    regular_price: Decimal,
    tipo_descuento: str,
    valor: Decimal,
) -> Decimal:
    td = (tipo_descuento or "").strip().lower()
    if td == "porcentaje":
        return (regular_price * (Decimal(1) - valor / Decimal(100))).quantize(Decimal("0.01"))
    if td == "precio_fijo":
        return valor.quantize(Decimal("0.01"))
    raise ValueError("tipo_descuento inválido")
