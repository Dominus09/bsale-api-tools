"""Helpers monetarios con Decimal (sin float para dinero)."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any

MONEY_QUANT = Decimal("0.0001")
PCT_QUANT = Decimal("0.0001")
PCT_COMMERCIAL_QUANT = Decimal("0.01")  # márgenes/markups comerciales UI
ZERO = Decimal("0")


def D(value: Any) -> Decimal:
    """Convierte a Decimal de forma segura. None → error explícito del caller."""
    if isinstance(value, Decimal):
        return value
    if value is None:
        raise TypeError("Cannot convert None to Decimal; use optional handling")
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError(f"Invalid decimal value: {value!r}") from exc


def quantize_money(value: Decimal) -> Decimal:
    return value.quantize(MONEY_QUANT, rounding=ROUND_HALF_UP)


def quantize_pct(value: Decimal) -> Decimal:
    return value.quantize(PCT_QUANT, rounding=ROUND_HALF_UP)


def quantize_commercial_pct(value: Decimal) -> Decimal:
    """Porcentajes comerciales predeterminados (2 decimales, HALF_UP)."""
    return value.quantize(PCT_COMMERCIAL_QUANT, rounding=ROUND_HALF_UP)


def optional_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    return D(value)
