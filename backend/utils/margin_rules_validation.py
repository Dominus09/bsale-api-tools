"""Validaciones para edición de bsale.margin_rules (sin alterar cálculo de márgenes)."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any


def parse_margin_value(raw: Any, *, field: str) -> Decimal:
    if raw is None or (isinstance(raw, str) and not raw.strip()):
        raise ValueError(f"{field} es obligatorio.")
    try:
        value = Decimal(str(raw).replace(",", ".").strip())
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field} debe ser numérico.") from exc
    if value < 0:
        raise ValueError(f"{field} no puede ser negativo.")
    return value


def validate_margin_rule_patch(
    *,
    min_margin: Any,
    max_margin: Any,
) -> tuple[Decimal, Decimal, list[str]]:
    """Devuelve (min, max) validados y advertencias no bloqueantes."""
    min_v = parse_margin_value(min_margin, field="Margen mínimo")
    max_v = parse_margin_value(max_margin, field="Margen máximo")
    if min_v > max_v:
        raise ValueError("El margen mínimo no puede ser mayor al margen máximo.")
    warnings: list[str] = []
    if min_v == 0 and max_v == 0:
        warnings.append("min_margin y max_margin son 0: la regla no restringe márgenes.")
    return min_v, max_v, warnings


def margin_rule_key(company_id: int, price_list_id: int, product_type_id: int | None) -> str:
    pt = "" if product_type_id is None else str(product_type_id)
    return f"{company_id}_{price_list_id}_{pt}"
