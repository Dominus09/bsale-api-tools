"""Identificación uniforme producto/variante para Excel y payloads Analítica V2."""

from __future__ import annotations

from typing import Any

VARIANT_MISSING = "Variante no informada"
BARCODE_MISSING = "Sin código de barras"
TYPE_MISSING = "Sin tipo"
PRODUCT_MISSING = "Producto no informado"


def format_product_type(name: Any) -> str:
    text = str(name or "").strip()
    return text or TYPE_MISSING


def format_product_plus_variant(product_name: Any, variant_name: Any) -> str:
    product = str(product_name or "").strip()
    variant = str(variant_name or "").strip()
    if product and variant:
        return f"{product} — {variant}"
    if product and not variant:
        return f"{product} — {VARIANT_MISSING}"
    if not product and variant:
        return variant
    return PRODUCT_MISSING


def format_barcode(barcode: Any) -> str:
    text = "" if barcode is None else str(barcode).strip()
    return text or BARCODE_MISSING


def identity_fields(
    *,
    product_type_name: Any = None,
    product_name: Any = None,
    variant_name: Any = None,
    barcode: Any = None,
) -> dict[str, str]:
    return {
        "product_type": format_product_type(product_type_name),
        "product_plus_variant": format_product_plus_variant(product_name, variant_name),
        "barcode": format_barcode(barcode),
    }
