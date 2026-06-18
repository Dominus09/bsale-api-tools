"""Variantes de código de barras para lookup (Excel pierde ceros iniciales)."""

from __future__ import annotations

import re
from typing import Any


def normalize_barcode_read(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return ""
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if value.is_integer():
            return str(int(value))
        return str(value).strip()
    text = str(value).strip()
    if not text:
        return ""
    formula_quoted = re.match(r'^=\s*["\']([^"\']+)["\']\s*$', text, flags=re.I)
    if formula_quoted:
        return formula_quoted.group(1).strip()
    if text.startswith("="):
        text = text[1:].strip()
        inner = re.match(r'^["\']([^"\']+)["\']$', text, flags=re.I)
        if inner:
            return inner.group(1).strip()
    if re.match(r"^\d+\.0+$", text):
        return text.split(".", 1)[0]
    if re.match(r"^[\d.]+e[+-]?\d+$", text, flags=re.I):
        try:
            as_float = float(text)
            if as_float.is_integer():
                return str(int(as_float))
        except ValueError:
            pass
    return text


def barcode_lookup_candidates(read: str) -> list[str]:
    """
    Genera variantes de búsqueda sin quitar ceros del valor leído.
    Solo agrega padding 12/13/14 cuando el código es numérico y más corto.
    """
    base = normalize_barcode_read(read)
    if not base:
        return []

    seen: set[str] = set()
    out: list[str] = []

    def add(candidate: str) -> None:
        c = candidate.strip()
        if c and c not in seen:
            seen.add(c)
            out.append(c)

    add(base)

    if base.isdigit():
        for length in (12, 13, 14):
            if len(base) < length:
                add(base.zfill(length))

    return out
