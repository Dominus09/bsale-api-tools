"""Conversión segura de campos JSON Bsale (vacíos, null, strings)."""

from __future__ import annotations

from typing import Any


def _normalized_str(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    s = str(value).strip()
    if not s or s.lower() == "null":
        return None
    return s


def parse_optional_int(value: Any) -> int | None:
    s = _normalized_str(value)
    if s is None:
        return None
    try:
        if s.isdigit() or (s.startswith("-") and s[1:].isdigit()):
            return int(s)
        return int(float(s.replace(",", ".")))
    except (TypeError, ValueError):
        return None


def parse_int(value: Any, default: int = 0) -> int:
    parsed = parse_optional_int(value)
    return default if parsed is None else parsed


def parse_optional_float(value: Any) -> float | None:
    s = _normalized_str(value)
    if s is None:
        return None
    try:
        return float(s.replace(",", "."))
    except (TypeError, ValueError):
        return None


def parse_float(value: Any, default: float = 0.0) -> float:
    parsed = parse_optional_float(value)
    return default if parsed is None else parsed
