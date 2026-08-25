"""Utilidades SEC y normalización de texto para módulo Cargas."""

from __future__ import annotations

import re
import unicodedata
from typing import Any

_SEC_RE = re.compile(r"\(SEC\s*([0-9]+)\)", re.IGNORECASE)
_MULTISPACE_RE = re.compile(r"\s+")


def extract_sec(product_text: Any) -> int | None:
    """Extrae SEC N desde '(SEC 24)' en la descripción. Sin fuzzy."""
    if product_text is None:
        return None
    m = _SEC_RE.search(str(product_text))
    if not m:
        return None
    value = int(m.group(1))
    return value if value > 0 else None


def strip_accents(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def normalize_search_text(value: Any) -> str:
    if value is None:
        return ""
    text = strip_accents(str(value)).casefold().strip()
    return _MULTISPACE_RE.sub(" ", text)


def normalize_header(value: Any) -> str:
    text = normalize_search_text(value)
    text = re.sub(r"[^\w\s]", " ", text)
    return _MULTISPACE_RE.sub(" ", text).strip()


def parse_number(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        n = float(value)
        return n if n == n else None
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "-"}:
        return None
    text = text.replace("$", "").replace(" ", "")
    if "," in text and "." in text:
        # 1.234,56 o 1,234.56
        if text.rfind(",") > text.rfind("."):
            text = text.replace(".", "").replace(",", ".")
        else:
            text = text.replace(",", "")
    elif "," in text:
        parts = text.split(",")
        if len(parts) == 2 and len(parts[1]) <= 3:
            text = text.replace(",", ".")
        else:
            text = text.replace(",", "")
    try:
        return float(text)
    except ValueError:
        return None


def boxes_and_loose_from_units(units: float, sec: int | None) -> tuple[float | None, float]:
    """Interpreta unidades vs cajas completas + sueltas usando SEC."""
    if sec is None or sec <= 0:
        return None, float(units)
    full = int(units // sec)
    loose = units - (full * sec)
    return float(full), float(loose)


def units_from_boxes_and_loose(*, boxes: float, loose: float, sec: int | None) -> float:
    if boxes and (sec is None or sec <= 0):
        raise ValueError("No se pueden cargar cajas sin SEC en el producto")
    sec_i = int(sec or 0)
    return float(boxes) * sec_i + float(loose)
