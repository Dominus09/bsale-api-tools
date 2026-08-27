"""Utilidades SEC y normalización de texto para módulo Cargas."""

from __future__ import annotations

import math
import re
import unicodedata
from decimal import Decimal, InvalidOperation
from typing import Any

_SEC_RE = re.compile(r"\(SEC\s*([0-9]+)\)", re.IGNORECASE)
_MULTISPACE_RE = re.compile(r"\s+")
_FLOATISH_BARCODE_RE = re.compile(r"^(\d+)\.0+$")


def normalize_barcode(value: Any) -> str | None:
    """
    Normaliza código de barras siempre como texto.

    Acepta str/int/float/Decimal de Excel sin corromper EAN:
    ``7802100505323.0`` → ``\"7802100505323\"`` (nunca ``...3230``).
    Conserva ceros iniciales cuando el valor llega como texto.
    """
    if value is None:
        return None

    # pandas / numpy NaN
    try:
        if value is not None and value != value:  # NaN != NaN
            return None
    except Exception:
        pass

    if isinstance(value, bool):
        return None

    if isinstance(value, int):
        return str(value) if value >= 0 else None

    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        # Excel suele entregar EAN como float entero
        if abs(value - round(value)) < 1e-9:
            return str(int(round(value)))
        return None

    if isinstance(value, Decimal):
        try:
            if value != value:  # pragma: no cover
                return None
            if value == value.to_integral_value():
                return str(int(value))
        except (InvalidOperation, ValueError, OverflowError):
            return None
        return None

    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "null", "<na>", "nat"}:
        return None

    # "7802100505323.0" / "7802100505323.000"
    m = _FLOATISH_BARCODE_RE.fullmatch(text)
    if m:
        return m.group(1)

    # Texto con ceros iniciales u otros dígitos: conservar solo dígitos
    # si el valor es puramente numérico (con posibles separadores no dígito).
    digits = re.sub(r"\D", "", text)
    if digits and re.fullmatch(r"[\d\s\.\-]+", text):
        # Evitar el bug ".0" → dígito extra: ya cubierto por FLOATISH arriba;
        # si quedó algo como "7802.105" no es EAN entero → rechazar
        if "." in text and not _FLOATISH_BARCODE_RE.fullmatch(text.replace(" ", "")):
            # podría ser notación científica Excel
            try:
                as_float = float(text.replace(" ", "").replace(",", "."))
            except ValueError:
                return digits if digits.isdigit() else None
            if math.isfinite(as_float) and abs(as_float - round(as_float)) < 1e-9:
                return str(int(round(as_float)))
            return None
        return digits

    # Alfanumérico raro: conservar limpio sin inventar
    cleaned = text.strip()
    return cleaned or None


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
