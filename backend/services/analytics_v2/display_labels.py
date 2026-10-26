"""Etiquetas comerciales visibles para administración (sin IDs técnicos).

Los identificadores internos (price_list_id, office_id) se conservan en el contrato
JSON para filtros y joins; esta capa solo define qué texto ve el usuario.
"""

from __future__ import annotations

import re
from typing import Any, Mapping

_OFFICE_TECH_SUFFIX = re.compile(r"\s*[·/\-]\s*Oficina\s+\d+\s*$", re.IGNORECASE)
_LISTA_TECH = re.compile(r"^\s*Lista(\s+de\s+precios)?\s+\d+\s*$", re.IGNORECASE)

# Denominación administrativa oficial por empresa → office_id.
# Unifica «Quillotana I» (nunca «Quillotana 1») y nombres cortos de sucursal.
OFFICE_ADMIN_LABELS: dict[int, dict[int, str]] = {
    3: {
        1: "Bodega Central",
        2: "Bodega 1",
        3: "Supermercado",
        4: "Quillotana I",
        5: "Quillotana II",
        6: "Bajas",
    }
}


def humanize_catalog_name(value: str | None) -> str | None:
    """Normaliza nombres RAW en mayúsculas a una forma legible; no inventa texto."""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    letters = [c for c in text if c.isalpha()]
    if letters and sum(1 for c in letters if c.isupper()) / len(letters) >= 0.85:
        parts: list[str] = []
        for token in re.split(r"(\s+|/|\(|\))", text):
            if not token or token.isspace() or token in {"/", "(", ")"}:
                parts.append(token)
            elif token.upper() in {"I", "II", "III", "IV", "V"}:
                parts.append(token.upper())
            else:
                parts.append(token[:1].upper() + token[1:].lower() if len(token) > 1 else token.upper())
        return "".join(parts)
    return text


def price_list_visible_name(
    *,
    name: str | None = None,
    commercial_label: str | None = None,
    price_list_id: int | None = None,
) -> str:
    """Nombre comercial para selectores y columnas Excel. Nunca «Lista 12».

    Prioriza la etiqueta comercial autorizada; el nombre RAW es respaldo.
    """
    label = (commercial_label or "").strip()
    if label and not _LISTA_TECH.match(label):
        return label
    raw = humanize_catalog_name(name)
    if raw and not _LISTA_TECH.match(raw):
        return raw
    if price_list_id is not None:
        return f"Lista sin nombre — requiere corrección del maestro (ref. {int(price_list_id)})"
    return "Lista sin nombre — requiere corrección del maestro"


def price_column_header(visible_name: str) -> str:
    """Encabezado de columna de precio vigente (sin ID)."""
    base = (visible_name or "").strip() or "lista activa"
    if base.lower().startswith("precio "):
        return base
    return f"Precio {base}"


def office_visible_name(
    *,
    name: str | None = None,
    office_id: int | None = None,
    company_id: int | None = None,
) -> str:
    """Nombre de sucursal oficial. Nunca «Oficina 4» ni «Quillotana 1»."""
    if company_id is not None and office_id is not None:
        alias = OFFICE_ADMIN_LABELS.get(int(company_id), {}).get(int(office_id))
        if alias:
            return alias
    text = (name or "").strip()
    if text:
        cleaned = _OFFICE_TECH_SUFFIX.sub("", text).strip()
        # Evitar variantes «Quillotana 1» si el maestro trae romano o dígito.
        lowered = cleaned.lower().replace("quillotana", "quillotana")
        if re.fullmatch(r"quillotana\s*1", lowered, flags=re.IGNORECASE):
            return "Quillotana I"
        if re.fullmatch(r"quillotana\s*2", lowered, flags=re.IGNORECASE):
            return "Quillotana II"
        return humanize_catalog_name(cleaned) or cleaned
    if office_id is not None:
        return f"Sucursal sin nombre — requiere corrección del maestro (ref. {int(office_id)})"
    return "Sucursal sin nombre — requiere corrección del maestro"


def office_map_from_rows(
    rows: list[Mapping[str, Any]],
    *,
    company_id: int | None = None,
) -> dict[int, str]:
    """Mapa office_id → etiqueta visible a partir de filas RAW/catálogo."""
    out: dict[int, str] = {}
    for r in rows:
        oid = r.get("office_id", r.get("bsale_id"))
        if oid is None:
            continue
        out[int(oid)] = office_visible_name(
            name=r.get("name"),
            office_id=int(oid),
            company_id=company_id,
        )
    return out
