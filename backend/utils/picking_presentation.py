"""Normalización de presentación para picking (no altera datos persistidos ni ORS)."""

from __future__ import annotations

import re
import unicodedata
from typing import Any

# Variantes ortográficas conocidas → etiqueta canónica de comuna (solo presentación).
_COMMUNE_CANONICAL: dict[str, str] = {
    "quellon": "Quellón",
    "quellón": "Quellón",
}

_SECTOR_TOKENS = (
    "rural",
    "urbano",
    "urbana",
    "norte",
    "sur",
    "oriente",
    "poniente",
    "centro",
)


def strip_accents(value: str) -> str:
    nfkd = unicodedata.normalize("NFKD", value or "")
    return "".join(ch for ch in nfkd if not unicodedata.combining(ch))


def normalize_commune_key(raw: str | None) -> str:
    """Clave comparable: minúsculas, sin acentos, espacios colapsados."""
    if not raw or not str(raw).strip():
        return ""
    t = strip_accents(str(raw)).strip().lower()
    t = re.sub(r"\s+", " ", t)
    return t


def parse_commune_sector(raw: str | None) -> dict[str, str]:
    """Separa comuna y sector solo para presentación.

    Ej.: ``Quellón rural`` → comuna Quellón, sector Rural.
    No fusiona localidades distintas solo por similitud de nombre.
    """
    label = re.sub(r"\s+", " ", (raw or "").strip())
    if not label:
        return {
            "raw": "",
            "commune": "",
            "sector": "",
            "commune_key": "",
            "display_commune": "",
            "display_sector": "",
            "header_label": "",
        }
    parts = label.split(" ")
    sector = ""
    commune_parts = parts
    if len(parts) >= 2 and strip_accents(parts[-1]).lower() in _SECTOR_TOKENS:
        sector = parts[-1]
        commune_parts = parts[:-1]
    commune_raw = " ".join(commune_parts).strip() or label
    key = normalize_commune_key(commune_raw)
    # Quitar token sector de la clave de comuna si quedó embebido.
    for tok in _SECTOR_TOKENS:
        if key.endswith(" " + tok):
            key = key[: -(len(tok) + 1)].strip()
            if not sector:
                sector = tok
    display_commune = _COMMUNE_CANONICAL.get(key) or _title_es(commune_raw)
    display_sector = _title_es(sector) if sector else ""
    header = display_commune
    if display_sector:
        header = f"{display_commune} · {display_sector}"
    return {
        "raw": label,
        "commune": commune_raw,
        "sector": sector,
        "commune_key": key,
        "display_commune": display_commune,
        "display_sector": display_sector,
        "header_label": header,
    }


def _title_es(value: str) -> str:
    if not value:
        return ""
    return " ".join(p.capitalize() if p else "" for p in value.split(" "))


def visible_stop_number(route_order: Any, *, min_route_order: int | None = None) -> int:
    """Numeración visible 1..N. Si route_order empieza en 0, suma 1."""
    try:
        ro = int(route_order)
    except (TypeError, ValueError):
        return 1
    base = int(min_route_order) if min_route_order is not None else 0
    # Si el mínimo conocido es 0 o el valor es 0, presentación 1-based desde ese piso.
    if base <= 0:
        return ro - base + 1 if ro >= base else ro + 1
    # Si ORS ya usa 1..N, conservar.
    return ro if ro >= 1 else ro + 1


def enrich_client_presentation(
    clients: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Añade campos de presentación sin mutar semántica comercial."""
    orders: list[int] = []
    for c in clients:
        try:
            orders.append(int(c.get("route_order")))
        except (TypeError, ValueError):
            continue
    min_ro = min(orders) if orders else 0
    out: list[dict[str, Any]] = []
    last_commune_key = None
    for c in clients:
        geo = parse_commune_sector(c.get("city"))
        stop = visible_stop_number(c.get("route_order"), min_route_order=min_ro)
        show_header = bool(geo["commune_key"]) and geo["commune_key"] != last_commune_key
        if geo["commune_key"]:
            last_commune_key = geo["commune_key"]
        row = dict(c)
        row["stop_number"] = stop
        row["commune_key"] = geo["commune_key"]
        row["display_commune"] = geo["display_commune"]
        row["display_sector"] = geo["display_sector"]
        row["commune_header"] = geo["header_label"] if show_header else ""
        row["show_commune_header"] = show_header
        out.append(row)
    return out


def commercial_pack_label(*, units_per_box: Any, unidades: Any) -> str | None:
    """Etiqueta de presentación comercial (SEC*) sin convertir a caja física inventada."""
    try:
        upb = float(units_per_box) if units_per_box is not None else 0.0
    except (TypeError, ValueError):
        upb = 0.0
    if upb <= 0:
        return None
    upb_i = int(upb) if float(upb).is_integer() else upb
    try:
        u = float(unidades or 0)
    except (TypeError, ValueError):
        u = 0.0
    packs = None
    if u > 0 and upb > 0:
        packs = round(u / upb, 2)
        if float(packs).is_integer():
            packs = int(packs)
    if packs is None:
        return f"Presentación SEC{upb_i}"
    return f"{packs} × SEC{upb_i}"
