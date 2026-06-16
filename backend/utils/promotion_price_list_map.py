"""Mapeo empresa → lista de precios para promociones y etiquetas sucursales."""

from __future__ import annotations

COMPANY_PRICE_LIST_MAP: dict[str, str] = {
    "la quillotana spa": "Supermercado La Quillotana",
    "minimarket": "Minimarket",
    "carlos romero": "Quillotana V",
}


def normalize_company_name(name: str) -> str:
    return (name or "").strip().lower()


def mapped_price_list_for_company(company_name: str) -> str | None:
    key = normalize_company_name(company_name)
    if not key:
        return None
    return COMPANY_PRICE_LIST_MAP.get(key)
