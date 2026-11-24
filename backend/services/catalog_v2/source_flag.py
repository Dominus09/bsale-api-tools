"""Selector reversible de fuente de catálogo — aislado por consumidor.

Default: legacy. Un flag global NO activa V2 en picking/OC/stock.

Variables:
  CATALOG_SOURCE=legacy|v2                 # fallback global (default legacy)
  CATALOG_SOURCE_PUBLIC=legacy|v2          # solo GET /api/catalog (cat.quillotana)
  CATALOG_SOURCE_ERP_PREVIEW=legacy|v2     # solo rutas catalog-v2 (siempre puede leer V2)

Consumidores que NUNCA leen V2 vía este selector (hard deny):
  picking, dispatch, purchase_orders, order_pricing
"""

from __future__ import annotations

import os
from typing import Literal

CatalogSource = Literal["legacy", "v2"]
CatalogConsumer = Literal[
    "public_web",
    "erp_preview",
    "order_pricing",
    "picking",
    "dispatch",
    "purchase_orders",
    "unknown",
]

_HARD_LEGACY_CONSUMERS: frozenset[str] = frozenset(
    {
        "picking",
        "dispatch",
        "purchase_orders",
        "order_pricing",
    }
)


def _normalize(raw: str | None) -> CatalogSource | None:
    if raw is None:
        return None
    key = raw.strip().lower()
    if key in ("legacy", "v1", "catalog_view"):
        return "legacy"
    if key in ("v2", "raw", "bsale_raw"):
        return "v2"
    return None


def resolve_catalog_source(
    *,
    consumer: CatalogConsumer | str,
    override: str | None = None,
) -> CatalogSource:
    """Resuelve fuente efectiva. Default legacy. Hard-deny en consumidores operativos."""
    c = (consumer or "unknown").strip().lower()
    if c in _HARD_LEGACY_CONSUMERS:
        return "legacy"

    if override is not None:
        ov = _normalize(override)
        if ov is not None:
            return ov

    if c == "public_web":
        # Público SOLO cambia con CATALOG_SOURCE_PUBLIC explícito.
        # CATALOG_SOURCE global no basta (evita activación accidental masiva).
        return _normalize(os.getenv("CATALOG_SOURCE_PUBLIC")) or "legacy"

    if c == "erp_preview":
        # Preview interno puede forzar V2 sin tocar público.
        per = _normalize(os.getenv("CATALOG_SOURCE_ERP_PREVIEW"))
        if per is not None:
            return per
        return "v2"

    global_src = _normalize(os.getenv("CATALOG_SOURCE"))
    return global_src or "legacy"


def catalog_source_status() -> dict:
    return {
        "CATALOG_SOURCE": os.getenv("CATALOG_SOURCE") or "(unset→legacy)",
        "CATALOG_SOURCE_PUBLIC": os.getenv("CATALOG_SOURCE_PUBLIC") or "(unset→legacy)",
        "CATALOG_SOURCE_ERP_PREVIEW": os.getenv("CATALOG_SOURCE_ERP_PREVIEW")
        or "(unset→v2 for preview routes)",
        "effective_public_web": resolve_catalog_source(consumer="public_web"),
        "effective_erp_preview": resolve_catalog_source(consumer="erp_preview"),
        "effective_picking": resolve_catalog_source(consumer="picking"),
        "effective_order_pricing": resolve_catalog_source(consumer="order_pricing"),
        "hard_legacy_consumers": sorted(_HARD_LEGACY_CONSUMERS),
        "note_es": (
            "Activar V2 público requiere CATALOG_SOURCE_PUBLIC=v2 explícito. "
            "Picking/OC/order_pricing permanecen en legacy aunque CATALOG_SOURCE=v2. "
            "Si V2 falla con flag público activo → HTTP 503 (sin fallback a legacy)."
        ),
    }
