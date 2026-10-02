"""Recursos de precios y costos (prioridad media)."""

from __future__ import annotations

from backend.services.bsale_raw.core.registry import REGISTRY, KeyKind, Priority, ResourceSpec

TWO_HOURS = 2 * 3600

VARIANT_PRICES = REGISTRY.register(
    ResourceSpec(
        name="variant_prices",
        list_endpoint="/v1/price_lists/{parent_id}/details.json",
        item_endpoint="/v1/price_lists/{parent_id}/details/{id}.json",
        raw_table="bsale_raw.variant_prices",
        key_kind=KeyKind.CHILD,
        priority=Priority.MEDIUM,
        parent="price_lists",
        webhook_topics=("price",),
        point_filters=("variantid", "code", "barcode"),
        freshness_sla_seconds=TWO_HOURS,
        needs_live_verification=(
            "webhook resource /v2/price_lists/{pl}/details.json?variant= ¿equivale a /v1 ?variantid=?",
            "details.json no documenta filtro state ni fecha: sólo full reconcile por lista",
        ),
    )
)

VARIANT_COSTS = REGISTRY.register(
    ResourceSpec(
        name="variant_costs",
        list_endpoint="/v1/variants/{parent_id}/costs.json",
        item_endpoint=None,
        raw_table="bsale_raw.variant_costs",
        key_kind=KeyKind.ENTITY,
        priority=Priority.MEDIUM,
        parent="variants",
        full_reconcile=True,
        freshness_sla_seconds=TWO_HOURS,
        needs_live_verification=(
            "costs.json es 1 request por variante: medir volumen real",
            "history ¿está paginado o completo?",
            "¿responde para variantes inactivas?",
        ),
    )
)
