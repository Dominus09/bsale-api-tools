"""Recursos de precios y costos (prioridad media)."""

from __future__ import annotations

from backend.services.bsale_raw.core.rate_limit import RequestPriority
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
        request_priority=RequestPriority.P3_PRICES,
        parent="price_lists",
        webhook_topics=("price",),
        point_filters=("variantid", "code", "barcode"),
        freshness_sla_seconds=TWO_HOURS,
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
        request_priority=RequestPriority.P5_COSTS,
        parent="variants",
        full_reconcile=True,
        freshness_sla_seconds=TWO_HOURS,
        needs_live_verification=(
            "history sin metadata de paginación: NO se declara histórico completo",
            "¿responde para variantes inactivas?",
        ),
    )
)
