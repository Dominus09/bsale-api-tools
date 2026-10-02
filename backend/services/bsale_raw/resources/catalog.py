"""Recursos de catálogo (prioridad alta): productos, variantes y clientes."""

from __future__ import annotations

from backend.services.bsale_raw.core.registry import REGISTRY, KeyKind, Priority, ResourceSpec

TWO_HOURS = 2 * 3600

PRODUCTS = REGISTRY.register(
    ResourceSpec(
        name="products",
        list_endpoint="/v1/products.json",
        item_endpoint="/v1/products/{id}.json",
        raw_table="bsale_raw.products",
        key_kind=KeyKind.ENTITY,
        priority=Priority.HIGH,
        webhook_topics=("product",),
        state_filter=True,
        expand=("product_type", "product_taxes"),
        freshness_sla_seconds=TWO_HOURS,
        needs_live_verification=(
            "listado sin state ¿incluye inactivos?",
            "webhook resource /v2/products/{id}.json ¿equivale a /v1?",
            "expand=[product_taxes] en listado",
        ),
    )
)

VARIANTS = REGISTRY.register(
    ResourceSpec(
        name="variants",
        list_endpoint="/v1/variants.json",
        item_endpoint="/v1/variants/{id}.json",
        raw_table="bsale_raw.variants",
        key_kind=KeyKind.ENTITY,
        priority=Priority.HIGH,
        parent="products",
        webhook_topics=("variant",),
        state_filter=True,
        point_filters=("productid", "code", "barcode"),
        freshness_sla_seconds=TWO_HOURS,
        needs_live_verification=(
            "listado sin state ¿incluye inactivas?",
            "webhook resource /v2/variants/{id}.json ¿equivale a /v1?",
        ),
    )
)

CLIENTS = REGISTRY.register(
    ResourceSpec(
        name="clients",
        list_endpoint="/v1/clients.json",
        item_endpoint="/v1/clients/{id}.json",
        raw_table="bsale_raw.clients",
        key_kind=KeyKind.ENTITY,
        priority=Priority.HIGH,
        state_filter=True,
        point_filters=("code",),
        expand=("contacts", "attributes", "payment_type"),
        freshness_sla_seconds=6 * 3600,
        needs_live_verification=(
            "no hay webhook de clientes documentado en CL",
            "no hay filtro por fecha de modificación: sólo full reconcile",
        ),
    )
)
