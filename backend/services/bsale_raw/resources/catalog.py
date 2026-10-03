"""Recursos de catálogo (prioridad alta): productos, variantes y clientes.

Observado en fase 2: el listado SIN ``state`` devuelve activos e inactivos. El full scan usa una
sola consulta sin ``state`` (ni ``expand``) y conserva el ``state`` de cada ítem; ``state=0/1`` es
sólo auditoría.

Identidad técnica siempre ``(company_id, bsale_id)``. SKU (``code``) y barcode (``bar_code``) son
columnas de búsqueda: sin unicidad, sin deduplicación; duplicados o vacíos se guardan tal cual.
``product_id`` es la relación que entrega Bsale para ESA empresa: nunca se resuelve ni se repara
por SKU, barcode o nombre.
"""

from __future__ import annotations

from backend.services.bsale_raw.core.rate_limit import RequestPriority
from backend.services.bsale_raw.core.registry import (
    REGISTRY,
    KeyKind,
    Priority,
    ResourceSpec,
    TypedColumn,
    optional_int,
    optional_relation_id,
    optional_text,
)

TWO_HOURS = 2 * 3600

PRODUCTS = REGISTRY.register(
    ResourceSpec(
        name="products",
        list_endpoint="/v1/products.json",
        item_endpoint="/v1/products/{id}.json",
        raw_table="bsale_raw.products",
        key_kind=KeyKind.ENTITY,
        priority=Priority.HIGH,
        request_priority=RequestPriority.P4_CATALOG,
        webhook_topics=("product",),
        state_filter=True,
        expand=("product_type",),
        freshness_sla_seconds=TWO_HOURS,
        typed_columns=(
            TypedColumn("state", "state", optional_int),
            TypedColumn("name", "name", optional_text),
            TypedColumn("product_type_id", "product_type", optional_relation_id),
            TypedColumn("classification", "classification", optional_int),
            TypedColumn("stock_control", "stockControl", optional_int),
        ),
        pipeline_enabled=True,
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
        request_priority=RequestPriority.P4_CATALOG,
        parent="products",
        webhook_topics=("variant",),
        state_filter=True,
        point_filters=("productid", "code", "barcode"),
        freshness_sla_seconds=TWO_HOURS,
        typed_columns=(
            TypedColumn("state", "state", optional_int),
            TypedColumn("product_id", "product", optional_relation_id),
            TypedColumn("code", "code", optional_text),
            TypedColumn("bar_code", "barCode", optional_text),
            TypedColumn("description", "description", optional_text),
            TypedColumn("unlimited_stock", "unlimitedStock", optional_int),
        ),
        pipeline_enabled=True,
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
        request_priority=RequestPriority.P6_CLIENTS_CONFIG,
        state_filter=True,
        point_filters=("code",),
        expand=("contacts", "attributes", "payment_type"),
        freshness_sla_seconds=6 * 3600,
        needs_live_verification=("no hay webhook de clientes documentado en CL",),
    )
)
