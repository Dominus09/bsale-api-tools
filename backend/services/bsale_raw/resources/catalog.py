"""Recursos de catálogo (prioridad alta): productos, variantes y clientes.

Observado en fase 2: el listado SIN ``state`` devuelve activos e inactivos. El full scan usa una
sola consulta sin ``state`` (ni ``expand``) y conserva el ``state`` de cada ítem; ``state=0/1`` es
sólo auditoría.

Identidad técnica siempre ``(company_id, bsale_id)``. SKU (``code``) y barcode (``bar_code``) son
columnas de búsqueda: sin unicidad, sin deduplicación; duplicados o vacíos se guardan tal cual.
``product_id`` es la relación que entrega Bsale para ESA empresa: nunca se resuelve ni se repara
por SKU, barcode o nombre.

Asociaciones: ``variants.product{id}`` → ``product_id`` y ``products.product_type{id}`` →
``product_type_id`` vienen en el mismo ítem del listado (sin requests extra). Sin ``expand``,
``products.product_taxes`` trae sólo ``{href}``; con ``expand=[product_taxes]`` el listado entrega la
relación completa (LIVE VALIDATED C3: probe ``EXPAND_COMPLETE``). La resuelve ``core/product_tax_engine.py``
(no el motor genérico), con ``GET /v1/products/{id}/product_taxes.json`` sólo como respaldo. El paso
``products`` sigue sin ``expand`` para no alterar el payload que ya guarda.
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

PRODUCT_TAXES = REGISTRY.register(
    ResourceSpec(
        name="product_taxes",
        list_endpoint="/v1/products/{parent_id}/product_taxes.json",
        item_endpoint=None,
        raw_table="bsale_raw.product_taxes",
        key_kind=KeyKind.ENTITY,
        priority=Priority.HIGH,
        request_priority=RequestPriority.P4_CATALOG,
        parent="products",
        freshness_sla_seconds=26 * 3600,
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
