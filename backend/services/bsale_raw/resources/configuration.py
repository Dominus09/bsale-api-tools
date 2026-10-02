"""Recursos de configuración (prioridad baja): sucursales, impuestos, tipos de documento/producto, listas de precio."""

from __future__ import annotations

from backend.services.bsale_raw.core.rate_limit import RequestPriority
from backend.services.bsale_raw.core.registry import (
    REGISTRY,
    KeyKind,
    Priority,
    ResourceSpec,
    TypedColumn,
    optional_int,
    optional_text,
)

SIX_HOURS = 6 * 3600
P6 = RequestPriority.P6_CLIENTS_CONFIG

OFFICES = REGISTRY.register(
    ResourceSpec(
        name="offices",
        list_endpoint="/v1/offices.json",
        item_endpoint="/v1/offices/{id}.json",
        raw_table="bsale_raw.offices",
        key_kind=KeyKind.ENTITY,
        priority=Priority.LOW,
        request_priority=P6,
        state_filter=True,
        freshness_sla_seconds=SIX_HOURS,
        needs_live_verification=("listado sin state ¿incluye inactivas? (verificado sólo en products/variants/clients)",),
        typed_columns=(
            TypedColumn("state", "state", optional_int),
            TypedColumn("name", "name", optional_text),
            TypedColumn("is_virtual", "isVirtual", optional_int),
            TypedColumn("cost_center", "costCenter", optional_text),
        ),
        pipeline_enabled=True,
    )
)

TAXES = REGISTRY.register(
    ResourceSpec(
        name="taxes",
        list_endpoint="/v1/taxes.json",
        item_endpoint="/v1/taxes/{id}.json",
        raw_table="bsale_raw.taxes",
        key_kind=KeyKind.ENTITY,
        priority=Priority.LOW,
        request_priority=P6,
        state_filter=True,
        freshness_sla_seconds=SIX_HOURS,
    )
)

DOCUMENT_TYPES = REGISTRY.register(
    ResourceSpec(
        name="document_types",
        list_endpoint="/v1/document_types.json",
        item_endpoint="/v1/document_types/{id}.json",
        raw_table="bsale_raw.document_types",
        key_kind=KeyKind.ENTITY,
        priority=Priority.LOW,
        request_priority=P6,
        state_filter=True,
        expand=("book_type",),
        freshness_sla_seconds=SIX_HOURS,
    )
)

PRODUCT_TYPES = REGISTRY.register(
    ResourceSpec(
        name="product_types",
        list_endpoint="/v1/product_types.json",
        item_endpoint="/v1/product_types/{id}.json",
        raw_table="bsale_raw.product_types",
        key_kind=KeyKind.ENTITY,
        priority=Priority.LOW,
        request_priority=P6,
        state_filter=True,
        freshness_sla_seconds=SIX_HOURS,
    )
)

PRICE_LISTS = REGISTRY.register(
    ResourceSpec(
        name="price_lists",
        list_endpoint="/v1/price_lists.json",
        item_endpoint="/v1/price_lists/{id}.json",
        raw_table="bsale_raw.price_lists",
        key_kind=KeyKind.ENTITY,
        priority=Priority.LOW,
        request_priority=P6,
        state_filter=True,
        expand=("coin",),
        freshness_sla_seconds=SIX_HOURS,
    )
)
