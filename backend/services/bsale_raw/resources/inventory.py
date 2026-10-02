"""Recursos de inventario: stock actual (crítico), recepciones y consumos."""

from __future__ import annotations

from backend.services.bsale_raw.core.registry import REGISTRY, KeyKind, Priority, ResourceSpec

STOCKS = REGISTRY.register(
    ResourceSpec(
        name="stocks",
        list_endpoint="/v1/stocks.json",
        item_endpoint="/v1/stocks/{id}.json",
        raw_table="bsale_raw.stocks",
        key_kind=KeyKind.STOCK,
        priority=Priority.CRITICAL,
        webhook_topics=("stock", "document"),
        point_filters=("variantid", "officeid", "code", "barcode"),
        freshness_sla_seconds=15 * 60,
        needs_live_verification=(
            "webhook resource /v2/stocks.json?variant=&office= ¿equivale a /v1 ?variantid=&officeid=?",
            "¿stocks.json devuelve filas de variantes inactivas?",
            "¿existen filas con quantity=0 para todas las combinaciones variante×sucursal?",
            "packs: sólo stock físico",
        ),
    )
)

STOCK_RECEPTIONS = REGISTRY.register(
    ResourceSpec(
        name="stock_receptions",
        list_endpoint="/v1/stocks/receptions.json",
        item_endpoint="/v1/stocks/receptions/{id}.json",
        raw_table="bsale_raw.stock_receptions",
        key_kind=KeyKind.ENTITY,
        priority=Priority.MEDIUM,
        incremental_filter="admissiondate",
        point_filters=("officeid", "documentnumber"),
        freshness_sla_seconds=2 * 3600,
        needs_live_verification=("admissiondate filtra día exacto; no hay rango documentado",),
    )
)

STOCK_RECEPTION_DETAILS = REGISTRY.register(
    ResourceSpec(
        name="stock_reception_details",
        list_endpoint="/v1/stocks/receptions/{parent_id}/details.json",
        item_endpoint="/v1/stocks/receptions/{parent_id}/details/{id}.json",
        raw_table="bsale_raw.stock_reception_details",
        key_kind=KeyKind.CHILD,
        priority=Priority.MEDIUM,
        parent="stock_receptions",
        freshness_sla_seconds=2 * 3600,
    )
)

STOCK_CONSUMPTIONS = REGISTRY.register(
    ResourceSpec(
        name="stock_consumptions",
        list_endpoint="/v1/stocks/consumptions.json",
        item_endpoint="/v1/stocks/consumptions/{id}.json",
        raw_table="bsale_raw.stock_consumptions",
        key_kind=KeyKind.ENTITY,
        priority=Priority.MEDIUM,
        incremental_filter="consumptiondate",
        point_filters=("officeid",),
        freshness_sla_seconds=2 * 3600,
        needs_live_verification=("consumptiondate filtra día exacto; no hay rango documentado",),
    )
)

STOCK_CONSUMPTION_DETAILS = REGISTRY.register(
    ResourceSpec(
        name="stock_consumption_details",
        list_endpoint="/v1/stocks/consumptions/{parent_id}/details.json",
        item_endpoint="/v1/stocks/consumptions/{parent_id}/details/{id}.json",
        raw_table="bsale_raw.stock_consumption_details",
        key_kind=KeyKind.CHILD,
        priority=Priority.MEDIUM,
        parent="stock_consumptions",
        freshness_sla_seconds=2 * 3600,
    )
)
