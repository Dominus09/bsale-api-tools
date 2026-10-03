"""Recursos de inventario: stock actual (crítico), recepciones y consumos.

Stock (observado en fase 2): count C1=12.587, C2=1.264, C3=35.160; filtros ``variantid``,
``officeid`` y ambos combinados funcionan. El scanner se particiona por company + office, es NO
destructivo (sólo UPSERT) y la frescura se lleva por company + office. El reconcile destructivo
es un proceso separado y menos frecuente. Motor: ``core/stock_engine.py``.

Identidad ``(company_id, variant_id, office_id)``; el ``id`` Bsale del registro va a
``bsale_stock_id`` sin unicidad. Las tres cantidades se guardan tal cual (sin recalcular).
"""

from __future__ import annotations

from backend.services.bsale_raw.core.models import SyncMode
from backend.services.bsale_raw.core.rate_limit import RequestPriority
from backend.services.bsale_raw.core.registry import (
    REGISTRY,
    KeyKind,
    Priority,
    ResourceSpec,
    TypedColumn,
    optional_int,
    optional_numeric,
)

P2 = RequestPriority.P2_STOCK

STOCKS = REGISTRY.register(
    ResourceSpec(
        name="stocks",
        list_endpoint="/v1/stocks.json",
        item_endpoint="/v1/stocks/{id}.json",
        raw_table="bsale_raw.stocks",
        key_kind=KeyKind.STOCK,
        priority=Priority.CRITICAL,
        request_priority=P2,
        webhook_topics=("stock", "document"),
        partition_by_office=True,
        point_filters=("variantid", "officeid", "code", "barcode"),
        freshness_sla_seconds=15 * 60,
        needs_live_verification=(
            "¿filas con quantity=0 para todas las combinaciones variante×sucursal?",
            "¿stocks.json devuelve filas de variantes inactivas?",
        ),
        typed_columns=(
            TypedColumn("bsale_stock_id", "id", optional_int),
            TypedColumn("quantity", "quantity", optional_numeric),
            TypedColumn("quantity_reserved", "quantityReserved", optional_numeric),
            TypedColumn("quantity_available", "quantityAvailable", optional_numeric),
        ),
        pipeline_enabled=True,
        pipeline_modes=(SyncMode.SCANNER, SyncMode.FULL_RECONCILE, SyncMode.POINT),
        point_key="variant",
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
        request_priority=RequestPriority.P5_COSTS,
        incremental_filter="admissiondate",
        point_filters=("officeid", "documentnumber"),
        full_scan_global_allowed=False,
        reconcile_window_days=7,
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
        request_priority=RequestPriority.P5_COSTS,
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
        request_priority=RequestPriority.P5_COSTS,
        incremental_filter="consumptiondate",
        point_filters=("officeid",),
        full_scan_global_allowed=False,
        reconcile_window_days=7,
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
        request_priority=RequestPriority.P5_COSTS,
        parent="stock_consumptions",
        freshness_sla_seconds=2 * 3600,
    )
)
