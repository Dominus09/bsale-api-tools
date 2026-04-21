"""
Job programado: sync Bsale Distribuidora.

La lógica vive en ``backend.services.distribuidora.sync_service``; este módulo
reexporta nombres usados por ``main.py`` y scripts.
"""

from __future__ import annotations

import logging

from backend.services.distribuidora.sync_related_service import (
    run_sync_distribuidora_related_background,
)
from backend.services.distribuidora.sync_service import (
    ADVISORY_LOCK_KEY,
    bsale_token_distribuidora_configured,
    resync_bsale_distribuidora_range,
    run_incremental_distribuidora_background,
    run_resync_distribuidora_background,
    sync_bsale_distribuidora_incremental,
    sync_bsale_distribuidora_orders_incremental,
    sync_bsale_distribuidora_sales_incremental,
)

logger = logging.getLogger(__name__)


def sync_bsale_distribuidora(*, strict_token: bool = False):
    """
    Sync programado: órdenes (tipo 33) y ventas (1/6/9) por separado, luego relaciones documentales.
    """
    orders = sync_bsale_distribuidora_orders_incremental(strict_token=strict_token)
    sales = sync_bsale_distribuidora_sales_incremental(strict_token=strict_token)
    try:
        run_sync_distribuidora_related_background()
    except Exception:
        logger.exception("sync_bsale_distribuidora: related sync tras documentos falló")
    return {"orders": orders, "sales": sales}


__all__ = [
    "ADVISORY_LOCK_KEY",
    "bsale_token_distribuidora_configured",
    "resync_bsale_distribuidora_range",
    "run_incremental_distribuidora_background",
    "run_resync_distribuidora_background",
    "sync_bsale_distribuidora",
    "sync_bsale_distribuidora_incremental",
    "sync_bsale_distribuidora_orders_incremental",
    "sync_bsale_distribuidora_sales_incremental",
]
