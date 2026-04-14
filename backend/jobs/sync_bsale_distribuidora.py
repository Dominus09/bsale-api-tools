"""
Job programado: sync Bsale Distribuidora.

La lógica vive en ``backend.services.distribuidora.sync_service``; este módulo
reexporta nombres usados por ``main.py`` y scripts.
"""

from __future__ import annotations

from backend.services.distribuidora.sync_service import (
    ADVISORY_LOCK_KEY,
    bsale_token_distribuidora_configured,
    resync_bsale_distribuidora_range,
    run_incremental_distribuidora_background,
    run_resync_distribuidora_background,
    sync_bsale_distribuidora_incremental,
)


def sync_bsale_distribuidora(*, strict_token: bool = False):
    """Alias del sync incremental (compatibilidad con el loop de FastAPI)."""
    return sync_bsale_distribuidora_incremental(strict_token=strict_token)


__all__ = [
    "ADVISORY_LOCK_KEY",
    "bsale_token_distribuidora_configured",
    "resync_bsale_distribuidora_range",
    "run_incremental_distribuidora_background",
    "run_resync_distribuidora_background",
    "sync_bsale_distribuidora",
    "sync_bsale_distribuidora_incremental",
]
