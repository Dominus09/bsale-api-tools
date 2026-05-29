"""Logs de auditoría para GET /dispatch-plans/{id}/dashboard."""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

DASHBOARD_PAYLOAD_WARN_BYTES = 500 * 1024
DASHBOARD_DURATION_WARN_MS = 3000.0


def log_dashboard_debug(
    *,
    planning_id: int,
    duration_ms: float,
    invoice_rows: int,
    payload_bytes: int,
    include_margin: bool = False,
    include_items: bool = False,
) -> None:
    logger.info(
        "[DASHBOARD_DEBUG]\n"
        "planning_id=%s\n"
        "duration_ms=%.0f\n"
        "invoice_rows=%s\n"
        "payload_bytes=%s\n"
        "include_margin=%s\n"
        "include_items=%s\n"
        "picking_counts=deferred (use /picking-cliente /picking-producto)",
        planning_id,
        duration_ms,
        invoice_rows,
        payload_bytes,
        include_margin,
        include_items,
    )
    if duration_ms > DASHBOARD_DURATION_WARN_MS:
        logger.warning(
            "[DASHBOARD_WARNING] planning_id=%s duration_ms=%.0f (>%.0f)",
            planning_id,
            duration_ms,
            DASHBOARD_DURATION_WARN_MS,
        )
    if payload_bytes > DASHBOARD_PAYLOAD_WARN_BYTES:
        logger.warning(
            "[DASHBOARD_WARNING] planning_id=%s payload_bytes=%s (> %s); "
            "usar endpoints separados (picking-cliente / picking-producto)",
            planning_id,
            payload_bytes,
            DASHBOARD_PAYLOAD_WARN_BYTES,
        )
