"""Utilidades compartidas para dispatch-prep (planning-rows, observaciones)."""

from __future__ import annotations

import json
import logging
import os
from datetime import date
from typing import Any

logger = logging.getLogger(__name__)

LOG_TAG = "[DISPATCH_PREP_DEBUG]"

WIDE_RANGE_DAYS = 7
DEFAULT_DISPATCH_PREP_LIMIT = 500
MAX_DISPATCH_PREP_LIMIT = 500
WIDE_RANGE_WARNING_ES = (
    "Rango amplio, cargando resultados limitados. "
    "Use un rango de una semana o menos para ver todo el detalle."
)


def dispatch_prep_debug_enabled() -> bool:
    return os.environ.get("DISPATCH_PREP_DEBUG", "true").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def range_span_days(d0: date, d1: date) -> int:
    a, b = (d0, d1) if d0 <= d1 else (d1, d0)
    return (b - a).days + 1


def is_wide_date_range(d0: date, d1: date) -> bool:
    return range_span_days(d0, d1) > WIDE_RANGE_DAYS


def effective_page_limit(requested: int, d0: date, d1: date) -> int:
    lim = max(1, min(int(requested), MAX_DISPATCH_PREP_LIMIT))
    if is_wide_date_range(d0, d1):
        return min(lim, MAX_DISPATCH_PREP_LIMIT)
    return lim


def wide_range_meta(d0: date, d1: date) -> dict[str, Any]:
    span = range_span_days(d0, d1)
    wide = span > WIDE_RANGE_DAYS
    return {
        "range_days": span,
        "wide_range": wide,
        "warning": WIDE_RANGE_WARNING_ES if wide else None,
    }


def log_dispatch_prep(
    endpoint: str,
    *,
    date_from: date,
    date_to: date,
    sql_ms: float | None = None,
    total_ms: float | None = None,
    rows_count: int | None = None,
    payload_bytes: int | None = None,
    **extra: Any,
) -> None:
    if not dispatch_prep_debug_enabled():
        return
    parts = [
        LOG_TAG,
        f"endpoint={endpoint}",
        f"date_from={date_from}",
        f"date_to={date_to}",
    ]
    if sql_ms is not None:
        parts.append(f"sql_ms={sql_ms}")
    if total_ms is not None:
        parts.append(f"total_ms={total_ms}")
    if rows_count is not None:
        parts.append(f"rows_count={rows_count}")
    if payload_bytes is not None:
        parts.append(f"payload_bytes={payload_bytes}")
    for k, v in extra.items():
        if v is not None:
            parts.append(f"{k}={v}")
    logger.info(" ".join(parts))


def payload_size_bytes(payload: dict[str, Any]) -> int:
    return len(json.dumps(payload, default=str).encode("utf-8"))
