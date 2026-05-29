"""Logs temporales para auditoría del detalle de planificación."""

from __future__ import annotations

import logging
import time
from contextlib import contextmanager
from typing import Any, Iterator

logger = logging.getLogger(__name__)


def log_plan_detail_debug(
    endpoint: str,
    *,
    planning_id: int | None = None,
    query: str | None = None,
    rows: int | None = None,
    elapsed_ms: float | None = None,
    error: str | None = None,
    extra: dict[str, Any] | None = None,
) -> None:
    parts = [f"[PLAN_DETAIL_DEBUG] endpoint={endpoint}"]
    if planning_id is not None:
        parts.append(f"planning_id={planning_id}")
    if query:
        parts.append(f"query={query}")
    if rows is not None:
        parts.append(f"rows={rows}")
    if elapsed_ms is not None:
        parts.append(f"elapsed_ms={elapsed_ms:.1f}")
    if error:
        parts.append(f"error={error}")
    if extra:
        for k, v in extra.items():
            parts.append(f"{k}={v}")
    if error:
        logger.warning(" ".join(parts))
    else:
        logger.info(" ".join(parts))


@contextmanager
def plan_detail_step(
    endpoint: str,
    *,
    planning_id: int,
    query: str,
) -> Iterator[None]:
    t0 = time.perf_counter()
    log_plan_detail_debug(endpoint, planning_id=planning_id, query=query)
    err: str | None = None
    try:
        yield
    except Exception as exc:
        err = repr(exc)
        raise
    finally:
        elapsed = (time.perf_counter() - t0) * 1000.0
        log_plan_detail_debug(
            endpoint,
            planning_id=planning_id,
            query=query,
            elapsed_ms=elapsed,
            error=err,
        )
