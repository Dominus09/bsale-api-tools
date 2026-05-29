"""Logs para DeadlockDetected y errores SQL en picking / facturación."""

from __future__ import annotations

import logging
import re
import time
from typing import Any

logger = logging.getLogger(__name__)


def _tables_from_sql(sql: str) -> list[str]:
    found: list[str] = []
    for m in re.finditer(
        r"(?:FROM|JOIN)\s+([\w]+\.[\w]+|[\w]+)",
        sql,
        flags=re.IGNORECASE,
    ):
        t = m.group(1)
        if t.upper() not in ("SELECT", "LATERAL", "TRUE", "FALSE"):
            found.append(t)
    return list(dict.fromkeys(found))


def log_deadlock_debug(
    *,
    plan_id: int | None,
    endpoint: str,
    sql: str,
    exc: BaseException,
    duration_ms: float | None = None,
) -> None:
    err_name = type(exc).__name__
    is_deadlock = "deadlock" in err_name.lower() or "deadlock" in str(exc).lower()
    tag = "[DEADLOCK_DEBUG]" if is_deadlock else "[SQL_ERROR_DEBUG]"
    tables = _tables_from_sql(sql)
    logger.warning(
        "%s\n"
        "endpoint=%s\n"
        "plan_id=%s\n"
        "error=%s\n"
        "duration_ms=%s\n"
        "tables=%s\n"
        "sql=%s",
        tag,
        endpoint,
        plan_id,
        exc,
        f"{duration_ms:.0f}" if duration_ms is not None else None,
        tables,
        " ".join(sql.split()),
    )


def timed_execute(
    cur: Any,
    sql: str,
    params: tuple | list | None,
    *,
    plan_id: int | None,
    endpoint: str,
) -> Any:
    t0 = time.perf_counter()
    try:
        return cur.execute(sql, params)
    except Exception as exc:
        ms = (time.perf_counter() - t0) * 1000.0
        log_deadlock_debug(
            plan_id=plan_id,
            endpoint=endpoint,
            sql=sql,
            exc=exc,
            duration_ms=ms,
        )
        raise
    finally:
        if time.perf_counter() - t0 > 0.5:
            ms = (time.perf_counter() - t0) * 1000.0
            logger.info(
                "[SQL_SLOW] endpoint=%s plan_id=%s duration_ms=%.0f",
                endpoint,
                plan_id,
                ms,
            )
