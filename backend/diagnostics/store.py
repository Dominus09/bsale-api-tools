"""Almacenamiento en memoria (ring buffer) para diagnósticos."""

from __future__ import annotations

import os
import threading
import time
from collections import deque
from dataclasses import dataclass
from typing import Any

_lock = threading.Lock()
_PROCESS_START_MONO = time.monotonic()

_DEFAULT_MAX = 500


def process_uptime_seconds() -> int:
    return int(time.monotonic() - _PROCESS_START_MONO)


def _max_records() -> int:
    try:
        n = int(os.getenv("DIAGNOSTICS_MAX_LOGS", str(_DEFAULT_MAX)))
    except ValueError:
        n = _DEFAULT_MAX
    return max(50, min(5000, n))


@dataclass
class RequestRecord:
    ts_iso: str
    method: str
    path: str
    status_code: int
    duration_ms: float
    client_ip: str | None
    user: str | None
    user_agent: str | None
    origin: str | None
    error: str | None = None


@dataclass
class LogRecordView:
    ts_iso: str
    level: str
    module: str
    message: str
    detail: str | None = None


@dataclass
class ErrorRecordView:
    ts_iso: str
    endpoint: str | None
    status_code: int | None
    message: str
    detail: str | None = None


_requests: deque[RequestRecord] = deque(maxlen=_max_records())
_logs: deque[LogRecordView] = deque(maxlen=_max_records())
_errors: deque[ErrorRecordView] = deque(maxlen=_max_records())


def append_request(rec: RequestRecord) -> None:
    with _lock:
        _requests.append(rec)
        if rec.status_code >= 400 or rec.error:
            _errors.append(
                ErrorRecordView(
                    ts_iso=rec.ts_iso,
                    endpoint=rec.path,
                    status_code=rec.status_code,
                    message=rec.error or f"HTTP {rec.status_code}",
                    detail=None,
                )
            )


def append_log(rec: LogRecordView) -> None:
    with _lock:
        _logs.append(rec)
        if rec.level.upper() in ("ERROR", "CRITICAL"):
            _errors.append(
                ErrorRecordView(
                    ts_iso=rec.ts_iso,
                    endpoint=None,
                    status_code=None,
                    message=rec.message[:500],
                    detail=(rec.detail or "")[:1500] or None,
                )
            )


def list_requests(limit: int = 200) -> list[dict[str, Any]]:
    with _lock:
        items = list(_requests)[-limit:]
    return [__request_to_dict(r) for r in reversed(items)]


def list_logs(limit: int = 200) -> list[dict[str, Any]]:
    with _lock:
        items = list(_logs)[-limit:]
    return [__log_to_dict(r) for r in reversed(items)]


def list_errors(limit: int = 100) -> list[dict[str, Any]]:
    with _lock:
        items = list(_errors)[-limit:]
    return [__error_to_dict(r) for r in reversed(items)]


def __request_to_dict(r: RequestRecord) -> dict[str, Any]:
    return {
        "timestamp": r.ts_iso,
        "method": r.method,
        "path": r.path,
        "statusCode": r.status_code,
        "durationMs": round(r.duration_ms, 2),
        "user": r.user,
        "clientIp": r.client_ip,
        "origin": r.origin,
        "userAgent": r.user_agent,
        "error": r.error,
    }


def __log_to_dict(r: LogRecordView) -> dict[str, Any]:
    return {
        "timestamp": r.ts_iso,
        "level": r.level.lower(),
        "module": r.module,
        "message": r.message,
        "detail": r.detail,
    }


def __error_to_dict(r: ErrorRecordView) -> dict[str, Any]:
    return {
        "timestamp": r.ts_iso,
        "endpoint": r.endpoint,
        "statusCode": r.status_code,
        "message": r.message,
        "detail": r.detail,
    }


def global_avg_duration_ms() -> float | None:
    with _lock:
        if not _requests:
            return None
        durs = [r.duration_ms for r in _requests]
    return round(sum(durs) / len(durs), 2)


def recent_request_count() -> int:
    with _lock:
        return len(_requests)


def recent_error_count() -> int:
    with _lock:
        return len(_errors)


def aggregate_endpoint_stats() -> list[dict[str, Any]]:
    """Promedio simple por método+path desde el buffer de requests."""
    with _lock:
        snap = list(_requests)
    buckets: dict[tuple[str, str], list[float]] = {}
    err_counts: dict[tuple[str, str], int] = {}
    last_ts: dict[tuple[str, str], str] = {}
    for r in snap:
        key = (r.method, r.path)
        buckets.setdefault(key, []).append(r.duration_ms)
        if r.status_code >= 400:
            err_counts[key] = err_counts.get(key, 0) + 1
        last_ts[key] = r.ts_iso
    out: list[dict[str, Any]] = []
    for (method, path), durs in buckets.items():
        out.append(
            {
                "method": method,
                "path": path,
                "description": "",
                "status": "monitored",
                "lastCall": last_ts.get((method, path)),
                "avgDurationMs": round(sum(durs) / len(durs), 2) if durs else 0,
                "recentErrors": err_counts.get((method, path), 0),
                "callCount": len(durs),
            }
        )
    out.sort(key=lambda x: (x["path"], x["method"]))
    return out
