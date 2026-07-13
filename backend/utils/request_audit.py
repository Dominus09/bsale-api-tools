"""Auditoría temporal de estabilidad para requests HTTP (diagnóstico ECONNRESET).

Instrumentación SIN cambios de lógica comercial:

- ``request_id`` propagado vía contextvar (middleware → router → service → SQL).
- ``RequestAuditMiddleware`` (ASGI puro): REQUEST_START / REQUEST_END / EXCEPTION /
  SEND_FAILED para TODAS las requests, con status, duración, bytes y memoria.
  ``SEND_FAILED`` es la firma servidor del ECONNRESET del cliente: el handler
  terminó pero el socket ya estaba cerrado (proxy/cliente cortó antes).
- ``RequestAudit``: checkpoints por etapa (hora, duración, memoria, request_id) y
  captura de excepciones fatales con stack completo + última query + último paso.
- ``timed_query``: por query registra query_name, rows, execution_ms y
  connection_id (backend PID de PostgreSQL); emite ``[SLOW QUERY]`` sobre umbral.
- ``log_pg_connection_stats``: no existe pool (``backend/db.py`` abre conexión por
  request), así que se registra el uso real de ``pg_stat_activity`` vs
  ``max_connections`` y se emite ``POOL_EXHAUSTED`` al acercarse al límite.
- Si el proceso muere: ``faulthandler`` habilitado + registro en memoria de la
  última función/query por request (``_last_activity``), volcado vía ``atexit``.

Activación: ``REQUEST_AUDIT`` (default on). Umbrales: ``REQUEST_AUDIT_SLOW_QUERY_MS``
(default 500) y ``REQUEST_AUDIT_SLOW_SERIALIZE_MS`` (default 200).
"""

from __future__ import annotations

import atexit
import contextvars
import faulthandler
import logging
import os
import sys
import time
import traceback
import uuid
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger("request_audit")

AUDIT_TAG = "[REQUEST_AUDIT]"

_request_id_var: contextvars.ContextVar[str] = contextvars.ContextVar(
    "request_audit_id", default="-"
)

# Última actividad conocida por si el proceso muere sin traceback (req. 11).
_last_activity: dict[str, Any] = {}

try:
    faulthandler.enable()
except Exception:  # p. ej. stderr no disponible bajo ciertos supervisores
    pass


def request_audit_enabled() -> bool:
    return os.environ.get("REQUEST_AUDIT", "true").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def slow_query_threshold_ms() -> float:
    try:
        return float(os.environ.get("REQUEST_AUDIT_SLOW_QUERY_MS", "500"))
    except ValueError:
        return 500.0


def slow_serialize_threshold_ms() -> float:
    try:
        return float(os.environ.get("REQUEST_AUDIT_SLOW_SERIALIZE_MS", "200"))
    except ValueError:
        return 200.0


def new_request_id() -> str:
    return uuid.uuid4().hex[:12]


def get_request_id() -> str:
    return _request_id_var.get()


def rss_mb() -> float | None:
    """RSS del proceso en MB. psutil → resource (Linux) → ctypes (Windows)."""
    try:
        import psutil  # type: ignore[import-not-found]

        return round(psutil.Process().memory_info().rss / 1048576.0, 1)
    except Exception:
        pass
    try:
        import resource

        # Linux reporta ru_maxrss en KB (pico, no actual; suficiente para tendencia).
        return round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0, 1)
    except Exception:
        pass
    try:
        import ctypes
        import ctypes.wintypes

        class _PMC(ctypes.Structure):
            _fields_ = [
                ("cb", ctypes.wintypes.DWORD),
                ("PageFaultCount", ctypes.wintypes.DWORD),
                ("PeakWorkingSetSize", ctypes.c_size_t),
                ("WorkingSetSize", ctypes.c_size_t),
                ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                ("PagefileUsage", ctypes.c_size_t),
                ("PeakPagefileUsage", ctypes.c_size_t),
            ]

        pmc = _PMC()
        pmc.cb = ctypes.sizeof(_PMC)
        kernel32 = ctypes.windll.kernel32
        kernel32.GetCurrentProcess.restype = ctypes.wintypes.HANDLE
        handle = kernel32.GetCurrentProcess()
        fn = ctypes.windll.psapi.GetProcessMemoryInfo
        fn.argtypes = [
            ctypes.wintypes.HANDLE,
            ctypes.POINTER(_PMC),
            ctypes.wintypes.DWORD,
        ]
        if fn(handle, ctypes.byref(pmc), pmc.cb):
            return round(pmc.WorkingSetSize / 1048576.0, 1)
    except Exception:
        pass
    return None


def allocated_blocks() -> int:
    """Bloques de memoria Python vivos (proxy barato de 'objetos creados')."""
    try:
        return sys.getallocatedblocks()
    except Exception:
        return -1


def classify_exception(exc: BaseException) -> str:
    """Etiqueta de cierre inesperado (req. 3) sin depender de imports opcionales."""
    if isinstance(exc, ConnectionResetError):
        return "ConnectionResetError"
    if isinstance(exc, BrokenPipeError):
        return "BrokenPipeError"
    try:
        import asyncio

        if isinstance(exc, asyncio.CancelledError):
            return "CancelledError"
        if isinstance(exc, asyncio.TimeoutError):
            return "asyncio.TimeoutError"
    except Exception:
        pass
    try:
        import psycopg2

        if isinstance(exc, psycopg2.OperationalError):
            return "psycopg2.OperationalError"
        if isinstance(exc, psycopg2.InterfaceError):
            return "psycopg2.InterfaceError"
    except Exception:
        pass
    return type(exc).__name__


# Marcadores PG de bloqueos / cortes (req. 9), buscados en el mensaje de error.
_PG_FATAL_MARKERS = (
    "deadlock detected",
    "statement timeout",
    "canceling statement",
    "connection lost",
    "server closed the connection",
    "ssl connection has been closed",
    "ssl syscall error",
    "terminating connection",
)


def pg_fatal_marker(exc: BaseException) -> str | None:
    text = str(exc).lower()
    for marker in _PG_FATAL_MARKERS:
        if marker in text:
            return marker
    return None


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def _record_activity(**fields: Any) -> None:
    _last_activity.update(fields, ts=_now_iso())


@atexit.register
def _dump_last_activity_on_exit() -> None:
    if _last_activity:
        # Si el worker muere (OOM kill / señal) esto puede ser el único rastro.
        logger.warning(
            "%s PROCESS_EXIT last_activity=%s", AUDIT_TAG, dict(_last_activity)
        )


class RequestAudit:
    """Checkpoints por etapa de un request + captura de excepción fatal."""

    def __init__(self, endpoint: str) -> None:
        self.endpoint = endpoint
        self.request_id = get_request_id()
        self._t0 = time.perf_counter()
        self._t_last = self._t0
        self.last_step: str = "start"
        self.last_query: str | None = None
        self.rss_start = rss_mb()
        self.blocks_start = allocated_blocks()
        if request_audit_enabled():
            logger.info(
                "%s STEP request_id=%s endpoint=%s step=start ts=%s rss_mb=%s",
                AUDIT_TAG,
                self.request_id,
                endpoint,
                _now_iso(),
                self.rss_start,
            )
        _record_activity(
            request_id=self.request_id, endpoint=endpoint, step="start", query=None
        )

    def elapsed_ms(self) -> float:
        return round((time.perf_counter() - self._t0) * 1000.0, 2)

    def step(self, name: str, **fields: Any) -> None:
        """Registra fin de etapa: hora, duración desde etapa anterior, memoria."""
        now = time.perf_counter()
        step_ms = round((now - self._t_last) * 1000.0, 2)
        self._t_last = now
        self.last_step = name
        _record_activity(request_id=self.request_id, step=name)
        if not request_audit_enabled():
            return
        parts = [
            AUDIT_TAG,
            "STEP",
            f"request_id={self.request_id}",
            f"endpoint={self.endpoint}",
            f"step={name}",
            f"ts={_now_iso()}",
            f"step_ms={step_ms}",
            f"elapsed_ms={self.elapsed_ms()}",
            f"rss_mb={rss_mb()}",
        ]
        for k, v in fields.items():
            if v is not None:
                parts.append(f"{k}={v}")
        logger.info(" ".join(parts))

    def set_query(self, query_name: str) -> None:
        self.last_query = query_name
        _record_activity(request_id=self.request_id, query=query_name)

    def memory_report(self, *, payload_bytes: int | None = None) -> dict[str, Any]:
        rss_end = rss_mb()
        blocks_end = allocated_blocks()
        report = {
            "rss_start_mb": self.rss_start,
            "rss_end_mb": rss_end,
            "rss_delta_mb": (
                round(rss_end - self.rss_start, 1)
                if rss_end is not None and self.rss_start is not None
                else None
            ),
            "objects_delta": (
                blocks_end - self.blocks_start
                if blocks_end >= 0 and self.blocks_start >= 0
                else None
            ),
            "payload_kb": (
                round(payload_bytes / 1024.0, 1) if payload_bytes is not None else None
            ),
        }
        if request_audit_enabled():
            logger.info(
                "%s MEMORY request_id=%s endpoint=%s %s",
                AUDIT_TAG,
                self.request_id,
                self.endpoint,
                " ".join(f"{k}={v}" for k, v in report.items()),
            )
        return report

    def log_fatal(self, exc: BaseException) -> None:
        """Nunca perder un traceback: tipo, mensaje, stack, tiempo, última query/paso."""
        marker = pg_fatal_marker(exc)
        logger.error(
            "%s FATAL request_id=%s endpoint=%s exception_type=%s classified=%s "
            "pg_marker=%s message=%r elapsed_ms=%s last_step=%s last_query=%s\n%s",
            AUDIT_TAG,
            self.request_id,
            self.endpoint,
            type(exc).__name__,
            classify_exception(exc),
            marker,
            str(exc)[:500],
            self.elapsed_ms(),
            self.last_step,
            self.last_query,
            "".join(traceback.format_exception(type(exc), exc, exc.__traceback__)),
        )


def timed_query(
    cur,
    query_name: str,
    sql: str,
    params: Any = None,
    *,
    audit: RequestAudit | None = None,
) -> float:
    """``cur.execute`` instrumentado: query_name, rows, execution_ms, connection_id.

    Emite ``[SLOW QUERY]`` si supera ``REQUEST_AUDIT_SLOW_QUERY_MS`` (500 ms) y
    ``[QUERY_ERROR]`` con clasificación si la query falla. No altera semántica:
    ejecuta exactamente el mismo SQL/params que recibiría ``cur.execute``.
    """
    rid = audit.request_id if audit else get_request_id()
    if audit:
        audit.set_query(query_name)
    else:
        _record_activity(request_id=rid, query=query_name)
    try:
        connection_id = cur.connection.get_backend_pid()
    except Exception:
        connection_id = None

    t0 = time.perf_counter()
    try:
        if params is None:
            cur.execute(sql)
        else:
            cur.execute(sql, params)
    except Exception as exc:
        ms = round((time.perf_counter() - t0) * 1000.0, 2)
        logger.error(
            "%s QUERY_ERROR request_id=%s query_name=%s execution_ms=%s "
            "connection_id=%s classified=%s pg_marker=%s error=%r",
            AUDIT_TAG,
            rid,
            query_name,
            ms,
            connection_id,
            classify_exception(exc),
            pg_fatal_marker(exc),
            str(exc)[:300],
        )
        raise
    ms = round((time.perf_counter() - t0) * 1000.0, 2)
    rows = cur.rowcount
    if request_audit_enabled():
        logger.info(
            "%s QUERY request_id=%s query_name=%s rows=%s execution_ms=%s connection_id=%s",
            AUDIT_TAG,
            rid,
            query_name,
            rows,
            ms,
            connection_id,
        )
    if ms > slow_query_threshold_ms():
        logger.warning(
            "[SLOW QUERY] request_id=%s query_name=%s rows=%s execution_ms=%s "
            "connection_id=%s threshold_ms=%s",
            rid,
            query_name,
            rows,
            ms,
            connection_id,
            slow_query_threshold_ms(),
        )
    return ms


def log_pg_connection_stats(cur, *, label: str) -> dict[str, Any] | None:
    """Estado de conexiones del servidor PG (no hay pool en la app; ver backend/db.py).

    Registra used / idle / waiting / max_connections y emite ``POOL_EXHAUSTED``
    si el uso supera el 90% del límite del servidor.
    """
    if not request_audit_enabled():
        return None
    try:
        cur.execute(
            """
            SELECT
                (SELECT setting::int FROM pg_settings WHERE name = 'max_connections'),
                count(*) FILTER (WHERE state = 'active'),
                count(*) FILTER (WHERE state = 'idle'),
                count(*) FILTER (WHERE wait_event_type IS NOT NULL AND state = 'active'),
                count(*)
            FROM pg_stat_activity
            """
        )
        max_conn, active, idle, waiting, total = cur.fetchone()
    except Exception as exc:
        logger.warning(
            "%s PG_STATS_FAILED request_id=%s label=%s error=%r",
            AUDIT_TAG,
            get_request_id(),
            label,
            str(exc)[:200],
        )
        try:
            cur.connection.rollback()
        except Exception:
            pass
        return None
    stats = {
        "pool_size": "sin_pool_conexion_por_request",
        "max_connections": max_conn,
        "used": total,
        "active": active,
        "idle": idle,
        "waiting": waiting,
    }
    logger.info(
        "%s PG_CONNECTIONS request_id=%s label=%s %s",
        AUDIT_TAG,
        get_request_id(),
        label,
        " ".join(f"{k}={v}" for k, v in stats.items()),
    )
    if max_conn and total >= max_conn * 0.9:
        logger.error(
            "POOL_EXHAUSTED request_id=%s used=%s max_connections=%s "
            "(sin pool en la app: cada request abre conexión nueva)",
            get_request_id(),
            total,
            max_conn,
        )
    return stats


class RequestAuditMiddleware:
    """Middleware ASGI global temporal (req. 10).

    ASGI puro (no ``BaseHTTPMiddleware``) para poder observar los mensajes
    ``http.response.start`` / ``http.response.body`` y capturar errores DURANTE
    el envío: si el cliente/proxy cerró el socket, el fallo ocurre en ``send``
    (firma servidor del ECONNRESET del frontend) y se registra como SEND_FAILED
    con los bytes ya enviados.
    """

    def __init__(self, app) -> None:
        self.app = app

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] != "http" or not request_audit_enabled():
            await self.app(scope, receive, send)
            return

        rid = new_request_id()
        token = _request_id_var.set(rid)
        method = scope.get("method", "-")
        path = scope.get("path", "-")
        t0 = time.perf_counter()
        rss0 = rss_mb()
        state: dict[str, Any] = {
            "status": None,
            "bytes": 0,
            "t_first_byte": None,
            "send_error": None,
            "disconnected": False,
        }

        logger.info(
            "%s REQUEST_START request_id=%s method=%s path=%s ts=%s rss_mb=%s",
            AUDIT_TAG,
            rid,
            method,
            path,
            _now_iso(),
            rss0,
        )

        async def send_wrapper(message) -> None:
            if message["type"] == "http.response.start":
                state["status"] = message.get("status")
                state["t_first_byte"] = time.perf_counter()
            elif message["type"] == "http.response.body":
                state["bytes"] += len(message.get("body") or b"")
            try:
                await send(message)
            except BaseException as exc:
                state["send_error"] = exc
                logger.error(
                    "%s SEND_FAILED request_id=%s method=%s path=%s classified=%s "
                    "status=%s bytes_sent=%s elapsed_ms=%.0f error=%r "
                    "(cliente/proxy cerró el socket antes de recibir la respuesta "
                    "→ ECONNRESET aguas arriba)",
                    AUDIT_TAG,
                    rid,
                    method,
                    path,
                    classify_exception(exc),
                    state["status"],
                    state["bytes"],
                    (time.perf_counter() - t0) * 1000.0,
                    str(exc)[:300],
                )
                raise

        async def receive_wrapper():
            message = await receive()
            if message.get("type") == "http.disconnect":
                state["disconnected"] = True
                logger.warning(
                    "%s CLIENT_DISCONNECT request_id=%s method=%s path=%s "
                    "elapsed_ms=%.0f (el cliente abortó mientras el handler seguía "
                    "trabajando)",
                    AUDIT_TAG,
                    rid,
                    method,
                    path,
                    (time.perf_counter() - t0) * 1000.0,
                )
            return message

        exception: BaseException | None = None
        try:
            await self.app(scope, receive_wrapper, send_wrapper)
        except BaseException as exc:
            exception = exc
            if exc is not state["send_error"]:
                logger.error(
                    "%s EXCEPTION request_id=%s method=%s path=%s exception_type=%s "
                    "classified=%s elapsed_ms=%.0f message=%r last_activity=%s\n%s",
                    AUDIT_TAG,
                    rid,
                    method,
                    path,
                    type(exc).__name__,
                    classify_exception(exc),
                    (time.perf_counter() - t0) * 1000.0,
                    str(exc)[:500],
                    dict(_last_activity),
                    "".join(
                        traceback.format_exception(type(exc), exc, exc.__traceback__)
                    ),
                )
            raise
        finally:
            total_ms = (time.perf_counter() - t0) * 1000.0
            send_ms = (
                (time.perf_counter() - state["t_first_byte"]) * 1000.0
                if state["t_first_byte"] is not None
                else None
            )
            if send_ms is not None and send_ms > slow_serialize_threshold_ms():
                logger.warning(
                    "%s SLOW_SEND request_id=%s path=%s send_ms=%.0f bytes=%s "
                    "(serialización Response/streaming sobre umbral %sms)",
                    AUDIT_TAG,
                    rid,
                    path,
                    send_ms,
                    state["bytes"],
                    slow_serialize_threshold_ms(),
                )
            logger.info(
                "%s REQUEST_END request_id=%s method=%s path=%s status=%s "
                "time_ms=%.0f send_ms=%s bytes=%s rss_mb=%s disconnected=%s exception=%s",
                AUDIT_TAG,
                rid,
                method,
                path,
                state["status"],
                total_ms,
                round(send_ms, 1) if send_ms is not None else None,
                state["bytes"],
                rss_mb(),
                state["disconnected"],
                type(exception).__name__ if exception else None,
            )
            _request_id_var.reset(token)
