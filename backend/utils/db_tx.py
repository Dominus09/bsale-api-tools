"""Gestión explícita de transacciones PostgreSQL con logs (anti idle-in-transaction)."""

from __future__ import annotations

import logging
import time
from contextlib import contextmanager
from typing import Any, Iterator

logger = logging.getLogger("db_tx")

TX_TAG = "[DB_TX]"


def pg_backend_pid(conn) -> int | None:
    try:
        return int(conn.get_backend_pid())
    except Exception:
        return None


def log_tx(
    event: str,
    *,
    job: str,
    conn=None,
    pid: int | None = None,
    duration_ms: float | None = None,
    error: str | None = None,
    **extra: Any,
) -> None:
    parts = [TX_TAG, event, f"job={job}"]
    resolved_pid = pid if pid is not None else (pg_backend_pid(conn) if conn is not None else None)
    if resolved_pid is not None:
        parts.append(f"pg_pid={resolved_pid}")
    if duration_ms is not None:
        parts.append(f"duration_ms={round(duration_ms, 2)}")
    if error:
        parts.append(f"error={error[:300]!r}")
    for k, v in extra.items():
        if v is not None:
            parts.append(f"{k}={v}")
    logger.info(" ".join(parts))


def safe_rollback(conn, *, job: str) -> None:
    """Rollback que no propaga; registra el evento."""
    try:
        conn.rollback()
        log_tx("ROLLBACK", job=job, conn=conn)
    except Exception as exc:
        log_tx("ROLLBACK_FAILED", job=job, conn=conn, error=str(exc))


def safe_commit(conn, *, job: str) -> None:
    t0 = time.perf_counter()
    conn.commit()
    log_tx(
        "COMMIT",
        job=job,
        conn=conn,
        duration_ms=(time.perf_counter() - t0) * 1000.0,
    )


def release_transaction(conn, *, job: str) -> None:
    """Cierra cualquier transacción abierta (commit no-op si no hay trabajo).

    Usar ANTES de llamadas HTTP para no dejar la sesión en
    ``idle in transaction`` sosteniendo locks (p. ej. AccessShareLock sobre
    ``distribuidora.documents``).
    """
    try:
        conn.commit()
        log_tx("TX_RELEASE", job=job, conn=conn, reason="before_http_or_idle")
    except Exception:
        safe_rollback(conn, job=job)


@contextmanager
def managed_connection(
    get_connection_fn,
    *,
    job: str,
    autocommit: bool = False,
) -> Iterator[Any]:
    """Abre conexión, la cierra en ``finally`` con rollback de seguridad."""
    conn = get_connection_fn()
    pid = pg_backend_pid(conn)
    t0 = time.perf_counter()
    log_tx("CONN_OPEN", job=job, pid=pid, autocommit=autocommit)
    try:
        if autocommit:
            conn.autocommit = True
        yield conn
    except Exception as exc:
        if not autocommit:
            safe_rollback(conn, job=job)
        log_tx(
            "CONN_ERROR",
            job=job,
            pid=pid,
            error=str(exc),
            duration_ms=(time.perf_counter() - t0) * 1000.0,
        )
        raise
    finally:
        try:
            if not getattr(conn, "closed", False) and not autocommit:
                # Evita idle in transaction si el caller olvidó commit/rollback.
                try:
                    conn.rollback()
                except Exception:
                    pass
            conn.close()
            log_tx(
                "CONN_CLOSE",
                job=job,
                pid=pid,
                duration_ms=(time.perf_counter() - t0) * 1000.0,
            )
        except Exception as exc:
            log_tx("CONN_CLOSE_FAILED", job=job, pid=pid, error=str(exc))


@contextmanager
def transaction(conn, *, job: str) -> Iterator[None]:
    """Bloque BEGIN implícito → COMMIT; en error ROLLBACK."""
    pid = pg_backend_pid(conn)
    t0 = time.perf_counter()
    log_tx("TX_BEGIN", job=job, pid=pid)
    try:
        yield
        safe_commit(conn, job=job)
        log_tx(
            "TX_END",
            job=job,
            pid=pid,
            duration_ms=(time.perf_counter() - t0) * 1000.0,
            result="commit",
        )
    except Exception as exc:
        safe_rollback(conn, job=job)
        log_tx(
            "TX_END",
            job=job,
            pid=pid,
            duration_ms=(time.perf_counter() - t0) * 1000.0,
            result="rollback",
            error=str(exc),
        )
        raise
