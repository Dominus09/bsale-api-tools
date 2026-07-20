"""
Runner explícito de migraciones DDL de distribuidora.

    python -m backend.jobs.apply_distribuidora_schema

ÚNICO proceso autorizado a ejecutar ``001_schema.sql`` … y el resto del DDL
versionado. Los syncs / endpoints / crons NO deben aplicar schema.
"""

from __future__ import annotations

import logging
import os
import sys
import time

from backend.db import get_connection
from backend.repositories.distribuidora.sync_repo import (
    DISTRIBUIDORA_SCHEMA_FILES,
    apply_distribuidora_migrations,
)
from backend.utils.bsale_token_env import load_dotenv_if_available
from backend.utils.db_tx import log_tx, pg_backend_pid, safe_commit, safe_rollback

_LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"
JOB = "apply_distribuidora_schema"


def _configure_logging() -> None:
    level = getattr(logging, os.getenv("LOG_LEVEL", "INFO").strip().upper(), logging.INFO)
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(level)
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(logging.Formatter(_LOG_FORMAT))
    root.addHandler(h)


def main() -> int:
    load_dotenv_if_available()
    _configure_logging()
    logger = logging.getLogger(__name__)
    logger.info(
        "%s INICIO files=%s",
        JOB,
        len(DISTRIBUIDORA_SCHEMA_FILES),
    )
    t0 = time.perf_counter()
    conn = get_connection()
    pid = pg_backend_pid(conn)
    log_tx("CONN_OPEN", job=JOB, pid=pid)
    try:
        cur = conn.cursor()
        log_tx("TX_BEGIN", job=JOB, pid=pid)
        applied = apply_distribuidora_migrations(cur)
        safe_commit(conn, job=JOB)
        cur.close()
        elapsed = round(time.perf_counter() - t0, 3)
        logger.info(
            "%s OK applied=%s duration_s=%s pg_pid=%s",
            JOB,
            len(applied),
            elapsed,
            pid,
        )
        print(f"[{JOB}] OK {len(applied)} archivos en {elapsed}s (pg_pid={pid})", flush=True)
        return 0
    except Exception as e:
        safe_rollback(conn, job=JOB)
        logger.exception("%s ERROR: %s", JOB, e)
        print(f"[{JOB}] ERROR: {e}", file=sys.stderr, flush=True)
        return 1
    finally:
        try:
            conn.close()
            log_tx(
                "CONN_CLOSE",
                job=JOB,
                pid=pid,
                duration_ms=(time.perf_counter() - t0) * 1000.0,
            )
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
