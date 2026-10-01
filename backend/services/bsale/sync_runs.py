"""
Registro de corridas de sync Bsale en ``bsale.sync_runs`` (migración 050).

Las tablas existentes no encajan: ``distribuidora.sync_status`` no guarda error/fin/empresas y
``distribuidora.sync_state`` es estado por (sync_type, mode, office_id), no historial por corrida.
Si la tabla aún no existe, el registro se omite con warning (el exit code sigue siendo real).
"""

from __future__ import annotations

import logging
from typing import Any, Iterable

from psycopg2.extras import Json

from backend.services.bsale.sync_common import ConnectionFactory, default_connection_factory

logger = logging.getLogger(__name__)

STATUS_RUNNING = "running"
STATUS_SUCCESS = "success"
STATUS_FAILED = "failed"
STATUS_PARTIAL = "partial"


def compute_run_status(
    *,
    expected_company_ids: Iterable[int],
    processed_company_ids: Iterable[int],
    errors: list[str],
) -> str:
    """
    success sólo si todas las empresas esperadas se procesaron y no hubo ningún error.
    partial si al menos una empresa se procesó completa; failed en otro caso.
    """
    expected = set(expected_company_ids)
    processed = set(processed_company_ids)
    if expected and expected <= processed and not errors:
        return STATUS_SUCCESS
    if processed:
        return STATUS_PARTIAL
    return STATUS_FAILED


class SyncRunRecorder:
    def __init__(
        self,
        job: str,
        *,
        connection_factory: ConnectionFactory = default_connection_factory,
    ) -> None:
        self.job = job
        self._connection_factory = connection_factory
        self.run_id: int | None = None

    def _execute(self, sql: str, params: tuple) -> Any:
        conn = self._connection_factory()
        try:
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute("SELECT to_regclass('bsale.sync_runs')")
            row = cur.fetchone()
            if not row or row[0] is None:
                logger.warning(
                    "[BSALE_SYNC] bsale.sync_runs no existe; aplicar backend/sql/050_bsale_sync_runs.sql"
                )
                return None
            cur.execute(sql, params)
            return cur.fetchone() if cur.description else None
        finally:
            conn.close()

    def start(self, *, companies_expected: list[int]) -> None:
        try:
            row = self._execute(
                """
                INSERT INTO bsale.sync_runs (job, status, companies_expected)
                VALUES (%s, %s, %s)
                RETURNING id
                """,
                (self.job, STATUS_RUNNING, companies_expected),
            )
            self.run_id = int(row[0]) if row else None
        except Exception:
            logger.exception("[BSALE_SYNC] no se pudo registrar inicio de corrida job=%s", self.job)

    def finish(
        self,
        *,
        status: str,
        error: str | None,
        companies_expected: list[int],
        companies_processed: list[int],
        stats: dict[str, Any],
    ) -> None:
        try:
            if self.run_id is None:
                self._execute(
                    """
                    INSERT INTO bsale.sync_runs (
                        job, status, finished_at, duration_ms, error,
                        companies_expected, companies_processed, stats
                    ) VALUES (%s, %s, NOW(), 0, %s, %s, %s, %s)
                    """,
                    (self.job, status, error, companies_expected, companies_processed, Json(stats)),
                )
                return
            self._execute(
                """
                UPDATE bsale.sync_runs
                SET status = %s,
                    finished_at = NOW(),
                    duration_ms = (EXTRACT(EPOCH FROM (NOW() - started_at)) * 1000)::bigint,
                    error = %s,
                    companies_expected = %s,
                    companies_processed = %s,
                    stats = %s
                WHERE id = %s
                """,
                (status, error, companies_expected, companies_processed, Json(stats), self.run_id),
            )
        except Exception:
            logger.exception("[BSALE_SYNC] no se pudo registrar fin de corrida job=%s", self.job)
