"""Etapas, timeouts y logs SQL para GET /dispatch-plans/{id}/dashboard."""

from __future__ import annotations

import logging
import re
import time
from contextlib import contextmanager
from typing import Any, Iterator

from backend.db import get_connection

logger = logging.getLogger(__name__)

DASHBOARD_STATEMENT_TIMEOUT = "30s"
SQL_SLOW_MS = 500.0

# Referencia para optimización (NO ejecutar en GET /dashboard).
PICKING_CLIENT_COUNT_SQL = """
SELECT COUNT(*)::int
FROM distribuidora.dispatch_plan_orders dpo
INNER JOIN distribuidora.v_dispatch_plan_invoiced_documents inv
    ON inv.dispatch_plan_id = dpo.dispatch_plan_id
   AND inv.oc_document_id = dpo.oc_document_id
   AND inv.status = 'confirmed'
WHERE dpo.dispatch_plan_id = %s
""".strip()

PICKING_PRODUCT_COUNT_SQL = """
SELECT COUNT(*)::int
FROM (
    SELECT 1
    FROM distribuidora.dispatch_plan_orders dpo
    INNER JOIN distribuidora.v_dispatch_plan_invoiced_documents inv
        ON inv.dispatch_plan_id = dpo.dispatch_plan_id
       AND inv.oc_document_id = dpo.oc_document_id
       AND inv.status = 'confirmed'
    INNER JOIN distribuidora.document_details dd
        ON dd.document_id = inv.related_document_id
    LEFT JOIN bsale.products_master pm
        ON pm.barcode = NULLIF(BTRIM(dd.variant_code), '')
    WHERE dpo.dispatch_plan_id = %s
    GROUP BY
        COALESCE(NULLIF(BTRIM(pm.product_type), ''), 'Sin tipo'),
        dd.variant_description,
        NULLIF(BTRIM(dd.variant_code), '')
) g
""".strip()


def log_picking_count_sql_reference(plan_id: int) -> None:
    """Solo log; no ejecuta (evita bloquear dashboard por métricas de picking)."""
    logger.info(
        "[DASHBOARD_PICKING_SQL_REF] plan_id=%s metric=picking_client_count\n%s",
        plan_id,
        PICKING_CLIENT_COUNT_SQL,
    )
    logger.info(
        "[DASHBOARD_PICKING_SQL_REF] plan_id=%s metric=picking_product_count\n%s",
        plan_id,
        PICKING_PRODUCT_COUNT_SQL,
    )


class DashboardStageRun:
    """Marca etapas del dashboard con tiempo acumulado desde el inicio del request."""

    def __init__(self, plan_id: int) -> None:
        self.plan_id = plan_id
        self._t0 = time.perf_counter()
        self.last_stage: str | None = None
        self.last_label: str | None = None

    def elapsed_ms(self) -> float:
        return (time.perf_counter() - self._t0) * 1000.0

    def log_stage(self, stage: str | int, label: str, **extra: Any) -> None:
        self.last_stage = str(stage)
        self.last_label = label
        extra_parts = [f"{k}={v}" for k, v in extra.items()]
        extra_line = "\n".join(extra_parts) if extra_parts else ""
        logger.info(
            "[DASHBOARD_STAGE]\n"
            "stage=%s\n"
            "label=%s\n"
            "plan_id=%s\n"
            "elapsed_ms=%.0f"
            + ("\n%s" if extra_line else ""),
            stage,
            label,
            self.plan_id,
            self.elapsed_ms(),
            extra_line,
        )


def _query_label(query: str) -> str:
    q = " ".join(str(query).split())
    low = q.lower()
    if "v_dispatch_plan_invoiced_documents" in low:
        return "v_dispatch_plan_invoiced_documents"
    if "v_purchase_document_status_full" in low:
        return "v_purchase_document_status_full"
    if "dispatch_plan_orders" in low:
        return "dispatch_plan_orders"
    if "dispatch_plan_picking_snapshots" in low:
        return "dispatch_plan_picking_snapshots"
    if "document_details" in low:
        return "document_details"
    if "compute_plan_commercial_margin" in low or "confirmed_docs" in low:
        return "margin_commercial_cte"
    m = re.search(
        r"\bFROM\s+([\w.]+\.?\w+)",
        q,
        re.IGNORECASE,
    )
    if m:
        return m.group(1)
    return q[:100] + ("…" if len(q) > 100 else "")


class TimedCursor:
    """Cursor con log de duración por execute y aviso si supera SQL_SLOW_MS."""

    def __init__(self, cur: Any, plan_id: int, run: DashboardStageRun | None = None) -> None:
        self._cur = cur
        self._plan_id = plan_id
        self._run = run

    def execute(self, query: str, vars: Any = None) -> Any:
        label = _query_label(query)
        t0 = time.perf_counter()
        stage_hint = ""
        if self._run and self._run.last_label:
            stage_hint = f" stage={self._run.last_stage}({self._run.last_label})"
        logger.info(
            "[DASHBOARD_SQL] start plan_id=%s query=%s%s",
            self._plan_id,
            label,
            stage_hint,
        )
        try:
            return self._cur.execute(query, vars)
        except Exception as exc:
            ms = (time.perf_counter() - t0) * 1000.0
            logger.error(
                "[DASHBOARD_SQL] error plan_id=%s query=%s duration_ms=%.0f error=%s%s",
                self._plan_id,
                label,
                ms,
                exc,
                stage_hint,
            )
            raise
        finally:
            ms = (time.perf_counter() - t0) * 1000.0
            logger.info(
                "[DASHBOARD_SQL] end plan_id=%s query=%s duration_ms=%.0f%s",
                self._plan_id,
                label,
                ms,
                stage_hint,
            )
            if ms >= SQL_SLOW_MS:
                logger.warning(
                    "[DASHBOARD_SQL_SLOW] plan_id=%s query=%s duration_ms=%.0f (>%.0f)%s",
                    self._plan_id,
                    label,
                    ms,
                    SQL_SLOW_MS,
                    stage_hint,
                )

    def __getattr__(self, name: str) -> Any:
        return getattr(self._cur, name)


def log_repo_start(plan_id: int, repo_fn: str, **extra: Any) -> float:
    logger.info(
        "[DASHBOARD_REPO] start plan_id=%s repo=%s extra=%s",
        plan_id,
        repo_fn,
        extra or None,
    )
    return time.perf_counter()


def log_repo_end(
    plan_id: int,
    repo_fn: str,
    t0: float,
    *,
    rows: int | None = None,
    error: str | None = None,
) -> None:
    ms = (time.perf_counter() - t0) * 1000.0
    if error:
        logger.error(
            "[DASHBOARD_REPO] end plan_id=%s repo=%s duration_ms=%.0f error=%s",
            plan_id,
            repo_fn,
            ms,
            error,
        )
    else:
        logger.info(
            "[DASHBOARD_REPO] end plan_id=%s repo=%s duration_ms=%.0f rows=%s",
            plan_id,
            repo_fn,
            ms,
            rows,
        )
    if ms >= SQL_SLOW_MS:
        logger.warning(
            "[DASHBOARD_SQL_SLOW] plan_id=%s repo=%s duration_ms=%.0f (>%.0f)",
            plan_id,
            repo_fn,
            ms,
            SQL_SLOW_MS,
        )


@contextmanager
def dashboard_connection(
    plan_id: int,
    run: DashboardStageRun | None = None,
) -> Iterator[tuple[TimedCursor, Any]]:
    """Conexión con statement_timeout=30s y cursor instrumentado."""
    conn = get_connection()
    try:
        raw = conn.cursor()
        raw.execute(f"SET statement_timeout = '{DASHBOARD_STATEMENT_TIMEOUT}'")
        timed = TimedCursor(raw, plan_id, run)
        yield timed, conn
        timed.close()
    finally:
        conn.close()
