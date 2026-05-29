"""Auditoría temporal planificación dispatch_plan (errores 500)."""

from __future__ import annotations

import logging
import os
from typing import Any

from backend.db import get_connection
from backend.repositories.distribuidora import dispatch_plan_repo as repo

logger = logging.getLogger(__name__)

PLAN_DEBUG_RERAISE = os.getenv("PLAN_DEBUG_RERAISE", "true").strip().lower() in (
    "1",
    "true",
    "yes",
)


def _table_exists(cur, name: str) -> bool:
    cur.execute(
        """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'distribuidora'
              AND table_name = %s
        )
        """,
        (name,),
    )
    row = cur.fetchone()
    return bool(row[0]) if row else False


def _view_exists(cur, name: str) -> bool:
    cur.execute(
        """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.views
            WHERE table_schema = 'distribuidora'
              AND table_name = %s
        )
        """,
        (name,),
    )
    row = cur.fetchone()
    return bool(row[0]) if row else False


def collect_plan_debug_context(plan_id: int) -> dict[str, Any]:
    """Estado SQL del plan antes de dashboard / picking."""
    ctx: dict[str, Any] = {
        "plan_id": plan_id,
        "dispatch_plan_exists": False,
        "session_id": None,
        "plan_status": None,
        "orders_count": None,
        "route_stops_count": None,
        "snapshot_count": None,
        "picking_snapshot_count": None,
        "view_invoiced_exists": None,
        "view_purchase_status_exists": None,
        "schema_caps": None,
        "errors": [],
    }

    conn = get_connection()
    try:
        cur = conn.cursor()

        try:
            cur.execute(
                """
                SELECT id, plan_session_id, status
                FROM distribuidora.dispatch_plan
                WHERE id = %s
                """,
                (plan_id,),
            )
            row = cur.fetchone()
            if row:
                ctx["dispatch_plan_exists"] = True
                ctx["session_id"] = row[1]
                ctx["plan_status"] = row[2]
        except Exception as exc:
            ctx["errors"].append(f"dispatch_plan: {exc!r}")

        if _table_exists(cur, "dispatch_plan_orders"):
            try:
                cur.execute(
                    """
                    SELECT COUNT(*)::int
                    FROM distribuidora.dispatch_plan_orders
                    WHERE dispatch_plan_id = %s
                    """,
                    (plan_id,),
                )
                r = cur.fetchone()
                ctx["orders_count"] = int(r[0] or 0) if r else 0
                cur.execute(
                    """
                    SELECT COUNT(DISTINCT route_order)::int
                    FROM distribuidora.dispatch_plan_orders
                    WHERE dispatch_plan_id = %s
                    """,
                    (plan_id,),
                )
                r2 = cur.fetchone()
                ctx["route_stops_count"] = int(r2[0] or 0) if r2 else 0
            except Exception as exc:
                ctx["errors"].append(f"orders_count: {exc!r}")

        ctx["view_invoiced_exists"] = _view_exists(cur, "v_dispatch_plan_invoiced_documents")
        ctx["view_purchase_status_exists"] = _view_exists(
            cur, "v_purchase_document_status_full"
        )

        if _table_exists(cur, "dispatch_plan_picking_snapshots"):
            try:
                cur.execute(
                    """
                    SELECT COUNT(*)::int
                    FROM distribuidora.dispatch_plan_picking_snapshots
                    WHERE dispatch_plan_id = %s
                    """,
                    (plan_id,),
                )
                r = cur.fetchone()
                ctx["picking_snapshot_count"] = int(r[0] or 0) if r else 0
                ctx["snapshot_count"] = ctx["picking_snapshot_count"]
            except Exception as exc:
                ctx["errors"].append(f"picking_snapshots: {exc!r}")

        try:
            ctx["schema_caps"] = repo._history_schema_caps(cur)
        except Exception as exc:
            ctx["errors"].append(f"schema_caps: {exc!r}")

        cur.close()
    except Exception as exc:
        ctx["errors"].append(f"connection: {exc!r}")
    finally:
        conn.close()

    return ctx


def log_plan_debug_context(plan_id: int, endpoint: str) -> dict[str, Any]:
    ctx = collect_plan_debug_context(plan_id)
    logger.info(
        "[PLAN_DEBUG]\n"
        "endpoint=%s\n"
        "plan_id=%s\n"
        "dispatch_plan_exists=%s\n"
        "session_id=%s\n"
        "plan_status=%s\n"
        "orders_count=%s\n"
        "route_stops_count=%s\n"
        "snapshot_count=%s\n"
        "picking_snapshot_count=%s\n"
        "view_invoiced_exists=%s\n"
        "view_purchase_status_exists=%s\n"
        "schema_caps=%s\n"
        "errors=%s",
        endpoint,
        ctx.get("plan_id"),
        ctx.get("dispatch_plan_exists"),
        ctx.get("session_id"),
        ctx.get("plan_status"),
        ctx.get("orders_count"),
        ctx.get("route_stops_count"),
        ctx.get("snapshot_count"),
        ctx.get("picking_snapshot_count"),
        ctx.get("view_invoiced_exists"),
        ctx.get("view_purchase_status_exists"),
        ctx.get("schema_caps"),
        ctx.get("errors"),
    )
    return ctx


def plan_debug_on_error(
    endpoint: str,
    plan_id: int,
    exc: BaseException,
    ctx: dict[str, Any] | None = None,
) -> None:
    logger.exception(
        "[PLAN_DEBUG] %s plan_id=%s error=%s ctx=%s",
        endpoint,
        plan_id,
        str(exc),
        ctx,
    )
