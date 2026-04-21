"""Estado UI de sync tipado (órdenes / ventas) desde ``sync_state`` y ``sync_logs``."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from backend.db import get_connection
from backend.repositories.distribuidora.sync_repo import ensure_distribuidora_schema
from backend.services.distribuidora.sync_service import (
    ADVISORY_LOCK_KEY,
    PROCESS_ORDERS,
    PROCESS_SALES,
)

logger = logging.getLogger(__name__)

_SYNC_LOG_RUNNING_TTL = timedelta(minutes=45)


def _iso_utc(dt: Any) -> str | None:
    if dt is None:
        return None
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    return str(dt)


def _parse_last_message_snapshot(raw: Any) -> dict[str, Any]:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw
    s = str(raw).strip()
    if not s or not s.startswith("{"):
        return {}
    try:
        return json.loads(s)
    except json.JSONDecodeError:
        return {}


def _ui_status_from_log_row(
    row: tuple[Any, ...] | None,
    *,
    sync_state_status: str | None,
) -> str:
    """
    ``ok`` | ``running`` | ``error``.

    * ``running``: último log sin ``finished_at`` y ``started_at`` reciente.
    * ``error``: último log terminado en error, o ``sync_state.last_status``.
    """
    if sync_state_status and str(sync_state_status).lower() == "error":
        return "error"
    if not row:
        return "ok"
    status, finished_at, started_at = row[0], row[1], row[2]
    st = str(status or "").lower()
    if st == "running" and finished_at is None and started_at is not None:
        try:
            sa = started_at if isinstance(started_at, datetime) else datetime.fromisoformat(
                str(started_at).replace("Z", "+00:00")
            )
            if sa.tzinfo is None:
                sa = sa.replace(tzinfo=timezone.utc)
            if datetime.now(timezone.utc) - sa <= _SYNC_LOG_RUNNING_TTL:
                return "running"
        except Exception:
            logger.debug("sync-status: no se pudo interpretar started_at", exc_info=True)
    if st == "error":
        return "error"
    return "ok"


def _fetch_latest_log(cur, process_name: str) -> tuple[Any, ...] | None:
    cur.execute(
        """
        SELECT status, finished_at, started_at
        FROM distribuidora.sync_logs
        WHERE process_name = %s
        ORDER BY id DESC
        LIMIT 1
        """,
        (process_name,),
    )
    return cur.fetchone()


def _fetch_sync_state_row(cur, process_name: str) -> dict[str, Any] | None:
    cur.execute(
        """
        SELECT process_name, last_sync, last_status, last_message, updated_at
        FROM distribuidora.sync_state
        WHERE process_name = %s
        """,
        (process_name,),
    )
    r = cur.fetchone()
    if not r:
        return None
    return {
        "process_name": r[0],
        "last_sync": r[1],
        "last_status": r[2],
        "last_message": r[3],
        "updated_at": r[4],
    }


def get_distribuidora_sync_status_payload() -> dict[str, Any]:
    """
    Combina ``sync_state`` (``documents_orders``, ``documents_sales``) con el último ``sync_logs``.

    Métricas numéricas provienen del JSON en ``last_message`` escrito al finalizar cada sync OK.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        ensure_distribuidora_schema(cur)
        conn.commit()

        cur.execute("SELECT pg_try_advisory_lock(%s)", (ADVISORY_LOCK_KEY,))
        lock_busy = not bool(cur.fetchone()[0])
        if not lock_busy:
            cur.execute("SELECT pg_advisory_unlock(%s)", (ADVISORY_LOCK_KEY,))

        orders_state = _fetch_sync_state_row(cur, PROCESS_ORDERS)
        sales_state = _fetch_sync_state_row(cur, PROCESS_SALES)

        orders_log = _fetch_latest_log(cur, PROCESS_ORDERS)
        sales_log = _fetch_latest_log(cur, PROCESS_SALES)

        o_snap = _parse_last_message_snapshot(
            orders_state.get("last_message") if orders_state else None
        )
        s_snap = _parse_last_message_snapshot(
            sales_state.get("last_message") if sales_state else None
        )

        o_status = _ui_status_from_log_row(
            orders_log,
            sync_state_status=(orders_state or {}).get("last_status"),
        )
        s_status = _ui_status_from_log_row(
            sales_log,
            sync_state_status=(sales_state or {}).get("last_status"),
        )
        if o_snap.get("error"):
            o_status = "error"
        if s_snap.get("error"):
            s_status = "error"

        orders_out = {
            "last_run": _iso_utc((orders_state or {}).get("updated_at"))
            or _iso_utc((orders_state or {}).get("last_sync")),
            "processed": int(o_snap.get("processed") or 0),
            "visibles": int(o_snap.get("visibles") or 0),
            "ocultas": int(o_snap.get("ocultas") or 0),
            "status": o_status,
        }
        sales_out = {
            "last_run": _iso_utc((sales_state or {}).get("updated_at"))
            or _iso_utc((sales_state or {}).get("last_sync")),
            "processed": int(s_snap.get("processed") or 0),
            "boletas": int(s_snap.get("boletas") or 0),
            "facturas": int(s_snap.get("facturas") or 0),
            "nc": int(s_snap.get("nc") or 0),
            "monto_neto": float(s_snap.get("monto_neto") or 0),
            "status": s_status,
        }

        cur.close()
        return {
            "orders": orders_out,
            "sales": sales_out,
            "sync_lock_active": lock_busy,
        }
    finally:
        try:
            conn.close()
        except Exception:
            pass
