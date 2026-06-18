"""Persistencia de costos operacionales por sesión y camión (ORS 2.0)."""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


def _table_exists(cur) -> bool:
    try:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'distribuidora'
              AND table_name = 'route_operational_costs'
            """
        )
        return cur.fetchone() is not None
    except Exception:
        return False


def get_operational_costs(
    cur,
    *,
    plan_session_id: str,
    truck_id: int,
) -> dict[str, Any] | None:
    if not _table_exists(cur):
        return None
    try:
        cur.execute(
            """
            SELECT ferry_clp, per_diem_clp, other_clp, diesel_clp_per_liter
            FROM distribuidora.route_operational_costs
            WHERE plan_session_id = %s AND truck_id = %s
            """,
            (plan_session_id.strip(), int(truck_id)),
        )
    except Exception as exc:
        logger.warning(
            "[ORS_STABILITY_DEBUG] route_operational_costs read error: %s",
            exc,
        )
        return None
    row = cur.fetchone()
    if not row:
        return None
    cols = [d[0] for d in cur.description]
    return dict(zip(cols, row))


def upsert_operational_costs(
    cur,
    *,
    plan_session_id: str,
    truck_id: int,
    ferry_clp: int,
    per_diem_clp: int,
    other_clp: int,
    diesel_clp_per_liter: float | None = None,
) -> None:
    if not _table_exists(cur):
        logger.warning(
            "[ORS_STABILITY_DEBUG] skip upsert_operational_costs — table missing"
        )
        return
    cur.execute(
        """
        INSERT INTO distribuidora.route_operational_costs (
            plan_session_id, truck_id,
            ferry_clp, per_diem_clp, other_clp,
            diesel_clp_per_liter, updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (plan_session_id, truck_id) DO UPDATE
        SET ferry_clp = EXCLUDED.ferry_clp,
            per_diem_clp = EXCLUDED.per_diem_clp,
            other_clp = EXCLUDED.other_clp,
            diesel_clp_per_liter = EXCLUDED.diesel_clp_per_liter,
            updated_at = NOW()
        """,
        (
            plan_session_id.strip(),
            int(truck_id),
            max(0, int(ferry_clp)),
            max(0, int(per_diem_clp)),
            max(0, int(other_clp)),
            round(float(diesel_clp_per_liter), 2) if diesel_clp_per_liter is not None else None,
        ),
    )
