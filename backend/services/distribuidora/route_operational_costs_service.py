"""Costos operacionales editables en planificación ORS (ferry, viáticos, otros)."""

from __future__ import annotations

from typing import Any

from backend.db import get_connection
from backend.repositories.distribuidora.route_operational_costs_repo import (
    get_operational_costs,
    upsert_operational_costs,
)
from backend.utils.ors_stability import log_error


def _defaults(truck_id: int) -> dict[str, Any]:
    return {
        "plan_session_id": "",
        "truck_id": truck_id,
        "ferry_clp": 0,
        "per_diem_clp": 0,
        "other_clp": 0,
        "diesel_clp_per_liter": None,
    }


def get_route_operational_costs(plan_session_id: str, truck_id: int) -> dict[str, Any]:
    sid = plan_session_id.strip()
    tid = int(truck_id)
    conn = get_connection()
    try:
        cur = conn.cursor()
        row = get_operational_costs(cur, plan_session_id=sid, truck_id=tid)
        cur.close()
    except Exception as exc:
        log_error("GET /planificacion/operational-costs", exc)
        out = _defaults(tid)
        out["plan_session_id"] = sid
        return out
    finally:
        conn.close()

    if not row:
        out = _defaults(tid)
        out["plan_session_id"] = sid
        return out

    diesel = row.get("diesel_clp_per_liter")
    return {
        "plan_session_id": sid,
        "truck_id": tid,
        "ferry_clp": int(row.get("ferry_clp") or 0),
        "per_diem_clp": int(row.get("per_diem_clp") or 0),
        "other_clp": int(row.get("other_clp") or 0),
        "diesel_clp_per_liter": float(diesel) if diesel is not None else None,
    }


def save_route_operational_costs(
    *,
    plan_session_id: str,
    truck_id: int,
    ferry_clp: int = 0,
    per_diem_clp: int = 0,
    other_clp: int = 0,
    diesel_clp_per_liter: float | None = None,
) -> dict[str, Any]:
    sid = plan_session_id.strip()
    tid = int(truck_id)
    conn = get_connection()
    try:
        cur = conn.cursor()
        upsert_operational_costs(
            cur,
            plan_session_id=sid,
            truck_id=tid,
            ferry_clp=max(0, int(ferry_clp)),
            per_diem_clp=max(0, int(per_diem_clp)),
            other_clp=max(0, int(other_clp)),
            diesel_clp_per_liter=diesel_clp_per_liter,
        )
        conn.commit()
        cur.close()
    finally:
        conn.close()
    return get_route_operational_costs(sid, tid)
