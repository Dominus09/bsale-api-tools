"""Persistencia de dotación y tarifas por ruta en planificación ORS."""

from __future__ import annotations

from typing import Any


def get_route_crew_by_session(cur, plan_session_id: str) -> dict[str, dict[str, Any]]:
    cur.execute(
        """
        SELECT camion, truck_id, driver_count, assistant_count,
               driver_cost_clp, assistant_cost_clp
        FROM distribuidora.ors_plan_route_crew
        WHERE plan_session_id = %s
        """,
        (plan_session_id.strip(),),
    )
    cols = [d[0] for d in cur.description]
    out: dict[str, dict[str, Any]] = {}
    for row in cur.fetchall():
        rec = dict(zip(cols, row))
        out[str(rec["camion"])] = rec
    return out


def upsert_route_crew(
    cur,
    *,
    plan_session_id: str,
    camion: str,
    truck_id: int | None,
    driver_count: int,
    assistant_count: int,
    driver_cost_clp: int,
    assistant_cost_clp: int,
) -> None:
    cur.execute(
        """
        INSERT INTO distribuidora.ors_plan_route_crew (
            plan_session_id, camion, truck_id,
            driver_count, assistant_count,
            driver_cost_clp, assistant_cost_clp, updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (plan_session_id, camion) DO UPDATE
        SET truck_id = EXCLUDED.truck_id,
            driver_count = EXCLUDED.driver_count,
            assistant_count = EXCLUDED.assistant_count,
            driver_cost_clp = EXCLUDED.driver_cost_clp,
            assistant_cost_clp = EXCLUDED.assistant_cost_clp,
            updated_at = NOW()
        """,
        (
            plan_session_id.strip(),
            camion.strip(),
            truck_id,
            int(driver_count),
            int(assistant_count),
            int(driver_cost_clp),
            int(assistant_cost_clp),
        ),
    )
