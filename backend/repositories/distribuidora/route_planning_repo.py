"""CRUD ``distribuidora.route_planning`` y ``route_planning_summary``."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

from psycopg2.extras import execute_values

from backend.db import get_connection

_ROUTE_PLANNING_SELECT = """
    id,
    planning_date,
    document_id,
    oc_number,
    client_id,
    client_name,
    municipality,
    address,
    lat,
    lon,
    total_amount,
    truck,
    status,
    route_name,
    driver,
    assistant_1,
    assistant_2,
    departure_time,
    general_observation,
    created_at,
    updated_at
"""

_LINE_PATCHABLE = frozenset(
    {
        "truck",
        "status",
        "route_name",
        "driver",
        "assistant_1",
        "assistant_2",
        "departure_time",
        "general_observation",
    }
)

_SUMMARY_PATCHABLE = frozenset(
    {
        "route_name",
        "driver",
        "assistant_1",
        "assistant_2",
        "departure_time",
        "general_observation",
    }
)


def _in_placeholders(n: int) -> str:
    return ", ".join(["%s"] * n)


def fetch_enriched_orders_by_document_ids(document_ids: list[int]) -> list[dict[str, Any]]:
    if not document_ids:
        return []
    ph = _in_placeholders(len(document_ids))
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            SELECT
                v.document_id,
                v.number AS oc_number,
                v.client_id,
                v.nombre_fantasia AS client_name,
                v.municipality,
                v.address,
                v.total_amount,
                c.lat::double precision AS lat,
                c.lon::double precision AS lon
            FROM distribuidora.v_orders_purchase_enriched v
            LEFT JOIN bsale.clients c
                ON c.company_id = 3
               AND c.bsale_id = v.client_id
            WHERE v.document_id IN ({ph})
            """,
            tuple(document_ids),
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        cur.close()
        conn.close()


def existing_planned_document_ids(planning_date: date, document_ids: list[int]) -> set[int]:
    if not document_ids:
        return set()
    ph = _in_placeholders(len(document_ids))
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            SELECT document_id
            FROM distribuidora.route_planning
            WHERE planning_date = %s
              AND document_id IN ({ph})
            """,
            (planning_date, *document_ids),
        )
        return {int(r[0]) for r in cur.fetchall()}
    finally:
        cur.close()
        conn.close()


def insert_planning_rows(
    planning_date: date,
    truck: str,
    rows: list[dict[str, Any]],
    *,
    route_name: str | None = None,
    driver: str | None = None,
    assistant_1: str | None = None,
    assistant_2: str | None = None,
    departure_time: str | None = None,
    general_observation: str | None = None,
) -> int:
    if not rows:
        return 0
    conn = get_connection()
    cur = conn.cursor()
    try:
        tpl = [
            (
                planning_date,
                int(r["document_id"]),
                r.get("oc_number"),
                r.get("client_id"),
                r.get("client_name"),
                r.get("municipality"),
                r.get("address"),
                r.get("lat"),
                r.get("lon"),
                r.get("total_amount"),
                truck,
                "planned",
                route_name,
                driver,
                assistant_1,
                assistant_2,
                departure_time,
                general_observation,
            )
            for r in rows
        ]
        execute_values(
            cur,
            """
            INSERT INTO distribuidora.route_planning (
                planning_date, document_id, oc_number, client_id, client_name,
                municipality, address, lat, lon, total_amount, truck, status,
                route_name, driver, assistant_1, assistant_2, departure_time,
                general_observation, created_at, updated_at
            ) VALUES %s
            """,
            tpl,
            template=(
                "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),NOW())"
            ),
            page_size=len(tpl),
        )
        conn.commit()
        return len(tpl)
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def list_route_planning(
    planning_date: date,
    truck: str | None = None,
) -> tuple[list[dict[str, Any]], int, Decimal]:
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            SELECT {_ROUTE_PLANNING_SELECT.strip()}
            FROM distribuidora.route_planning
            WHERE planning_date = %s
              AND (%s::text IS NULL OR %s::text = '' OR truck = %s)
            ORDER BY truck ASC, id ASC
            """,
            (planning_date, truck, truck, truck),
        )
        cols = [d[0] for d in cur.description]
        items = [dict(zip(cols, row)) for row in cur.fetchall()]

        cur.execute(
            """
            SELECT
                COUNT(DISTINCT client_id) FILTER (WHERE client_id IS NOT NULL),
                COALESCE(SUM(total_amount), 0)
            FROM distribuidora.route_planning
            WHERE planning_date = %s
              AND (%s::text IS NULL OR %s::text = '' OR truck = %s)
            """,
            (planning_date, truck, truck, truck),
        )
        n_clients, total_amt = cur.fetchone()
        return items, int(n_clients or 0), total_amt if total_amt is not None else Decimal("0")
    finally:
        cur.close()
        conn.close()


def update_route_planning(row_id: int, updates: dict[str, Any]) -> dict[str, Any] | None:
    if not updates:
        return None
    bad = set(updates) - _LINE_PATCHABLE
    if bad:
        raise ValueError(f"Campos no permitidos en route_planning: {sorted(bad)}")
    sets: list[str] = []
    params: list[Any] = []
    for key in sorted(updates.keys()):
        sets.append(f"{key} = %s")
        params.append(updates[key])
    sets.append("updated_at = NOW()")
    params.append(row_id)
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            UPDATE distribuidora.route_planning
            SET {", ".join(sets)}
            WHERE id = %s
            RETURNING {_ROUTE_PLANNING_SELECT.strip()}
            """,
            tuple(params),
        )
        row = cur.fetchone()
        if not row:
            conn.commit()
            return None
        cols = [d[0] for d in cur.description]
        conn.commit()
        return dict(zip(cols, row))
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def delete_route_planning(row_id: int) -> bool:
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "DELETE FROM distribuidora.route_planning WHERE id = %s",
            (row_id,),
        )
        n = cur.rowcount
        conn.commit()
        return n > 0
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def list_route_planning_summaries(planning_date: date) -> list[dict[str, Any]]:
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT
                id,
                planning_date,
                truck,
                route_name,
                driver,
                assistant_1,
                assistant_2,
                departure_time,
                total_clients,
                total_amount,
                general_observation,
                created_at,
                updated_at
            FROM distribuidora.route_planning_summary
            WHERE planning_date = %s
            ORDER BY truck ASC, id ASC
            """,
            (planning_date,),
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        cur.close()
        conn.close()


def update_route_planning_summary(
    summary_id: int,
    updates: dict[str, Any],
) -> dict[str, Any] | None:
    if not updates:
        return None
    bad = set(updates) - _SUMMARY_PATCHABLE
    if bad:
        raise ValueError(f"Campos no permitidos en route_planning_summary: {sorted(bad)}")
    sets: list[str] = []
    params: list[Any] = []
    for key in sorted(updates.keys()):
        sets.append(f"{key} = %s")
        params.append(updates[key])
    sets.append("updated_at = NOW()")
    params.append(summary_id)
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            UPDATE distribuidora.route_planning_summary
            SET {", ".join(sets)}
            WHERE id = %s
            RETURNING
                id,
                planning_date,
                truck,
                route_name,
                driver,
                assistant_1,
                assistant_2,
                departure_time,
                total_clients,
                total_amount,
                general_observation,
                created_at,
                updated_at
            """,
            tuple(params),
        )
        row = cur.fetchone()
        if not row:
            conn.commit()
            return None
        cols = [d[0] for d in cur.description]
        conn.commit()
        return dict(zip(cols, row))
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()
