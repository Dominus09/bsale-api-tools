"""Persistencia de dispatch_plan y órdenes snapshot."""

from __future__ import annotations

from datetime import date
from typing import Any


def _cols(cur) -> list[str]:
    return [d[0] for d in cur.description]


def get_plan_by_id(cur, plan_id: int) -> dict[str, Any] | None:
    cur.execute(
        """
        SELECT dp.*, t.name AS truck_name
        FROM distribuidora.dispatch_plan dp
        LEFT JOIN distribuidora.trucks t ON t.id = dp.truck_id
        WHERE dp.id = %s
        """,
        (plan_id,),
    )
    row = cur.fetchone()
    if not row:
        return None
    return dict(zip(_cols(cur), row))


def list_plans_by_session(cur, plan_session_id: str) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT dp.*, t.name AS truck_name
        FROM distribuidora.dispatch_plan dp
        LEFT JOIN distribuidora.trucks t ON t.id = dp.truck_id
        WHERE dp.plan_session_id = %s
        ORDER BY dp.created_at DESC
        """,
        (plan_session_id.strip(),),
    )
    return [dict(zip(_cols(cur), r)) for r in cur.fetchall()]


def get_latest_plan_for_truck_session(
    cur,
    *,
    plan_session_id: str,
    truck_id: int,
) -> dict[str, Any] | None:
    cur.execute(
        """
        SELECT dp.*, t.name AS truck_name
        FROM distribuidora.dispatch_plan dp
        LEFT JOIN distribuidora.trucks t ON t.id = dp.truck_id
        WHERE dp.plan_session_id = %s
          AND dp.truck_id = %s
        ORDER BY dp.created_at DESC
        LIMIT 1
        """,
        (plan_session_id.strip(), truck_id),
    )
    row = cur.fetchone()
    if not row:
        return None
    return dict(zip(_cols(cur), row))


def insert_dispatch_plan(cur, fields: dict[str, Any]) -> int:
    cur.execute(
        """
        INSERT INTO distribuidora.dispatch_plan (
            plan_session_id, planning_date, truck_id, route_name, status,
            driver_count, assistant_count, driver_cost_clp, assistant_cost_clp,
            diesel_price_per_liter, km_total, duration_min, liters_estimated,
            fuel_cost_clp, ferry_cost_clp, toll_cost_clp, extras_cost_clp,
            crew_cost_clp, total_route_cost_clp, route_geometry, confirmed_at
        )
        VALUES (
            %(plan_session_id)s, %(planning_date)s, %(truck_id)s, %(route_name)s,
            %(status)s, %(driver_count)s, %(assistant_count)s,
            %(driver_cost_clp)s, %(assistant_cost_clp)s,
            %(diesel_price_per_liter)s, %(km_total)s, %(duration_min)s,
            %(liters_estimated)s, %(fuel_cost_clp)s, %(ferry_cost_clp)s,
            %(toll_cost_clp)s, %(extras_cost_clp)s, %(crew_cost_clp)s,
            %(total_route_cost_clp)s, %(route_geometry)s::jsonb, %(confirmed_at)s
        )
        RETURNING id
        """,
        fields,
    )
    return int(cur.fetchone()[0])


def insert_plan_orders(cur, plan_id: int, orders: list[dict[str, Any]]) -> None:
    for o in orders:
        cur.execute(
            """
            INSERT INTO distribuidora.dispatch_plan_orders (
                dispatch_plan_id, oc_document_id, oc_number, route_order,
                client_id, client_name, address, city, seller_name,
                payment_method, document_type_to_generate, oc_total_amount,
                lat, lng
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (dispatch_plan_id, oc_document_id) DO UPDATE
            SET route_order = EXCLUDED.route_order,
                client_name = EXCLUDED.client_name,
                address = EXCLUDED.address,
                city = EXCLUDED.city,
                seller_name = EXCLUDED.seller_name,
                payment_method = EXCLUDED.payment_method,
                document_type_to_generate = EXCLUDED.document_type_to_generate,
                oc_total_amount = EXCLUDED.oc_total_amount,
                lat = EXCLUDED.lat,
                lng = EXCLUDED.lng
            """,
            (
                plan_id,
                int(o["oc_document_id"]),
                o.get("oc_number"),
                int(o.get("route_order") or 0),
                o.get("client_id"),
                o.get("client_name"),
                o.get("address"),
                o.get("city"),
                o.get("seller_name"),
                o.get("payment_method"),
                o.get("document_type_to_generate"),
                o.get("oc_total_amount"),
                o.get("lat"),
                o.get("lng"),
            ),
        )


def list_plan_orders(cur, plan_id: int) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT *
        FROM distribuidora.dispatch_plan_orders
        WHERE dispatch_plan_id = %s
        ORDER BY route_order ASC, oc_document_id ASC
        """,
        (plan_id,),
    )
    return [dict(zip(_cols(cur), r)) for r in cur.fetchall()]


def update_plan_status(cur, plan_id: int, status: str) -> None:
    cur.execute(
        """
        UPDATE distribuidora.dispatch_plan
        SET status = %s, updated_at = NOW()
        WHERE id = %s
        """,
        (status, plan_id),
    )


def list_invoiced_documents(cur, plan_id: int) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT *
        FROM distribuidora.v_dispatch_plan_invoiced_documents
        WHERE dispatch_plan_id = %s
        ORDER BY route_order ASC, oc_document_id ASC
        """,
        (plan_id,),
    )
    return [dict(zip(_cols(cur), r)) for r in cur.fetchall()]
