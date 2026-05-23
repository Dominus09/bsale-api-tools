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


def next_planning_code(cur) -> str:
    cur.execute("SELECT nextval('distribuidora.dispatch_plan_code_seq')")
    n = int(cur.fetchone()[0])
    return f"PLAN-{n:05d}"


def insert_dispatch_plan(cur, fields: dict[str, Any]) -> int:
    cur.execute(
        """
        INSERT INTO distribuidora.dispatch_plan (
            plan_session_id, planning_date, truck_id, route_name, status,
            planning_code, planning_name, truck_name,
            driver_count, assistant_count, driver_cost_clp, assistant_cost_clp,
            diesel_price_per_liter, km_total, duration_min, liters_estimated,
            fuel_cost_clp, ferry_cost_clp, toll_cost_clp, extras_cost_clp,
            crew_cost_clp, total_route_cost_clp, route_geometry, confirmed_at
        )
        VALUES (
            %(plan_session_id)s, %(planning_date)s, %(truck_id)s, %(route_name)s,
            %(status)s, %(planning_code)s, %(planning_name)s, %(truck_name)s,
            %(driver_count)s, %(assistant_count)s,
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


def list_recent_plans(cur, *, limit: int = 50) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT
            dp.id,
            dp.planning_code,
            dp.planning_name,
            dp.planning_date,
            dp.truck_id,
            COALESCE(NULLIF(BTRIM(dp.truck_name), ''), t.name, dp.route_name) AS truck_name,
            dp.route_name,
            dp.status,
            dp.created_at,
            dp.confirmed_at,
            dp.total_route_cost_clp,
            dp.final_margin_clp,
            dp.net_operational_clp,
            COUNT(dpo.id)::int AS order_count,
            COALESCE(SUM(dpo.oc_total_amount), 0) AS total_oc_amount,
            inv.invoiced_confirmed,
            inv.invoiced_probable,
            inv.invoiced_pending
        FROM distribuidora.dispatch_plan dp
        LEFT JOIN distribuidora.trucks t ON t.id = dp.truck_id
        LEFT JOIN distribuidora.dispatch_plan_orders dpo ON dpo.dispatch_plan_id = dp.id
        LEFT JOIN LATERAL (
            SELECT
                COUNT(*) FILTER (WHERE v.status = 'confirmed')::int AS invoiced_confirmed,
                COUNT(*) FILTER (WHERE v.status = 'probable')::int AS invoiced_probable,
                COUNT(*) FILTER (WHERE v.status = 'missing')::int AS invoiced_pending
            FROM distribuidora.v_dispatch_plan_invoiced_documents v
            WHERE v.dispatch_plan_id = dp.id
        ) inv ON TRUE
        WHERE dp.status <> 'draft'
        GROUP BY dp.id, t.name, inv.invoiced_confirmed, inv.invoiced_probable, inv.invoiced_pending
        ORDER BY dp.created_at DESC
        LIMIT %s
        """,
        (max(1, min(int(limit), 200)),),
    )
    return [dict(zip(_cols(cur), r)) for r in cur.fetchall()]


def update_plan_margin(
    cur,
    plan_id: int,
    *,
    invoiced_sales_clp: int | None = None,
    commercial_margin_clp: int | None = None,
    final_margin_clp: int | None = None,
    net_operational_clp: int | None = None,
    margin_computation_source: str | None = None,
    margin_lines_with_cost: int | None = None,
    margin_lines_total: int | None = None,
) -> None:
    cur.execute(
        """
        UPDATE distribuidora.dispatch_plan
        SET invoiced_sales_clp = %s,
            commercial_margin_clp = %s,
            final_margin_clp = %s,
            net_operational_clp = %s,
            margin_computation_source = %s,
            margin_lines_with_cost = %s,
            margin_lines_total = %s,
            margin_calculated_at = NOW(),
            updated_at = NOW()
        WHERE id = %s
        """,
        (
            invoiced_sales_clp,
            commercial_margin_clp,
            final_margin_clp,
            net_operational_clp,
            margin_computation_source,
            margin_lines_with_cost,
            margin_lines_total,
            plan_id,
        ),
    )


def insert_picking_snapshot(
    cur,
    *,
    plan_id: int,
    picking_type: str,
    payload: str,
) -> int:
    cur.execute(
        """
        INSERT INTO distribuidora.dispatch_plan_picking_snapshots (
            dispatch_plan_id, picking_type, payload
        )
        VALUES (%s, %s, %s::jsonb)
        RETURNING id
        """,
        (plan_id, picking_type, payload),
    )
    return int(cur.fetchone()[0])


def insert_plan_orders(cur, plan_id: int, orders: list[dict[str, Any]]) -> None:
    for o in orders:
        cur.execute(
            """
            INSERT INTO distribuidora.dispatch_plan_orders (
                dispatch_plan_id, oc_document_id, oc_number, route_order,
                client_id, client_name, fantasy_name, address, city, seller_name,
                payment_method, document_type_to_generate, oc_total_amount,
                lat, lng
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (dispatch_plan_id, oc_document_id) DO UPDATE
            SET route_order = EXCLUDED.route_order,
                client_name = EXCLUDED.client_name,
                fantasy_name = EXCLUDED.fantasy_name,
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
                o.get("fantasy_name"),
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
