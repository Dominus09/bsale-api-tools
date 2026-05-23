"""Persistencia de dispatch_plan y órdenes snapshot."""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

logger = logging.getLogger(__name__)

_HISTORY_SCHEMA_CAPS: dict[str, bool] | None = None


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
    """Listado liviano por sesión (sin route_geometry ni joins pesados)."""
    cur.execute(
        """
        SELECT
            dp.id,
            dp.plan_session_id,
            dp.planning_code,
            dp.planning_name,
            dp.planning_date,
            dp.truck_id,
            dp.route_name,
            dp.status,
            COALESCE(NULLIF(BTRIM(dp.truck_name), ''), NULLIF(BTRIM(t.name), ''), dp.route_name) AS truck_name,
            dp.created_at,
            dp.confirmed_at,
            dp.km_total,
            dp.total_route_cost_clp,
            dp.driver_count,
            dp.assistant_count,
            (SELECT COUNT(*)::int
             FROM distribuidora.dispatch_plan_orders o
             WHERE o.dispatch_plan_id = dp.id) AS order_count,
            (SELECT COALESCE(SUM(o.oc_total_amount), 0)
             FROM distribuidora.dispatch_plan_orders o
             WHERE o.dispatch_plan_id = dp.id) AS total_oc_amount
        FROM distribuidora.dispatch_plan dp
        LEFT JOIN distribuidora.trucks t ON t.id = dp.truck_id
        WHERE dp.plan_session_id = %s
        ORDER BY dp.created_at DESC
        """,
        (plan_session_id.strip(),),
    )
    return [dict(zip(_cols(cur), r)) for r in cur.fetchall()]


def get_plan_header(cur, plan_id: int) -> dict[str, Any] | None:
    """Cabecera del plan sin route_geometry (payload liviano)."""
    caps = _history_schema_caps(cur)
    planning_code = (
        "dp.planning_code"
        if caps["planning_code"]
        else "('PLAN-' || LPAD(dp.id::text, 5, '0')) AS planning_code"
    )
    planning_name = (
        "dp.planning_name"
        if caps["planning_name"]
        else "dp.route_name AS planning_name"
    )
    margin_select = ""
    if caps["commercial_margin_clp"]:
        margin_select = """
            dp.commercial_margin_clp,
            dp.net_operational_clp,
            dp.final_margin_clp,
            dp.margin_computation_source,
        """
    order_sub = (
        """
            (SELECT COUNT(*)::int
             FROM distribuidora.dispatch_plan_orders o
             WHERE o.dispatch_plan_id = dp.id) AS order_count,
            (SELECT COALESCE(SUM(o.oc_total_amount), 0)
             FROM distribuidora.dispatch_plan_orders o
             WHERE o.dispatch_plan_id = dp.id) AS total_oc_amount
        """
        if caps["has_orders_table"]
        else "0::int AS order_count, 0::numeric AS total_oc_amount"
    )
    cur.execute(
        f"""
        SELECT
            dp.id,
            dp.plan_session_id,
            {planning_code},
            {planning_name},
            dp.planning_date,
            dp.truck_id,
            dp.route_name,
            dp.status,
            COALESCE(NULLIF(BTRIM(dp.truck_name), ''), NULLIF(BTRIM(t.name), ''), dp.route_name) AS truck_name,
            dp.created_at,
            dp.confirmed_at,
            dp.km_total,
            dp.duration_min,
            dp.total_route_cost_clp,
            dp.driver_count,
            dp.assistant_count,
            dp.driver_cost_clp,
            dp.assistant_cost_clp,
            dp.fuel_cost_clp,
            dp.crew_cost_clp,
            {margin_select}
            {order_sub}
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


def planning_code_from_id(plan_id: int) -> str:
    """Código único derivado del BIGSERIAL (seguro bajo concurrencia)."""
    return f"PLAN-{int(plan_id):05d}"


def insert_dispatch_plan(cur, fields: dict[str, Any]) -> tuple[int, str]:
    """
    Inserta el plan y asigna planning_code desde el id generado (misma transacción).
    No usar secuencias ni MAX() — evita UniqueViolation en uq_dispatch_plan_planning_code.
    """
    payload = dict(fields)
    payload.pop("planning_code", None)

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
            %(status)s, NULL, %(planning_name)s, %(truck_name)s,
            %(driver_count)s, %(assistant_count)s,
            %(driver_cost_clp)s, %(assistant_cost_clp)s,
            %(diesel_price_per_liter)s, %(km_total)s, %(duration_min)s,
            %(liters_estimated)s, %(fuel_cost_clp)s, %(ferry_cost_clp)s,
            %(toll_cost_clp)s, %(extras_cost_clp)s, %(crew_cost_clp)s,
            %(total_route_cost_clp)s, %(route_geometry)s::jsonb, %(confirmed_at)s
        )
        RETURNING id
        """,
        payload,
    )
    plan_id = int(cur.fetchone()[0])
    planning_code = planning_code_from_id(plan_id)
    cur.execute(
        """
        UPDATE distribuidora.dispatch_plan
        SET planning_code = %s, updated_at = NOW()
        WHERE id = %s
        """,
        (planning_code, plan_id),
    )
    logger.info(
        "[PLANNING_CODE_DEBUG] inserted id=%s generated planning_code=%s",
        plan_id,
        planning_code,
    )
    return plan_id, planning_code


def _history_schema_caps(cur) -> dict[str, bool]:
    """Detecta columnas/vistas disponibles (migraciones 021–025 pueden estar parciales)."""
    global _HISTORY_SCHEMA_CAPS
    if _HISTORY_SCHEMA_CAPS is not None:
        return _HISTORY_SCHEMA_CAPS

    cur.execute(
        """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'distribuidora'
              AND table_name = 'dispatch_plan'
        )
        """
    )
    has_plan_table = bool(cur.fetchone()[0])

    dp_cols: set[str] = set()
    if has_plan_table:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'distribuidora'
              AND table_name = 'dispatch_plan'
            """
        )
        dp_cols = {r[0] for r in cur.fetchall()}

    cur.execute(
        """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'distribuidora'
              AND table_name = 'dispatch_plan_orders'
        )
        """
    )
    has_orders_table = bool(cur.fetchone()[0])

    cur.execute(
        """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.views
            WHERE table_schema = 'distribuidora'
              AND table_name = 'v_dispatch_plan_invoiced_documents'
        )
        """
    )
    has_invoiced_view = bool(cur.fetchone()[0])

    caps = {
        "has_plan_table": has_plan_table,
        "has_orders_table": has_orders_table,
        "planning_code": "planning_code" in dp_cols,
        "planning_name": "planning_name" in dp_cols,
        "truck_name_col": "truck_name" in dp_cols,
        "final_margin_clp": "final_margin_clp" in dp_cols,
        "commercial_margin_clp": "commercial_margin_clp" in dp_cols,
        "net_operational_clp": "net_operational_clp" in dp_cols,
        "invoiced_view": has_invoiced_view,
    }
    _HISTORY_SCHEMA_CAPS = caps
    logger.info("[PLANNING_HISTORY_DEBUG] schema_caps=%s", caps)
    return caps


def _build_list_recent_plans_sql(caps: dict[str, bool]) -> str:
    planning_code = (
        "dp.planning_code"
        if caps["planning_code"]
        else "('PLAN-' || LPAD(dp.id::text, 5, '0')) AS planning_code"
    )
    planning_name = (
        "dp.planning_name"
        if caps["planning_name"]
        else "dp.route_name AS planning_name"
    )
    if caps["truck_name_col"]:
        truck_name = (
            "COALESCE(NULLIF(BTRIM(MAX(dp.truck_name)), ''), "
            "NULLIF(BTRIM(MAX(t.name)), ''), MAX(dp.route_name)) AS truck_name"
        )
    else:
        truck_name = (
            "COALESCE(NULLIF(BTRIM(MAX(t.name)), ''), MAX(dp.route_name)) AS truck_name"
        )
    final_margin = (
        "dp.final_margin_clp"
        if caps["final_margin_clp"]
        else "NULL::integer AS final_margin_clp"
    )
    net_operational = (
        "dp.net_operational_clp"
        if caps["net_operational_clp"]
        else "NULL::integer AS net_operational_clp"
    )

    orders_join = ""
    order_count = "0::int AS order_count"
    total_oc = "0::numeric AS total_oc_amount"
    if caps["has_orders_table"]:
        orders_join = (
            "LEFT JOIN distribuidora.dispatch_plan_orders dpo "
            "ON dpo.dispatch_plan_id = dp.id"
        )
        order_count = "COUNT(dpo.id)::int AS order_count"
        total_oc = "COALESCE(SUM(dpo.oc_total_amount), 0) AS total_oc_amount"

    inv_join = ""
    if caps["invoiced_view"]:
        inv_join = """
        LEFT JOIN LATERAL (
            SELECT
                COUNT(*) FILTER (WHERE v.status = 'confirmed')::int AS invoiced_confirmed,
                COUNT(*) FILTER (WHERE v.status = 'probable')::int AS invoiced_probable,
                COUNT(*) FILTER (WHERE v.status = 'missing')::int AS invoiced_pending
            FROM distribuidora.v_dispatch_plan_invoiced_documents v
            WHERE v.dispatch_plan_id = dp.id
        ) inv ON TRUE
        """
        inv_select = """
            COALESCE(MAX(inv.invoiced_confirmed), 0)::int AS invoiced_confirmed,
            COALESCE(MAX(inv.invoiced_probable), 0)::int AS invoiced_probable,
            COALESCE(MAX(inv.invoiced_pending), 0)::int AS invoiced_pending
        """
    else:
        inv_select = """
            0::int AS invoiced_confirmed,
            0::int AS invoiced_probable,
            0::int AS invoiced_pending
        """

    return f"""
        SELECT
            dp.id,
            {planning_code},
            {planning_name},
            dp.planning_date,
            dp.truck_id,
            {truck_name},
            dp.route_name,
            dp.status,
            dp.created_at,
            dp.confirmed_at,
            COALESCE(dp.total_route_cost_clp, 0) AS total_route_cost_clp,
            {final_margin},
            {net_operational},
            {order_count},
            {total_oc},
            {inv_select}
        FROM distribuidora.dispatch_plan dp
        LEFT JOIN distribuidora.trucks t ON t.id = dp.truck_id
        {orders_join}
        {inv_join}
        GROUP BY dp.id
        ORDER BY dp.created_at DESC NULLS LAST
        LIMIT %s
    """


def list_recent_plans_light(cur, *, limit: int = 50) -> list[dict[str, Any]]:
    """Historial liviano: sin vista facturación ni route_geometry."""
    lim = max(1, min(int(limit), 200))
    caps = _history_schema_caps(cur)
    if not caps["has_plan_table"]:
        logger.warning("[PLANNING_HISTORY_DEBUG] dispatch_plan table missing — empty list")
        return []

    planning_code = (
        "COALESCE(NULLIF(BTRIM(dp.planning_code), ''), 'PLAN-' || LPAD(dp.id::text, 5, '0'))"
        if caps["planning_code"]
        else "('PLAN-' || LPAD(dp.id::text, 5, '0'))"
    )
    planning_name = (
        "COALESCE(NULLIF(BTRIM(dp.planning_name), ''), dp.route_name)"
        if caps["planning_name"]
        else "dp.route_name"
    )
    truck_name = (
        "COALESCE(NULLIF(BTRIM(dp.truck_name), ''), NULLIF(BTRIM(t.name), ''), dp.route_name)"
        if caps["truck_name_col"]
        else "COALESCE(NULLIF(BTRIM(t.name), ''), dp.route_name)"
    )
    order_sub = ""
    if caps["has_orders_table"]:
        order_sub = """
            (SELECT COUNT(*)::int
             FROM distribuidora.dispatch_plan_orders o
             WHERE o.dispatch_plan_id = dp.id) AS order_count,
            (SELECT COALESCE(SUM(o.oc_total_amount), 0)
             FROM distribuidora.dispatch_plan_orders o
             WHERE o.dispatch_plan_id = dp.id) AS total_oc_amount
        """
    else:
        order_sub = "0::int AS order_count, 0::numeric AS total_oc_amount"

    sql = f"""
        SELECT
            dp.id,
            {planning_code} AS planning_code,
            {planning_name} AS planning_name,
            {truck_name} AS truck_name,
            dp.status,
            dp.created_at,
            {order_sub}
        FROM distribuidora.dispatch_plan dp
        LEFT JOIN distribuidora.trucks t ON t.id = dp.truck_id
        ORDER BY dp.created_at DESC NULLS LAST
        LIMIT %s
    """
    logger.info("[PLANNING_HISTORY_DEBUG] list_recent_plans_light limit=%s", lim)
    cur.execute(sql, (lim,))
    return [dict(zip(_cols(cur), r)) for r in cur.fetchall()]


def list_recent_plans(cur, *, limit: int = 50) -> list[dict[str, Any]]:
    lim = max(1, min(int(limit), 200))
    try:
        rows = list_recent_plans_light(cur, limit=lim)
        logger.info(
            "[PLANNING_HISTORY_DEBUG] rows=%s planning_ids=%s",
            len(rows),
            [r.get("id") for r in rows[:25]],
        )
        return rows
    except Exception as exc:
        logger.exception(
            "[PLANNING_HISTORY_DEBUG] light list failed: %s",
            exc,
        )
        return list_recent_plans_minimal(cur, limit=lim)


def list_recent_plans_minimal(cur, *, limit: int = 50) -> list[dict[str, Any]]:
    """Fallback sin vistas ni columnas opcionales (migraciones parciales)."""
    lim = max(1, min(int(limit), 200))
    caps = _history_schema_caps(cur)
    logger.info("[PLANNING_HISTORY_DEBUG] fallback minimal query limit=%s", lim)

    if caps["has_orders_table"]:
        order_count_sql = """
            (SELECT COUNT(*)::int
             FROM distribuidora.dispatch_plan_orders dpo
             WHERE dpo.dispatch_plan_id = dp.id) AS order_count
        """
        total_oc_sql = """
            (SELECT COALESCE(SUM(dpo.oc_total_amount), 0)
             FROM distribuidora.dispatch_plan_orders dpo
             WHERE dpo.dispatch_plan_id = dp.id) AS total_oc_amount
        """
    else:
        order_count_sql = "0::int AS order_count"
        total_oc_sql = "0::numeric AS total_oc_amount"

    planning_code_sql = (
        "dp.planning_code"
        if caps["planning_code"]
        else "('PLAN-' || LPAD(dp.id::text, 5, '0')) AS planning_code"
    )
    planning_name_sql = (
        "dp.planning_name"
        if caps["planning_name"]
        else "dp.route_name AS planning_name"
    )

    cur.execute(
        f"""
        SELECT
            dp.id,
            {planning_code_sql},
            {planning_name_sql},
            dp.planning_date,
            dp.truck_id,
            COALESCE(NULLIF(BTRIM(t.name), ''), dp.route_name) AS truck_name,
            dp.route_name,
            dp.status,
            dp.created_at,
            dp.confirmed_at,
            COALESCE(dp.total_route_cost_clp, 0) AS total_route_cost_clp,
            NULL::integer AS final_margin_clp,
            NULL::integer AS net_operational_clp,
            {order_count_sql},
            {total_oc_sql},
            0::int AS invoiced_confirmed,
            0::int AS invoiced_probable,
            0::int AS invoiced_pending
        FROM distribuidora.dispatch_plan dp
        LEFT JOIN distribuidora.trucks t ON t.id = dp.truck_id
        ORDER BY dp.created_at DESC NULLS LAST
        LIMIT %s
        """,
        (lim,),
    )
    rows = [dict(zip(_cols(cur), r)) for r in cur.fetchall()]
    logger.info(
        "[PLANNING_HISTORY_DEBUG] fallback rows=%s planning_ids=%s",
        len(rows),
        [r.get("id") for r in rows[:25]],
    )
    return rows


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
