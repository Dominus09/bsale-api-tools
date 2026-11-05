"""Asignaciones pre-factura OC → grupo de carga (mismo plan)."""

from __future__ import annotations

from typing import Any


def _cols(cur) -> list[str]:
    return [d[0] for d in cur.description]


def _row_dict(cur, row) -> dict[str, Any]:
    return dict(zip(_cols(cur), row))


def organization_columns_ready(cur) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'distribuidora'
          AND table_name = 'dispatch_plan'
          AND column_name = 'load_organization_status'
        LIMIT 1
        """
    )
    return cur.fetchone() is not None


def assignments_table_ready(cur) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'distribuidora'
          AND table_name = 'dispatch_plan_order_load_assignments'
        LIMIT 1
        """
    )
    return cur.fetchone() is not None


def get_organization_meta(cur, plan_id: int) -> dict[str, Any] | None:
    if not organization_columns_ready(cur):
        return {
            "load_organization_status": "legacy",
            "load_organization_version": 0,
            "load_organization_updated_at": None,
            "schema_ready": False,
        }
    cur.execute(
        """
        SELECT load_organization_status,
               load_organization_version,
               load_organization_updated_at
        FROM distribuidora.dispatch_plan
        WHERE id = %s
        """,
        (plan_id,),
    )
    row = cur.fetchone()
    if not row:
        return None
    return {
        "load_organization_status": row[0],
        "load_organization_version": int(row[1] or 0),
        "load_organization_updated_at": row[2],
        "schema_ready": True,
    }


def lock_plan_for_organization(cur, plan_id: int) -> dict[str, Any] | None:
    """Bloqueo de fila del plan para guardado atómico."""
    if not organization_columns_ready(cur):
        cur.execute(
            """
            SELECT id, status
            FROM distribuidora.dispatch_plan
            WHERE id = %s
            FOR UPDATE
            """,
            (plan_id,),
        )
        row = cur.fetchone()
        if not row:
            return None
        return {
            "id": int(row[0]),
            "status": row[1],
            "load_organization_status": "legacy",
            "load_organization_version": 0,
        }
    cur.execute(
        """
        SELECT id, status, load_organization_status, load_organization_version
        FROM distribuidora.dispatch_plan
        WHERE id = %s
        FOR UPDATE
        """,
        (plan_id,),
    )
    row = cur.fetchone()
    if not row:
        return None
    return {
        "id": int(row[0]),
        "status": row[1],
        "load_organization_status": row[2],
        "load_organization_version": int(row[3] or 0),
    }


def list_order_assignments(cur, plan_id: int) -> list[dict[str, Any]]:
    if not assignments_table_ready(cur):
        return []
    cur.execute(
        """
        SELECT a.id, a.dispatch_plan_id, a.oc_document_id, a.load_batch_id,
               a.client_id, a.assigned_at, a.assigned_by,
               b.name AS load_batch_name, b.sort_order AS load_batch_sort_order
        FROM distribuidora.dispatch_plan_order_load_assignments a
        INNER JOIN distribuidora.dispatch_plan_load_batches b
          ON b.id = a.load_batch_id AND b.dispatch_plan_id = a.dispatch_plan_id
        WHERE a.dispatch_plan_id = %s
        ORDER BY b.sort_order ASC, a.oc_document_id ASC
        """,
        (plan_id,),
    )
    return [_row_dict(cur, r) for r in cur.fetchall()]


def list_plan_orders_for_organization(cur, plan_id: int) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT o.id,
               o.dispatch_plan_id,
               o.oc_document_id,
               o.oc_number,
               o.client_id,
               o.client_name,
               o.oc_total_amount,
               o.route_order,
               a.load_batch_id,
               b.name AS load_batch_name
        FROM distribuidora.dispatch_plan_orders o
        LEFT JOIN distribuidora.dispatch_plan_order_load_assignments a
          ON a.dispatch_plan_id = o.dispatch_plan_id
         AND a.oc_document_id = o.oc_document_id
        LEFT JOIN distribuidora.dispatch_plan_load_batches b
          ON b.id = a.load_batch_id AND b.dispatch_plan_id = o.dispatch_plan_id
        WHERE o.dispatch_plan_id = %s
        ORDER BY o.route_order NULLS LAST, o.oc_document_id
        """,
        (plan_id,),
    )
    if cur.description is None:
        return []
    # Si la tabla de assignments no existe, la JOIN falla — caller valida schema.
    return [_row_dict(cur, r) for r in cur.fetchall()]


def replace_order_assignments(
    cur,
    *,
    plan_id: int,
    assignments: list[dict[str, Any]],
    assigned_by: str | None,
) -> list[dict[str, Any]]:
    """Reemplazo idempotente del set de asignaciones del plan (dentro de TX abierta)."""
    cur.execute(
        """
        DELETE FROM distribuidora.dispatch_plan_order_load_assignments
        WHERE dispatch_plan_id = %s
        """,
        (plan_id,),
    )
    out: list[dict[str, Any]] = []
    for a in assignments:
        cur.execute(
            """
            INSERT INTO distribuidora.dispatch_plan_order_load_assignments (
                dispatch_plan_id, oc_document_id, load_batch_id, client_id, assigned_by
            )
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id, dispatch_plan_id, oc_document_id, load_batch_id,
                      client_id, assigned_at, assigned_by
            """,
            (
                plan_id,
                int(a["oc_document_id"]),
                int(a["load_batch_id"]),
                a.get("client_id"),
                assigned_by,
            ),
        )
        out.append(_row_dict(cur, cur.fetchone()))
    return out


def update_organization_meta(
    cur,
    *,
    plan_id: int,
    status: str,
    from_version: int,
) -> dict[str, Any] | None:
    cur.execute(
        """
        UPDATE distribuidora.dispatch_plan
        SET load_organization_status = %s,
            load_organization_version = load_organization_version + 1,
            load_organization_updated_at = NOW()
        WHERE id = %s
          AND load_organization_version = %s
        RETURNING load_organization_status, load_organization_version, load_organization_updated_at
        """,
        (status, plan_id, from_version),
    )
    row = cur.fetchone()
    if not row:
        return None
    return {
        "load_organization_status": row[0],
        "load_organization_version": int(row[1] or 0),
        "load_organization_updated_at": row[2],
    }


def insert_organization_event(
    cur,
    *,
    plan_id: int,
    action: str,
    user_name: str | None,
    payload: dict[str, Any],
) -> None:
    cur.execute(
        """
        INSERT INTO distribuidora.dispatch_plan_order_events (
            dispatch_plan_id, action, user_name, payload
        )
        VALUES (%s, %s, %s, %s::jsonb)
        """,
        (
            plan_id,
            action,
            user_name,
            __import__("json").dumps(payload, default=str),
        ),
    )
