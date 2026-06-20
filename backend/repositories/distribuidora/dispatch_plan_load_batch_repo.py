"""Grupos de carga (pickings múltiples) y asignación documental por plan."""

from __future__ import annotations

import json
from typing import Any

from psycopg2.extras import Json


def _cols(cur) -> list[str]:
    return [d[0] for d in cur.description]


def _row_dict(cur, row) -> dict[str, Any]:
    return dict(zip(_cols(cur), row))


def list_load_batches(cur, plan_id: int) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT id, dispatch_plan_id, sort_order, name, description, created_at, updated_at
        FROM distribuidora.dispatch_plan_load_batches
        WHERE dispatch_plan_id = %s
        ORDER BY sort_order ASC, id ASC
        """,
        (plan_id,),
    )
    return [_row_dict(cur, r) for r in cur.fetchall()]


def insert_load_batch(
    cur,
    *,
    plan_id: int,
    name: str,
    description: str | None,
    sort_order: int,
) -> dict[str, Any]:
    cur.execute(
        """
        INSERT INTO distribuidora.dispatch_plan_load_batches (
            dispatch_plan_id, sort_order, name, description
        )
        VALUES (%s, %s, %s, %s)
        RETURNING id, dispatch_plan_id, sort_order, name, description, created_at, updated_at
        """,
        (plan_id, sort_order, name, (description or "").strip() or None),
    )
    return _row_dict(cur, cur.fetchone())


def update_load_batch(
    cur,
    *,
    plan_id: int,
    batch_id: int,
    name: str,
    description: str | None,
    sort_order: int | None = None,
) -> dict[str, Any] | None:
    if sort_order is not None:
        cur.execute(
            """
            UPDATE distribuidora.dispatch_plan_load_batches
            SET name = %s,
                description = %s,
                sort_order = %s,
                updated_at = NOW()
            WHERE id = %s AND dispatch_plan_id = %s
            RETURNING id, dispatch_plan_id, sort_order, name, description, created_at, updated_at
            """,
            (name, (description or "").strip() or None, sort_order, batch_id, plan_id),
        )
    else:
        cur.execute(
            """
            UPDATE distribuidora.dispatch_plan_load_batches
            SET name = %s,
                description = %s,
                updated_at = NOW()
            WHERE id = %s AND dispatch_plan_id = %s
            RETURNING id, dispatch_plan_id, sort_order, name, description, created_at, updated_at
            """,
            (name, (description or "").strip() or None, batch_id, plan_id),
        )
    row = cur.fetchone()
    return _row_dict(cur, row) if row else None


def delete_load_batch(cur, *, plan_id: int, batch_id: int) -> bool:
    cur.execute(
        """
        UPDATE distribuidora.dispatch_plan_document_load_assignments
        SET load_batch_id = NULL
        WHERE dispatch_plan_id = %s AND load_batch_id = %s
        """,
        (plan_id, batch_id),
    )
    cur.execute(
        """
        DELETE FROM distribuidora.dispatch_plan_load_batches
        WHERE id = %s AND dispatch_plan_id = %s
        """,
        (batch_id, plan_id),
    )
    return cur.rowcount > 0


def next_batch_sort_order(cur, plan_id: int) -> int:
    cur.execute(
        """
        SELECT COALESCE(MAX(sort_order), 0) + 1
        FROM distribuidora.dispatch_plan_load_batches
        WHERE dispatch_plan_id = %s
        """,
        (plan_id,),
    )
    return int(cur.fetchone()[0])


def list_document_assignments(cur, plan_id: int) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT
            a.id,
            a.dispatch_plan_id,
            a.load_batch_id,
            a.related_document_id,
            a.oc_document_id,
            a.document_number,
            a.client_name,
            a.document_total,
            a.assigned_at,
            b.name AS load_batch_name,
            b.sort_order AS load_batch_sort_order
        FROM distribuidora.dispatch_plan_document_load_assignments a
        LEFT JOIN distribuidora.dispatch_plan_load_batches b
            ON b.id = a.load_batch_id
        WHERE a.dispatch_plan_id = %s
        ORDER BY a.document_number ASC NULLS LAST, a.related_document_id ASC
        """,
        (plan_id,),
    )
    return [_row_dict(cur, r) for r in cur.fetchall()]


def upsert_document_assignment(
    cur,
    *,
    plan_id: int,
    related_document_id: int,
    load_batch_id: int | None,
    oc_document_id: int | None = None,
    document_number: int | None = None,
    client_name: str | None = None,
    document_total: float | None = None,
) -> None:
    cur.execute(
        """
        INSERT INTO distribuidora.dispatch_plan_document_load_assignments (
            dispatch_plan_id, load_batch_id, related_document_id,
            oc_document_id, document_number, client_name, document_total
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (dispatch_plan_id, related_document_id) DO UPDATE
        SET load_batch_id = COALESCE(
                EXCLUDED.load_batch_id,
                dispatch_plan_document_load_assignments.load_batch_id
            ),
            oc_document_id = COALESCE(EXCLUDED.oc_document_id, dispatch_plan_document_load_assignments.oc_document_id),
            document_number = COALESCE(EXCLUDED.document_number, dispatch_plan_document_load_assignments.document_number),
            client_name = COALESCE(EXCLUDED.client_name, dispatch_plan_document_load_assignments.client_name),
            document_total = COALESCE(EXCLUDED.document_total, dispatch_plan_document_load_assignments.document_total),
            assigned_at = NOW()
        """,
        (
            plan_id,
            load_batch_id,
            related_document_id,
            oc_document_id,
            document_number,
            client_name,
            document_total,
        ),
    )


def related_document_ids_for_batch(cur, plan_id: int, load_batch_id: int) -> list[int]:
    cur.execute(
        """
        SELECT related_document_id
        FROM distribuidora.dispatch_plan_document_load_assignments
        WHERE dispatch_plan_id = %s AND load_batch_id = %s
        ORDER BY document_number ASC NULLS LAST, related_document_id ASC
        """,
        (plan_id, load_batch_id),
    )
    return [int(r[0]) for r in cur.fetchall()]


def insert_order_event(
    cur,
    *,
    plan_id: int,
    action: str,
    user_name: str | None = None,
    reason: str | None = None,
    oc_document_id: int | None = None,
    oc_number: int | None = None,
    picking_id: int | None = None,
    picking_version: int | None = None,
    payload: dict[str, Any] | None = None,
) -> int:
    cur.execute(
        """
        INSERT INTO distribuidora.dispatch_plan_order_events (
            dispatch_plan_id, action, user_name, reason,
            oc_document_id, oc_number, picking_id, picking_version, payload
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        """,
        (
            plan_id,
            action,
            user_name,
            (reason or "").strip() or None,
            oc_document_id,
            oc_number,
            picking_id,
            picking_version,
            Json(payload or {}),
        ),
    )
    return int(cur.fetchone()[0])


def list_order_events(cur, plan_id: int, *, limit: int = 50) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT
            id, dispatch_plan_id, action, user_name, reason,
            oc_document_id, oc_number, picking_id, picking_version,
            payload, created_at
        FROM distribuidora.dispatch_plan_order_events
        WHERE dispatch_plan_id = %s
        ORDER BY created_at DESC, id DESC
        LIMIT %s
        """,
        (plan_id, limit),
    )
    rows = []
    for r in cur.fetchall():
        d = _row_dict(cur, r)
        if isinstance(d.get("payload"), str):
            d["payload"] = json.loads(d["payload"])
        rows.append(d)
    return rows


def search_orders_not_in_plan(
    cur,
    plan_id: int,
    *,
    q: str,
    limit: int = 20,
) -> list[dict[str, Any]]:
    term = f"%{q.strip()}%"
    cur.execute(
        """
        SELECT
            d.document_id AS oc_document_id,
            d.number AS oc_number,
            d.client_id,
            COALESCE(
                NULLIF(BTRIM(p.nombre_fantasia), ''),
                NULLIF(BTRIM(c.nombre_fantasia), ''),
                NULLIF(BTRIM(c.company), ''),
                CONCAT_WS(
                    ' ',
                    NULLIF(BTRIM(c.first_name), ''),
                    NULLIF(BTRIM(c.last_name), '')
                )
            ) AS client_name,
            COALESCE(p.total_amount, d.total_amount) AS oc_total_amount
        FROM distribuidora.v_documents_latest d
        LEFT JOIN distribuidora.v_orders_purchase p ON p.document_id = d.document_id
        LEFT JOIN bsale.clients c
            ON c.company_id = 3 AND c.bsale_id = d.client_id
        WHERE d.company_id = 3
          AND d.document_type_id = 33
          AND NOT EXISTS (
              SELECT 1
              FROM distribuidora.dispatch_plan_orders dpo
              WHERE dpo.dispatch_plan_id = %s
                AND dpo.oc_document_id = d.document_id
          )
          AND (
              d.number::text ILIKE %s
              OR COALESCE(
                  NULLIF(BTRIM(p.nombre_fantasia), ''),
                  NULLIF(BTRIM(c.nombre_fantasia), ''),
                  NULLIF(BTRIM(c.company), ''),
                  ''
              ) ILIKE %s
          )
        ORDER BY d.emission_date DESC NULLS LAST, d.document_id DESC
        LIMIT %s
        """,
        (plan_id, term, term, limit),
    )
    return [_row_dict(cur, r) for r in cur.fetchall()]


def plan_has_current_picking(cur, plan_id: int) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM distribuidora.dispatch_plan_pickings
        WHERE dispatch_plan_id = %s AND is_current = TRUE
        LIMIT 1
        """,
        (plan_id,),
    )
    return cur.fetchone() is not None


def max_route_order(cur, plan_id: int) -> int:
    cur.execute(
        """
        SELECT COALESCE(MAX(route_order), 0)
        FROM distribuidora.dispatch_plan_orders
        WHERE dispatch_plan_id = %s
        """,
        (plan_id,),
    )
    return int(cur.fetchone()[0])
