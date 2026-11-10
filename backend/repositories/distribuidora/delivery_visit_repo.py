"""Persistencia P4: visitas de entrega y eventos."""

from __future__ import annotations

from typing import Any

from psycopg2.extras import Json


def _row(cur, r) -> dict[str, Any]:
    cols = [d[0] for d in cur.description]
    if isinstance(r, dict) or hasattr(r, "keys"):
        return dict(r)
    return dict(zip(cols, r))


def tables_ready(cur) -> bool:
    cur.execute(
        """
        SELECT (
            to_regclass('distribuidora.delivery_visits') IS NOT NULL
            AND to_regclass('distribuidora.delivery_visit_events') IS NOT NULL
        ) AS tables_ready
        """
    )
    row = cur.fetchone()
    if row is None:
        return False
    if isinstance(row, dict) or hasattr(row, "keys"):
        return bool(row["tables_ready"])
    return bool(row[0])


def get_active_visit(
    cur, *, plan_id: int, customer_key: str
) -> dict[str, Any] | None:
    cur.execute(
        """
        SELECT *
        FROM distribuidora.delivery_visits
        WHERE dispatch_plan_id = %s
          AND customer_key = %s
          AND status = 'in_progress'
        ORDER BY id DESC
        LIMIT 1
        """,
        (int(plan_id), customer_key),
    )
    r = cur.fetchone()
    return _row(cur, r) if r else None


def get_visit(cur, visit_id: int) -> dict[str, Any] | None:
    cur.execute(
        "SELECT * FROM distribuidora.delivery_visits WHERE id = %s",
        (int(visit_id),),
    )
    r = cur.fetchone()
    return _row(cur, r) if r else None


def find_by_idem(
    cur, *, driver_user_id: int, idempotency_key: str
) -> dict[str, Any] | None:
    cur.execute(
        """
        SELECT * FROM distribuidora.delivery_visits
        WHERE driver_user_id = %s AND idempotency_key = %s
        ORDER BY id DESC LIMIT 1
        """,
        (int(driver_user_id), idempotency_key),
    )
    r = cur.fetchone()
    return _row(cur, r) if r else None


def insert_visit(cur, **kw) -> dict[str, Any]:
    cur.execute(
        """
        INSERT INTO distribuidora.delivery_visits (
            company_id, dispatch_plan_id, customer_key, client_id, driver_user_id,
            status, start_lat, start_lng, start_accuracy_m, start_gps_captured_at,
            start_gps_source, gps_exception_code, gps_exception_note, idempotency_key
        ) VALUES (
            %(company_id)s, %(dispatch_plan_id)s, %(customer_key)s, %(client_id)s,
            %(driver_user_id)s, 'in_progress', %(start_lat)s, %(start_lng)s,
            %(start_accuracy_m)s, %(start_gps_captured_at)s, %(start_gps_source)s,
            %(gps_exception_code)s, %(gps_exception_note)s, %(idempotency_key)s
        )
        RETURNING *
        """,
        kw,
    )
    return _row(cur, cur.fetchone())


def finalize_visit(
    cur,
    visit_id: int,
    *,
    expected_version: int,
    status: str,
    result_reason: str | None,
    result_note: str | None,
    end_lat: float | None,
    end_lng: float | None,
    end_accuracy_m: float | None,
    end_gps_captured_at,
    end_gps_source: str | None,
) -> dict[str, Any] | None:
    cur.execute(
        """
        UPDATE distribuidora.delivery_visits
        SET status = %s,
            result_reason = %s,
            result_note = %s,
            ended_at = NOW(),
            end_lat = %s,
            end_lng = %s,
            end_accuracy_m = %s,
            end_gps_captured_at = %s,
            end_gps_source = %s,
            version = version + 1,
            updated_at = NOW()
        WHERE id = %s
          AND status = 'in_progress'
          AND version = %s
        RETURNING *
        """,
        (
            status,
            result_reason,
            result_note,
            end_lat,
            end_lng,
            end_accuracy_m,
            end_gps_captured_at,
            end_gps_source,
            int(visit_id),
            int(expected_version),
        ),
    )
    r = cur.fetchone()
    return _row(cur, r) if r else None


def insert_event(
    cur,
    *,
    visit_id: int,
    action: str,
    actor_user_id: int | None,
    payload: dict[str, Any] | None = None,
) -> int:
    cur.execute(
        """
        INSERT INTO distribuidora.delivery_visit_events (
            visit_id, action, actor_user_id, payload
        ) VALUES (%s, %s, %s, %s)
        RETURNING id
        """,
        (int(visit_id), action, actor_user_id, Json(payload or {})),
    )
    row = cur.fetchone()
    if isinstance(row, dict) or hasattr(row, "keys"):
        return int(row["id"])
    return int(row[0])
