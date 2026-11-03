"""Persistencia de sesiones de confirmación simple_planning."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any
from uuid import UUID


def _cols(cur) -> list[str]:
    return [d[0] for d in cur.description]


def insert_session(cur, fields: dict[str, Any]) -> dict[str, Any]:
    payload = dict(fields)
    if isinstance(payload.get("payload"), (dict, list)):
        payload["payload"] = json.dumps(payload["payload"])
    cur.execute(
        """
        INSERT INTO distribuidora.simple_planning_sessions (
            id, user_id, company_id, office_id, planning_date, truck_id,
            dispatch_name, driver_name, plan_session_id, payload, status,
            expires_at, idempotency_key
        )
        VALUES (
            %(id)s, %(user_id)s, %(company_id)s, %(office_id)s, %(planning_date)s,
            %(truck_id)s, %(dispatch_name)s, %(driver_name)s, %(plan_session_id)s,
            %(payload)s::jsonb, %(status)s, %(expires_at)s, %(idempotency_key)s
        )
        RETURNING *
        """,
        payload,
    )
    row = cur.fetchone()
    return dict(zip(_cols(cur), row))


def get_session_by_id_for_update(cur, session_id: UUID | str) -> dict[str, Any] | None:
    cur.execute(
        """
        SELECT *
        FROM distribuidora.simple_planning_sessions
        WHERE id = %s
        FOR UPDATE
        """,
        (str(session_id),),
    )
    row = cur.fetchone()
    if not row:
        return None
    return dict(zip(_cols(cur), row))


def get_session_by_idempotency(
    cur, *, user_id: int, idempotency_key: str
) -> dict[str, Any] | None:
    cur.execute(
        """
        SELECT *
        FROM distribuidora.simple_planning_sessions
        WHERE user_id = %s AND idempotency_key = %s
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (int(user_id), str(idempotency_key)),
    )
    row = cur.fetchone()
    if not row:
        return None
    return dict(zip(_cols(cur), row))


def mark_session_status(
    cur,
    session_id: UUID | str,
    *,
    status: str,
    confirmed_plan_id: int | None = None,
    last_error: str | None = None,
    now: datetime | None = None,
) -> None:
    cur.execute(
        """
        UPDATE distribuidora.simple_planning_sessions
        SET status = %s,
            confirmed_plan_id = COALESCE(%s, confirmed_plan_id),
            last_error = %s,
            updated_at = COALESCE(%s, NOW())
        WHERE id = %s
        """,
        (
            status,
            confirmed_plan_id,
            last_error,
            now,
            str(session_id),
        ),
    )
