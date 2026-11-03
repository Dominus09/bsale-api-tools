"""Alcance de lectura de dispatch_plan para capability-only simple_planning.

No inventa autoría: solo sesiones ``simple_planning_sessions`` del usuario
(``confirmed_plan_id`` / ``plan_session_id``) o asignación explícita
``driver_user_id``. Roles operativos plenos no se filtran aquí.
"""

from __future__ import annotations

import logging
from typing import Any

from backend.auth.permissions import is_simple_planning_capability_only
from backend.db import get_connection

logger = logging.getLogger(__name__)


def _user_id(user: dict[str, Any] | None) -> int | None:
    if not user:
        return None
    raw = user.get("id") if user.get("id") is not None else user.get("uid")
    try:
        return int(raw) if raw is not None else None
    except (TypeError, ValueError):
        return None


def _sessions_table_ready(cur) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'distribuidora'
          AND table_name = 'simple_planning_sessions'
        LIMIT 1
        """
    )
    return cur.fetchone() is not None


def _driver_col_ready(cur) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'distribuidora'
          AND table_name = 'dispatch_plan'
          AND column_name = 'driver_user_id'
        LIMIT 1
        """
    )
    return cur.fetchone() is not None


def plan_ids_readable_by_capability_user(cur, user_id: int) -> set[int]:
    """IDs con evidencia de pertenencia o asignación explícita. Sin inventar."""
    ids: set[int] = set()
    if _sessions_table_ready(cur):
        cur.execute(
            """
            SELECT confirmed_plan_id
            FROM distribuidora.simple_planning_sessions
            WHERE user_id = %s
              AND confirmed_plan_id IS NOT NULL
            """,
            (int(user_id),),
        )
        for (pid,) in cur.fetchall() or []:
            if pid is not None:
                ids.add(int(pid))
        cur.execute(
            """
            SELECT dp.id
            FROM distribuidora.simple_planning_sessions s
            INNER JOIN distribuidora.dispatch_plan dp
              ON dp.plan_session_id = s.plan_session_id
            WHERE s.user_id = %s
            """,
            (int(user_id),),
        )
        for (pid,) in cur.fetchall() or []:
            if pid is not None:
                ids.add(int(pid))
    if _driver_col_ready(cur):
        cur.execute(
            """
            SELECT id
            FROM distribuidora.dispatch_plan
            WHERE driver_user_id = %s
            """,
            (int(user_id),),
        )
        for (pid,) in cur.fetchall() or []:
            if pid is not None:
                ids.add(int(pid))
    return ids


def user_can_read_dispatch_plan(user: dict[str, Any] | None, plan_id: int) -> bool:
    """True si el usuario puede ver el plan.

    Capability-only: solo con vínculo en sesiones propias o ``driver_user_id``.
    Resto de staff ERP: sin filtro adicional (comportamiento histórico).
    """
    if not user:
        return False
    if not is_simple_planning_capability_only(user):
        return True
    uid = _user_id(user)
    if uid is None:
        return False
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        allowed = plan_ids_readable_by_capability_user(cur, uid)
        cur.close()
        return int(plan_id) in allowed
    except Exception:
        logger.exception(
            "dispatch_plan_read_scope_failed user=%s plan=%s", uid, plan_id
        )
        return False
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def filter_plans_for_user(
    user: dict[str, Any] | None, plans: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    if not user or not is_simple_planning_capability_only(user):
        return plans
    uid = _user_id(user)
    if uid is None:
        return []
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        allowed = plan_ids_readable_by_capability_user(cur, uid)
        cur.close()
    except Exception:
        logger.exception("dispatch_plan_list_scope_failed user=%s", uid)
        return []
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
    out: list[dict[str, Any]] = []
    for p in plans:
        try:
            pid = int(p.get("id"))
        except (TypeError, ValueError):
            continue
        if pid in allowed:
            out.append(p)
    return out


def user_can_assign_driver_on_plan(
    user: dict[str, Any] | None, plan_id: int
) -> bool:
    """Ops plenos, o capability-only sobre un plan propio/explícito."""
    from backend.services.distribuidora.dispatch_plan_driver_service import (
        can_assign_dispatch_driver,
    )

    if can_assign_dispatch_driver(user):
        return True
    if is_simple_planning_capability_only(user):
        return user_can_read_dispatch_plan(user, plan_id)
    return False
