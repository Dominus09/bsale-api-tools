"""Auditoría de eventos de autenticación/administración (opt-in).

No registra passwords, hashes ni tokens. Requiere tabla ``app.auth_events``
(migración propuesta) y ``AUTH_AUDIT_EVENTS=1``.
"""

from __future__ import annotations

import logging
import os
from typing import Any

logger = logging.getLogger(__name__)

# Eventos previstos para etapa 1+ y módulo Usuarios
EVENT_LOGIN_SUCCESS = "login_success"
EVENT_LOGIN_FAILURE = "login_failure"
EVENT_LOGOUT = "logout"
EVENT_PASSWORD_CHANGE = "password_change"
EVENT_PASSWORD_RESET = "password_reset"
EVENT_USER_ACTIVATED = "user_activated"
EVENT_USER_DEACTIVATED = "user_deactivated"
EVENT_ROLE_CHANGE = "role_change"
EVENT_COMPANIES_CHANGE = "companies_change"
EVENT_LEGACY_PASSWORD_UPGRADED = "legacy_password_upgraded"
EVENT_USER_CREATED = "user_created"
EVENT_PASSWORD_SET_VIA_TOKEN = "password_set_via_token"

# Retención recomendada (documentación): 180 días login; 365 días admin.
DEFAULT_RETENTION_DAYS_LOGIN = 180
DEFAULT_RETENTION_DAYS_ADMIN = 365


def audit_enabled() -> bool:
    return (os.getenv("AUTH_AUDIT_EVENTS") or "").strip().lower() in ("1", "true", "yes", "on")


def record_auth_event(
    *,
    event_type: str,
    email: str | None = None,
    user_id: int | None = None,
    success: bool | None = None,
    ip: str | None = None,
    user_agent: str | None = None,
    actor_user_id: int | None = None,
    detail: dict[str, Any] | None = None,
) -> None:
    if not audit_enabled():
        return
    # Import diferido: evita acoplar arranque si la tabla no existe.
    try:
        from backend.db import get_connection
    except Exception:
        return

    from psycopg2.extras import Json

    safe_detail = dict(detail or {})
    for banned in ("password", "password_hash", "token", "authorization", "secret"):
        safe_detail.pop(banned, None)

    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'app' AND table_name = 'auth_events'
            LIMIT 1
            """
        )
        if not cur.fetchone():
            cur.close()
            return
        cur.execute(
            """
            INSERT INTO app.auth_events (
                event_type, user_id, email, success, ip, user_agent, actor_user_id, detail
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s
            )
            """,
            (
                event_type,
                user_id,
                (email or "").strip().lower() or None,
                success,
                (ip or "")[:64] or None,
                (user_agent or "")[:512] or None,
                actor_user_id,
                Json(safe_detail),
            ),
        )
        conn.commit()
        cur.close()
    except Exception:
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                pass
        logger.warning("auth audit insert skipped/failed event_type=%s", event_type)
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
