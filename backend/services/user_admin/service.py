"""Servicio de administración de usuarios staff (bsale.users)."""

from __future__ import annotations

import hashlib
import logging
import secrets
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import HTTPException

from backend.auth.audit import (
    EVENT_COMPANIES_CHANGE,
    EVENT_PASSWORD_RESET,
    EVENT_PASSWORD_SET_VIA_TOKEN,
    EVENT_ROLE_CHANGE,
    EVENT_USER_ACTIVATED,
    EVENT_USER_CREATED,
    EVENT_USER_DEACTIVATED,
    record_auth_event,
)
from backend.auth.passwords import hash_password
from backend.auth.permissions import ADMIN_ROLES, normalized_role
from backend.auth.user_admin_access import ASSIGNABLE_ROLES, can_assign_role
from backend.db import get_connection

logger = logging.getLogger(__name__)

RESET_TOKEN_TTL_HOURS = 24
MIN_PASSWORD_LEN = 10


def _table_exists(cur, schema: str, table: str) -> bool:
    cur.execute(
        """
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = %s AND table_name = %s LIMIT 1
        """,
        (schema, table),
    )
    return cur.fetchone() is not None


def _column_exists(cur, schema: str, table: str, column: str) -> bool:
    cur.execute(
        """
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s AND column_name = %s LIMIT 1
        """,
        (schema, table, column),
    )
    return cur.fetchone() is not None


def _hash_token(raw: str) -> str:
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _actor_id(actor: dict[str, Any]) -> int | None:
    raw = actor.get("id") or actor.get("uid")
    try:
        return int(raw) if raw is not None else None
    except (TypeError, ValueError):
        return None


def _is_admin_role(role: str | None) -> bool:
    return normalized_role({"role": role}) in ADMIN_ROLES


def count_active_admins(cur) -> int:
    cur.execute(
        """
        SELECT COUNT(*) FROM bsale.users
        WHERE active = TRUE AND lower(trim(role)) = ANY(%s)
        """,
        (list(ADMIN_ROLES),),
    )
    row = cur.fetchone()
    return int(row[0] or 0)


def _ensure_not_last_admin(cur, *, user_id: int, new_active: bool | None, new_role: str | None) -> None:
    # Bloqueo de fila para carreras al desactivar administradores en paralelo.
    cur.execute(
        "SELECT active, role FROM bsale.users WHERE id = %s LIMIT 1 FOR UPDATE",
        (user_id,),
    )
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
    was_active = bool(row[0])
    was_admin = _is_admin_role(row[1])
    will_active = was_active if new_active is None else bool(new_active)
    will_role = row[1] if new_role is None else new_role
    will_admin = _is_admin_role(will_role)
    if was_active and was_admin and (not will_active or not will_admin):
        # Recuento bajo el mismo snapshot transaccional.
        cur.execute(
            """
            SELECT COUNT(*) FROM bsale.users
            WHERE active = TRUE AND lower(trim(role)) = ANY(%s)
            FOR SHARE
            """,
            (list(ADMIN_ROLES),),
        )
        n = int((cur.fetchone() or [0])[0] or 0)
        if n <= 1:
            raise HTTPException(
                status_code=409,
                detail="No se puede desactivar o degradar al último administrador autorizado",
            )


def _guard_self_privilege_change(
    *,
    actor: dict[str, Any],
    user_id: int,
    role: str | None,
    active: bool | None,
) -> None:
    """Impide auto-escalamiento / auto-desactivación de privilegios."""
    actor_id = _actor_id(actor)
    if actor_id is None or int(actor_id) != int(user_id):
        return
    if role is not None and role.strip().lower() != normalized_role(actor):
        raise HTTPException(
            status_code=403,
            detail="No puede cambiar su propio rol; solicite a otro administrador autorizado",
        )
    if active is False:
        raise HTTPException(
            status_code=403,
            detail="No puede desactivar su propia cuenta",
        )


def _load_companies_map(cur, user_ids: list[int]) -> dict[int, list[dict[str, Any]]]:
    out: dict[int, list[dict[str, Any]]] = {uid: [] for uid in user_ids}
    if not user_ids or not _table_exists(cur, "app", "user_companies"):
        return out
    cur.execute(
        """
        SELECT uc.user_id, uc.company_id, c.name
        FROM app.user_companies uc
        LEFT JOIN bsale.companies c ON c.company_id = uc.company_id
        WHERE uc.user_id = ANY(%s)
        ORDER BY uc.company_id
        """,
        (user_ids,),
    )
    for uid, cid, name in cur.fetchall():
        out.setdefault(int(uid), []).append(
            {"company_id": int(cid), "name": name}
        )
    return out


def _load_last_logins(cur, user_ids: list[int]) -> dict[int, datetime | None]:
    out: dict[int, datetime | None] = {uid: None for uid in user_ids}
    if not user_ids or not _table_exists(cur, "app", "auth_events"):
        return out
    cur.execute(
        """
        SELECT user_id, MAX(created_at)
        FROM app.auth_events
        WHERE event_type = 'login_success' AND user_id = ANY(%s)
        GROUP BY user_id
        """,
        (user_ids,),
    )
    for uid, ts in cur.fetchall():
        out[int(uid)] = ts
    return out


def list_users(*, q: str | None = None, active: bool | None = None) -> list[dict[str, Any]]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        clauses: list[str] = []
        params: list[Any] = []
        if q and q.strip():
            clauses.append(
                "(lower(email) LIKE %s OR lower(COALESCE(nombre_usuario, '')) LIKE %s)"
            )
            like = f"%{q.strip().lower()}%"
            params.extend([like, like])
        if active is not None:
            clauses.append("active = %s")
            params.append(active)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        cur.execute(
            f"""
            SELECT id, email, role, active, created_at, nombre_usuario
            FROM bsale.users
            {where}
            ORDER BY lower(COALESCE(nombre_usuario, email))
            """,
            params,
        )
        rows = cur.fetchall()
        ids = [int(r[0]) for r in rows]
        companies = _load_companies_map(cur, ids)
        last_logins = _load_last_logins(cur, ids)
        cur.close()
        return [
            {
                "id": int(r[0]),
                "email": r[1],
                "role": r[2],
                "active": bool(r[3]) if r[3] is not None else False,
                "created_at": r[4].isoformat() if r[4] else None,
                "nombre_usuario": r[5],
                "companies": companies.get(int(r[0]), []),
                "last_login_at": last_logins.get(int(r[0])).isoformat()
                if last_logins.get(int(r[0]))
                else None,
            }
            for r in rows
        ]
    finally:
        conn.close()


def get_user(user_id: int) -> dict[str, Any]:
    items = list_users()
    for it in items:
        if it["id"] == user_id:
            return it
    raise HTTPException(status_code=404, detail="Usuario no encontrado")


def list_companies_catalog() -> list[dict[str, Any]]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        has_active = _column_exists(cur, "bsale", "companies", "active")
        if has_active:
            cur.execute(
                """
                SELECT company_id, name, active
                FROM bsale.companies
                ORDER BY company_id
                """
            )
            rows = cur.fetchall()
            cur.close()
            return [
                {
                    "company_id": int(r[0]),
                    "name": r[1],
                    "active": bool(r[2]) if r[2] is not None else True,
                }
                for r in rows
            ]
        cur.execute("SELECT company_id, name FROM bsale.companies ORDER BY company_id")
        rows = cur.fetchall()
        cur.close()
        return [
            {"company_id": int(r[0]), "name": r[1], "active": True} for r in rows
        ]
    finally:
        conn.close()


def roles_catalog() -> dict[str, Any]:
    return {
        "assignable_roles": sorted(ASSIGNABLE_ROLES),
        "module_matrix": [
            {
                "role": "adm",
                "label": "Administración técnica",
                "modules": {
                    "user_admin": True,
                    "diagnostics": True,
                    "costs": True,
                    "margins": True,
                    "promotions_approve": True,
                    "gondolas_operate": True,
                    "gondolas_cancel": True,
                },
            },
            {
                "role": "gerencia",
                "label": "Gerencia",
                "modules": {
                    "user_admin": False,
                    "user_admin_view": True,
                    "diagnostics": True,
                    "costs": True,
                    "margins": True,
                    "promotions_approve": True,
                    "gondolas_operate": True,
                    "gondolas_cancel": True,
                },
            },
            {
                "role": "operativo",
                "label": "Operativo",
                "modules": {
                    "user_admin": False,
                    "diagnostics": False,
                    "costs": False,
                    "margins": False,
                    "promotions_approve": False,
                    "gondolas_operate": True,
                    "gondolas_cancel": False,
                },
            },
        ],
        "notes": [
            "Los permisos se resuelven en backend; esta matriz es informativa.",
            "Mutar usuarios exige permiso «Administrar usuarios» (AUTH_USER_ADMIN_EMAILS o fallback adm).",
            "Asignar rol adm exige AUTH_ASSIGN_ADM_EMAILS o ser adm autorizado.",
            "No se crean roles nuevos desde esta pantalla.",
        ],
    }


def _set_user_companies(cur, user_id: int, company_ids: list[int]) -> None:
    if not _table_exists(cur, "app", "user_companies"):
        if company_ids:
            raise HTTPException(
                status_code=409,
                detail="Tabla app.user_companies no disponible; aplique la migración 061 antes de asignar empresas",
            )
        return
    unique = sorted({int(c) for c in company_ids})
    if unique:
        cur.execute(
            "SELECT company_id FROM bsale.companies WHERE company_id = ANY(%s)",
            (unique,),
        )
        found = {int(r[0]) for r in cur.fetchall()}
        missing = [c for c in unique if c not in found]
        if missing:
            raise HTTPException(
                status_code=400,
                detail=f"Empresas inexistentes: {missing}",
            )
    cur.execute("DELETE FROM app.user_companies WHERE user_id = %s", (user_id,))
    for cid in unique:
        cur.execute(
            """
            INSERT INTO app.user_companies (user_id, company_id)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING
            """,
            (user_id, cid),
        )


def create_user(
    *,
    actor: dict[str, Any],
    email: str,
    nombre_usuario: str | None,
    role: str,
    company_ids: list[int] | None,
    password: str | None,
    invite: bool,
) -> dict[str, Any]:
    if not can_assign_role(actor, role):
        raise HTTPException(status_code=403, detail="No autorizado a asignar este rol")
    email_key = email.strip().lower()
    if not email_key or "@" not in email_key:
        raise HTTPException(status_code=400, detail="Correo inválido")
    role_n = role.strip().lower()
    if invite and password:
        raise HTTPException(
            status_code=400,
            detail="Use invitación o contraseña inicial, no ambas",
        )
    if not invite:
        if not password or len(password) < MIN_PASSWORD_LEN:
            raise HTTPException(
                status_code=400,
                detail=f"Contraseña de al menos {MIN_PASSWORD_LEN} caracteres",
            )
        pwd_hash = hash_password(password)
    else:
        # Placeholder bcrypt ilegible; el titular define la clave vía token.
        pwd_hash = hash_password(secrets.token_urlsafe(32))

    conn = get_connection()
    delivery: dict[str, Any] | None = None
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT 1 FROM bsale.users WHERE lower(trim(email)) = %s LIMIT 1",
            (email_key,),
        )
        if cur.fetchone():
            raise HTTPException(status_code=409, detail="El correo ya está registrado")
        cur.execute(
            """
            INSERT INTO bsale.users (email, password_hash, role, active, nombre_usuario)
            VALUES (%s, %s, %s, TRUE, %s)
            RETURNING id, email, role, active, created_at, nombre_usuario
            """,
            (email_key, pwd_hash, role_n, (nombre_usuario or "").strip() or None),
        )
        row = cur.fetchone()
        user_id = int(row[0])
        _set_user_companies(cur, user_id, company_ids or [])
        if invite:
            delivery = _create_password_token(
                cur,
                user_id=user_id,
                purpose="invite",
                created_by=_actor_id(actor),
            )
        conn.commit()
        cur.close()
    except HTTPException:
        conn.rollback()
        raise
    except Exception:
        conn.rollback()
        logger.exception("create_user failed")
        raise HTTPException(status_code=500, detail="No se pudo crear el usuario") from None
    finally:
        conn.close()

    record_auth_event(
        event_type=EVENT_USER_CREATED,
        email=email_key,
        user_id=user_id,
        success=True,
        actor_user_id=_actor_id(actor),
        detail={"role": role_n, "invite": invite},
    )
    out = get_user(user_id)
    if delivery:
        out["delivery"] = delivery
    return out


def update_user(
    *,
    actor: dict[str, Any],
    user_id: int,
    nombre_usuario: str | None = None,
    email: str | None = None,
    role: str | None = None,
    active: bool | None = None,
    company_ids: list[int] | None = None,
) -> dict[str, Any]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT email, role, active, nombre_usuario FROM bsale.users WHERE id = %s",
            (user_id,),
        )
        prev = cur.fetchone()
        if not prev:
            raise HTTPException(status_code=404, detail="Usuario no encontrado")
        prev_email, prev_role, prev_active, prev_name = prev

        if role is not None:
            if not can_assign_role(actor, role):
                raise HTTPException(status_code=403, detail="No autorizado a asignar este rol")
            role = role.strip().lower()

        _guard_self_privilege_change(actor=actor, user_id=user_id, role=role, active=active)

        _ensure_not_last_admin(
            cur,
            user_id=user_id,
            new_active=active,
            new_role=role,
        )

        new_email = prev_email
        if email is not None:
            new_email = email.strip().lower()
            if "@" not in new_email:
                raise HTTPException(status_code=400, detail="Correo inválido")
            cur.execute(
                """
                SELECT 1 FROM bsale.users
                WHERE lower(trim(email)) = %s AND id <> %s LIMIT 1
                """,
                (new_email, user_id),
            )
            if cur.fetchone():
                raise HTTPException(status_code=409, detail="El correo ya está registrado")

        sets: list[str] = []
        params: list[Any] = []
        if email is not None:
            sets.append("email = %s")
            params.append(new_email)
        if nombre_usuario is not None:
            sets.append("nombre_usuario = %s")
            params.append(nombre_usuario.strip() or None)
        if role is not None:
            sets.append("role = %s")
            params.append(role)
        if active is not None:
            sets.append("active = %s")
            params.append(active)
        if sets:
            params.append(user_id)
            cur.execute(
                f"UPDATE bsale.users SET {', '.join(sets)} WHERE id = %s",
                params,
            )

        if company_ids is not None:
            _set_user_companies(cur, user_id, company_ids)

        # Invalidar sesiones si se desactiva (cuando exista token_version).
        if active is False and bool(prev_active):
            if _column_exists(cur, "bsale", "users", "token_version"):
                cur.execute(
                    """
                    UPDATE bsale.users
                    SET token_version = COALESCE(token_version, 0) + 1
                    WHERE id = %s
                    """,
                    (user_id,),
                )

        conn.commit()
        cur.close()
    except HTTPException:
        conn.rollback()
        raise
    except Exception:
        conn.rollback()
        logger.exception("update_user failed")
        raise HTTPException(status_code=500, detail="No se pudo actualizar el usuario") from None
    finally:
        conn.close()

    actor_id = _actor_id(actor)
    if role is not None and role != prev_role:
        record_auth_event(
            event_type=EVENT_ROLE_CHANGE,
            email=new_email if email else prev_email,
            user_id=user_id,
            success=True,
            actor_user_id=actor_id,
            detail={"from": prev_role, "to": role},
        )
    if active is not None and bool(active) != bool(prev_active):
        record_auth_event(
            event_type=EVENT_USER_ACTIVATED if active else EVENT_USER_DEACTIVATED,
            email=new_email if email else prev_email,
            user_id=user_id,
            success=True,
            actor_user_id=actor_id,
        )
    if company_ids is not None:
        record_auth_event(
            event_type=EVENT_COMPANIES_CHANGE,
            email=new_email if email else prev_email,
            user_id=user_id,
            success=True,
            actor_user_id=actor_id,
            detail={"company_ids": company_ids},
        )
    return get_user(user_id)


def _create_password_token(
    cur,
    *,
    user_id: int,
    purpose: str,
    created_by: int | None,
) -> dict[str, Any]:
    if not _table_exists(cur, "app", "auth_password_tokens"):
        raise HTTPException(
            status_code=409,
            detail="Tabla app.auth_password_tokens no disponible; aplique la migración 063",
        )
    # Invalidar tokens previos no usados del mismo propósito.
    cur.execute(
        """
        UPDATE app.auth_password_tokens
        SET used_at = now()
        WHERE user_id = %s AND purpose = %s AND used_at IS NULL
        """,
        (user_id, purpose),
    )
    raw = secrets.token_urlsafe(32)
    th = _hash_token(raw)
    expires = datetime.now(timezone.utc) + timedelta(hours=RESET_TOKEN_TTL_HOURS)
    cur.execute(
        """
        INSERT INTO app.auth_password_tokens (
            user_id, token_hash, purpose, expires_at, created_by
        ) VALUES (%s, %s, %s, %s, %s)
        RETURNING id, expires_at
        """,
        (user_id, th, purpose, expires, created_by),
    )
    tid, exp = cur.fetchone()
    # El valor en claro SOLO sale aquí; no se audita ni se lista.
    return {
        "token_id": int(tid),
        "purpose": purpose,
        "expires_at": exp.isoformat() if hasattr(exp, "isoformat") else str(exp),
        "one_time_token": raw,
        "delivery_hint": (
            "Entregue este token al titular por un canal seguro. "
            "No se volverá a mostrar. Caduca en 24 horas."
        ),
    }


def issue_password_reset(*, actor: dict[str, Any], user_id: int) -> dict[str, Any]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT email, active FROM bsale.users WHERE id = %s",
            (user_id,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Usuario no encontrado")
        email, active = row[0], row[1]
        if not active:
            raise HTTPException(
                status_code=409,
                detail="Reactive la cuenta antes de restablecer la contraseña",
            )
        delivery = _create_password_token(
            cur,
            user_id=user_id,
            purpose="reset",
            created_by=_actor_id(actor),
        )
        # Invalidar sesiones previas si hay token_version.
        if _column_exists(cur, "bsale", "users", "token_version"):
            cur.execute(
                """
                UPDATE bsale.users
                SET token_version = COALESCE(token_version, 0) + 1
                WHERE id = %s
                """,
                (user_id,),
            )
        conn.commit()
        cur.close()
    except HTTPException:
        conn.rollback()
        raise
    except Exception:
        conn.rollback()
        logger.exception("issue_password_reset failed")
        raise HTTPException(status_code=500, detail="No se pudo emitir el restablecimiento") from None
    finally:
        conn.close()

    record_auth_event(
        event_type=EVENT_PASSWORD_RESET,
        email=email,
        user_id=user_id,
        success=True,
        actor_user_id=_actor_id(actor),
        detail={"mode": "admin_one_time_token"},
    )
    return {
        "user_id": user_id,
        "email": email,
        "delivery": delivery,
    }


def consume_password_token(*, raw_token: str, new_password: str) -> dict[str, Any]:
    if not raw_token or not raw_token.strip():
        raise HTTPException(status_code=400, detail="Token requerido")
    if not new_password or len(new_password) < MIN_PASSWORD_LEN:
        raise HTTPException(
            status_code=400,
            detail=f"Contraseña de al menos {MIN_PASSWORD_LEN} caracteres",
        )
    th = _hash_token(raw_token.strip())
    conn = get_connection()
    try:
        cur = conn.cursor()
        if not _table_exists(cur, "app", "auth_password_tokens"):
            raise HTTPException(status_code=503, detail="Restablecimiento no disponible")
        cur.execute(
            """
            SELECT id, user_id, purpose, expires_at, used_at
            FROM app.auth_password_tokens
            WHERE token_hash = %s
            LIMIT 1
            FOR UPDATE
            """,
            (th,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=400, detail="Token inválido o expirado")
        tid, user_id, purpose, expires_at, used_at = row
        if used_at is not None:
            raise HTTPException(status_code=400, detail="Token ya utilizado")
        now = datetime.now(timezone.utc)
        exp = expires_at
        if exp.tzinfo is None:
            exp = exp.replace(tzinfo=timezone.utc)
        if exp < now:
            raise HTTPException(status_code=400, detail="Token inválido o expirado")
        pwd_hash = hash_password(new_password)
        cur.execute(
            "UPDATE bsale.users SET password_hash = %s, active = TRUE WHERE id = %s",
            (pwd_hash, user_id),
        )
        cur.execute(
            "UPDATE app.auth_password_tokens SET used_at = now() WHERE id = %s",
            (tid,),
        )
        if _column_exists(cur, "bsale", "users", "token_version"):
            cur.execute(
                """
                UPDATE bsale.users
                SET token_version = COALESCE(token_version, 0) + 1
                WHERE id = %s
                """,
                (user_id,),
            )
        cur.execute("SELECT email FROM bsale.users WHERE id = %s", (user_id,))
        email = cur.fetchone()[0]
        conn.commit()
        cur.close()
    except HTTPException:
        conn.rollback()
        raise
    except Exception:
        conn.rollback()
        logger.exception("consume_password_token failed")
        raise HTTPException(status_code=500, detail="No se pudo actualizar la contraseña") from None
    finally:
        conn.close()

    record_auth_event(
        event_type=EVENT_PASSWORD_SET_VIA_TOKEN,
        email=email,
        user_id=int(user_id),
        success=True,
        detail={"purpose": purpose},
    )
    return {"ok": True, "email": email}


def list_auth_events(
    *,
    limit: int = 100,
    offset: int = 0,
    user_id: int | None = None,
    event_type: str | None = None,
) -> dict[str, Any]:
    limit = max(1, min(int(limit), 500))
    offset = max(0, int(offset))
    conn = get_connection()
    try:
        cur = conn.cursor()
        if not _table_exists(cur, "app", "auth_events"):
            cur.close()
            return {
                "items": [],
                "total": 0,
                "meta": {
                    "available": False,
                    "message": "Auditoría no disponible hasta aplicar migración 062 y AUTH_AUDIT_EVENTS=1",
                },
            }
        clauses: list[str] = []
        params: list[Any] = []
        if user_id is not None:
            clauses.append("user_id = %s")
            params.append(user_id)
        if event_type:
            clauses.append("event_type = %s")
            params.append(event_type)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        cur.execute(f"SELECT COUNT(*) FROM app.auth_events{where}", params)
        total = int(cur.fetchone()[0] or 0)
        cur.execute(
            f"""
            SELECT id, event_type, user_id, email, success, ip, user_agent,
                   actor_user_id, detail, created_at
            FROM app.auth_events
            {where}
            ORDER BY created_at DESC
            LIMIT %s OFFSET %s
            """,
            [*params, limit, offset],
        )
        rows = cur.fetchall()
        cur.close()
        items = []
        for r in rows:
            detail = r[8] if isinstance(r[8], dict) else (r[8] or {})
            # Nunca devolver campos sensibles si alguien los metió por error.
            if isinstance(detail, dict):
                detail = {
                    k: v
                    for k, v in detail.items()
                    if k not in ("password", "password_hash", "token", "one_time_token", "authorization")
                }
            items.append(
                {
                    "id": int(r[0]),
                    "event_type": r[1],
                    "user_id": int(r[2]) if r[2] is not None else None,
                    "email": r[3],
                    "success": r[4],
                    "ip": r[5],
                    "user_agent": r[6],
                    "actor_user_id": int(r[7]) if r[7] is not None else None,
                    "detail": detail,
                    "created_at": r[9].isoformat() if r[9] else None,
                }
            )
        return {"items": items, "total": total, "meta": {"available": True}}
    finally:
        conn.close()
