"""Estado de sesión staff: uid en JWT, active check y token_version (opt-in).

La invalidación real de JWT exige ``token_version`` en BD + claim ``tv`` + flag
``AUTH_ENFORCE_TOKEN_VERSION=1``. Sin eso, un JWT firmado no se puede revocar.
"""

from __future__ import annotations

import logging
import os
from typing import Any

from fastapi import HTTPException

from backend.db import get_connection

logger = logging.getLogger(__name__)


def _env_flag(name: str, *, default: bool = False) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    return raw in ("1", "true", "yes", "on")


def check_active_on_request() -> bool:
    return _env_flag("AUTH_CHECK_ACTIVE_ON_REQUEST", default=False)


def enforce_token_version() -> bool:
    return _env_flag("AUTH_ENFORCE_TOKEN_VERSION", default=False)


def emit_company_ids_claim() -> bool:
    """Si hay filas en app.user_companies, incluir claim; sin filas → no restringir."""
    return _env_flag("AUTH_EMIT_COMPANY_IDS", default=False)


def require_user_companies() -> bool:
    """Cutover: sin filas en user_companies = denegar (default off para no bloquear gerencia)."""
    return _env_flag("AUTH_REQUIRE_USER_COMPANIES", default=False)


def _table_exists(cur, schema: str, table: str) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = %s AND table_name = %s
        LIMIT 1
        """,
        (schema, table),
    )
    return cur.fetchone() is not None


def _column_exists(cur, schema: str, table: str, column: str) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s AND column_name = %s
        LIMIT 1
        """,
        (schema, table, column),
    )
    return cur.fetchone() is not None


def load_staff_login_row(email_key: str) -> dict[str, Any] | None:
    """Carga fila de login. ``token_version`` = 0 si la columna aún no existe."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        has_tv = _column_exists(cur, "bsale", "users", "token_version")
        cols = "id, password_hash, role, active"
        if has_tv:
            cols += ", COALESCE(token_version, 0) AS token_version"
        cur.execute(
            f"""
            SELECT {cols}
            FROM bsale.users
            WHERE lower(trim(email)) = %s
            LIMIT 1
            """,
            (email_key,),
        )
        row = cur.fetchone()
        cur.close()
        if not row:
            return None
        out: dict[str, Any] = {
            "id": int(row[0]),
            "password_hash": row[1],
            "role": row[2],
            "active": bool(row[3]) if row[3] is not None else False,
            "token_version": int(row[4]) if has_tv and len(row) > 4 and row[4] is not None else 0,
            "has_token_version_column": has_tv,
        }
        return out
    finally:
        conn.close()


def upgrade_legacy_password(*, user_id: int, new_hash: str, previous_stored: str) -> bool:
    """Actualiza hash solo si el valor almacenado no cambió (evita carrera). Sin log de secretos."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE bsale.users
            SET password_hash = %s
            WHERE id = %s AND password_hash = %s
            """,
            (new_hash, user_id, previous_stored),
        )
        ok = cur.rowcount == 1
        conn.commit()
        cur.close()
        if ok:
            logger.info("legacy password upgraded to bcrypt user_id=%s", user_id)
        return ok
    except Exception:
        conn.rollback()
        logger.exception("failed to upgrade legacy password user_id=%s", user_id)
        return False
    finally:
        conn.close()


def load_user_company_ids(user_id: int) -> list[int] | None:
    """None = no restringir (tabla ausente o flag off). Lista vacía solo si require cutover."""
    if not emit_company_ids_claim() and not require_user_companies():
        return None
    conn = get_connection()
    try:
        cur = conn.cursor()
        if not _table_exists(cur, "app", "user_companies"):
            cur.close()
            return None
        cur.execute(
            """
            SELECT company_id
            FROM app.user_companies
            WHERE user_id = %s
            ORDER BY company_id
            """,
            (user_id,),
        )
        ids = [int(r[0]) for r in cur.fetchall()]
        cur.close()
        if not ids:
            if require_user_companies():
                return []
            return None
        return ids
    except Exception:
        logger.exception("load_user_company_ids failed user_id=%s", user_id)
        return None
    finally:
        conn.close()


def fetch_staff_session_state(user_id: int | None, email: str) -> dict[str, Any] | None:
    conn = get_connection()
    try:
        cur = conn.cursor()
        has_tv = _column_exists(cur, "bsale", "users", "token_version")
        if user_id is not None:
            if has_tv:
                cur.execute(
                    """
                    SELECT id, active, COALESCE(token_version, 0), role, email
                    FROM bsale.users WHERE id = %s LIMIT 1
                    """,
                    (int(user_id),),
                )
            else:
                cur.execute(
                    """
                    SELECT id, active, 0, role, email
                    FROM bsale.users WHERE id = %s LIMIT 1
                    """,
                    (int(user_id),),
                )
        else:
            if has_tv:
                cur.execute(
                    """
                    SELECT id, active, COALESCE(token_version, 0), role, email
                    FROM bsale.users
                    WHERE lower(trim(email)) = lower(trim(%s))
                    LIMIT 1
                    """,
                    (email,),
                )
            else:
                cur.execute(
                    """
                    SELECT id, active, 0, role, email
                    FROM bsale.users
                    WHERE lower(trim(email)) = lower(trim(%s))
                    LIMIT 1
                    """,
                    (email,),
                )
        row = cur.fetchone()
        cur.close()
        if not row:
            return None
        return {
            "id": int(row[0]),
            "active": bool(row[1]) if row[1] is not None else False,
            "token_version": int(row[2] or 0),
            "role": row[3],
            "email": row[4],
            "has_token_version_column": has_tv,
        }
    finally:
        conn.close()


def bump_token_version(user_id: int) -> int | None:
    """Incrementa ``token_version`` para invalidar JWT previos. Requiere columna migrada.

    Pensado para desactivación / reset de password (módulo Usuarios). Devuelve la
    nueva versión o ``None`` si la columna no existe aún.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        if not _column_exists(cur, "bsale", "users", "token_version"):
            cur.close()
            return None
        cur.execute(
            """
            UPDATE bsale.users
            SET token_version = COALESCE(token_version, 0) + 1
            WHERE id = %s
            RETURNING token_version
            """,
            (int(user_id),),
        )
        row = cur.fetchone()
        conn.commit()
        cur.close()
        return int(row[0]) if row else None
    except Exception:
        conn.rollback()
        logger.exception("bump_token_version failed user_id=%s", user_id)
        return None
    finally:
        conn.close()


def enrich_staff_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Normaliza ``id``/``uid`` sin exigir BD. Validación estricta solo con flags."""
    out = dict(payload)
    uid = out.get("uid") if out.get("uid") is not None else out.get("id")
    if uid is not None:
        try:
            out["id"] = int(uid)
            out["uid"] = int(uid)
        except (TypeError, ValueError):
            pass

    need_db = check_active_on_request() or enforce_token_version()
    if not need_db:
        return out

    email = str(out.get("email") or "")
    state = fetch_staff_session_state(out.get("id"), email)
    if state is None:
        raise HTTPException(status_code=401, detail="Usuario no encontrado o sesión inválida")
    if check_active_on_request() and not state["active"]:
        raise HTTPException(status_code=401, detail="Cuenta desactivada")
    if enforce_token_version():
        if not state["has_token_version_column"]:
            # Columna ausente: no improvisar revocación; documentado como bloqueo de cutover.
            logger.warning(
                "AUTH_ENFORCE_TOKEN_VERSION activo pero bsale.users.token_version ausente; "
                "se omite comprobación de versión"
            )
        else:
            claim_tv = out.get("tv")
            if claim_tv is None:
                raise HTTPException(status_code=401, detail="Sesión revocada o desactualizada")
            try:
                claim_tv_i = int(claim_tv)
            except (TypeError, ValueError):
                raise HTTPException(status_code=401, detail="Sesión revocada o desactualizada") from None
            if claim_tv_i != int(state["token_version"]):
                raise HTTPException(status_code=401, detail="Sesión revocada o desactualizada")

    out["id"] = state["id"]
    out["uid"] = state["id"]
    out["role"] = state.get("role") or out.get("role")
    return out
