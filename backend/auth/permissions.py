"""Permisos centralizados del ERP — nunca comparar roles como strings en routers."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Annotated, Any

from fastapi import Header, HTTPException, Request

from backend.db import get_connection
from backend.auth.jwt import decode_staff_token

logger = logging.getLogger(__name__)

BearerDep = Annotated[str | None, Header(alias="Authorization")]

MANAGEMENT_ROLES = frozenset(
    {
        "gerencia",
        "adm",
        "admin",
    }
)

ADMIN_ROLES = frozenset(
    {
        "adm",
        "admin",
        "superadmin",
        "super_admin",
        "administrator",
    }
)

MARGIN_VIEW_EXTRA_ROLES = frozenset(
    {
        "finanzas",
        "finance",
    }
)

EXECUTIVE_PERMISSION_KEYS = (
    "commercial_validation",
    "diagnostics",
    "costs",
    "margins",
)


def normalized_role(user: dict[str, Any] | None) -> str:
    if not user:
        return ""
    return str(user.get("role") or "").strip().lower()


def has_management_access(user: dict[str, Any] | None) -> bool:
    """Acceso gerencial / ejecutivo (gerencia, adm, admin y roles admin legacy)."""
    role = normalized_role(user)
    if role in MANAGEMENT_ROLES:
        return True
    return role in ADMIN_ROLES


def has_admin_access(user: dict[str, Any] | None) -> bool:
    """Acceso técnico de administración."""
    return normalized_role(user) in ADMIN_ROLES


def has_margin_view_access(user: dict[str, Any] | None) -> bool:
    """Márgenes en planificación y módulos financieros sensibles."""
    role = normalized_role(user)
    return has_management_access(user) or role in MARGIN_VIEW_EXTRA_ROLES


def has_operational_access(user: dict[str, Any] | None) -> bool:
    """Futuro: módulos operacionales (hoy cualquier usuario staff autenticado)."""
    return bool(normalized_role(user) or user and user.get("email"))


def has_sales_access(user: dict[str, Any] | None) -> bool:
    """Futuro: módulos comerciales de venta."""
    return has_operational_access(user)


def resolve_executive_permissions(user: dict[str, Any] | None) -> dict[str, bool]:
    mgmt = has_management_access(user)
    margins = has_margin_view_access(user)
    return {
        "commercial_validation": mgmt,
        "diagnostics": mgmt,
        "costs": mgmt,
        "margins": margins,
    }


def _log_auth_denial(
    *,
    user: dict[str, Any],
    endpoint: str | None,
    required_permission: str,
    reason: str,
) -> None:
    logger.warning(
        "[AUTH] user_id=%s email=%s role=%s endpoint=%s required_permission=%s "
        "timestamp=%s reason=%s",
        user.get("id"),
        user.get("email"),
        user.get("role"),
        endpoint or "-",
        required_permission,
        datetime.now(timezone.utc).isoformat(),
        reason,
    )


def _fetch_staff_user_id(email: str) -> int | None:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id FROM bsale.users
            WHERE lower(trim(email)) = lower(trim(%s)) AND active = TRUE
            LIMIT 1
            """,
            (email,),
        )
        row = cur.fetchone()
        cur.close()
        return int(row[0]) if row else None
    except Exception:
        return None
    finally:
        conn.close()


def build_auth_me_response(user: dict[str, Any]) -> dict[str, Any]:
    email = str(user.get("email") or "")
    user_id = user.get("id") or _fetch_staff_user_id(email)
    permissions = resolve_executive_permissions(user)
    return {
        "id": user_id,
        "email": email,
        "role": user.get("role"),
        "management_access": has_management_access(user),
        "admin_access": has_admin_access(user),
        "permissions": permissions,
    }


def require_management_access(
    authorization: BearerDep,
    request: Request,
    *,
    required_permission: str = "management_access",
) -> dict[str, Any]:
    """Dependencia FastAPI: exige acceso gerencial/ejecutivo."""
    user = decode_staff_token(authorization)
    if not has_management_access(user):
        _log_auth_denial(
            user=user,
            endpoint=request.url.path,
            required_permission=required_permission,
            reason="Rol sin acceso gerencial",
        )
        raise HTTPException(
            status_code=403,
            detail="Acceso restringido a usuarios con permisos de gerencia",
        )
    return user


def require_admin_access(
    authorization: BearerDep,
    request: Request,
    *,
    required_permission: str = "admin_access",
) -> dict[str, Any]:
    """Dependencia FastAPI: exige rol de administración técnica."""
    user = decode_staff_token(authorization)
    if not has_admin_access(user):
        _log_auth_denial(
            user=user,
            endpoint=request.url.path,
            required_permission=required_permission,
            reason="Rol sin acceso de administración",
        )
        raise HTTPException(
            status_code=403,
            detail="Acceso restringido a administradores",
        )
    return user
