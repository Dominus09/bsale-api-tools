"""Permisos centralizados del ERP — nunca comparar roles como strings en routers."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Annotated, Any

from fastapi import Header, HTTPException, Request

from backend.db import get_connection
from backend.auth.jwt import decode_staff_token
from backend.auth.staff_session import enrich_staff_payload

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
    from backend.auth.hierarchy import is_owner

    if is_owner(user):
        return True
    role = normalized_role(user)
    if role in MANAGEMENT_ROLES:
        return True
    return role in ADMIN_ROLES


def has_admin_access(user: dict[str, Any] | None) -> bool:
    """Acceso técnico de administración (rol adm) o OWNER configurado.

    OWNER hereda módulos que hoy exigen ``require_admin_access`` (p. ej. sync
    histórico de NC). No implica privilegios de infraestructura/PostgreSQL.
    """
    from backend.auth.hierarchy import is_owner

    if is_owner(user):
        return True
    return normalized_role(user) in ADMIN_ROLES


def has_margin_view_access(user: dict[str, Any] | None) -> bool:
    """Márgenes en planificación y módulos financieros sensibles."""
    role = normalized_role(user)
    return has_management_access(user) or role in MARGIN_VIEW_EXTRA_ROLES


def has_operational_access(user: dict[str, Any] | None) -> bool:
    """Futuro: módulos operacionales (hoy cualquier usuario staff autenticado)."""
    return bool(normalized_role(user) or user and user.get("email"))


SIMPLE_PLANNING_ROLES = frozenset(
    {
        "gerencia",
        "operativo",
        "adm",
        "admin",
    }
)


def has_simple_planning_access(user: dict[str, Any] | None) -> bool:
    """Planificación simplificada (lista/Excel): gerencia/operativo o capability ERP.

    No exige privilegios de administración global del panel Usuarios.
    OWNER y roles de gerencia/adm/operativo heredan acceso.

    La segunda persona (Excel) debe recibir ``app.user_erp_capabilities.simple_planning``
    **y** app ERP activa. La capability sola no concede ERP ni PWA ni admin.
    No usar perfiles PWA (065) para esta pantalla.
    """
    from backend.auth.hierarchy import is_owner
    from backend.auth.multiapp import (
        CAP_SIMPLE_PLANNING,
        get_scope,
        has_erp_capability,
    )

    if not user:
        return False
    if is_owner(user):
        return True
    role = normalized_role(user)
    if role in SIMPLE_PLANNING_ROLES:
        return True
    caps = user.get("erp_capabilities") or []
    if isinstance(caps, (list, tuple, set, frozenset)):
        if CAP_SIMPLE_PLANNING in {str(c).strip().lower() for c in caps}:
            return True
    try:
        return has_erp_capability(get_scope(user), CAP_SIMPLE_PLANNING)
    except Exception:
        return False


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
    from backend.auth.hierarchy import is_owner
    from backend.auth.multiapp import scope_public_dict
    from backend.auth.user_admin_access import permission_flags

    email = str(user.get("email") or "")
    user_id = user.get("id") or user.get("uid") or _fetch_staff_user_id(email)
    enriched = {**user, "email": email, "id": user_id}
    permissions = resolve_executive_permissions(enriched)
    permissions.update(permission_flags(enriched))
    owner = is_owner(enriched)
    multi = scope_public_dict(enriched)
    return {
        "id": user_id,
        "email": email,
        "role": user.get("role"),
        "is_owner": owner,
        "management_access": has_management_access(enriched) or owner,
        "admin_access": has_admin_access(enriched),
        "permissions": permissions,
        **multi,
    }


def require_management_access(
    authorization: BearerDep = None,
    request: Request = None,
    *,
    required_permission: str = "management_access",
) -> dict[str, Any]:
    """Dependencia FastAPI: exige acceso gerencial/ejecutivo (+ app ERP)."""
    from backend.auth.multiapp import require_erp_app

    user = require_erp_app(authorization, request)
    if not has_management_access(user):
        _log_auth_denial(
            user=user,
            endpoint=(request.url.path if request is not None else "-"),
            required_permission=required_permission,
            reason="Rol sin acceso gerencial",
        )
        raise HTTPException(
            status_code=403,
            detail="Acceso restringido a usuarios con permisos de gerencia",
        )
    return user


def require_admin_access(
    authorization: BearerDep = None,
    request: Request = None,
    *,
    required_permission: str = "admin_access",
) -> dict[str, Any]:
    """Dependencia FastAPI: exige rol de administración técnica (+ app ERP)."""
    from backend.auth.multiapp import require_erp_app

    user = require_erp_app(authorization, request)
    if not has_admin_access(user):
        _log_auth_denial(
            user=user,
            endpoint=(request.url.path if request is not None else "-"),
            required_permission=required_permission,
            reason="Rol sin acceso de administración",
        )
        raise HTTPException(
            status_code=403,
            detail="Acceso restringido a administradores",
        )
    return user
