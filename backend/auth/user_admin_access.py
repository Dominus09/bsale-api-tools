"""Autorización explícita: permiso «Administrar usuarios».

No se confía en localStorage. FastAPI valida en cada mutación.

Bootstrap:
- ``AUTH_USER_ADMIN_EMAILS``: correos autorizados a mutar usuarios.
- En production/staging/prod: si la allowlist está vacía, **nadie** muta
  (no hay fallback a todos los ``adm``).
- En development: fallback a ``ADMIN_ROLES`` solo si
  ``AUTH_USER_ADMIN_DEV_FALLBACK=1`` (opt-in local).
- ``AUTH_ASSIGN_ADM_EMAILS``: quién puede asignar rol ``adm``.

Recuperación administrativa controlada: configurar allowlist en Coolify y
reiniciar el API. No modificar filas reales desde este módulo.
"""

from __future__ import annotations

import logging
import os
from typing import Annotated, Any

from fastapi import Header, HTTPException, Request

from backend.auth.jwt import decode_staff_token
from backend.auth.permissions import (
    ADMIN_ROLES,
    MANAGEMENT_ROLES,
    has_admin_access,
    has_management_access,
    normalized_role,
)
from backend.auth.staff_session import enrich_staff_payload

logger = logging.getLogger(__name__)

BearerDep = Annotated[str | None, Header(alias="Authorization")]

ASSIGNABLE_ROLES = frozenset({"operativo", "gerencia", "adm"})
USER_ADMIN_VIEW_ROLES = MANAGEMENT_ROLES | ADMIN_ROLES

_warned_empty_allowlist = False


def _runtime_environment() -> str:
    """Vacío o desconocido ≠ development (evita fallback accidental en Coolify)."""
    return (os.getenv("ENVIRONMENT") or os.getenv("ENV") or "").strip().lower()


def _is_explicit_local_dev() -> bool:
    return _runtime_environment() in ("development", "dev", "local")


def _is_protected_runtime() -> bool:
    """Production/staging y también entornos no identificados: sin mutación automática."""
    env = _runtime_environment()
    if env in ("production", "staging", "prod"):
        return True
    # Sin ENVIRONMENT o valor desconocido: tratar como protegido.
    return not _is_explicit_local_dev()


def _env_flag(name: str, *, default: bool = False) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    return raw in ("1", "true", "yes", "on")


def _parse_email_allowlist(env_name: str) -> frozenset[str]:
    raw = (os.getenv(env_name) or "").strip()
    if not raw:
        return frozenset()
    return frozenset(
        part.strip().lower()
        for part in raw.split(",")
        if part.strip() and "@" in part
    )


def user_admin_email_allowlist() -> frozenset[str]:
    return _parse_email_allowlist("AUTH_USER_ADMIN_EMAILS")


def assign_adm_email_allowlist() -> frozenset[str]:
    return _parse_email_allowlist("AUTH_ASSIGN_ADM_EMAILS")


def _actor_email(user: dict[str, Any] | None) -> str:
    if not user:
        return ""
    return str(user.get("email") or "").strip().lower()


def can_view_user_admin(user: dict[str, Any] | None) -> bool:
    role = normalized_role(user)
    return role in USER_ADMIN_VIEW_ROLES or has_management_access(user)


def can_mutate_user_admin(user: dict[str, Any] | None) -> bool:
    """Permiso explícito «Administrar usuarios» (mutaciones)."""
    global _warned_empty_allowlist
    email = _actor_email(user)
    allow = user_admin_email_allowlist()
    if allow:
        if email not in allow:
            return False
        return has_management_access(user) or has_admin_access(user)

    # Allowlist vacía: producción/staging/entorno desconocido → nadie muta.
    if _is_protected_runtime():
        if not _warned_empty_allowlist:
            _warned_empty_allowlist = True
            logger.warning(
                "AUTH_USER_ADMIN_EMAILS vacío (env=%r): mutaciones de usuarios denegadas. "
                "Configure la allowlist para habilitar el panel administrativo.",
                _runtime_environment() or "(unset)",
            )
        return False

    # Solo development/dev/local explícito + flag opt-in.
    if _is_explicit_local_dev() and _env_flag("AUTH_USER_ADMIN_DEV_FALLBACK", default=False):
        return has_admin_access(user)
    return False


def can_assign_adm_role(actor: dict[str, Any] | None) -> bool:
    if not can_mutate_user_admin(actor):
        return False
    email = _actor_email(actor)
    grant = assign_adm_email_allowlist()
    if grant:
        return email in grant
    # Sin AUTH_ASSIGN_ADM_EMAILS: nadie asigna rol adm (ni aunque pueda mutar).
    return False


def can_assign_role(actor: dict[str, Any] | None, target_role: str) -> bool:
    if not can_mutate_user_admin(actor):
        return False
    role = (target_role or "").strip().lower()
    if role not in ASSIGNABLE_ROLES:
        return False
    if role == "adm":
        return can_assign_adm_role(actor)
    return True


def permission_flags(user: dict[str, Any] | None) -> dict[str, bool]:
    return {
        "user_admin_view": can_view_user_admin(user),
        "user_admin": can_mutate_user_admin(user),
        "user_admin_assign_adm": can_assign_adm_role(user),
    }


def require_user_admin_view(
    authorization: BearerDep = None,
    request: Request = None,
) -> dict[str, Any]:
    user = enrich_staff_payload(decode_staff_token(authorization))
    if not can_view_user_admin(user):
        raise HTTPException(
            status_code=403,
            detail="Acceso restringido: requiere gerencia o administración",
        )
    return user


def require_user_admin_mutate(
    authorization: BearerDep = None,
    request: Request = None,
) -> dict[str, Any]:
    user = enrich_staff_payload(decode_staff_token(authorization))
    if not can_mutate_user_admin(user):
        raise HTTPException(
            status_code=403,
            detail="Acceso restringido: requiere permiso «Administrar usuarios»",
        )
    return user
