"""Autorización explícita: permiso «Administrar usuarios» + jerarquía OWNER.

No se confía en localStorage ni en claims enviados por el frontend.
OWNER = AUTH_OWNER_USER_IDS ∩ id de sesión enriquecido (bsale.users).

Bootstrap:
- ``AUTH_OWNER_USER_IDS``: IDs propietario (máxima autoridad del panel).
- ``AUTH_USER_ADMIN_EMAILS``: correos autorizados a mutar (no-OWNER).
- ``AUTH_ASSIGN_ADM_EMAILS``: gerencia autorizada a asignar rol ``adm``.
- En production/staging/unknown: allowlist vacía ⇒ nadie muta salvo OWNER.
- ``AUTH_USER_ADMIN_DEV_FALLBACK=1`` solo en development|dev|local (nunca prod).
"""

from __future__ import annotations

import logging
import os
from typing import Annotated, Any

from fastapi import Header, HTTPException, Request

from backend.auth.hierarchy import (
    RANK_ADM,
    RANK_GERENCIA,
    RANK_OWNER,
    actor_rank,
    is_owner,
    is_owner_id,
    subject_rank,
)
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
FORBIDDEN_ASSIGN_ROLES = frozenset(
    {"owner", "superadmin", "super_admin", "administrator", "admin"}
)

_warned_empty_allowlist = False


def _runtime_environment() -> str:
    """Vacío o desconocido ≠ development (evita fallback accidental en Coolify)."""
    return (os.getenv("ENVIRONMENT") or os.getenv("ENV") or "").strip().lower()


def _is_explicit_local_dev() -> bool:
    return _runtime_environment() in ("development", "dev", "local")


def _is_protected_runtime() -> bool:
    env = _runtime_environment()
    if env in ("production", "staging", "prod"):
        return True
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
    if is_owner(user):
        return True
    role = normalized_role(user)
    return role in USER_ADMIN_VIEW_ROLES or has_management_access(user)


def can_mutate_user_admin(user: dict[str, Any] | None) -> bool:
    """Permiso explícito «Administrar usuarios» (mutaciones)."""
    global _warned_empty_allowlist
    if is_owner(user):
        return True

    email = _actor_email(user)
    allow = user_admin_email_allowlist()
    if allow:
        if email not in allow:
            return False
        return has_management_access(user) or has_admin_access(user)

    if _is_protected_runtime():
        if not _warned_empty_allowlist:
            _warned_empty_allowlist = True
            logger.warning(
                "AUTH_USER_ADMIN_EMAILS vacío (env=%r): mutaciones denegadas salvo OWNER. "
                "Configure AUTH_OWNER_USER_IDS y/o la allowlist.",
                _runtime_environment() or "(unset)",
            )
        return False

    if _is_explicit_local_dev() and _env_flag("AUTH_USER_ADMIN_DEV_FALLBACK", default=False):
        # Solo adm local; OWNER ya retornó arriba. Nunca activar en producción.
        return has_admin_access(user)
    return False


def can_assign_adm_role(actor: dict[str, Any] | None) -> bool:
    """Solo OWNER, o GERENCIA en AUTH_ASSIGN_ADM_EMAILS (nunca ADM)."""
    if is_owner(actor):
        return True
    if not can_mutate_user_admin(actor):
        return False
    if normalized_role(actor) != "gerencia":
        return False
    grant = assign_adm_email_allowlist()
    if not grant:
        return False
    return _actor_email(actor) in grant


def can_assign_role(actor: dict[str, Any] | None, target_role: str) -> bool:
    if not can_mutate_user_admin(actor):
        return False
    role = (target_role or "").strip().lower()
    if role in FORBIDDEN_ASSIGN_ROLES or role == "owner":
        return False
    if role not in ASSIGNABLE_ROLES:
        return False
    if role == "gerencia":
        return is_owner(actor)
    if role == "adm":
        return can_assign_adm_role(actor)
    # operativo: mutador autorizado cuyo rango permite gestionar operativo
    return can_manage_subject(actor, target_id=None, target_role="operativo")


def can_manage_subject(
    actor: dict[str, Any] | None,
    *,
    target_id: int | None,
    target_role: str | None,
) -> bool:
    """¿Puede el actor actuar sobre un sujeto con ese id/rol actuales?"""
    if not can_mutate_user_admin(actor):
        return False
    if is_owner_id(target_id):
        return False
    target_r = subject_rank(user_id=target_id, role=target_role)
    if target_r >= RANK_OWNER:
        return False
    if is_owner(actor):
        return target_r < RANK_OWNER
    ar = actor_rank(actor)
    if ar >= RANK_GERENCIA:
        # Gerencia autorizada: ADM y OPERATIVO (no otras gerencias ni OWNER).
        return target_r < RANK_GERENCIA
    if ar >= RANK_ADM:
        # ADM autorizado: solo OPERATIVO (no otros ADM).
        return target_r < RANK_ADM
    return False


def assert_panel_action_allowed(
    actor: dict[str, Any],
    *,
    target_id: int | None,
    current_role: str | None,
    new_role: str | None = None,
    changing_active: bool | None = None,
    changing_companies: bool = False,
    password_reset: bool = False,
) -> None:
    """Validación unificada de mutaciones del panel (actual + nuevo rol)."""
    if target_id is not None and is_owner_id(target_id):
        raise HTTPException(
            status_code=403,
            detail=(
                "Las cuentas OWNER no se desactivan, no cambian de rol, "
                "empresas ni contraseña desde este panel. "
                "Use el procedimiento de recuperación documentado."
            ),
        )

    if not can_manage_subject(actor, target_id=target_id, target_role=current_role):
        raise HTTPException(
            status_code=403,
            detail="No autorizado a administrar esta cuenta según la jerarquía vigente",
        )

    if new_role is not None:
        nr = new_role.strip().lower()
        if not can_assign_role(actor, nr):
            raise HTTPException(status_code=403, detail="No autorizado a asignar este rol")
        if not can_manage_subject(actor, target_id=None, target_role=nr):
            raise HTTPException(
                status_code=403,
                detail="El rol solicitado supera su autoridad jerárquica",
            )

    actor_id = None
    raw = actor.get("id") if actor.get("id") is not None else actor.get("uid")
    try:
        actor_id = int(raw) if raw is not None else None
    except (TypeError, ValueError):
        actor_id = None

    if actor_id is not None and target_id is not None and int(actor_id) == int(target_id):
        if new_role is not None and normalized_role({"role": new_role}) != normalized_role(
            {"role": current_role}
        ):
            raise HTTPException(
                status_code=403,
                detail="No puede cambiar su propio rol; solicite a una autoridad superior",
            )
        if changing_active is False:
            raise HTTPException(status_code=403, detail="No puede desactivar su propia cuenta")
        if changing_companies:
            raise HTTPException(
                status_code=403,
                detail="No puede modificar sus propias empresas autorizadas desde este panel",
            )
        if password_reset and is_owner(actor):
            raise HTTPException(
                status_code=403,
                detail="OWNER no restablece su contraseña desde este panel",
            )


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
