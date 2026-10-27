"""Jerarquía OWNER > GERENCIA > ADM > OPERATIVO.

OWNER no es un rol en bsale.users: se reconoce por AUTH_OWNER_USER_IDS
(IDs reales de bsale.users) frente a la identidad enriquecida del JWT.
"""

from __future__ import annotations

import os
from typing import Any

from backend.auth.permissions import ADMIN_ROLES, normalized_role

RANK_OWNER = 40
RANK_GERENCIA = 30
RANK_ADM = 20
RANK_OPERATIVO = 10
RANK_UNKNOWN = 0


def owner_user_ids() -> frozenset[int]:
    """IDs de propietario desde env. Vacío ⇒ nadie es OWNER."""
    raw = (os.getenv("AUTH_OWNER_USER_IDS") or "").strip()
    if not raw:
        return frozenset()
    out: set[int] = set()
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            uid = int(part)
        except ValueError:
            continue
        if uid > 0:
            out.add(uid)
    return frozenset(out)


def _user_id(user: dict[str, Any] | None) -> int | None:
    if not user:
        return None
    raw = user.get("id") if user.get("id") is not None else user.get("uid")
    try:
        return int(raw) if raw is not None else None
    except (TypeError, ValueError):
        return None


def is_owner_id(user_id: int | None) -> bool:
    if user_id is None:
        return False
    try:
        return int(user_id) in owner_user_ids()
    except (TypeError, ValueError):
        return False


def is_owner(user: dict[str, Any] | None) -> bool:
    """True solo si el id autenticado está en AUTH_OWNER_USER_IDS."""
    return is_owner_id(_user_id(user))


def role_rank(role: str | None) -> int:
    r = normalized_role({"role": role})
    if r == "gerencia":
        return RANK_GERENCIA
    if r in ADMIN_ROLES:
        return RANK_ADM
    if r == "operativo":
        return RANK_OPERATIVO
    return RANK_UNKNOWN


def subject_rank(*, user_id: int | None, role: str | None) -> int:
    """Rango efectivo del sujeto (OWNER por ID, aunque role=gerencia)."""
    if is_owner_id(user_id):
        return RANK_OWNER
    return role_rank(role)


def actor_rank(actor: dict[str, Any] | None) -> int:
    return subject_rank(user_id=_user_id(actor), role=(actor or {}).get("role"))
