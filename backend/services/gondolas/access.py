"""Acceso a Control de Góndolas: permiso de operación por rol y empresa verificada en backend.

Operar recorridos (crear, escanear, cerrar, reponer): operativo, gerencia y administración.
Cancelar un recorrido: solo gerencia y administración. El aislamiento por empresa y la
disponibilidad de BSALE RAW se verifican con ``resolve_company_scope`` de Analítica V2.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from fastapi import HTTPException, Request

from backend.auth.jwt import decode_staff_token
from backend.auth.permissions import BearerDep, has_management_access, normalized_role

logger = logging.getLogger(__name__)

OPERATE_EXTRA_ROLES = frozenset({"operativo"})


def gondola_capabilities(user: dict[str, Any] | None) -> dict[str, bool]:
    operate = has_management_access(user) or normalized_role(user) in OPERATE_EXTRA_ROLES
    return {"operate": operate, "cancel": has_management_access(user)}


def _require(request: Request, authorization: str | None, capability: str, label: str) -> dict[str, Any]:
    user = decode_staff_token(authorization)
    if not gondola_capabilities(user).get(capability):
        logger.warning(
            "[AUTH] gondolas user_id=%s role=%s endpoint=%s required_permission=%s timestamp=%s reason=%s",
            user.get("id"),
            user.get("role"),
            request.url.path,
            capability,
            datetime.now(timezone.utc).isoformat(),
            f"Rol sin permiso de {label} en Control de Góndolas",
        )
        raise HTTPException(status_code=403, detail=f"Acceso restringido: requiere permiso de {label} en Control de Góndolas")
    return user


def require_gondola_operate(request: Request, authorization: BearerDep = None) -> dict[str, Any]:
    """Dependencia FastAPI: trabajar recorridos de góndolas (operativo, gerencia, adm)."""
    return _require(request, authorization, "operate", "operación")


def require_gondola_cancel(request: Request, authorization: BearerDep = None) -> dict[str, Any]:
    """Dependencia FastAPI: cancelar recorridos (solo gerencia y administración)."""
    return _require(request, authorization, "cancel", "cancelación")
