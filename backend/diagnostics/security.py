"""Habilitación del módulo de diagnóstico y verificación de permisos gerenciales."""

from __future__ import annotations

import os
from typing import Annotated

from fastapi import Header, Request

from backend.auth.permissions import require_management_access

BearerDep = Annotated[str | None, Header(alias="Authorization")]


def diagnostics_feature_enabled() -> bool:
    """
    Por defecto: activo en desarrollo; en staging/production requiere ENABLE_DIAGNOSTICS=true.
    Desactivar globalmente con DISABLE_DIAGNOSTICS=1.
    """
    if os.getenv("DISABLE_DIAGNOSTICS", "").strip().lower() in ("1", "true", "yes"):
        return False
    env = (os.getenv("ENVIRONMENT") or os.getenv("ENV") or "development").strip().lower()
    if env in ("production", "staging", "prod"):
        return os.getenv("ENABLE_DIAGNOSTICS", "").strip().lower() in ("1", "true", "yes")
    return True


def require_diagnostics_enabled() -> None:
    from fastapi import HTTPException

    if not diagnostics_feature_enabled():
        raise HTTPException(status_code=404, detail="Not found")


def require_diagnostics_admin(authorization: BearerDep, request: Request) -> dict:
    """Diagnóstico ERP — requiere acceso gerencial centralizado."""
    require_diagnostics_enabled()
    return require_management_access(
        authorization,
        request,
        required_permission="diagnostics",
    )
