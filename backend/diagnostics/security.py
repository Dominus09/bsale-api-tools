"""Habilitación del módulo de diagnóstico y verificación de rol administrador (JWT staff)."""

from __future__ import annotations

import os
from typing import Annotated

import jwt
from fastapi import Header, HTTPException

from backend.routers.auth import SECRET

_ADMIN_ROLES = frozenset(
    {
        "admin",
        "superadmin",
        "super_admin",
        "administrator",
    }
)


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
    if not diagnostics_feature_enabled():
        # 404 para no revelar existencia del endpoint en entornos cerrados
        raise HTTPException(status_code=404, detail="Not found")


def decode_staff_token(authorization: str | None) -> dict:
    require_diagnostics_enabled()
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Se requiere autenticación")
    token = authorization[7:].strip()
    if not token:
        raise HTTPException(status_code=401, detail="Token vacío")
    try:
        payload = jwt.decode(token, SECRET, algorithms=["HS256"])
    except jwt.PyJWTError as e:
        raise HTTPException(status_code=401, detail="Token inválido") from e
    role = str(payload.get("role") or "").strip().lower()
    if role not in _ADMIN_ROLES:
        raise HTTPException(
            status_code=403,
            detail="Solo usuarios con rol de administración pueden acceder al diagnóstico",
        )
    return payload


BearerDep = Annotated[str | None, Header(alias="Authorization")]


def require_diagnostics_admin(authorization: BearerDep) -> dict:
    return decode_staff_token(authorization)
