"""Auth compartida telemetría móvil (heartbeat, gps_track)."""

from __future__ import annotations

import os

from fastapi import HTTPException

from backend.utils.auth_staff import decode_staff_token


def verify_operaciones_mobile_auth(
    x_heartbeat_key: str | None,
    authorization: str | None,
) -> None:
    """
    - ``OPERACIONES_HEARTBEAT_API_KEY`` definida → ``X-Heartbeat-Key`` o Bearer con ese valor.
    - Sin clave → abierto (validación de vendedor en servicio).
    - JWT staff válido → permitido.
    """
    if authorization and authorization.lower().startswith("bearer "):
        token = authorization[7:].strip()
        secret = os.getenv("OPERACIONES_HEARTBEAT_API_KEY", "").strip()
        if secret and token == secret:
            return
        try:
            decode_staff_token(authorization)
            return
        except HTTPException:
            pass

    secret = os.getenv("OPERACIONES_HEARTBEAT_API_KEY", "").strip()
    if not secret:
        return

    key = (x_heartbeat_key or "").strip()
    if not key and authorization and authorization.lower().startswith("bearer "):
        key = authorization[7:].strip()
    if key != secret:
        raise HTTPException(status_code=401, detail="Clave de telemetría inválida")
