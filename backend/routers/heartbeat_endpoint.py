"""
POST heartbeat — handler compartido (panel /operaciones y app /app_distribuidora).
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Annotated

from fastapi import Header, HTTPException

from backend.schemas.operaciones import HeartbeatAckResponse, HeartbeatRequest
from backend.services import heartbeat_service
from backend.utils.auth_staff import decode_staff_token

logger = logging.getLogger(__name__)


def _verify_heartbeat_auth(
    x_heartbeat_key: str | None,
    authorization: str | None,
) -> None:
    """
    Auth compatible con despliegue actual:
    - Si ``OPERACIONES_HEARTBEAT_API_KEY`` está definida → exige ``X-Heartbeat-Key`` o Bearer con ese valor.
    - Si no hay clave → acepta sin token (solo valida vendedor en BD).
    - Bearer JWT staff válido → siempre permitido (pruebas / herramientas internas).
    """
    if authorization and authorization.lower().startswith("bearer "):
        token = authorization[7:].strip()
        secret = os.getenv("OPERACIONES_HEARTBEAT_API_KEY", "").strip()
        if secret and token == secret:
            return
        try:
            decode_staff_token(authorization)
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug("heartbeat auth: JWT staff OK")
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
        raise HTTPException(status_code=401, detail="Clave heartbeat inválida")


async def handle_heartbeat(
    body: HeartbeatRequest,
    x_heartbeat_key: Annotated[str | None, Header(alias="X-Heartbeat-Key")] = None,
    authorization: Annotated[str | None, Header(alias="Authorization")] = None,
) -> HeartbeatAckResponse:
    _verify_heartbeat_auth(x_heartbeat_key, authorization)

    server_ts = datetime.now(timezone.utc)

    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(
            "heartbeat payload vendedor=%s ts=%s lat=%s lng=%s bateria=%s pendientes=%s",
            body.vendedor_id,
            body.timestamp,
            body.lat,
            body.lng,
            body.bateria,
            body.pendientes,
        )

    try:
        hb_id = heartbeat_service.insert_heartbeat(
            vendedor_id=body.vendedor_id,
            timestamp=body.timestamp,
            lat=body.lat,
            lng=body.lng,
            bateria=body.bateria,
            conexion=body.conexion,
            pendientes=body.pendientes,
            app_version=body.app_version,
            dispositivo=body.dispositivo,
        )
    except ValueError as e:
        logger.warning("heartbeat validación: %s", e)
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        err = str(e).lower()
        if "operaciones_heartbeat" in err and ("does not exist" in err or "no existe" in err):
            logger.error("heartbeat: tabla bsale.operaciones_heartbeat no existe — ejecutar sql/bsale_operaciones_heartbeat.sql")
            raise HTTPException(
                status_code=503,
                detail="Tabla operaciones_heartbeat no configurada en el servidor",
            ) from e
        logger.exception("heartbeat insert falló: %s", e)
        raise HTTPException(status_code=500, detail="No se pudo registrar heartbeat") from e

    if logger.isEnabledFor(logging.DEBUG):
        logger.debug("heartbeat ACK id=%s vendedor=%s server_ts=%s", hb_id, body.vendedor_id, server_ts.isoformat())

    return HeartbeatAckResponse(ack=True, server_timestamp=server_ts)
