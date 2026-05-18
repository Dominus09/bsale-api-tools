"""POST gps_track — handler compartido."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Annotated

from fastapi import Header, HTTPException

from backend.schemas.operaciones import GpsTrackRequest, TelemetryAckResponse
from backend.services import gps_track_service
from backend.utils.operaciones_mobile_auth import verify_operaciones_mobile_auth

logger = logging.getLogger(__name__)


async def handle_gps_track(
    body: GpsTrackRequest,
    x_heartbeat_key: Annotated[str | None, Header(alias="X-Heartbeat-Key")] = None,
    authorization: Annotated[str | None, Header(alias="Authorization")] = None,
) -> TelemetryAckResponse:
    logger.info(
        "[GPS-Track] request recibido vendedor=%s ts=%s lat=%s lng=%s",
        body.vendedor_id,
        body.timestamp,
        body.lat,
        body.lng,
    )
    verify_operaciones_mobile_auth(x_heartbeat_key, authorization)
    server_ts = datetime.now(timezone.utc)

    logger.debug(
        "gps_track payload vendedor=%s accuracy=%s speed=%s battery=%s app=%s",
        body.vendedor_id,
        body.accuracy,
        body.speed,
        body.battery,
        body.app_version,
    )

    try:
        track_id = gps_track_service.insert_gps_track(
            vendedor_id=body.vendedor_id,
            timestamp=body.timestamp,
            lat=body.lat,
            lng=body.lng,
            accuracy=body.accuracy,
            speed=body.speed,
            battery=body.battery,
            app_version=body.app_version,
        )
    except ValueError as e:
        logger.warning("gps_track validación: %s", e)
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        err = str(e).lower()
        if "operaciones_gps_track" in err and ("does not exist" in err or "no existe" in err):
            raise HTTPException(
                status_code=503,
                detail="Tabla operaciones_gps_track no configurada en el servidor",
            ) from e
        logger.exception("gps_track insert falló: %s", e)
        raise HTTPException(status_code=500, detail="No se pudo registrar gps_track") from e

    logger.info(
        "[GPS-Track] insert OK id=%s vendedor=%s → ACK server_ts=%s",
        track_id,
        body.vendedor_id,
        server_ts.isoformat(),
    )

    return TelemetryAckResponse(ack=True, server_timestamp=server_ts)
