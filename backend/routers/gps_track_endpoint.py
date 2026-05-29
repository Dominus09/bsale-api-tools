"""POST gps_track — handler compartido (single + batch)."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Annotated

from fastapi import Header, HTTPException

from backend.schemas.operaciones import GpsTrackRequest, TelemetryAckResponse
from backend.services import gps_track_service
from backend.utils.operaciones_mobile_auth import verify_operaciones_mobile_auth

logger = logging.getLogger(__name__)


def _http_gps_error(e: Exception) -> HTTPException:
    err = str(e).lower()
    if "operaciones_gps_track" in err and ("does not exist" in err or "no existe" in err):
        return HTTPException(
            status_code=503,
            detail="Tabla operaciones_gps_track no configurada en el servidor",
        )
    if isinstance(e, ValueError):
        return HTTPException(status_code=400, detail=str(e))
    logger.exception("gps_track insert falló: %s", e)
    return HTTPException(status_code=500, detail="No se pudo registrar gps_track")


async def handle_gps_track(
    body: GpsTrackRequest,
    x_heartbeat_key: Annotated[str | None, Header(alias="X-Heartbeat-Key")] = None,
    authorization: Annotated[str | None, Header(alias="Authorization")] = None,
) -> TelemetryAckResponse:
    verify_operaciones_mobile_auth(x_heartbeat_key, authorization)
    server_ts = datetime.now(timezone.utc)

    try:
        if body.puntos:
            n = len(body.puntos)
            logger.info(
                "[GPS_TRACK] mode=batch vendedor=%s puntos=%s session_id=%s point_ids=%s",
                body.vendedor_id,
                n,
                body.session_id,
                len(body.point_ids or []),
            )
            insertados = gps_track_service.insert_gps_track_batch(
                vendedor_id=body.vendedor_id,
                default_timestamp=body.timestamp,
                puntos=body.puntos,
                battery=body.battery,
                app_version=body.app_version,
            )
            logger.info(
                "[GPS_TRACK] mode=batch insertados=%s vendedor=%s → ACK",
                insertados,
                body.vendedor_id,
            )
            return TelemetryAckResponse(
                ack=True,
                server_timestamp=server_ts,
                insertados=insertados,
            )

        logger.info(
            "[GPS_TRACK] mode=single vendedor=%s ts=%s lat=%s lng=%s",
            body.vendedor_id,
            body.timestamp,
            body.lat,
            body.lng_efectivo(),
        )
        track_id = gps_track_service.insert_gps_track(
            vendedor_id=body.vendedor_id,
            timestamp=body.timestamp,
            lat=float(body.lat),  # type: ignore[arg-type]
            lng=body.lng_efectivo(),
            accuracy=body.accuracy,
            speed=body.speed,
            battery=body.battery,
            app_version=body.app_version,
        )
        logger.info(
            "[GPS_TRACK] mode=single insert OK id=%s vendedor=%s → ACK",
            track_id,
            body.vendedor_id,
        )
        return TelemetryAckResponse(
            ack=True,
            server_timestamp=server_ts,
            insertados=1,
        )
    except HTTPException:
        raise
    except Exception as e:
        raise _http_gps_error(e) from e
