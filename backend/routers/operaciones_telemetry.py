"""
Telemetría móvil — router dedicado (heartbeat + gps_track).

Montaje en ``main.py`` con ``prefix="/operaciones"`` para garantizar:
- POST /operaciones/heartbeat
- POST /operaciones/gps_track
- POST /operaciones/gps-track  (alias guión, por si la app usa kebab-case)
"""

from __future__ import annotations

import logging
from typing import Annotated

from fastapi import APIRouter, Header

from backend.routers.gps_track_endpoint import handle_gps_track
from backend.routers.heartbeat_endpoint import handle_heartbeat
from backend.schemas.distribuidora import SyncRequest, SyncResponse
from backend.schemas.operaciones import (
    GpsTrackRequest,
    HeartbeatRequest,
    TelemetryAckResponse,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/operaciones", tags=["Operaciones Telemetría Móvil"])


@router.post(
    "/heartbeat",
    response_model=TelemetryAckResponse,
    summary="Heartbeat vendedor (telemetría)",
    operation_id="operaciones_telemetry_heartbeat",
)
async def telemetry_heartbeat(
    body: HeartbeatRequest,
    x_heartbeat_key: Annotated[str | None, Header(alias="X-Heartbeat-Key")] = None,
    authorization: Annotated[str | None, Header(alias="Authorization")] = None,
) -> TelemetryAckResponse:
    logger.debug("telemetry router → heartbeat vendedor=%s", body.vendedor_id)
    return await handle_heartbeat(body, x_heartbeat_key, authorization)


@router.post(
    "/gps_track",
    response_model=TelemetryAckResponse,
    summary="GPS track — un punto (cola móvil)",
    operation_id="operaciones_telemetry_gps_track",
)
async def telemetry_gps_track(
    body: GpsTrackRequest,
    x_heartbeat_key: Annotated[str | None, Header(alias="X-Heartbeat-Key")] = None,
    authorization: Annotated[str | None, Header(alias="Authorization")] = None,
) -> TelemetryAckResponse:
    logger.info(
        "telemetry router → gps_track REQUEST vendedor=%s ts=%s",
        body.vendedor_id,
        body.timestamp,
    )
    return await handle_gps_track(body, x_heartbeat_key, authorization)


@router.post(
    "/gps-track",
    response_model=TelemetryAckResponse,
    summary="GPS track (alias con guión)",
    operation_id="operaciones_telemetry_gps_track_kebab",
    include_in_schema=True,
)
async def telemetry_gps_track_kebab(
    body: GpsTrackRequest,
    x_heartbeat_key: Annotated[str | None, Header(alias="X-Heartbeat-Key")] = None,
    authorization: Annotated[str | None, Header(alias="Authorization")] = None,
) -> TelemetryAckResponse:
    return await handle_gps_track(body, x_heartbeat_key, authorization)


@router.post(
    "/visita_sync",
    response_model=SyncResponse,
    summary="Sync visitas (alias cola móvil bajo /operaciones)",
    operation_id="operaciones_telemetry_visita_sync",
)
@router.post(
    "/visita-sync",
    response_model=SyncResponse,
    summary="Sync visitas (alias kebab)",
    include_in_schema=True,
)
def telemetry_visita_sync(body: SyncRequest) -> SyncResponse:
    from backend.routers.app_distribuidora import post_visitas_sync

    return post_visitas_sync(body)


@router.get(
    "/telemetry/health",
    summary="Comprobar que telemetría está montada",
    operation_id="operaciones_telemetry_health",
)
def telemetry_health() -> dict:
    return {
        "ok": True,
        "routes": [
            "POST /operaciones/heartbeat",
            "POST /operaciones/gps_track",
            "POST /operaciones/gps-track",
            "POST /operaciones/visita_sync",
        ],
    }
