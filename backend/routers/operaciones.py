"""
Panel operacional Quillotana: monitoreo vendedores, rutas e incidencias.

Montaje: ``/operaciones``
- GET*: JWT staff
- POST ``/heartbeat``: app móvil (sin JWT staff; ver heartbeat_endpoint)
"""

from __future__ import annotations

import logging
import time
from datetime import date
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from fastapi.responses import FileResponse

from backend.routers.heartbeat_endpoint import handle_heartbeat
from backend.schemas.operaciones import (
    HeartbeatAckResponse,
    HeartbeatRequest,
    IncidenciasListResponse,
    OperacionesDashboardResponse,
    OperacionesMetricasResponse,
    RutaMapaResponse,
    VendedorDetalleResponse,
    VendedoresListResponse,
)
from backend.services import operaciones_service
from backend.services.visita_foto_service import path_for_key, path_for_visita_id
from backend.utils.auth_staff import require_staff_user

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/operaciones", tags=["Operaciones Quillotana"])


def _parse_fecha(fecha: date | None) -> date:
    return fecha or date.today()


@router.post(
    "/heartbeat",
    response_model=HeartbeatAckResponse,
    summary="Telemetría app móvil (GPS, batería, pendientes)",
    responses={
        200: {"description": "ACK"},
        400: {"description": "Payload inválido"},
        401: {"description": "Auth heartbeat"},
        503: {"description": "Tabla no migrada"},
    },
)
async def post_operaciones_heartbeat(
    body: HeartbeatRequest,
    x_heartbeat_key: Annotated[str | None, Header(alias="X-Heartbeat-Key")] = None,
    authorization: Annotated[str | None, Header(alias="Authorization")] = None,
) -> HeartbeatAckResponse:
    return await handle_heartbeat(body, x_heartbeat_key, authorization)


@router.get(
    "/dashboard",
    response_model=OperacionesDashboardResponse,
    summary="KPIs del día y resumen por vendedor",
)
def get_operaciones_dashboard(
    fecha: Annotated[date | None, Query(description="Día operativo (YYYY-MM-DD)")] = None,
    _user: dict = Depends(require_staff_user),
) -> OperacionesDashboardResponse:
    t0 = time.perf_counter()
    try:
        return operaciones_service.get_dashboard(_parse_fecha(fecha))
    except Exception as e:
        logger.exception("operaciones dashboard: %s", e)
        raise HTTPException(status_code=500, detail="Error al cargar dashboard operacional") from e
    finally:
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("operaciones /dashboard %.0f ms", (time.perf_counter() - t0) * 1000)


@router.get(
    "/vendedores",
    response_model=VendedoresListResponse,
    summary="Listado operacional de vendedores",
)
def get_operaciones_vendedores(
    fecha: Annotated[date | None, Query()] = None,
    _user: dict = Depends(require_staff_user),
) -> VendedoresListResponse:
    try:
        return operaciones_service.get_vendedores(_parse_fecha(fecha))
    except Exception as e:
        logger.exception("operaciones vendedores: %s", e)
        raise HTTPException(status_code=500, detail="Error al listar vendedores") from e


@router.get(
    "/vendedor/{codigo}",
    response_model=VendedorDetalleResponse,
    summary="Detalle vendedor: timeline, incidencias, métricas del día",
)
def get_operaciones_vendedor(
    codigo: str,
    fecha: Annotated[date | None, Query()] = None,
    _user: dict = Depends(require_staff_user),
) -> VendedorDetalleResponse:
    try:
        out = operaciones_service.get_vendedor_detalle(codigo.strip(), _parse_fecha(fecha))
    except Exception as e:
        logger.exception("operaciones vendedor %s: %s", codigo, e)
        raise HTTPException(status_code=500, detail="Error al cargar vendedor") from e
    if out is None:
        raise HTTPException(status_code=404, detail="Vendedor no encontrado")
    return out


@router.get(
    "/ruta/{ruta_id}",
    response_model=RutaMapaResponse,
    summary="Marcadores de ruta para mapa operacional",
)
def get_operaciones_ruta(
    ruta_id: int,
    _user: dict = Depends(require_staff_user),
) -> RutaMapaResponse:
    try:
        out = operaciones_service.get_ruta_mapa(ruta_id)
    except Exception as e:
        logger.exception("operaciones ruta %s: %s", ruta_id, e)
        raise HTTPException(status_code=500, detail="Error al cargar ruta") from e
    if out is None:
        raise HTTPException(status_code=404, detail="Ruta no encontrada")
    return out


@router.get(
    "/incidencias",
    response_model=IncidenciasListResponse,
    summary="Incidencias del día (filtrable por vendedor)",
)
def get_operaciones_incidencias(
    fecha: Annotated[date | None, Query()] = None,
    vendedor: Annotated[str | None, Query(description="Código vendedor (ej. vendedor_1)")] = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 200,
    _user: dict = Depends(require_staff_user),
) -> IncidenciasListResponse:
    try:
        return operaciones_service.get_incidencias(_parse_fecha(fecha), vendedor=vendedor, limit=limit)
    except Exception as e:
        logger.exception("operaciones incidencias: %s", e)
        raise HTTPException(status_code=500, detail="Error al listar incidencias") from e


@router.get(
    "/foto/{visita_id}",
    summary="Evidencia fotográfica de una visita (JWT staff)",
    responses={404: {"description": "Sin imagen"}},
)
def get_operaciones_visita_foto(
    visita_id: int,
    _user: dict = Depends(require_staff_user),
):
    from backend.db import get_connection

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT foto_url FROM bsale.visitas WHERE id = %s", (visita_id,))
        row = cur.fetchone()
        cur.close()
    finally:
        conn.close()

    stored = str(row[0]).strip() if row and row[0] else None
    if stored and (stored.startswith("http://") or stored.startswith("https://")):
        from fastapi.responses import RedirectResponse

        return RedirectResponse(stored, status_code=302)

    path = path_for_key(stored) if stored else None
    if path is None:
        path = path_for_visita_id(visita_id)
    if path is None:
        raise HTTPException(status_code=404, detail="Sin imagen para esta visita")
    media = "image/jpeg"
    if path.suffix.lower() == ".png":
        media = "image/png"
    elif path.suffix.lower() == ".webp":
        media = "image/webp"
    return FileResponse(path, media_type=media)


@router.get(
    "/metricas",
    response_model=OperacionesMetricasResponse,
    summary="Métricas agregadas del día",
)
def get_operaciones_metricas(
    fecha: Annotated[date | None, Query()] = None,
    _user: dict = Depends(require_staff_user),
) -> OperacionesMetricasResponse:
    try:
        return operaciones_service.get_metricas(_parse_fecha(fecha))
    except Exception as e:
        logger.exception("operaciones metricas: %s", e)
        raise HTTPException(status_code=500, detail="Error al cargar métricas") from e
