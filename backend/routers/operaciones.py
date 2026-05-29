"""
Panel operacional Quillotana: monitoreo vendedores, rutas e incidencias.

Montaje: ``/operaciones``
- GET*: JWT staff
- POST ``/heartbeat``, ``/gps_track``: app móvil (sin JWT staff)
"""

from __future__ import annotations

import logging
import time
from datetime import date
from typing import Annotated

import csv
import io

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from fastapi.responses import FileResponse, StreamingResponse

from backend.routers.gps_track_endpoint import handle_gps_track
from backend.routers.heartbeat_endpoint import handle_heartbeat
from backend.schemas.operaciones import (
    GeorefActualizarRequest,
    GeorefActualizarResponse,
    GeorefEstadoPatchRequest,
    GeorefEstadoPatchResponse,
    GeorefPendientesDebug,
    GeorefHistorialResponse,
    GeorefPendientesResponse,
    GeorefResumen,
    MapaGlobalResponse,
    VendedorRecorridoResponse,
    GpsTrackRequest,
    HeartbeatAckResponse,
    HeartbeatRequest,
    TelemetryAckResponse,
    IncidenciasListResponse,
    OperacionesDashboardResponse,
    OperacionesMetricasResponse,
    RutaMapaResponse,
    VendedorDetalleResponse,
    VendedoresListResponse,
)
from backend.services import operaciones_recorrido_service as recorrido_service
from backend.services import operaciones_service
from backend.services import rutero_georef_service as georef_service
from backend.services.rutero_georef_historial_service import list_historial
from backend.services.visita_foto_service import path_for_key, path_for_visita_id
from backend.utils.auth_staff import decode_staff_token, require_staff_user
from backend.utils.operaciones_mobile_auth import verify_operaciones_mobile_auth

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/operaciones", tags=["Operaciones Quillotana"])


def _parse_fecha(fecha: date | None) -> date:
    return fecha or date.today()


def _auth_staff_o_movil(
    authorization: Annotated[str | None, Header(alias="Authorization")] = None,
    x_heartbeat_key: Annotated[str | None, Header(alias="X-Heartbeat-Key")] = None,
) -> str:
    """
    Staff JWT → ``staff``.
    Telemetría móvil / sin clave → ``mobile``.
    """
    if authorization and authorization.lower().startswith("bearer "):
        try:
            decode_staff_token(authorization)
            return "staff"
        except HTTPException:
            pass
    verify_operaciones_mobile_auth(x_heartbeat_key, authorization)
    return "mobile"


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
) -> TelemetryAckResponse:
    return await handle_heartbeat(body, x_heartbeat_key, authorization)


@router.post(
    "/gps_track",
    response_model=TelemetryAckResponse,
    summary="Punto GPS tracking (cola móvil)",
    operation_id="operaciones_router_gps_track",
    responses={
        200: {"description": "ACK"},
        400: {"description": "Payload inválido"},
        401: {"description": "Auth telemetría"},
        503: {"description": "Tabla no migrada"},
    },
)
async def post_operaciones_gps_track(
    body: GpsTrackRequest,
    x_heartbeat_key: Annotated[str | None, Header(alias="X-Heartbeat-Key")] = None,
    authorization: Annotated[str | None, Header(alias="Authorization")] = None,
) -> TelemetryAckResponse:
    logger.info("[GPS-Track] operaciones.router → vendedor=%s", body.vendedor_id)
    return await handle_gps_track(body, x_heartbeat_key, authorization)


@router.post(
    "/gps_track/",
    response_model=TelemetryAckResponse,
    summary="GPS track (con slash final)",
    include_in_schema=False,
)
async def post_operaciones_gps_track_slash(
    body: GpsTrackRequest,
    x_heartbeat_key: Annotated[str | None, Header(alias="X-Heartbeat-Key")] = None,
    authorization: Annotated[str | None, Header(alias="Authorization")] = None,
) -> TelemetryAckResponse:
    return await handle_gps_track(body, x_heartbeat_key, authorization)


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
    "/vendedor/{codigo}/recorrido",
    response_model=VendedorRecorridoResponse,
    summary="Recorrido cronológico del día (visitas, incidencias, GPS)",
)
def get_operaciones_vendedor_recorrido(
    codigo: str,
    fecha: Annotated[date | None, Query()] = None,
    _user: dict = Depends(require_staff_user),
) -> VendedorRecorridoResponse:
    try:
        raw = recorrido_service.get_vendedor_recorrido(codigo.strip(), _parse_fecha(fecha))
    except Exception as e:
        logger.exception("operaciones recorrido %s: %s", codigo, e)
        raise HTTPException(status_code=500, detail="Error al cargar recorrido") from e
    if raw is None:
        raise HTTPException(status_code=404, detail="Vendedor no encontrado")
    return VendedorRecorridoResponse.model_validate(raw)


@router.get(
    "/mapa-global",
    response_model=MapaGlobalResponse,
    summary="Mapa operacional: todos los vendedores activos con GPS",
)
def get_operaciones_mapa_global(
    fecha: Annotated[date | None, Query()] = None,
    _user: dict = Depends(require_staff_user),
) -> MapaGlobalResponse:
    try:
        return operaciones_service.get_mapa_global(_parse_fecha(fecha))
    except Exception as e:
        logger.exception("operaciones mapa-global: %s", e)
        raise HTTPException(status_code=500, detail="Error al cargar mapa global") from e


@router.get(
    "/georef-historial/{ruta_id}",
    response_model=GeorefHistorialResponse,
    summary="Historial de cambios georef de un cliente rutero",
)
def get_georef_historial(
    ruta_id: int,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    _user: dict = Depends(require_staff_user),
) -> GeorefHistorialResponse:
    try:
        items = list_historial(ruta_id, limit=limit)
        return GeorefHistorialResponse(ruta_id=ruta_id, items=items)
    except Exception as e:
        logger.exception("georef-historial ruta_id=%s: %s", ruta_id, e)
        raise HTTPException(status_code=500, detail="Error al cargar historial georef") from e


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


@router.get(
    "/georef-pendientes",
    response_model=GeorefPendientesResponse,
    summary="Clientes sin georef (view rutero; app móvil o ERP)",
)
def get_georef_pendientes(
    vendedor: Annotated[
        str | None,
        Query(description="Código vendedor (obligatorio para app móvil)"),
    ] = None,
    vista: Annotated[
        str | None,
        Query(
            description="erp = panel ERP (usa solo_pendientes); omitir = listado app móvil",
        ),
    ] = None,
    solo_pendientes: Annotated[
        bool,
        Query(description="ERP: solo clientes sin georef efectiva"),
    ] = False,
    estado: Annotated[
        str | None,
        Query(description="ERP: pendiente | capturada | rechazada | aplicada"),
    ] = None,
    comuna: Annotated[str | None, Query(description="ERP: filtrar por comuna")] = None,
    fecha: Annotated[
        date | None,
        Query(
            description="App: opcional, filtra por día operativo (misma regla que /vendedor/ruta)",
        ),
    ] = None,
    debug: Annotated[
        bool,
        Query(description="Diagnóstico: total_sql (vista), total_post_filtro, duplicados"),
    ] = False,
    auth_mode: str = Depends(_auth_staff_o_movil),
) -> GeorefPendientesResponse:
    v = (vendedor or "").strip()
    if auth_mode == "mobile" and not v:
        raise HTTPException(
            status_code=400,
            detail="Parámetro vendedor es obligatorio para la app móvil.",
        )
    try:
        debug_out: GeorefPendientesDebug | None = None
        if (vista or "").strip().lower() == "erp" and auth_mode == "staff":
            resumen_dict = georef_service.get_georef_resumen(vendedor_codigo=v or None)
            items = georef_service.list_georef_erp(
                vendedor_codigo=v or None,
                solo_pendientes=solo_pendientes,
                estado=estado,
                comuna=comuna,
            )
            resumen = GeorefResumen(**resumen_dict)
        else:
            fecha_movil = fecha if fecha is not None else (
                date.today() if auth_mode == "mobile" else None
            )
            items, dbg = georef_service.list_georef_pendientes_movil(
                v,
                fecha=fecha_movil,
                debug=debug,
            )
            if dbg is not None:
                debug_out = GeorefPendientesDebug(**dbg)
            resumen = GeorefResumen(
                total=len(items),
                pendientes=len(items),
                capturados=0,
                aplicados=0,
            )
        if debug and debug_out is None:
            debug_out = GeorefPendientesDebug(
                total_sql=0,
                total_post_filtro=len(items),
                duplicados=0,
            )
        return GeorefPendientesResponse(
            total=len(items),
            items=items,
            resumen=resumen,
            debug=debug_out,
        )
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e)) from e
    except Exception as e:
        logger.exception("georef-pendientes vendedor=%s", v)
        raise HTTPException(status_code=500, detail="Error al listar georef pendientes") from e


@router.get(
    "/georef-export",
    summary="Exportar CSV de georef (pendientes por defecto)",
)
def get_georef_export(
    vendedor: Annotated[str | None, Query()] = None,
    solo_pendientes: Annotated[bool, Query()] = True,
    _user: dict = Depends(require_staff_user),
) -> StreamingResponse:
    v = (vendedor or "").strip() or None
    try:
        rows = georef_service.list_georef_erp(
            vendedor_codigo=v,
            solo_pendientes=solo_pendientes,
        )
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e)) from e
    except Exception as e:
        logger.exception("georef-export vendedor=%s", v)
        raise HTTPException(status_code=500, detail="Error al exportar georef") from e

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(
        ["vendedor", "cliente", "cliente_codigo", "direccion", "comuna", "estado", "ruta_id"]
    )
    for r in rows:
        writer.writerow(
            [
                r.get("vendedor_codigo") or "",
                r.get("cliente_nombre") or "",
                r.get("cliente_codigo") or "",
                r.get("direccion") or "",
                r.get("comuna") or "",
                r.get("georef_estado") or "",
                r.get("ruta_id") or "",
            ]
        )
    buf.seek(0)
    suffix = "pendientes" if solo_pendientes else "georef"
    filename = f"georef_{suffix}.csv"
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.post(
    "/georef-actualizar",
    response_model=GeorefActualizarResponse,
    summary="Captura georef en rutero (app móvil)",
)
def post_georef_actualizar(
    body: GeorefActualizarRequest,
    x_heartbeat_key: Annotated[str | None, Header(alias="X-Heartbeat-Key")] = None,
    authorization: Annotated[str | None, Header(alias="Authorization")] = None,
) -> GeorefActualizarResponse:
    verify_operaciones_mobile_auth(x_heartbeat_key, authorization)
    por = (body.actualizada_por or body.vendedor_id).strip()
    try:
        out = georef_service.capturar_georef(
            rutero_id=body.ruta_id,
            lat=body.lat,
            lon=body.lon,
            actualizada_por=por,
            vendedor_esperado=body.vendedor_id,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e)) from e
    except Exception as e:
        logger.exception("georef-actualizar ruta_id=%s", body.ruta_id)
        raise HTTPException(status_code=500, detail="Error al guardar georef") from e

    if out is None:
        raise HTTPException(status_code=404, detail="Cliente rutero no encontrado")
    return GeorefActualizarResponse(**out)


@router.patch(
    "/georef-estado",
    response_model=GeorefEstadoPatchResponse,
    summary="ERP: marcar georef aplicada o volver a pendiente",
)
def patch_georef_estado(
    body: GeorefEstadoPatchRequest,
    user: dict = Depends(require_staff_user),
) -> GeorefEstadoPatchResponse:
    por = (body.actualizada_por or user.get("email") or user.get("username") or "erp")
    if isinstance(por, str):
        por = por.strip()[:50]
    else:
        por = "erp"
    try:
        out = georef_service.actualizar_estado_georef(
            rutero_id=body.ruta_id,
            georef_estado=body.georef_estado,
            actualizada_por=por,
            motivo_rechazo=body.motivo_rechazo,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e)) from e
    except Exception as e:
        logger.exception("georef-estado ruta_id=%s", body.ruta_id)
        raise HTTPException(status_code=500, detail="Error al actualizar estado georef") from e

    if out is None:
        raise HTTPException(status_code=404, detail="Cliente rutero no encontrado")
    return GeorefEstadoPatchResponse(**out)
