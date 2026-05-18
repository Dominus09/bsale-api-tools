"""Esquemas API panel operaciones (monitoreo vendedores / rutas)."""

from __future__ import annotations

from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, Field

EstadoConexion = Literal["activo", "atrasado", "offline"]
EstadoVisitaMapa = Literal["visitado", "pendiente", "incidencia"]


class OperacionesDashboardKpis(BaseModel):
    fecha: date
    total_clientes: int = 0
    clientes_visitados: int = 0
    clientes_pendientes: int = 0
    incidencias: int = 0
    vendedores_activos: int = 0
    vendedores_total: int = 0
    porcentaje_cumplimiento: float = 0.0
    visitas_pending_sync: int = 0
    ultima_sincronizacion: datetime | None = None
    kilometros_recorridos: float = 0.0


class OperacionesDashboardResponse(BaseModel):
    kpis: OperacionesDashboardKpis
    vendedores_resumen: list["VendedorOperacionesRow"] = Field(default_factory=list)


class GpsActual(BaseModel):
    lat: float | None = None
    lon: float | None = None
    updated_at: datetime | None = None


class VendedorOperacionesRow(BaseModel):
    codigo: str
    nombre: str
    activo: bool = True
    ruta_id: int | None = None
    estado_ruta: str | None = None
    estado_conexion: EstadoConexion = "offline"
    visitas_realizadas: int = 0
    visitas_pendientes: int = 0
    incidencias: int = 0
    porcentaje_avance: float = 0.0
    ultima_sync: datetime | None = None
    pending_sync_count: int = 0
    bateria_pct: int | None = None
    gps: GpsActual | None = None
    kilometros_recorridos: float = 0.0
    usa_heartbeat: bool = False
    conexion_red: str | None = None


class VendedoresListResponse(BaseModel):
    fecha: date
    items: list[VendedorOperacionesRow]


class VisitaTimelineItem(BaseModel):
    id: int
    cliente_id: str
    nombre_fantasia: str | None = None
    direccion: str | None = None
    comuna: str | None = None
    orden_ruta: int
    estado: str
    tipo_incidencia: str | None = None
    observacion: str | None = None
    foto_url: str | None = None
    fecha_hora_visita: datetime | None = None
    lat_visita: float | None = None
    lon_visita: float | None = None
    distancia_metros: float | None = None
    sync_status: str


class VendedorDetalleResponse(BaseModel):
    codigo: str
    nombre: str
    fecha: date
    ruta_id: int | None = None
    estado_ruta: str | None = None
    hora_inicio: datetime | None = None
    hora_fin: datetime | None = None
    porcentaje_cumplimiento: float = 0.0
    kilometros_recorridos: float = 0.0
    estado_conexion: EstadoConexion = "offline"
    ultima_sync: datetime | None = None
    timeline: list[VisitaTimelineItem] = Field(default_factory=list)
    incidencias: list[VisitaTimelineItem] = Field(default_factory=list)


class MarcadorMapa(BaseModel):
    visita_id: int
    cliente_id: str
    nombre_fantasia: str | None = None
    lat: float
    lon: float
    estado: EstadoVisitaMapa
    vendedor: str
    tipo_incidencia: str | None = None


class VendedorUbicacionMapa(BaseModel):
    codigo: str
    nombre: str
    lat: float
    lon: float
    estado_conexion: EstadoConexion
    updated_at: datetime | None = None


class RutaMapaResponse(BaseModel):
    fecha: date
    ruta_id: int
    vendedor: str
    vendedor_nombre: str | None = None
    marcadores: list[MarcadorMapa] = Field(default_factory=list)
    vendedor_ubicacion: VendedorUbicacionMapa | None = None


class IncidenciaRow(BaseModel):
    id: int
    ruta_id: int
    vendedor: str
    vendedor_nombre: str | None = None
    cliente_id: str
    nombre_fantasia: str | None = None
    comuna: str | None = None
    tipo_incidencia: str | None = None
    observacion: str | None = None
    foto_url: str | None = None
    tiene_foto: bool = False
    fecha_hora_visita: datetime | None = None
    sync_status: str


class IncidenciasListResponse(BaseModel):
    fecha: date
    total: int
    items: list[IncidenciaRow]


class OperacionesMetricasResponse(BaseModel):
    fecha: date
    dashboard: OperacionesDashboardKpis
    por_vendedor: list[VendedorOperacionesRow] = Field(default_factory=list)


class HeartbeatRequest(BaseModel):
    """Telemetría desde app móvil (POST /operaciones/heartbeat)."""

    vendedor_id: str = Field(..., min_length=1, max_length=64)
    timestamp: datetime
    lat: float | None = None
    lng: float | None = None
    bateria: int | None = Field(None, ge=0, le=100)
    conexion: str | None = Field(None, max_length=32)
    pendientes: int | None = Field(None, ge=0)
    app_version: str | None = Field(None, max_length=64)
    dispositivo: str | None = Field(None, max_length=128)


class HeartbeatResponse(BaseModel):
    ok: bool = True
    id: int
    vendedor_id: str
    timestamp: datetime
