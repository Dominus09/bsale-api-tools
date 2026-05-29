"""Esquemas API panel operaciones (monitoreo vendedores / rutas)."""

from __future__ import annotations

from datetime import date, datetime
from typing import Literal

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, field_validator, model_validator

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

    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    vendedor_id: str = Field(
        ...,
        min_length=1,
        max_length=64,
        validation_alias=AliasChoices("vendedor_id", "vendedorId"),
    )
    timestamp: datetime
    lat: float | None = None
    lng: float | None = None
    bateria: int | None = Field(None, ge=0, le=100)
    conexion: str | None = Field(None, max_length=32)
    pendientes: int | None = Field(None, ge=0)
    app_version: str | None = Field(
        None,
        max_length=64,
        validation_alias=AliasChoices("app_version", "appVersion"),
    )
    dispositivo: str | None = Field(None, max_length=128)

    @field_validator("vendedor_id", mode="before")
    @classmethod
    def _strip_vendedor(cls, v: object) -> object:
        if isinstance(v, str):
            return v.strip()
        return v


class TelemetryAckResponse(BaseModel):
    """ACK telemetría móvil (heartbeat, gps_track)."""

    model_config = ConfigDict(populate_by_name=True)

    ack: bool = True
    server_timestamp: datetime | None = None
    insertados: int | None = None


# Alias retrocompatible
HeartbeatAckResponse = TelemetryAckResponse


class GpsTrackPunto(BaseModel):
    """Un punto dentro de ``puntos[]`` (formato batch app móvil)."""

    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    lat: float
    lon: float | None = Field(default=None, validation_alias=AliasChoices("lon", "lng"))
    lng: float | None = Field(default=None, validation_alias=AliasChoices("lng", "lon"))
    accuracy_m: float | None = Field(
        None,
        validation_alias=AliasChoices("accuracy_m", "accuracy"),
    )
    speed: float | None = None
    timestamp: datetime | None = None

    def lng_efectivo(self) -> float:
        if self.lon is not None:
            return float(self.lon)
        if self.lng is not None:
            return float(self.lng)
        raise ValueError("Cada punto requiere lon o lng")


class GpsTrackRequest(BaseModel):
    """
  POST /operaciones/gps_track — formato legacy (lat/lng) o batch (puntos[]).
    """

    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    vendedor_id: str = Field(
        ...,
        min_length=1,
        max_length=64,
        validation_alias=AliasChoices("vendedor_id", "vendedorId"),
    )
    timestamp: datetime
    session_id: str | None = Field(
        None,
        max_length=128,
        validation_alias=AliasChoices("session_id", "sessionId"),
    )
    point_ids: list[str] = Field(default_factory=list)
    puntos: list[GpsTrackPunto] = Field(default_factory=list)
    lat: float | None = None
    lon: float | None = Field(default=None, validation_alias=AliasChoices("lon", "lng"))
    lng: float | None = Field(default=None, validation_alias=AliasChoices("lng", "lon"))
    accuracy: float | None = Field(
        None,
        validation_alias=AliasChoices("accuracy", "accuracy_m"),
    )
    speed: float | None = None
    battery: int | None = Field(
        None,
        ge=0,
        le=100,
        validation_alias=AliasChoices("battery", "bateria", "bateria_pct"),
    )
    app_version: str | None = Field(
        None,
        max_length=64,
        validation_alias=AliasChoices("app_version", "appVersion"),
    )

    @field_validator("vendedor_id", mode="before")
    @classmethod
    def _strip_vendedor_gps(cls, v: object) -> object:
        if isinstance(v, str):
            return v.strip()
        return v

    def lng_efectivo(self) -> float:
        if self.lon is not None:
            return float(self.lon)
        if self.lng is not None:
            return float(self.lng)
        raise ValueError("lat/lng requeridos en formato single")

    @model_validator(mode="after")
    def _formato_single_o_batch(self) -> "GpsTrackRequest":
        if self.puntos:
            return self
        if self.lat is not None and (self.lng is not None or self.lon is not None):
            return self
        raise ValueError("Se requiere lat/lng (formato single) o puntos[] (formato batch)")


GeorefEstado = Literal["pendiente", "capturada", "aplicada"]


class ClienteGeorefRow(BaseModel):
    """Fila de georef operacional (view o rutero)."""

    cliente_codigo: str
    cliente_nombre: str
    vendedor_codigo: str
    ruta_id: int = Field(description="PK bsale.rutero.id")
    direccion: str | None = None
    comuna: str | None = None
    lat: float | None = None
    lon: float | None = None
    georef_estado: GeorefEstado | str = "pendiente"
    georef_actualizada_at: datetime | None = None
    georef_actualizada_por: str | None = None


class GeorefResumen(BaseModel):
    total: int = 0
    pendientes: int = 0
    capturados: int = 0
    aplicados: int = 0


class GeorefPendientesDebug(BaseModel):
    """Diagnóstico temporal (?debug=true)."""

    total_sql: int = Field(
        description="Filas en bsale.v_clientes_sin_georef con vendedor_codigo (vista legacy)"
    )
    total_post_filtro: int = Field(
        description="Filas devueltas tras filtro rutero (georef + vendedor ruta + dedupe)"
    )
    duplicados: int = Field(
        description="Filas rutero extra antes de DISTINCT ON (bsale_id)"
    )


class GeorefPendientesResponse(BaseModel):
    total: int
    items: list[ClienteGeorefRow] = Field(default_factory=list)
    resumen: GeorefResumen = Field(default_factory=GeorefResumen)
    debug: GeorefPendientesDebug | None = None


class GeorefActualizarRequest(BaseModel):
    """Captura GPS desde app móvil."""

    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    ruta_id: int = Field(..., ge=1, validation_alias=AliasChoices("ruta_id", "rutero_id"))
    lat: float
    lon: float = Field(validation_alias=AliasChoices("lon", "lng"))
    vendedor_id: str = Field(
        ...,
        min_length=1,
        max_length=50,
        validation_alias=AliasChoices("vendedor_id", "vendedorId", "vendedor"),
    )
    actualizada_por: str | None = Field(
        None,
        max_length=50,
        validation_alias=AliasChoices("actualizada_por", "actualizadaPor"),
    )

    @field_validator("vendedor_id", mode="before")
    @classmethod
    def _strip_v(cls, v: object) -> object:
        if isinstance(v, str):
            return v.strip()
        return v


class GeorefActualizarResponse(BaseModel):
    ack: bool = True
    ruta_id: int
    cliente_codigo: str
    lat: float
    lon: float
    georef_estado: str
    georef_actualizada_at: datetime | None = None
    georef_actualizada_por: str | None = None


class GeorefEstadoPatchRequest(BaseModel):
    """ERP: marcar aplicada o volver a pendiente."""

    ruta_id: int = Field(..., ge=1, validation_alias=AliasChoices("ruta_id", "rutero_id"))
    georef_estado: Literal["pendiente", "aplicada"]
    actualizada_por: str | None = Field(None, max_length=50)


class GeorefEstadoPatchResponse(BaseModel):
    ok: bool = True
    ruta_id: int
    cliente_codigo: str
    lat: float | None = None
    lon: float | None = None
    georef_estado: str
    georef_actualizada_at: datetime | None = None
    georef_actualizada_por: str | None = None
