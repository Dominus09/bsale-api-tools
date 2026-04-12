"""
Modelos Pydantic para el módulo app_distribuidora (rutas del día y visitas).
Solo validación / serialización; la persistencia es SQL con psycopg2.
"""

from datetime import date, datetime
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

EstadoVisita = Literal["pendiente", "visitado", "incidencia"]
TipoIncidencia = Literal["local cerrado", "sin stock", "no compra", "fuera de ruta", "otros"]
SyncStatus = Literal["synced", "pending_sync"]


class VisitaCreate(BaseModel):
    """Cuerpo mínimo y campos opcionales para registrar una visita (app móvil / sync offline)."""

    model_config = ConfigDict(str_strip_whitespace=True)

    local_action_id: str = Field(..., max_length=128, description="Id único por acción en el dispositivo.")
    ruta_id: int = Field(..., ge=1)
    cliente_id: str = Field(..., max_length=128)
    orden_ruta: int = Field(..., ge=1)

    estado: EstadoVisita = "pendiente"
    tipo_incidencia: TipoIncidencia | None = None
    con_compra: bool = False
    observacion: str | None = None
    foto_url: str | None = None

    lat_cliente: Decimal | float | None = None
    lon_cliente: Decimal | float | None = None
    lat_visita: Decimal | float | None = None
    lon_visita: Decimal | float | None = None

    fecha_hora_visita: datetime | None = None
    sync_status: SyncStatus = "pending_sync"


class SyncRequest(BaseModel):
    """Lote de visitas enviadas al sincronizar tras trabajo offline."""

    visitas: list[VisitaCreate] = Field(default_factory=list)


class VisitaResponse(BaseModel):
    """Fila de bsale.visitas tal como se expone en la API."""

    model_config = ConfigDict(from_attributes=True)

    id: int
    ruta_id: int
    cliente_id: str
    orden_ruta: int
    estado: str
    tipo_incidencia: str | None
    con_compra: bool
    observacion: str | None
    foto_url: str | None
    lat_cliente: Decimal | None
    lon_cliente: Decimal | None
    lat_visita: Decimal | None
    lon_visita: Decimal | None
    distancia_metros: Decimal | None
    validacion_estado: str
    fecha_hora_visita: datetime | None
    sync_status: str
    local_action_id: str
    created_at: datetime
    updated_at: datetime


class RutaResponse(BaseModel):
    """Ruta del día con visitas anidadas (orden por orden_ruta en la consulta SQL)."""

    id: int
    fecha: date
    vendedor: str
    estado: str
    hora_inicio: datetime | None
    hora_fin: datetime | None
    total_clientes: int
    clientes_visitados: int
    clientes_pendientes: int
    porcentaje_cumplimiento: Decimal | None
    created_at: datetime
    updated_at: datetime
    visitas: list[VisitaResponse] = Field(default_factory=list)


class SyncResponse(BaseModel):
    """Resumen del procesamiento de un lote de sincronización."""

    sincronizados: int
    omitidos: int
    errores: int


class VisitaAltaResponse(BaseModel):
    """Respuesta del POST unitario de visita."""

    mensaje: str
    insertado: bool
    data: VisitaResponse | None = None
