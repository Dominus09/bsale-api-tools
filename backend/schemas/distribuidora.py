"""
Modelos Pydantic para el módulo app_distribuidora (rutas del día y visitas).
Solo validación / serialización; la persistencia es SQL con psycopg2.
"""

from datetime import date, datetime
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

EstadoVisita = Literal["pendiente", "visitado", "incidencia"]
TipoIncidencia = Literal[
    "local cerrado",
    "sin stock",
    "no compra",
    "fuera de ruta",
    "otros",
    "atencion telefonica",
]
SyncStatus = Literal["synced", "pending_sync"]

TipoUsuarioApp = Literal["vendedor", "chofer", "bodega"]


class LoginRequest(BaseModel):
    """Credenciales de la app móvil (vendedores_app, no ERP)."""

    model_config = ConfigDict(str_strip_whitespace=True)

    codigo: str = Field(..., min_length=1, max_length=50, description="Ej. vendedor_1")
    password: str = Field(..., min_length=1, description="Contraseña en texto plano solo en tránsito HTTPS")


class LoginSuccessResponse(BaseModel):
    """Respuesta exitosa del login (sin token JWT hasta que lo definan)."""

    success: bool = True
    vendedor: str = Field(description="Mismo valor que codigo en BD")
    nombre: str
    tipo_usuario: TipoUsuarioApp = Field(
        description="Rol en la app: vendedor, chofer o bodega (desde bsale.vendedores_app.tipo_usuario).",
    )


class VisitaUpdate(BaseModel):
    """
    Actualización de una visita ya creada en servidor (p. ej. desde GET /vendedor/ruta + rutero).
    La app no inserta filas: solo envía el ``id`` de la visita y los campos a persistir.
    """

    model_config = ConfigDict(str_strip_whitespace=True)

    id: int = Field(..., ge=1, description="PK de bsale.visitas (lo devuelve GET /vendedor/ruta).")

    estado: EstadoVisita = "pendiente"
    tipo_incidencia: TipoIncidencia | None = None
    con_compra: bool | None = Field(
        default=None,
        description="true/false si hubo compra. Si se omite el campo, no se altera con_compra en BD.",
    )
    observacion: str | None = None
    foto_url: str | None = None

    lat_visita: Decimal | float | None = None
    lon_visita: Decimal | float | None = None

    fecha_hora_visita: datetime | None = None
    sync_status: SyncStatus | None = None


class SyncRequest(BaseModel):
    """Lote de actualizaciones de visitas existentes (solo UPDATE por ``id``)."""

    visitas: list[VisitaUpdate] = Field(default_factory=list)


class VisitaResponse(BaseModel):
    """Fila de bsale.visitas tal como se expone en la API."""

    model_config = ConfigDict(from_attributes=True)

    id: int
    ruta_id: int
    cliente_id: str
    nombre_fantasia: str | None = None
    direccion: str | None = None
    comuna: str | None = None
    rut_clean: str | None = None
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
    """Resumen del procesamiento de un lote de sincronización (solo actualizaciones)."""

    sincronizados: int
    errores: int


class VisitaAltaResponse(BaseModel):
    """Respuesta simple del POST unitario de actualización de visita."""

    mensaje: str
    ok: bool = True
