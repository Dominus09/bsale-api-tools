"""
API móvil / distribuidora: rutas del día y visitas generadas en servidor desde rutero.

Las visitas se crean al cargar la ruta (GET /vendedor/ruta); la app solo las actualiza (POST).

Prefijo de montaje en main: /app_distribuidora
"""

from __future__ import annotations

import logging
import time
from datetime import date
from typing import Annotated, cast

import bcrypt
import psycopg2
from fastapi import APIRouter, Header, HTTPException, Query
from backend.db import get_connection
from backend.schemas.distribuidora import (
    LoginRequest,
    LoginSuccessResponse,
    TipoUsuarioApp,
    RutaResponse,
    SyncRequest,
    SyncResponse,
    VisitaAltaResponse,
    VisitaUpdate,
)
from backend.routers.heartbeat_endpoint import handle_heartbeat
from backend.schemas.operaciones import HeartbeatAckResponse, HeartbeatRequest
from backend.services.visita_foto_service import normalize_and_persist_foto_url
from backend.utils.geo import coordenadas_visita_validas, distancia_y_estado_validacion

logger = logging.getLogger(__name__)

router = APIRouter(tags=["App Distribuidora"])

# Misma lógica que mapa / rutas web: sábado extra en `dia_extra` sin tocar `dia_atencion`.
_SQL_RUTERO_DIA_OPERATIVO_R = """(
    CASE
        WHEN LOWER(TRIM(COALESCE(r.dia_extra::text, ''))) = 'sabado' THEN 'Sabado'
        ELSE TRIM(COALESCE(r.dia_atencion::text, ''))
    END
)"""

_DIA_SEMANA_ES = (
    "lunes",
    "martes",
    "miércoles",
    "jueves",
    "viernes",
    "sábado",
    "domingo",
)


def _dia_atencion_desde_fecha(fecha: date) -> str:
    """Nombre del día en español (minúsculas), alineado a la comparación con `dia_operativo` en rutero."""
    return _DIA_SEMANA_ES[fecha.weekday()]


def _es_atencion_telefonica(tipo_incidencia: str | None) -> bool:
    return (tipo_incidencia or "").strip().lower() == "atencion telefonica"


class ValidacionVisitaError(ValueError):
    """Fallo de reglas de negocio al actualizar una visita (mensaje listo para API)."""

    def __init__(self, mensaje: str):
        super().__init__(mensaje)
        self.mensaje = mensaje


def _validacion_negocio_campos(
    estado: str,
    tipo_incidencia: str | None,
    foto_url,
    lat_visita,
    lon_visita,
) -> str | None:
    """Reglas de negocio (telefonía / GPS visitado). None = OK."""
    if _es_atencion_telefonica(tipo_incidencia):
        if foto_url is None or not str(foto_url).strip():
            return "Debe subir evidencia para atención telefónica"
        return None
    if estado == "visitado":
        if not coordenadas_visita_validas(lat_visita, lon_visita):
            return "Se requieren coordenadas GPS para registrar la visita en terreno."
    return None


def _distancia_y_validacion_por_tipo(
    tipo_incidencia: str | None,
    lat_cliente,
    lon_cliente,
    lat_visita,
    lon_visita,
) -> tuple[float | None, str]:
    """Distancia Haversine + validacion_estado; atención telefónica no usa GPS."""
    if _es_atencion_telefonica(tipo_incidencia):
        return None, "validado"
    return distancia_y_estado_validacion(
        lat_cliente,
        lon_cliente,
        lat_visita,
        lon_visita,
    )


_RUTA_DIA_SELECT_COLS = """
            SELECT
                id,
                fecha,
                vendedor,
                estado,
                hora_inicio,
                hora_fin,
                total_clientes,
                clientes_visitados,
                clientes_pendientes,
                porcentaje_cumplimiento,
                created_at,
                updated_at
            FROM bsale.rutas_dia
            WHERE fecha = %s
              AND LOWER(TRIM(COALESCE(vendedor::text, ''))) = %s
            LIMIT 1
"""

_RUTA_DIA_BY_ID_SELECT = """
            SELECT
                id,
                fecha,
                vendedor,
                estado,
                hora_inicio,
                hora_fin,
                total_clientes,
                clientes_visitados,
                clientes_pendientes,
                porcentaje_cumplimiento,
                created_at,
                updated_at
            FROM bsale.rutas_dia
            WHERE id = %s
"""

_VISITAS_POR_RUTA_SELECT = """
                SELECT
                    id,
                    ruta_id,
                    cliente_id,
                    nombre_fantasia,
                    direccion,
                    comuna,
                    rut_clean,
                    orden_ruta,
                    estado,
                    tipo_incidencia,
                    con_compra,
                    observacion,
                    foto_url,
                    lat_cliente,
                    lon_cliente,
                    lat_visita,
                    lon_visita,
                    distancia_metros,
                    validacion_estado,
                    fecha_hora_visita,
                    sync_status,
                    local_action_id,
                    created_at,
                    updated_at
                FROM bsale.visitas
                WHERE ruta_id = %s
                ORDER BY orden_ruta ASC, id ASC
"""


def _count_visitas_por_ruta(cur, ruta_id: int) -> int:
    cur.execute(
        "SELECT COUNT(*) FROM bsale.visitas WHERE ruta_id = %s",
        (ruta_id,),
    )
    row = cur.fetchone()
    if not row or row[0] is None:
        return 0
    return int(row[0])


def _fetch_visitas_por_ruta(cur, ruta_id: int) -> list[dict]:
    cur.execute(_VISITAS_POR_RUTA_SELECT, (ruta_id,))
    return _rows_to_dict_list(cur)


def _fetch_ruta_dia_row(cur, fecha: date, v: str):
    cur.execute(_RUTA_DIA_SELECT_COLS, (fecha, v))
    return cur.fetchone()


def _insert_ruta_dia_si_ausente(cur, fecha: date, v: str):
    """
    Garantiza una fila en rutas_dia; devuelve la tupla completa y la lista de columnas
    (mismo shape que el SELECT principal).

    Alta explícita para producción: estado en_progreso y contadores en cero.
    """
    row = _fetch_ruta_dia_row(cur, fecha, v)
    if row:
        cols = [c[0] for c in cur.description]
        return cols, row

    cur.execute(
        """
            INSERT INTO bsale.rutas_dia (
                fecha,
                vendedor,
                estado,
                total_clientes,
                clientes_visitados,
                clientes_pendientes,
                porcentaje_cumplimiento
            )
            VALUES (
                %s,
                %s,
                'en_progreso',
                0,
                0,
                0,
                0
            )
            RETURNING
                id,
                fecha,
                vendedor,
                estado,
                hora_inicio,
                hora_fin,
                total_clientes,
                clientes_visitados,
                clientes_pendientes,
                porcentaje_cumplimiento,
                created_at,
                updated_at
        """,
        (fecha, v),
    )
    row = cur.fetchone()
    cols = [c[0] for c in cur.description]
    if row:
        return cols, row

    # Carrera muy rara: otro proceso insertó entre el SELECT y el INSERT.
    row = _fetch_ruta_dia_row(cur, fecha, v)
    if not row:
        raise RuntimeError("no se pudo crear ni leer rutas_dia")
    cols = [c[0] for c in cur.description]
    return cols, row


def _poblar_visitas_desde_rutero(cur, ruta_id: int, fecha: date, v: str) -> int:
    """
    Si no hay visitas para la ruta, las crea desde bsale.rutero.

    - Filtro vendedor: ``company`` o ``vendedor`` (minúsculas) coincide con el código de ruta.
    - Día: ``dia_operativo`` (``dia_extra`` sábado → Sabado; si no ``dia_atencion``), comparado sin tildes.
    - Incluye clientes de atención telefónica en rutero (la evidencia se exige al cerrar POST /visitas).
    - Snapshot: nombre_fantasia, dirección, comuna, rut_clean, lat/lon desde rutero.
    """
    cur.execute(
        "SELECT id FROM bsale.rutas_dia WHERE id = %s FOR UPDATE",
        (ruta_id,),
    )
    if cur.fetchone() is None:
        return 0

    cur.execute(
        "SELECT COUNT(*) FROM bsale.visitas WHERE ruta_id = %s",
        (ruta_id,),
    )
    (n_visitas,) = cur.fetchone()
    if n_visitas and int(n_visitas) > 0:
        return 0

    dia = _dia_atencion_desde_fecha(fecha)
    cur.execute(
        """
            SELECT
                r.bsale_id,
                r.nombre_fantasia,
                r.address,
                r.municipality,
                r.rut_clean,
                r.lat,
                r.lon
            FROM bsale.rutero r
            WHERE r.company_id = 3
              AND r.activo = TRUE
              AND (
                    LOWER(TRIM(COALESCE(r.company::text, ''))) = %s
                 OR LOWER(TRIM(COALESCE(r.vendedor::text, ''))) = %s
              )
              AND translate(
                    lower(trim("""
        + _SQL_RUTERO_DIA_OPERATIVO_R
        + """
                    )),
                    'áéíóúü',
                    'aeiouu'
                  ) = translate(lower(trim(%s)), 'áéíóúü', 'aeiouu')
            ORDER BY
              CASE WHEN r.orden_manual IS NOT NULL AND r.orden_manual > 0 THEN 0 ELSE 1 END,
              r.orden_manual ASC NULLS LAST,
              r.orden_ruta ASC NULLS LAST,
              r.bsale_id ASC
        """,
        (v, v, dia),
    )
    clientes = cur.fetchall()
    clientes = [c for c in clientes if c[0] is not None]
    if not clientes:
        return 0

    ts_ms = int(time.time() * 1000)
    insertados = 0
    for orden, row in enumerate(clientes, start=1):
        bsale_id, nombre_fantasia, address, municipality, rut_clean, lat, lon = row
        cliente_str = str(int(bsale_id))
        local_action_id = f"init_{cliente_str}_{ts_ms}_{orden}"
        if len(local_action_id) > 128:
            local_action_id = local_action_id[:128]

        cur.execute(
            """
            INSERT INTO bsale.visitas (
                ruta_id,
                cliente_id,
                nombre_fantasia,
                direccion,
                comuna,
                rut_clean,
                orden_ruta,
                estado,
                lat_cliente,
                lon_cliente,
                local_action_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, 'pendiente', %s, %s, %s)
            """,
            (
                ruta_id,
                cliente_str,
                nombre_fantasia,
                address,
                municipality,
                rut_clean,
                orden,
                lat,
                lon,
                local_action_id,
            ),
        )
        insertados += 1

    cur.execute(
        """
        UPDATE bsale.rutas_dia
        SET total_clientes = %s,
            clientes_pendientes = %s
        WHERE id = %s
        """,
        (insertados, insertados, ruta_id),
    )
    return insertados


def _rows_to_dict_list(cur) -> list[dict]:
    """Convierte el resultado de fetchall() en lista de dicts (mismo patrón que otros routers)."""
    columns = [col[0] for col in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


def _fetchone_dict(cur) -> dict | None:
    columns = [col[0] for col in cur.description]
    row = cur.fetchone()
    if row is None:
        return None
    return dict(zip(columns, row))


def _actualizar_visita_sql(cur, body: VisitaUpdate) -> bool:
    """
    UPDATE de una fila existente en bsale.visitas por ``id``.
    No inserta: las filas deben existir (p. ej. generadas desde rutero en GET /vendedor/ruta).

    :returns: True si se actualizó una fila, False si ``id`` no existe.
    :raises ValidacionVisitaError: reglas de negocio (evidencia telefónica / GPS visitado).
    """
    cur.execute(
        """
        SELECT lat_cliente, lon_cliente, lat_visita, lon_visita
        FROM bsale.visitas
        WHERE id = %s
        FOR UPDATE
        """,
        (body.id,),
    )
    ex = cur.fetchone()
    if not ex:
        return False

    lat_c, lon_c, lat_v0, lon_v0 = ex
    lat_ve = body.lat_visita if body.lat_visita is not None else lat_v0
    lon_ve = body.lon_visita if body.lon_visita is not None else lon_v0

    msg = _validacion_negocio_campos(
        body.estado,
        body.tipo_incidencia,
        body.foto_url,
        lat_ve,
        lon_ve,
    )
    if msg:
        raise ValidacionVisitaError(msg)

    distancia, estado_val = _distancia_y_validacion_por_tipo(
        body.tipo_incidencia,
        lat_c,
        lon_c,
        lat_ve,
        lon_ve,
    )

    foto_persistida = body.foto_url
    if body.foto_url is not None and str(body.foto_url).strip():
        foto_persistida = normalize_and_persist_foto_url(body.id, body.foto_url)

    logger.info(
        "Actualizar visita id=%s con_compra=%r (enviado en request) estado=%s",
        body.id,
        body.con_compra,
        body.estado,
    )

    cur.execute(
        """
        UPDATE bsale.visitas
        SET
            estado = %s,
            tipo_incidencia = %s,
            observacion = %s,
            foto_url = %s,
            con_compra = COALESCE(%s, con_compra),
            lat_visita = COALESCE(%s, lat_visita),
            lon_visita = COALESCE(%s, lon_visita),
            distancia_metros = %s,
            validacion_estado = %s,
            fecha_hora_visita = COALESCE(%s, fecha_hora_visita),
            sync_status = COALESCE(%s, sync_status),
            updated_at = clock_timestamp()
        WHERE id = %s
        """,
        (
            body.estado,
            body.tipo_incidencia,
            body.observacion,
            foto_persistida,
            body.con_compra,
            body.lat_visita,
            body.lon_visita,
            distancia,
            estado_val,
            body.fecha_hora_visita,
            body.sync_status,
            body.id,
        ),
    )
    return cur.rowcount > 0


_TIPOS_USUARIO_APP = frozenset({"vendedor", "chofer", "bodega"})


def _tipo_usuario_app_validado(rec: dict) -> TipoUsuarioApp:
    """Exige ``tipo_usuario`` no nulo y uno de los valores permitidos (insensible a mayúsculas)."""
    raw = rec.get("tipo_usuario")
    if raw is None:
        raise HTTPException(
            status_code=403,
            detail="Usuario sin tipo_usuario asignado. Contacte al administrador.",
        )
    t = str(raw).strip().lower()
    if not t:
        raise HTTPException(
            status_code=403,
            detail="Usuario sin tipo_usuario asignado. Contacte al administrador.",
        )
    if t not in _TIPOS_USUARIO_APP:
        raise HTTPException(
            status_code=403,
            detail="tipo_usuario no válido en el servidor. Contacte al administrador.",
        )
    return cast(TipoUsuarioApp, t)


def _password_hash_a_bytes(stored) -> bytes:
    """Normaliza lo que devuelve psycopg2 (str o memoryview) a bytes para bcrypt."""
    if isinstance(stored, memoryview):
        return bytes(stored)
    if isinstance(stored, bytes):
        return stored
    return str(stored).encode("utf-8")


@router.post("/login", response_model=LoginSuccessResponse)
def post_login_vendedor_app(body: LoginRequest):
    """
    Login de la app móvil (tabla bsale.vendedores_app, hash bcrypt).

    La respuesta incluye ``tipo_usuario`` (vendedor | chofer | bodega); debe estar definido en BD.
    """
    codigo = body.codigo.strip()
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT codigo, nombre, password_hash, tipo_usuario
            FROM bsale.vendedores_app
            WHERE codigo = %s AND activo = true
            LIMIT 1
            """,
            (codigo,),
        )
        row = cur.fetchone()
        if not row:
            cur.close()
            raise HTTPException(
                status_code=401,
                detail="Código o contraseña incorrectos.",
            )
        cols = [c[0] for c in cur.description]
        rec = dict(zip(cols, row))
        cur.close()

        plain = body.password.encode("utf-8")
        raw_ph = rec["password_hash"]
        hashed = _password_hash_a_bytes(raw_ph)
        check_ok = bcrypt.checkpw(plain, hashed)
        if not check_ok:
            raise HTTPException(
                status_code=401,
                detail="Código o contraseña incorrectos.",
            )
    finally:
        conn.close()

    tipo = _tipo_usuario_app_validado(rec)
    return LoginSuccessResponse(
        success=True,
        vendedor=rec["codigo"],
        nombre=rec["nombre"],
        tipo_usuario=tipo,
    )


@router.get("/vendedor/ruta", response_model=RutaResponse)
def get_ruta_del_dia(
    fecha: date = Query(..., description="Fecha de la ruta"),
    vendedor: str = Query(..., min_length=1, max_length=255, description="Código o id de vendedor"),
):
    """
    Obtiene la ruta del día en bsale.rutas_dia y las visitas asociadas ordenadas por orden_ruta.

    Si no existe ``rutas_dia`` para (fecha, vendedor), se crea con métricas en cero.
    Si ``COUNT(visitas) = 0`` para esa ruta, se ejecuta ``_poblar_visitas_desde_rutero`` y luego se vuelve a leer
    ``rutas_dia`` y ``visitas`` antes de responder (nunca se devuelve ``visitas`` sin intentar poblar antes).
    """
    v = (vendedor or "").strip().lower()
    conn = get_connection()
    try:
        cur = conn.cursor()
        try:
            cols, ruta_row = _insert_ruta_dia_si_ausente(cur, fecha, v)
            ruta = dict(zip(cols, ruta_row))
            ruta_id = ruta["id"]

            if _count_visitas_por_ruta(cur, ruta_id) == 0:
                _poblar_visitas_desde_rutero(cur, ruta_id, fecha, v)

            cur.execute(_RUTA_DIA_BY_ID_SELECT, (ruta_id,))
            ruta_row = cur.fetchone()
            cols_r = [c[0] for c in cur.description]
            ruta = dict(zip(cols_r, ruta_row))

            visitas = _fetch_visitas_por_ruta(cur, ruta_id)
            ruta["visitas"] = visitas

            conn.commit()
        except (psycopg2.Error, RuntimeError):
            conn.rollback()
            logger.exception(
                "Error al obtener o inicializar GET /vendedor/ruta (fecha=%s vendedor=%s)",
                fecha,
                v,
            )
            raise HTTPException(
                status_code=500,
                detail="No se pudo obtener o inicializar la ruta del día.",
            ) from None
        finally:
            cur.close()
    finally:
        conn.close()

    return RutaResponse.model_validate(ruta)


@router.post("/visitas", response_model=VisitaAltaResponse)
def post_visita(body: VisitaUpdate):
    """
    Actualiza una visita existente por ``id`` (no inserta filas).

    ``tipo_incidencia`` = ``atencion telefonica``: exige ``foto_url`` y no aplica validación GPS.
    Estado ``visitado`` en terreno: exige ``lat_visita`` y ``lon_visita`` (o valores ya guardados).
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        try:
            ok = _actualizar_visita_sql(cur, body)
        except ValidacionVisitaError as e:
            conn.rollback()
            cur.close()
            raise HTTPException(status_code=400, detail=e.mensaje) from None
        except psycopg2.Error:
            logger.exception("Error SQL al actualizar visita (id=%s)", body.id)
            conn.rollback()
            cur.close()
            raise HTTPException(
                status_code=500,
                detail="Error al procesar la actualización de la visita.",
            ) from None

        if not ok:
            conn.rollback()
            cur.close()
            raise HTTPException(
                status_code=404,
                detail="No existe la visita indicada.",
            )

        conn.commit()
        cur.close()
        return VisitaAltaResponse(mensaje="Visita actualizada", ok=True)
    finally:
        conn.close()


@router.post("/visitas/sync", response_model=SyncResponse)
def post_visitas_sync(body: SyncRequest):
    """
    Actualiza visitas existentes una por una (solo UPDATE por ``id``).
    Cada ítem usa su propia conexión/transacción; los fallos no afectan al resto.
    """
    sincronizados = 0
    errores = 0

    for item in body.visitas:
        conn = get_connection()
        try:
            cur = conn.cursor()
            try:
                ok = _actualizar_visita_sql(cur, item)
            except ValidacionVisitaError as e:
                conn.rollback()
                errores += 1
                logger.warning("Sync: validación rechazada id=%s: %s", item.id, e.mensaje)
                cur.close()
                continue
            except psycopg2.Error:
                conn.rollback()
                errores += 1
                logger.exception("Sync: error SQL para visita id=%s", item.id)
                cur.close()
                continue

            if not ok:
                conn.rollback()
                errores += 1
                logger.warning("Sync: visita inexistente id=%s", item.id)
                cur.close()
                continue

            conn.commit()
            sincronizados += 1
            cur.close()
        finally:
            conn.close()

    return SyncResponse(sincronizados=sincronizados, errores=errores)


@router.post(
    "/heartbeat",
    response_model=HeartbeatAckResponse,
    summary="Telemetría operacional (alias app móvil)",
    tags=["App Distribuidora", "Operaciones Quillotana"],
)
async def post_app_heartbeat(
    body: HeartbeatRequest,
    x_heartbeat_key: Annotated[str | None, Header(alias="X-Heartbeat-Key")] = None,
    authorization: Annotated[str | None, Header(alias="Authorization")] = None,
) -> HeartbeatAckResponse:
    """Misma lógica que ``POST /operaciones/heartbeat`` (base URL ``/app_distribuidora``)."""
    return await handle_heartbeat(body, x_heartbeat_key, authorization)


@router.post(
    "/operaciones/heartbeat",
    response_model=HeartbeatAckResponse,
    summary="Telemetría operacional (ruta relativa desde app)",
    tags=["App Distribuidora", "Operaciones Quillotana"],
)
async def post_app_operaciones_heartbeat(
    body: HeartbeatRequest,
    x_heartbeat_key: Annotated[str | None, Header(alias="X-Heartbeat-Key")] = None,
    authorization: Annotated[str | None, Header(alias="Authorization")] = None,
) -> HeartbeatAckResponse:
    """Si la app usa base ``/app_distribuidora`` + path ``operaciones/heartbeat``."""
    return await handle_heartbeat(body, x_heartbeat_key, authorization)
