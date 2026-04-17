"""
API móvil / distribuidora: rutas del día, visitas e idempotencia por local_action_id.

Prefijo de montaje en main: /app_distribuidora
"""

from __future__ import annotations

import logging
import time
from datetime import date

import bcrypt
import psycopg2
from fastapi import APIRouter, HTTPException, Query
from psycopg2 import errors as pg_errors

from backend.db import get_connection
from backend.schemas.distribuidora import (
    LoginRequest,
    LoginSuccessResponse,
    RutaResponse,
    SyncRequest,
    SyncResponse,
    VisitaAltaResponse,
    VisitaCreate,
    VisitaResponse,
)
from backend.utils.geo import distancia_y_estado_validacion

logger = logging.getLogger(__name__)

router = APIRouter(tags=["App Distribuidora"])

# Misma exclusión que GET /distribuidora/ruta-detalle (no visitas telefónicas en ruta terreno).
_SQL_RUTERO_EXCL_TELEFONICO = """
          AND LOWER(TRIM(COALESCE(dia_atencion::text, ''))) <> 'telefonico'
          AND LOWER(COALESCE(tipo_atencion::text, '')) <> 'telefonico'
"""

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
    """Nombre del día en español (minúsculas), alineado a bsale.rutero.dia_atencion en comparaciones LOWER()."""
    return _DIA_SEMANA_ES[fecha.weekday()]


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


def _fetch_ruta_dia_row(cur, fecha: date, v: str):
    cur.execute(_RUTA_DIA_SELECT_COLS, (fecha, v))
    return cur.fetchone()


def _insert_ruta_dia_si_ausente(cur, fecha: date, v: str):
    """
    Garantiza una fila en rutas_dia; devuelve la tupla completa y la lista de columnas
    (mismo shape que el SELECT principal).
    """
    row = _fetch_ruta_dia_row(cur, fecha, v)
    if row:
        cols = [c[0] for c in cur.description]
        return cols, row

    cur.execute(
        """
            INSERT INTO bsale.rutas_dia (fecha, vendedor)
            VALUES (%s, %s)
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
    Si no hay visitas para la ruta, las crea desde bsale.rutero (día = dia_atencion).
    Devuelve cuántas filas insertó.
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
                r.lat,
                r.lon
            FROM bsale.rutero r
            WHERE r.company_id = 3
              AND r.activo = TRUE
              AND LOWER(TRIM(COALESCE(r.vendedor::text, ''))) = %s
              AND LOWER(TRIM(COALESCE(r.dia_atencion::text, ''))) = LOWER(TRIM(%s))
        """
        + _SQL_RUTERO_EXCL_TELEFONICO
        + """
            ORDER BY
              CASE WHEN r.orden_manual IS NOT NULL AND r.orden_manual > 0 THEN 0 ELSE 1 END,
              r.orden_manual ASC NULLS LAST,
              r.orden_ruta ASC NULLS LAST,
              r.bsale_id ASC
        """,
        (v, dia),
    )
    clientes = cur.fetchall()
    clientes = [c for c in clientes if c[0] is not None]
    if not clientes:
        return 0

    ts_ms = int(time.time() * 1000)
    insertados = 0
    for orden, (bsale_id, lat, lon) in enumerate(clientes, start=1):
        cliente_str = str(int(bsale_id))
        local_action_id = f"init-{ts_ms}-{cliente_str}-{orden}"
        cur.execute(
            """
            INSERT INTO bsale.visitas (
                ruta_id,
                cliente_id,
                orden_ruta,
                estado,
                lat_cliente,
                lon_cliente,
                local_action_id
            ) VALUES (%s, %s, %s, 'pendiente', %s, %s, %s)
            """,
            (ruta_id, cliente_str, orden, lat, lon, local_action_id),
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


def _insertar_visita_sql(cur, body: VisitaCreate) -> dict | None:
    """
    Intenta insertar una fila en bsale.visitas.
    Devuelve el dict de la fila insertada, o None si local_action_id ya existía (ON CONFLICT).
    """
    distancia, estado_val = distancia_y_estado_validacion(
        body.lat_cliente,
        body.lon_cliente,
        body.lat_visita,
        body.lon_visita,
    )

    cur.execute(
        """
        INSERT INTO bsale.visitas (
            ruta_id,
            cliente_id,
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
            local_action_id
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s, %s
        )
        ON CONFLICT (local_action_id) DO NOTHING
        RETURNING
            id,
            ruta_id,
            cliente_id,
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
        """,
        (
            body.ruta_id,
            body.cliente_id,
            body.orden_ruta,
            body.estado,
            body.tipo_incidencia,
            body.con_compra,
            body.observacion,
            body.foto_url,
            body.lat_cliente,
            body.lon_cliente,
            body.lat_visita,
            body.lon_visita,
            distancia,
            estado_val,
            body.fecha_hora_visita,
            body.sync_status,
            body.local_action_id,
        ),
    )
    return _fetchone_dict(cur)


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
    Login de vendedores de la app móvil (tabla bsale.vendedores_app, hash bcrypt).
    """
    codigo = body.codigo.strip()
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT codigo, nombre, password_hash
            FROM bsale.vendedores_app
            WHERE codigo = %s AND activo = true
            LIMIT 1
            """,
            (codigo,),
        )
        row = cur.fetchone()
        if not row:
            cur.close()
            # --- DEBUG temporal (quitar tras depurar): no hay fila para este codigo ---
            logger.warning(
                "DEBUG_LOGIN_TEMP sin_fila_en_bd codigo_consultado=%r",
                codigo,
            )
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
        # --- DEBUG temporal (quitar tras depurar): diagnóstico hash / bcrypt ---
        logger.warning(
            "DEBUG_LOGIN_TEMP codigo_consultado=%r tipo_password_hash=%s longitud_hash_bytes=%s",
            codigo,
            type(raw_ph).__name__,
            len(hashed),
        )
        check_ok = bcrypt.checkpw(plain, hashed)
        logger.warning("DEBUG_LOGIN_TEMP bcrypt_checkpw=%s", check_ok)
        if not check_ok:
            raise HTTPException(
                status_code=401,
                detail="Código o contraseña incorrectos.",
            )
    finally:
        conn.close()

    return LoginSuccessResponse(
        success=True,
        vendedor=rec["codigo"],
        nombre=rec["nombre"],
    )


@router.get("/vendedor/ruta", response_model=RutaResponse)
def get_ruta_del_dia(
    fecha: date = Query(..., description="Fecha de la ruta"),
    vendedor: str = Query(..., min_length=1, max_length=255, description="Código o id de vendedor"),
):
    """
    Obtiene la ruta del día en bsale.rutas_dia y las visitas asociadas ordenadas por orden_ruta.

    Si no existe ``rutas_dia`` para (fecha, vendedor), se crea. Si la ruta no tiene visitas,
    se generan desde ``bsale.rutero`` según el día de la semana de ``fecha`` (columna ``dia_atencion``).
    """
    v = (vendedor or "").strip().lower()
    conn = get_connection()
    try:
        cur = conn.cursor()
        try:
            cols, ruta_row = _insert_ruta_dia_si_ausente(cur, fecha, v)
            ruta = dict(zip(cols, ruta_row))
            ruta_id = ruta["id"]

            _poblar_visitas_desde_rutero(cur, ruta_id, fecha, v)

            cur.execute(
                """
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
                """,
                (ruta_id,),
            )
            ruta_row = cur.fetchone()
            cols_r = [c[0] for c in cur.description]
            ruta = dict(zip(cols_r, ruta_row))

            cur.execute(
                """
                SELECT
                    id,
                    ruta_id,
                    cliente_id,
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
                """,
                (ruta_id,),
            )
            visitas = _rows_to_dict_list(cur)
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
def post_visita(body: VisitaCreate):
    """
    Registra una visita. Si local_action_id ya existe, no inserta (respuesta idempotente).
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        try:
            fila = _insertar_visita_sql(cur, body)
        except pg_errors.ForeignKeyViolation:
            conn.rollback()
            cur.close()
            raise HTTPException(
                status_code=400,
                detail="La ruta indicada no existe o no es válida para esta visita.",
            ) from None
        except psycopg2.Error:
            logger.exception("Error SQL al insertar visita (local_action_id=%s)", body.local_action_id)
            conn.rollback()
            cur.close()
            raise HTTPException(
                status_code=500,
                detail="error al procesar visita",
            ) from None

        if fila is None:
            conn.commit()
            cur.close()
            return VisitaAltaResponse(mensaje="visita ya registrada", insertado=False, data=None)

        conn.commit()
        cur.close()
        return VisitaAltaResponse(
            mensaje="visita registrada",
            insertado=True,
            data=VisitaResponse.model_validate(fila),
        )
    finally:
        conn.close()


@router.post("/visitas/sync", response_model=SyncResponse)
def post_visitas_sync(body: SyncRequest):
    """
    Procesa un lote de visitas: inserta las nuevas y omite duplicados por local_action_id.
    Cada ítem se confirma o revierte de forma independiente para maximizar visitas guardadas.
    """
    sincronizados = 0
    omitidos = 0
    errores = 0

    for item in body.visitas:
        conn = get_connection()
        try:
            cur = conn.cursor()
            try:
                fila = _insertar_visita_sql(cur, item)
            except pg_errors.ForeignKeyViolation:
                conn.rollback()
                errores += 1
                logger.warning(
                    "Sync: FK inválida para local_action_id=%s ruta_id=%s",
                    item.local_action_id,
                    item.ruta_id,
                )
                cur.close()
                continue
            except psycopg2.Error:
                conn.rollback()
                errores += 1
                logger.exception("Sync: error SQL para local_action_id=%s", item.local_action_id)
                cur.close()
                continue

            if fila is None:
                conn.commit()
                omitidos += 1
            else:
                conn.commit()
                sincronizados += 1
            cur.close()
        finally:
            conn.close()

    return SyncResponse(
        sincronizados=sincronizados,
        omitidos=omitidos,
        errores=errores,
    )
