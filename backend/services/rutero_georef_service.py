"""
Georreferencia operacional sobre bsale.rutero (sin tocar bsale.clients).
Coordenadas de captura en lat_operacional / lon_operacional (sync_rutero no las pisa).
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

from backend.db import get_connection
from backend.utils.rutero_dia_sql import dia_atencion_desde_fecha, where_dia_operativo_r
from backend.utils.rutero_coords_sql import (
    R_LAT,
    R_LAT_AS,
    R_LON,
    R_LON_AS,
    RET_LAT,
    RET_LON,
    WHERE_HAS_GEOREF_R,
    WHERE_SIN_GEOREF_EFECTIVA_R,
)

logger = logging.getLogger(__name__)

_GEOREF_ESTADOS = frozenset({"pendiente", "capturada", "aplicada"})

_CLIENTE_NOMBRE_SQL = """
    COALESCE(
        NULLIF(TRIM(r.nombre_fantasia), ''),
        NULLIF(
            TRIM(
                CONCAT_WS(
                    ' ',
                    NULLIF(TRIM(r.first_name), ''),
                    NULLIF(TRIM(r.last_name), '')
                )
            ),
            ''
        ),
        'Cliente #' || r.bsale_id::text
    )
"""

_SELECT_GEOREF_ERP = f"""
    SELECT
        r.bsale_id::text AS cliente_codigo,
        {_CLIENTE_NOMBRE_SQL} AS cliente_nombre,
        LOWER(TRIM(COALESCE(r.vendedor::text, r.company::text, ''))) AS vendedor_codigo,
        r.id AS ruta_id,
        NULLIF(TRIM(r.address), '') AS direccion,
        NULLIF(TRIM(r.municipality), '') AS comuna,
        {R_LAT_AS},
        {R_LON_AS},
        r.georef_estado,
        r.georef_actualizada_at,
        r.georef_actualizada_por
    FROM bsale.rutero r
"""


def _norm_vendedor(v: str) -> str:
    return (v or "").strip().lower()


def _row_to_item(row: tuple, cols: list[str]) -> dict[str, Any]:
    return dict(zip(cols, row))


def _ensure_georef_schema(cur) -> None:
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'bsale'
          AND table_name = 'rutero'
          AND column_name IN ('georef_estado', 'lat_operacional', 'lon_operacional')
        """
    )
    found = {r[0] for r in cur.fetchall()}
    missing = {"georef_estado", "lat_operacional", "lon_operacional"} - found
    if missing:
        raise RuntimeError(
            "Columnas georef incompletas en bsale.rutero "
            f"(faltan: {sorted(missing)}). Ejecute backend/sql/rutero_add_georef.sql"
        )


def _vendedor_clause(vendedor_codigo: str | None) -> tuple[str, list[Any]]:
    """ERP: código en columna vendedor o company (COALESCE)."""
    v = _norm_vendedor(vendedor_codigo) if vendedor_codigo else ""
    if not v:
        return "", []
    return (
        "LOWER(TRIM(COALESCE(r.vendedor::text, r.company::text, ''))) = %s",
        [v],
    )


def _vendedor_clause_ruta(vendedor_codigo: str) -> tuple[str, list[Any]]:
    """App móvil / ruta del día: company o vendedor coincide (misma regla que visitas)."""
    v = _norm_vendedor(vendedor_codigo)
    if not v:
        return "", []
    return (
        """(
            LOWER(TRIM(COALESCE(r.company::text, ''))) = %s
         OR LOWER(TRIM(COALESCE(r.vendedor::text, ''))) = %s
        )""",
        [v, v],
    )


def _count_view_sin_georef(cur, vendedor: str) -> int:
    """Filas en ``v_clientes_sin_georef`` con ``vendedor_codigo`` (COALESCE vendedor|company)."""
    cur.execute(
        """
        SELECT COUNT(*)::int
        FROM bsale.v_clientes_sin_georef
        WHERE vendedor_codigo = %s
        """,
        [vendedor],
    )
    row = cur.fetchone()
    return int(row[0] or 0) if row else 0


def get_georef_resumen(vendedor_codigo: str | None = None) -> dict[str, int]:
    """Contadores para panel ERP (activos empresa 3)."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        _ensure_georef_schema(cur)
        v_clause, v_params = _vendedor_clause(vendedor_codigo)
        v_and = f" AND {v_clause}" if v_clause else ""
        cur.execute(
            f"""
            SELECT
                COUNT(*)::int AS total,
                COUNT(*) FILTER (
                    WHERE r.georef_estado = 'pendiente'
                      AND {WHERE_SIN_GEOREF_EFECTIVA_R}
                )::int AS pendientes,
                COUNT(*) FILTER (
                    WHERE r.georef_estado = 'capturada'
                      AND {WHERE_HAS_GEOREF_R}
                )::int AS capturados,
                COUNT(*) FILTER (
                    WHERE r.georef_estado = 'aplicada'
                )::int AS aplicados
            FROM bsale.rutero r
            WHERE r.company_id = 3
              AND r.activo = TRUE
              {v_and}
            """,
            v_params,
        )
        row = cur.fetchone()
        cur.close()
        if not row:
            return {"total": 0, "pendientes": 0, "capturados": 0, "aplicados": 0}
        return {
            "total": int(row[0] or 0),
            "pendientes": int(row[1] or 0),
            "capturados": int(row[2] or 0),
            "aplicados": int(row[3] or 0),
        }
    finally:
        conn.close()


_GEOREF_ERP_FILTROS = frozenset({"todas", "pendiente", "capturada", "aplicada"})

_WHERE_GEOREF_PENDIENTE_OPERATIVO = (
    f"(r.georef_estado = 'pendiente' AND {WHERE_SIN_GEOREF_EFECTIVA_R})"
)

_WHERE_GEOREF_TODAS_OPERATIVO = (
    f"({_WHERE_GEOREF_PENDIENTE_OPERATIVO} "
    "OR r.georef_estado = 'capturada' "
    "OR r.georef_estado = 'aplicada')"
)


def _where_georef_erp_estado(estado: str | None) -> str:
    est = (estado or "todas").strip().lower()
    if est not in _GEOREF_ERP_FILTROS:
        raise ValueError("estado inválido")
    if est == "pendiente":
        return _WHERE_GEOREF_PENDIENTE_OPERATIVO
    if est == "capturada":
        return "r.georef_estado = 'capturada'"
    if est == "aplicada":
        return "r.georef_estado = 'aplicada'"
    return _WHERE_GEOREF_TODAS_OPERATIVO


def list_georef_erp(
    vendedor_codigo: str | None = None,
    estado: str | None = "todas",
    *,
    solo_pendientes: bool | None = None,
) -> list[dict[str, Any]]:
    """
    Listado panel ERP (flujo operacional georef).

  - ``pendiente``: ``georef_estado='pendiente'`` y sin coords efectivas.
  - ``capturada`` / ``aplicada``: por estado.
  - ``todas``: unión de los tres (no el universo completo del rutero).
    """
    if solo_pendientes is not None:
        estado = "pendiente" if solo_pendientes else "todas"

    conn = get_connection()
    try:
        cur = conn.cursor()
        _ensure_georef_schema(cur)
        wheres = ["r.company_id = 3", "r.activo = TRUE", _where_georef_erp_estado(estado)]
        params: list[Any] = []

        v_clause, v_params = _vendedor_clause(vendedor_codigo)
        if v_clause:
            wheres.append(v_clause)
            params.extend(v_params)

        cur.execute(
            f"""
            {_SELECT_GEOREF_ERP}
            WHERE {' AND '.join(wheres)}
            ORDER BY vendedor_codigo, ruta_id, cliente_nombre
            """,
            params,
        )
        cols = [c[0] for c in cur.description]
        rows = [_row_to_item(r, cols) for r in cur.fetchall()]
        cur.close()
        return rows
    finally:
        conn.close()


def list_georef_pendientes_movil(
    vendedor_codigo: str,
    *,
    fecha: date | None = None,
    debug: bool = False,
) -> tuple[list[dict[str, Any]], dict[str, int] | None]:
    """
    Pendientes reales para app móvil.

    - Georef efectiva: ``WHERE_SIN_GEOREF_EFECTIVA_R`` (NULL o 0,0), alineado a Rutero geo=sin.
    - Vendedor: ``company`` o ``vendedor`` = código (igual que GET /vendedor/ruta).
    - Una fila por ``bsale_id`` (evita duplicados si hubiera más de un rutero activo).
    - ``fecha`` opcional: filtra por día operativo (sábado vía ``dia_extra``).
    """
    v = _norm_vendedor(vendedor_codigo)
    if not v:
        raise ValueError("vendedor_codigo es obligatorio")

    conn = get_connection()
    try:
        cur = conn.cursor()
        _ensure_georef_schema(cur)

        wheres = [
            "r.company_id = 3",
            "r.activo = TRUE",
            WHERE_SIN_GEOREF_EFECTIVA_R,
        ]
        params: list[Any] = []

        v_clause, v_params = _vendedor_clause_ruta(v)
        wheres.append(v_clause)
        params.extend(v_params)

        if fecha is not None:
            dia_clause, dia_params = where_dia_operativo_r(dia_atencion_desde_fecha(fecha))
            wheres.append(dia_clause)
            params.extend(dia_params)

        where_sql = " AND ".join(wheres)

        debug_info: dict[str, int] | None = None
        if debug:
            total_sql = _count_view_sin_georef(cur, v)
            cur.execute(
                f"""
                SELECT
                    COUNT(*)::int,
                    COUNT(DISTINCT r.bsale_id)::int
                FROM bsale.rutero r
                WHERE {where_sql}
                """,
                params,
            )
            cnt_row = cur.fetchone()
            total_antes = int(cnt_row[0] or 0) if cnt_row else 0
            distinct_antes = int(cnt_row[1] or 0) if cnt_row else 0
            debug_info = {
                "total_sql": total_sql,
                "total_post_filtro": 0,
                "duplicados": max(0, total_antes - distinct_antes),
            }

        cur.execute(
            f"""
            SELECT DISTINCT ON (r.bsale_id)
                r.bsale_id::text AS cliente_codigo,
                {_CLIENTE_NOMBRE_SQL} AS cliente_nombre,
                LOWER(TRIM(COALESCE(r.vendedor::text, r.company::text, ''))) AS vendedor_codigo,
                r.id AS ruta_id,
                NULLIF(TRIM(r.address), '') AS direccion,
                NULLIF(TRIM(r.municipality), '') AS comuna,
                {R_LAT_AS},
                {R_LON_AS},
                r.georef_estado
            FROM bsale.rutero r
            WHERE {where_sql}
            ORDER BY r.bsale_id, r.id DESC
            """,
            params,
        )
        cols = [c[0] for c in cur.description]
        rows = [_row_to_item(r, cols) for r in cur.fetchall()]
        if debug_info is not None:
            debug_info["total_post_filtro"] = len(rows)
        logger.info(
            "georef_pendientes_movil vendedor=%s fecha=%s items=%s debug=%s",
            v,
            fecha.isoformat() if fecha else None,
            len(rows),
            debug_info,
        )
        cur.close()
        return rows, debug_info
    finally:
        conn.close()


def list_pendientes_desde_view(
    vendedor_codigo: str | None = None,
    *,
    fecha: date | None = None,
    debug: bool = False,
) -> tuple[list[dict[str, Any]], dict[str, int] | None]:
    """Retrocompatible: delega en ``list_georef_pendientes_movil``."""
    v = _norm_vendedor(vendedor_codigo) if vendedor_codigo else ""
    if not v:
        return [], {"total_sql": 0, "total_post_filtro": 0, "duplicados": 0} if debug else None
    return list_georef_pendientes_movil(v, fecha=fecha, debug=debug)


def list_georef_operativa(
    vendedor_codigo: str | None = None,
    estado: str | None = None,
    solo_pendientes: bool = False,
) -> list[dict[str, Any]]:
    """Retrocompatible: delega en ``list_georef_erp``."""
    est = (estado or "").strip().lower()
    if est:
        return list_georef_erp(vendedor_codigo, estado=est)
    return list_georef_erp(
        vendedor_codigo,
        solo_pendientes=solo_pendientes,
    )


def _coords_efectivas_validas(lat, lon) -> bool:
    if lat is None or lon is None:
        return False
    try:
        la, lo = float(lat), float(lon)
    except (TypeError, ValueError):
        return False
    if la == 0.0 and lo == 0.0:
        return False
    return True


def _fetch_rutero_vendedor(cur, rutero_id: int) -> tuple[str, str] | None:
    cur.execute(
        """
        SELECT
            LOWER(TRIM(COALESCE(vendedor::text, company::text, ''))),
            georef_estado
        FROM bsale.rutero
        WHERE id = %s AND company_id = 3 AND activo = TRUE
        """,
        (rutero_id,),
    )
    row = cur.fetchone()
    if not row:
        return None
    return str(row[0] or ""), str(row[1] or "pendiente")


def _fetch_estado_coords(cur, rutero_id: int) -> tuple[str, float | None, float | None] | None:
    cur.execute(
        f"""
        SELECT georef_estado, {RET_LAT}, {RET_LON}
        FROM bsale.rutero
        WHERE id = %s AND company_id = 3 AND activo = TRUE
        """,
        (rutero_id,),
    )
    row = cur.fetchone()
    if not row:
        return None
    return str(row[0] or "pendiente"), row[1], row[2]


def capturar_georef(
    rutero_id: int,
    lat: float,
    lon: float,
    actualizada_por: str,
    vendedor_esperado: str | None = None,
) -> dict[str, Any] | None:
    """App móvil: persiste en lat_operacional/lon_operacional y marca ``capturada``."""
    por = (actualizada_por or "").strip()[:50]
    if not por:
        raise ValueError("actualizada_por es obligatorio")

    conn = get_connection()
    try:
        cur = conn.cursor()
        _ensure_georef_schema(cur)
        meta = _fetch_rutero_vendedor(cur, rutero_id)
        if meta is None:
            cur.close()
            return None

        v_row, _ = meta
        if vendedor_esperado:
            ve = _norm_vendedor(vendedor_esperado)
            if ve and v_row != ve:
                raise ValueError("El cliente no pertenece al vendedor indicado")

        cur.execute(
            f"""
            UPDATE bsale.rutero
            SET
                lat_operacional = %s,
                lon_operacional = %s,
                georef_estado = 'capturada',
                georef_actualizada_at = clock_timestamp(),
                georef_actualizada_por = %s
            WHERE id = %s
              AND company_id = 3
              AND activo = TRUE
            RETURNING
                bsale_id::text,
                {RET_LAT},
                {RET_LON},
                georef_estado,
                georef_actualizada_at,
                georef_actualizada_por
            """,
            (lat, lon, por, rutero_id),
        )
        row = cur.fetchone()
        if not row:
            conn.rollback()
            cur.close()
            return None
        conn.commit()
        cur.close()
        return {
            "ruta_id": rutero_id,
            "cliente_codigo": row[0],
            "lat": float(row[1]) if row[1] is not None else lat,
            "lon": float(row[2]) if row[2] is not None else lon,
            "georef_estado": row[3],
            "georef_actualizada_at": row[4],
            "georef_actualizada_por": row[5],
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def actualizar_estado_georef(
    rutero_id: int,
    georef_estado: str,
    actualizada_por: str,
) -> dict[str, Any] | None:
    """ERP staff: marcar aplicada o volver a pendiente (no borra coordenadas operacionales)."""
    est = (georef_estado or "").strip().lower()
    if est not in ("pendiente", "aplicada"):
        raise ValueError("georef_estado debe ser pendiente o aplicada")

    por = (actualizada_por or "").strip()[:50] or "erp"

    conn = get_connection()
    try:
        cur = conn.cursor()
        _ensure_georef_schema(cur)

        if est == "aplicada":
            cur.execute(
                f"""
                SELECT {RET_LAT}, {RET_LON}
                FROM bsale.rutero
                WHERE id = %s AND company_id = 3 AND activo = TRUE
                """,
                (rutero_id,),
            )
            coords = cur.fetchone()
            if coords is None:
                cur.close()
                return None
            if not _coords_efectivas_validas(coords[0], coords[1]):
                cur.close()
                raise ValueError("No se puede marcar aplicada sin georreferencia.")

        cur.execute(
            f"""
            UPDATE bsale.rutero
            SET
                georef_estado = %s,
                georef_actualizada_at = clock_timestamp(),
                georef_actualizada_por = %s
            WHERE id = %s
              AND company_id = 3
              AND activo = TRUE
            RETURNING
                bsale_id::text,
                {RET_LAT},
                {RET_LON},
                georef_estado,
                georef_actualizada_at,
                georef_actualizada_por
            """,
            (est, por, rutero_id),
        )
        row = cur.fetchone()
        if not row:
            conn.rollback()
            cur.close()
            return None
        conn.commit()
        cur.close()
        return {
            "ruta_id": rutero_id,
            "cliente_codigo": row[0],
            "lat": row[1],
            "lon": row[2],
            "georef_estado": row[3],
            "georef_actualizada_at": row[4],
            "georef_actualizada_por": row[5],
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
