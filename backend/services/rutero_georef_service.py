"""
Georreferencia operacional sobre bsale.rutero (sin tocar bsale.clients).
Coordenadas de captura en lat_operacional / lon_operacional (sync_rutero no las pisa).
"""

from __future__ import annotations

import logging
from typing import Any

from backend.db import get_connection
from backend.utils.rutero_coords_sql import (
    R_LAT,
    R_LAT_AS,
    R_LON,
    R_LON_AS,
    WHERE_HAS_GEOREF_R,
    WHERE_SOLO_PENDIENTE_GEOREF_R,
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
    v = _norm_vendedor(vendedor_codigo) if vendedor_codigo else ""
    if not v:
        return "", []
    return (
        "LOWER(TRIM(COALESCE(r.vendedor::text, r.company::text, ''))) = %s",
        [v],
    )


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
                    WHERE {WHERE_SOLO_PENDIENTE_GEOREF_R}
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


def list_georef_erp(
    vendedor_codigo: str | None = None,
    solo_pendientes: bool = True,
) -> list[dict[str, Any]]:
    """
    Listado panel ERP.

    - ``solo_pendientes=True``: sin coords efectivas o ``georef_estado = pendiente``.
    - ``solo_pendientes=False``: pendientes + capturados + aplicados (seguimiento).
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        _ensure_georef_schema(cur)
        wheres = ["r.company_id = 3", "r.activo = TRUE"]
        params: list[Any] = []

        v_clause, v_params = _vendedor_clause(vendedor_codigo)
        if v_clause:
            wheres.append(v_clause)
            params.extend(v_params)

        if solo_pendientes:
            wheres.append(WHERE_SOLO_PENDIENTE_GEOREF_R)
        else:
            wheres.append(
                "(r.georef_estado IN ('capturada', 'aplicada') "
                f"OR {WHERE_SOLO_PENDIENTE_GEOREF_R})"
            )

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


def list_pendientes_desde_view(
    vendedor_codigo: str | None = None,
) -> list[dict[str, Any]]:
    """Filas de ``bsale.v_clientes_sin_georef`` (app móvil)."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        _ensure_georef_schema(cur)
        wheres = ["1=1"]
        params: list[Any] = []
        v = _norm_vendedor(vendedor_codigo) if vendedor_codigo else ""
        if v:
            wheres.append("vendedor_codigo = %s")
            params.append(v)
        cur.execute(
            f"""
            SELECT
                cliente_codigo,
                cliente_nombre,
                vendedor_codigo,
                ruta_id,
                direccion,
                NULL::text AS comuna,
                lat,
                lon,
                georef_estado
            FROM bsale.v_clientes_sin_georef
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


def list_georef_operativa(
    vendedor_codigo: str | None = None,
    estado: str | None = None,
    solo_pendientes: bool = False,
) -> list[dict[str, Any]]:
    """Retrocompatible: delega en ``list_georef_erp`` si no hay filtro por estado."""
    est = (estado or "").strip().lower()
    if not est:
        return list_georef_erp(vendedor_codigo, solo_pendientes=solo_pendientes)

    conn = get_connection()
    try:
        cur = conn.cursor()
        _ensure_georef_schema(cur)
        wheres = ["r.company_id = 3", "r.activo = TRUE", "r.georef_estado = %s"]
        params: list[Any] = [est]

        v_clause, v_params = _vendedor_clause(vendedor_codigo)
        if v_clause:
            wheres.append(v_clause)
            params.extend(v_params)

        if solo_pendientes:
            wheres.append(WHERE_SOLO_PENDIENTE_GEOREF_R)

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
                {R_LAT},
                {R_LON},
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
    """ERP staff: marcar aplicada o volver a pendiente."""
    est = (georef_estado or "").strip().lower()
    if est not in ("pendiente", "aplicada"):
        raise ValueError("georef_estado debe ser pendiente o aplicada")

    por = (actualizada_por or "").strip()[:50] or "erp"

    conn = get_connection()
    try:
        cur = conn.cursor()
        _ensure_georef_schema(cur)
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
                {R_LAT},
                {R_LON},
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
