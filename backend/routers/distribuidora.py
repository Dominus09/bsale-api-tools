from fastapi import APIRouter

from backend.db import get_connection

router = APIRouter(prefix="/distribuidora", tags=["Distribuidora"])


@router.get("/rutero")
def get_rutero():
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                id,
                bsale_id,
                first_name,
                last_name,
                nombre_fantasia,
                phone,
                vendedor,
                dia_atencion,
                dia_extra,
                municipality,
                lat,
                lon,
                tipo_atencion,
                orden_ruta
            FROM bsale.rutero
            WHERE company_id = 3
              AND activo = TRUE
            """
        )
        columns = [col[0] for col in cur.description]
        rows = cur.fetchall()
        data = [dict(zip(columns, row)) for row in rows]
        cur.close()
    finally:
        conn.close()

    return data


@router.get("/mapa")
def get_mapa():
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                bsale_id,
                first_name,
                last_name,
                nombre_fantasia,
                phone,
                vendedor,
                dia_atencion,
                dia_extra,
                municipality,
                lat,
                lon,
                tipo_atencion,
                orden_ruta
            FROM bsale.rutero
            WHERE company_id = 3
              AND activo = TRUE
              AND lat IS NOT NULL
              AND lon IS NOT NULL
            """
        )
        columns = [col[0] for col in cur.description]
        clientes = [dict(zip(columns, row)) for row in cur.fetchall()]

        cur.execute(
            """
            SELECT vendedor, nombre, lat, lon
            FROM bsale.puntos_base
            """
        )
        columns = [col[0] for col in cur.description]
        bases = [dict(zip(columns, row)) for row in cur.fetchall()]
        cur.close()
    finally:
        conn.close()

    return {
        "clientes": clientes,
        "bases": bases,
    }


@router.get("/resumen")
def get_resumen():
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                vendedor,
                dia_atencion,
                COUNT(*) AS cantidad
            FROM bsale.rutero
            WHERE company_id = 3
              AND activo = TRUE
            GROUP BY vendedor, dia_atencion
            ORDER BY vendedor, dia_atencion
            """
        )
        columns = [col[0] for col in cur.description]
        data = [dict(zip(columns, row)) for row in cur.fetchall()]
        cur.close()
    finally:
        conn.close()

    return data
