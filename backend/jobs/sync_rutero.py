"""
Sincronización bsale.clients → bsale.rutero (empresa 3, vendedores de ruta).

- Upsert por (company_id, bsale_id); coincide con el listado de Pendientes (clients sin día).
- Si cambian vendedor o dia_atencion en origen → orden_manual = NULL en rutero.
- Filas que dejan de cumplir el filtro → activo = FALSE (no se borran).
"""

from __future__ import annotations

import logging
from typing import Any

from backend.db import get_connection

logger = logging.getLogger(__name__)

_COMPANY_ID = 3
# Lock para evitar solapamiento si hay varios workers (cada uno lanza su propio loop).
_ADVISORY_LOCK_KEY = 4_817_293_001

_VENDEDORES_SQL = (
    "vendedor_1",
    "vendedor_2",
    "vendedor_3",
    "vendedor_4",
)


def _vendedores_placeholders() -> tuple[str, tuple[str, ...]]:
    ph = ", ".join(["%s"] * len(_VENDEDORES_SQL))
    return ph, _VENDEDORES_SQL


def sync_rutero() -> dict[str, Any]:
    """
    Ejecuta la sincronización y devuelve contadores para logs / API.

    Campos que siempre se toman de clients (alineado a rutero_sync_from_clients + requisitos):
    first_name, last_name, nombre_fantasia (nombre visible), phone, address, municipality,
    lat, lon, vendedor, dia_atencion, rut_clean, más columnas auxiliares ya usadas en INSERT histórico.
    """
    ph, vends = _vendedores_placeholders()
    conn = get_connection()
    stats: dict[str, Any] = {
        "ok": True,
        "omitido_concurrencia": False,
        "clientes_fuente": 0,
        "nuevos": 0,
        "actualizados": 0,
        "filas_upsert_afectadas": 0,
        "inactivados": 0,
        "sin_dia": 0,
        "errores": None,
    }
    got_lock = False
    try:
        cur = conn.cursor()
        cur.execute("SELECT pg_try_advisory_lock(%s)", (_ADVISORY_LOCK_KEY,))
        got_lock = bool(cur.fetchone()[0])
        if not got_lock:
            stats["omitido_concurrencia"] = True
            logger.info("sync_rutero omitido (otro proceso tiene el advisory lock)")
            cur.close()
            return stats

        cur.execute(
            """
            SELECT bsale_id
            FROM bsale.rutero
            WHERE company_id = %s
            """,
            (_COMPANY_ID,),
        )
        antes_rutero_ids = {int(r[0]) for r in cur.fetchall() if r and r[0] is not None}

        cur.execute(
            f"""
            SELECT
                c.company_id,
                c.bsale_id,
                c.first_name,
                c.last_name,
                c.code,
                c.phone,
                c.company,
                c.facebook,
                c.city,
                c.municipality,
                c.address,
                c.created,
                c.updated,
                c.dia_atencion,
                c.nombre_fantasia,
                c.vendedor,
                c.lat,
                c.lon,
                CASE
                    WHEN LOWER(TRIM(COALESCE(c.dia_atencion::text, ''))) = 'telefonico'
                    THEN 'telefonico'
                    ELSE 'terreno'
                END AS tipo_atencion
            FROM bsale.clients c
            WHERE c.company_id = %s
              AND LOWER(TRIM(COALESCE(c.vendedor::text, ''))) IN ({ph})
            """,
            (_COMPANY_ID, *vends),
        )
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        clientes = [dict(zip(cols, row)) for row in rows]
        stats["clientes_fuente"] = len(clientes)

        for c in clientes:
            da = c.get("dia_atencion")
            if da is None or (isinstance(da, str) and not str(da).strip()):
                stats["sin_dia"] += 1

        bid_fuente = {int(c["bsale_id"]) for c in clientes}
        stats["nuevos"] = sum(1 for bid in bid_fuente if bid not in antes_rutero_ids)

        # rut_clean: puede no existir en clients antiguos
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'bsale'
              AND table_name = 'clients'
              AND column_name = 'rut_clean'
            """
        )
        has_rut_clean = cur.fetchone() is not None

        if has_rut_clean:
            insert_sql = f"""
            INSERT INTO bsale.rutero (
                company_id, bsale_id, first_name, last_name, code, phone, company, facebook,
                city, municipality, address, created, updated, dia_atencion, dia_extra,
                nombre_fantasia, vendedor, rut_clean, lat, lon, tipo_atencion, activo
            )
            SELECT
                c.company_id,
                c.bsale_id,
                c.first_name,
                c.last_name,
                c.code,
                c.phone,
                c.company,
                c.facebook,
                c.city,
                c.municipality,
                c.address,
                c.created,
                c.updated,
                c.dia_atencion,
                NULL::text,
                c.nombre_fantasia,
                LOWER(TRIM(COALESCE(c.vendedor::text, ''))) AS vendedor,
                c.rut_clean,
                c.lat,
                c.lon,
                CASE
                    WHEN LOWER(TRIM(COALESCE(c.dia_atencion::text, ''))) = 'telefonico'
                    THEN 'telefonico'
                    ELSE 'terreno'
                END,
                TRUE
            FROM bsale.clients c
            WHERE c.company_id = %s
              AND LOWER(TRIM(COALESCE(c.vendedor::text, ''))) IN ({ph})
            ON CONFLICT (company_id, bsale_id) DO UPDATE SET
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                code = EXCLUDED.code,
                phone = EXCLUDED.phone,
                company = EXCLUDED.company,
                facebook = EXCLUDED.facebook,
                city = EXCLUDED.city,
                municipality = EXCLUDED.municipality,
                address = EXCLUDED.address,
                created = EXCLUDED.created,
                updated = EXCLUDED.updated,
                dia_atencion = EXCLUDED.dia_atencion,
                nombre_fantasia = EXCLUDED.nombre_fantasia,
                vendedor = EXCLUDED.vendedor,
                rut_clean = EXCLUDED.rut_clean,
                lat = EXCLUDED.lat,
                lon = EXCLUDED.lon,
                tipo_atencion = EXCLUDED.tipo_atencion,
                activo = TRUE,
                orden_manual = CASE
                    WHEN LOWER(TRIM(COALESCE(bsale.rutero.vendedor::text, '')))
                         IS DISTINCT FROM LOWER(TRIM(COALESCE(EXCLUDED.vendedor::text, '')))
                      OR LOWER(TRIM(COALESCE(bsale.rutero.dia_atencion::text, '')))
                         IS DISTINCT FROM LOWER(TRIM(COALESCE(EXCLUDED.dia_atencion::text, '')))
                    THEN NULL
                    ELSE bsale.rutero.orden_manual
                END
            """
        else:
            insert_sql = f"""
            INSERT INTO bsale.rutero (
                company_id, bsale_id, first_name, last_name, code, phone, company, facebook,
                city, municipality, address, created, updated, dia_atencion, dia_extra,
                nombre_fantasia, vendedor, rut_clean, lat, lon, tipo_atencion, activo
            )
            SELECT
                c.company_id,
                c.bsale_id,
                c.first_name,
                c.last_name,
                c.code,
                c.phone,
                c.company,
                c.facebook,
                c.city,
                c.municipality,
                c.address,
                c.created,
                c.updated,
                c.dia_atencion,
                NULL::text,
                c.nombre_fantasia,
                LOWER(TRIM(COALESCE(c.vendedor::text, ''))) AS vendedor,
                NULL::varchar,
                c.lat,
                c.lon,
                CASE
                    WHEN LOWER(TRIM(COALESCE(c.dia_atencion::text, ''))) = 'telefonico'
                    THEN 'telefonico'
                    ELSE 'terreno'
                END,
                TRUE
            FROM bsale.clients c
            WHERE c.company_id = %s
              AND LOWER(TRIM(COALESCE(c.vendedor::text, ''))) IN ({ph})
            ON CONFLICT (company_id, bsale_id) DO UPDATE SET
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                code = EXCLUDED.code,
                phone = EXCLUDED.phone,
                company = EXCLUDED.company,
                facebook = EXCLUDED.facebook,
                city = EXCLUDED.city,
                municipality = EXCLUDED.municipality,
                address = EXCLUDED.address,
                created = EXCLUDED.created,
                updated = EXCLUDED.updated,
                dia_atencion = EXCLUDED.dia_atencion,
                nombre_fantasia = EXCLUDED.nombre_fantasia,
                vendedor = EXCLUDED.vendedor,
                lat = EXCLUDED.lat,
                lon = EXCLUDED.lon,
                tipo_atencion = EXCLUDED.tipo_atencion,
                activo = TRUE,
                orden_manual = CASE
                    WHEN LOWER(TRIM(COALESCE(bsale.rutero.vendedor::text, '')))
                         IS DISTINCT FROM LOWER(TRIM(COALESCE(EXCLUDED.vendedor::text, '')))
                      OR LOWER(TRIM(COALESCE(bsale.rutero.dia_atencion::text, '')))
                         IS DISTINCT FROM LOWER(TRIM(COALESCE(EXCLUDED.dia_atencion::text, '')))
                    THEN NULL
                    ELSE bsale.rutero.orden_manual
                END
            """

        cur.execute(insert_sql, (_COMPANY_ID, *vends))
        merged = cur.rowcount if cur.rowcount is not None and cur.rowcount >= 0 else 0
        stats["filas_upsert_afectadas"] = merged
        stats["actualizados"] = max(0, merged - stats["nuevos"])

        cur.execute(
            f"""
            UPDATE bsale.rutero AS r
            SET activo = FALSE
            WHERE r.company_id = %s
              AND NOT EXISTS (
                  SELECT 1
                  FROM bsale.clients AS c
                  WHERE c.company_id = %s
                    AND c.bsale_id = r.bsale_id
                    AND LOWER(TRIM(COALESCE(c.vendedor::text, ''))) IN ({ph})
              )
            """,
            (_COMPANY_ID, _COMPANY_ID, *vends),
        )
        stats["inactivados"] = cur.rowcount if cur.rowcount is not None else 0

        conn.commit()

        logger.info(
            "sync_rutero: fuente=%s nuevos=%s upsert_afectadas=%s inactivados=%s sin_dia=%s rut_clean_col=%s",
            stats["clientes_fuente"],
            stats["nuevos"],
            merged,
            stats["inactivados"],
            stats["sin_dia"],
            has_rut_clean,
        )
        cur.close()
    except Exception as e:
        logger.exception("sync_rutero falló: %s", e)
        stats["ok"] = False
        stats["errores"] = str(e)
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        try:
            if got_lock:
                c2 = conn.cursor()
                c2.execute("SELECT pg_advisory_unlock(%s)", (_ADVISORY_LOCK_KEY,))
                c2.close()
        except Exception:
            logger.exception("sync_rutero: no se pudo liberar advisory lock")
        try:
            conn.close()
        except Exception:
            pass

    return stats


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    out = sync_rutero()
    print(out)
