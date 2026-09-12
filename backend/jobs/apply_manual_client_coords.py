"""
Aplica coordenadas manuales confirmadas a la fuente maestra de clientes.

Fuente: bsale.clients.lat / bsale.clients.lon
(mismo fallback permanente que usa delivery_map_service.py).

Además, si el cliente existe en bsale.rutero, actualiza lat_operacional /
lon_operacional (y lat/lon espejo) para que COALESCE operacional funcione
sin esperar sync_rutero. No crea filas nuevas de rutero.

Uso (contenedor Coolify / backend con PG_*):

    python -m backend.jobs.apply_manual_client_coords --execute

Dry-run (default):

    python -m backend.jobs.apply_manual_client_coords
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from backend.db import get_connection

# Coordenadas confirmadas manualmente (no van al frontend ni al delivery_map).
# Clave = bsale_id (client_id en dispatch_plan_orders).
MANUAL_COORDS: dict[int, dict[str, Any]] = {
    # Casa Esquila — PLAN-00044
    3411: {
        "name": "Casa Esquila",
        "lat": -42.378674115374466,
        "lon": -73.65026453890569,
    },
    # Supermercado la michelada — PLAN-00044
    3851: {
        "name": "Supermercado la michelada",
        "lat": -42.32447079164018,
        "lon": -73.56903424065618,
    },
}


def _apply(execute: bool) -> dict[str, Any]:
    conn = get_connection()
    report: dict[str, Any] = {"execute": execute, "clients": [], "rutero": []}
    try:
        cur = conn.cursor()
        for bsale_id, meta in MANUAL_COORDS.items():
            lat = float(meta["lat"])
            lon = float(meta["lon"])
            cur.execute(
                """
                SELECT bsale_id, company, nombre_fantasia, lat, lon
                FROM bsale.clients
                WHERE company_id = 3 AND bsale_id = %s
                """,
                (bsale_id,),
            )
            row = cur.fetchone()
            if not row:
                report["clients"].append(
                    {"bsale_id": bsale_id, "ok": False, "error": "not_found"}
                )
                continue
            before = {"lat": row[3], "lon": row[4]}
            if execute:
                cur.execute(
                    """
                    UPDATE bsale.clients
                    SET lat = %s,
                        lon = %s,
                        updated = CURRENT_TIMESTAMP
                    WHERE company_id = 3
                      AND bsale_id = %s
                    RETURNING bsale_id, lat, lon
                    """,
                    (lat, lon, bsale_id),
                )
                after = cur.fetchone()
            else:
                after = (bsale_id, lat, lon)
            report["clients"].append(
                {
                    "bsale_id": bsale_id,
                    "name": meta["name"],
                    "ok": True,
                    "before": before,
                    "after": {"lat": after[1], "lon": after[2]},
                }
            )

            cur.execute(
                """
                SELECT id, lat, lon, lat_operacional, lon_operacional, georef_estado
                FROM bsale.rutero
                WHERE company_id = 3 AND bsale_id = %s
                """,
                (bsale_id,),
            )
            rrows = cur.fetchall()
            if not rrows:
                report["rutero"].append(
                    {"bsale_id": bsale_id, "updated": 0, "note": "no_rutero_row"}
                )
            else:
                updated = 0
                for rr in rrows:
                    rid = int(rr[0])
                    if execute:
                        cur.execute(
                            """
                            UPDATE bsale.rutero
                            SET lat = %s,
                                lon = %s,
                                lat_operacional = %s,
                                lon_operacional = %s,
                                georef_estado = CASE
                                    WHEN georef_estado IS NULL OR georef_estado = 'pendiente'
                                      THEN 'capturada'
                                    ELSE georef_estado
                                END,
                                georef_actualizada_at = clock_timestamp(),
                                georef_actualizada_por = 'manual_coords_job'
                            WHERE id = %s
                              AND company_id = 3
                            """,
                            (lat, lon, lat, lon, rid),
                        )
                        updated += cur.rowcount or 0
                    else:
                        updated += 1
                report["rutero"].append(
                    {"bsale_id": bsale_id, "updated": updated, "rutero_ids": [int(r[0]) for r in rrows]}
                )

        if execute:
            conn.commit()
        else:
            conn.rollback()
        cur.close()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return report


def _validate_plan_44() -> dict[str, Any]:
    """Valida cobertura GPS del PLAN-00044 vía misma lógica de fallback."""
    from backend.utils.rutero_coords_sql import R_LAT, R_LON

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            f"""
            WITH o AS (
              SELECT
                o.client_id,
                o.client_name,
                o.lat AS order_lat,
                o.lng AS order_lng,
                {R_LAT} AS rutero_lat,
                {R_LON} AS rutero_lng,
                c.lat AS client_lat,
                c.lon AS client_lng
              FROM distribuidora.dispatch_plan_orders o
              LEFT JOIN bsale.rutero r
                ON o.client_id IS NOT NULL AND r.bsale_id = o.client_id
              LEFT JOIN bsale.clients c
                ON o.client_id IS NOT NULL AND c.bsale_id = o.client_id
              WHERE o.dispatch_plan_id = 44
            ),
            agg AS (
              SELECT
                COALESCE(client_id::text, lower(trim(coalesce(client_name,'')))) AS k,
                MAX(client_name) AS client_name,
                MAX(client_id) AS client_id,
                BOOL_OR(
                  (order_lat IS NOT NULL AND order_lng IS NOT NULL AND NOT (order_lat=0 AND order_lng=0))
                  OR (rutero_lat IS NOT NULL AND rutero_lng IS NOT NULL AND NOT (rutero_lat=0 AND rutero_lng=0))
                  OR (client_lat IS NOT NULL AND client_lng IS NOT NULL AND NOT (client_lat=0 AND client_lng=0))
                ) AS has_gps,
                MAX(COALESCE(
                  CASE WHEN order_lat IS NOT NULL AND order_lng IS NOT NULL AND NOT (order_lat=0 AND order_lng=0)
                       THEN order_lat END,
                  CASE WHEN rutero_lat IS NOT NULL AND rutero_lng IS NOT NULL AND NOT (rutero_lat=0 AND rutero_lng=0)
                       THEN rutero_lat END,
                  client_lat
                )) AS lat,
                MAX(COALESCE(
                  CASE WHEN order_lat IS NOT NULL AND order_lng IS NOT NULL AND NOT (order_lat=0 AND order_lng=0)
                       THEN order_lng END,
                  CASE WHEN rutero_lat IS NOT NULL AND rutero_lng IS NOT NULL AND NOT (rutero_lat=0 AND rutero_lng=0)
                       THEN rutero_lng END,
                  client_lng
                )) AS lng
              FROM o
              GROUP BY 1
            )
            SELECT
              (SELECT COUNT(*) FROM o) AS pedidos,
              COUNT(*) AS clientes,
              COUNT(*) FILTER (WHERE has_gps) AS con_gps,
              COUNT(*) FILTER (WHERE NOT has_gps) AS sin_gps
            FROM agg
            """,
        )
        summary = cur.fetchone()
        cur.execute(
            """
            SELECT bsale_id, lat, lon
            FROM bsale.clients
            WHERE company_id = 3 AND bsale_id IN (3411, 3851)
            ORDER BY bsale_id
            """,
        )
        clients = [{"bsale_id": r[0], "lat": r[1], "lon": r[2]} for r in cur.fetchall()]
        cur.close()
        return {
            "pedidos": summary[0],
            "clientes": summary[1],
            "con_gps": summary[2],
            "sin_gps": summary[3],
            "clients_master": clients,
        }
    finally:
        conn.close()


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Aplicar coords manuales a bsale.clients")
    p.add_argument(
        "--execute",
        action="store_true",
        help="Persiste cambios (sin este flag solo dry-run)",
    )
    args = p.parse_args(argv)
    report = _apply(execute=bool(args.execute))
    report["validation"] = _validate_plan_44()
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    v = report["validation"]
    if args.execute and (v.get("sin_gps") != 0 or v.get("con_gps") != 12):
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
