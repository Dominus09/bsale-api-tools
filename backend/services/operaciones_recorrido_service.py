"""
Recorrido cronológico del vendedor (visitas, incidencias, GPS, telemetría).
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import Any

from backend.db import get_connection
from backend.services.gps_track_service import ensure_gps_track_table, km_for_vendedor_day
from backend.services.heartbeat_service import _ensure_utc, load_snapshots
from backend.services.operaciones_visitas import ESTADO_INCIDENCIA, es_visita_realizada

logger = logging.getLogger(__name__)


def _ts_key(ts: datetime | None) -> float:
    if ts is None:
        return float("inf")
    u = _ensure_utc(ts)
    return u.timestamp()


def get_vendedor_recorrido(codigo: str, fecha: date | None = None) -> dict[str, Any] | None:
    f = fecha or date.today()
    cod = codigo.strip()
    if not cod:
        return None

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT codigo, nombre FROM bsale.vendedores_app
            WHERE codigo = %s AND tipo_usuario = 'vendedor'
            LIMIT 1
            """,
            (cod,),
        )
        va = cur.fetchone()
        if not va:
            return None

        cur.execute(
            """
            SELECT id, hora_inicio
            FROM bsale.rutas_dia
            WHERE vendedor = %s AND fecha = %s
            LIMIT 1
            """,
            (cod, f),
        )
        rd = cur.fetchone()
        ruta_id = int(rd[0]) if rd else None
        hora_inicio_ruta = rd[1] if rd else None

        eventos: list[dict[str, Any]] = []

        if ruta_id is not None:
            cur.execute(
                """
                SELECT
                    id, cliente_id, nombre_fantasia, estado, tipo_incidencia,
                    fecha_hora_visita, lat_visita, lon_visita, orden_ruta
                FROM bsale.visitas
                WHERE ruta_id = %s
                ORDER BY fecha_hora_visita NULLS LAST, orden_ruta, id
                """,
                (ruta_id,),
            )
            for row in cur.fetchall():
                vid, cliente_id, nombre, estado, tipo_inc, fh, lat, lon, _orden = row
                if lat is None or lon is None:
                    continue
                try:
                    la, lo = float(lat), float(lon)
                except (TypeError, ValueError):
                    continue
                if la == 0.0 and lo == 0.0:
                    continue
                est = str(estado or "")
                if est == ESTADO_INCIDENCIA:
                    tipo_ev = "incidencia"
                elif es_visita_realizada(est):
                    tipo_ev = "visita"
                else:
                    continue
                eventos.append(
                    {
                        "tipo": tipo_ev,
                        "cliente": str(nombre or cliente_id or ""),
                        "cliente_id": str(cliente_id or ""),
                        "visita_id": int(vid),
                        "lat": la,
                        "lon": lo,
                        "timestamp": fh,
                        "detalle": tipo_inc if tipo_inc else est,
                    }
                )

        ensure_gps_track_table(cur)
        cur.execute(
            """
            SELECT lat, lng, timestamp
            FROM bsale.operaciones_gps_track
            WHERE vendedor_id = %s
              AND timestamp >= %s::date
              AND timestamp < (%s::date + interval '1 day')
            ORDER BY timestamp ASC
            """,
            (cod, f, f),
        )
        gps_rows = cur.fetchall()
        gps_count = len(gps_rows)

        inicio: dict[str, Any] | None = None
        ultima: dict[str, Any] | None = None

        hb_map = load_snapshots(cur, f)
        hb = hb_map.get(cod)

        if hora_inicio_ruta and ruta_id:
            inicio = {
                "lat": None,
                "lon": None,
                "timestamp": hora_inicio_ruta,
                "fuente": "ruta",
            }

        if gps_rows:
            lat0, lng0, ts0 = gps_rows[0]
            if inicio is None or (ts0 and _ts_key(ts0) < _ts_key(inicio.get("timestamp"))):
                inicio = {
                    "lat": float(lat0),
                    "lon": float(lng0),
                    "timestamp": _ensure_utc(ts0),
                    "fuente": "gps",
                }
            lat_l, lng_l, ts_l = gps_rows[-1]
            ultima = {
                "lat": float(lat_l),
                "lon": float(lng_l),
                "timestamp": _ensure_utc(ts_l),
                "fuente": "gps",
            }

        if hb and hb.lat is not None and hb.lng is not None:
            hb_pos = {
                "lat": hb.lat,
                "lon": hb.lng,
                "timestamp": hb.last_timestamp,
                "fuente": "heartbeat",
            }
            if ultima is None or _ts_key(hb.last_timestamp) >= _ts_key(ultima.get("timestamp")):
                ultima = hb_pos

        eventos.sort(key=lambda e: _ts_key(e.get("timestamp")))

        puntos: list[dict[str, Any]] = []
        for i, ev in enumerate(eventos, start=1):
            puntos.append(
                {
                    "orden": i,
                    "tipo": ev["tipo"],
                    "cliente": ev["cliente"],
                    "cliente_id": ev.get("cliente_id"),
                    "visita_id": ev.get("visita_id"),
                    "lat": ev["lat"],
                    "lon": ev["lon"],
                    "timestamp": ev["timestamp"],
                    "detalle": ev.get("detalle"),
                }
            )

        linea_gps = [
            {"lat": float(r[0]), "lon": float(r[1]), "timestamp": _ensure_utc(r[2])}
            for r in gps_rows
        ]

        km_m, _ = km_for_vendedor_day(cur, cod, f)

        clientes_asignados = 0
        visitados = 0
        incidencias_count = 0
        primera_visita: datetime | None = None
        ultima_visita: datetime | None = None

        if ruta_id is not None:
            cur.execute(
                "SELECT COUNT(*)::int FROM bsale.visitas WHERE ruta_id = %s",
                (ruta_id,),
            )
            clientes_asignados = int(cur.fetchone()[0] or 0)
            visitados = sum(1 for e in eventos if e["tipo"] == "visita")
            incidencias_count = sum(1 for e in eventos if e["tipo"] == "incidencia")
            ts_visitas = [e["timestamp"] for e in eventos if e.get("timestamp")]
            if ts_visitas:
                primera_visita = min(ts_visitas, key=_ts_key)
                ultima_visita = max(ts_visitas, key=_ts_key)

        tiempo_activo_min: int | None = None
        if primera_visita and ultima_visita:
            tiempo_activo_min = max(
                0,
                int((_ts_key(ultima_visita) - _ts_key(primera_visita)) / 60),
            )

        cur.close()
    finally:
        conn.close()

    return {
        "vendedor_id": cod,
        "vendedor_nombre": str(va[1]),
        "fecha": f,
        "ruta_id": ruta_id,
        "inicio": inicio,
        "ultima_posicion": ultima,
        "puntos": puntos,
        "linea_gps": linea_gps,
        "km_recorridos": round(km_m / 1000.0, 2),
        "metricas": {
            "clientes_asignados": clientes_asignados,
            "visitados": visitados,
            "incidencias": incidencias_count,
            "km_recorridos": round(km_m / 1000.0, 2),
            "primera_visita": primera_visita,
            "ultima_visita": ultima_visita,
            "tiempo_activo_minutos": tiempo_activo_min,
            "gps_puntos_recibidos": gps_count,
        },
    }
