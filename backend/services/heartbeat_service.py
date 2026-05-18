"""
Telemetría heartbeat vendedores (app móvil → panel operaciones).
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any

from backend.db import get_connection
from backend.utils.ruta_zonas import haversine_m

logger = logging.getLogger(__name__)

_ONLINE_MINUTES = float(os.getenv("OPERACIONES_HEARTBEAT_ONLINE_MINUTES", "2"))
_ATRASADO_MINUTES = float(os.getenv("OPERACIONES_HEARTBEAT_ATRASADO_MINUTES", "10"))


@dataclass(frozen=True)
class HeartbeatSnapshot:
    vendedor_id: str
    last_timestamp: datetime
    lat: float | None
    lng: float | None
    bateria: int | None
    conexion: str | None
    pendientes: int | None
    km_metros: float
    app_version: str | None
    dispositivo: str | None


def _ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def estado_conexion_desde_heartbeat(
    last_timestamp: datetime | None,
    *,
    fecha_operativa: date,
) -> str | None:
    """
    ``activo`` (online), ``atrasado`` o ``offline``.
    None si no hay heartbeat (usar fallback legacy).
    """
    if last_timestamp is None:
        return None
    ts = _ensure_utc(last_timestamp)
    hoy = date.today()
    if fecha_operativa < hoy:
        return "offline"
    if fecha_operativa > hoy:
        return "offline"

    age = datetime.now(timezone.utc) - ts
    online_td = timedelta(minutes=_ONLINE_MINUTES)
    atrasado_td = timedelta(minutes=_ATRASADO_MINUTES)

    if age <= online_td:
        estado = "activo"
    elif age <= atrasado_td:
        estado = "atrasado"
    else:
        estado = "offline"

    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(
            "heartbeat estado vendedor ts=%s age_sec=%.0f → %s (online<%sm atrasado<%sm)",
            ts.isoformat(),
            age.total_seconds(),
            estado,
            _ONLINE_MINUTES,
            _ATRASADO_MINUTES,
        )
    return estado


def km_desde_puntos(puntos: list[tuple[float, float]]) -> float:
    """Suma Haversine entre puntos consecutivos con coordenadas válidas."""
    if len(puntos) < 2:
        return 0.0
    total = 0.0
    for i in range(1, len(puntos)):
        lat1, lon1 = puntos[i - 1]
        lat2, lon2 = puntos[i]
        total += haversine_m(lat1, lon1, lat2, lon2)
    return total


def _km_por_vendedor(rows: list[tuple]) -> dict[str, float]:
    """rows: (vendedor_id, lat, lng, timestamp ordenado)."""
    by_v: dict[str, list[tuple[float, float]]] = {}
    for vid, lat, lng, _ts in rows:
        if lat is None or lng is None:
            continue
        by_v.setdefault(str(vid), []).append((float(lat), float(lng)))
    out: dict[str, float] = {}
    for vid, pts in by_v.items():
        km = km_desde_puntos(pts)
        out[vid] = km
        if logger.isEnabledFor(logging.DEBUG) and km > 0:
            logger.debug("heartbeat km vendedor=%s puntos=%s metros=%.0f", vid, len(pts), km)
    return out


def _merge_gps_track_into_snapshots(
    out: dict[str, HeartbeatSnapshot],
    fecha: date,
    cur,
) -> dict[str, HeartbeatSnapshot]:
    """Enriquece con ``operaciones_gps_track`` (km preferente, GPS más reciente)."""
    try:
        from backend.services.gps_track_service import load_day_stats

        gps_map = load_day_stats(cur, fecha)
    except Exception as e:
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("gps_track snapshots no disponibles: %s", e)
        return out

    for vid, gps in gps_map.items():
        existing = out.get(vid)
        use_gps_pos = existing is None or gps.last_timestamp >= existing.last_timestamp
        km = gps.km_metros if gps.km_metros > 0 else (existing.km_metros if existing else 0.0)
        if existing is None:
            out[vid] = HeartbeatSnapshot(
                vendedor_id=vid,
                last_timestamp=gps.last_timestamp,
                lat=gps.lat,
                lng=gps.lng,
                bateria=gps.battery,
                conexion=None,
                pendientes=None,
                km_metros=km,
                app_version=gps.app_version,
                dispositivo=None,
            )
        else:
            out[vid] = HeartbeatSnapshot(
                vendedor_id=vid,
                last_timestamp=gps.last_timestamp if use_gps_pos else existing.last_timestamp,
                lat=gps.lat if use_gps_pos else existing.lat,
                lng=gps.lng if use_gps_pos else existing.lng,
                bateria=gps.battery if gps.battery is not None else existing.bateria,
                conexion=existing.conexion,
                pendientes=existing.pendientes,
                km_metros=km,
                app_version=gps.app_version or existing.app_version,
                dispositivo=existing.dispositivo,
            )
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                "telemetría merge vendedor=%s gps_puntos=%s km_m=%.0f",
                vid,
                gps.point_count,
                km,
            )
    return out


def load_snapshots(cur, fecha: date) -> dict[str, HeartbeatSnapshot]:
    """Último heartbeat del día + km (heartbeat + gps_track, preferir gps_track)."""
    day_start = fecha
    cur.execute(
        """
        SELECT DISTINCT ON (vendedor_id)
            vendedor_id, timestamp, lat, lng, bateria, conexion, pendientes,
            app_version, dispositivo
        FROM bsale.operaciones_heartbeat
        WHERE timestamp >= %s::date
          AND timestamp < (%s::date + interval '1 day')
        ORDER BY vendedor_id, timestamp DESC
        """,
        (day_start, day_start),
    )
    latest = {str(r[0]): r for r in cur.fetchall()}

    cur.execute(
        """
        SELECT vendedor_id, lat, lng, timestamp
        FROM bsale.operaciones_heartbeat
        WHERE timestamp >= %s::date
          AND timestamp < (%s::date + interval '1 day')
          AND lat IS NOT NULL AND lng IS NOT NULL
        ORDER BY vendedor_id, timestamp ASC
        """,
        (day_start, day_start),
    )
    km_map = _km_por_vendedor(cur.fetchall())

    out: dict[str, HeartbeatSnapshot] = {}
    for vid, row in latest.items():
        out[vid] = HeartbeatSnapshot(
            vendedor_id=vid,
            last_timestamp=_ensure_utc(row[1]),
            lat=float(row[2]) if row[2] is not None else None,
            lng=float(row[3]) if row[3] is not None else None,
            bateria=int(row[4]) if row[4] is not None else None,
            conexion=str(row[5]) if row[5] else None,
            pendientes=int(row[6]) if row[6] is not None else None,
            km_metros=km_map.get(vid, 0.0),
            app_version=str(row[7]) if row[7] else None,
            dispositivo=str(row[8]) if row[8] else None,
        )
    return _merge_gps_track_into_snapshots(out, fecha, cur)


_table_ready = False


def ensure_heartbeat_table(cur) -> None:
    """Crea la tabla si no existe (idempotente; útil si falta migración manual)."""
    global _table_ready
    if _table_ready:
        return
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS bsale.operaciones_heartbeat (
          id BIGSERIAL PRIMARY KEY,
          vendedor_id VARCHAR(64) NOT NULL,
          timestamp TIMESTAMPTZ NOT NULL,
          lat DOUBLE PRECISION NULL,
          lng DOUBLE PRECISION NULL,
          bateria SMALLINT NULL,
          conexion VARCHAR(32) NULL,
          pendientes INTEGER NULL,
          app_version VARCHAR(64) NULL,
          dispositivo VARCHAR(128) NULL,
          created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
        )
        """,
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_operaciones_heartbeat_vendedor_ts
          ON bsale.operaciones_heartbeat (vendedor_id, timestamp DESC)
        """,
    )
    _table_ready = True


def insert_heartbeat(
    *,
    vendedor_id: str,
    timestamp: datetime,
    lat: float | None,
    lng: float | None,
    bateria: int | None,
    conexion: str | None,
    pendientes: int | None,
    app_version: str | None,
    dispositivo: str | None,
) -> int:
    vid = vendedor_id.strip()
    if not vid:
        raise ValueError("vendedor_id vacío")

    ts = _ensure_utc(timestamp)

    conn = get_connection()
    try:
        cur = conn.cursor()
        ensure_heartbeat_table(cur)
        cur.execute(
            """
            SELECT 1 FROM bsale.vendedores_app
            WHERE codigo = %s AND tipo_usuario = 'vendedor'
            LIMIT 1
            """,
            (vid,),
        )
        if not cur.fetchone():
            cur.close()
            raise ValueError(f"Vendedor no encontrado: {vid}")

        cur.execute(
            """
            INSERT INTO bsale.operaciones_heartbeat (
                vendedor_id, timestamp, lat, lng, bateria, conexion,
                pendientes, app_version, dispositivo
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                vid,
                ts,
                lat,
                lng,
                bateria,
                (conexion or "").strip()[:32] or None,
                pendientes,
                (app_version or "").strip()[:64] or None,
                (dispositivo or "").strip()[:128] or None,
            ),
        )
        row = cur.fetchone()
        hb_id = int(row[0])
        conn.commit()
        cur.close()
    finally:
        conn.close()

    logger.info(
        "heartbeat recibido id=%s vendedor=%s ts=%s bateria=%s pendientes=%s",
        hb_id,
        vid,
        ts.isoformat(),
        bateria,
        pendientes,
    )
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(
            "heartbeat detalle vendedor=%s lat=%s lng=%s conexion=%s app=%s device=%s",
            vid,
            lat,
            lng,
            conexion,
            app_version,
            dispositivo,
        )
    return hb_id


def latest_for_vendedor(codigo: str, fecha: date | None = None) -> HeartbeatSnapshot | None:
    conn = get_connection()
    try:
        cur = conn.cursor()
        if fecha:
            snaps = load_snapshots(cur, fecha)
            cur.close()
            return snaps.get(codigo.strip())
        cur.execute(
            """
            SELECT vendedor_id, timestamp, lat, lng, bateria, conexion, pendientes,
                   app_version, dispositivo
            FROM bsale.operaciones_heartbeat
            WHERE vendedor_id = %s
            ORDER BY timestamp DESC
            LIMIT 1
            """,
            (codigo.strip(),),
        )
        row = cur.fetchone()
        cur.close()
    finally:
        conn.close()
    if not row:
        return None
    return HeartbeatSnapshot(
        vendedor_id=str(row[0]),
        last_timestamp=_ensure_utc(row[1]),
        lat=float(row[2]) if row[2] is not None else None,
        lng=float(row[3]) if row[3] is not None else None,
        bateria=int(row[4]) if row[4] is not None else None,
        conexion=str(row[5]) if row[5] else None,
        pendientes=int(row[6]) if row[6] is not None else None,
        km_metros=0.0,
        app_version=str(row[7]) if row[7] else None,
        dispositivo=str(row[8]) if row[8] else None,
    )
