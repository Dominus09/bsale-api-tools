"""
GPS track points (app móvil POST /operaciones/gps_track).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, timezone

from backend.db import get_connection
from backend.services.heartbeat_service import _ensure_utc, km_desde_puntos

logger = logging.getLogger(__name__)

_table_ready = False


@dataclass(frozen=True)
class GpsTrackDayStats:
    vendedor_id: str
    last_timestamp: datetime
    lat: float
    lng: float
    battery: int | None
    app_version: str | None
    km_metros: float
    point_count: int


def ensure_gps_track_table(cur) -> None:
    global _table_ready
    if _table_ready:
        return
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS bsale.operaciones_gps_track (
          id BIGSERIAL PRIMARY KEY,
          vendedor_id VARCHAR(64) NOT NULL,
          timestamp TIMESTAMPTZ NOT NULL,
          lat DOUBLE PRECISION NOT NULL,
          lng DOUBLE PRECISION NOT NULL,
          accuracy DOUBLE PRECISION NULL,
          speed DOUBLE PRECISION NULL,
          battery SMALLINT NULL,
          app_version VARCHAR(64) NULL,
          created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
        )
        """,
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_operaciones_gps_track_vendedor_ts
          ON bsale.operaciones_gps_track (vendedor_id, timestamp DESC)
        """,
    )
    _table_ready = True


def _validate_vendedor(cur, vendedor_id: str) -> None:
    cur.execute(
        """
        SELECT 1 FROM bsale.vendedores_app
        WHERE codigo = %s AND tipo_usuario = 'vendedor'
        LIMIT 1
        """,
        (vendedor_id,),
    )
    if not cur.fetchone():
        raise ValueError(f"Vendedor no encontrado: {vendedor_id}")


def insert_gps_track(
    *,
    vendedor_id: str,
    timestamp: datetime,
    lat: float,
    lng: float,
    accuracy: float | None = None,
    speed: float | None = None,
    battery: int | None = None,
    app_version: str | None = None,
) -> int:
    vid = vendedor_id.strip()
    if not vid:
        raise ValueError("vendedor_id vacío")
    if lat is None or lng is None:
        raise ValueError("lat y lng son obligatorios para gps_track")

    ts = _ensure_utc(timestamp)

    conn = get_connection()
    try:
        cur = conn.cursor()
        ensure_gps_track_table(cur)
        _validate_vendedor(cur, vid)
        cur.execute(
            """
            INSERT INTO bsale.operaciones_gps_track (
                vendedor_id, timestamp, lat, lng, accuracy, speed, battery, app_version
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                vid,
                ts,
                float(lat),
                float(lng),
                float(accuracy) if accuracy is not None else None,
                float(speed) if speed is not None else None,
                battery,
                (app_version or "").strip()[:64] or None,
            ),
        )
        row = cur.fetchone()
        track_id = int(row[0])
        conn.commit()
        cur.close()
    finally:
        conn.close()

    logger.info(
        "[GPS-Track] SQL insert OK id=%s vendedor=%s ts=%s lat=%s lng=%s battery=%s",
        track_id,
        vid,
        ts.isoformat(),
        lat,
        lng,
        battery,
    )
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(
            "gps_track detalle vendedor=%s accuracy=%s speed=%s app=%s",
            vid,
            accuracy,
            speed,
            app_version,
        )
    return track_id


def insert_gps_track_batch(
    *,
    vendedor_id: str,
    default_timestamp: datetime,
    puntos: list,
    battery: int | None = None,
    app_version: str | None = None,
) -> int:
    """
    Inserta múltiples puntos en una transacción.
    ``puntos``: elementos con ``lat``, ``lng_efectivo()``, ``accuracy_m``, ``timestamp``, ``speed``.
    """
    vid = vendedor_id.strip()
    if not vid:
        raise ValueError("vendedor_id vacío")
    if not puntos:
        raise ValueError("puntos[] vacío")

    conn = get_connection()
    insertados = 0
    try:
        cur = conn.cursor()
        ensure_gps_track_table(cur)
        _validate_vendedor(cur, vid)
        for p in puntos:
            lat = float(p.lat)
            lng = float(p.lng_efectivo())
            ts = _ensure_utc(p.timestamp if p.timestamp is not None else default_timestamp)
            acc = getattr(p, "accuracy_m", None)
            if acc is None:
                acc = getattr(p, "accuracy", None)
            spd = getattr(p, "speed", None)
            cur.execute(
                """
                INSERT INTO bsale.operaciones_gps_track (
                    vendedor_id, timestamp, lat, lng, accuracy, speed, battery, app_version
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    vid,
                    ts,
                    lat,
                    lng,
                    float(acc) if acc is not None else None,
                    float(spd) if spd is not None else None,
                    battery,
                    (app_version or "").strip()[:64] or None,
                ),
            )
            insertados += 1
        conn.commit()
        cur.close()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    logger.info(
        "[GPS-Track] SQL batch OK vendedor=%s insertados=%s",
        vid,
        insertados,
    )
    return insertados


def load_day_stats(cur, fecha: date) -> dict[str, GpsTrackDayStats]:
    """Último punto y km Haversine del día por vendedor."""
    day_start = fecha
    cur.execute(
        """
        SELECT DISTINCT ON (vendedor_id)
            vendedor_id, timestamp, lat, lng, battery, app_version
        FROM bsale.operaciones_gps_track
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
        FROM bsale.operaciones_gps_track
        WHERE timestamp >= %s::date
          AND timestamp < (%s::date + interval '1 day')
        ORDER BY vendedor_id, timestamp ASC
        """,
        (day_start, day_start),
    )
    by_v: dict[str, list[tuple[float, float]]] = {}
    counts: dict[str, int] = {}
    for vid, lat, lng, _ts in cur.fetchall():
        vid_s = str(vid)
        by_v.setdefault(vid_s, []).append((float(lat), float(lng)))
        counts[vid_s] = counts.get(vid_s, 0) + 1

    out: dict[str, GpsTrackDayStats] = {}
    for vid, row in latest.items():
        pts = by_v.get(vid, [])
        km = km_desde_puntos(pts)
        if logger.isEnabledFor(logging.DEBUG) and km > 0:
            logger.debug("gps_track km vendedor=%s puntos=%s metros=%.0f", vid, len(pts), km)
        out[vid] = GpsTrackDayStats(
            vendedor_id=vid,
            last_timestamp=_ensure_utc(row[1]),
            lat=float(row[2]),
            lng=float(row[3]),
            battery=int(row[4]) if row[4] is not None else None,
            app_version=str(row[5]) if row[5] else None,
            km_metros=km,
            point_count=counts.get(vid, 0),
        )
    return out


def km_for_vendedor_day(cur, vendedor_id: str, fecha: date) -> tuple[float, int]:
    """Metros recorridos y cantidad de puntos GPS del día (Haversine)."""
    ensure_gps_track_table(cur)
    vid = vendedor_id.strip()
    cur.execute(
        """
        SELECT lat, lng
        FROM bsale.operaciones_gps_track
        WHERE vendedor_id = %s
          AND timestamp >= %s::date
          AND timestamp < (%s::date + interval '1 day')
        ORDER BY timestamp ASC
        """,
        (vid, fecha, fecha),
    )
    pts = [(float(r[0]), float(r[1])) for r in cur.fetchall()]
    return km_desde_puntos(pts), len(pts)
