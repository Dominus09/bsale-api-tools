"""Auditoría bsale.rutero_georef_historial."""

from __future__ import annotations

import logging
from typing import Any

from backend.db import get_connection

logger = logging.getLogger(__name__)

_table_ready = False


def ensure_historial_table(cur) -> None:
    global _table_ready
    if _table_ready:
        return
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS bsale.rutero_georef_historial (
            id BIGSERIAL PRIMARY KEY,
            ruta_id INTEGER NOT NULL,
            estado_anterior VARCHAR(30),
            estado_nuevo VARCHAR(30) NOT NULL,
            lat DOUBLE PRECISION,
            lon DOUBLE PRECISION,
            usuario VARCHAR(50),
            fecha TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            motivo TEXT
        )
        """,
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_rutero_georef_hist_ruta
            ON bsale.rutero_georef_historial (ruta_id, fecha DESC)
        """,
    )
    _table_ready = True


def registrar_cambio(
    cur,
    *,
    ruta_id: int,
    estado_anterior: str | None,
    estado_nuevo: str,
    lat: float | None,
    lon: float | None,
    usuario: str,
    motivo: str | None = None,
) -> None:
    ensure_historial_table(cur)
    cur.execute(
        """
        INSERT INTO bsale.rutero_georef_historial (
            ruta_id, estado_anterior, estado_nuevo, lat, lon, usuario, motivo
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (
            ruta_id,
            (estado_anterior or "").strip()[:30] or None,
            estado_nuevo.strip()[:30],
            lat,
            lon,
            (usuario or "sistema").strip()[:50],
            (motivo or "").strip()[:2000] or None,
        ),
    )


def list_historial(ruta_id: int, limit: int = 50) -> list[dict[str, Any]]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        ensure_historial_table(cur)
        cur.execute(
            """
            SELECT
                id, ruta_id, estado_anterior, estado_nuevo,
                lat, lon, usuario, fecha, motivo
            FROM bsale.rutero_georef_historial
            WHERE ruta_id = %s
            ORDER BY fecha DESC, id DESC
            LIMIT %s
            """,
            (ruta_id, min(max(limit, 1), 200)),
        )
        cols = [c[0] for c in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        cur.close()
        return rows
    finally:
        conn.close()
