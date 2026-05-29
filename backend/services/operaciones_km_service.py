"""Cálculo km GPS vs ruta planificada (visitas)."""

from __future__ import annotations

from backend.services.heartbeat_service import km_desde_puntos
from backend.services.operaciones_visitas import sql_in_estados_realizados

_ESTADOS_REALIZADOS = sql_in_estados_realizados()


def km_ruta_planificada(cur, ruta_id: int | None) -> float:
    """Metros según distancia_metros acumulada o cadena Haversine por orden de ruta."""
    if ruta_id is None:
        return 0.0

    cur.execute(
        f"""
        SELECT COALESCE(SUM(v.distancia_metros), 0)::float
        FROM bsale.visitas v
        WHERE v.ruta_id = %s
          AND v.distancia_metros IS NOT NULL
          AND v.estado IN ({_ESTADOS_REALIZADOS})
        """,
        (ruta_id,),
    )
    row = cur.fetchone()
    sum_m = float(row[0] or 0) if row else 0.0
    if sum_m > 0:
        return sum_m

    cur.execute(
        """
        SELECT lat_visita, lon_visita
        FROM bsale.visitas
        WHERE ruta_id = %s
          AND lat_visita IS NOT NULL
          AND lon_visita IS NOT NULL
        ORDER BY orden_ruta, id
        """,
        (ruta_id,),
    )
    pts: list[tuple[float, float]] = []
    for lat, lon in cur.fetchall():
        try:
            la, lo = float(lat), float(lon)
        except (TypeError, ValueError):
            continue
        if la == 0.0 and lo == 0.0:
            continue
        pts.append((la, lo))
    return km_desde_puntos(pts)
