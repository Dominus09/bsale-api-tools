"""
Cálculo de distancias geográficas (Haversine) para validación de visitas.
"""

from __future__ import annotations

import math

# Umbral de negocio: visita dentro de este radio respecto al punto de referencia del cliente (metros)
UMBRAL_VALIDACION_METROS = 300


def distancia_metros_haversine(
    lat1: float,
    lon1: float,
    lat2: float,
    lon2: float,
) -> float:
    """
    Distancia en metros entre dos puntos WGS84 (lat/lon en grados decimales).
    """
    r = 6371000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = (
        math.sin(dphi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(max(0.0, 1.0 - a)))
    return r * c


def _coord_valida(lat, lon) -> bool:
    if lat is None or lon is None:
        return False
    try:
        la = float(lat)
        lo = float(lon)
    except (TypeError, ValueError):
        return False
    return -90.0 <= la <= 90.0 and -180.0 <= lo <= 180.0


def coordenadas_visita_validas(lat_visita, lon_visita) -> bool:
    """True si lat/lon de la visita son usables para validación Haversine (API app)."""
    return _coord_valida(lat_visita, lon_visita)


def distancia_y_estado_validacion(
    lat_cliente,
    lon_cliente,
    lat_visita,
    lon_visita,
) -> tuple[float | None, str]:
    """
    Calcula distancia cliente ↔ visita y el valor de validacion_estado para persistir en BD.

    Reglas:
    - Coordenadas completas en ambos puntos: distancia Haversine;
      <= 300 m → 'validado', > 300 m → 'fuera_rango'
    - Falta alguna coordenada o no son válidas → sin_gps, distancia None
    """
    if not (
        _coord_valida(lat_cliente, lon_cliente)
        and _coord_valida(lat_visita, lon_visita)
    ):
        return None, "sin_gps"

    d = distancia_metros_haversine(
        float(lat_cliente),
        float(lon_cliente),
        float(lat_visita),
        float(lon_visita),
    )
    d_redondeada = round(d, 2)
    if d_redondeada <= UMBRAL_VALIDACION_METROS:
        return d_redondeada, "validado"
    return d_redondeada, "fuera_rango"
