import os

import requests

from backend.utils.config import ORS_API_KEY
from backend.utils.ruta_zonas import haversine_m

BASE_URL = "https://api.openrouteservice.org/v2/directions/driving-car"


def _ors_coords_debug_enabled() -> bool:
    return os.getenv("RUTA_DEBUG_ORS_COORDS", "").strip().lower() in ("1", "true", "yes")


def _coords_casi_iguales(a: list[float], b: list[float], eps: float = 1e-5) -> bool:
    return abs(float(a[0]) - float(b[0])) < eps and abs(float(a[1]) - float(b[1])) < eps


def _haversine_cadena_km(coordinates: list[list[float]]) -> float:
    """Suma de tramos rectos entre puntos consecutivos (cota inferior aprox. del recorrido real)."""
    if len(coordinates) < 2:
        return 0.0
    total_m = 0.0
    for i in range(len(coordinates) - 1):
        lon1, lat1 = float(coordinates[i][0]), float(coordinates[i][1])
        lon2, lat2 = float(coordinates[i + 1][0]), float(coordinates[i + 1][1])
        total_m += haversine_m(lat1, lon1, lat2, lon2)
    return total_m / 1000.0


def _debug_log_coords_antes(coordinates: list[list[float]]) -> None:
    if not _ors_coords_debug_enabled():
        return
    print("=== COORDENADAS ENVIADAS A ORS ===")
    for i, c in enumerate(coordinates):
        print(i, c)
    if coordinates:
        cerrado = _coords_casi_iguales(coordinates[0], coordinates[-1])
        print("circuito_cerrado (primera == última coordenada):", cerrado)


def _debug_log_distancia_despues(coordinates: list[list[float]], data: dict) -> None:
    if not _ors_coords_debug_enabled():
        return
    havers_km = _haversine_cadena_km(coordinates)
    routes = data.get("routes") or []
    ors_km: float | None = None
    if routes:
        sm = routes[0].get("summary") or {}
        dist_m = sm.get("distance")
        if dist_m is not None:
            ors_km = float(dist_m) / 1000.0
    print("=== COMPARACIÓN DISTANCIA (debug) ===")
    print(f"  Haversine suma tramos consecutivos (aprox.): {havers_km:.3f} km")
    print(f"  ORS summary distance: {ors_km if ors_km is not None else '—'} km")
    if ors_km is not None and ors_km < havers_km * 0.85:
        print(
            "  NOTA: ORS puede ser < Haversine (camino no geodésico); si ORS << Haversine, revisar coords."
        )


def get_route(coordinates: list[list[float]]) -> dict:
    """
    Petición POST a ORS Directions.

    Para una ruta diaria cerrada, los callers deben enviar en ``coordinates``:
    ``[base_lonlat, cliente1, ..., clienteN, base_lonlat]`` (ida y vuelta a base).

    ``coordinates``: ``[[lon, lat], ...]``
    """
    _debug_log_coords_antes(coordinates)

    headers = {
        "Authorization": ORS_API_KEY,
        "Content-Type": "application/json",
    }

    body = {"coordinates": coordinates}

    response = requests.post(BASE_URL, json=body, headers=headers)

    if response.status_code != 200:
        raise Exception(f"ORS error: {response.text}")

    data = response.json()
    _debug_log_distancia_despues(coordinates, data)
    return data
