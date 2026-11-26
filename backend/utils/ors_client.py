import logging
import os

import requests

from backend.utils.config import ORS_API_KEY
from backend.utils.ruta_zonas import haversine_m

BASE_URL = "https://api.openrouteservice.org/v2/directions/driving-car"
logger = logging.getLogger(__name__)

# Cada tramo debe terminar antes de que el proxy del ERP alcance su timeout.
# Sin timeout, una llamada ORS puede bloquear indefinidamente un worker FastAPI.
ORS_CONNECT_TIMEOUT_S = 8
ORS_READ_TIMEOUT_S = 35


class OrsServiceError(RuntimeError):
    """Error seguro para logs: nunca incluye HTML, token ni payload remoto."""


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

    try:
        response = requests.post(
            BASE_URL,
            json=body,
            headers=headers,
            timeout=(ORS_CONNECT_TIMEOUT_S, ORS_READ_TIMEOUT_S),
        )
    except requests.exceptions.Timeout as exc:
        logger.warning("ORS_REQUEST_FAILED kind=timeout waypoints=%s", len(coordinates))
        raise OrsServiceError("ORS excedió el tiempo máximo de respuesta.") from exc
    except requests.exceptions.RequestException as exc:
        logger.warning("ORS_REQUEST_FAILED kind=connection waypoints=%s", len(coordinates))
        raise OrsServiceError("No fue posible conectar con el servicio ORS.") from exc

    # Nunca propagar response.text: proveedores y proxies pueden devolver HTML
    # completo (como la página 502 de Cloudflare) o información interna.
    if response.status_code != 200:
        status = response.status_code
        remote_code = None
        try:
            payload = response.json()
            err = payload.get("error") if isinstance(payload, dict) else None
            value = err.get("code") if isinstance(err, dict) else None
            if type(value) is int and 0 <= value <= 99999:
                remote_code = value
        except (ValueError, TypeError):
            pass
        category = ("quota" if status == 429 else "authorization" if status in (401, 403)
                    else "upstream" if status >= 500 else "request_rejected")
        logger.warning("ORS_REQUEST_FAILED kind=%s http_status=%s ors_code=%s waypoints=%s key_configured=%s",
                       category, status, remote_code, len(coordinates), bool(ORS_API_KEY))
        if status == 429:
            reason = "ORS alcanzó su límite temporal de solicitudes."
        elif status in (401, 403):
            reason = "ORS rechazó la autorización configurada."
        elif status >= 500:
            reason = "ORS no está disponible temporalmente."
        else:
            reason = "ORS rechazó el cálculo solicitado."
        raise OrsServiceError(f"{reason} (HTTP {status}).")

    try:
        data = response.json()
    except ValueError as exc:
        logger.warning("ORS_REQUEST_FAILED kind=invalid_json http_status=200 waypoints=%s", len(coordinates))
        raise OrsServiceError("ORS devolvió una respuesta no JSON.") from exc
    if not isinstance(data, dict) or not isinstance(data.get("routes"), list) or not data["routes"]:
        logger.warning("ORS_REQUEST_FAILED kind=empty_routes http_status=200 waypoints=%s", len(coordinates))
        raise OrsServiceError("ORS devolvió una respuesta sin rutas válidas.")
    _debug_log_distancia_despues(coordinates, data)
    return data
