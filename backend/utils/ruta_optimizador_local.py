"""
Optimización local del orden de visitas (ORS solo traza después).

Pipeline:
1. Preorden por sectores angulares (bins de atan2) y, dentro de cada sector, por distancia a la base.
2. 2-opt con coste penalizado: aristas > umbral km reciben coste extra (evita saltos largos artificiales).

Colas / anclas: preorden angular desde el último punto fijo + 2-opt abierto con el mismo coste penalizado.
"""

from __future__ import annotations

import logging
import math
import os
from typing import TYPE_CHECKING

from backend.utils.ruta_zonas import angulo_radial_desde_base, haversine_m

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)

_N_SECTORES = max(4, min(16, int(os.getenv("RUTA_SECTORES_ANGULO", "8"))))


def _umbral_salto_m() -> float:
    return float(os.getenv("RUTA_UMBRAL_SALTO_KM", "20")) * 1000.0


def _peso_penal_salto() -> float:
    return float(os.getenv("RUTA_PESO_PENAL_SALTO", "2"))


def _lat_lon(c: dict) -> tuple[float, float]:
    return float(c["lat"]), float(c["lon"])


def _dist_m(a: dict, b: dict) -> float:
    la, lo = _lat_lon(a)
    lb, lo2 = _lat_lon(b)
    return haversine_m(la, lo, lb, lo2)


def _edge_cost_penalizado(a: dict, b: dict) -> float:
    """Coste de arista en metros: distancia + penalización por salto > umbral."""
    d = _dist_m(a, b)
    um = _umbral_salto_m()
    exceso = max(0.0, d - um)
    return d + exceso * _peso_penal_salto()


def preordenar_sectores_angular_distancia(clientes: list[dict], base: dict) -> list[dict]:
    """
    Agrupa por sector de ángulo alrededor de la base; dentro del sector ordena por distancia creciente.
    """
    blat = float(base["lat"])
    blon = float(base["lon"])
    nsec = _N_SECTORES
    two_pi = 2 * math.pi
    items: list[tuple[int, float, float, dict]] = []
    for c in clientes:
        dc = dict(c)
        ang = angulo_radial_desde_base(dc, blat, blon)
        sector = int(((ang + math.pi) / two_pi) * nsec)
        if sector >= nsec:
            sector = nsec - 1
        if sector < 0:
            sector = 0
        dm = haversine_m(blat, blon, float(dc["lat"]), float(dc["lon"]))
        items.append((sector, ang, dm, dc))
    items.sort(key=lambda x: (x[0], x[1], x[2]))
    return [t[3] for t in items]


def _angulo_desde_punto(c: dict, plat: float, plon: float) -> float:
    return math.atan2(float(c["lat"]) - plat, float(c["lon"]) - plon)


def preordenar_desde_ancla(ancla: dict, clientes: list[dict]) -> list[dict]:
    """Cola: orden polar desde la ancla + distancia a la ancla."""
    plat, plon = float(ancla["lat"]), float(ancla["lon"])
    items: list[tuple[float, float, dict]] = []
    for c in clientes:
        dc = dict(c)
        ang = _angulo_desde_punto(dc, plat, plon)
        dm = haversine_m(plat, plon, float(dc["lat"]), float(dc["lon"]))
        items.append((ang, dm, dc))
    items.sort(key=lambda x: (x[0], x[1]))
    return [t[2] for t in items]


def tour_length_closed(base: dict, path: list[dict]) -> float:
    """Metros geodésicos (sin penalizar)."""
    if not path:
        return 0.0
    t = _dist_m(base, path[0])
    for i in range(len(path) - 1):
        t += _dist_m(path[i], path[i + 1])
    t += _dist_m(path[-1], base)
    return t


def tour_cost_closed_penalizado(base: dict, path: list[dict]) -> float:
    """Coste para 2-opt (metros + penalización de saltos largos)."""
    if not path:
        return 0.0
    t = _edge_cost_penalizado(base, path[0])
    for i in range(len(path) - 1):
        t += _edge_cost_penalizado(path[i], path[i + 1])
    t += _edge_cost_penalizado(path[-1], base)
    return t


def tour_length_open(anchor: dict, path: list[dict], base: dict) -> float:
    if not path:
        return _dist_m(anchor, base)
    t = _dist_m(anchor, path[0])
    for i in range(len(path) - 1):
        t += _dist_m(path[i], path[i + 1])
    t += _dist_m(path[-1], base)
    return t


def tour_cost_open_penalizado(anchor: dict, path: list[dict], base: dict) -> float:
    if not path:
        return _edge_cost_penalizado(anchor, base)
    t = _edge_cost_penalizado(anchor, path[0])
    for i in range(len(path) - 1):
        t += _edge_cost_penalizado(path[i], path[i + 1])
    t += _edge_cost_penalizado(path[-1], base)
    return t


def dos_opt_cerrado(base: dict, path: list[dict], *, max_passes: int = 80) -> list[dict]:
    if len(path) < 4:
        return [dict(c) for c in path]
    pth = [dict(c) for c in path]
    n = len(pth)
    passes_cap = min(max_passes, max(20, 2000 // max(n, 1)))
    for _ in range(passes_cap):
        improved = False
        for i in range(0, n - 1):
            for j in range(i + 2, n):
                new_p = pth[: i + 1] + pth[i + 1 : j + 1][::-1] + pth[j + 1 :]
                if tour_cost_closed_penalizado(base, new_p) + 1e-6 < tour_cost_closed_penalizado(base, pth):
                    pth = new_p
                    improved = True
                    break
            if improved:
                break
        if not improved:
            break
    return pth


def dos_opt_abierto(anchor: dict, path: list[dict], base: dict, *, max_passes: int = 80) -> list[dict]:
    if len(path) < 2:
        return [dict(c) for c in path]
    pth = [dict(c) for c in path]
    n = len(pth)
    passes_cap = min(max_passes, max(20, 2000 // max(n, 1)))
    for _ in range(passes_cap):
        improved = False
        for i in range(0, n - 1):
            for j in range(i + 2, n):
                new_p = pth[: i + 1] + pth[i + 1 : j + 1][::-1] + pth[j + 1 :]
                if tour_cost_open_penalizado(anchor, new_p, base) + 1e-6 < tour_cost_open_penalizado(
                    anchor, pth, base
                ):
                    pth = new_p
                    improved = True
                    break
            if improved:
                break
        if not improved:
            break
    return pth


def optimizar_secuencia_cerrado(base: dict, clientes: list[dict]) -> tuple[list[dict], list[dict]]:
    if not clientes:
        return [], []
    inicial = preordenar_sectores_angular_distancia(clientes, base)
    optimizado = dos_opt_cerrado(base, inicial)
    return inicial, optimizado


def optimizar_cola_desde_ancla(
    prev: dict | None,
    despues: list[dict],
    base: dict,
) -> list[dict]:
    if not despues:
        return []
    if prev is None:
        _, opt = optimizar_secuencia_cerrado(base, despues)
        return opt
    inicial = preordenar_desde_ancla(prev, despues)
    return dos_opt_abierto(prev, inicial, base)


def log_resumen_optimizacion(
    *,
    base: dict,
    inicial: list[dict],
    optimizado: list[dict],
    km_ors: float,
    min_ors: float,
    bloque_k: int | None = None,
) -> None:
    ids_ini = [c.get("bsale_id") for c in inicial]
    ids_opt = [c.get("bsale_id") for c in optimizado]
    km_haversine = tour_length_closed(base, optimizado) / 1000.0
    logger.info(
        "ruta_opt total_clientes=%d bloque_hasta_indice=%s orden_inicial_bsale_ids=%s "
        "orden_optimizado_bsale_ids=%s km_ors=%.3f min_ors=%.1f km_haversine_cerrado=%.3f base=%s",
        len(optimizado),
        bloque_k,
        ids_ini,
        ids_opt,
        km_ors,
        min_ors,
        km_haversine,
        (base.get("nombre") or base.get("vendedor") or "")[:40],
    )
