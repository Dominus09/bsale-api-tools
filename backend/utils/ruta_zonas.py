"""
Agrupación geográfica de clientes para optimización de ruta por tramos (ORS).

K-means en (lon, lat) es suficiente para zonas locales; el orden de grupos respecto
a la base usa distancia Haversine al centroide.
"""

from __future__ import annotations

import math
import random
def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Distancia en metros entre dos puntos WGS84."""
    r = 6_371_000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlmb / 2) ** 2
    return 2 * r * math.asin(min(1.0, math.sqrt(a)))


def _dist_sq_xy(a: tuple[float, float], b: tuple[float, float]) -> float:
    dx = a[0] - b[0]
    dy = a[1] - b[1]
    return dx * dx + dy * dy


def _kmeans_labels_ll(
    points: list[tuple[float, float]],
    k: int,
    *,
    max_iter: int = 50,
    seed: int = 42,
) -> list[int]:
    """
    K-means en el plano lon–lat (suficiente para clusters ~comuna).
    Devuelve una etiqueta 0..k-1 por punto.
    """
    n = len(points)
    if n == 0:
        return []
    if k <= 1:
        return [0] * n
    k = min(k, n)
    rng = random.Random(seed)

    # Inicialización: k índices distintos
    pick = list(range(n))
    rng.shuffle(pick)
    centroids = [points[pick[i]] for i in range(k)]

    labels = [0] * n
    for _ in range(max_iter):
        changed = False
        for i, p in enumerate(points):
            best_j = 0
            best_d = float("inf")
            for j, c in enumerate(centroids):
                d = _dist_sq_xy(p, c)
                if d < best_d:
                    best_d = d
                    best_j = j
            if labels[i] != best_j:
                labels[i] = best_j
                changed = True

        new_c: list[tuple[float, float]] = []
        for j in range(k):
            members = [points[i] for i in range(n) if labels[i] == j]
            if not members:
                new_c.append(centroids[j])
            else:
                mx = sum(m[0] for m in members) / len(members)
                my = sum(m[1] for m in members) / len(members)
                new_c.append((mx, my))
        centroids = new_c
        if not changed:
            break

    return labels


def elegir_num_zonas(n: int) -> int:
    """k=2 o k=3 según tamaño (solo se usa con n > umbral en el router)."""
    if n < 30:
        return 2
    return 3


def agrupar_clientes_por_zona_kmeans(clientes: list[dict], k: int) -> list[list[dict]]:
    """Parte `clientes` en k grupos por cercanía geográfica (K-means lon/lat)."""
    if not clientes:
        return []
    if k <= 1:
        return [list(clientes)]

    pts: list[tuple[float, float]] = []
    for c in clientes:
        pts.append((float(c["lon"]), float(c["lat"])))

    labels = _kmeans_labels_ll(pts, k)
    buckets: list[list[dict]] = [[] for _ in range(k)]
    for c, lab in zip(clientes, labels):
        buckets[lab].append(c)

    return [g for g in buckets if g]


def ordenar_grupos_por_cercania_a_base(grupos: list[list[dict]], base: dict) -> list[list[dict]]:
    """Grupos más cercanos a la base primero (centroide del grupo vs base)."""
    blat = float(base["lat"])
    blon = float(base["lon"])

    def score(grupo: list[dict]) -> float:
        if not grupo:
            return float("inf")
        alat = sum(float(c["lat"]) for c in grupo) / len(grupo)
        alon = sum(float(c["lon"]) for c in grupo) / len(grupo)
        return haversine_m(blat, blon, alat, alon)

    return sorted(grupos, key=score)


def ordenar_clientes_en_grupo_por_distancia_a_base(grupo: list[dict], base: dict) -> list[dict]:
    blat = float(base["lat"])
    blon = float(base["lon"])
    return sorted(
        grupo,
        key=lambda c: haversine_m(blat, blon, float(c["lat"]), float(c["lon"])),
    )
