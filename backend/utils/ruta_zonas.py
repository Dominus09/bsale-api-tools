"""
Pipeline de optimización de ruta por etapas (preorden radial, K-means, ORS por grupo).

1. Preordenamiento radial respecto a la base (atan2).
2. K-means en (lon, lat) con k ∈ {2, 3, 4} según volumen; cada cliente lleva `cluster_id`.
3. Agrupación por cluster y orden de grupos por cercanía del centroide a la base.
4. ORS por grupo (entrada ordenada radialmente dentro del grupo); unión de visitas.
5. La métrica y geometría finales de la ruta completa las calcula el router con ORS
   secuencial BASE → ruta_final → BASE (`_geom_km_ruta_completa`).
"""

from __future__ import annotations

import math
import random
from collections import defaultdict


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Distancia en metros entre dos puntos WGS84."""
    r = 6_371_000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlmb / 2) ** 2
    return 2 * r * math.asin(min(1.0, math.sqrt(a)))


def angulo_radial_desde_base(c: dict, blat: float, blon: float) -> float:
    """Ángulo en plano lat/lon respecto a la base (mismo criterio que atan2 del usuario)."""
    return math.atan2(float(c["lat"]) - blat, float(c["lon"]) - blon)


def preordenar_radial_clientes(clientes: list[dict], base: dict) -> list[dict]:
    """Paso 1: copia la lista ordenada por ángulo atan2(lat - base.lat, lon - base.lon)."""
    blat = float(base["lat"])
    blon = float(base["lon"])
    return sorted((dict(c) for c in clientes), key=lambda c: angulo_radial_desde_base(c, blat, blon))


def elegir_k_clusters(n: int) -> int:
    """Paso 2: k ∈ {2, 3, 4} según cantidad de clientes."""
    if n < 16:
        return 2
    if n < 30:
        return 3
    return min(4, max(2, n // 10))


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
    """K-means en lon–lat; etiquetas 0..k-1."""
    n = len(points)
    if n == 0:
        return []
    if k <= 1:
        return [0] * n
    k = min(k, n)
    rng = random.Random(seed)
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


def clientes_con_cluster_kmeans(clientes_radial: list[dict], k: int) -> list[dict]:
    """
    Pasos 2–3: K-means sobre [lon, lat] en el orden radial; añade `cluster_id` a cada fila.
    """
    if not clientes_radial:
        return []
    if k <= 1:
        return [{**dict(c), "cluster_id": 0} for c in clientes_radial]

    pts = [(float(c["lon"]), float(c["lat"])) for c in clientes_radial]
    labels = _kmeans_labels_ll(pts, k)
    return [{**dict(c), "cluster_id": int(lab)} for c, lab in zip(clientes_radial, labels)]


def ordenar_grupos_por_cercania_a_base(grupos: list[list[dict]], base: dict) -> list[list[dict]]:
    """Paso 4: clusters más cercanos a la base primero (centroide vs base)."""
    blat = float(base["lat"])
    blon = float(base["lon"])

    def score(grupo: list[dict]) -> float:
        if not grupo:
            return float("inf")
        alat = sum(float(c["lat"]) for c in grupo) / len(grupo)
        alon = sum(float(c["lon"]) for c in grupo) / len(grupo)
        return haversine_m(blat, blon, alat, alon)

    return sorted(grupos, key=score)


def listas_grupos_cluster_ordenados(clientes_tagged: list[dict], base: dict) -> list[list[dict]]:
    """Agrupa por `cluster_id` y ordena los grupos por distancia del centroide a la base."""
    by_cluster: dict[int, list[dict]] = defaultdict(list)
    for c in clientes_tagged:
        by_cluster[int(c["cluster_id"])].append(c)
    grupos = list(by_cluster.values())
    return ordenar_grupos_por_cercania_a_base(grupos, base)


def ordenar_grupo_radial_desde_base(grupo: list[dict], base: dict) -> list[dict]:
    """Orden radial dentro del grupo (semilla para la petición ORS del paso 5)."""
    blat = float(base["lat"])
    blon = float(base["lon"])
    return sorted(grupo, key=lambda c: angulo_radial_desde_base(c, blat, blon))
