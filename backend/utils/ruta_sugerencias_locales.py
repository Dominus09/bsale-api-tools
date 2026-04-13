"""
Sugerencias puntuales de mejora en el orden de ruta (no reemplaza la ruta completa).

Usa distancia Haversine entre visitas consecutivas (misma base que el optimizador local)
para estimar el efecto de intercambiar dos visitas adyacentes; ORS no se llama aquí.
"""

from __future__ import annotations

from backend.utils.ruta_zonas import haversine_m


def _nombre_cliente_fila(row: dict) -> str:
    fan = str(row.get("nombre_fantasia") or "").strip()
    if fan:
        return fan
    fn = str(row.get("first_name") or "").strip()
    ln = str(row.get("last_name") or "").strip()
    full = f"{fn} {ln}".strip()
    bid = row.get("bsale_id")
    return full or (f"Cliente #{bid}" if bid is not None else "Cliente")


def _m_tramo(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    return haversine_m(lat1, lon1, lat2, lon2)


def sugerencias_swap_adyacentes(
    base: dict,
    clientes_ordenados: list[dict],
    *,
    min_delta_km: float = 0.5,
) -> list[dict]:
    """
    Para cada par adyacente (A, B) en el orden actual, compara el coste local
    prev → A → B → next con prev → B → A → next (circuito con base en extremos).

    Si el intercambio reduce la distancia Haversine acumulada en esos tres tramos
    en al menos ``min_delta_km``, se emite una sugerencia (delta_km negativo).
    """
    n = len(clientes_ordenados)
    if n < 2:
        return []

    blat = float(base["lat"])
    blon = float(base["lon"])

    umbral = max(0.0, float(min_delta_km))
    out: list[dict] = []

    for i in range(n - 1):
        a = clientes_ordenados[i]
        b = clientes_ordenados[i + 1]
        alat, alon = float(a["lat"]), float(a["lon"])
        blat_b, blon_b = float(b["lat"]), float(b["lon"])

        if i == 0:
            plat, plon = blat, blon
        else:
            p = clientes_ordenados[i - 1]
            plat, plon = float(p["lat"]), float(p["lon"])

        if i + 2 >= n:
            nlat, nlon = blat, blon
        else:
            nx = clientes_ordenados[i + 2]
            nlat, nlon = float(nx["lat"]), float(nx["lon"])

        antes_m = (
            _m_tramo(plat, plon, alat, alon)
            + _m_tramo(alat, alon, blat_b, blon_b)
            + _m_tramo(blat_b, blon_b, nlat, nlon)
        )
        despues_m = (
            _m_tramo(plat, plon, blat_b, blon_b)
            + _m_tramo(blat_b, blon_b, alat, alon)
            + _m_tramo(alat, alon, nlat, nlon)
        )
        delta_m = despues_m - antes_m
        delta_km = delta_m / 1000.0

        if delta_km > -umbral:
            continue

        na = _nombre_cliente_fila(a)
        nb = _nombre_cliente_fila(b)
        oa = i + 1
        ob = i + 2
        ahorro = -delta_km
        mensaje = (
            f"Sugerencia: mover «{nb}» (visita {ob}) antes de «{na}» (visita {oa}); "
            f"ahorro aprox. {ahorro:.1f} km (Haversine en tramo local)"
        )

        out.append(
            {
                "id": f"swap_adyacente_{i}",
                "tipo": "swap_adyacente",
                "indice_a": i,
                "indice_b": i + 1,
                "orden_visita_a": oa,
                "orden_visita_b": ob,
                "bsale_id_a": int(a["bsale_id"]),
                "bsale_id_b": int(b["bsale_id"]),
                "nombre_a": na,
                "nombre_b": nb,
                "delta_km": round(delta_km, 3),
                "mensaje": mensaje,
            }
        )

    out.sort(key=lambda s: float(s["delta_km"]))
    return out
