from fastapi import APIRouter, HTTPException, Query
from pydantic import AliasChoices, BaseModel, ConfigDict, Field

from backend.db import get_connection


class OrdenManualBody(BaseModel):
    cliente_id: int = Field(..., ge=1, description="bsale_id del cliente en rutero")
    orden_manual: int = Field(..., gt=0, description="Solo valores > 0 (orden de visita cliente)")


class OrdenManualResetBody(BaseModel):
    vendedor: str = Field(..., min_length=1)
    dia: str = Field(..., min_length=1)


class OrdenManualBulkItem(BaseModel):
    """`id` en JSON = bsale_id (contrato API)."""

    model_config = ConfigDict(populate_by_name=True)
    cliente_id: int = Field(..., ge=1, validation_alias=AliasChoices("id", "cliente_id"))
    orden_manual: int = Field(..., gt=0)


class OptimizarRutaBody(BaseModel):
    vendedor: str = Field(..., min_length=1)
    dia: str = Field(..., min_length=1)

router = APIRouter(prefix="/distribuidora", tags=["Distribuidora"])


def _rows_to_json(cur) -> list[dict]:
    columns = [col[0] for col in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


def _as_float(value) -> float | None:
    if value is None:
        return None
    return float(value)


def _base_respuesta_publica(base_row: dict) -> dict:
    """
    Punto de partida/llegada de la ruta (no es cliente, sin orden de visita).
    Solo nombre + coordenadas para API y front.
    """
    nombre = (base_row.get("nombre") or base_row.get("vendedor") or "").strip() or "Base"
    lat = _as_float(base_row.get("lat"))
    lon = _as_float(base_row.get("lon"))
    return {
        "nombre": nombre,
        "lat": lat,
        "lon": lon,
    }


def _clientes_validos_coords(clientes: list[dict]) -> list[dict]:
    return [
        c
        for c in clientes
        if c.get("lat") is not None
        and c.get("lon") is not None
        and float(c["lat"]) != 0
        and float(c["lon"]) != 0
    ]


def _clientes_orden_visita_desde_ors(clientes: list[dict], route: dict) -> list[dict]:
    """
    Intenta ordenar clientes según steps[].way_points[0] de ORS (índices al mismo orden de coords
    enviadas: 0=base, 1..n=clientes, n+1=base). Si los índices no encajan (p. ej. geometría densa),
    devuelve el mismo orden con orden_visita 1..n.
    """
    n = len(clientes)
    if n == 0:
        return []

    segments = route.get("segments") or []
    steps: list = []
    if segments and isinstance(segments[0], dict):
        steps = segments[0].get("steps") or []

    orden_coords: list[int] = []
    for step in steps:
        if not isinstance(step, dict):
            continue
        wp = step.get("way_points")
        if not isinstance(wp, (list, tuple)) or len(wp) < 1:
            continue
        w0 = wp[0]
        if isinstance(w0, int):
            orden_coords.append(w0)

    orden_final = list(dict.fromkeys(orden_coords))
    max_idx_valido = n + 1  # coords: 0..n+1

    indices_cliente: list[int] = []
    for idx in orden_final:
        if idx == 0 or idx == max_idx_valido:
            continue
        if not isinstance(idx, int):
            continue
        if 1 <= idx <= n:
            indices_cliente.append(idx)

    indices_unicos: list[int] = []
    vistos: set[int] = set()
    for idx in indices_cliente:
        if idx not in vistos:
            vistos.add(idx)
            indices_unicos.append(idx)

    if not indices_unicos:
        salida = []
        for i, c in enumerate(clientes, start=1):
            fila = dict(c)
            fila["orden_visita"] = i
            salida.append(fila)
        return salida

    salida: list[dict] = []
    for idx in indices_unicos:
        fila = dict(clientes[idx - 1])
        salida.append(fila)

    for k in range(1, n + 1):
        if k not in vistos:
            fila = dict(clientes[k - 1])
            salida.append(fila)
            vistos.add(k)

    for i, fila in enumerate(salida, start=1):
        fila["orden_visita"] = i

    return salida


def _clientes_orden_manual(
    manual_rows: list[dict],
    todos_validos: list[dict],
) -> list[dict]:
    """
    Orden: primero filas con orden_manual (orden_manual ASC), luego el resto
    como en rutero (orden_ruta NULLS LAST, bsale_id).
    """
    by_id = {c["bsale_id"]: dict(c) for c in todos_validos}
    vistos: set[int] = set()
    salida: list[dict] = []

    for m in manual_rows:
        bid = m.get("bsale_id")
        if bid is None or bid in vistos:
            continue
        base = by_id.get(bid)
        if base is None:
            continue
        fila = dict(base)
        salida.append(fila)
        vistos.add(bid)

    resto = sorted(
        (c for c in todos_validos if c["bsale_id"] not in vistos),
        key=lambda c: (
            c.get("orden_ruta") is None,
            c.get("orden_ruta") if c.get("orden_ruta") is not None else 0,
            c["bsale_id"],
        ),
    )
    for c in resto:
        salida.append(dict(c))

    for i, fila in enumerate(salida, start=1):
        fila["orden_visita"] = i

    return salida


def _geometria_ruta_secuencial(
    base: dict,
    clientes_ordenados: list[dict],
) -> tuple[object | None, float, float] | None:
    """
    ORS en el orden fijo dado (base → visitas → base), sin reoptimizar secuencia.
    Devuelve (geometry, km_totales, minutos_totales) o None si no se pudo calcular.
    """
    from backend.utils.ors_client import get_route

    if not clientes_ordenados:
        return (None, 0.0, 0.0)

    coords: list[list[float]] = [[float(base["lon"]), float(base["lat"])]]
    for c in clientes_ordenados:
        coords.append([float(c["lon"]), float(c["lat"])])
    coords.append([float(base["lon"]), float(base["lat"])])

    if len(coords) < 2:
        return None
    if len(coords) > 50:
        return None

    try:
        ors_data = get_route(coords)
    except Exception:
        return None

    if "routes" not in ors_data or not ors_data["routes"]:
        return None

    route = ors_data["routes"][0]
    summary = route.get("summary") or {}
    dist_m = summary.get("distance")
    dur_s = summary.get("duration")
    km = float(dist_m) / 1000 if dist_m is not None else 0.0
    mins = float(dur_s) / 60 if dur_s is not None else 0.0
    return (route.get("geometry"), km, mins)


def _decode_polyline_lonlat(polyline_str: str, precision: int = 5) -> list[list[float]]:
    """Polyline codificada → [[lon, lat], ...] (algoritmo Google, precisión típica ORS=5)."""
    if not polyline_str or not isinstance(polyline_str, str):
        return []
    index = 0
    lat = 0
    lng = 0
    coordinates: list[list[float]] = []
    factor = 10**precision
    strlen = len(polyline_str)
    while index < strlen:
        result = 1
        shift = 0
        while True:
            if index >= strlen:
                return coordinates
            b = ord(polyline_str[index]) - 63 - 1
            index += 1
            result += b << shift
            shift += 5
            if b < 31:
                break
        dlat = ~(result >> 1) if (result & 1) else (result >> 1)
        lat += dlat

        result = 1
        shift = 0
        while True:
            if index >= strlen:
                return coordinates
            b = ord(polyline_str[index]) - 63 - 1
            index += 1
            result += b << shift
            shift += 5
            if b < 31:
                break
        dlng = ~(result >> 1) if (result & 1) else (result >> 1)
        lng += dlng

        coordinates.append([lng / factor, lat / factor])
    return coordinates


def _route_geometry_to_lonlat_coords(geometry: object) -> list[list[float]]:
    """Unifica geometría ORS (GeoJSON LineString o polyline codificada)."""
    if geometry is None:
        return []
    if isinstance(geometry, str):
        c5 = _decode_polyline_lonlat(geometry, 5)
        if c5:
            return c5
        return _decode_polyline_lonlat(geometry, 6)
    if isinstance(geometry, dict):
        if geometry.get("type") == "LineString":
            coords = geometry.get("coordinates")
            if isinstance(coords, list) and coords:
                out: list[list[float]] = []
                for p in coords:
                    if isinstance(p, (list, tuple)) and len(p) >= 2:
                        out.append([float(p[0]), float(p[1])])
                return out
    return []


def _ors_optimize_single_roundtrip(base: dict, clientes: list[dict]) -> dict:
    """
    Una llamada ORS: base → clientes (orden enviado) → base.
    Orden de visita según steps de la respuesta (`_clientes_orden_visita_desde_ors`).
    """
    from backend.utils.ors_client import get_route

    if not clientes:
        return {
            "km_totales": 0.0,
            "minutos_totales": 0.0,
            "geometry": None,
            "clientes": [],
        }

    coords: list[list[float]] = []
    coords.append([float(base["lon"]), float(base["lat"])])
    for c in clientes:
        coords.append([float(c["lon"]), float(c["lat"])])
    coords.append([float(base["lon"]), float(base["lat"])])

    if len(coords) < 2:
        return {"error": "No hay suficientes puntos para calcular ruta"}

    if len(coords) > 50:
        return {
            "error": "Demasiados puntos para ORS",
            "total_coords": len(coords),
        }

    try:
        ors_data = get_route(coords)
    except Exception as e:
        print("ERROR ORS:", str(e))
        return {
            "error": "Fallo al calcular ruta",
            "detalle": str(e),
            "coords_enviadas": coords,
        }

    if "routes" not in ors_data or not ors_data["routes"]:
        return {
            "error": "ORS no devolvió rutas",
            "respuesta_ors": ors_data,
        }

    route = ors_data["routes"][0]
    summary = route["summary"]
    geometry = route.get("geometry")
    clientes_ordenados = _clientes_orden_visita_desde_ors(clientes, route)

    return {
        "km_totales": summary["distance"] / 1000,
        "minutos_totales": summary["duration"] / 60,
        "geometry": geometry,
        "clientes": clientes_ordenados,
    }


def _geom_km_encadenado(
    base: dict,
    clientes_ordenados: list[dict],
) -> tuple[object | None, float, float] | None:
    """
    Ruta larga: varias peticiones ORS encadenadas (sin volver a base entre tramos),
    suma km/minutos y une geometrías en un LineString GeoJSON.
    Límite ORS: 50 coordenadas por petición.
    """
    from backend.utils.ors_client import get_route

    n = len(clientes_ordenados)
    if n == 0:
        return (None, 0.0, 0.0)

    blat = float(base["lat"])
    blon = float(base["lon"])
    merged_coords: list[list[float]] = []
    total_km = 0.0
    total_mins = 0.0
    pos = 0
    prev_lon, prev_lat = blon, blat

    while pos < n:
        remaining = n - pos
        if pos == 0:
            if remaining + 2 <= 50:
                chunk_len = remaining
                is_last = True
            else:
                chunk_len = min(49, remaining - 1)
                is_last = False
        elif remaining + 2 <= 50:
            chunk_len = remaining
            is_last = True
        else:
            chunk_len = min(49, remaining - 1)
            is_last = False

        chunk = clientes_ordenados[pos : pos + chunk_len]
        pos += len(chunk)
        if not chunk:
            break

        coords: list[list[float]] = [[prev_lon, prev_lat]]
        coords.extend([float(c["lon"]), float(c["lat"])] for c in chunk)
        if is_last:
            coords.append([blon, blat])

        if len(coords) < 2 or len(coords) > 50:
            return None

        try:
            ors_data = get_route(coords)
        except Exception:
            return None

        if "routes" not in ors_data or not ors_data["routes"]:
            return None

        route = ors_data["routes"][0]
        summary = route.get("summary") or {}
        dist_m = summary.get("distance")
        dur_s = summary.get("duration")
        if dist_m is not None:
            total_km += float(dist_m) / 1000
        if dur_s is not None:
            total_mins += float(dur_s) / 60

        part = _route_geometry_to_lonlat_coords(route.get("geometry"))
        if part:
            if merged_coords and part[0] == merged_coords[-1]:
                part = part[1:]
            merged_coords.extend(part)

        prev_lon = float(chunk[-1]["lon"])
        prev_lat = float(chunk[-1]["lat"])

    if not merged_coords:
        return None

    geometry: object = {"type": "LineString", "coordinates": merged_coords}
    return (geometry, total_km, total_mins)


def _geom_km_ruta_completa(
    base: dict,
    clientes_ordenados: list[dict],
) -> tuple[object | None, float, float] | None:
    """Una petición secuencial si cabe en ORS; si no, tramos encadenados."""
    seq = _geometria_ruta_secuencial(base, clientes_ordenados)
    if seq is not None:
        return seq
    return _geom_km_encadenado(base, clientes_ordenados)


def _ors_optimize_from_base_clientes_por_zonas(base: dict, clientes: list[dict]) -> dict:
    """
    Agrupa clientes por zona (K-means), ordena grupos por cercanía a la base,
    en cada grupo ordena por distancia a la base y llama ORS (ida y vuelta a base).
    La visita global es grupo1 → grupo2 → …; km/geometría final coherente (secuencial o encadenado).
    """
    from backend.utils.ruta_zonas import (
        agrupar_clientes_por_zona_kmeans,
        elegir_num_zonas,
        ordenar_clientes_en_grupo_por_distancia_a_base,
        ordenar_grupos_por_cercania_a_base,
    )

    k = elegir_num_zonas(len(clientes))
    grupos_raw = agrupar_clientes_por_zona_kmeans(clientes, k)
    grupos = ordenar_grupos_por_cercania_a_base(grupos_raw, base)

    merged: list[dict] = []
    for g in grupos:
        intra = ordenar_clientes_en_grupo_por_distancia_a_base(g, base)
        sub = _ors_optimize_single_roundtrip(base, intra)
        if "error" in sub:
            return sub
        merged.extend(sub["clientes"])

    for i, row in enumerate(merged, start=1):
        row["orden_visita"] = i

    geo = _geom_km_ruta_completa(base, merged)
    if geo is None:
        return {"error": "No se pudo calcular geometría/km de la ruta combinada"}

    geometry, km_totales, minutos_totales = geo
    return {
        "km_totales": km_totales,
        "minutos_totales": minutos_totales,
        "geometry": geometry,
        "clientes": merged,
    }


# Máximo clientes para una sola optimización ORS «monolítica» (sin partición por zonas).
_ORS_OPTIMIZAR_UN_SOLO_BLOQUE_MAX = 14


def _ors_optimize_from_base_clientes(base: dict, clientes: list[dict]) -> dict:
    """
    Optimización ORS. Con muchos clientes: K-means por zona, ORS por grupo,
    orden global grupo a grupo (grupos más cercanos a la base primero).
    """
    if not clientes:
        return {
            "km_totales": 0.0,
            "minutos_totales": 0.0,
            "geometry": None,
            "clientes": [],
        }

    if len(clientes) <= _ORS_OPTIMIZAR_UN_SOLO_BLOQUE_MAX:
        return _ors_optimize_single_roundtrip(base, clientes)

    return _ors_optimize_from_base_clientes_por_zonas(base, clientes)


def _persistir_orden_manual_vendedor_dia(
    conn,
    v: str,
    d: str,
    clientes_con_orden_visita: list[dict],
) -> None:
    """NULL orden_manual para el día y asigna según orden_visita (1..n)."""
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE bsale.rutero
        SET orden_manual = NULL
        WHERE company_id = 3
          AND activo = TRUE
          AND LOWER(vendedor) = LOWER(%s)
          AND LOWER(dia_atencion) = LOWER(%s)
          AND LOWER(COALESCE(tipo_atencion, '')) <> 'telefonico'
        """,
        (v, d),
    )
    for c in clientes_con_orden_visita:
        ov = c.get("orden_visita")
        bid = c.get("bsale_id")
        if ov is None or bid is None:
            continue
        ov_int = int(ov)
        if ov_int <= 0:
            raise HTTPException(
                status_code=400,
                detail="orden_visita / orden_manual debe ser mayor que 0",
            )
        cur.execute(
            """
            UPDATE bsale.rutero
            SET orden_manual = %s
            WHERE company_id = 3
              AND activo = TRUE
              AND bsale_id = %s
              AND LOWER(vendedor) = LOWER(%s)
              AND LOWER(dia_atencion) = LOWER(%s)
            """,
            (ov_int, int(bid), v, d),
        )
    conn.commit()
    cur.close()


def _cargar_contexto_ruta(v: str, d: str) -> tuple[dict, list[dict], list[dict]]:
    """Punto base + filas rutero con orden_manual + todas las filas del día (terreno)."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT vendedor, nombre, lat, lon
            FROM bsale.puntos_base
            WHERE LOWER(vendedor) = LOWER(%s)
            LIMIT 1
            """,
            (v,),
        )
        bases = _rows_to_json(cur)
        if not bases:
            raise HTTPException(
                status_code=404,
                detail=f"No hay punto base para el vendedor '{v}'",
            )
        base = bases[0]
        base_lon = _as_float(base.get("lon"))
        base_lat = _as_float(base.get("lat"))
        if base_lon is None or base_lat is None:
            raise HTTPException(
                status_code=400,
                detail="El punto base no tiene lat/lon válidos",
            )

        cur.execute(
            """
            SELECT
                bsale_id,
                first_name,
                last_name,
                nombre_fantasia,
                phone,
                vendedor,
                dia_atencion,
                dia_extra,
                municipality,
                lat,
                lon,
                tipo_atencion,
                orden_ruta,
                orden_manual
            FROM bsale.rutero
            WHERE company_id = 3
              AND activo = TRUE
              AND LOWER(vendedor) = LOWER(%s)
              AND LOWER(dia_atencion) = LOWER(%s)
              AND lat IS NOT NULL
              AND lon IS NOT NULL
              AND LOWER(tipo_atencion) <> 'telefonico'
              AND orden_manual IS NOT NULL
            ORDER BY orden_manual ASC, bsale_id
            """,
            (v, d),
        )
        manual_raw = _rows_to_json(cur)

        cur.execute(
            """
            SELECT
                bsale_id,
                first_name,
                last_name,
                nombre_fantasia,
                phone,
                vendedor,
                dia_atencion,
                dia_extra,
                municipality,
                lat,
                lon,
                tipo_atencion,
                orden_ruta,
                orden_manual
            FROM bsale.rutero
            WHERE company_id = 3
              AND activo = TRUE
              AND LOWER(vendedor) = LOWER(%s)
              AND LOWER(dia_atencion) = LOWER(%s)
              AND lat IS NOT NULL
              AND lon IS NOT NULL
              AND LOWER(tipo_atencion) <> 'telefonico'
            ORDER BY orden_ruta NULLS LAST, bsale_id
            """,
            (v, d),
        )
        clientes = _rows_to_json(cur)
        cur.close()
    finally:
        conn.close()

    return base, manual_raw, clientes


@router.get("/ruta-detalle")
def get_ruta_detalle(
    vendedor: str = Query(..., min_length=1, description="Código vendedor (ej. vendedor_1)"),
    dia: str = Query(..., min_length=1, description="Día de atención (ej. Lunes), coincide con dia_atencion"),
):
    """Ruta: si hay orden_manual se respeta la secuencia (sin reoptimizar); si no, ORS optimiza."""
    v = vendedor.strip()
    d = dia.strip()
    if not v or not d:
        raise HTTPException(status_code=400, detail="vendedor y dia son obligatorios")

    base, manual_raw, clientes_rows = _cargar_contexto_ruta(v, d)
    clientes = _clientes_validos_coords(clientes_rows)
    manual_valid = _clientes_validos_coords(manual_raw)
    print("TOTAL CLIENTES VALIDOS:", len(clientes))

    if len(clientes) == 0:
        return {
            "vendedor": v,
            "dia": d,
            "km_totales": 0.0,
            "minutos_totales": 0.0,
            "geometry": None,
            "clientes": clientes,
            "base": _base_respuesta_publica(base),
        }

    if len(manual_valid) > 0:
        clientes_ordenados = _clientes_orden_manual(manual_valid, clientes)
        geo = _geometria_ruta_secuencial(base, clientes_ordenados)
        if geo is None:
            geometry, km_totales, minutos_totales = None, 0.0, 0.0
        else:
            geometry, km_totales, minutos_totales = geo
        return {
            "vendedor": v,
            "dia": d,
            "km_totales": km_totales,
            "minutos_totales": minutos_totales,
            "geometry": geometry,
            "clientes": clientes_ordenados,
            "base": _base_respuesta_publica(base),
        }

    ors_payload = _ors_optimize_from_base_clientes(base, clientes)
    if "error" in ors_payload:
        return ors_payload

    return {
        "vendedor": v,
        "dia": d,
        "km_totales": ors_payload["km_totales"],
        "minutos_totales": ors_payload["minutos_totales"],
        "geometry": ors_payload["geometry"],
        "clientes": ors_payload["clientes"],
        "base": _base_respuesta_publica(base),
    }


@router.post("/optimizar-ruta")
def post_optimizar_ruta(body: OptimizarRutaBody):
    """
    ORS optimiza la visita y persiste orden_manual = 1..n en rutero para ese vendedor y día.
    """
    v = body.vendedor.strip()
    d = body.dia.strip()
    if not v or not d:
        raise HTTPException(status_code=400, detail="vendedor y dia son obligatorios")

    base, _manual_raw, clientes_rows = _cargar_contexto_ruta(v, d)
    clientes = _clientes_validos_coords(clientes_rows)
    if len(clientes) == 0:
        raise HTTPException(
            status_code=400,
            detail="No hay clientes terreno con coordenadas válidas para este día",
        )

    payload = _ors_optimize_from_base_clientes(base, clientes)
    if "error" in payload:
        return payload

    conn = get_connection()
    try:
        _persistir_orden_manual_vendedor_dia(conn, v, d, payload["clientes"])
    finally:
        conn.close()

    return {
        "vendedor": v,
        "dia": d,
        "base": _base_respuesta_publica(base),
        "km_totales": payload["km_totales"],
        "minutos_totales": payload["minutos_totales"],
        "geometry": payload["geometry"],
        "clientes": payload["clientes"],
    }


@router.post("/orden-manual-bulk")
def post_orden_manual_bulk(items: list[OrdenManualBulkItem]):
    """Actualiza orden_manual en lote (`id` = bsale_id)."""
    if not items:
        raise HTTPException(status_code=400, detail="Lista vacía")
    if len(items) > 500:
        raise HTTPException(status_code=400, detail="Demasiados ítems (máx. 500)")

    conn = get_connection()
    try:
        cur = conn.cursor()
        for it in items:
            cur.execute(
                """
                UPDATE bsale.rutero
                SET orden_manual = %s
                WHERE company_id = 3
                  AND activo = TRUE
                  AND bsale_id = %s
                """,
                (it.orden_manual, it.cliente_id),
            )
            if cur.rowcount == 0:
                conn.rollback()
                cur.close()
                raise HTTPException(
                    status_code=404,
                    detail=f"No hay fila activa en rutero para bsale_id={it.cliente_id}",
                )
        conn.commit()
        cur.close()
    finally:
        conn.close()

    return {"ok": True, "actualizados": len(items)}


@router.get("/analisis-km")
def get_analisis_km(
    max_pares: int = Query(
        300,
        ge=1,
        le=800,
        description="Máximo de combinaciones vendedor+día a evaluar (cada una llama a ORS).",
    ),
):
    """
    Kilómetros ORS por vendedor y día (base → clientes → base), ordenado de mayor a menor km.
    `eficiencia` = km / cliente (promedio km por visita).
    """
    from backend.utils.ors_client import get_route

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT DISTINCT
                TRIM(COALESCE(vendedor::text, '')) AS vendedor,
                TRIM(COALESCE(dia_atencion::text, '')) AS dia
            FROM bsale.rutero
            WHERE company_id = 3
              AND activo = TRUE
              AND lat IS NOT NULL
              AND lon IS NOT NULL
              AND LOWER(tipo_atencion) <> 'telefonico'
              AND TRIM(COALESCE(vendedor::text, '')) <> ''
              AND TRIM(COALESCE(dia_atencion::text, '')) <> ''
            ORDER BY vendedor, dia_atencion
            LIMIT %s
            """,
            (max_pares,),
        )
        pares = _rows_to_json(cur)

        resultados: list[dict] = []
        for p in pares:
            v = (p.get("vendedor") or "").strip()
            d = (p.get("dia") or "").strip()
            if not v or not d:
                continue

            cur.execute(
                """
                SELECT vendedor, nombre, lat, lon
                FROM bsale.puntos_base
                WHERE LOWER(vendedor) = LOWER(%s)
                LIMIT 1
                """,
                (v,),
            )
            bases = _rows_to_json(cur)
            if not bases:
                resultados.append(
                    {
                        "vendedor": v,
                        "dia": d,
                        "km": 0.0,
                        "clientes": 0,
                        "eficiencia": 0.0,
                        "error": "sin_punto_base",
                    }
                )
                continue
            base = bases[0]
            blon = _as_float(base.get("lon"))
            blat = _as_float(base.get("lat"))
            if blon is None or blat is None:
                resultados.append(
                    {
                        "vendedor": v,
                        "dia": d,
                        "km": 0.0,
                        "clientes": 0,
                        "eficiencia": 0.0,
                        "error": "base_sin_coordenadas",
                    }
                )
                continue

            cur.execute(
                """
                SELECT
                    bsale_id,
                    first_name,
                    last_name,
                    nombre_fantasia,
                    phone,
                    vendedor,
                    dia_atencion,
                    dia_extra,
                    municipality,
                    lat,
                    lon,
                    tipo_atencion,
                    orden_ruta
                FROM bsale.rutero
                WHERE company_id = 3
                  AND activo = TRUE
                  AND LOWER(vendedor) = LOWER(%s)
                  AND LOWER(dia_atencion) = LOWER(%s)
                  AND lat IS NOT NULL
                  AND lon IS NOT NULL
                  AND LOWER(tipo_atencion) <> 'telefonico'
                ORDER BY orden_ruta NULLS LAST, bsale_id
                """,
                (v, d),
            )
            clientes = _clientes_validos_coords(_rows_to_json(cur))
            n = len(clientes)
            if n == 0:
                resultados.append(
                    {
                        "vendedor": v,
                        "dia": d,
                        "km": 0.0,
                        "clientes": 0,
                        "eficiencia": 0.0,
                    }
                )
                continue

            coords: list[list[float]] = [[float(base["lon"]), float(base["lat"])]]
            for c in clientes:
                coords.append([float(c["lon"]), float(c["lat"])])
            coords.append([float(base["lon"]), float(base["lat"])])

            if len(coords) > 50:
                resultados.append(
                    {
                        "vendedor": v,
                        "dia": d,
                        "km": 0.0,
                        "clientes": n,
                        "eficiencia": 0.0,
                        "error": "demasiados_puntos_ors",
                    }
                )
                continue

            try:
                ors_data = get_route(coords)
            except Exception as e:
                print("ERROR ORS analisis-km:", v, d, str(e))
                resultados.append(
                    {
                        "vendedor": v,
                        "dia": d,
                        "km": 0.0,
                        "clientes": n,
                        "eficiencia": 0.0,
                        "error": "ors_fallo",
                    }
                )
                continue

            if "routes" not in ors_data or not ors_data["routes"]:
                resultados.append(
                    {
                        "vendedor": v,
                        "dia": d,
                        "km": 0.0,
                        "clientes": n,
                        "eficiencia": 0.0,
                        "error": "ors_sin_rutas",
                    }
                )
                continue

            summary = ors_data["routes"][0].get("summary") or {}
            dist_m = summary.get("distance")
            if dist_m is None:
                km = 0.0
            else:
                km = float(dist_m) / 1000.0

            ef = round(km / n, 2) if n else 0.0
            resultados.append(
                {
                    "vendedor": v,
                    "dia": d,
                    "km": round(km, 1),
                    "clientes": n,
                    "eficiencia": ef,
                }
            )

        cur.close()
    finally:
        conn.close()

    resultados.sort(key=lambda r: float(r.get("km") or 0), reverse=True)
    return resultados


@router.get("/rutero")
def get_rutero():
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT *
            FROM bsale.rutero
            WHERE company_id = 3
              AND activo = TRUE
            """
        )
        data = _rows_to_json(cur)
        cur.close()
    finally:
        conn.close()

    return data


@router.get("/sin-georef")
def get_sin_georef():
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT *
            FROM bsale.rutero
            WHERE company_id = 3
              AND activo = TRUE
              AND (lat IS NULL OR lon IS NULL)
            """
        )
        data = _rows_to_json(cur)
        cur.close()
    finally:
        conn.close()

    return data


@router.get("/pendientes")
def get_pendientes():
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT *
            FROM bsale.clients
            WHERE company_id = 3
              AND vendedor IN (
                  'vendedor_1',
                  'vendedor_2',
                  'vendedor_3',
                  'vendedor_4'
              )
              AND (
                  dia_atencion IS NULL
                  OR lat IS NULL
              )
            """
        )
        data = _rows_to_json(cur)
        cur.close()
    finally:
        conn.close()

    return data


@router.get("/mapa")
def get_mapa():
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                bsale_id,
                first_name,
                last_name,
                nombre_fantasia,
                phone,
                vendedor,
                dia_atencion,
                dia_extra,
                municipality,
                lat,
                lon,
                tipo_atencion,
                orden_ruta,
                orden_manual
            FROM bsale.rutero
            WHERE company_id = 3
              AND activo = TRUE
              AND lat IS NOT NULL
              AND lon IS NOT NULL
            """
        )
        clientes = _rows_to_json(cur)

        cur.execute(
            """
            SELECT vendedor, nombre, lat, lon
            FROM bsale.puntos_base
            """
        )
        bases = _rows_to_json(cur)
        cur.close()
    finally:
        conn.close()

    return {
        "clientes": clientes,
        "bases": bases,
    }


@router.post("/orden-manual/reset")
def post_orden_manual_reset(body: OrdenManualResetBody):
    """Pone orden_manual en NULL para el vendedor y día (vuelve a ORS en ruta-detalle)."""
    v = body.vendedor.strip()
    d = body.dia.strip()
    if not v or not d:
        raise HTTPException(status_code=400, detail="vendedor y dia son obligatorios")

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE bsale.rutero
            SET orden_manual = NULL
            WHERE company_id = 3
              AND activo = TRUE
              AND LOWER(vendedor) = LOWER(%s)
              AND LOWER(dia_atencion) = LOWER(%s)
            """,
            (v, d),
        )
        n = cur.rowcount
        conn.commit()
        cur.close()
    finally:
        conn.close()

    return {"ok": True, "actualizados": n}


@router.post("/orden-manual")
def post_orden_manual(body: OrdenManualBody):
    """Persiste orden_manual para un cliente (bsale_id) en rutero."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE bsale.rutero
            SET orden_manual = %s
            WHERE company_id = 3
              AND activo = TRUE
              AND bsale_id = %s
            """,
            (body.orden_manual, body.cliente_id),
        )
        if cur.rowcount == 0:
            cur.close()
            raise HTTPException(
                status_code=404,
                detail=f"No hay fila activa en rutero para bsale_id={body.cliente_id}",
            )
        conn.commit()
        cur.close()
    finally:
        conn.close()

    return {"ok": True}


@router.get("/resumen")
def get_resumen():
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                vendedor,
                dia_atencion,
                COUNT(*) AS cantidad
            FROM bsale.rutero
            WHERE company_id = 3
              AND activo = TRUE
            GROUP BY vendedor, dia_atencion
            ORDER BY vendedor, dia_atencion
            """
        )
        data = _rows_to_json(cur)
        cur.close()
    finally:
        conn.close()

    return data
