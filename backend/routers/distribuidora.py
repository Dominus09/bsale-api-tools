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


class AsignarDiaAtencionBody(BaseModel):
    bsale_id: int = Field(..., ge=1)
    dia_atencion: str = Field(..., min_length=1, description="Día de atención (ej. Lunes)")


class ObservacionRuteroBody(BaseModel):
    """`cliente_id` = PK `bsale.rutero.id` (fila del cliente en rutero)."""

    cliente_id: int = Field(..., ge=1)
    observaciones: str | None = Field(default=None, description="Texto libre; vacío o null → NULL en BD")


class OptimizarRutaDesdeBody(BaseModel):
    """Reoptimiza solo la cola a partir de un índice (0 = primer cliente = toda la ruta)."""

    vendedor: str = Field(..., min_length=1)
    dia: str = Field(..., min_length=1)
    desde_indice: int = Field(
        ...,
        ge=0,
        description="Índice 0-based del primer cliente del tramo a reoptimizar (antes = [:i] intacto)",
    )


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


def _ors_route_start_clients_end(start: dict, clientes: list[dict], end: dict) -> dict:
    """
    Una llamada ORS: start → clientes (orden enviado) → end.
    `start` y `end` son dicts con lat/lon (p. ej. base o último cliente fijo).
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
    coords.append([float(start["lon"]), float(start["lat"])])
    for c in clientes:
        coords.append([float(c["lon"]), float(c["lat"])])
    coords.append([float(end["lon"]), float(end["lat"])])

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


def _ors_optimize_single_roundtrip(base: dict, clientes: list[dict]) -> dict:
    """Una llamada ORS: base → clientes → base."""
    return _ors_route_start_clients_end(base, clientes, base)


def _ors_ordered_tail_chunked(start: dict, despues: list[dict], base: dict) -> dict:
    """
    Cola larga: varias peticiones ORS [prev]→chunk→… y cierre final en base.
    Devuelve la lista de clientes de la cola en el orden ORS agregado.
    """
    from backend.utils.ors_client import get_route

    n = len(despues)
    if n == 0:
        return {"clientes": []}

    pos = 0
    prev_lon, prev_lat = float(start["lon"]), float(start["lat"])
    blat = float(base["lat"])
    blon = float(base["lon"])
    out: list[dict] = []

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

        chunk = despues[pos : pos + chunk_len]
        pos += len(chunk)
        if not chunk:
            break

        coords: list[list[float]] = [[prev_lon, prev_lat]]
        for c in chunk:
            coords.append([float(c["lon"]), float(c["lat"])])
        if is_last:
            coords.append([blon, blat])

        if len(coords) < 2 or len(coords) > 50:
            return {"error": "Tramo demasiado largo para ORS (chunk interno)"}

        try:
            ors_data = get_route(coords)
        except Exception as e:
            print("ERROR ORS (subruta):", str(e))
            return {"error": "Fallo al calcular subruta", "detalle": str(e)}

        if "routes" not in ors_data or not ors_data["routes"]:
            return {"error": "ORS no devolvió rutas en subruta"}

        route = ors_data["routes"][0]
        ordered = _clientes_orden_visita_desde_ors(chunk, route)
        out.extend(ordered)

        prev_lon = float(chunk[-1]["lon"])
        prev_lat = float(chunk[-1]["lat"])

    return {"clientes": out}


def _ors_reoptimizar_cola(prev: dict | None, despues: list[dict], base: dict) -> dict:
    """
    ORS solo sobre `despues`: inicio en `prev` (último cliente fijo) o en base si no hay prev.
    """
    if not despues:
        return {"error": "No hay tramo a reoptimizar"}
    start = prev if prev is not None else base
    if 1 + len(despues) + 1 <= 50:
        return _ors_route_start_clients_end(start, despues, base)
    sub = _ors_ordered_tail_chunked(start, despues, base)
    if "error" in sub:
        return sub
    return {"clientes": sub["clientes"]}


def _lista_visita_actual_ruta(
    base: dict,
    manual_raw: list[dict],
    clientes_rows: list[dict],
) -> tuple[list[dict] | None, dict | None]:
    """
    Misma secuencia que expone GET ruta-detalle: manual si hay; si no, ORS completo.
    Devuelve (clientes_ordenados, error_payload).
    """
    clientes = _clientes_validos_coords(clientes_rows)
    manual_valid = _clientes_validos_coords(manual_raw)
    if not clientes:
        return [], None
    if len(manual_valid) > 0:
        return _clientes_orden_manual(manual_valid, clientes), None
    payload = _ors_optimize_from_base_clientes(base, clientes)
    if "error" in payload:
        return None, payload
    return payload["clientes"], None


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
    Pipeline de calidad: (1) preorden radial atan2(lat-base, lon-base); (2–3) K-means k∈{2,3,4}
    en [lon,lat] con `cluster_id`; (4) grupos ordenados por centroide vs base; (5) ORS por grupo
    (base→grupo→base, semilla radial intra-grupo); (6) concatenación; (7) ORS directions
    BASE→ruta_final→BASE vía `_geom_km_ruta_completa` para km y geometría coherentes.
    """
    from backend.utils.ruta_zonas import (
        clientes_con_cluster_kmeans,
        elegir_k_clusters,
        listas_grupos_cluster_ordenados,
        ordenar_grupo_radial_desde_base,
        preordenar_radial_clientes,
    )

    radial = preordenar_radial_clientes(clientes, base)
    k = elegir_k_clusters(len(radial))
    tagged = clientes_con_cluster_kmeans(radial, k)
    grupos = listas_grupos_cluster_ordenados(tagged, base)

    merged: list[dict] = []
    for g in grupos:
        intra = ordenar_grupo_radial_desde_base(g, base)
        if len(intra) + 2 <= 50:
            sub = _ors_route_start_clients_end(base, intra, base)
        else:
            sub = _ors_optimize_from_base_clientes(base, intra)
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
    Optimización ORS. Con muchos clientes: pipeline radial + K-means (2–4 grupos) + ORS
    por grupo y ruta final BASE→visitas→BASE para métricas y trazado.
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


@router.post("/optimizar-ruta-desde")
def post_optimizar_ruta_desde(body: OptimizarRutaDesdeBody):
    """
    Mantiene el orden de los primeros `desde_indice` clientes y vuelve a optimizar con ORS
    el resto (inicio del tramo ORS: último cliente fijo o la base si desde_indice==0).
    Persiste orden_manual = 1..n para el día.
    """
    v = body.vendedor.strip()
    d = body.dia.strip()
    if not v or not d:
        raise HTTPException(status_code=400, detail="vendedor y dia son obligatorios")

    base, manual_raw, clientes_rows = _cargar_contexto_ruta(v, d)
    clientes_validos = _clientes_validos_coords(clientes_rows)
    if len(clientes_validos) == 0:
        raise HTTPException(
            status_code=400,
            detail="No hay clientes terreno con coordenadas válidas para este día",
        )

    orden, err_ctx = _lista_visita_actual_ruta(base, manual_raw, clientes_rows)
    if err_ctx is not None:
        return err_ctx
    assert orden is not None

    n = len(orden)
    if body.desde_indice > n:
        raise HTTPException(status_code=400, detail="desde_indice fuera de rango")
    if body.desde_indice == n:
        raise HTTPException(status_code=400, detail="No hay clientes a reoptimizar a partir de ese índice")

    antes = orden[: body.desde_indice]
    despues = orden[body.desde_indice :]
    prev = antes[-1] if antes else None

    tail_payload = _ors_reoptimizar_cola(prev, despues, base)
    if "error" in tail_payload:
        return tail_payload

    despues_opt = tail_payload.get("clientes")
    if not isinstance(despues_opt, list):
        return {"error": "Respuesta ORS inválida"}

    merged: list[dict] = [dict(c) for c in antes] + [dict(c) for c in despues_opt]
    for i, fila in enumerate(merged, start=1):
        fila["orden_visita"] = i

    geo = _geom_km_ruta_completa(base, merged)
    if geo is None:
        return {"error": "No se pudo calcular geometría/km de la ruta combinada"}
    geometry, km_totales, minutos_totales = geo

    conn = get_connection()
    try:
        _persistir_orden_manual_vendedor_dia(conn, v, d, merged)
    finally:
        conn.close()

    return {
        "vendedor": v,
        "dia": d,
        "base": _base_respuesta_publica(base),
        "km_totales": km_totales,
        "minutos_totales": minutos_totales,
        "geometry": geometry,
        "clientes": merged,
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
def get_rutero(
    vendedor: str = Query(..., min_length=1, description="Código vendedor"),
    dia: str = Query(..., min_length=1, description="Día de atención (dia_atencion)"),
):
    """
    Listado del rutero para un vendedor y día: orden manual (NULL al final), nombre,
    municipio, teléfono, observaciones. Incluye `tipo_atencion` y `activo` para columna Estado.
    """
    v = vendedor.strip()
    d = dia.strip()
    if not v or not d:
        raise HTTPException(status_code=400, detail="vendedor y dia son obligatorios")

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                r.id,
                r.vendedor,
                r.dia_atencion,
                r.orden_manual,
                r.orden_ruta,
                COALESCE(
                    NULLIF(TRIM(r.nombre_fantasia), ''),
                    NULLIF(
                        TRIM(
                            CONCAT_WS(
                                ' ',
                                NULLIF(TRIM(r.first_name), ''),
                                NULLIF(TRIM(r.last_name), '')
                            )
                        ),
                        ''
                    ),
                    'Cliente #' || r.bsale_id::text
                ) AS cliente_nombre,
                r.municipality,
                r.lat,
                r.lon,
                r.phone AS telefono,
                r.observaciones,
                r.tipo_atencion,
                r.activo,
                r.bsale_id
            FROM bsale.rutero r
            WHERE r.company_id = 3
              AND r.activo = TRUE
              AND LOWER(TRIM(r.vendedor)) = LOWER(TRIM(%s))
              AND LOWER(TRIM(r.dia_atencion)) = LOWER(TRIM(%s))
            ORDER BY r.orden_manual ASC NULLS LAST,
                     r.orden_ruta ASC NULLS LAST,
                     r.bsale_id
            """,
            (v, d),
        )
        data = _rows_to_json(cur)
        cur.close()
    finally:
        conn.close()

    return data


def _rutero_fila_por_id(cur, row_id: int) -> dict | None:
    cur.execute(
        """
        SELECT
            r.id,
            r.vendedor,
            r.dia_atencion,
            r.orden_manual,
            r.orden_ruta,
            COALESCE(
                NULLIF(TRIM(r.nombre_fantasia), ''),
                NULLIF(
                    TRIM(
                        CONCAT_WS(
                            ' ',
                            NULLIF(TRIM(r.first_name), ''),
                            NULLIF(TRIM(r.last_name), '')
                        )
                    ),
                    ''
                ),
                'Cliente #' || r.bsale_id::text
            ) AS cliente_nombre,
            r.municipality,
            r.lat,
            r.lon,
            r.phone AS telefono,
            r.observaciones,
            r.tipo_atencion,
            r.activo,
            r.bsale_id
        FROM bsale.rutero r
        WHERE r.id = %s AND r.company_id = 3
        """,
        (row_id,),
    )
    rows = _rows_to_json(cur)
    return rows[0] if rows else None


@router.post("/observacion")
def post_observacion_rutero(body: ObservacionRuteroBody):
    """Persiste observaciones por fila rutero (`cliente_id` = `bsale.rutero.id`)."""
    obs = body.observaciones
    if obs is not None:
        obs = obs.strip() or None

    conn = get_connection()
    fila: dict | None = None
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE bsale.rutero
            SET observaciones = %s
            WHERE id = %s
              AND company_id = 3
              AND activo = TRUE
            """,
            (obs, body.cliente_id),
        )
        if cur.rowcount == 0:
            cur.close()
            raise HTTPException(status_code=404, detail="Fila rutero no encontrada o inactiva")
        conn.commit()
        fila = _rutero_fila_por_id(cur, body.cliente_id)
        cur.close()
    finally:
        conn.close()

    if fila is None:
        raise HTTPException(status_code=500, detail="No se pudo leer la fila actualizada")
    return fila


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
    """
    Clientes de ruta sin día de atención asignado (no entran al rutero operativo hasta tener día).
    """
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
                  OR TRIM(COALESCE(dia_atencion::text, '')) = ''
              )
            ORDER BY vendedor,
                     municipality NULLS LAST,
                     bsale_id
            """
        )
        data = _rows_to_json(cur)
        cur.close()
    finally:
        conn.close()

    return data


@router.post("/pendientes/asignar-dia")
def post_pendientes_asignar_dia(body: AsignarDiaAtencionBody):
    """Asigna `dia_atencion` en `bsale.clients` (empresa 3)."""
    d = body.dia_atencion.strip()
    if not d:
        raise HTTPException(status_code=400, detail="dia_atencion es obligatorio")

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE bsale.clients
            SET dia_atencion = %s,
                updated = CURRENT_TIMESTAMP
            WHERE company_id = 3
              AND bsale_id = %s
              AND vendedor IN (
                  'vendedor_1',
                  'vendedor_2',
                  'vendedor_3',
                  'vendedor_4'
              )
            """,
            (d, body.bsale_id),
        )
        if cur.rowcount == 0:
            cur.close()
            raise HTTPException(
                status_code=404,
                detail="Cliente no encontrado o no pertenece a los vendedores de ruta",
            )
        conn.commit()
        cur.execute(
            """
            SELECT *
            FROM bsale.clients
            WHERE company_id = 3 AND bsale_id = %s
            """,
            (body.bsale_id,),
        )
        row = _rows_to_json(cur)
        cur.close()
    finally:
        conn.close()

    return row[0] if row else {"ok": True, "bsale_id": body.bsale_id, "dia_atencion": d}


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
