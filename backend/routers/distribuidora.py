import logging
import os
from io import BytesIO

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import AliasChoices, BaseModel, ConfigDict, Field

from backend.db import get_connection
from backend.utils.ruta_optimizador_local import (
    log_resumen_optimizacion,
    optimizar_cola_desde_ancla,
    optimizar_secuencia_cerrado,
    tour_length_closed,
)
from backend.utils.ruta_zonas import haversine_m
from backend.utils.ruta_sugerencias_locales import sugerencias_swap_adyacentes
from backend.utils.rutero_coords_sql import (
    B_LAT_AS,
    B_LON_AS,
    R_LAT_AS,
    R_LON_AS,
    WHERE_HAS_GEOREF_BARE,
    WHERE_HAS_GEOREF_R,
    WHERE_SIN_GEOREF_BARE,
    WHERE_SIN_GEOREF_R,
)

logger = logging.getLogger(__name__)


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
    bloque_hasta_indice: int | None = Field(
        default=None,
        ge=0,
        description=(
            "Si se informa: visitas con índice 0..k-1 quedan fijas según el orden actual en BD; "
            "solo se reordena la cola desde k. Ej. k=4 fija las primeras 4 visitas. "
            "Omitir = optimizar toda la ruta desde cero."
        ),
    )
    tiempo_por_cliente_min: float | None = Field(
        default=None,
        gt=0,
        le=240,
        description="Minutos de atención por visita para el tiempo total real (default env RUTA_TIEMPO_ATENCION_CLIENTE_MIN).",
    )


class AsignarDiaAtencionBody(BaseModel):
    bsale_id: int = Field(..., ge=1)
    dia_atencion: str = Field(..., min_length=1, description="Día de atención (ej. Lunes)")


class ObservacionRuteroBody(BaseModel):
    """`cliente_id` = PK `bsale.rutero.id` (fila del cliente en rutero)."""

    cliente_id: int = Field(..., ge=1)
    observaciones: str | None = Field(default=None, description="Texto libre; vacío o null → NULL en BD")


class RuteroSabadoPatchBody(BaseModel):
    """Marca o quita atención de sábado vía ``dia_extra`` (valor fijo ``sabado`` en BD)."""

    model_config = ConfigDict(str_strip_whitespace=True)

    rut_clean: str = Field(..., min_length=1, max_length=64)
    activo: bool


class RuteroTipoAtencionPatchBody(BaseModel):
    """Valores UI típicos `TERRENO` / `TELEFONICO`; se persisten como `terreno` / `telefonico` (CHECK en BD)."""

    tipo_atencion: str = Field(..., min_length=1, description="terreno o telefonico (cualquier mayúscula)")


def _normalize_rutero_tipo_atencion(raw: str) -> str:
    t = (raw or "").strip().lower()
    if "telefon" in t:
        return "telefonico"
    if t in ("terreno", ""):
        return "terreno"
    raise HTTPException(
        status_code=400,
        detail="tipo_atencion debe ser TERRENO o TELEFONICO",
    )


class OptimizarRutaDesdeBody(BaseModel):
    """Reoptimiza solo la cola a partir de un índice (0 = primer cliente = toda la ruta)."""

    vendedor: str = Field(..., min_length=1)
    dia: str = Field(..., min_length=1)
    desde_indice: int = Field(
        ...,
        ge=0,
        description="Índice 0-based del primer cliente del tramo a reoptimizar (antes = [:i] intacto)",
    )
    tiempo_por_cliente_min: float | None = Field(
        default=None,
        gt=0,
        le=240,
        description="Minutos de atención por visita para tiempo total real.",
    )


router = APIRouter(prefix="/distribuidora", tags=["Distribuidora"])

# Rutas / ORS / mapa: excluye telefónicos por `dia_atencion` (valor Bsale "telefonico") y por `tipo_atencion`.
# Solo filtros en lectura/escritura de orden; no se mutan columnas.
_SQL_RUTA_EXCL_DIA_TELEFONICO = "\n          AND LOWER(TRIM(COALESCE(dia_atencion::text, ''))) <> 'telefonico'"
_SQL_RUTA_EXCL_TIPO_TELEFONICO = "\n          AND LOWER(COALESCE(tipo_atencion::text, '')) <> 'telefonico'"

# Día operativo: sábado extra (`dia_extra`) sin alterar la tabla; el resto sigue `dia_atencion`.
_SQL_DIA_OPERATIVO_SQL_BARE = """(
    CASE
        WHEN LOWER(TRIM(COALESCE(dia_extra::text, ''))) = 'sabado' THEN 'Sabado'
        ELSE TRIM(COALESCE(dia_atencion::text, ''))
    END
)"""
_SQL_MATCH_DIA_OPERATIVO_BARE = (
    f"\n          AND LOWER(TRIM({_SQL_DIA_OPERATIVO_SQL_BARE})) = LOWER(TRIM(%s))"
)


def _norm_vendedor(s: str | None) -> str:
    """Código vendedor estable (minúsculas, sin espacios extremos); alineado a sync_rutero y filtros SQL."""
    return (s or "").strip().lower()


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


def _orden_manual_valido(om: object) -> bool:
    """Solo orden_manual explícito y > 0 cuenta como visita ordenada en BD."""
    if om is None:
        return False
    try:
        return int(om) > 0
    except (TypeError, ValueError):
        return False


def _ordenar_clientes_por_distancia_desde_base(base: dict, clientes: list[dict]) -> list[dict]:
    """Orden de respaldo: más cercano a la base primero (Haversine)."""
    blat = float(base["lat"])
    blon = float(base["lon"])

    def dist_m(c: dict) -> float:
        try:
            return float(
                haversine_m(blat, blon, float(c["lat"]), float(c["lon"])),
            )
        except (TypeError, ValueError, KeyError):
            return 1e18

    out = [dict(x) for x in sorted(clientes, key=dist_m)]
    for i, row in enumerate(out, start=1):
        row["orden_visita"] = i
    return out


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


def _orden_actual_rutero_terreno(
    base: dict,
    manual_raw: list[dict],
    clientes_rows: list[dict],
) -> list[dict]:
    """
    Orden de visita actual: orden_manual > 0 en BD; si no hay ninguno, orden por distancia a la base
    (misma heurística que GET ruta-detalle sin manual).
    """
    clientes = _clientes_validos_coords(clientes_rows)
    if not clientes:
        return []
    manual_solo_validos = [c for c in manual_raw if _orden_manual_valido(c.get("orden_manual"))]
    manual_valid = _clientes_validos_coords(manual_solo_validos)
    if len(manual_valid) > 0:
        return _clientes_orden_manual(manual_valid, clientes)
    return _ordenar_clientes_por_distancia_desde_base(base, clientes)


def _tiempo_atencion_cliente_default() -> float:
    return float(os.getenv("RUTA_TIEMPO_ATENCION_CLIENTE_MIN", "10"))


def _with_tiempos_reales(resp: dict, tiempo_por_cliente_min: float | None = None) -> dict:
    """Añade minutos de atención y tiempo total real (conducción ORS + n × tiempo por cliente)."""
    if not isinstance(resp, dict):
        return resp
    if resp.get("error"):
        return resp
    clientes = resp.get("clientes")
    if not isinstance(clientes, list):
        return resp
    n = len(clientes)
    min_cond = float(resp.get("minutos_totales") or 0.0)
    t_cli = tiempo_por_cliente_min if tiempo_por_cliente_min is not None else _tiempo_atencion_cliente_default()
    min_at = float(n) * t_cli
    return {
        **resp,
        "minutos_conduccion": round(min_cond, 2),
        "minutos_atencion": round(min_at, 2),
        "minutos_total_real": round(min_cond + min_at, 2),
        "tiempo_por_cliente_min": t_cli,
    }


def _coords_ors_base_clientes_base(base: dict, clientes_ordenados: list[dict]) -> list[list[float]]:
    """
    Secuencia enviada a ORS Directions para una ruta cerrada: BASE → clientes → BASE.
    Formato ORS: [lon, lat] por punto.
    """
    coords: list[list[float]] = [[float(base["lon"]), float(base["lat"])]]
    for c in clientes_ordenados:
        coords.append([float(c["lon"]), float(c["lat"])])
    coords.append([float(base["lon"]), float(base["lat"])])
    return coords


def _ors_max_waypoints_per_chunk() -> int:
    """ORS limita waypoints por petición; troceamos rutas largas (default 20, máx. 50)."""
    raw = os.getenv("ORS_MAX_WAYPOINTS_PER_REQUEST", "20").strip()
    try:
        n = int(raw)
    except ValueError:
        n = 20
    return max(5, min(n, 50))


def _lonlat_casi_iguales(a: list[float], b: list[float], eps: float = 1e-5) -> bool:
    return abs(float(a[0]) - float(b[0])) < eps and abs(float(a[1]) - float(b[1])) < eps


def _ors_dividir_tramos_solapados(coords: list[list[float]], max_wp: int) -> list[list[list[float]]]:
    """
    Lista de tramos consecutivos de hasta ``max_wp`` puntos cada uno.
    Solape de 1 waypoint entre tramos para mantener continuidad (orden preservado).
    """
    n = len(coords)
    if n <= max_wp:
        return [coords]
    chunks: list[list[list[float]]] = []
    i = 0
    while i < n:
        j = min(i + max_wp, n)
        chunks.append(coords[i:j])
        if j >= n:
            break
        i = j - 1
    return chunks


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


def _ors_route_merge_chunks(
    coords: list[list[float]],
    max_wp: int | None = None,
) -> tuple[object | None, float, float] | None:
    """
    Varias peticiones ORS Directions en tramos (mismo orden de waypoints), une geometrías
    en un LineString GeoJSON [lon, lat] y suma distancia/duración de cada tramo.
    """
    from backend.utils.ors_client import get_route

    if len(coords) < 2:
        return None
    cap = max_wp if max_wp is not None else _ors_max_waypoints_per_chunk()
    chunks = _ors_dividir_tramos_solapados(coords, cap)
    merged: list[list[float]] = []
    total_km = 0.0
    total_mins = 0.0

    for chunk in chunks:
        if len(chunk) < 2:
            return None
        try:
            ors_data = get_route(chunk)
        except Exception:
            return None
        if "routes" not in ors_data or not ors_data["routes"]:
            return None
        route = ors_data["routes"][0]
        summary = route.get("summary") or {}
        dist_m = summary.get("distance")
        dur_s = summary.get("duration")
        if dist_m is not None:
            total_km += float(dist_m) / 1000.0
        if dur_s is not None:
            total_mins += float(dur_s) / 60.0
        part = _route_geometry_to_lonlat_coords(route.get("geometry"))
        if not part:
            return None
        if merged:
            if _lonlat_casi_iguales(part[0], merged[-1]):
                part = part[1:]
            elif part[0] == merged[-1]:
                part = part[1:]
        merged.extend(part)

    if not merged:
        return None
    geometry: object = {"type": "LineString", "coordinates": merged}
    return (geometry, total_km, total_mins)


def _geometria_ruta_secuencial(
    base: dict,
    clientes_ordenados: list[dict],
) -> tuple[object | None, float, float] | None:
    """
    ORS en el orden fijo (base → visitas → base). Rutas largas: varios tramos ORS unidos
    (``ORS_MAX_WAYPOINTS_PER_REQUEST``, default 20 waypoints c/u, solape 1).
    """
    if not clientes_ordenados:
        return (None, 0.0, 0.0)
    coords = _coords_ors_base_clientes_base(base, clientes_ordenados)
    if len(coords) < 2:
        return None
    return _ors_route_merge_chunks(coords, _ors_max_waypoints_per_chunk())


def _ors_reoptimizar_cola(prev: dict | None, despues: list[dict], base: dict) -> dict:
    """
    Reordena solo la cola con el mismo pipeline local (ángulo + 2-opt) que la ruta completa.
    ORS no decide el orden; solo se usa después vía `_geom_km_ruta_completa` en el endpoint.
    """
    if not despues:
        return {"error": "No hay tramo a reoptimizar"}
    out = optimizar_cola_desde_ancla(prev, despues, base)
    return {"clientes": out}


def _lista_visita_actual_ruta(
    base: dict,
    manual_raw: list[dict],
    clientes_rows: list[dict],
) -> tuple[list[dict] | None, dict | None]:
    """
    Secuencia alineada con GET ruta-detalle: manual válido (>0) si hay; si no, orden por distancia a la base.
    """
    clientes = _clientes_validos_coords(clientes_rows)
    if not clientes:
        return [], None
    orden = _orden_actual_rutero_terreno(base, manual_raw, clientes_rows)
    return orden, None


def _geom_km_encadenado(
    base: dict,
    clientes_ordenados: list[dict],
) -> tuple[object | None, float, float] | None:
    """Misma lógica que `_geometria_ruta_secuencial` (ORS por tramos unidos)."""
    return _geometria_ruta_secuencial(base, clientes_ordenados)


def _geom_km_ruta_completa(
    base: dict,
    clientes_ordenados: list[dict],
) -> tuple[object | None, float, float] | None:
    """Geometría y km/min ORS para BASE→clientes→BASE (trocea automáticamente rutas largas)."""
    return _geometria_ruta_secuencial(base, clientes_ordenados)


def _ors_optimize_from_base_clientes(base: dict, clientes: list[dict]) -> dict:
    """
    Orden de visitas: preorden angular + 2-opt local (una sola secuencia, sin particionar).
    ORS (OpenRouteService) solo calcula geometría y km/min en el orden ya fijado.
    """
    if not clientes:
        return {
            "km_totales": 0.0,
            "minutos_totales": 0.0,
            "geometry": None,
            "clientes": [],
        }

    inicial, optimizado = optimizar_secuencia_cerrado(base, clientes)
    for i, row in enumerate(optimizado, start=1):
        row["orden_visita"] = i

    geo = _geom_km_ruta_completa(base, optimizado)
    if geo is None:
        km_h = tour_length_closed(base, optimizado) / 1000.0
        return {
            "km_totales": round(km_h, 2),
            "minutos_totales": 0.0,
            "geometry": None,
            "clientes": optimizado,
            "advertencia_ors": (
                "No se pudo trazar la ruta con ORS; se guardará el orden optimizado y "
                "los km mostrados son aproximados (Haversine, ida y vuelta a la base)."
            ),
        }

    geometry, km_totales, minutos_totales = geo
    log_resumen_optimizacion(
        base=base,
        inicial=inicial,
        optimizado=optimizado,
        km_ors=km_totales,
        min_ors=minutos_totales,
        bloque_k=None,
    )
    return {
        "km_totales": km_totales,
        "minutos_totales": minutos_totales,
        "geometry": geometry,
        "clientes": optimizado,
    }


def _persistir_orden_manual_vendedor_dia(
    conn,
    v: str,
    d: str,
    clientes_con_orden_visita: list[dict],
) -> None:
    """NULL orden_manual para el día y asigna según orden_visita (1..n)."""
    v = _norm_vendedor(v)
    d = (d or "").strip()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE bsale.rutero
        SET orden_manual = NULL
        WHERE company_id = 3
          AND activo = TRUE
          AND LOWER(TRIM(COALESCE(vendedor::text, ''))) = %s
        """
        + _SQL_MATCH_DIA_OPERATIVO_BARE
        + """
          AND LOWER(COALESCE(tipo_atencion, '')) <> 'telefonico'
        """
        + _SQL_RUTA_EXCL_DIA_TELEFONICO
        + "\n        ",
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
              AND LOWER(TRIM(COALESCE(vendedor::text, ''))) = %s
            """
            + _SQL_MATCH_DIA_OPERATIVO_BARE
            + """
              AND LOWER(COALESCE(tipo_atencion, '')) <> 'telefonico'
            """
            + _SQL_RUTA_EXCL_DIA_TELEFONICO
            + "\n            ",
            (ov_int, int(bid), v, d),
        )
    conn.commit()
    cur.close()


def _cargar_contexto_ruta(v: str, d: str) -> tuple[dict, list[dict], list[dict]]:
    """Punto base + filas rutero con orden_manual + todas las filas del día (terreno)."""
    v = _norm_vendedor(v)
    d = (d or "").strip()
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                LOWER(TRIM(COALESCE(vendedor::text, ''))) AS vendedor,
                nombre,
                lat,
                lon
            FROM bsale.puntos_base
            WHERE LOWER(TRIM(COALESCE(vendedor::text, ''))) = %s
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
                LOWER(TRIM(COALESCE(vendedor::text, ''))) AS vendedor,
                dia_atencion,
                dia_extra,
                """
            + _SQL_DIA_OPERATIVO_SQL_BARE
            + """ AS dia_operativo,
                municipality,
                """
            + B_LAT_AS
            + ",\n                "
            + B_LON_AS
            + """,
                tipo_atencion,
                orden_ruta,
                orden_manual
            FROM bsale.rutero
            WHERE company_id = 3
              AND activo = TRUE
              AND LOWER(TRIM(COALESCE(vendedor::text, ''))) = %s
            """
            + _SQL_MATCH_DIA_OPERATIVO_BARE
            + f"""
              AND {WHERE_HAS_GEOREF_BARE}
            """
            + _SQL_RUTA_EXCL_DIA_TELEFONICO
            + _SQL_RUTA_EXCL_TIPO_TELEFONICO
            + """
              AND orden_manual IS NOT NULL
              AND orden_manual > 0
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
                LOWER(TRIM(COALESCE(vendedor::text, ''))) AS vendedor,
                dia_atencion,
                dia_extra,
                """
            + _SQL_DIA_OPERATIVO_SQL_BARE
            + """ AS dia_operativo,
                municipality,
                """
            + B_LAT_AS
            + ",\n                "
            + B_LON_AS
            + """,
                tipo_atencion,
                orden_ruta,
                orden_manual
            FROM bsale.rutero
            WHERE company_id = 3
              AND activo = TRUE
              AND LOWER(TRIM(COALESCE(vendedor::text, ''))) = %s
            """
            + _SQL_MATCH_DIA_OPERATIVO_BARE
            + f"""
              AND {WHERE_HAS_GEOREF_BARE}
            """
            + _SQL_RUTA_EXCL_DIA_TELEFONICO
            + _SQL_RUTA_EXCL_TIPO_TELEFONICO
            + """
            ORDER BY orden_ruta NULLS LAST, bsale_id
            """,
            (v, d),
        )
        clientes = _rows_to_json(cur)
        cur.close()
    finally:
        conn.close()

    return base, manual_raw, clientes


def _build_ruta_detalle_response(v: str, d: str) -> dict:
    """GET /ruta-detalle: orden manual (>0) si hay; si no, orden por distancia a la base + trazado ORS si existe."""
    v = _norm_vendedor(v)
    d = (d or "").strip()
    base, manual_raw, clientes_rows = _cargar_contexto_ruta(v, d)
    clientes = _clientes_validos_coords(clientes_rows)
    manual_valid = _clientes_validos_coords(manual_raw)

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
            geometry = None
            km_totales = round(tour_length_closed(base, clientes_ordenados) / 1000.0, 2)
            minutos_totales = 0.0
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

    clientes_ordenados = _ordenar_clientes_por_distancia_desde_base(base, clientes)
    geo = _geom_km_ruta_completa(base, clientes_ordenados)
    if geo is None:
        km_h = tour_length_closed(base, clientes_ordenados) / 1000.0
        return {
            "vendedor": v,
            "dia": d,
            "km_totales": round(km_h, 2),
            "minutos_totales": 0.0,
            "geometry": None,
            "clientes": clientes_ordenados,
            "base": _base_respuesta_publica(base),
            "sin_orden_manual": True,
        }
    geometry, km_totales, minutos_totales = geo
    return {
        "vendedor": v,
        "dia": d,
        "km_totales": km_totales,
        "minutos_totales": minutos_totales,
        "geometry": geometry,
        "clientes": clientes_ordenados,
        "base": _base_respuesta_publica(base),
        "sin_orden_manual": True,
    }


# Colores sugeridos por orden de día (Lunes → Sábado típico)
# Paleta más saturada y distinguible en mapas claros (resumen semanal / gerencia).
_COLORES_RUTA_SEMANA = (
    "#b91c1c",  # red-700
    "#1d4ed8",  # blue-700
    "#15803d",  # green-700
    "#a16207",  # yellow-700
    "#7e22ce",  # purple-700
    "#c2410c",  # orange-700
    "#0e7490",  # cyan-700
    "#4338ca",  # indigo-700
)

# Umbral suave: km por cliente por encima → marca alerta_calidad en el resumen
_KM_POR_CLIENTE_ALERTA = 25.0


def _dia_normalizado(s: str) -> str:
    t = s.strip().lower()
    for a, b in (("á", "a"), ("é", "e"), ("í", "i"), ("ó", "o"), ("ú", "u")):
        t = t.replace(a, b)
    return t


def _dia_sort_key(dia: str) -> tuple:
    nd = _dia_normalizado(dia)
    orden = ("lunes", "martes", "miercoles", "jueves", "viernes", "sabado", "domingo")
    for i, ref in enumerate(orden):
        if nd == ref or nd.startswith(ref[:3]):
            return (0, i)
    return (1, nd)


def _sort_dias_semana(distinct: list[str]) -> list[str]:
    return sorted({str(d).strip() for d in distinct if d and str(d).strip()}, key=_dia_sort_key)


def _color_resumen_dia(dia: str, idx: int) -> str:
    """Sábado operativo siempre morado (#7e22ce); el resto rota en la paleta semanal."""
    if _dia_normalizado(dia) == "sabado":
        return "#7e22ce"
    return _COLORES_RUTA_SEMANA[idx % len(_COLORES_RUTA_SEMANA)]


def _dias_rutero_vendedor(v: str) -> list[str]:
    v = _norm_vendedor(v)
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT DISTINCT TRIM(dia_op) AS d
            FROM (
                SELECT """
            + _SQL_DIA_OPERATIVO_SQL_BARE
            + """ AS dia_op
                FROM bsale.rutero
                WHERE company_id = 3
                  AND activo = TRUE
                  AND LOWER(TRIM(COALESCE(vendedor::text, ''))) = %s
            """
            + _SQL_RUTA_EXCL_DIA_TELEFONICO
            + _SQL_RUTA_EXCL_TIPO_TELEFONICO
            + """
            ) x
            WHERE dia_op IS NOT NULL
              AND TRIM(dia_op) <> ''
            """,
            (v,),
        )
        raw = [r[0] for r in cur.fetchall() if r and r[0]]
        cur.close()
    finally:
        conn.close()
    return _sort_dias_semana(raw)


def _build_resumen_vendedor_response(v: str) -> dict:
    """
    Resumen semanal: reutiliza GET ruta-detalle / optimización persistida vía orden_manual.
    """
    v = _norm_vendedor(v)
    dias = _dias_rutero_vendedor(v)
    salida_dias: list[dict] = []
    km_total = 0.0
    min_total = 0.0
    clientes_total = 0

    for idx, dia in enumerate(dias):
        det = _build_ruta_detalle_response(v, dia)
        if isinstance(det, dict) and det.get("error"):
            logger.warning("resumen_vendedor v=%s dia=%s sin ruta: %s", v, dia, det.get("error"))
            continue
        clis = det.get("clientes") or []
        n_c = len(clis)
        km = float(det.get("km_totales") or 0.0)
        mins = float(det.get("minutos_totales") or 0.0)
        km_por_cliente = km / n_c if n_c else 0.0
        color = _color_resumen_dia(dia, idx)
        salida_dias.append(
            {
                "dia": dia,
                "color": color,
                "km_totales": round(km, 2),
                "minutos_totales": round(mins, 1),
                "clientes_count": n_c,
                "geometry": det.get("geometry"),
                "base": det.get("base"),
                "clientes": clis,
                "alerta_calidad": bool(n_c and km_por_cliente > _KM_POR_CLIENTE_ALERTA),
                "km_por_cliente": round(km_por_cliente, 2),
            }
        )
        km_total += km
        min_total += mins
        clientes_total += n_c

    n_dias = len(salida_dias)
    kms_vals = [d["km_totales"] for d in salida_dias]

    return {
        "vendedor": v,
        "dias": salida_dias,
        "km_total_semana": round(km_total, 2),
        "min_total_semana": round(min_total, 1),
        "clientes_total_semana": clientes_total,
        "promedio_km_por_dia": round(km_total / n_dias, 2) if n_dias else 0.0,
        "km_dia_mas_largo": max(kms_vals) if kms_vals else 0.0,
        "km_dia_mas_corto": min(kms_vals) if kms_vals else 0.0,
    }


@router.get("/ruta-detalle")
def get_ruta_detalle(
    vendedor: str = Query(..., min_length=1, description="Código vendedor (ej. vendedor_1)"),
    dia: str = Query(
        ...,
        min_length=1,
        description="Día operativo (ej. Lunes, Sabado): dia_extra=sabado → Sabado; resto = dia_atencion.",
    ),
):
    """Ruta: si hay orden_manual se respeta la secuencia (sin reoptimizar); si no, orden local + ORS trazado."""
    v = _norm_vendedor(vendedor)
    d = dia.strip()
    if not v or not d:
        raise HTTPException(status_code=400, detail="vendedor y dia son obligatorios")

    return _with_tiempos_reales(_build_ruta_detalle_response(v, d))


@router.get("/ruta-sugerencias")
def get_ruta_sugerencias(
    vendedor: str = Query(..., min_length=1, description="Código vendedor"),
    dia: str = Query(..., min_length=1, description="Día de atención"),
    min_delta_km: float = Query(
        0.5,
        ge=0.05,
        le=500,
        description="Ahorro mínimo estimado (km, Haversine en tramo local) para listar el swap",
    ),
):
    """
    Sugerencias puntuales (p. ej. intercambiar dos visitas consecutivas) según la **misma secuencia**
    que expone GET /ruta-detalle (índices alineados con la lista del panel). No persiste cambios.
    """
    v = _norm_vendedor(vendedor)
    d = dia.strip()
    if not v or not d:
        raise HTTPException(status_code=400, detail="vendedor y dia son obligatorios")

    det = _build_ruta_detalle_response(v, d)
    if not isinstance(det, dict):
        raise HTTPException(status_code=500, detail="respuesta de ruta inválida")
    if det.get("error"):
        return {
            "vendedor": v,
            "dia": d,
            "metrica": "haversine_tramo_local",
            "min_delta_km": float(min_delta_km),
            "sugerencias": [],
            "error": det.get("error"),
            "nota": "Sin sugerencias porque la ruta no pudo calcularse.",
        }

    base_pub = det.get("base") if isinstance(det.get("base"), dict) else {}
    try:
        base = {
            "lat": float(base_pub.get("lat")),
            "lon": float(base_pub.get("lon")),
            "nombre": base_pub.get("nombre"),
        }
    except (TypeError, ValueError):
        return {
            "vendedor": v,
            "dia": d,
            "metrica": "haversine_tramo_local",
            "min_delta_km": float(min_delta_km),
            "sugerencias": [],
            "error": "base_sin_coordenadas",
            "nota": "La base no tiene lat/lon válidos.",
        }

    raw_clientes = det.get("clientes")
    orden: list[dict] = []
    if isinstance(raw_clientes, list):
        orden = [dict(x) for x in raw_clientes if isinstance(x, dict)]

    sugs = sugerencias_swap_adyacentes(base, orden, min_delta_km=min_delta_km)
    return {
        "vendedor": v,
        "dia": d,
        "metrica": "haversine_tramo_local",
        "min_delta_km": float(min_delta_km),
        "sugerencias": sugs,
        "nota": (
            "Solo estimación local; no se modifica la ruta hasta que el usuario aplique el cambio "
            "(orden_manual en la app)."
        ),
    }


@router.post("/optimizar-ruta")
def post_optimizar_ruta(body: OptimizarRutaBody):
    """
    Optimización híbrida: orden local (sectores + 2-opt penalizado) y ORS solo para trazar.
    Si `bloque_hasta_indice` está definido, respeta el orden actual en BD hasta ese índice y solo reordena la cola.
    """
    v = _norm_vendedor(body.vendedor)
    d = body.dia.strip()
    if not v or not d:
        raise HTTPException(status_code=400, detail="vendedor y dia son obligatorios")

    base, manual_raw, clientes_rows = _cargar_contexto_ruta(v, d)
    clientes = _clientes_validos_coords(clientes_rows)
    if len(clientes) == 0:
        raise HTTPException(
            status_code=400,
            detail="No hay clientes en terreno para este día",
        )

    if body.bloque_hasta_indice is None:
        payload = _ors_optimize_from_base_clientes(base, clientes)
    else:
        k = int(body.bloque_hasta_indice)
        orden_actual = _orden_actual_rutero_terreno(base, manual_raw, clientes_rows)
        n = len(orden_actual)
        if k < 0 or k > n:
            raise HTTPException(status_code=400, detail="bloque_hasta_indice fuera de rango")
        if k == n:
            raise HTTPException(
                status_code=400,
                detail="No hay tramo variable para optimizar: el bloque cubre todos los clientes",
            )
        fijos = [dict(x) for x in orden_actual[:k]]
        variable = [dict(x) for x in orden_actual[k:]]
        prev = fijos[-1] if fijos else None
        tail_opt = optimizar_cola_desde_ancla(prev, variable, base)
        merged = fijos + tail_opt
        for i, row in enumerate(merged, start=1):
            row["orden_visita"] = i
        geo = _geom_km_ruta_completa(base, merged)
        if geo is None:
            km_h = tour_length_closed(base, merged) / 1000.0
            payload = {
                "km_totales": round(km_h, 2),
                "minutos_totales": 0.0,
                "geometry": None,
                "clientes": merged,
                "advertencia_ors": (
                    "No se pudo trazar la ruta con ORS; se guardará el orden y los km son aproximados (Haversine)."
                ),
            }
        else:
            geometry, km_totales, minutos_totales = geo
            log_resumen_optimizacion(
                base=base,
                inicial=list(orden_actual),
                optimizado=merged,
                km_ors=km_totales,
                min_ors=minutos_totales,
                bloque_k=k,
            )
            payload = {
                "km_totales": km_totales,
                "minutos_totales": minutos_totales,
                "geometry": geometry,
                "clientes": merged,
            }

    if "error" in payload and (
        not isinstance(payload.get("clientes"), list) or len(payload["clientes"]) == 0
    ):
        return payload

    conn = get_connection()
    try:
        _persistir_orden_manual_vendedor_dia(conn, v, d, payload["clientes"])
    finally:
        conn.close()

    out = {
        "vendedor": v,
        "dia": d,
        "base": _base_respuesta_publica(base),
        "km_totales": payload["km_totales"],
        "minutos_totales": payload["minutos_totales"],
        "geometry": payload["geometry"],
        "clientes": payload["clientes"],
        "bloque_hasta_indice": body.bloque_hasta_indice,
    }
    adv = payload.get("advertencia_ors")
    if isinstance(adv, str) and adv.strip():
        out["advertencia_ors"] = adv.strip()
    return _with_tiempos_reales(out, body.tiempo_por_cliente_min)


@router.post("/optimizar-ruta-desde")
def post_optimizar_ruta_desde(body: OptimizarRutaDesdeBody):
    """
    Mantiene el orden de los primeros `desde_indice` clientes y reoptimiza la cola con el mismo
    pipeline local (ángulo desde el último fijo + 2-opt). ORS solo traza la ruta completa al final.
    Persiste orden_manual = 1..n para el día.
    """
    v = _norm_vendedor(body.vendedor)
    d = body.dia.strip()
    if not v or not d:
        raise HTTPException(status_code=400, detail="vendedor y dia son obligatorios")

    base, manual_raw, clientes_rows = _cargar_contexto_ruta(v, d)
    clientes_validos = _clientes_validos_coords(clientes_rows)
    if len(clientes_validos) == 0:
        raise HTTPException(
            status_code=400,
            detail="No hay clientes en terreno para este día",
        )

    orden = _orden_actual_rutero_terreno(base, manual_raw, clientes_rows)
    if not orden:
        raise HTTPException(
            status_code=400,
            detail="No hay clientes terreno con coordenadas válidas para este día",
        )

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
        km_h = tour_length_closed(base, merged) / 1000.0
        conn = get_connection()
        try:
            _persistir_orden_manual_vendedor_dia(conn, v, d, merged)
        finally:
            conn.close()
        out = {
            "vendedor": v,
            "dia": d,
            "base": _base_respuesta_publica(base),
            "km_totales": round(km_h, 2),
            "minutos_totales": 0.0,
            "geometry": None,
            "clientes": merged,
            "desde_indice": body.desde_indice,
            "advertencia_ors": (
                "No se pudo trazar la ruta con ORS; se guardó el orden y los km son aproximados (Haversine)."
            ),
        }
        return _with_tiempos_reales(out, body.tiempo_por_cliente_min)
    geometry, km_totales, minutos_totales = geo

    conn = get_connection()
    try:
        _persistir_orden_manual_vendedor_dia(conn, v, d, merged)
    finally:
        conn.close()

    out = {
        "vendedor": v,
        "dia": d,
        "base": _base_respuesta_publica(base),
        "km_totales": km_totales,
        "minutos_totales": minutos_totales,
        "geometry": geometry,
        "clientes": merged,
        "desde_indice": body.desde_indice,
    }
    return _with_tiempos_reales(out, body.tiempo_por_cliente_min)


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
                LOWER(TRIM(COALESCE(vendedor::text, ''))) AS vendedor,
                TRIM(dia_op) AS dia
            FROM (
                SELECT
                    vendedor,
                    """
            + _SQL_DIA_OPERATIVO_SQL_BARE
            + """ AS dia_op
                FROM bsale.rutero
                WHERE company_id = 3
                  AND activo = TRUE
                  AND """
            + WHERE_HAS_GEOREF_BARE
            + """
                  AND TRIM(COALESCE(vendedor::text, '')) <> ''
            """
            + _SQL_RUTA_EXCL_DIA_TELEFONICO
            + _SQL_RUTA_EXCL_TIPO_TELEFONICO
            + """
            ) sub
            WHERE TRIM(COALESCE(dia_op, '')) <> ''
            ORDER BY vendedor, dia
            LIMIT %s
            """,
            (max_pares,),
        )
        pares = _rows_to_json(cur)

        resultados: list[dict] = []
        for p in pares:
            v = _norm_vendedor(p.get("vendedor") if isinstance(p.get("vendedor"), str) else None)
            d = (p.get("dia") or "").strip()
            if not v or not d:
                continue

            cur.execute(
                """
                SELECT
                    LOWER(TRIM(COALESCE(vendedor::text, ''))) AS vendedor,
                    nombre,
                    lat,
                    lon
                FROM bsale.puntos_base
                WHERE LOWER(TRIM(COALESCE(vendedor::text, ''))) = %s
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
                    LOWER(TRIM(COALESCE(vendedor::text, ''))) AS vendedor,
                    dia_atencion,
                    dia_extra,
                    municipality,
                    """
                + B_LAT_AS
                + ",\n                    "
                + B_LON_AS
                + """,
                    tipo_atencion,
                    orden_ruta
                FROM bsale.rutero
                WHERE company_id = 3
                  AND activo = TRUE
                  AND LOWER(TRIM(COALESCE(vendedor::text, ''))) = %s
                """
                + _SQL_MATCH_DIA_OPERATIVO_BARE
                + f"""
                  AND {WHERE_HAS_GEOREF_BARE}
                """
                + _SQL_RUTA_EXCL_DIA_TELEFONICO
                + _SQL_RUTA_EXCL_TIPO_TELEFONICO
                + """
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

            coords = _coords_ors_base_clientes_base(base, clientes)

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


# SELECT compartido: GET /rutero y POST /observacion (fila actualizada).
_RUTERO_FILA_SELECT = (
    """
            SELECT
                r.id,
                LOWER(TRIM(COALESCE(r.vendedor::text, ''))) AS vendedor,
                r.dia_atencion,
                NULLIF(TRIM(COALESCE(r.dia_extra::text, '')), '') AS dia_extra,
                r.orden_manual,
                r.orden_ruta,
                NULLIF(TRIM(r.rut_clean::text), '') AS rut,
                r.bsale_id,
                NULLIF(TRIM(r.nombre_fantasia), '') AS nombre_fantasia,
                NULLIF(
                    TRIM(CONCAT_WS(' ', NULLIF(TRIM(r.first_name), ''), NULLIF(TRIM(r.last_name), ''))),
                    ''
                ) AS razon_social,
                NULLIF(TRIM(r.address), '') AS direccion,
                NULLIF(TRIM(r.municipality), '') AS municipality,
                NULLIF(TRIM(r.phone), '') AS telefono,
                r.tipo_atencion,
                r.activo,
                """
    + R_LAT_AS
    + ",\n                "
    + R_LON_AS
    + """,
                r.observaciones,
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
                ) AS cliente_nombre
            FROM bsale.rutero r
"""
)


def _rutero_list_where_order(
    vendedor: str | None,
    dia: str | None,
    tipo: str | None,
    geo: str | None,
    dia_estado: str | None,
    sabado: str | None = None,
) -> tuple[str, list]:
    """
    Filtros listado rutero (gestión): no excluye telefónicos ni georef (distinto de /mapa u ORS).
    - vendedor vacío: todos los vendedores.
    - dia vacío: sin filtrar por valor de dia_atencion (salvo dia_estado con/sin).
    - dia_estado=sin: solo sin día; en ese caso se ignora el filtro por día concreto.
    - sabado=con|sin: filtro por ``LOWER(TRIM(dia_extra)) = 'sabado'`` (insensible a mayúsculas/espacios).
    """
    wheres = ["r.company_id = 3", "r.activo = TRUE"]
    params: list = []

    v = _norm_vendedor(vendedor) if vendedor else ""
    if v:
        wheres.append("LOWER(TRIM(COALESCE(r.vendedor::text, ''))) = %s")
        params.append(v)

    de = (dia_estado or "").strip().lower()
    if de == "sin":
        wheres.append(
            "(r.dia_atencion IS NULL OR LOWER(TRIM(COALESCE(r.dia_atencion::text, ''))) = '')"
        )
    elif de == "con":
        wheres.append(
            "(r.dia_atencion IS NOT NULL AND LOWER(TRIM(COALESCE(r.dia_atencion::text, ''))) <> '')"
        )

    d_week = (dia or "").strip()
    if d_week and de != "sin":
        wheres.append("LOWER(TRIM(COALESCE(r.dia_atencion::text, ''))) = LOWER(TRIM(%s))")
        params.append(d_week)

    tipo_f = (tipo or "").strip().lower()
    if tipo_f == "telefonico":
        wheres.append(
            "(LOWER(TRIM(COALESCE(r.tipo_atencion::text, ''))) = 'telefonico' "
            "OR LOWER(TRIM(COALESCE(r.dia_atencion::text, ''))) = 'telefonico')"
        )
    elif tipo_f == "terreno":
        wheres.append(
            "(LOWER(TRIM(COALESCE(r.tipo_atencion::text, ''))) <> 'telefonico' "
            "OR r.tipo_atencion IS NULL) "
            "AND LOWER(TRIM(COALESCE(r.dia_atencion::text, ''))) <> 'telefonico'"
        )

    geo_f = (geo or "").strip().lower()
    if geo_f == "con":
        wheres.append(WHERE_HAS_GEOREF_R)
    elif geo_f == "sin":
        wheres.append(WHERE_SIN_GEOREF_R)

    sab = (sabado or "").strip().lower()
    if sab == "con":
        wheres.append("LOWER(TRIM(COALESCE(r.dia_extra::text, ''))) = 'sabado'")
    elif sab == "sin":
        wheres.append(
            "(r.dia_extra IS NULL OR LOWER(TRIM(COALESCE(r.dia_extra::text, ''))) = '' "
            "OR LOWER(TRIM(COALESCE(r.dia_extra::text, ''))) <> 'sabado')"
        )

    where_sql = " AND ".join(wheres)
    return where_sql, params


_RUTERO_LIST_ORDER = """
            ORDER BY
                CASE WHEN LOWER(TRIM(COALESCE(r.dia_extra::text, ''))) = 'sabado' THEN 0 ELSE 1 END,
                LOWER(TRIM(COALESCE(r.vendedor::text, ''))),
                LOWER(TRIM(COALESCE(r.dia_atencion::text, ''))) NULLS LAST,
                r.nombre_fantasia ASC NULLS LAST,
                r.orden_manual ASC NULLS LAST,
                r.orden_ruta ASC NULLS LAST,
                r.bsale_id
            """


@router.get("/rutero")
def get_rutero(
    vendedor: str | None = Query(None, description="Código vendedor; omitir o vacío = todos"),
    dia: str | None = Query(None, description="dia_atencion (ej. lunes); vacío = todos salvo dia_estado"),
    tipo: str | None = Query(None, description="terreno | telefonico"),
    geo: str | None = Query(None, description="con | sin — coordenadas en rutero"),
    dia_estado: str | None = Query(None, description="con | sin — tiene dia_atencion asignado"),
    sabado: str | None = Query(
        None,
        description="con | sin — LOWER(TRIM(dia_extra)) = 'sabado' (atención sábado); omitir = todos",
    ),
):
    """
    Listado completo del rutero (gestión): mismas columnas que antes, sin excluir telefónicos
    ni filas sin georef. Filtros opcionales por query (mapa y ORS siguen acotados aparte).
    """
    tipo_f = (tipo or "").strip().lower()
    geo_f = (geo or "").strip().lower()
    de = (dia_estado or "").strip().lower()
    sab_f = (sabado or "").strip().lower()
    if tipo_f and tipo_f not in ("terreno", "telefonico"):
        raise HTTPException(status_code=400, detail="tipo debe ser terreno o telefonico")
    if geo_f and geo_f not in ("con", "sin"):
        raise HTTPException(status_code=400, detail="geo debe ser con o sin")
    if de and de not in ("con", "sin"):
        raise HTTPException(status_code=400, detail="dia_estado debe ser con o sin")
    if sab_f and sab_f not in ("con", "sin"):
        raise HTTPException(status_code=400, detail="sabado debe ser con o sin")

    where_sql, params = _rutero_list_where_order(vendedor, dia, tipo, geo, dia_estado, sabado)

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            _RUTERO_FILA_SELECT + "\n            WHERE " + where_sql + _RUTERO_LIST_ORDER,
            tuple(params),
        )
        data = _rows_to_json(cur)
        cur.close()
    finally:
        conn.close()

    return data


def _rutero_fila_por_id(cur, row_id: int) -> dict | None:
    cur.execute(
        _RUTERO_FILA_SELECT + " WHERE r.id = %s AND r.company_id = 3",
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


@router.patch("/rutero/sabado")
def patch_rutero_sabado(body: RuteroSabadoPatchBody):
    """
    Asigna o quita atención de sábado: persiste ``'sabado'`` normalizado en ``dia_extra`` o ``NULL``.
    Actualiza por ``rut_clean`` (empresa 3, filas activas); puede afectar más de una fila si hay duplicados de RUT.
    """
    rut = (body.rut_clean or "").strip()
    if not rut:
        raise HTTPException(status_code=400, detail="rut_clean es obligatorio")

    dia_val = "sabado" if body.activo else None

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE bsale.rutero
            SET dia_extra = %s
            WHERE company_id = 3
              AND activo = TRUE
              AND TRIM(LOWER(COALESCE(rut_clean::text, ''))) = TRIM(LOWER(%s))
            """,
            (dia_val, rut),
        )
        n = cur.rowcount
        conn.commit()
        cur.close()
    finally:
        conn.close()

    if n == 0:
        raise HTTPException(
            status_code=404,
            detail="No se encontró fila activa en rutero con el RUT indicado.",
        )

    return {"updated": n, "rut_clean": rut, "activo": body.activo}


@router.patch("/rutero/{row_id}")
def patch_rutero_tipo_atencion(row_id: int, body: RuteroTipoAtencionPatchBody):
    """Actualiza `tipo_atencion` de una fila rutero por PK `bsale.rutero.id`."""
    if row_id < 1:
        raise HTTPException(status_code=400, detail="id inválido")
    tipo_db = _normalize_rutero_tipo_atencion(body.tipo_atencion)

    conn = get_connection()
    fila: dict | None = None
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE bsale.rutero
            SET tipo_atencion = %s
            WHERE id = %s
              AND company_id = 3
              AND activo = TRUE
            """,
            (tipo_db, row_id),
        )
        if cur.rowcount == 0:
            cur.close()
            raise HTTPException(status_code=404, detail="Fila rutero no encontrada o inactiva")
        conn.commit()
        fila = _rutero_fila_por_id(cur, row_id)
        cur.close()
    finally:
        conn.close()

    if fila is None:
        raise HTTPException(status_code=500, detail="No se pudo leer la fila actualizada")
    return fila


@router.get("/sin-georef/export")
def get_sin_georef_export():
    """
    Exporta filas rutero sin coordenadas a Excel (.xlsx).

    Deprecated: la navegación dejó de exponer página dedicada; el rutero centraliza filtros.
    Se mantiene el endpoint por si hay integraciones o enlaces antiguos.
    """
    import pandas as pd

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                COALESCE(
                    NULLIF(TRIM(nombre_fantasia), ''),
                    NULLIF(
                        TRIM(
                            CONCAT_WS(
                                ' ',
                                NULLIF(TRIM(first_name), ''),
                                NULLIF(TRIM(last_name), '')
                            )
                        ),
                        ''
                    ),
                    'Cliente #' || bsale_id::text
                ) AS cliente_nombre,
                LOWER(TRIM(COALESCE(vendedor::text, ''))) AS vendedor,
                municipality,
                address AS direccion,
                phone AS telefono
            FROM bsale.rutero
            WHERE company_id = 3
              AND activo = TRUE
              AND """
            + WHERE_SIN_GEOREF_BARE
            + """
            ORDER BY LOWER(TRIM(COALESCE(vendedor::text, ''))),
                     municipality NULLS LAST,
                     bsale_id
            """
        )
        rows = cur.fetchall()
        columns = [col[0] for col in cur.description]
        cur.close()
    finally:
        conn.close()

    df = pd.DataFrame(rows, columns=columns)
    buf = BytesIO()
    df.to_excel(buf, index=False, engine="openpyxl")
    buf.seek(0)
    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": 'attachment; filename="sin_georef.xlsx"'},
    )


@router.get("/sin-georef")
def get_sin_georef():
    """Deprecated: usar rutero con filtros; respuesta JSON conservada por compatibilidad."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT *
            FROM bsale.rutero
            WHERE company_id = 3
              AND activo = TRUE
              AND """
            + WHERE_SIN_GEOREF_BARE
            + """
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

    Deprecated: la UI unificada está en Rutero; endpoint conservado por compatibilidad.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT *
            FROM bsale.clients
            WHERE company_id = 3
              AND LOWER(TRIM(COALESCE(vendedor::text, ''))) IN (
                  'vendedor_1',
                  'vendedor_2',
                  'vendedor_3',
                  'vendedor_4'
              )
              AND (
                  dia_atencion IS NULL
                  OR LOWER(TRIM(COALESCE(dia_atencion::text, ''))) = ''
              )
            ORDER BY LOWER(TRIM(COALESCE(vendedor::text, ''))),
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
    """
    Asigna `dia_atencion` en `bsale.clients` (empresa 3).

    Deprecated: asignación desde Rutero; POST conservado por compatibilidad.
    """
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
              AND LOWER(TRIM(COALESCE(vendedor::text, ''))) IN (
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


@router.get("/resumen-vendedor")
def get_resumen_vendedor(
    vendedor: str = Query(..., min_length=1, description="Código vendedor (ej. vendedor_1)"),
):
    """
    Todas las rutas (días) del vendedor en un payload para mapa resumen semanal.
    Cada día reutiliza la misma lógica que /ruta-detalle (orden manual persistido u optimización).
    """
    return _build_resumen_vendedor_response(_norm_vendedor(vendedor))


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
                LOWER(TRIM(COALESCE(vendedor::text, ''))) AS vendedor,
                dia_atencion,
                dia_extra,
                """
            + _SQL_DIA_OPERATIVO_SQL_BARE
            + """ AS dia_operativo,
                municipality,
                """
            + B_LAT_AS
            + ",\n                "
            + B_LON_AS
            + """,
                tipo_atencion,
                orden_ruta,
                orden_manual
            FROM bsale.rutero
            WHERE company_id = 3
              AND activo = TRUE
              AND """
            + WHERE_HAS_GEOREF_BARE
            + """
            """
            + _SQL_RUTA_EXCL_DIA_TELEFONICO
            + _SQL_RUTA_EXCL_TIPO_TELEFONICO
            + "\n        ",
        )
        clientes = _rows_to_json(cur)

        cur.execute(
            """
            SELECT DISTINCT TRIM(dia_op) AS d
            FROM (
                SELECT """
            + _SQL_DIA_OPERATIVO_SQL_BARE
            + """ AS dia_op
                FROM bsale.rutero
                WHERE company_id = 3
                  AND activo = TRUE
                  AND """
            + WHERE_HAS_GEOREF_BARE
            + """
            """
            + _SQL_RUTA_EXCL_DIA_TELEFONICO
            + _SQL_RUTA_EXCL_TIPO_TELEFONICO
            + """
            ) x
            WHERE TRIM(COALESCE(dia_op, '')) <> ''
            ORDER BY 1
            """
        )
        dias_atencion = _sort_dias_semana([str(r[0]).strip() for r in cur.fetchall() if r and r[0]])

        cur.execute(
            """
            SELECT DISTINCT LOWER(TRIM(COALESCE(vendedor::text, ''))) AS v
            FROM bsale.rutero
            WHERE company_id = 3
              AND activo = TRUE
              AND TRIM(COALESCE(vendedor::text, '')) <> ''
            ORDER BY 1
            """
        )
        vendedores = [str(r[0]).strip() for r in cur.fetchall() if r and r[0]]

        cur.execute(
            """
            SELECT
                LOWER(TRIM(COALESCE(vendedor::text, ''))) AS vendedor,
                nombre,
                lat,
                lon
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
        "dias_atencion": dias_atencion,
        "vendedores": vendedores,
    }


@router.post("/orden-manual/reset")
def post_orden_manual_reset(body: OrdenManualResetBody):
    """Pone orden_manual en NULL para el vendedor y día (vuelve a ORS en ruta-detalle)."""
    v = _norm_vendedor(body.vendedor)
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
              AND LOWER(TRIM(COALESCE(vendedor::text, ''))) = %s
            """
            + _SQL_MATCH_DIA_OPERATIVO_BARE
            + _SQL_RUTA_EXCL_DIA_TELEFONICO
            + _SQL_RUTA_EXCL_TIPO_TELEFONICO
            + "\n        ",
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


@router.post("/sync-rutero")
def post_sync_rutero():
    """
    Sincroniza bsale.clients → bsale.rutero (empresa 3, vendedores de ruta).
    Misma lógica que el job programado; útil para forzar actualización manual.
    """
    from backend.jobs.sync_rutero import sync_rutero

    try:
        return sync_rutero()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.get("/resumen")
def get_resumen():
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                LOWER(TRIM(COALESCE(vendedor::text, ''))) AS vendedor,
                """
            + _SQL_DIA_OPERATIVO_SQL_BARE
            + """ AS dia_operativo,
                COUNT(*) AS cantidad
            FROM bsale.rutero
            WHERE company_id = 3
              AND activo = TRUE
            GROUP BY LOWER(TRIM(COALESCE(vendedor::text, ''))),
                """
            + _SQL_DIA_OPERATIVO_SQL_BARE
            + """
            ORDER BY vendedor, 2
            """
        )
        data = _rows_to_json(cur)
        cur.close()
    finally:
        conn.close()

    return data
