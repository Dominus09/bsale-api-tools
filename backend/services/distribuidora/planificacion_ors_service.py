"""Rutas ORS planificación despacho: bodega depot + optimización cerrada + costo logístico."""

from __future__ import annotations

import logging
from typing import Any

from backend.db import get_connection
from backend.utils.json_safe import serialize_row
from backend.utils.ors_stability import log_debug, log_error
from backend.repositories.distribuidora.ors_plan_route_crew_repo import (
    get_route_crew_by_session,
    upsert_route_crew,
)
from backend.routers.distribuidora import _ors_route_merge_chunks
from backend.services.distribuidora.logistics_cost_service import (
    DEFAULT_ASSISTANT_COST_CLP_PER_TRIP,
    DEFAULT_DRIVER_COST_CLP_PER_TRIP,
    compute_route_costs,
    crew_config_as_dict,
    get_logistics_cost_settings,
    load_active_trucks,
    resolve_truck,
)
from backend.services.distribuidora.system_config_service import (
    DEFAULT_DIESEL_CLP_PER_LITER,
    get_diesel_price_per_liter,
)
from backend.utils.ruta_optimizador_local import optimizar_secuencia_cerrado

logger = logging.getLogger(__name__)

# Bodega Quillotana — depot fijo inicio/fin de cada ruta.
BODEGA_LAT = -43.13147486008401
BODEGA_LNG = -73.63921301814756


def depot_base() -> dict[str, float]:
    return {"lat": BODEGA_LAT, "lon": BODEGA_LNG}


def _stops_to_clientes(stops: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for s in stops:
        lat = float(s["lat"])
        lon = float(s["lon"])
        doc_id = int(s["document_id"])
        out.append(
            {
                "lat": lat,
                "lon": lon,
                "document_id": doc_id,
                "bsale_id": doc_id,
            }
        )
    return out


def _resolve_crew_for_leg(
    leg: dict[str, Any],
    saved: dict[str, dict[str, Any]],
    default_driver_rate: int,
    default_assistant_rate: int,
) -> tuple[int, int, int, int]:
    camion = str(leg.get("camion") or "").strip()
    saved_row = saved.get(camion) or {}
    driver_count = leg.get("driver_count")
    if driver_count is None:
        driver_count = saved_row.get("driver_count", 1)
    assistant_count = leg.get("assistant_count")
    if assistant_count is None:
        assistant_count = saved_row.get("assistant_count", 0)
    d_rate = leg.get("driver_cost_clp")
    if d_rate is None:
        d_rate = saved_row.get("driver_cost_clp", default_driver_rate)
    a_rate = leg.get("assistant_cost_clp")
    if a_rate is None:
        a_rate = saved_row.get("assistant_cost_clp", default_assistant_rate)
    return (
        max(0, int(driver_count)),
        max(0, int(assistant_count)),
        int(d_rate),
        int(a_rate),
    )


def compute_ors_route_for_truck(
    *,
    camion: str,
    stops: list[dict[str, Any]],
    truck_id: int | None,
    trucks_by_id: dict[int, dict[str, Any]],
    trucks_by_name: dict[str, dict[str, Any]],
    diesel_clp: float,
    logistics_settings: Any,
    driver_count: int = 1,
    assistant_count: int = 0,
    driver_cost_clp_per_trip: int | None = None,
    assistant_cost_clp_per_trip: int | None = None,
) -> dict[str, Any] | None:
    """
    Optimiza orden de visitas (cerrado desde bodega) y calcula geometría ORS bodega→clientes→bodega.
    """
    if len(stops) < 1:
        return None

    clientes = _stops_to_clientes(stops)
    _, optimizado = optimizar_secuencia_cerrado(depot_base(), clientes)

    coords: list[list[float]] = [[BODEGA_LNG, BODEGA_LAT]]
    stops_ordered: list[dict[str, Any]] = []
    for i, c in enumerate(optimizado, start=1):
        lon, lat = float(c["lon"]), float(c["lat"])
        coords.append([lon, lat])
        stops_ordered.append(
            {
                "document_id": int(c["document_id"]),
                "stop_index": i,
                "lat": lat,
                "lng": lon,
            }
        )
    coords.append([BODEGA_LNG, BODEGA_LAT])

    merged = _ors_route_merge_chunks(coords, None)
    if merged is None:
        return None

    geometry, km, mins = merged
    truck = resolve_truck(truck_id, camion, trucks_by_id, trucks_by_name)
    costs = compute_route_costs(
        distance_km=float(km),
        duration_min=float(mins),
        truck=truck,
        truck_id=truck_id,
        diesel_clp=diesel_clp,
        settings=logistics_settings,
        driver_count=driver_count,
        assistant_count=assistant_count,
        driver_cost_clp_per_trip=driver_cost_clp_per_trip,
        assistant_cost_clp_per_trip=assistant_cost_clp_per_trip,
    )

    return {
        "camion": camion,
        "truck_id": costs.truck_id if costs.truck_id is not None else truck_id,
        "truck_name": costs.truck_name,
        "distance_km": round(float(km), 3),
        "duration_min": round(float(mins), 2),
        "geometry": geometry,
        "coordinates": coords,
        "stops_ordered": stops_ordered,
        "includes_depot_return": True,
        "depot": {"lat": BODEGA_LAT, "lng": BODEGA_LNG},
        **costs.as_dict(),
    }


def compute_planificacion_ors_routes(
    legs: list[dict[str, Any]],
    *,
    plan_session_id: str | None = None,
    persist_crew: bool = True,
) -> dict[str, Any]:
    """
    ``legs``: { camion, truck_id?, stops, driver_count?, assistant_count? }
    """
    log_debug("POST /planificacion/ors-routes", extra={"legs": len(legs or [])})
    if not legs:
        depot = depot_base()
        return {
            "ok": True,
            "routes": [],
            "depot": depot,
            "diesel_price_per_liter": round(DEFAULT_DIESEL_CLP_PER_LITER, 2),
            "crew_defaults": {
                "driver_cost_clp_per_trip": DEFAULT_DRIVER_COST_CLP_PER_TRIP,
                "assistant_cost_clp_per_trip": DEFAULT_ASSISTANT_COST_CLP_PER_TRIP,
            },
            "totals": {
                "distance_km": 0.0,
                "duration_min": 0.0,
                "liters_estimated": 0.0,
                "fuel_cost_clp": 0,
                "crew_cost_clp": 0,
                "total_cost_clp": 0,
            },
        }

    conn = get_connection()
    try:
        try:
            diesel_clp = get_diesel_price_per_liter(conn)
            trucks_by_id, trucks_by_name = load_active_trucks(conn)
            logistics_settings = get_logistics_cost_settings(conn)
        except Exception as exc:
            log_error("POST /planificacion/ors-routes", exc, extra={"phase": "config"})
            diesel_clp = DEFAULT_DIESEL_CLP_PER_LITER
            trucks_by_id, trucks_by_name = {}, {}
            logistics_settings = get_logistics_cost_settings()
        default_d, default_a = logistics_settings.crew_rates()
        saved: dict[str, dict[str, Any]] = {}
        if plan_session_id and str(plan_session_id).strip():
            try:
                cur = conn.cursor()
                saved = get_route_crew_by_session(cur, str(plan_session_id).strip())
                cur.close()
            except Exception as exc:
                log_error(
                    "POST /planificacion/ors-routes",
                    exc,
                    extra={"phase": "route_crew_load"},
                )
    finally:
        conn.close()

    routes: list[dict[str, Any]] = []
    tot_km = 0.0
    tot_min = 0.0
    tot_liters = 0.0
    tot_fuel = 0.0
    tot_crew = 0.0
    tot_route = 0.0

    conn_persist = get_connection() if persist_crew and plan_session_id else None
    try:
        cur_p = conn_persist.cursor() if conn_persist else None
        for leg in legs:
            camion = str(leg.get("camion") or "").strip()
            if not camion:
                continue
            raw_stops = leg.get("stops") or []
            stops: list[dict[str, Any]] = []
            for s in raw_stops:
                lng = s.get("lng")
                if lng is None:
                    lng = s.get("lon")
                stops.append(
                    {
                        "document_id": int(s["document_id"]),
                        "lat": float(s["lat"]),
                        "lon": float(lng),
                    }
                )
            tid = leg.get("truck_id")
            truck_id = int(tid) if tid is not None and str(tid).strip() != "" else None

            d_count, a_count, d_rate, a_rate = _resolve_crew_for_leg(
                leg, saved, default_d, default_a
            )

            row = compute_ors_route_for_truck(
                camion=camion,
                stops=stops,
                truck_id=truck_id,
                trucks_by_id=trucks_by_id,
                trucks_by_name=trucks_by_name,
                diesel_clp=diesel_clp,
                logistics_settings=logistics_settings,
                driver_count=d_count,
                assistant_count=a_count,
                driver_cost_clp_per_trip=d_rate,
                assistant_cost_clp_per_trip=a_rate,
            )
            if row is None:
                return {
                    "ok": False,
                    "error": f"No se pudo calcular la ruta ORS para {camion!r}.",
                }
            routes.append(row)
            tot_km += float(row["distance_km"])
            tot_min += float(row["duration_min"])
            tot_liters += float(row.get("liters_estimated") or 0)
            tot_fuel += int(row.get("fuel_cost_clp") or 0)
            tot_crew += int(row.get("crew_cost_clp") or 0)
            bd = row.get("cost_breakdown") or {}
            tot_route += int(bd.get("total_clp") or row.get("fuel_cost_clp") or 0)

            if cur_p is not None and plan_session_id:
                upsert_route_crew(
                    cur_p,
                    plan_session_id=str(plan_session_id).strip(),
                    camion=camion,
                    truck_id=truck_id,
                    driver_count=d_count,
                    assistant_count=a_count,
                    driver_cost_clp=int(row.get("driver_cost_clp") or d_rate),
                    assistant_cost_clp=int(row.get("assistant_cost_clp") or a_rate),
                )
        if conn_persist is not None:
            conn_persist.commit()
    finally:
        if conn_persist is not None:
            conn_persist.close()

    result = {
        "ok": True,
        "routes": [serialize_row(r) for r in routes],
        "depot": depot_base(),
        "diesel_price_per_liter": round(diesel_clp, 2),
        "crew_defaults": crew_config_as_dict(logistics_settings),
        "totals": {
            "distance_km": round(tot_km, 3),
            "duration_min": round(tot_min, 2),
            "liters_estimated": round(tot_liters, 2),
            "fuel_cost_clp": int(tot_fuel),
            "crew_cost_clp": int(tot_crew),
            "total_cost_clp": int(tot_route),
        },
    }
    log_debug(
        "POST /planificacion/ors-routes",
        rows=len(routes),
        extra={"ok": True},
    )
    return result


def get_plan_route_crew(plan_session_id: str) -> dict[str, Any]:
    log_debug("GET /planificacion/route-crew", extra={"session": plan_session_id[:12]})
    conn = get_connection()
    try:
        cur = conn.cursor()
        rows = get_route_crew_by_session(cur, plan_session_id.strip())
        cur.close()
        try:
            settings = get_logistics_cost_settings(conn)
        except Exception:
            settings = get_logistics_cost_settings()
        defaults = crew_config_as_dict(settings)
        routes = []
        for camion, rec in rows.items():
            routes.append(
                {
                    "camion": camion,
                    "truck_id": rec.get("truck_id"),
                    "driver_count": int(rec.get("driver_count") or 1),
                    "assistant_count": int(rec.get("assistant_count") or 0),
                    "driver_cost_clp": int(rec.get("driver_cost_clp") or defaults["driver_cost_clp_per_trip"]),
                    "assistant_cost_clp": int(
                        rec.get("assistant_cost_clp") or defaults["assistant_cost_clp_per_trip"]
                    ),
                }
            )
        out = {
            "plan_session_id": plan_session_id.strip(),
            "routes": routes,
            "defaults": defaults,
        }
        log_debug("GET /planificacion/route-crew", rows=len(routes))
        return out
    except Exception as exc:
        log_error("GET /planificacion/route-crew", exc)
        return {
            "plan_session_id": plan_session_id.strip(),
            "routes": [],
            "defaults": crew_config_as_dict(get_logistics_cost_settings()),
        }
    finally:
        conn.close()


def save_plan_route_crew(
    plan_session_id: str,
    routes: list[dict[str, Any]],
) -> dict[str, Any]:
    conn = get_connection()
    try:
        settings = get_logistics_cost_settings(conn)
        default_d, default_a = settings.crew_rates()
        cur = conn.cursor()
        for r in routes:
            camion = str(r.get("camion") or "").strip()
            if not camion:
                continue
            tid = r.get("truck_id")
            truck_id = int(tid) if tid is not None and str(tid).strip() != "" else None
            upsert_route_crew(
                cur,
                plan_session_id=plan_session_id.strip(),
                camion=camion,
                truck_id=truck_id,
                driver_count=max(0, int(r.get("driver_count", 1))),
                assistant_count=max(0, int(r.get("assistant_count", 0))),
                driver_cost_clp=int(r.get("driver_cost_clp") or default_d),
                assistant_cost_clp=int(r.get("assistant_cost_clp") or default_a),
            )
        conn.commit()
        cur.close()
        return get_plan_route_crew(plan_session_id)
    finally:
        conn.close()
