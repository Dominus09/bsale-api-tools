"""Pre‑planificación (OC tipo 33) y cálculo de rutas ORS para despacho."""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from backend.services.distribuidora.dispatch_planning_list_service import (
    list_dispatch_planning_orders,
)
from backend.services.distribuidora.logistics_cost_service import (
    crew_config_as_dict,
    get_logistics_cost_settings,
    update_logistics_cost_settings,
)
from backend.services.distribuidora.planificacion_ors_service import (
    BODEGA_LAT,
    BODEGA_LNG,
    compute_planificacion_ors_routes,
    depot_base,
    get_plan_route_crew,
    save_plan_route_crew,
)
from backend.services.distribuidora.route_operational_costs_service import (
    get_route_operational_costs,
    save_route_operational_costs,
)
from backend.services.distribuidora.system_config_service import (
    DEFAULT_DIESEL_CLP_PER_LITER,
    get_diesel_price_per_liter,
    set_diesel_price_per_liter,
)
from backend.utils.ors_stability import log_error

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/distribuidora/planificacion", tags=["Distribuidora planificación despacho"])


@router.get("/orders")
def get_planificacion_orders(
    emission_date_from: date = Query(...),
    emission_date_to: date = Query(...),
    delivery_day: str = Query(
        "all",
        description="todos | lunes | martes | miercoles | jueves | viernes | sabado",
    ),
):
    try:
        items = list_dispatch_planning_orders(
            emission_date_from=emission_date_from,
            emission_date_to=emission_date_to,
            delivery_day=delivery_day.strip().lower(),
        )
        return {"items": items}
    except Exception as exc:
        log_error("GET /planificacion/orders", exc)
        return {"items": []}


@router.get("/fuel-config")
def get_fuel_config():
    d = depot_base()
    try:
        clp = get_diesel_price_per_liter()
    except Exception as exc:
        log_error("GET /planificacion/fuel-config", exc)
        clp = DEFAULT_DIESEL_CLP_PER_LITER
    return {
        "diesel_price_per_liter": round(clp, 2),
        "depot": {"lat": d["lat"], "lng": d["lon"]},
    }


class FuelConfigBody(BaseModel):
    diesel_price_per_liter: float = Field(..., gt=0, description="CLP por litro diesel")


@router.put("/fuel-config")
def put_fuel_config(body: FuelConfigBody):
    try:
        clp = set_diesel_price_per_liter(body.diesel_price_per_liter)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    d = depot_base()
    return {
        "diesel_price_per_liter": round(clp, 2),
        "depot": {"lat": d["lat"], "lng": d["lon"]},
    }


@router.get("/crew-config")
def get_crew_config():
    """Tarifas base chofer/peoneta por vuelta (system_config)."""
    try:
        return crew_config_as_dict(get_logistics_cost_settings())
    except Exception as exc:
        log_error("GET /planificacion/crew-config", exc)
        from backend.services.distribuidora.logistics_cost_service import (
            LogisticsCostSettings,
        )

        return crew_config_as_dict(LogisticsCostSettings())


class CrewConfigBody(BaseModel):
    driver_cost_clp_per_trip: int | None = Field(None, ge=0)
    assistant_cost_clp_per_trip: int | None = Field(None, ge=0)


@router.put("/crew-config")
def put_crew_config(body: CrewConfigBody):
    patch: dict[str, Any] = {}
    if body.driver_cost_clp_per_trip is not None:
        patch["driver_cost_clp_per_trip"] = body.driver_cost_clp_per_trip
    if body.assistant_cost_clp_per_trip is not None:
        patch["assistant_cost_clp_per_trip"] = body.assistant_cost_clp_per_trip
    if not patch:
        raise HTTPException(status_code=400, detail="Indique al menos una tarifa.")
    settings = update_logistics_cost_settings(patch)
    return crew_config_as_dict(settings)


class OrsStopInput(BaseModel):
    document_id: int
    lat: float
    lng: float


class OrsRouteInput(BaseModel):
    camion: str = Field(..., min_length=1, max_length=64)
    truck_id: int | None = None
    stops: list[OrsStopInput] = Field(..., min_length=1)
    driver_count: int | None = Field(None, ge=0, le=10)
    assistant_count: int | None = Field(None, ge=0, le=10)
    coordinates: list[list[float]] | None = Field(
        default=None,
        description="Legacy: ignorado si hay stops; use stops para optimización con bodega.",
    )


class OrsRoutesRequestBody(BaseModel):
    routes: list[OrsRouteInput] = Field(..., min_length=1, max_length=10)
    plan_session_id: str | None = Field(
        None,
        min_length=8,
        max_length=64,
        description="Id de sesión para persistir dotación por camión.",
    )
    diesel_price_per_liter: float | None = Field(
        None,
        gt=0,
        description="Override CLP/L para recalcular combustible en esta petición.",
    )


def _leg_to_service_dict(leg: OrsRouteInput) -> dict[str, Any]:
    out: dict[str, Any] = {
        "camion": leg.camion,
        "truck_id": leg.truck_id,
        "stops": [
            {"document_id": s.document_id, "lat": s.lat, "lng": s.lng}
            for s in leg.stops
        ],
    }
    if leg.driver_count is not None:
        out["driver_count"] = leg.driver_count
    if leg.assistant_count is not None:
        out["assistant_count"] = leg.assistant_count
    return out


@router.post("/ors-routes")
def post_planificacion_ors_routes(body: OrsRoutesRequestBody):
    """
    Ruta cerrada BODEGA → clientes (orden optimizado) → BODEGA.
    Métricas ORS reales + litros/costo según ``trucks.km_per_liter``, diesel y personal.
    """
    try:
        legs = [_leg_to_service_dict(leg) for leg in body.routes]
        result = compute_planificacion_ors_routes(
            legs,
            plan_session_id=body.plan_session_id,
            persist_crew=bool(body.plan_session_id),
            diesel_price_per_liter=body.diesel_price_per_liter,
        )
    except Exception as exc:
        log_error("POST /planificacion/ors-routes", exc)
        raise HTTPException(
            status_code=502,
            detail=f"Error al calcular rutas ORS: {exc}",
        ) from exc
    if not result.get("ok"):
        raise HTTPException(
            status_code=502,
            detail=result.get("error") or "Error ORS",
        )
    depot = result.get("depot") or depot_base()
    return {
        "routes": result.get("routes") or [],
        "depot": {
            "lat": float(depot.get("lat", BODEGA_LAT)),
            "lng": float(depot.get("lon", depot.get("lng", BODEGA_LNG))),
        },
        "diesel_price_per_liter": result.get("diesel_price_per_liter"),
        "crew_defaults": result.get("crew_defaults"),
        "totals": result.get("totals") or {},
    }


class RouteCrewRow(BaseModel):
    camion: str = Field(..., min_length=1, max_length=64)
    truck_id: int | None = None
    driver_count: int = Field(1, ge=0, le=10)
    assistant_count: int = Field(0, ge=0, le=10)
    driver_cost_clp: int | None = Field(None, ge=0)
    assistant_cost_clp: int | None = Field(None, ge=0)


class RouteCrewSaveBody(BaseModel):
    plan_session_id: str = Field(..., min_length=8, max_length=64)
    routes: list[RouteCrewRow] = Field(..., min_length=0, max_length=10)


@router.get("/route-crew")
def get_route_crew(plan_session_id: str = Query(..., min_length=8, max_length=64)):
    try:
        return get_plan_route_crew(plan_session_id.strip())
    except Exception as exc:
        log_error("GET /planificacion/route-crew", exc)
        return {
            "plan_session_id": plan_session_id.strip(),
            "routes": [],
            "defaults": crew_config_as_dict(None),
        }


@router.put("/route-crew")
def put_route_crew(body: RouteCrewSaveBody):
    return save_plan_route_crew(
        body.plan_session_id.strip(),
        [r.model_dump() for r in body.routes],
    )


@router.get("/operational-costs")
def get_operational_costs_endpoint(
    plan_session_id: str = Query(..., min_length=8, max_length=64),
    truck_id: int = Query(..., ge=1),
):
    try:
        return get_route_operational_costs(plan_session_id.strip(), truck_id)
    except Exception as exc:
        log_error("GET /planificacion/operational-costs", exc)
        return {
            "plan_session_id": plan_session_id.strip(),
            "truck_id": truck_id,
            "ferry_clp": 0,
            "per_diem_clp": 0,
            "other_clp": 0,
            "diesel_clp_per_liter": None,
        }


class OperationalCostsBody(BaseModel):
    plan_session_id: str = Field(..., min_length=8, max_length=64)
    truck_id: int = Field(..., ge=1)
    ferry_clp: int = Field(0, ge=0)
    per_diem_clp: int = Field(0, ge=0)
    other_clp: int = Field(0, ge=0)
    diesel_clp_per_liter: float | None = Field(None, gt=0)


@router.put("/operational-costs")
def put_operational_costs(body: OperationalCostsBody):
    try:
        return save_route_operational_costs(
            plan_session_id=body.plan_session_id.strip(),
            truck_id=body.truck_id,
            ferry_clp=body.ferry_clp,
            per_diem_clp=body.per_diem_clp,
            other_clp=body.other_clp,
            diesel_clp_per_liter=body.diesel_clp_per_liter,
        )
    except Exception as exc:
        log_error("PUT /planificacion/operational-costs", exc)
        raise HTTPException(
            status_code=500,
            detail=f"Error al guardar costos operacionales: {exc}",
        ) from exc
