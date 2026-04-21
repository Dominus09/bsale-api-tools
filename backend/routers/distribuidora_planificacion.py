"""Pre‑planificación (OC tipo 33) y cálculo de rutas ORS para despacho."""

from __future__ import annotations

from datetime import date
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from backend.routers.distribuidora import _ors_route_merge_chunks
from backend.services.distribuidora.dispatch_planning_list_service import (
    list_dispatch_planning_orders,
)

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
    items = list_dispatch_planning_orders(
        emission_date_from=emission_date_from,
        emission_date_to=emission_date_to,
        delivery_day=delivery_day.strip().lower(),
    )
    return {"items": items}


class OrsRouteInput(BaseModel):
    camion: str = Field(..., min_length=1, max_length=64)
    coordinates: list[list[float]] = Field(
        ...,
        description="Secuencia [lon, lat] en orden de visita (mínimo 2 puntos distintos).",
    )


class OrsRoutesRequestBody(BaseModel):
    routes: list[OrsRouteInput] = Field(..., min_length=1, max_length=10)


def _dedupe_adjacent_coords(coords: list[list[float]]) -> list[list[float]]:
    out: list[list[float]] = []
    for p in coords:
        if len(p) < 2:
            continue
        lon, lat = float(p[0]), float(p[1])
        if out and abs(out[-1][0] - lon) < 1e-7 and abs(out[-1][1] - lat) < 1e-7:
            continue
        out.append([lon, lat])
    return out


@router.post("/ors-routes")
def post_planificacion_ors_routes(body: OrsRoutesRequestBody):
    """
    Calcula geometría y métricas ORS por camión (misma lógica de troceo que el mapa rutero).
    """
    out_routes: list[dict[str, Any]] = []
    for leg in body.routes:
        coords = _dedupe_adjacent_coords(leg.coordinates)
        if len(coords) < 2:
            raise HTTPException(
                status_code=400,
                detail=f"Camión {leg.camion!r}: se requieren al menos 2 coordenadas distintas.",
            )
        merged = _ors_route_merge_chunks(coords, None)
        if merged is None:
            raise HTTPException(
                status_code=502,
                detail=f"No se pudo calcular la ruta ORS para {leg.camion!r}.",
            )
        geometry, km, mins = merged
        out_routes.append(
            {
                "camion": leg.camion,
                "distance_km": round(float(km), 3),
                "duration_min": round(float(mins), 2),
                "geometry": geometry,
                "coordinates": coords,
            }
        )
    return {"routes": out_routes}
