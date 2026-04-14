"""API planificación de rutas Distribuidora (OC por camión y día)."""

from __future__ import annotations

from datetime import date

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field

from backend.services.distribuidora.route_planning_service import (
    AlreadyPlannedError,
    MissingDocumentsError,
    create_route_planning,
    delete_route_planning_row,
    get_route_planning,
    patch_route_planning,
)

router = APIRouter(prefix="/distribuidora", tags=["Distribuidora planificación rutas"])


class RoutePlanningCreateBody(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    planning_date: date
    document_ids: list[int] = Field(..., min_length=1)
    truck: str = Field(..., min_length=1)


class RoutePlanningPatchBody(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    truck: str | None = None
    status: str | None = None


@router.post("/route-planning")
def post_route_planning(body: RoutePlanningCreateBody):
    try:
        return create_route_planning(
            planning_date=body.planning_date,
            document_ids=body.document_ids,
            truck=body.truck,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except MissingDocumentsError as e:
        raise HTTPException(
            status_code=400,
            detail={
                "message": "Algunos document_id no existen en órdenes de compra",
                "document_ids": sorted(e.document_ids),
            },
        ) from e
    except AlreadyPlannedError as e:
        raise HTTPException(
            status_code=409,
            detail={
                "message": "Ya planificados para esa fecha",
                "document_ids": sorted(e.document_ids),
            },
        ) from e


@router.get("/route-planning")
def list_route_planning_api(
    planning_date: date = Query(..., description="Día de reparto"),
    truck: str | None = Query(None, description="Filtrar por camión"),
):
    return get_route_planning(planning_date=planning_date, truck=truck)


@router.patch("/route-planning/{row_id}")
def patch_route_planning_api(row_id: int, body: RoutePlanningPatchBody):
    try:
        row = patch_route_planning(row_id, truck=body.truck, status=body.status)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    if not row:
        raise HTTPException(status_code=404, detail="Fila no encontrada")
    return row


@router.delete("/route-planning/{row_id}", status_code=204)
def delete_route_planning_api(row_id: int):
    if not delete_route_planning_row(row_id):
        raise HTTPException(status_code=404, detail="Fila no encontrada")
