"""API planificación de rutas Distribuidora (OC por camión y día)."""

from __future__ import annotations

from datetime import date

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field

from backend.services.distribuidora.route_planning_service import (
    AlreadyPlannedError,
    InvalidTruckError,
    MissingDocumentsError,
    create_route_planning,
    create_route_planning_batch,
    delete_route_planning_row,
    get_route_planning,
    list_route_planning_summaries,
    patch_route_planning,
    patch_route_planning_summary,
)

router = APIRouter(prefix="/distribuidora", tags=["Distribuidora planificación rutas"])


class PlanningAssignmentItem(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    document_id: int = Field(..., ge=1)
    truck: str = Field(..., min_length=1)


class RoutePlanningBatchBody(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    planning_date: date
    assignments: list[PlanningAssignmentItem] = Field(..., min_length=1)


class RoutePlanningCreateBody(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    planning_date: date
    document_ids: list[int] = Field(..., min_length=1)
    truck: str = Field(..., min_length=1)
    route_name: str | None = None
    driver: str | None = None
    assistant_1: str | None = None
    assistant_2: str | None = None
    departure_time: str | None = None
    general_observation: str | None = None


class RoutePlanningPatchBody(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    truck: str | None = None
    status: str | None = None
    route_name: str | None = None
    driver: str | None = None
    assistant_1: str | None = None
    assistant_2: str | None = None
    departure_time: str | None = None
    general_observation: str | None = None


class RoutePlanningSummaryPatchBody(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    route_name: str | None = None
    driver: str | None = None
    assistant_1: str | None = None
    assistant_2: str | None = None
    departure_time: str | None = None
    general_observation: str | None = None


@router.post("/route-planning/batch")
def post_route_planning_batch(body: RoutePlanningBatchBody):
    pairs = [(a.document_id, a.truck) for a in body.assignments]
    try:
        return create_route_planning_batch(
            planning_date=body.planning_date,
            assignments=pairs,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except InvalidTruckError as e:
        raise HTTPException(
            status_code=400,
            detail={"message": "Camión no permitido", "truck": e.truck},
        ) from e
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


@router.post("/route-planning")
def post_route_planning(body: RoutePlanningCreateBody):
    try:
        return create_route_planning(
            planning_date=body.planning_date,
            document_ids=body.document_ids,
            truck=body.truck,
            route_name=body.route_name,
            driver=body.driver,
            assistant_1=body.assistant_1,
            assistant_2=body.assistant_2,
            departure_time=body.departure_time,
            general_observation=body.general_observation,
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


@router.get("/route-planning/summary")
def get_route_planning_summaries_api(
    planning_date: date = Query(..., description="Día de reparto"),
):
    return list_route_planning_summaries(planning_date)


@router.patch("/route-planning/summary/{summary_id}")
def patch_route_planning_summary_api(summary_id: int, body: RoutePlanningSummaryPatchBody):
    data = body.model_dump(exclude_unset=True)
    try:
        row = patch_route_planning_summary(summary_id, data)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    if not row:
        raise HTTPException(status_code=404, detail="Resumen no encontrado")
    return row


@router.get("/route-planning")
def list_route_planning_api(
    planning_date: date = Query(..., description="Día de reparto"),
    truck: str | None = Query(None, description="Filtrar por camión"),
):
    return get_route_planning(planning_date=planning_date, truck=truck)


@router.patch("/route-planning/{row_id}")
def patch_route_planning_api(row_id: int, body: RoutePlanningPatchBody):
    data = body.model_dump(exclude_unset=True)
    try:
        row = patch_route_planning(row_id, data)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    if not row:
        raise HTTPException(status_code=404, detail="Fila no encontrada")
    return row


@router.delete("/route-planning/{row_id}", status_code=204)
def delete_route_planning_api(row_id: int):
    if not delete_route_planning_row(row_id):
        raise HTTPException(status_code=404, detail="Fila no encontrada")
