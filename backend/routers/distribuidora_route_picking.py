"""API picking por cliente (planificación de rutas)."""

from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query

from backend.services.distribuidora.route_picking_service import get_route_picking
from backend.auth.permissions import require_erp_ops_staff

router = APIRouter(
    prefix="/distribuidora",
    tags=["Distribuidora picking"],
    dependencies=[Depends(require_erp_ops_staff)],
)


@router.get("/route-picking")
def list_route_picking(
    planning_date: date = Query(..., description="Día de reparto"),
    truck: str = Query(..., min_length=1, description="Camión (mismo código que en planificación)"),
):
    try:
        return get_route_picking(planning_date=planning_date, truck=truck)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
