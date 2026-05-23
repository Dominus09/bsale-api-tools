"""API dispatch_plan: confirmación, Excel facturación, facturación vinculada, picking."""

from __future__ import annotations

from datetime import date
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from backend.services.distribuidora import dispatch_plan_service as svc
from backend.utils.auth_staff import BearerDep, decode_staff_token, require_staff_user

router = APIRouter(prefix="/distribuidora/dispatch-plans", tags=["Distribuidora dispatch plan"])


class DispatchPlanOrderInput(BaseModel):
    oc_document_id: int
    oc_number: int | None = None
    route_order: int = 0
    client_id: int | None = None
    client_name: str | None = None
    address: str | None = None
    city: str | None = None
    seller_name: str | None = None
    payment_method: str | None = None
    document_type_to_generate: str | None = None
    oc_total_amount: float | None = None
    lat: float | None = None
    lng: float | None = None


class ConfirmDispatchPlanBody(BaseModel):
    plan_session_id: str = Field(..., min_length=8, max_length=64)
    truck_id: int
    route_name: str = Field(..., min_length=1, max_length=128)
    planning_name: str | None = Field(
        None,
        max_length=128,
        description="Nombre operativo ej. Castro Sur",
    )
    driver_count: int = Field(1, ge=0, le=10)
    assistant_count: int = Field(0, ge=0, le=10)
    driver_cost_clp: int = Field(0, ge=0)
    assistant_cost_clp: int = Field(0, ge=0)
    diesel_price_per_liter: float = Field(..., gt=0)
    km_total: float = Field(0, ge=0)
    duration_min: float = Field(0, ge=0)
    liters_estimated: float = Field(0, ge=0)
    fuel_cost_clp: int = Field(0, ge=0)
    ferry_cost_clp: int = Field(0, ge=0)
    toll_cost_clp: int = Field(0, ge=0)
    extras_cost_clp: int = Field(0, ge=0)
    crew_cost_clp: int = Field(0, ge=0)
    total_route_cost_clp: int = Field(0, ge=0)
    route_geometry: dict[str, Any] | None = None
    orders: list[DispatchPlanOrderInput] = Field(..., min_length=1)
    planning_date: date | None = None


class StatusBody(BaseModel):
    status: str


@router.get("")
def list_recent_plans(limit: int = Query(50, ge=1, le=200)):
    try:
        items = svc.list_recent_plans(limit=limit)
        return {"items": items}
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Error al cargar historial de planificaciones: {exc}",
        ) from exc


@router.get("/margin-audit")
def margin_audit():
    """Auditoría: campos margen/costo en raw_data Bsale (documentos y líneas)."""
    return svc.get_margin_audit()


@router.get("/by-session/{plan_session_id}")
def list_plans_by_session(plan_session_id: str):
    return {"items": svc.list_session_plans(plan_session_id)}


@router.post("/confirm")
def confirm_plan(body: ConfirmDispatchPlanBody):
    try:
        return svc.confirm_dispatch_plan(
            plan_session_id=body.plan_session_id,
            truck_id=body.truck_id,
            route_name=body.route_name,
            planning_name=body.planning_name,
            driver_count=body.driver_count,
            assistant_count=body.assistant_count,
            driver_cost_clp=body.driver_cost_clp,
            assistant_cost_clp=body.assistant_cost_clp,
            diesel_price_per_liter=body.diesel_price_per_liter,
            km_total=body.km_total,
            duration_min=body.duration_min,
            liters_estimated=body.liters_estimated,
            fuel_cost_clp=body.fuel_cost_clp,
            ferry_cost_clp=body.ferry_cost_clp,
            toll_cost_clp=body.toll_cost_clp,
            extras_cost_clp=body.extras_cost_clp,
            crew_cost_clp=body.crew_cost_clp,
            total_route_cost_clp=body.total_route_cost_clp,
            route_geometry=body.route_geometry,
            orders=[o.model_dump() for o in body.orders],
            planning_date=body.planning_date,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.get("/{plan_id}")
def get_plan(plan_id: int):
    data = svc.get_dispatch_plan(plan_id)
    if not data:
        raise HTTPException(status_code=404, detail="Plan no encontrado")
    return data


@router.get("/{plan_id}/dashboard")
def get_plan_dashboard(plan_id: int, authorization: BearerDep = None):
    role: str | None = None
    if authorization:
        try:
            role = str(decode_staff_token(authorization).get("role") or "")
        except HTTPException:
            role = None
    try:
        return svc.get_plan_dashboard(plan_id, user_role=role)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e


@router.patch("/{plan_id}/status")
def patch_plan_status(plan_id: int, body: StatusBody):
    try:
        return svc.update_dispatch_plan_status(plan_id, body.status)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.get("/{plan_id}/invoiced-documents")
def get_invoiced_documents(plan_id: int):
    try:
        return svc.get_invoiced_documents(plan_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e


@router.get("/{plan_id}/billing-export")
def export_billing_excel(plan_id: int):
    try:
        data, fname = svc.build_billing_excel_bytes(plan_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return StreamingResponse(
        iter([data]),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )


@router.get("/{plan_id}/picking-by-client")
def picking_by_client(
    plan_id: int,
    validate: bool = Query(True, description="Advertir OCs sin documento confirmado"),
):
    try:
        return svc.get_picking_by_client(plan_id, validate=validate)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.get("/{plan_id}/picking-by-product")
def picking_by_product(plan_id: int, validate: bool = Query(True)):
    try:
        return svc.get_picking_by_product(plan_id, validate=validate)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.post("/{plan_id}/repair-snapshot")
def repair_snapshot(plan_id: int, _user: dict = Depends(require_staff_user)):
    """Solo rellena campos vacíos del snapshot (planes históricos)."""
    try:
        return svc.repair_order_snapshots(plan_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.post("/{plan_id}/picking-generated")
def post_picking_generated(plan_id: int):
    try:
        return svc.mark_picking_generated(plan_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
