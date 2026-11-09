"""API dispatch_plan: confirmación, Excel facturación, facturación vinculada, picking."""

from __future__ import annotations

import json
import time
from datetime import date
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from backend.services.distribuidora import dispatch_plan_service as svc
from backend.services.distribuidora import dispatch_plan_cuadratura_service as cuad_svc
from backend.services.distribuidora import dispatch_plan_load_batch_service as batch_svc
from backend.services.distribuidora import (
    dispatch_plan_load_organization_service as load_org_svc,
)
from backend.services.distribuidora.dispatch_plan_load_organization_service import (
    LoadOrganizationConflictError,
)
from backend.auth.permissions import (
    is_simple_planning_capability_only,
    require_erp_ops_staff,
)
from backend.services.distribuidora.dispatch_plan_access import (
    filter_plans_for_user,
    user_can_assign_driver_on_plan,
    user_can_read_dispatch_plan,
)
from backend.utils.auth_staff import BearerDep, decode_staff_token, require_staff_user
from backend.utils.dashboard_debug import log_dashboard_debug
from backend.utils.dashboard_stage import DashboardStageRun, log_picking_count_sql_reference
from backend.utils.ors_stability import log_error
from backend.utils.plan_debug import PLAN_DEBUG_RERAISE, log_plan_debug_context, plan_debug_on_error

router = APIRouter(
    prefix="/distribuidora/dispatch-plans",
    tags=["Distribuidora dispatch plan"],
    dependencies=[Depends(require_staff_user)],
)


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


class PickingGenerateBody(BaseModel):
    reason: str | None = Field(None, max_length=500)


class LoadBatchBody(BaseModel):
    name: str = Field(..., min_length=1, max_length=128)
    description: str | None = Field(None, max_length=500)


class LoadBatchUpdateBody(BaseModel):
    name: str = Field(..., min_length=1, max_length=128)
    description: str | None = Field(None, max_length=500)
    sort_order: int | None = Field(None, ge=0, le=999)


class PickingAssignmentItem(BaseModel):
    related_document_id: int
    load_batch_id: int | None = None
    oc_document_id: int | None = None
    document_number: int | None = None
    client_name: str | None = None
    document_total: float | None = None


class PickingAssignmentsBody(BaseModel):
    assignments: list[PickingAssignmentItem] = Field(default_factory=list)


class OrderLoadAssignmentItem(BaseModel):
    oc_document_id: int = Field(..., ge=1, description="document_id canónico Bsale/ERP")
    load_batch_id: int = Field(..., ge=1)


class LoadOrganizationBody(BaseModel):
    assignments: list[OrderLoadAssignmentItem] = Field(default_factory=list)
    organization_version: int = Field(..., ge=0)
    mark_organized: bool = False
    allow_split_client: bool = False


class AddOrderToPlanBody(BaseModel):
    oc_document_id: int | None = Field(None, ge=1)
    oc_document_ids: list[int] = Field(default_factory=list)
    regenerate_picking: bool = False
    reason: str | None = Field(None, max_length=500)
    acknowledge_incomplete_weight: bool = False
    acknowledge_unevaluable_weight: bool = False


def _deny_plan_if_out_of_scope(user: dict, plan_id: int) -> None:
    """Capability-only: 404 (sin filtrar existencia) si no hay vínculo explícito."""
    if not user_can_read_dispatch_plan(user, plan_id):
        raise HTTPException(status_code=404, detail="Plan no encontrado")


@router.get("")
def list_recent_plans(
    limit: int = Query(50, ge=1, le=200),
    user: dict = Depends(require_staff_user),
):
    try:
        items = svc.list_recent_plans(limit=limit)
        return {"items": filter_plans_for_user(user, items)}
    except Exception as exc:
        log_error("GET /dispatch-plans", exc)
        return {"items": []}


@router.get("/margin-audit")
def margin_audit(_ops: dict = Depends(require_erp_ops_staff)):
    """Auditoría: campos margen/costo en raw_data Bsale (documentos y líneas)."""
    return svc.get_margin_audit()


@router.get("/by-session/{plan_session_id}")
def list_plans_by_session(
    plan_session_id: str,
    user: dict = Depends(require_staff_user),
):
    try:
        items = svc.list_session_plans(plan_session_id)
        return {"items": filter_plans_for_user(user, items)}
    except Exception as exc:
        log_error("GET /dispatch-plans/by-session", exc)
        return {"items": []}


class AssignDriverBody(BaseModel):
    driver_user_id: int = Field(..., ge=1)
    reason: str = Field(..., min_length=3, max_length=500)
    company_id: int | None = Field(None, ge=1)
    office_id: int | None = Field(None, ge=1)


class UpdatePeonetasBody(BaseModel):
    # CHECK DB: assistant_count 0..10
    assistant_count: int = Field(..., ge=0, le=10)
    reason: str = Field(..., min_length=3, max_length=500)


@router.get("/available-drivers")
def list_available_drivers(
    company_id: int | None = Query(None, ge=1),
    office_id: int | None = Query(None, ge=1),
    diagnostics: bool = Query(False),
    user: dict = Depends(require_staff_user),
):
    from backend.services.distribuidora import dispatch_plan_driver_service as driver_svc

    # Capability-only puede listar choferes para asignar en planes propios.
    if not (
        driver_svc.can_assign_dispatch_driver(user)
        or is_simple_planning_capability_only(user)
    ):
        raise HTTPException(status_code=403, detail="Sin permiso para listar choferes")
    try:
        payload = driver_svc.list_available_drivers(
            company_id=company_id,
            office_id=office_id,
            include_diagnostics=bool(diagnostics),
        )
        return payload
    except Exception as exc:
        log_error("GET /dispatch-plans/available-drivers", exc)
        raise HTTPException(status_code=503, detail="No se pudieron listar choferes") from exc


@router.get("/driver-eligibility/{user_id}")
def get_driver_eligibility(
    user_id: int,
    company_id: int | None = Query(None, ge=1),
    office_id: int | None = Query(None, ge=1),
    user: dict = Depends(require_staff_user),
):
    """Requisitos pendientes de un usuario para ser chofer elegible (admin)."""
    from backend.services.distribuidora import dispatch_plan_driver_service as driver_svc

    if not (
        driver_svc.can_assign_dispatch_driver(user)
        or is_simple_planning_capability_only(user)
    ):
        raise HTTPException(status_code=403, detail="Sin permiso para diagnosticar choferes")
    try:
        return driver_svc.diagnose_driver_eligibility(
            int(user_id), company_id=company_id, office_id=office_id
        )
    except Exception as exc:
        log_error("GET /dispatch-plans/driver-eligibility", exc)
        raise HTTPException(status_code=503, detail="No se pudo diagnosticar elegibilidad") from exc


@router.get("/mine-as-driver")
def list_my_driver_plans(
    limit: int = Query(50, ge=1, le=200),
    user: dict = Depends(require_staff_user),
):
    """Prep PWA: despachos del chofer autenticado (requiere perfil chofer + PWA)."""
    from backend.auth.multiapp import APP_PWA, assert_profiles, get_scope, has_app_access
    from backend.services.distribuidora import dispatch_plan_driver_service as driver_svc

    scope = get_scope(user)
    if not has_app_access(scope, APP_PWA):
        raise HTTPException(status_code=403, detail="Acceso PWA requerido")
    assert_profiles(scope, ("chofer",), detail="Perfil chofer requerido")
    uid = user.get("id") or user.get("uid")
    if uid is None:
        raise HTTPException(status_code=401, detail="Usuario sin id")
    return {
        "items": driver_svc.list_plans_for_driver_user(int(uid), limit=limit),
        "driver_user_id": int(uid),
    }


@router.post("/confirm")
def confirm_plan(
    body: ConfirmDispatchPlanBody,
    _ops: dict = Depends(require_erp_ops_staff),
):
    """Confirmación ORS (mapa). Planificadores Excel capability-only → 403."""
    from backend.services.distribuidora.dispatch_plan_oc_guard import (
        OcConfirmConflictError,
        OcDocumentStateError,
    )

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
            planning_origin="ors",
            ors_metrics_calculated=True,
        )
    except OcConfirmConflictError as e:
        raise HTTPException(status_code=409, detail=str(e)) from e
    except OcDocumentStateError as e:
        raise HTTPException(status_code=409, detail=str(e)) from e
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.get("/{plan_id}")
def get_plan(plan_id: int, user: dict = Depends(require_staff_user)):
    _deny_plan_if_out_of_scope(user, plan_id)
    data = svc.get_dispatch_plan(plan_id)
    if not data:
        raise HTTPException(status_code=404, detail="Plan no encontrado")
    return data


@router.get("/{plan_id}/header")
def get_plan_header(plan_id: int, user: dict = Depends(require_staff_user)):
    """Cabecera liviana (sin geometry, sin órdenes)."""
    _deny_plan_if_out_of_scope(user, plan_id)
    try:
        plan = svc.get_plan_header(plan_id)
    except Exception as exc:
        log_error("GET /dispatch-plans/header", exc, planning_id=plan_id)
        plan = None
    if not plan:
        raise HTTPException(status_code=404, detail="Plan no encontrado")
    return {"plan": plan}


@router.get("/{plan_id}/driver")
def get_plan_driver(plan_id: int, user: dict = Depends(require_staff_user)):
    from backend.services.distribuidora import dispatch_plan_driver_service as driver_svc

    _deny_plan_if_out_of_scope(user, plan_id)
    try:
        return driver_svc.get_plan_driver(plan_id)
    except driver_svc.DriverAssignError as exc:
        raise HTTPException(
            status_code=exc.http_status,
            detail={"message": str(exc), "code": exc.code},
        ) from exc


@router.post("/{plan_id}/assign-driver")
def assign_plan_driver(
    plan_id: int,
    body: AssignDriverBody,
    user: dict = Depends(require_staff_user),
):
    from backend.services.distribuidora import dispatch_plan_driver_service as driver_svc

    if not user_can_assign_driver_on_plan(user, plan_id):
        raise HTTPException(status_code=403, detail="Sin permiso para asignar chofer")
    try:
        return driver_svc.assign_driver_to_plan(
            plan_id=plan_id,
            driver_user_id=int(body.driver_user_id),
            reason=body.reason,
            actor=user,
            company_id=body.company_id,
            office_id=body.office_id,
        )
    except driver_svc.DriverAssignError as exc:
        raise HTTPException(
            status_code=exc.http_status,
            detail={"message": str(exc), "code": exc.code},
        ) from exc
    except Exception as exc:
        log_error("POST /dispatch-plans/assign-driver", exc, planning_id=plan_id)
        raise HTTPException(status_code=503, detail="No se pudo asignar chofer") from exc


@router.post("/{plan_id}/peonetas")
def update_plan_peonetas(
    plan_id: int,
    body: UpdatePeonetasBody,
    user: dict = Depends(require_staff_user),
):
    """Actualiza cantidad de peonetas del despacho (assistant_count + peonetas_recorded)."""
    from backend.services.distribuidora import dispatch_plan_driver_service as driver_svc

    if not user_can_assign_driver_on_plan(user, plan_id):
        raise HTTPException(status_code=403, detail="Sin permiso para modificar peonetas")
    try:
        return driver_svc.update_peonetas_on_plan(
            plan_id=plan_id,
            assistant_count=body.assistant_count,
            reason=body.reason,
            actor=user,
        )
    except driver_svc.DriverAssignError as exc:
        raise HTTPException(
            status_code=exc.http_status,
            detail={"message": str(exc), "code": exc.code},
        ) from exc
    except Exception as exc:
        log_error("POST /dispatch-plans/peonetas", exc, planning_id=plan_id)
        raise HTTPException(status_code=503, detail="No se pudo actualizar peonetas") from exc


@router.get("/{plan_id}/driver-access/{driver_user_id}")
def check_driver_plan_access(
    plan_id: int,
    driver_user_id: int,
    user: dict = Depends(require_staff_user),
):
    """¿El chofer tiene acceso operativo al plan?

    - Asignadores (gerencia/operativo/adm): pueden consultar cualquier chofer.
    - Otros: solo pueden consultar su propio ``driver_user_id`` (no sondear ajenos).
    """
    from backend.services.distribuidora import dispatch_plan_driver_service as driver_svc

    _deny_plan_if_out_of_scope(user, plan_id)
    uid = user.get("id") or user.get("uid")
    is_assigner = driver_svc.can_assign_dispatch_driver(user) or (
        is_simple_planning_capability_only(user)
        and user_can_assign_driver_on_plan(user, plan_id)
    )
    if not is_assigner:
        if uid is None or int(uid) != int(driver_user_id):
            raise HTTPException(
                status_code=403,
                detail="Solo puede consultar su propio acceso de chofer",
            )
    return {
        "plan_id": plan_id,
        "driver_user_id": driver_user_id,
        "has_access": driver_svc.driver_has_access_to_plan(driver_user_id, plan_id),
    }


@router.get("/{plan_id}/dashboard")
def get_plan_dashboard(
    plan_id: int,
    user: dict = Depends(require_staff_user),
    authorization: BearerDep = None,
    include_items: bool = Query(False, description="Incluir filas invoiced_items (más pesado)"),
    include_margin: bool = Query(
        False,
        description="Calcular margen comercial en esta petición (lento; usar bajo demanda)",
    ),
):
    """Solo facturación y métricas; pickings en /picking-cliente y /picking-producto."""
    _deny_plan_if_out_of_scope(user, plan_id)
    stage_run = DashboardStageRun(plan_id)
    stage_run.log_stage(1, "start_endpoint")
    log_picking_count_sql_reference(plan_id)
    ctx: dict[str, Any] | None = None
    role: str | None = None
    if authorization:
        try:
            role = str(decode_staff_token(authorization).get("role") or "")
        except HTTPException:
            role = None
    t0 = time.perf_counter()
    try:
        result = svc.get_plan_dashboard(
            plan_id,
            user_role=role,
            include_items=include_items,
            include_margin=include_margin,
            stage_run=stage_run,
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except Exception as exc:
        stuck = stage_run.last_stage or "unknown"
        stuck_label = stage_run.last_label or "unknown"
        ctx = log_plan_debug_context(plan_id, "GET /dispatch-plans/{id}/dashboard")
        plan_debug_on_error("GET /dispatch-plans/{id}/dashboard", plan_id, exc, ctx)
        log_error(
            "GET /dispatch-plans/dashboard",
            exc,
            planning_id=plan_id,
            extra={"last_stage": stuck, "last_label": stuck_label},
        )
        if PLAN_DEBUG_RERAISE:
            raise HTTPException(
                status_code=500,
                detail=(
                    f"{type(exc).__name__}: {exc} "
                    f"(last_stage={stuck} {stuck_label})"
                ),
            ) from exc
        from backend.utils.ors_stability import empty_plan_dashboard

        plan = svc.get_plan_header(plan_id)
        result = empty_plan_dashboard(
            plan_id,
            plan,
            degraded_message=f"Error al cargar dashboard: {exc}",
        )
    stage_run.log_stage(5, "response_ready")
    duration_ms = (time.perf_counter() - t0) * 1000.0
    try:
        payload_bytes = len(json.dumps(result, default=str).encode("utf-8"))
    except Exception as json_exc:
        if ctx is None:
            ctx = log_plan_debug_context(plan_id, "GET /dispatch-plans/{id}/dashboard")
        plan_debug_on_error(
            "GET /dispatch-plans/{id}/dashboard#json",
            plan_id,
            json_exc,
            ctx,
        )
        if PLAN_DEBUG_RERAISE:
            raise HTTPException(
                status_code=500,
                detail=f"JSON serialize: {json_exc}",
            ) from json_exc
        payload_bytes = 0
    invoice_rows = len(result.get("invoiced_items") or []) if isinstance(result, dict) else 0
    log_dashboard_debug(
        planning_id=plan_id,
        duration_ms=duration_ms,
        invoice_rows=invoice_rows,
        payload_bytes=payload_bytes,
        include_margin=include_margin,
        include_items=include_items,
    )
    return result


@router.patch("/{plan_id}/status")
def patch_plan_status(
    plan_id: int,
    body: StatusBody,
    _ops: dict = Depends(require_erp_ops_staff),
):
    try:
        return svc.update_dispatch_plan_status(plan_id, body.status)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.post("/{plan_id}/refresh-commercial-margin")
def refresh_commercial_margin(
    plan_id: int,
    user: dict = Depends(require_staff_user),
):
    """
    Recalcula margen comercial desde documentos actuales y opcionalmente
    persiste el resultado en el plan (acción explícita del usuario).
    """
    from backend.auth.permissions import has_margin_view_access

    _deny_plan_if_out_of_scope(user, plan_id)
    if not has_margin_view_access(user):
        raise HTTPException(status_code=403, detail="Sin permiso de márgenes")
    try:
        return svc.compute_plan_margin(plan_id, persist=True)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except Exception as exc:
        log_error("POST /dispatch-plans/refresh-commercial-margin", exc, planning_id=plan_id)
        raise HTTPException(
            status_code=503, detail="No se pudo actualizar el margen comercial"
        ) from exc


@router.get("/{plan_id}/invoiced-documents")
def get_invoiced_documents(plan_id: int, user: dict = Depends(require_staff_user)):
    """Facturación vinculada (equivalente operacional a /facturacion)."""
    _deny_plan_if_out_of_scope(user, plan_id)
    try:
        return svc.get_invoiced_documents(plan_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except Exception as exc:
        log_error("GET /dispatch-plans/invoiced-documents", exc, planning_id=plan_id)
        from backend.utils.ors_stability import empty_invoiced_documents_response

        return empty_invoiced_documents_response(plan_id)


@router.get("/{plan_id}/billing-export")
def export_billing_excel(plan_id: int, user: dict = Depends(require_staff_user)):
    _deny_plan_if_out_of_scope(user, plan_id)
    try:
        data, fname = svc.build_billing_excel_bytes(plan_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return StreamingResponse(
        iter([data]),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )


def _picking_by_client_handler(
    plan_id: int,
    validate: bool,
    *,
    include_probable: bool = False,
    version: int | None = None,
    picking_id: int | None = None,
    load_batch_id: int | None = None,
) -> dict[str, Any]:
    ctx = log_plan_debug_context(plan_id, "GET /dispatch-plans/{id}/picking-cliente")
    t0 = time.perf_counter()
    try:
        result = svc.get_picking_by_client(
            plan_id,
            validate=validate,
            include_probable=include_probable,
            version=version,
            picking_id=picking_id,
            load_batch_id=load_batch_id,
        )
    except Exception as exc:
        plan_debug_on_error("GET /dispatch-plans/{id}/picking-cliente", plan_id, exc, ctx)
        log_error("GET /dispatch-plans/picking-cliente", exc, planning_id=plan_id)
        result = {
            "dispatch_plan_id": plan_id,
            "ready": False,
            "reason": str(exc),
            "clients": [],
            "degraded": True,
        }
    duration_ms = (time.perf_counter() - t0) * 1000.0
    clients = result.get("clients") if isinstance(result.get("clients"), list) else []
    try:
        payload_bytes = len(json.dumps(result, default=str).encode("utf-8"))
    except Exception:
        payload_bytes = 0
    log_dashboard_debug(
        planning_id=plan_id,
        duration_ms=duration_ms,
        invoice_rows=0,
        payload_bytes=payload_bytes,
    )
    return result


def _picking_by_product_handler(
    plan_id: int,
    validate: bool,
    *,
    include_probable: bool = False,
    version: int | None = None,
    picking_id: int | None = None,
    load_batch_id: int | None = None,
) -> dict[str, Any]:
    ctx = log_plan_debug_context(plan_id, "GET /dispatch-plans/{id}/picking-producto")
    t0 = time.perf_counter()
    try:
        result = svc.get_picking_by_product(
            plan_id,
            validate=validate,
            include_probable=include_probable,
            version=version,
            picking_id=picking_id,
            load_batch_id=load_batch_id,
        )
    except Exception as exc:
        plan_debug_on_error("GET /dispatch-plans/{id}/picking-producto", plan_id, exc, ctx)
        log_error("GET /dispatch-plans/picking-producto", exc, planning_id=plan_id)
        result = {
            "dispatch_plan_id": plan_id,
            "ready": False,
            "reason": str(exc),
            "items": [],
            "degraded": True,
        }
    duration_ms = (time.perf_counter() - t0) * 1000.0
    items = result.get("items") if isinstance(result.get("items"), list) else []
    try:
        payload_bytes = len(json.dumps(result, default=str).encode("utf-8"))
    except Exception:
        payload_bytes = 0
    log_dashboard_debug(
        planning_id=plan_id,
        duration_ms=duration_ms,
        invoice_rows=0,
        payload_bytes=payload_bytes,
    )
    return result


@router.post("/{plan_id}/picking/generate")
def post_picking_generate(
    plan_id: int,
    validate: bool = Query(True),
    include_probable: bool = Query(False),
    body: PickingGenerateBody | None = None,
    user: dict = Depends(require_erp_ops_staff),
):
    """Calcula y persiste picking (nueva versión; reemplaza la versión actual)."""
    try:
        reason = body.reason if body else None
        action = "regenerate" if reason else "generate"
        return svc.generate_plan_picking(
            plan_id,
            validate=validate,
            include_probable=include_probable,
            regenerated_by=str(user.get("email") or ""),
            regeneration_reason=reason,
            regeneration_action=action,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.get("/{plan_id}/pickings")
def list_pickings(
    plan_id: int,
    limit: int = Query(30, ge=1, le=100),
    user: dict = Depends(require_staff_user),
):
    _deny_plan_if_out_of_scope(user, plan_id)
    try:
        return svc.list_plan_pickings(plan_id, limit=limit)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e


@router.get("/{plan_id}/picking-cliente")
def picking_cliente(
    plan_id: int,
    validate: bool = Query(False, description="Ignorado si hay picking persistido"),
    include_probable: bool = Query(False, description="Ignorado en lectura persistida"),
    version: int | None = Query(None, ge=1),
    picking_id: int | None = Query(None, ge=1),
    load_batch_id: int | None = Query(None, ge=1),
    user: dict = Depends(require_staff_user),
):
    _deny_plan_if_out_of_scope(user, plan_id)
    return _picking_by_client_handler(
        plan_id,
        validate,
        include_probable=include_probable,
        version=version,
        picking_id=picking_id,
        load_batch_id=load_batch_id,
    )


@router.get("/{plan_id}/picking-producto")
def picking_producto(
    plan_id: int,
    validate: bool = Query(False),
    include_probable: bool = Query(False),
    version: int | None = Query(None, ge=1),
    picking_id: int | None = Query(None, ge=1),
    load_batch_id: int | None = Query(None, ge=1),
    user: dict = Depends(require_staff_user),
):
    _deny_plan_if_out_of_scope(user, plan_id)
    return _picking_by_product_handler(
        plan_id,
        validate,
        include_probable=include_probable,
        version=version,
        picking_id=picking_id,
        load_batch_id=load_batch_id,
    )


@router.get("/{plan_id}/picking-cliente/export")
def export_picking_cliente(
    plan_id: int,
    version: int | None = Query(None, ge=1),
    picking_id: int | None = Query(None, ge=1),
    user: dict = Depends(require_staff_user),
):
    _deny_plan_if_out_of_scope(user, plan_id)
    try:
        data, fname = svc.build_picking_client_excel_bytes(
            plan_id,
            version=version,
            picking_id=picking_id,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return StreamingResponse(
        iter([data]),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )


@router.get("/{plan_id}/picking-producto/export")
def export_picking_producto(
    plan_id: int,
    version: int | None = Query(None, ge=1),
    picking_id: int | None = Query(None, ge=1),
    load_batch_id: int | None = Query(None, ge=1),
    user: dict = Depends(require_staff_user),
):
    _deny_plan_if_out_of_scope(user, plan_id)
    try:
        data, fname = svc.build_picking_product_excel_bytes(
            plan_id,
            version=version,
            picking_id=picking_id,
            load_batch_id=load_batch_id,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return StreamingResponse(
        iter([data]),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )


@router.post("/{plan_id}/document-load-reconciliation")
def post_document_load_reconciliation(
    plan_id: int,
    overwrite_manual: bool = Query(False),
    user: dict = Depends(require_erp_ops_staff),
):
    """Concilia facturas/boletas del picking con grupos OC (organización anticipada)."""
    _deny_plan_if_out_of_scope(user, plan_id)
    try:
        return batch_svc.reconcile_document_load_assignments(
            plan_id, overwrite_manual=bool(overwrite_manual)
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


class DocumentLoadReassignmentBody(BaseModel):
    oc_document_ids: list[int] = Field(default_factory=list)
    related_document_ids: list[int] = Field(default_factory=list)
    confirm: bool = False


@router.post("/{plan_id}/document-load-reassignment/preview")
def post_document_load_reassignment_preview(
    plan_id: int,
    body: DocumentLoadReassignmentBody,
    user: dict = Depends(require_erp_ops_staff),
):
    """Vista previa de traslado documental selectivo (sin escritura)."""
    _deny_plan_if_out_of_scope(user, plan_id)
    try:
        return batch_svc.preview_document_load_reassignment(
            plan_id,
            oc_document_ids=list(body.oc_document_ids or []),
            related_document_ids=list(body.related_document_ids or []),
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.post("/{plan_id}/document-load-reassignment/apply")
def post_document_load_reassignment_apply(
    plan_id: int,
    body: DocumentLoadReassignmentBody,
    user: dict = Depends(require_erp_ops_staff),
):
    """Aplica traslado documental selectivo con confirmación explícita."""
    _deny_plan_if_out_of_scope(user, plan_id)
    try:
        return batch_svc.apply_document_load_reassignment(
            plan_id,
            oc_document_ids=list(body.oc_document_ids or []),
            related_document_ids=list(body.related_document_ids or []),
            confirm=bool(body.confirm),
            user=user,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.get("/{plan_id}/picking-by-client")
def picking_by_client(
    plan_id: int,
    validate: bool = Query(False),
    include_probable: bool = Query(False),
    version: int | None = Query(None, ge=1),
    picking_id: int | None = Query(None, ge=1),
    load_batch_id: int | None = Query(None, ge=1),
    user: dict = Depends(require_staff_user),
):
    """Retrocompatible: alias de /picking-cliente."""
    _deny_plan_if_out_of_scope(user, plan_id)
    return _picking_by_client_handler(
        plan_id,
        validate,
        include_probable=include_probable,
        version=version,
        picking_id=picking_id,
        load_batch_id=load_batch_id,
    )


@router.get("/{plan_id}/picking-by-product")
def picking_by_product(
    plan_id: int,
    validate: bool = Query(False),
    include_probable: bool = Query(False),
    version: int | None = Query(None, ge=1),
    picking_id: int | None = Query(None, ge=1),
    load_batch_id: int | None = Query(None, ge=1),
    user: dict = Depends(require_staff_user),
):
    """Retrocompatible: alias de /picking-producto."""
    _deny_plan_if_out_of_scope(user, plan_id)
    return _picking_by_product_handler(
        plan_id,
        validate,
        include_probable=include_probable,
        version=version,
        picking_id=picking_id,
        load_batch_id=load_batch_id,
    )


@router.get("/{plan_id}/load-batches")
def get_load_batches(plan_id: int, user: dict = Depends(require_staff_user)):
    _deny_plan_if_out_of_scope(user, plan_id)
    try:
        return batch_svc.list_load_batches(plan_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e


@router.post("/{plan_id}/load-batches")
def post_load_batch(
    plan_id: int,
    body: LoadBatchBody,
    user: dict = Depends(require_erp_ops_staff),
):
    _deny_plan_if_out_of_scope(user, plan_id)
    try:
        return batch_svc.create_load_batch(
            plan_id, name=body.name, description=body.description
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.put("/{plan_id}/load-batches/{batch_id}")
def put_load_batch(
    plan_id: int,
    batch_id: int,
    body: LoadBatchUpdateBody,
    user: dict = Depends(require_erp_ops_staff),
):
    _deny_plan_if_out_of_scope(user, plan_id)
    try:
        return batch_svc.update_load_batch(
            plan_id,
            batch_id,
            name=body.name,
            description=body.description,
            sort_order=body.sort_order,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.delete("/{plan_id}/load-batches/{batch_id}")
def delete_load_batch(
    plan_id: int,
    batch_id: int,
    user: dict = Depends(require_erp_ops_staff),
):
    _deny_plan_if_out_of_scope(user, plan_id)
    try:
        return batch_svc.delete_load_batch(plan_id, batch_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.get("/{plan_id}/picking-assignments")
def get_picking_assignments(plan_id: int, user: dict = Depends(require_staff_user)):
    _deny_plan_if_out_of_scope(user, plan_id)
    try:
        return batch_svc.get_picking_assignments(plan_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e


@router.put("/{plan_id}/picking-assignments")
def put_picking_assignments(
    plan_id: int,
    body: PickingAssignmentsBody,
    user: dict = Depends(require_erp_ops_staff),
):
    _deny_plan_if_out_of_scope(user, plan_id)
    try:
        return batch_svc.save_picking_assignments(
            plan_id, [a.model_dump() for a in body.assignments]
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.get("/{plan_id}/load-organization")
def get_load_organization(plan_id: int, user: dict = Depends(require_staff_user)):
    """Organización anticipada OC→grupo (pre-factura). No altera picking ni facturas."""
    _deny_plan_if_out_of_scope(user, plan_id)
    try:
        return load_org_svc.get_load_organization(plan_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e


@router.put("/{plan_id}/load-organization")
def put_load_organization(
    plan_id: int,
    body: LoadOrganizationBody,
    user: dict = Depends(require_erp_ops_staff),
):
    """Guarda asignaciones pre-factura. Atómico, idempotente, con versión optimista."""
    _deny_plan_if_out_of_scope(user, plan_id)
    try:
        return load_org_svc.save_load_organization(
            plan_id,
            assignments=[a.model_dump() for a in body.assignments],
            organization_version=body.organization_version,
            mark_organized=body.mark_organized,
            allow_split_client=body.allow_split_client,
            user=user,
        )
    except LoadOrganizationConflictError as e:
        raise HTTPException(status_code=409, detail=str(e)) from e
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.get("/{plan_id}/orders/search")
def search_orders_for_plan(
    plan_id: int,
    q: str = Query(..., min_length=2, max_length=80),
    limit: int = Query(20, ge=1, le=50),
    user: dict = Depends(require_staff_user),
):
    _deny_plan_if_out_of_scope(user, plan_id)
    try:
        return batch_svc.search_orders_for_plan(plan_id, q=q, limit=limit)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.get("/{plan_id}/orders/add-preview")
def preview_add_order(
    plan_id: int,
    oc_document_id: int | None = Query(None, ge=1),
    oc_document_ids: list[int] | None = Query(None),
    user: dict = Depends(require_staff_user),
):
    _deny_plan_if_out_of_scope(user, plan_id)
    try:
        return batch_svc.preview_add_order(
            plan_id,
            oc_document_id=oc_document_id,
            oc_document_ids=oc_document_ids,
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e


@router.post("/{plan_id}/orders/add")
def add_order_to_plan(
    plan_id: int,
    body: AddOrderToPlanBody,
    user: dict = Depends(require_erp_ops_staff),
):
    from backend.services.distribuidora.dispatch_plan_supplemental_order_service import (
        SupplementalOrderError,
    )

    _deny_plan_if_out_of_scope(user, plan_id)
    try:
        return batch_svc.add_order_to_plan(
            plan_id,
            oc_document_id=body.oc_document_id,
            oc_document_ids=body.oc_document_ids,
            regenerate_picking=body.regenerate_picking,
            reason=body.reason,
            user=user,
            acknowledge_incomplete_weight=body.acknowledge_incomplete_weight,
            acknowledge_unevaluable_weight=body.acknowledge_unevaluable_weight,
        )
    except SupplementalOrderError as e:
        raise HTTPException(
            status_code=int(e.http_status or 400),
            detail={"message": str(e), "code": e.code, **(e.details or {})},
        ) from e
    except ValueError as e:
        detail = str(e)
        status = 400
        low = detail.lower()
        if "sobrecarga" in low or "cuadratura" in low or "capacidad" in low:
            status = 409
        raise HTTPException(status_code=status, detail=detail) from e


@router.get("/{plan_id}/picking-regeneration-log")
def get_picking_regeneration_log(
    plan_id: int,
    limit: int = Query(50, ge=1, le=200),
    user: dict = Depends(require_staff_user),
):
    _deny_plan_if_out_of_scope(user, plan_id)
    return batch_svc.list_picking_regeneration_log(plan_id, limit=limit)


@router.post("/{plan_id}/repair-snapshot")
def repair_snapshot(plan_id: int, _user: dict = Depends(require_erp_ops_staff)):
    """Solo rellena campos vacíos del snapshot (planes históricos)."""
    try:
        return svc.repair_order_snapshots(plan_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.post("/{plan_id}/picking-generated")
def post_picking_generated(
    plan_id: int,
    _ops: dict = Depends(require_erp_ops_staff),
):
    try:
        return svc.mark_picking_generated(plan_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


class CuadraturaCreditNoteRow(BaseModel):
    documento_venta: str = ""
    nota_credito: str = ""
    monto: int = Field(0, ge=0)
    motivo: str = ""


class CuadraturaNotLoadedRow(BaseModel):
    cliente: str = ""
    documento: str = ""
    monto: int = Field(0, ge=0)
    motivo: str = ""


class CuadraturaDocumentRow(BaseModel):
    related_document_id: int | None = None
    document_number: int | None = None
    oc_document_id: int | None = None
    client_name: str = ""
    monto_clp: int = Field(0, ge=0)
    medio_pago: str = "pendiente"
    observacion: str = ""
    route_order: int | None = None


class CuadraturaCreditNoteV2Row(BaseModel):
    documento: str = ""
    nota_credito: str = ""
    monto: int = Field(0, ge=0)
    observacion: str = ""


class CuadraturaNotLoadedV2Row(BaseModel):
    producto: str = ""
    producto_variante: str | None = None
    cantidad: float = Field(0, ge=0)
    motivo: str = ""
    product_id: int | None = None
    variant_id: int | None = None
    codigo_barras: str | None = None


class CuadraturaCashCountRow(BaseModel):
    denominacion_clp: int = Field(..., ge=1)
    cantidad: int = Field(0, ge=0)
    subtotal_clp: int = Field(0, ge=0)


class CuadraturaSaveBody(BaseModel):
    schema_version: int | None = None
    transferencia_clp: int = Field(0, ge=0)
    efectivo_clp: int = Field(0, ge=0)
    cheque_clp: int = Field(0, ge=0)
    debito_clp: int = Field(0, ge=0)
    observacion: str | None = None
    credit_notes: list[CuadraturaCreditNoteRow] = Field(default_factory=list)
    not_loaded: list[CuadraturaNotLoadedRow] = Field(default_factory=list)
    documents: list[CuadraturaDocumentRow] | None = None
    credit_notes_v2: list[CuadraturaCreditNoteV2Row] = Field(default_factory=list)
    not_loaded_v2: list[CuadraturaNotLoadedV2Row] = Field(default_factory=list)
    cash_count: list[CuadraturaCashCountRow] = Field(default_factory=list)
    expected_picking_version: int | None = Field(
        None, ge=1, description="Versión adoptada esperada (concurrencia F3-D3)."
    )
    adopted_picking_version: int | None = Field(
        None, ge=1, description="Alias de expected_picking_version."
    )


class CuadraturaCloseBody(BaseModel):
    observacion: str | None = None
    closed_by: str | None = None


class CuadraturaAdoptPickingBody(BaseModel):
    expected_available_version: int | None = Field(
        None, ge=1, description="Versión vigente que el cliente pretende adoptar."
    )


def _raise_cuadratura_error(exc: Exception) -> None:
    from backend.services.distribuidora.dispatch_plan_cuadratura_service import (
        CuadraturaConflictError,
    )

    if isinstance(exc, CuadraturaConflictError):
        raise HTTPException(
            status_code=int(exc.http_status or 409),
            detail={"message": str(exc), "code": exc.code, **(exc.details or {})},
        ) from exc
    if isinstance(exc, ValueError):
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/{plan_id}/cuadratura")
def get_cuadratura(plan_id: int, user: dict = Depends(require_staff_user)):
    _deny_plan_if_out_of_scope(user, plan_id)
    try:
        return cuad_svc.get_dispatch_plan_cuadratura(plan_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except Exception as exc:
        log_error("GET /dispatch-plans/cuadratura", exc)
        raise HTTPException(status_code=500, detail="Error al cargar cuadratura") from exc


@router.put("/{plan_id}/cuadratura")
def put_cuadratura(
    plan_id: int,
    body: CuadraturaSaveBody,
    user: dict = Depends(require_erp_ops_staff),
):
    _deny_plan_if_out_of_scope(user, plan_id)
    try:
        payload = body.model_dump(exclude_none=True)
        if body.documents is not None:
            payload["documents"] = [r.model_dump() for r in body.documents]
            payload["schema_version"] = 2
        if body.credit_notes_v2 is not None:
            payload["credit_notes_v2"] = [r.model_dump() for r in body.credit_notes_v2]
        if body.not_loaded_v2 is not None:
            payload["not_loaded_v2"] = [r.model_dump() for r in body.not_loaded_v2]
        if body.cash_count is not None:
            payload["cash_count"] = [r.model_dump() for r in body.cash_count]
        if body.credit_notes:
            payload["credit_notes"] = [r.model_dump() for r in body.credit_notes]
        if body.not_loaded:
            payload["not_loaded"] = [r.model_dump() for r in body.not_loaded]
        return cuad_svc.save_dispatch_plan_cuadratura(plan_id, payload)
    except Exception as exc:
        try:
            _raise_cuadratura_error(exc)
        except HTTPException:
            raise
        log_error("PUT /dispatch-plans/cuadratura", exc)
        raise HTTPException(status_code=500, detail="Error al guardar cuadratura") from exc


@router.post("/{plan_id}/cuadratura/adopt-picking")
def adopt_cuadratura_picking(
    plan_id: int,
    body: CuadraturaAdoptPickingBody | None = None,
    user: dict = Depends(require_erp_ops_staff),
):
    _deny_plan_if_out_of_scope(user, plan_id)
    try:
        data = body.model_dump(exclude_none=True) if body else {}
        return cuad_svc.adopt_cuadratura_picking(
            plan_id,
            user=user,
            expected_available_version=data.get("expected_available_version"),
        )
    except Exception as exc:
        try:
            _raise_cuadratura_error(exc)
        except HTTPException:
            raise
        log_error("POST /dispatch-plans/cuadratura/adopt-picking", exc)
        raise HTTPException(
            status_code=500, detail="Error al adoptar picking en cuadratura"
        ) from exc


@router.post("/{plan_id}/cuadratura/close")
def close_cuadratura(
    plan_id: int,
    body: CuadraturaCloseBody | None = None,
    user: dict = Depends(require_erp_ops_staff),
):
    _deny_plan_if_out_of_scope(user, plan_id)
    try:
        data = body.model_dump() if body else {}
        return cuad_svc.close_dispatch_plan_cuadratura(
            plan_id,
            closed_by=data.get("closed_by") or (
                user.get("email") or user.get("name") or user.get("sub")
            ),
            observacion=data.get("observacion"),
        )
    except Exception as exc:
        try:
            _raise_cuadratura_error(exc)
        except HTTPException:
            raise
        log_error("POST /dispatch-plans/cuadratura/close", exc)
        raise HTTPException(status_code=500, detail="Error al cerrar cuadratura") from exc


class DeliveryStopStatusBody(BaseModel):
    status: str = Field(..., pattern=r"^(pending|delivered)$")


@router.get("/by-code/{planning_code}/delivery-map")
def delivery_map_by_code(
    planning_code: str,
    user: dict = Depends(require_staff_user),
):
    """Mapa de entregas por código PLAN-xxxxx (uso en terreno)."""
    from backend.services.distribuidora import delivery_map_service as dmap

    try:
        plan_id = dmap.resolve_plan_id_from_picking_number(planning_code)
        if plan_id is None:
            raise LookupError(f"Plan '{planning_code}' no encontrado")
        _deny_plan_if_out_of_scope(user, int(plan_id))
        return dmap.get_delivery_map(plan_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        log_error("GET /dispatch-plans/by-code/delivery-map", exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/{plan_id}/delivery-map")
def delivery_map(
    plan_id: int,
    user: dict = Depends(require_staff_user),
):
    """Clientes/destinos del plan agrupados (1 marcador por cliente) + GPS."""
    _deny_plan_if_out_of_scope(user, plan_id)
    from backend.services.distribuidora import delivery_map_service as dmap

    try:
        return dmap.get_delivery_map(plan_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        log_error("GET /dispatch-plans/delivery-map", exc, planning_id=plan_id)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/{plan_id}/delivery-map/stops/{customer_key}/status")
def delivery_map_set_stop_status(
    plan_id: int,
    customer_key: str,
    body: DeliveryStopStatusBody,
    user: dict = Depends(require_staff_user),
):
    """Marca parada cliente como delivered/pending (persiste en order_events).

    Requiere el mismo alcance de lectura que GET delivery-map (capability-only /
    vínculo explícito). P1 no habilita escritura PWA-chofer en este endpoint.
    """
    _deny_plan_if_out_of_scope(user, plan_id)
    from backend.services.distribuidora import delivery_map_service as dmap

    try:
        return dmap.set_delivery_stop_status(
            plan_id,
            customer_key,
            body.status,
            user_email=str(user.get("email") or "").strip() or "staff",
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        log_error("POST /dispatch-plans/delivery-map/status", exc, planning_id=plan_id)
        raise HTTPException(status_code=500, detail=str(exc)) from exc
