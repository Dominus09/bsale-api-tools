"""ERP — Seguimiento de entregas (consulta). Solo lectura."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Query

from backend.auth.permissions import require_erp_ops_staff
from backend.services.distribuidora import delivery_seguimiento_service as seg_svc
from backend.services.distribuidora import delivery_visit_service as visit_svc

router = APIRouter(
    prefix="/distribuidora/seguimiento-entregas",
    tags=["Seguimiento de entregas ERP"],
    dependencies=[Depends(require_erp_ops_staff)],
)


@router.get("/status")
def status(_user: dict = Depends(require_erp_ops_staff)):
    return visit_svc.schema_status()


@router.get("/dashboard")
def dashboard(
    date: str | None = Query(None, description="YYYY-MM-DD America/Santiago"),
    driver_user_id: int | None = Query(None),
    plan_id: int | None = Query(None),
    status: str | None = Query(None, description="completed|partial|not_delivered|pending"),
    client_q: str | None = Query(None, description="Filtro texto cliente"),
    user: dict = Depends(require_erp_ops_staff),
):
    return seg_svc.get_dashboard(
        user=user,
        day=date,
        driver_user_id=driver_user_id,
        plan_id=plan_id,
        status=status,
        client_q=client_q,
    )


@router.get("/by-driver")
def by_driver(
    date: str | None = Query(None),
    driver_user_id: int | None = Query(None),
    plan_id: int | None = Query(None),
    user: dict = Depends(require_erp_ops_staff),
):
    """Alias liviano: mismas filas que dashboard.by_driver."""
    out = seg_svc.get_dashboard(
        user=user,
        day=date,
        driver_user_id=driver_user_id,
        plan_id=plan_id,
    )
    return {
        "tables_ready": out.get("tables_ready"),
        "date": out.get("date"),
        "timezone": out.get("timezone"),
        "panel_updated_at_local": out.get("panel_updated_at_local"),
        "items": out.get("by_driver") or [],
        "read_only": True,
    }


@router.get("/planes/{plan_id}/clientes")
def plan_clients(
    plan_id: int,
    status: str | None = Query(None),
    client_q: str | None = Query(None),
    user: dict = Depends(require_erp_ops_staff),
):
    return seg_svc.get_plan_clients(
        user=user,
        plan_id=int(plan_id),
        status=status,
        client_q=client_q,
    )


@router.get("/visitas/{visit_id}")
def visit_detail(
    visit_id: int,
    user: dict = Depends(require_erp_ops_staff),
):
    """Detalle + evidencias con URL firmada (TTL corto)."""
    return seg_svc.get_visit_evidence_for_erp(user=user, visit_id=int(visit_id))
