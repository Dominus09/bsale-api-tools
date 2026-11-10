"""ERP — visibilidad operativa de visitas de entrega (P4). Solo lectura."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Query

from backend.auth.multiapp import require_app
from backend.services.distribuidora import delivery_visit_service as visit_svc

router = APIRouter(
    prefix="/distribuidora/delivery-visits",
    tags=["Delivery visits ERP P4"],
    dependencies=[Depends(require_app("erp"))],
)


@router.get("/status")
def status(user: dict = Depends(require_app("erp"))):
    _ = user
    return visit_svc.schema_status()


@router.get("")
def list_visits(
    limit: int = Query(100, ge=1, le=200),
    user: dict = Depends(require_app("erp")),
):
    return visit_svc.list_visits_for_erp(user=user, limit=limit)


@router.get("/{visit_id}")
def get_visit(
    visit_id: int,
    user: dict = Depends(require_app("erp")),
):
    """Detalle + URLs firmadas de evidencia/fachada (bucket privado)."""
    return visit_svc.get_visit_detail_for_erp(user=user, visit_id=int(visit_id))
