"""API Logística — auditoría de pesos."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query

from backend.services import logistics_weight_audit_service as svc
from backend.utils.auth_staff import require_staff_user

router = APIRouter(prefix="/logistics", tags=["logistics"])


@router.get("/weight-audit")
def weight_audit(
    company_id: int = Query(3, ge=1),
    office_id: int = Query(1, ge=1),
    _user: dict = Depends(require_staff_user),
):
    try:
        return svc.get_weight_audit(company_id=company_id, office_id=office_id)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/weight-audit/orders/{document_id}")
def weight_audit_order_detail(
    document_id: int,
    company_id: int = Query(3, ge=1),
    office_id: int = Query(1, ge=1),
    _user: dict = Depends(require_staff_user),
):
    try:
        data = svc.get_order_weight_detail(
            document_id=document_id,
            company_id=company_id,
            office_id=office_id,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    if not data:
        raise HTTPException(status_code=404, detail="Orden no encontrada")
    return data
