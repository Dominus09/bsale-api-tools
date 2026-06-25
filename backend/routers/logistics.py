"""API Logística — peso de órdenes y auditoría."""

from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field

from backend.routers.products_master import ProductMasterLogisticsPatch, _apply_patch
from backend.services import logistics_weight_audit_service as audit_svc
from backend.services import order_weight_service as ow_svc
from backend.utils.auth_staff import require_staff_user

router = APIRouter(prefix="/logistics", tags=["logistics"])


class CreateLogisticsBody(BaseModel):
    variant_id: int = Field(..., ge=1)
    company_id: int = Field(3, ge=1)


@router.get("/order-weights/search")
def search_order_weights(
    company_id: int = Query(3, ge=1),
    office_id: int = Query(1, ge=1),
    oc: int | None = Query(None, ge=1),
    cliente: str | None = Query(None, max_length=120),
    codigo_cliente: str | None = Query(None, max_length=40),
    date_from: date | None = Query(None),
    date_to: date | None = Query(None),
    estado: str | None = Query(None, pattern=r"^(completo|parcial|sin_peso|pendiente)$"),
    only_open: bool = Query(True),
    limit: int = Query(100, ge=1, le=500),
    _user: dict = Depends(require_staff_user),
):
    try:
        return ow_svc.search_orders(
            company_id=company_id,
            office_id=office_id,
            oc=oc,
            cliente=cliente,
            codigo_cliente=codigo_cliente,
            date_from=date_from,
            date_to=date_to,
            estado=estado,
            only_open=only_open,
            limit=limit,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/order-weights/{document_id}")
def get_order_weight(
    document_id: int,
    company_id: int = Query(3, ge=1),
    office_id: int = Query(1, ge=1),
    line_filter: str | None = Query(
        None,
        pattern=r"^(all|completo|manual|estimado|sin_peso)$",
    ),
    _user: dict = Depends(require_staff_user),
):
    try:
        data = ow_svc.get_order_weight(
            document_id=document_id,
            company_id=company_id,
            office_id=office_id,
            line_filter=line_filter or "all",
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    if not data:
        raise HTTPException(status_code=404, detail="Orden no encontrada")
    return data


@router.post("/order-weights/{document_id}/recalculate")
def recalculate_order_weight(
    document_id: int,
    company_id: int = Query(3, ge=1),
    office_id: int = Query(1, ge=1),
    user: dict = Depends(require_staff_user),
):
    try:
        return ow_svc.recalculate_order_weight(
            document_id=document_id,
            company_id=company_id,
            office_id=office_id,
            user_email=user.get("email"),
            persist=True,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.patch("/order-weights/products/{products_master_id}")
def patch_order_weight_product(
    products_master_id: int,
    body: ProductMasterLogisticsPatch,
    document_id: int = Query(..., ge=1),
    company_id: int = Query(3, ge=1),
    office_id: int = Query(1, ge=1),
    user: dict = Depends(require_staff_user),
):
    try:
        updated = _apply_patch(products_master_id, None, body)
        order = ow_svc.recalculate_order_weight(
            document_id=document_id,
            company_id=company_id,
            office_id=office_id,
            user_email=user.get("email"),
            persist=True,
        )
        return {"product": updated, "order": order}
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/order-weights/products/create-from-variant")
def create_logistics_from_variant(
    body: CreateLogisticsBody,
    document_id: int = Query(..., ge=1),
    office_id: int = Query(1, ge=1),
    user: dict = Depends(require_staff_user),
):
    try:
        created = ow_svc.create_logistics_from_variant(
            variant_id=body.variant_id,
            company_id=body.company_id,
        )
        order = ow_svc.recalculate_order_weight(
            document_id=document_id,
            company_id=body.company_id,
            office_id=office_id,
            user_email=user.get("email"),
            persist=True,
        )
        return {"product": created, "order": order}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/order-weights/{document_id}/history")
def order_weight_history(
    document_id: int,
    limit: int = Query(20, ge=1, le=100),
    _user: dict = Depends(require_staff_user),
):
    return ow_svc.get_order_history(document_id, limit=limit)


@router.get("/order-weights/{document_id}/export")
def export_order_weight(
    document_id: int,
    company_id: int = Query(3, ge=1),
    _user: dict = Depends(require_staff_user),
):
    csv_text = ow_svc.export_order_csv(document_id=document_id, company_id=company_id)
    return PlainTextResponse(
        content="\ufeff" + csv_text,
        media_type="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": f'attachment; filename="oc-peso-{document_id}.csv"',
        },
    )


@router.get("/weight-audit")
def weight_audit(
    company_id: int = Query(3, ge=1),
    office_id: int = Query(1, ge=1),
    _user: dict = Depends(require_staff_user),
):
    try:
        return audit_svc.get_weight_audit(company_id=company_id, office_id=office_id)
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
        data = audit_svc.get_order_weight_detail(
            document_id=document_id,
            company_id=company_id,
            office_id=office_id,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    if not data:
        raise HTTPException(status_code=404, detail="Orden no encontrada")
    return data
