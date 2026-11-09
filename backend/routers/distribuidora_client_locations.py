"""ERP — revisión de ubicaciones/fotos propuestas por chofer (P3)."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field

from backend.auth.permissions import require_erp_ops_staff
from backend.services.distribuidora import client_location_service as loc_svc

router = APIRouter(
    prefix="/distribuidora/client-locations",
    tags=["Client locations P3"],
)


class ReviewBody(BaseModel):
    approve: bool
    note: str | None = Field(None, max_length=500)


@router.get("/status")
def status(_user: dict = Depends(require_erp_ops_staff)):
    return loc_svc.schema_status()


@router.get("/pending")
def list_pending(
    limit: int = Query(100, ge=1, le=300),
    _user: dict = Depends(require_erp_ops_staff),
):
    return loc_svc.list_pending_for_erp(limit=limit)


@router.post("/proposals/{proposal_id}/review")
def review_proposal(
    proposal_id: int,
    body: ReviewBody,
    user: dict = Depends(require_erp_ops_staff),
):
    return loc_svc.review_proposal_erp(
        user=user,
        proposal_id=int(proposal_id),
        approve=bool(body.approve),
        note=body.note,
    )


@router.get("/clients/{client_id}/history")
def client_history(
    client_id: int,
    limit: int = Query(50, ge=1, le=200),
    _user: dict = Depends(require_erp_ops_staff),
):
    return loc_svc.history_for_client(int(client_id), limit=limit)
