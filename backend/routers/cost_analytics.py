"""API Analítica → Costos."""

from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query

from backend.services import cost_analytics_service as svc
from backend.services.sync_cost_receptions import sync_cost_receptions
from backend.utils.auth_staff import require_staff_user

router = APIRouter(prefix="/cost-analytics", tags=["cost-analytics"])


@router.get("/dashboard")
def cost_dashboard(
    company_id: int = Query(..., ge=1),
    office_id: int | None = Query(None, ge=1),
    date_from: datetime | None = Query(None),
    date_to: datetime | None = Query(None),
    _user: dict = Depends(require_staff_user),
):
    try:
        return svc.get_dashboard(
            company_id,
            office_id=office_id,
            date_from=date_from,
            date_to=date_to,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/offices")
def list_offices(
    company_id: int = Query(..., ge=1),
    _user: dict = Depends(require_staff_user),
):
    return svc.list_offices(company_id)


@router.get("/history/search")
def search_history(
    company_id: int = Query(..., ge=1),
    q: str = Query(..., min_length=2, max_length=120),
    limit: int = Query(50, ge=1, le=200),
    _user: dict = Depends(require_staff_user),
):
    try:
        return svc.search_cost_history(company_id, q, limit=limit)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/history")
def list_history(
    company_id: int = Query(..., ge=1),
    q: str | None = Query(None, max_length=120),
    variant_id: int | None = Query(None, ge=1),
    office_id: int | None = Query(None, ge=1),
    date_from: datetime | None = Query(None),
    date_to: datetime | None = Query(None),
    limit: int = Query(200, ge=1, le=500),
    offset: int = Query(0, ge=0),
    _user: dict = Depends(require_staff_user),
):
    return svc.list_history(
        company_id,
        q=q,
        variant_id=variant_id,
        office_id=office_id,
        date_from=date_from,
        date_to=date_to,
        limit=limit,
        offset=offset,
    )


@router.get("/history/variants/{variant_id}")
def variant_history(
    variant_id: int,
    company_id: int = Query(..., ge=1),
    limit: int = Query(200, ge=1, le=500),
    _user: dict = Depends(require_staff_user),
):
    return svc.get_variant_cost_history(company_id, variant_id, limit=limit)


@router.get("/receptions")
def list_receptions(
    company_id: int = Query(..., ge=1),
    date_from: datetime | None = Query(None),
    date_to: datetime | None = Query(None),
    office_id: int | None = Query(None, ge=1),
    document_type: str | None = Query(None, max_length=64),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    _user: dict = Depends(require_staff_user),
):
    return svc.list_receptions(
        company_id,
        date_from=date_from,
        date_to=date_to,
        office_id=office_id,
        document_type=document_type,
        limit=limit,
        offset=offset,
    )


@router.get("/receptions/{reception_id}")
def get_reception(
    reception_id: int,
    company_id: int = Query(..., ge=1),
    _user: dict = Depends(require_staff_user),
):
    try:
        return svc.get_reception(company_id, reception_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/alerts")
def cost_alerts(
    company_id: int = Query(..., ge=1),
    office_id: int | None = Query(None, ge=1),
    limit: int = Query(100, ge=1, le=500),
    _user: dict = Depends(require_staff_user),
):
    return svc.list_alerts(company_id, office_id=office_id, limit=limit)


@router.get("/products")
def list_products(
    company_id: int = Query(..., ge=1),
    q: str | None = Query(None, max_length=120),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    _user: dict = Depends(require_staff_user),
):
    return svc.list_products(company_id, q=q, limit=limit, offset=offset)


@router.get("/opportunities")
def list_opportunities(
    company_id: int = Query(..., ge=1),
    status: str | None = Query(
        None, pattern="^(oportunidad_compra|riesgo_comercial)$"
    ),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    _user: dict = Depends(require_staff_user),
):
    return svc.list_opportunities(
        company_id, status=status, limit=limit, offset=offset
    )


@router.get("/compare/branches")
def compare_branches(
    company_id: int = Query(..., ge=1),
    q: str | None = Query(None, max_length=120),
    limit: int = Query(20, ge=1, le=100),
    _user: dict = Depends(require_staff_user),
):
    return svc.list_branch_comparison(company_id, q=q, limit=limit)


@router.get("/margin-impact")
def margin_impact(
    company_id: int = Query(..., ge=1),
    variant_id: int = Query(..., ge=1),
    _user: dict = Depends(require_staff_user),
):
    return svc.get_margin_impact(company_id, variant_id)


def _user_email(user: dict) -> str:
    email = (user.get("email") or "").strip().lower()
    if not email:
        raise HTTPException(status_code=401, detail="Token sin email")
    return email


@router.get("/intelligence")
def cost_intelligence(
    company_id: int = Query(..., ge=1),
    user: dict = Depends(require_staff_user),
):
    return svc.get_intelligence(company_id, user_email=_user_email(user))


@router.get("/watchlist")
def get_watchlist(
    company_id: int = Query(..., ge=1),
    user: dict = Depends(require_staff_user),
):
    return svc.list_watchlist(company_id, user_email=_user_email(user))


@router.get("/watchlist/status")
def watchlist_variant_status(
    company_id: int = Query(..., ge=1),
    variant_id: int = Query(..., ge=1),
    user: dict = Depends(require_staff_user),
):
    return svc.watchlist_status_for_variant(
        company_id, variant_id, user_email=_user_email(user)
    )


@router.post("/watchlist")
def post_watchlist(
    company_id: int = Query(..., ge=1),
    variant_id: int = Query(..., ge=1),
    user: dict = Depends(require_staff_user),
):
    return svc.add_to_watchlist(
        company_id, variant_id, user_email=_user_email(user)
    )


@router.delete("/watchlist")
def delete_watchlist(
    company_id: int = Query(..., ge=1),
    variant_id: int = Query(..., ge=1),
    user: dict = Depends(require_staff_user),
):
    return svc.remove_from_watchlist(
        company_id, variant_id, user_email=_user_email(user)
    )


@router.get("/compare/offices")
def compare_offices(
    company_id: int = Query(..., ge=1),
    variant_id: int | None = Query(None, ge=1),
    q: str | None = Query(None, max_length=120),
    _user: dict = Depends(require_staff_user),
):
    try:
        return svc.compare_offices(company_id, variant_id=variant_id, q=q)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/sync")
def trigger_sync(
    company_id: int | None = Query(None, ge=1),
    lookback_days: int | None = Query(None, ge=1, le=365),
    _user: dict = Depends(require_staff_user),
):
    try:
        return sync_cost_receptions(
            company_id=company_id,
            lookback_days=lookback_days,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
