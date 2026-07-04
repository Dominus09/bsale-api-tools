"""API Análisis de Notas de Crédito — independiente del CRM Comercial."""

from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query
from starlette.concurrency import run_in_threadpool

from backend.auth.permissions import require_admin_access, require_management_access
from backend.services import returns_analytics_service as svc
from backend.services.sync_bsale_returns import (
    sync_bsale_returns_history,
    sync_bsale_returns_incremental,
)

router = APIRouter(prefix="/returns-analytics", tags=["returns-analytics"])


@router.get("/dashboard")
async def dashboard(
    _user: dict = Depends(require_management_access),
    date_from: date | None = Query(None),
    date_to: date | None = Query(None),
):
    return await run_in_threadpool(svc.get_dashboard, date_from=date_from, date_to=date_to)


@router.get("/rankings")
async def rankings(
    _user: dict = Depends(require_management_access),
    date_from: date | None = Query(None),
    date_to: date | None = Query(None),
):
    return await run_in_threadpool(svc.get_rankings, date_from=date_from, date_to=date_to)


@router.get("/returns")
async def list_returns(
    _user: dict = Depends(require_management_access),
    date_from: date | None = Query(None),
    date_to: date | None = Query(None),
    limit: int = Query(50, ge=1, le=200),
):
    return await run_in_threadpool(
        svc.list_returns,
        date_from=date_from,
        date_to=date_to,
        limit=limit,
    )


@router.get("/returns/{return_id}")
async def return_detail(
    return_id: int,
    _user: dict = Depends(require_management_access),
):
    try:
        return await run_in_threadpool(svc.get_return_detail, return_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/map")
async def map_data(
    _user: dict = Depends(require_management_access),
    date_from: date | None = Query(None),
    date_to: date | None = Query(None),
):
    return await run_in_threadpool(svc.get_map_data, date_from=date_from, date_to=date_to)


@router.get("/timeline")
async def timeline(
    _user: dict = Depends(require_management_access),
    date_from: date | None = Query(None),
    date_to: date | None = Query(None),
    grain: str = Query("day", pattern="^(day|week|month|year)$"),
):
    return await run_in_threadpool(
        svc.get_timeline,
        date_from=date_from,
        date_to=date_to,
        grain=grain,
    )


@router.get("/insights")
async def insights(
    _user: dict = Depends(require_management_access),
    date_from: date | None = Query(None),
    date_to: date | None = Query(None),
):
    return await run_in_threadpool(svc.get_insights, date_from=date_from, date_to=date_to)


@router.post("/sync/history")
async def trigger_history_sync(
    _user: dict = Depends(require_admin_access),
    resume: bool = Query(False),
    force: bool = Query(False),
):
    def _run():
        result = sync_bsale_returns_history(resume=resume, force=force)
        if not result.get("ok"):
            raise ValueError(result.get("error") or "Bootstrap histórico no ejecutado")
        return result

    try:
        return await run_in_threadpool(_run)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post("/sync/incremental")
async def trigger_incremental_sync(
    _user: dict = Depends(require_management_access),
):
    def _run():
        result = sync_bsale_returns_incremental()
        if not result.get("ok"):
            raise ValueError(result.get("error") or "Sync incremental falló")
        return result

    try:
        return await run_in_threadpool(_run)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.get("/sync/status")
async def sync_status(_user: dict = Depends(require_management_access)):
    return await run_in_threadpool(svc.get_sync_status)
