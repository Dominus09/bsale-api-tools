"""Análisis comercial de clientes (ventas ``v_sales`` + ``bsale.clients``)."""

from __future__ import annotations

from datetime import date

from fastapi import APIRouter, HTTPException, Query
from starlette.concurrency import run_in_threadpool

from backend.services.distribuidora import clients_analytics_service as cas

router = APIRouter(prefix="/distribuidora", tags=["Distribuidora clientes"])


def _validate_date_range(start_date: date | None, end_date: date | None) -> None:
    if start_date is not None and end_date is not None and start_date > end_date:
        raise HTTPException(
            status_code=400,
            detail="start_date no puede ser mayor que end_date",
        )


@router.get("/clients/frequency")
async def get_clients_frequency(
    start_date: date | None = Query(None),
    end_date: date | None = Query(None),
    seller: str | None = Query(None, description="Coincidencia exacta en seller_name"),
    municipality: str | None = Query(None, description="Coincidencia exacta en municipality"),
    limit: int = Query(2000, ge=1, le=cas.MAX_ANALYTICS_ROWS),
):
    _validate_date_range(start_date, end_date)

    def _run():
        rows = cas.list_clients_frequency(
            start_date=start_date,
            end_date=end_date,
            seller=seller,
            municipality=municipality,
            limit=limit,
        )
        return {"items": rows}

    return await run_in_threadpool(_run)


@router.get("/clients/inactive")
async def get_clients_inactive(
    days: int = Query(7, ge=1, le=3650, description="Umbral: días sin comprar (vs última venta UTC)"),
    limit: int = Query(2000, ge=1, le=cas.MAX_ANALYTICS_ROWS),
):
    def _run():
        rows = cas.list_clients_inactive(days=days, limit=limit)
        return {"items": rows}

    return await run_in_threadpool(_run)


@router.get("/clients/top")
async def get_clients_top(
    limit: int = Query(20, ge=1, le=cas.MAX_ANALYTICS_ROWS),
):
    def _run():
        rows = cas.list_clients_top(limit=limit)
        return {"items": rows}

    return await run_in_threadpool(_run)


@router.get("/clients/summary/sellers")
async def get_clients_summary_sellers(
    limit: int = Query(500, ge=1, le=cas.MAX_ANALYTICS_ROWS),
):
    def _run():
        items, totals = cas.summary_clients_by_seller(limit=limit)
        return {"items": items, "totals": totals}

    return await run_in_threadpool(_run)


@router.get("/clients")
async def get_clients_consolidated(
    start_date: date | None = Query(None),
    end_date: date | None = Query(None),
    seller: str | None = Query(None),
    municipality: str | None = Query(None),
    limit: int = Query(cas.CONSOLIDATED_DEFAULT_LIMIT, ge=1, le=cas.MAX_ANALYTICS_ROWS),
    offset: int = Query(0, ge=0),
):
    _validate_date_range(start_date, end_date)

    def _run():
        rows = cas.list_clients_consolidated(
            start_date=start_date,
            end_date=end_date,
            seller=seller,
            municipality=municipality,
            limit=limit,
            offset=offset,
        )
        return {"items": rows, "limit": limit, "offset": offset}

    return await run_in_threadpool(_run)
