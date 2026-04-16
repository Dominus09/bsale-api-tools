"""Planificación y análisis: órdenes y ventas desde ``v_orders`` / ``v_sales``."""

from __future__ import annotations

from datetime import date

from fastapi import APIRouter, HTTPException, Query
from starlette.concurrency import run_in_threadpool

from backend.services.distribuidora import planning_views_service as pvs

router = APIRouter(prefix="/distribuidora", tags=["Distribuidora planificación"])


def _validate_date_range(start_date: date | None, end_date: date | None) -> None:
    if start_date is not None and end_date is not None and start_date > end_date:
        raise HTTPException(
            status_code=400,
            detail="start_date no puede ser mayor que end_date",
        )


@router.get("/orders/summary/sellers")
async def get_orders_summary_sellers(
    start_date: date | None = Query(None),
    end_date: date | None = Query(None),
    is_invoiced: bool | None = Query(None),
):
    _validate_date_range(start_date, end_date)

    def _run():
        items, totals = pvs.summary_orders_by_seller(
            start_date=start_date,
            end_date=end_date,
            is_invoiced=is_invoiced,
        )
        return {"items": items, "totals": totals}

    return await run_in_threadpool(_run)


@router.get("/orders/summary/cities")
async def get_orders_summary_cities(
    start_date: date | None = Query(None),
    end_date: date | None = Query(None),
    is_invoiced: bool | None = Query(None),
):
    _validate_date_range(start_date, end_date)

    def _run():
        items, totals = pvs.summary_orders_by_city(
            start_date=start_date,
            end_date=end_date,
            is_invoiced=is_invoiced,
        )
        return {"items": items, "totals": totals}

    return await run_in_threadpool(_run)


@router.get("/orders")
async def get_orders_planning(
    start_date: date | None = Query(None, description="YYYY-MM-DD, inclusive"),
    end_date: date | None = Query(None, description="YYYY-MM-DD, inclusive"),
    seller: str | None = Query(None, description="Coincidencia exacta en seller_name"),
    municipality: str | None = Query(None, description="Coincidencia exacta en municipality"),
    is_invoiced: bool | None = Query(None, description="Filtrar por facturación"),
    limit: int = Query(pvs.DEFAULT_LIST_LIMIT, ge=1, le=pvs.MAX_LIST_ROWS),
    offset: int = Query(0, ge=0),
):
    _validate_date_range(start_date, end_date)

    def _run():
        items, total = pvs.list_orders_view(
            start_date=start_date,
            end_date=end_date,
            seller=seller,
            municipality=municipality,
            is_invoiced=is_invoiced,
            limit=limit,
            offset=offset,
        )
        return {
            "total": total,
            "limit": min(limit, pvs.MAX_LIST_ROWS),
            "offset": offset,
            "items": items,
        }

    return await run_in_threadpool(_run)


@router.get("/sales")
async def get_sales_planning(
    start_date: date | None = Query(None, description="YYYY-MM-DD, inclusive"),
    end_date: date | None = Query(None, description="YYYY-MM-DD, inclusive"),
    seller: str | None = Query(None, description="Coincidencia exacta en seller_name"),
    municipality: str | None = Query(None, description="Coincidencia exacta en municipality"),
    limit: int = Query(pvs.DEFAULT_LIST_LIMIT, ge=1, le=pvs.MAX_LIST_ROWS),
    offset: int = Query(0, ge=0),
):
    _validate_date_range(start_date, end_date)

    def _run():
        items, total = pvs.list_sales_view(
            start_date=start_date,
            end_date=end_date,
            seller=seller,
            municipality=municipality,
            limit=limit,
            offset=offset,
        )
        return {
            "total": total,
            "limit": min(limit, pvs.MAX_LIST_ROWS),
            "offset": offset,
            "items": items,
        }

    return await run_in_threadpool(_run)
