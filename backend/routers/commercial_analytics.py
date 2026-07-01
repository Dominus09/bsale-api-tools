"""Analítica comercial vendedores — /analytics/commercial."""

from __future__ import annotations

from datetime import date

from fastapi import APIRouter, HTTPException, Query
from starlette.concurrency import run_in_threadpool

from backend.services import commercial_analytics_service as svc

router = APIRouter(prefix="/analytics/commercial", tags=["Analítica comercial"])


def _validate_dates(date_from: date, date_to: date) -> None:
    if date_from > date_to:
        raise HTTPException(status_code=400, detail="date_from no puede ser mayor que date_to")


def _filters(
    *,
    date_from: date,
    date_to: date,
    compare_date_from: date | None = None,
    compare_date_to: date | None = None,
    seller: str | None = None,
    city: str | None = None,
    client_id: int | None = None,
    document_type: str | None = None,
) -> svc.CommercialFilters:
    _validate_dates(date_from, date_to)
    if compare_date_from and compare_date_to:
        _validate_dates(compare_date_from, compare_date_to)
    return svc.CommercialFilters(
        date_from=date_from,
        date_to=date_to,
        compare_date_from=compare_date_from,
        compare_date_to=compare_date_to,
        seller=seller,
        city=city,
        client_id=client_id,
        document_type=document_type,
    )


@router.get("/filter-options")
async def get_filter_options():
    return await run_in_threadpool(svc.list_filter_options)


@router.get("/dashboard")
async def get_dashboard(
    company_id: int = Query(3, ge=1),
    office_id: int = Query(1, ge=1),
    date_from: date = Query(...),
    date_to: date = Query(...),
    compare_date_from: date | None = Query(None),
    compare_date_to: date | None = Query(None),
    seller: str | None = Query(None),
    city: str | None = Query(None),
    client_id: int | None = Query(None),
    document_type: str | None = Query(None),
):
    if company_id != 3 or office_id != 1:
        raise HTTPException(status_code=400, detail="Solo soportado company_id=3 y office_id=1")
    f = _filters(
        date_from=date_from,
        date_to=date_to,
        compare_date_from=compare_date_from,
        compare_date_to=compare_date_to,
        seller=seller,
        city=city,
        client_id=client_id,
        document_type=document_type,
    )
    return await run_in_threadpool(svc.get_dashboard, f)


@router.get("/seller-performance")
async def get_seller_performance(
    company_id: int = Query(3, ge=1),
    office_id: int = Query(1, ge=1),
    date_from: date = Query(...),
    date_to: date = Query(...),
    compare_date_from: date | None = Query(None),
    compare_date_to: date | None = Query(None),
    seller: str | None = Query(None),
    city: str | None = Query(None),
    client_id: int | None = Query(None),
    document_type: str | None = Query(None),
    limit: int = Query(50, ge=1, le=svc.MAX_ROWS),
):
    if company_id != 3 or office_id != 1:
        raise HTTPException(status_code=400, detail="Solo soportado company_id=3 y office_id=1")
    f = _filters(
        date_from=date_from,
        date_to=date_to,
        compare_date_from=compare_date_from,
        compare_date_to=compare_date_to,
        seller=seller,
        city=city,
        client_id=client_id,
        document_type=document_type,
    )
    return await run_in_threadpool(svc.get_seller_performance, f, limit=limit)


@router.get("/unique-clients")
async def get_unique_clients(
    company_id: int = Query(3, ge=1),
    office_id: int = Query(1, ge=1),
    date_from: date = Query(...),
    date_to: date = Query(...),
    compare_date_from: date | None = Query(None),
    compare_date_to: date | None = Query(None),
    seller: str | None = Query(None),
    city: str | None = Query(None),
    client_id: int | None = Query(None),
    document_type: str | None = Query(None),
    limit: int = Query(500, ge=1, le=svc.MAX_ROWS),
):
    if company_id != 3 or office_id != 1:
        raise HTTPException(status_code=400, detail="Solo soportado company_id=3 y office_id=1")
    f = _filters(
        date_from=date_from,
        date_to=date_to,
        compare_date_from=compare_date_from,
        compare_date_to=compare_date_to,
        seller=seller,
        city=city,
        client_id=client_id,
        document_type=document_type,
    )
    return await run_in_threadpool(svc.get_unique_clients, f, limit=limit)


@router.get("/lost-clients")
async def get_lost_clients(
    company_id: int = Query(3, ge=1),
    office_id: int = Query(1, ge=1),
    date_from: date = Query(...),
    date_to: date = Query(...),
    compare_date_from: date | None = Query(None),
    compare_date_to: date | None = Query(None),
    seller: str | None = Query(None),
    city: str | None = Query(None),
    client_id: int | None = Query(None),
    document_type: str | None = Query(None),
    limit: int = Query(200, ge=1, le=svc.MAX_ROWS),
):
    if company_id != 3 or office_id != 1:
        raise HTTPException(status_code=400, detail="Solo soportado company_id=3 y office_id=1")
    f = _filters(
        date_from=date_from,
        date_to=date_to,
        compare_date_from=compare_date_from,
        compare_date_to=compare_date_to,
        seller=seller,
        city=city,
        client_id=client_id,
        document_type=document_type,
    )
    return await run_in_threadpool(svc.get_lost_clients, f, limit=limit)


@router.get("/recovered-clients")
async def get_recovered_clients(
    company_id: int = Query(3, ge=1),
    office_id: int = Query(1, ge=1),
    date_from: date = Query(...),
    date_to: date = Query(...),
    compare_date_from: date | None = Query(None),
    compare_date_to: date | None = Query(None),
    seller: str | None = Query(None),
    city: str | None = Query(None),
    client_id: int | None = Query(None),
    document_type: str | None = Query(None),
    limit: int = Query(200, ge=1, le=svc.MAX_ROWS),
):
    if company_id != 3 or office_id != 1:
        raise HTTPException(status_code=400, detail="Solo soportado company_id=3 y office_id=1")
    f = _filters(
        date_from=date_from,
        date_to=date_to,
        compare_date_from=compare_date_from,
        compare_date_to=compare_date_to,
        seller=seller,
        city=city,
        client_id=client_id,
        document_type=document_type,
    )
    return await run_in_threadpool(svc.get_recovered_clients, f, limit=limit)


@router.get("/product-performance")
async def get_product_performance(
    company_id: int = Query(3, ge=1),
    office_id: int = Query(1, ge=1),
    date_from: date = Query(...),
    date_to: date = Query(...),
    compare_date_from: date | None = Query(None),
    compare_date_to: date | None = Query(None),
    seller: str | None = Query(None),
    city: str | None = Query(None),
    client_id: int | None = Query(None),
    document_type: str | None = Query(None),
    limit: int = Query(100, ge=1, le=svc.MAX_ROWS),
):
    if company_id != 3 or office_id != 1:
        raise HTTPException(status_code=400, detail="Solo soportado company_id=3 y office_id=1")
    f = _filters(
        date_from=date_from,
        date_to=date_to,
        compare_date_from=compare_date_from,
        compare_date_to=compare_date_to,
        seller=seller,
        city=city,
        client_id=client_id,
        document_type=document_type,
    )
    return await run_in_threadpool(svc.get_product_performance, f, seller=seller, limit=limit)


@router.get("/cross-selling")
async def get_cross_selling(
    company_id: int = Query(3, ge=1),
    office_id: int = Query(1, ge=1),
    date_from: date = Query(...),
    date_to: date = Query(...),
    seller: str | None = Query(None),
    city: str | None = Query(None),
    client_id: int | None = Query(None),
    document_type: str | None = Query(None),
    limit: int = Query(100, ge=1, le=svc.MAX_ROWS),
):
    if company_id != 3 or office_id != 1:
        raise HTTPException(status_code=400, detail="Solo soportado company_id=3 y office_id=1")
    f = _filters(
        date_from=date_from,
        date_to=date_to,
        seller=seller,
        city=city,
        client_id=client_id,
        document_type=document_type,
    )
    return await run_in_threadpool(svc.get_cross_selling, f, limit=limit)


@router.get("/client-profile")
async def get_client_profile(
    company_id: int = Query(3, ge=1),
    office_id: int = Query(1, ge=1),
    client_id: int = Query(...),
    date_from: date = Query(...),
    date_to: date = Query(...),
    document_type: str | None = Query(None),
):
    if company_id != 3 or office_id != 1:
        raise HTTPException(status_code=400, detail="Solo soportado company_id=3 y office_id=1")
    f = svc.CommercialFilters(
        date_from=date_from,
        date_to=date_to,
        document_type=document_type,
    )
    return await run_in_threadpool(svc.get_client_profile, f, client_id)


@router.get("/summary")
async def get_summary(
    company_id: int = Query(3, ge=1),
    office_id: int = Query(1, ge=1),
    date_from: date = Query(...),
    date_to: date = Query(...),
    compare_date_from: date | None = Query(None),
    compare_date_to: date | None = Query(None),
    seller: str | None = Query(None),
    city: str | None = Query(None),
    client_id: int | None = Query(None),
    document_type: str | None = Query(None),
):
    if company_id != 3 or office_id != 1:
        raise HTTPException(status_code=400, detail="Solo soportado company_id=3 y office_id=1")
    f = _filters(
        date_from=date_from,
        date_to=date_to,
        compare_date_from=compare_date_from,
        compare_date_to=compare_date_to,
        seller=seller,
        city=city,
        client_id=client_id,
        document_type=document_type,
    )
    return await run_in_threadpool(svc.get_summary, f)
