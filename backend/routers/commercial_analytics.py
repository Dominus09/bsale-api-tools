"""Analítica comercial vendedores — /analytics/commercial."""

from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from starlette.concurrency import run_in_threadpool

from backend.auth.permissions import require_management_access
from backend.services import commercial_analytics_service as svc

router = APIRouter(prefix="/analytics/commercial", tags=["Analítica comercial"])


class CommercialSimulatorBody(BaseModel):
    scenario: str = Field(..., description="recuperar_clientes | subir_ticket | cross_selling")
    seller: str | None = None
    pct_recuperacion: float = Field(0.3, ge=0.05, le=1.0)
    ticket_uplift_pct: float = Field(0.1, ge=0.01, le=0.5)
    cross_clients: int = Field(10, ge=1, le=200)
    date_from: date
    date_to: date
    compare_date_from: date | None = None
    compare_date_to: date | None = None
    document_type: str | None = None


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


@router.get("/bundle")
async def get_bundle(
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
    seller_limit: int = Query(50, ge=1, le=svc.MAX_ROWS),
    unique_limit: int = Query(300, ge=1, le=svc.MAX_ROWS),
    lost_limit: int = Query(100, ge=1, le=svc.MAX_ROWS),
    cross_limit: int = Query(100, ge=1, le=svc.MAX_ROWS),
    product_limit: int = Query(50, ge=1, le=svc.MAX_ROWS),
):
    """Una sola sesión DB: dashboard + summary + vendedores + clientes + productos."""
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

    def _run():
        return svc.get_commercial_bundle(
            f,
            seller_limit=seller_limit,
            unique_limit=unique_limit,
            lost_limit=lost_limit,
            cross_limit=cross_limit,
            product_limit=product_limit,
        )

    return await run_in_threadpool(_run)


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


@router.get("/seller-profile")
async def get_seller_profile(
    company_id: int = Query(3, ge=1),
    office_id: int = Query(1, ge=1),
    seller_name: str = Query(..., min_length=1),
    date_from: date = Query(...),
    date_to: date = Query(...),
    compare_date_from: date | None = Query(None),
    compare_date_to: date | None = Query(None),
    document_type: str | None = Query(None),
):
    if company_id != 3 or office_id != 1:
        raise HTTPException(status_code=400, detail="Solo soportado company_id=3 y office_id=1")
    f = svc.CommercialFilters(
        date_from=date_from,
        date_to=date_to,
        compare_date_from=compare_date_from,
        compare_date_to=compare_date_to,
        seller=seller_name,
        document_type=document_type,
    )
    return await run_in_threadpool(svc.get_seller_profile, f, seller_name)


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


@router.get("/commercial-map")
async def get_commercial_map(
    company_id: int = Query(3, ge=1),
    office_id: int = Query(1, ge=1),
    date_from: date = Query(...),
    date_to: date = Query(...),
    compare_date_from: date | None = Query(None),
    compare_date_to: date | None = Query(None),
    seller: str | None = Query(None),
    city: str | None = Query(None),
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
        document_type=document_type,
    )
    return await run_in_threadpool(svc.get_commercial_map, f, limit=limit)


@router.post("/simulator")
async def post_commercial_simulator(
    body: CommercialSimulatorBody,
    company_id: int = Query(3, ge=1),
    office_id: int = Query(1, ge=1),
):
    if company_id != 3 or office_id != 1:
        raise HTTPException(status_code=400, detail="Solo soportado company_id=3 y office_id=1")
    _validate_dates(body.date_from, body.date_to)
    f = svc.CommercialFilters(
        date_from=body.date_from,
        date_to=body.date_to,
        compare_date_from=body.compare_date_from,
        compare_date_to=body.compare_date_to,
        seller=body.seller,
        document_type=body.document_type,
    )

    def _run():
        return svc.run_commercial_simulator(
            f,
            scenario=body.scenario,
            seller=body.seller,
            pct_recuperacion=body.pct_recuperacion,
            ticket_uplift_pct=body.ticket_uplift_pct,
            cross_clients=body.cross_clients,
        )

    return await run_in_threadpool(_run)


@router.get("/validation")
async def get_validation(
    _admin: dict = Depends(require_management_access),
    company_id: int = Query(3, ge=1),
    office_id: int = Query(1, ge=1),
    date_from: date = Query(...),
    date_to: date = Query(...),
    compare_date_from: date | None = Query(None),
    compare_date_to: date | None = Query(None),
    seller: str | None = Query(None),
    city: str | None = Query(None),
    document_type: str | None = Query(None),
    bsale_dashboard_total: float | None = Query(
        None,
        description="Total del dashboard oficial Bsale para interpretar regla de negocio",
    ),
):
    """Auditoría pre-deploy del motor comercial (solo gerencia)."""
    if company_id != 3 or office_id != 1:
        raise HTTPException(status_code=400, detail="Solo soportado company_id=3 y office_id=1")
    f = _filters(
        date_from=date_from,
        date_to=date_to,
        compare_date_from=compare_date_from,
        compare_date_to=compare_date_to,
        seller=seller,
        city=city,
        document_type=document_type,
    )

    def _run() -> dict:
        return svc.get_commercial_validation(
            f,
            bsale_dashboard_total=bsale_dashboard_total,
        )

    return await run_in_threadpool(_run)
