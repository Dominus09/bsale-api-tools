"""Análisis comercial de clientes (ventas ``v_sales`` + ``bsale.clients``)."""

from __future__ import annotations

from datetime import date
from io import BytesIO

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from starlette.concurrency import run_in_threadpool

from backend.services.distribuidora import clientes_analisis_completo_service as cac
from backend.services.distribuidora import clients_analytics_service as cas

router = APIRouter(prefix="/distribuidora", tags=["Distribuidora clientes"])


def _parse_seller_ids_csv(raw: str | None, *, max_items: int = 24) -> list[int] | None:
    if raw is None or not str(raw).strip():
        return None
    out: list[int] = []
    for part in str(raw).split(","):
        p = part.strip()
        if not p:
            continue
        try:
            out.append(int(p))
        except ValueError:
            continue
    if not out:
        return None
    return out[:max_items]


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


@router.get("/clients/dashboard")
async def get_clients_dashboard(
    chart_days: int = Query(30, ge=7, le=120),
    kpi_year: int | None = Query(None, ge=2000, le=2100),
    kpi_month: int | None = Query(None, ge=1, le=12),
    recover_min_days: int = Query(7, ge=1, le=3650),
):
    """Bundle para dashboard comercial: ventas diarias, vendedores, KPI mes y clientes a recuperar."""

    def _run():
        return cas.clients_commercial_dashboard(
            chart_days=chart_days,
            kpi_year=kpi_year,
            kpi_month=kpi_month,
            recover_min_days=recover_min_days,
        )

    return await run_in_threadpool(_run)


@router.get("/clients/summary/sellers")
async def get_clients_summary_sellers(
    limit: int = Query(500, ge=1, le=cas.MAX_ANALYTICS_ROWS),
    start_date: date | None = Query(None),
    end_date: date | None = Query(None),
    seller_ids: str | None = Query(
        None,
        description="IDs de vendedor Bsale (``documents.seller_id``) separados por coma, ej. 80,85,59,89",
    ),
):
    _validate_date_range(start_date, end_date)

    def _run():
        ids = _parse_seller_ids_csv(seller_ids)
        items, totals = cas.summary_clients_by_seller(
            limit=limit,
            start_date=start_date,
            end_date=end_date,
            seller_ids=ids,
        )
        return {"items": items, "totals": totals}

    return await run_in_threadpool(_run)


@router.get("/clientes/analisis")
async def get_clientes_analisis(
    limit: int = Query(5000, ge=1, le=cac.MAX_ANALISIS_CLIENTES),
):
    """
    Análisis de clientes con montos 30/60 días, frecuencia mensual (año calendario actual)
    y clasificación A–E (misma lógica que negocio definida en SQL).
    """

    def _run():
        return {"items": cac.list_clientes_analisis(limit=limit)}

    return await run_in_threadpool(_run)


@router.get("/clientes/analisis/export")
def get_clientes_analisis_export(
    limit: int = Query(10000, ge=1, le=cac.MAX_ANALISIS_CLIENTES),
):
    """Descarga Excel (.xlsx) con el mismo criterio que GET /clientes/analisis."""
    data, fname = cac.build_clientes_analisis_excel_bytes(limit=limit)
    return StreamingResponse(
        BytesIO(data),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )


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
