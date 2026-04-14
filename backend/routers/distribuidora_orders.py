"""API órdenes de compra Distribuidora (vista enriquecida)."""

from __future__ import annotations

from datetime import date

from fastapi import APIRouter, HTTPException, Query

from backend.services.distribuidora.orders_service import (
    get_purchase_order_detail,
    list_purchase_orders,
)

router = APIRouter(prefix="/distribuidora", tags=["Distribuidora órdenes"])


@router.get("/orders/purchase")
def get_orders_purchase(
    only_not_invoiced: bool = Query(False),
    emission_date_from: date | None = Query(None),
    emission_date_to: date | None = Query(None),
    delivery_search: str | None = Query(
        None,
        description="Búsqueda ILIKE dentro de observaciones (ej. martes)",
    ),
    client_id: int | None = Query(None),
    user_id: int | None = Query(None),
    limit: int = Query(500, ge=1, le=5000),
    offset: int = Query(0, ge=0),
):
    rows, total = list_purchase_orders(
        only_not_invoiced=only_not_invoiced,
        emission_date_from=emission_date_from,
        emission_date_to=emission_date_to,
        delivery_search=delivery_search,
        client_id=client_id,
        user_id=user_id,
        limit=limit,
        offset=offset,
    )
    return {"total": total, "limit": limit, "offset": offset, "items": rows}


@router.get("/orders/purchase/{document_id}")
def get_order_purchase_detail(document_id: int):
    data = get_purchase_order_detail(document_id)
    if not data:
        raise HTTPException(status_code=404, detail="OC no encontrada")
    return data
