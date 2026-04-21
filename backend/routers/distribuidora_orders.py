"""API órdenes de compra Distribuidora (vista enriquecida)."""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from fastapi import APIRouter, HTTPException, Query

from backend.repositories.distribuidora.route_planning_repo import (
    fetch_enriched_orders_by_document_ids,
)
from backend.services.distribuidora.orders_service import (
    get_purchase_order_detail,
    list_dispatch_prep_by_municipality,
    list_dispatch_prep_observation_texts,
    list_dispatch_prep_planning_rows,
    list_purchase_orders,
)
from backend.services.distribuidora.sync_service import (
    DistribuidoraSyncService,
    bsale_token_distribuidora_configured,
)

router = APIRouter(prefix="/distribuidora", tags=["Distribuidora órdenes"])
logger = logging.getLogger(__name__)

# Ventana corta (días calendario UTC) para traer documentos recientes desde Bsale.
_RESYNC_OC_CALENDAR_DAYS = 2


def _preview_enriched_row(r: dict[str, Any]) -> dict[str, Any]:
    out = dict(r)
    for k, v in list(out.items()):
        if isinstance(v, Decimal):
            out[k] = float(v) if v is not None else None
    return out


@router.get("/orders/purchase/by-document-ids")
def get_orders_purchase_by_document_ids(
    ids: str = Query(
        ...,
        description="document_id separados por coma",
        max_length=8000,
    ),
):
    parts: list[int] = []
    for x in ids.split(","):
        x = x.strip()
        if not x:
            continue
        try:
            parts.append(int(x))
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Id inválido: {x}") from None
    if not parts:
        raise HTTPException(status_code=400, detail="Sin ids")
    if len(parts) > 500:
        raise HTTPException(status_code=400, detail="Máximo 500 document_id")
    rows = fetch_enriched_orders_by_document_ids(parts)
    return {"items": [_preview_enriched_row(r) for r in rows]}


@router.get("/orders/purchase")
def get_orders_purchase(
    only_not_invoiced: bool = Query(False),
    emission_date_from: date | None = Query(None),
    emission_date_to: date | None = Query(None),
    delivery_search: str | None = Query(
        None,
        description="ILIKE en observaciones; varios términos separados por coma (OR).",
    ),
    municipality: str | None = Query(
        None,
        description="Comuna/ciudad exacta (COALESCE municipality,city); varias separadas por coma (OR).",
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
        municipality=municipality,
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


@router.get("/orders/dispatch-prep/by-municipality")
def get_dispatch_prep_by_municipality(
    emission_date_from: date = Query(..., description="Inicio inclusive (fecha local)"),
    emission_date_to: date = Query(..., description="Fin inclusive (fecha local)"),
    only_not_invoiced: bool = Query(
        True,
        description="Si true: solo documentos con state = 0 (pendiente Bsale). Si false: todos los state.",
    ),
    day_filter: str | None = Query(
        None,
        description="Opcional: lunes|martes|miercoles|jueves|viernes|sabado (coincidencia en observaciones).",
    ),
):
    rows = list_dispatch_prep_by_municipality(
        emission_date_from=emission_date_from,
        emission_date_to=emission_date_to,
        only_not_invoiced=only_not_invoiced,
        day_filter=day_filter,
    )
    return {"items": rows}


@router.get("/orders/dispatch-prep/observaciones")
def get_dispatch_prep_observaciones(
    emission_date_from: date = Query(...),
    emission_date_to: date = Query(...),
    only_not_invoiced: bool = Query(True),
    limit: int = Query(2000, ge=1, le=5000),
    day_filter: str | None = Query(
        None,
        description="Opcional: día de la semana (mismo criterio que by-municipality).",
    ),
):
    texts = list_dispatch_prep_observation_texts(
        emission_date_from=emission_date_from,
        emission_date_to=emission_date_to,
        only_not_invoiced=only_not_invoiced,
        limit=limit,
        day_filter=day_filter,
    )
    return {"items": texts}


@router.get("/orders/dispatch-prep/planning-rows")
def get_dispatch_prep_planning_rows(
    emission_date_from: date = Query(...),
    emission_date_to: date = Query(...),
    only_not_invoiced: bool = Query(True),
    day_filter: str | None = Query(None),
    limit: int = Query(5000, ge=1, le=5000),
):
    rows = list_dispatch_prep_planning_rows(
        emission_date_from=emission_date_from,
        emission_date_to=emission_date_to,
        only_not_invoiced=only_not_invoiced,
        day_filter=day_filter,
        limit=limit,
    )
    return {"items": rows}


@router.post("/resync-oc")
def post_resync_oc():
    """
    Re-sincroniza Bsale → ``distribuidora.*`` para los últimos días (UTC), vía
    ``DistribuidoraSyncService.run_resync``. El GET de Bsale es por rango de emisión;
    las OC se persisten como ``document_type_id = 33``.
    """
    if not bsale_token_distribuidora_configured():
        logger.error("resync-oc: sin BSALE_TOKEN / BSALE_TOKEN_SPA")
        return {"ok": False, "error": "sin_token"}
    try:
        now = datetime.now(timezone.utc)
        start_day = now.date() - timedelta(days=_RESYNC_OC_CALENDAR_DAYS - 1)
        emission_from = datetime(
            start_day.year,
            start_day.month,
            start_day.day,
            0,
            0,
            0,
            tzinfo=timezone.utc,
        )
        result = DistribuidoraSyncService.run_resync(
            emission_from=emission_from,
            emission_to=now,
            strict_token=True,
        )
        total = int(result.get("documents_processed", 0) or 0)
        errores = int(result.get("document_errors", 0) or 0)
        return {
            "ok": True,
            "total": total,
            "errores": errores,
            "result": result,
        }
    except Exception as e:
        logger.error("Resync OC error: %s", e, exc_info=True)
        return {"ok": False, "error": str(e)}
