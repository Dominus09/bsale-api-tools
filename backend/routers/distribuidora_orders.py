"""API órdenes de compra Distribuidora (vista enriquecida)."""

from __future__ import annotations

import logging
import time
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from fastapi import APIRouter, BackgroundTasks, Body, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict

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
from backend.services.distribuidora.resync_oc_jobs import (
    create_job,
    get_job,
    run_resync_oc_job,
)
from backend.services.distribuidora.distribuidora_sync_status_service import (
    get_distribuidora_sync_status_payload,
)
from backend.services.distribuidora.sync_related_service import (
    run_sync_distribuidora_related_background,
    sync_distribuidora_related_documents,
)
from backend.services.distribuidora.live_sync_service import run_live_sync_on_demand
from backend.services.distribuidora.sync_service import (
    bsale_token_distribuidora_configured,
    sync_bsale_distribuidora_orders_incremental,
    sync_bsale_distribuidora_sales_incremental,
)

router = APIRouter(prefix="/distribuidora", tags=["Distribuidora órdenes"])
logger = logging.getLogger(__name__)

# Ventana corta (días calendario UTC) para traer documentos recientes desde Bsale.
_RESYNC_OC_CALENDAR_DAYS = 2


def _run_distribuidora_sync_sales_task() -> None:
    """Ejecutado vía ``BackgroundTasks`` (no bloquea HTTP)."""
    t0 = time.perf_counter()
    logger.info("distribuidora sync-sales background: inicio")
    try:
        stats = sync_bsale_distribuidora_sales_incremental(strict_token=True)
        if stats.get("skipped"):
            logger.warning(
                "distribuidora sync-sales background: omitido %s",
                stats.get("skip_reason"),
            )
            return
        if stats.get("omitido_concurrencia"):
            logger.warning("distribuidora sync-sales background: omitido por lock")
            return
        try:
            run_sync_distribuidora_related_background()
        except Exception:
            logger.exception("distribuidora sync-sales background: related falló")
    except Exception:
        logger.exception("distribuidora sync-sales background: error")
    finally:
        logger.info(
            "distribuidora sync-sales background: fin duracion_s=%.3f",
            time.perf_counter() - t0,
        )


class ResyncOcStartBody(BaseModel):
    model_config = ConfigDict(extra="ignore")
    emission_date_from: date | None = None
    emission_date_to: date | None = None


def _resync_oc_emission_bounds(
    body: ResyncOcStartBody | None,
) -> tuple[datetime, datetime, str, str]:
    """
    Devuelve ``(emission_from_utc, emission_to_utc, emission_date_from_str, emission_date_to_str)``.
    Si el cuerpo no trae fechas, usa la ventana corta de días calendario UTC.
    """
    now = datetime.now(timezone.utc)
    b = body or ResyncOcStartBody()
    if b.emission_date_from is not None and b.emission_date_to is not None:
        d0, d1 = b.emission_date_from, b.emission_date_to
        if d0 > d1:
            d0, d1 = d1, d0
        emission_from = datetime(
            d0.year,
            d0.month,
            d0.day,
            0,
            0,
            0,
            tzinfo=timezone.utc,
        )
        end_of_range = datetime(
            d1.year,
            d1.month,
            d1.day,
            23,
            59,
            59,
            tzinfo=timezone.utc,
        )
        emission_to = min(now, end_of_range)
        if emission_from > emission_to:
            emission_to = now
        return emission_from, emission_to, d0.isoformat(), d1.isoformat()

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
    return emission_from, now, start_day.isoformat(), now.date().isoformat()


def _preview_enriched_row(r: dict[str, Any]) -> dict[str, Any]:
    out = dict(r)
    for k, v in list(out.items()):
        if isinstance(v, Decimal):
            out[k] = float(v) if v is not None else None
    return out


@router.get("/sync-status")
def get_distribuidora_sync_status():
    """
    Último estado de sync tipado (``documents_orders``, ``documents_sales``) + métricas en
    ``last_message`` (JSON) y ``sync_logs`` para ``running`` / ``error``.
    """
    return get_distribuidora_sync_status_payload()


@router.post("/sync/live-now")
def post_distribuidora_sync_live_now():
    """
    Sync on-demand: cadena live documents → details → related → probable_matches.
    No ejecuta backfills mensuales.
    """
    if not bsale_token_distribuidora_configured():
        raise HTTPException(status_code=503, detail="sin_token")

    result = run_live_sync_on_demand(strict_token=True)
    if result.get("status") == "already_running":
        return JSONResponse(status_code=409, content=result)
    if not result.get("ok"):
        raise HTTPException(
            status_code=500,
            detail=result.get("message") or "live_sync_error",
        )
    return result


@router.post("/sync-orders")
def post_distribuidora_sync_orders():
    """
    Sync incremental de órdenes (tipo 33) y, si termina bien, sync de ``document_related``
    (secuencial, misma petición HTTP).
    """
    if not bsale_token_distribuidora_configured():
        raise HTTPException(status_code=503, detail="sin_token")

    order_stats = sync_bsale_distribuidora_orders_incremental(strict_token=True)
    if order_stats.get("skipped"):
        raise HTTPException(
            status_code=503,
            detail=str(order_stats.get("skip_reason") or "sync_omitido"),
        )
    if order_stats.get("omitido_concurrencia"):
        raise HTTPException(status_code=409, detail="sync_en_curso")

    orders_processed = int(order_stats.get("documents_processed") or 0)
    logger.info("[SYNC ORDERS] documentos procesados: %s", orders_processed)

    related_stats = sync_distribuidora_related_documents(strict_token=True)
    related_processed = int(related_stats.get("rows_inserted") or 0)

    if related_stats.get("skipped"):
        logger.warning(
            "[SYNC RELATED] omitido: %s",
            related_stats.get("skip_reason"),
        )
        msg = (
            "Órdenes sincronizadas correctamente. Relaciones no ejecutadas (sin token u otro motivo)."
        )
    elif related_stats.get("omitido_concurrencia"):
        logger.warning("[SYNC RELATED] omitido por lock de concurrencia")
        msg = (
            "Órdenes sincronizadas correctamente. Relaciones omitidas porque otro proceso "
            "tiene el lock de document_related."
        )
    else:
        logger.info("[SYNC RELATED] relaciones insertadas: %s", related_processed)
        msg = "Órdenes y relaciones sincronizadas correctamente"

    return {
        "ok": True,
        "orders_processed": orders_processed,
        "related_processed": related_processed,
        "message": msg,
    }


@router.post("/sync-sales")
def post_distribuidora_sync_sales(background_tasks: BackgroundTasks):
    """
    Encola sync incremental de ventas (1/6/9) + relaciones; **no bloquea** el HTTP (202).
    """
    if not bsale_token_distribuidora_configured():
        raise HTTPException(status_code=503, detail="sin_token")
    background_tasks.add_task(_run_distribuidora_sync_sales_task)
    return JSONResponse({"ok": True, "status": "queued"}, status_code=202)


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
    only_not_invoiced: bool = Query(
        False,
        description="Si true: solo OC sin factura/boleta en document_related (vista enriquecida).",
    ),
    invoice_status: str | None = Query(
        None,
        description="Filtro rápido: confirmed | probable | pending (estado unificado).",
    ),
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
        invoice_status=invoice_status,
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
        description="Si true: solo OC sin factura/boleta enlazada en document_related (vía detalles). Si false: todas.",
    ),
    day_filter: str | None = Query(
        None,
        description="Opcional: lunes|martes|miercoles|jueves|viernes|sabado (coincidencia en observaciones).",
    ),
    limit: int = Query(
        250,
        ge=1,
        le=300,
        description="Máximo de comunas (grupos) en la respuesta; reduce carga.",
    ),
):
    rows = list_dispatch_prep_by_municipality(
        emission_date_from=emission_date_from,
        emission_date_to=emission_date_to,
        only_not_invoiced=only_not_invoiced,
        day_filter=day_filter,
        limit=limit,
    )
    return {"items": rows}


@router.get("/orders/dispatch-prep/observaciones")
def get_dispatch_prep_observaciones(
    emission_date_from: date = Query(...),
    emission_date_to: date = Query(...),
    only_not_invoiced: bool = Query(
        True,
        description="Si true: solo textos de OC sin factura/boleta en document_related.",
    ),
    limit: int = Query(
        300,
        ge=1,
        le=300,
        description="Máximo de textos devueltos (capado a 300 para aligerar el endpoint).",
    ),
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
    only_not_invoiced: bool = Query(
        True,
        description="Si true: solo filas OC sin factura/boleta en document_related.",
    ),
    day_filter: str | None = Query(None),
    limit: int = Query(
        400,
        ge=1,
        le=1500,
        description="Tamaño de página (máx. 1500).",
    ),
    offset: int = Query(
        0,
        ge=0,
        le=500_000,
        description="Desplazamiento para paginación.",
    ),
):
    return list_dispatch_prep_planning_rows(
        emission_date_from=emission_date_from,
        emission_date_to=emission_date_to,
        only_not_invoiced=only_not_invoiced,
        day_filter=day_filter,
        limit=limit,
        offset=offset,
    )


@router.post("/resync-oc")
def post_resync_oc(
    background_tasks: BackgroundTasks,
    body: ResyncOcStartBody | None = Body(default=None),
):
    """
    Encola un resync Bsale → ``distribuidora.*`` en background (no bloquea el HTTP).

    Opcional JSON: ``emission_date_from``, ``emission_date_to`` (``YYYY-MM-DD``, UTC día
    calendario). Si omiten, se usa una ventana corta de días recientes.

    El cliente debe hacer polling a ``GET /distribuidora/resync-oc/status/{job_id}``.
    """
    if not bsale_token_distribuidora_configured():
        logger.error("resync-oc: sin BSALE_TOKEN / BSALE_TOKEN_SPA")
        return {"ok": False, "error": "sin_token"}
    emission_from, emission_to, df, dt = _resync_oc_emission_bounds(body)
    job = create_job(emission_date_from=df, emission_date_to=dt)
    jid = str(job["job_id"])
    background_tasks.add_task(
        run_resync_oc_job,
        jid,
        emission_from,
        emission_to,
        df,
        dt,
    )
    return {
        "ok": True,
        "job_id": jid,
        "status": "started",
        "emission_date_from": df,
        "emission_date_to": dt,
    }


@router.get("/resync-oc/status/{job_id}")
def get_resync_oc_status(job_id: str):
    rec = get_job(job_id)
    if not rec:
        return {"ok": False, "error": "job_not_found"}
    return {
        "ok": True,
        "job_id": rec["job_id"],
        "status": rec["status"],
        "processed_count": rec["processed_count"],
        "updated_count": rec["updated_count"],
        "error_count": rec["error_count"],
        "message": rec["message"],
        "emission_date_from": rec["emission_date_from"],
        "emission_date_to": rec["emission_date_to"],
        "started_at": rec["started_at"],
        "finished_at": rec["finished_at"],
    }
