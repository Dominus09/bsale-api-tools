"""Endpoints ERP: sync manual, resync y estado (Distribuidora Bsale)."""

from __future__ import annotations

from datetime import date

from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel, Field

from backend.services.distribuidora.orders_service import get_sync_status_payload
from backend.services.distribuidora.sync_service import (
    bsale_token_distribuidora_configured,
    run_incremental_distribuidora_background,
    run_resync_distribuidora_background,
)

router = APIRouter(prefix="/erp", tags=["ERP Distribuidora"])


class ResyncDistribuidoraBody(BaseModel):
    """Rango opcional (UTC). Fechas YYYY-MM-DD o ISO8601."""

    emission_from: str | None = Field(default=None, description="Inicio inclusive (fecha emisión)")
    emission_to: str | None = Field(default=None, description="Fin inclusive")


@router.post("/sync-distribuidora")
def post_sync_distribuidora(background_tasks: BackgroundTasks):
    """
    Encola sync incremental Bsale → ``distribuidora.*`` (documentos, detalles, atributos OC,
    referencias boleta/factura/OC). Respuesta inmediata para botón en frontend.
    """
    if not bsale_token_distribuidora_configured():
        raise HTTPException(
            status_code=400,
            detail="Defina BSALE_TOKEN o BSALE_TOKEN_SPA para ejecutar el sync.",
        )
    background_tasks.add_task(run_incremental_distribuidora_background)
    return {"status": "incremental encolado"}


@router.post("/resync-distribuidora")
def post_resync_distribuidora(
    background_tasks: BackgroundTasks,
    body: ResyncDistribuidoraBody | None = None,
):
    """
    Re-sync por rango de emisión (por meses en el job). Sin cuerpo: desde MIN(emission_date) en BD.
    """
    if not bsale_token_distribuidora_configured():
        raise HTTPException(
            status_code=400,
            detail="Defina BSALE_TOKEN o BSALE_TOKEN_SPA para ejecutar el resync.",
        )
    b = body or ResyncDistribuidoraBody()
    background_tasks.add_task(
        run_resync_distribuidora_background,
        b.emission_from,
        b.emission_to,
    )
    return {"status": "resync encolado", "range": {"emission_from": b.emission_from, "emission_to": b.emission_to}}


@router.get("/sync-distribuidora/status")
def get_sync_distribuidora_status():
    """Último estado de sync, último log y si el lock de sync está activo (otro proceso en curso)."""
    try:
        return get_sync_status_payload()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
