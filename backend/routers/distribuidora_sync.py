"""Endpoints ERP: sync manual, resync y estado (Distribuidora Bsale)."""

from __future__ import annotations

from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel, Field, field_validator

from backend.services.distribuidora.orders_service import get_sync_status_payload
from backend.services.distribuidora.sync_related_service import (
    run_sync_distribuidora_related_background,
)
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

    @field_validator("emission_from", "emission_to", mode="before")
    @classmethod
    def _reject_garbage_date_strings(cls, v: object) -> str | None:
        if v is None:
            return None
        if not isinstance(v, str):
            return str(v).strip() or None
        t = v.strip()
        if not t or t.lower() in ("string", "null", "undefined", "none", "nan", "-"):
            return None
        return t


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
    Re-sync por rango de emisión: el job recorre **día a día** en segundo plano (ventanas pequeñas a Bsale).
    Sin cuerpo: desde MIN(emission_date) en BD (o fallback días).
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
    return {
        "status": "resync iniciado",
        "range": {"emission_from": b.emission_from, "emission_to": b.emission_to},
    }


@router.post("/sync-distribuidora-related")
def post_sync_distribuidora_related(background_tasks: BackgroundTasks):
    """
    Encola sync de ``document_related`` (API ``relateddetailid``), acotado en días y cantidad de líneas.
    Lock distinto al sync incremental de documentos.
    """
    if not bsale_token_distribuidora_configured():
        raise HTTPException(
            status_code=400,
            detail="Defina BSALE_TOKEN o BSALE_TOKEN_SPA para ejecutar el sync.",
        )
    background_tasks.add_task(run_sync_distribuidora_related_background)
    return {"status": "related encolado"}


@router.get("/sync-distribuidora/status")
def get_sync_distribuidora_status():
    """Último estado de sync, último log y si el lock de sync está activo (otro proceso en curso)."""
    try:
        return get_sync_status_payload()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
