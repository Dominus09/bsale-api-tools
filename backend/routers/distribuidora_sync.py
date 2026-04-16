"""Endpoints ERP: sync manual, resync y estado (Distribuidora Bsale)."""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from fastapi import APIRouter, BackgroundTasks, Body, HTTPException
from pydantic import BaseModel, ConfigDict, Field, field_validator

from backend.services.distribuidora.orders_service import get_sync_status_payload
from backend.services.distribuidora.sync_related_service import (
    run_resync_related_range_background,
    run_sync_distribuidora_related_background,
)
from backend.services.distribuidora.sync_service import (
    bsale_token_distribuidora_configured,
    run_incremental_distribuidora_background,
    run_resync_distribuidora_background,
)

router = APIRouter(prefix="/erp", tags=["ERP Distribuidora"])
logger = logging.getLogger(__name__)


class ResyncRequest(BaseModel):
    """Cuerpo del resync: solo fechas de emisión (UTC). Opcional: sin cuerpo se usa rango por defecto en el job."""

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {"start_date": "2026-04-14", "end_date": "2026-04-16"},
                {},
            ]
        }
    )

    start_date: str | None = Field(
        default=None,
        description="Inicio inclusive (YYYY-MM-DD o ISO8601, UTC si no hay zona)",
        examples=["2026-04-14"],
    )
    end_date: str | None = Field(
        default=None,
        description="Fin inclusive",
        examples=["2026-04-16"],
    )

    @field_validator("start_date", "end_date", mode="before")
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


class ResyncRangeResponse(BaseModel):
    """Rango efectivo que recibirá el job (nombres alineados al dominio de emisión)."""

    emission_from: str | None = Field(
        default=None,
        description="Inicio normalizado o null si el job infiere el inicio",
    )
    emission_to: str | None = Field(
        default=None,
        description="Fin normalizado o null si el job infiere el fin",
    )


class ResyncDistribuidoraResponse(BaseModel):
    status: str
    range: ResyncRangeResponse


def _normalize_resync_date_param(field_label: str, raw: str | None) -> str | None:
    """Valida y normaliza a string ISO o ``YYYY-MM-DD`` para el job en background."""
    if raw is None:
        return None
    t = raw.strip()
    if not t:
        return None
    try:
        if len(t) == 10 and t[4] == "-" and t[7] == "-":
            datetime.strptime(t, "%Y-%m-%d")
            return t
        dt = datetime.fromisoformat(t.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat()
    except ValueError as e:
        raise HTTPException(
            status_code=400,
            detail=f"{field_label} no válida: {raw!r}",
        ) from e


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


@router.post(
    "/resync-distribuidora",
    response_model=ResyncDistribuidoraResponse,
    responses={400: {"description": "Fecha inválida"}},
)
def post_resync_distribuidora(
    background_tasks: BackgroundTasks,
    request: ResyncRequest | None = Body(default=None),
):
    """
    Re-sync por rango de emisión: el job recorre **día a día** en segundo plano (ventanas pequeñas a Bsale).
    Sin cuerpo o campos vacíos: el job usa MIN(emission_date) en BD / ``emission_to`` = ahora (ver ``sync_service``).
    """
    if not bsale_token_distribuidora_configured():
        raise HTTPException(
            status_code=400,
            detail="Defina BSALE_TOKEN o BSALE_TOKEN_SPA para ejecutar el resync.",
        )
    req = request or ResyncRequest()
    emission_from = _normalize_resync_date_param("start_date", req.start_date)
    emission_to = _normalize_resync_date_param("end_date", req.end_date)
    if emission_from is not None or emission_to is not None:
        logger.info("Resync distribuidora desde %s hasta %s", emission_from, emission_to)
    background_tasks.add_task(
        run_resync_distribuidora_background,
        emission_from,
        emission_to,
    )
    return ResyncDistribuidoraResponse(
        status="resync iniciado",
        range=ResyncRangeResponse(emission_from=emission_from, emission_to=emission_to),
    )


@router.post(
    "/resync-related",
    response_model=ResyncDistribuidoraResponse,
    responses={400: {"description": "Fechas inválidas o faltantes"}},
)
def post_resync_related(
    background_tasks: BackgroundTasks,
    request: ResyncRequest,
):
    """
    Re-sync **histórico** de ``document_related`` (API ``relateddetailid``), día a día en UTC,
    para OC (tipo 33) emitidas entre ``start_date`` y ``end_date`` (obligatorios, inclusive).
    """
    if not bsale_token_distribuidora_configured():
        raise HTTPException(
            status_code=400,
            detail="Defina BSALE_TOKEN o BSALE_TOKEN_SPA para ejecutar el resync.",
        )
    if not request.start_date or not request.end_date:
        raise HTTPException(
            status_code=400,
            detail="start_date y end_date son obligatorios para el resync histórico de relaciones.",
        )
    start_n = _normalize_resync_date_param("start_date", request.start_date)
    end_n = _normalize_resync_date_param("end_date", request.end_date)
    if start_n is None or end_n is None:
        raise HTTPException(
            status_code=400,
            detail="start_date y end_date deben ser fechas válidas (YYYY-MM-DD o ISO8601).",
        )
    logger.info("Resync related distribuidora desde %s hasta %s", start_n, end_n)
    background_tasks.add_task(
        run_resync_related_range_background,
        start_n,
        end_n,
    )
    return ResyncDistribuidoraResponse(
        status="resync related iniciado",
        range=ResyncRangeResponse(emission_from=start_n, emission_to=end_n),
    )


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
