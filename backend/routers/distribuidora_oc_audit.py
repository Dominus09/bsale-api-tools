"""API: auditoría de correlativos de OC (tipo 33) + comparación/recuperación manual."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from backend.auth.permissions import has_admin_access, has_management_access
from backend.services.distribuidora.bsale_client import BsaleClient
from backend.services.distribuidora.oc_audit_recovery_service import (
    RecoveryConflictError,
    RecoveryTransientError,
    RecoveryValidationError,
    confirm_oc_recovery,
    simulate_oc_recovery,
)
from backend.services.distribuidora.oc_correlative_audit_service import (
    DEFAULT_MAX_GAPS,
    DEFAULT_MAX_INVESTIGATE,
    DEFAULT_MAX_RANGE_SPAN,
    DEFAULT_SUGGEST_WINDOW,
    MAX_INVESTIGATE_BATCH,
    MAX_TAIL_PROBE,
    CorrelativeRangeError,
    audit_oc_correlatives,
    compare_bsale_pg_folio_tip,
    get_local_oc_folio_bounds,
    investigate_selected_folios,
    suggest_recent_folio_range,
)
from backend.services.distribuidora.oc_source_resolver import (
    COMPANY_ID,
    OC_DOCUMENT_TYPE_ID,
    OFFICE_ID,
)
from backend.services.distribuidora.sync_service import _bsale_token
from backend.utils.auth_staff import BearerDep, require_staff_user

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/distribuidora/oc-audit",
    tags=["distribuidora-oc-audit"],
)

ALLOWED_COMPANY_IDS = frozenset({1, 2, 3})
ALLOWED_OFFICE_IDS = frozenset({1, 2, 3, 4, 5, 6, 7, 8, 9})


def require_oc_audit_access(
    authorization: BearerDep = None,
) -> dict[str, Any]:
    """ERP + gerencia/adm/OWNER."""
    user = require_staff_user(authorization)
    if not (has_management_access(user) or has_admin_access(user)):
        raise HTTPException(
            status_code=403,
            detail="Acceso restringido a gerencia o administración",
        )
    return user


def _validate_scope(company_id: int, office_id: int) -> None:
    if int(company_id) not in ALLOWED_COMPANY_IDS:
        raise HTTPException(status_code=400, detail="company_id no autorizado")
    if int(office_id) not in ALLOWED_OFFICE_IDS:
        raise HTTPException(status_code=400, detail="office_id no autorizado")


def _bsale_client() -> BsaleClient:
    token = _bsale_token()
    if not token:
        raise HTTPException(status_code=503, detail="Token Bsale no configurado")
    # Tope de reintentos 429 para fallar seguro hacia el ERP (no colgar).
    return BsaleClient(token, max_429_retries=2)


def _user_ids(user: dict[str, Any]) -> tuple[int | None, str | None]:
    uid = user.get("id") or user.get("uid")
    try:
        user_id = int(uid) if uid is not None else None
    except (TypeError, ValueError):
        user_id = None
    email = str(user.get("email") or "").strip() or None
    return user_id, email


class LocalAuditBody(BaseModel):
    company_id: int = Field(default=COMPANY_ID, ge=1)
    office_id: int = Field(default=OFFICE_ID, ge=1)
    from_folio: int = Field(..., ge=1)
    to_folio: int = Field(..., ge=1)
    max_range_span: int = Field(default=DEFAULT_MAX_RANGE_SPAN, ge=1, le=DEFAULT_MAX_RANGE_SPAN)
    max_gaps: int = Field(default=DEFAULT_MAX_GAPS, ge=1, le=DEFAULT_MAX_GAPS)


class InvestigateBody(BaseModel):
    company_id: int = Field(default=COMPANY_ID, ge=1)
    office_id: int = Field(default=OFFICE_ID, ge=1)
    folios: list[int] = Field(..., min_length=1, max_length=MAX_INVESTIGATE_BATCH)
    max_folios: int = Field(default=MAX_INVESTIGATE_BATCH, ge=1, le=MAX_INVESTIGATE_BATCH)


class CompareBody(BaseModel):
    company_id: int = Field(default=COMPANY_ID, ge=1)
    office_id: int = Field(default=OFFICE_ID, ge=1)
    max_tail_probe: int = Field(default=MAX_TAIL_PROBE, ge=1, le=MAX_TAIL_PROBE)


class RecoverSimulateBody(BaseModel):
    company_id: int = Field(default=COMPANY_ID, ge=1)
    office_id: int = Field(default=OFFICE_ID, ge=1)
    folio: int = Field(..., ge=1)


class RecoverConfirmBody(BaseModel):
    company_id: int = Field(default=COMPANY_ID, ge=1)
    office_id: int = Field(default=OFFICE_ID, ge=1)
    folio: int = Field(..., ge=1)
    simulation_token: str = Field(..., min_length=16)


@router.get("/meta")
def get_oc_audit_meta(
    company_id: int = COMPANY_ID,
    office_id: int = OFFICE_ID,
    window: int = DEFAULT_SUGGEST_WINDOW,
    _user: dict = Depends(require_oc_audit_access),
):
    """Límites locales y rango reciente sugerido."""
    _validate_scope(company_id, office_id)
    try:
        bounds = get_local_oc_folio_bounds(
            company_id=int(company_id),
            office_id=int(office_id),
            document_type_id=OC_DOCUMENT_TYPE_ID,
        )
        suggested = suggest_recent_folio_range(
            company_id=int(company_id),
            office_id=int(office_id),
            window=int(window),
            document_type_id=OC_DOCUMENT_TYPE_ID,
        )
    except CorrelativeRangeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception:
        logger.exception("oc_audit_meta_failed")
        raise HTTPException(status_code=503, detail="No se pudo leer límites locales") from None
    return {
        "company_id": int(company_id),
        "office_id": int(office_id),
        "document_type_id": OC_DOCUMENT_TYPE_ID,
        "bounds": bounds,
        "suggested_range": suggested,
        "limits": {
            "max_range_span": DEFAULT_MAX_RANGE_SPAN,
            "max_gaps": DEFAULT_MAX_GAPS,
            "max_investigate_batch": MAX_INVESTIGATE_BATCH,
            "max_investigate": DEFAULT_MAX_INVESTIGATE,
            "max_tail_probe": MAX_TAIL_PROBE,
        },
        "writes_enabled": True,
        "recovery_enabled": True,
        "recovery_requires_simulation": True,
        "recovery_bulk_enabled": False,
        "compare_requires_manual_action": True,
    }


@router.post("/local")
def post_oc_audit_local(
    body: LocalAuditBody,
    _user: dict = Depends(require_oc_audit_access),
):
    """Auditoría local de correlativos: sin llamadas masivas a Bsale."""
    _validate_scope(body.company_id, body.office_id)
    try:
        report = audit_oc_correlatives(
            None,
            company_id=int(body.company_id),
            office_id=int(body.office_id),
            from_folio=int(body.from_folio),
            to_folio=int(body.to_folio),
            document_type_id=OC_DOCUMENT_TYPE_ID,
            investigate=False,
            max_range_span=int(body.max_range_span),
            max_gaps=int(body.max_gaps),
            include_sequence_probe=True,
        )
    except CorrelativeRangeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception:
        logger.exception("oc_audit_local_failed")
        raise HTTPException(status_code=503, detail="No se pudo ejecutar auditoría local") from None
    if report.get("wrote"):
        raise HTTPException(status_code=500, detail="auditoría local no debe escribir")
    return report


@router.post("/investigate")
def post_oc_audit_investigate(
    body: InvestigateBody,
    _user: dict = Depends(require_oc_audit_access),
):
    """Investiga en Bsale un lote explícito de folios (solo lectura)."""
    _validate_scope(body.company_id, body.office_id)
    if len(body.folios) > MAX_INVESTIGATE_BATCH:
        raise HTTPException(
            status_code=400,
            detail=f"Máximo {MAX_INVESTIGATE_BATCH} folios por solicitud",
        )
    client = _bsale_client()
    try:
        report = investigate_selected_folios(
            client,
            folios=list(body.folios),
            company_id=int(body.company_id),
            office_id=int(body.office_id),
            document_type_id=OC_DOCUMENT_TYPE_ID,
            max_folios=int(body.max_folios),
        )
    except CorrelativeRangeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("oc_audit_investigate_failed")
        raise HTTPException(
            status_code=502,
            detail=f"Error consultando Bsale: {type(exc).__name__}",
        ) from None
    if report.get("wrote"):
        raise HTTPException(status_code=500, detail="investigación no debe escribir")
    return report


@router.post("/compare")
def post_oc_audit_compare(
    body: CompareBody,
    _user: dict = Depends(require_oc_audit_access),
):
    """Compara máximo PG vs tip verificable en Bsale (acción manual, solo lectura)."""
    _validate_scope(body.company_id, body.office_id)
    client = _bsale_client()
    try:
        report = compare_bsale_pg_folio_tip(
            client,
            company_id=int(body.company_id),
            office_id=int(body.office_id),
            document_type_id=OC_DOCUMENT_TYPE_ID,
            max_tail_probe=int(body.max_tail_probe),
        )
    except CorrelativeRangeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("oc_audit_compare_failed")
        msg = str(exc)
        code = 429 if "429" in msg else 502
        raise HTTPException(
            status_code=code,
            detail=f"Error consultando Bsale: {type(exc).__name__}",
        ) from None
    if report.get("wrote"):
        raise HTTPException(status_code=500, detail="comparación no debe escribir")
    return report


@router.post("/recover/simulate")
def post_oc_audit_recover_simulate(
    body: RecoverSimulateBody,
    user: dict = Depends(require_oc_audit_access),
):
    """Simula recuperación de una OC (sin escrituras)."""
    _validate_scope(body.company_id, body.office_id)
    client = _bsale_client()
    user_id, email = _user_ids(user)
    try:
        return simulate_oc_recovery(
            client,
            folio=int(body.folio),
            company_id=int(body.company_id),
            office_id=int(body.office_id),
            document_type_id=OC_DOCUMENT_TYPE_ID,
            user_id=user_id,
            user_email=email,
        )
    except RecoveryValidationError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except RecoveryConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except RecoveryTransientError as exc:
        msg = str(exc)
        code = 429 if "429" in msg or "rate limit" in msg.lower() else 502
        raise HTTPException(status_code=code, detail=msg) from exc
    except CorrelativeRangeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception:
        logger.exception("oc_audit_recover_simulate_failed folio=%s", body.folio)
        raise HTTPException(
            status_code=502,
            detail="No se pudo simular la recuperación",
        ) from None


@router.post("/recover/confirm")
def post_oc_audit_recover_confirm(
    body: RecoverConfirmBody,
    user: dict = Depends(require_oc_audit_access),
):
    """Confirma recuperación de una OC tras simulación válida (una por solicitud)."""
    _validate_scope(body.company_id, body.office_id)
    client = _bsale_client()
    user_id, email = _user_ids(user)
    try:
        return confirm_oc_recovery(
            client,
            folio=int(body.folio),
            simulation_token=str(body.simulation_token),
            company_id=int(body.company_id),
            office_id=int(body.office_id),
            document_type_id=OC_DOCUMENT_TYPE_ID,
            user_id=user_id,
            user_email=email,
        )
    except RecoveryValidationError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except RecoveryConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except RecoveryTransientError as exc:
        msg = str(exc)
        code = 429 if "429" in msg or "rate limit" in msg.lower() else 502
        raise HTTPException(status_code=code, detail=msg) from exc
    except CorrelativeRangeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception:
        logger.exception("oc_audit_recover_confirm_failed folio=%s", body.folio)
        raise HTTPException(
            status_code=502,
            detail="No se pudo confirmar la recuperación",
        ) from None
