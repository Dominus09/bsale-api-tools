"""API read-only: auditoría de correlativos de OC (tipo 33)."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from backend.auth.permissions import has_admin_access, has_management_access
from backend.services.distribuidora.bsale_client import BsaleClient
from backend.services.distribuidora.oc_correlative_audit_service import (
    DEFAULT_MAX_GAPS,
    DEFAULT_MAX_INVESTIGATE,
    DEFAULT_MAX_RANGE_SPAN,
    DEFAULT_SUGGEST_WINDOW,
    MAX_INVESTIGATE_BATCH,
    CorrelativeRangeError,
    audit_oc_correlatives,
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
    """ERP + gerencia/adm/OWNER. Sin mutaciones en esta fase."""
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


@router.get("/meta")
def get_oc_audit_meta(
    company_id: int = COMPANY_ID,
    office_id: int = OFFICE_ID,
    window: int = DEFAULT_SUGGEST_WINDOW,
    _user: dict = Depends(require_oc_audit_access),
):
    """Límites locales y rango reciente sugerido (solo lectura)."""
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
        },
        "writes_enabled": False,
        "recovery_enabled": False,
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
    token = _bsale_token()
    if not token:
        raise HTTPException(status_code=503, detail="Token Bsale no configurado")
    client = BsaleClient(token)
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
        # No clasificar como inexistente: error de verificación.
        raise HTTPException(
            status_code=502,
            detail=f"Error consultando Bsale: {type(exc).__name__}",
        ) from None
    if report.get("wrote"):
        raise HTTPException(status_code=500, detail="investigación no debe escribir")
    return report
