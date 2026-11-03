"""API: preview y asignación preliminar de OC (planificación simplificada, solo lectura)."""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from backend.auth.multiapp import assert_company, assert_office, get_scope
from backend.auth.permissions import has_simple_planning_access
from backend.db import get_connection
from backend.services.distribuidora.oc_source_resolver import (
    COMPANY_ID,
    OC_DOCUMENT_TYPE_ID,
    OFFICE_ID,
)
from backend.services.distribuidora.simple_planning_assignment_service import (
    preview_truck_assignments,
)
from backend.services.distribuidora.simple_planning_preview_service import (
    MAX_FOLIOS,
    SimplePlanningPreviewError,
    preview_ocs_from_folios,
)
from backend.utils.auth_staff import BearerDep, require_staff_user

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/distribuidora/simple-planning",
    tags=["distribuidora-simple-planning"],
)

ALLOWED_COMPANY_IDS = frozenset({1, 2, 3})
ALLOWED_OFFICE_IDS = frozenset({1, 2, 3, 4, 5, 6, 7, 8, 9})


def require_simple_planning_access(
    authorization: BearerDep = None,
) -> dict[str, Any]:
    """JWT + ERP + rol/perfil de planificación (sin admin global obligatorio)."""
    user = require_staff_user(authorization)
    if not has_simple_planning_access(user):
        raise HTTPException(
            status_code=403,
            detail="Acceso restringido a gerencia u operativo de planificación",
        )
    return user


def _company_office_exist_in_catalog(company_id: int, office_id: int) -> None:
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            "SELECT 1 FROM bsale.companies WHERE company_id = %s LIMIT 1",
            (int(company_id),),
        )
        if not cur.fetchone():
            raise HTTPException(status_code=400, detail="Empresa inexistente en catálogo")
        cur.execute(
            """
            SELECT 1 FROM bsale.offices
            WHERE company_id = %s AND bsale_id = %s
            LIMIT 1
            """,
            (int(company_id), int(office_id)),
        )
        if not cur.fetchone():
            raise HTTPException(
                status_code=400,
                detail="Oficina inexistente o no pertenece a la empresa indicada",
            )
        cur.close()
    except HTTPException:
        raise
    except Exception:
        logger.exception(
            "simple_planning_catalog_scope_failed company=%s office=%s",
            company_id,
            office_id,
        )
        raise HTTPException(
            status_code=503,
            detail="No se pudo validar empresa/oficina en catálogo",
        ) from None
    finally:
        if conn is not None:
            conn.close()


def _validate_scope(user: dict[str, Any], company_id: int, office_id: int) -> None:
    if int(company_id) not in ALLOWED_COMPANY_IDS:
        raise HTTPException(status_code=400, detail="company_id no autorizado")
    if int(office_id) not in ALLOWED_OFFICE_IDS:
        raise HTTPException(status_code=400, detail="office_id no autorizado")
    scope = get_scope(user)
    assert_company(scope, int(company_id))
    assert_office(
        scope,
        company_id=int(company_id),
        office_id=int(office_id),
        require_assignment=False,
    )
    _company_office_exist_in_catalog(int(company_id), int(office_id))


class PreviewBody(BaseModel):
    company_id: int = Field(default=COMPANY_ID, ge=1)
    office_id: int = Field(default=OFFICE_ID, ge=1)
    folios: list[Any] = Field(..., min_length=1, max_length=MAX_FOLIOS * 2)


class AssignmentRow(BaseModel):
    folio: Any
    truck_id: int = Field(..., ge=1)
    driver_name: str | None = Field(None, max_length=128)
    expected_fingerprint: str | None = Field(None, max_length=64)


class AssignPreviewBody(BaseModel):
    company_id: int = Field(default=COMPANY_ID, ge=1)
    office_id: int = Field(default=OFFICE_ID, ge=1)
    planning_date: date | None = None
    dispatch_name: str | None = Field(None, max_length=128)
    assignments: list[AssignmentRow] = Field(..., min_length=1, max_length=MAX_FOLIOS)


@router.post("/preview")
def post_simple_planning_preview(
    body: PreviewBody,
    user: dict = Depends(require_simple_planning_access),
):
    """Previsualiza un listado de OC (solo lectura)."""
    _validate_scope(user, body.company_id, body.office_id)
    try:
        report = preview_ocs_from_folios(
            company_id=int(body.company_id),
            office_id=int(body.office_id),
            folios=list(body.folios),
            document_type_id=OC_DOCUMENT_TYPE_ID,
        )
    except SimplePlanningPreviewError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception:
        logger.exception("simple_planning_preview_failed")
        raise HTTPException(
            status_code=503,
            detail="No se pudo previsualizar el listado de OC",
        ) from None
    if report.get("wrote"):
        raise HTTPException(status_code=500, detail="preview no debe escribir")
    return report


@router.post("/assign-preview")
def post_simple_planning_assign_preview(
    body: AssignPreviewBody,
    user: dict = Depends(require_simple_planning_access),
):
    """Asignación preliminar OC→camión(es). Solo lectura; no confirma."""
    _validate_scope(user, body.company_id, body.office_id)
    try:
        report = preview_truck_assignments(
            company_id=int(body.company_id),
            office_id=int(body.office_id),
            planning_date=body.planning_date,
            dispatch_name=body.dispatch_name,
            assignments=[a.model_dump() for a in body.assignments],
        )
    except SimplePlanningPreviewError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception:
        logger.exception("simple_planning_assign_preview_failed")
        raise HTTPException(
            status_code=503,
            detail="No se pudo previsualizar la asignación a camiones",
        ) from None
    if report.get("wrote"):
        raise HTTPException(status_code=500, detail="assign-preview no debe escribir")
    return report
