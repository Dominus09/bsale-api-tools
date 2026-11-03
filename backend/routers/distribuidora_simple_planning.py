"""API: preview de listado OC para planificación simplificada (solo lectura)."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from backend.auth.multiapp import assert_company, assert_office, get_scope
from backend.db import get_connection
from backend.services.distribuidora.oc_source_resolver import (
    COMPANY_ID,
    OC_DOCUMENT_TYPE_ID,
    OFFICE_ID,
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

# Alcance operativo actual de Distribuidora (ampliable).
ALLOWED_COMPANY_IDS = frozenset({1, 2, 3})
ALLOWED_OFFICE_IDS = frozenset({1, 2, 3, 4, 5, 6, 7, 8, 9})


def require_simple_planning_access(
    authorization: BearerDep = None,
) -> dict[str, Any]:
    """JWT + ERP. Misma base que planificación vigente; sin panel admin."""
    return require_staff_user(authorization)


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
    """No confiar solo en company_id del body: allowlist + multiapp + catálogo."""
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


@router.post("/preview")
def post_simple_planning_preview(
    body: PreviewBody,
    user: dict = Depends(require_simple_planning_access),
):
    """Previsualiza un listado de OC (solo lectura; no confirma ni escribe)."""
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
