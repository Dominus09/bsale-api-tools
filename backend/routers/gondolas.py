"""Control de Góndolas — recorridos de exhibición y reposición.

Lee BSALE RAW (catálogo, stock por oficina y recepciones por oficina) y escribe solo
en las tablas propias `app.gondola_*`. Nunca modifica stock, documentos ni precios
de Bsale. Las tablas se crean con `backend/sql/gondolas/001_gondola_control.sql`,
migración que requiere autorización para ejecutarse en producción.
"""

from __future__ import annotations

import logging
from typing import Any, Callable

import psycopg2
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from backend.services.analytics_v2.access import AnalyticsV2AccessError
from backend.services.gondolas.access import require_gondola_cancel, require_gondola_operate
from backend.services.gondolas.service import (
    GondolasForbidden,
    GondolasNotFound,
    GondolasNotReady,
    GondolasService,
    GondolasValidationError,
    run_gondola_read,
    run_gondola_write,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/gondolas", tags=["Control de Góndolas"])

CompanyQuery = Query(..., ge=1, description="Empresa; se verifica contra el usuario en backend")


def _guard(endpoint: str, company_id: int, call: Callable[[], Any]) -> Any:
    try:
        return call()
    except AnalyticsV2AccessError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc
    except GondolasValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except GondolasNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except GondolasForbidden as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except GondolasNotReady as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except psycopg2.IntegrityError as exc:
        logger.warning("gondolas_integrity endpoint=%s company_id=%s", endpoint, company_id)
        raise HTTPException(
            status_code=409,
            detail="Ya existe un recorrido en curso para esa empresa, sucursal y tipo de producto",
        ) from exc
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("gondolas_endpoint_error endpoint=%s company_id=%s exc_type=%s",
                         endpoint, company_id, type(exc).__name__)
        raise HTTPException(status_code=500, detail="Error interno de Control de Góndolas") from exc


class CreateSessionBody(BaseModel):
    office_id: int = Field(ge=1)
    product_type_id: int = Field(ge=1)


class ScanBody(BaseModel):
    bar_code: str = Field(min_length=1, max_length=120)
    device: str | None = Field(default=None, max_length=40)
    variant_id: int | None = Field(default=None, ge=1)


class CancelBody(BaseModel):
    reason: str | None = Field(default=None, max_length=300)


class ReplenishmentBody(BaseModel):
    status: str = Field(min_length=1, max_length=40)
    note: str | None = Field(default=None, max_length=500)


@router.get("/options")
def gondola_options(company_id: int = CompanyQuery, user: dict = Depends(require_gondola_operate)):
    return _guard("options", company_id, lambda: run_gondola_read(user, company_id, lambda s: s.options()))


@router.get("/companies")
def gondola_companies(user: dict = Depends(require_gondola_operate)):
    """Empresas con BSALE RAW disponible y dentro del alcance del usuario."""
    from backend.services.gondolas.service import run_gondola_companies

    return _guard("companies", 0, lambda: run_gondola_companies(user))


@router.get("/catalog")
def gondola_catalog(
    company_id: int = CompanyQuery,
    office_id: int = Query(..., ge=1),
    product_type_id: int = Query(..., ge=1),
    q: str = Query(""),
    stock_filter: str = Query("todos"),
    exhibition_filter: str = Query(""),
    session_id: int | None = Query(None, ge=1),
    sort: str = Query("producto"),
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=500),
    user: dict = Depends(require_gondola_operate),
):
    return _guard("catalog", company_id, lambda: run_gondola_read(
        user, company_id,
        lambda s: s.catalog(
            office_id, product_type_id, q=q, stock_filter=stock_filter,
            exhibition_filter=exhibition_filter, session_id=session_id,
            sort=sort, page=page, page_size=page_size,
        ),
    ))


@router.get("/sessions")
def gondola_sessions(
    company_id: int = CompanyQuery,
    limit: int = Query(30, ge=1, le=100),
    user: dict = Depends(require_gondola_operate),
):
    return _guard("sessions", company_id, lambda: run_gondola_read(
        user, company_id, lambda s: s.list_sessions(limit)
    ))


@router.post("/sessions")
def gondola_create_session(
    body: CreateSessionBody,
    company_id: int = CompanyQuery,
    user: dict = Depends(require_gondola_operate),
):
    return _guard("create_session", company_id, lambda: run_gondola_write(
        user, company_id, lambda s: s.create_session(body.office_id, body.product_type_id)
    ))


@router.get("/sessions/{session_id}")
def gondola_get_session(
    session_id: int,
    company_id: int = CompanyQuery,
    user: dict = Depends(require_gondola_operate),
):
    return _guard("get_session", company_id, lambda: run_gondola_read(
        user, company_id, lambda s: s.get_session(session_id)
    ))


@router.post("/sessions/{session_id}/scans")
def gondola_register_scan(
    session_id: int,
    body: ScanBody,
    company_id: int = CompanyQuery,
    user: dict = Depends(require_gondola_operate),
):
    return _guard("register_scan", company_id, lambda: run_gondola_write(
        user, company_id,
        lambda s: s.register_scan(session_id, body.bar_code, body.device, body.variant_id),
    ))


@router.post("/sessions/{session_id}/close")
def gondola_close_session(
    session_id: int,
    company_id: int = CompanyQuery,
    user: dict = Depends(require_gondola_operate),
):
    return _guard("close_session", company_id, lambda: run_gondola_write(
        user, company_id, lambda s: s.close_session(session_id)
    ))


@router.post("/sessions/{session_id}/cancel")
def gondola_cancel_session(
    session_id: int,
    body: CancelBody,
    company_id: int = CompanyQuery,
    user: dict = Depends(require_gondola_cancel),
):
    return _guard("cancel_session", company_id, lambda: run_gondola_write(
        user, company_id, lambda s: s.cancel_session(session_id, body.reason)
    ))


@router.get("/sessions/{session_id}/replenishment")
def gondola_replenishment(
    session_id: int,
    company_id: int = CompanyQuery,
    user: dict = Depends(require_gondola_operate),
):
    return _guard("replenishment", company_id, lambda: run_gondola_read(
        user, company_id, lambda s: {"items": s.list_replenishment(session_id)}
    ))


@router.patch("/sessions/{session_id}/replenishment/{variant_id}")
def gondola_update_replenishment(
    session_id: int,
    variant_id: int,
    body: ReplenishmentBody,
    company_id: int = CompanyQuery,
    user: dict = Depends(require_gondola_operate),
):
    return _guard("update_replenishment", company_id, lambda: run_gondola_write(
        user, company_id,
        lambda s: s.update_replenishment(session_id, variant_id, body.status, body.note),
    ))
