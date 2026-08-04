"""API Analítica → Costos."""

from __future__ import annotations

from datetime import date, datetime

from fastapi import APIRouter, Depends, HTTPException, Query

from backend.schemas.cost_v2_read import (
    DEFAULT_LIMIT,
    MAX_LIMIT,
    CostV2ReadValidationError,
)
from backend.services import cost_analytics_service as svc
from backend.services import cost_v2_company_read_service as v2company
from backend.services import cost_v2_read_service as v2svc
from backend.services.sync_cost_receptions import sync_cost_receptions
from backend.utils.auth_staff import require_staff_user

router = APIRouter(prefix="/cost-analytics", tags=["cost-analytics"])


def _v2_http_error(exc: Exception) -> HTTPException:
    if isinstance(exc, CostV2ReadValidationError):
        return HTTPException(status_code=422, detail=str(exc))
    if isinstance(exc, LookupError):
        return HTTPException(status_code=404, detail=str(exc))
    return HTTPException(status_code=500, detail="Error interno al consultar Costos")


def _company_http_error(
    exc: Exception,
    *,
    endpoint: str,
    company_id: int | None = None,
    date_from: date | None = None,
    date_to: date | None = None,
    sort: str | None = None,
) -> HTTPException:
    if isinstance(exc, (CostV2ReadValidationError, LookupError)):
        return _v2_http_error(exc)
    from backend.services.cost_v2_company_read_service import log_company_endpoint_error

    log_company_endpoint_error(
        endpoint=endpoint,
        company_id=company_id,
        date_from=date_from,
        date_to=date_to,
        sort=sort,
        exc=exc,
    )
    # No exponer SQL ni TypeError al cliente
    return HTTPException(status_code=500, detail="Error interno al consultar Costos")

@router.get("/v2/receptions")
def list_v2_receptions(
    company_id: int = Query(..., ge=1),
    office_id: int = Query(..., ge=1),
    date_from: date = Query(...),
    date_to: date = Query(...),
    status: list[str] | None = Query(None),
    warning: list[str] | None = Query(None),
    barcode: str | None = Query(None, max_length=64),
    variant_id: int | None = Query(None, ge=1),
    document_number: int | None = Query(None),
    history_id: int | None = Query(None, ge=1),
    search: str | None = Query(None, max_length=120),
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
    cursor: str | None = Query(None, max_length=512),
    _user: dict = Depends(require_staff_user),
):
    """Listado read-only Costos V2 (paralelo; no reemplaza /receptions legacy)."""
    try:
        return v2svc.list_v2_receptions(
            company_id=company_id,
            office_id=office_id,
            date_from=date_from,
            date_to=date_to,
            limit=limit,
            cursor=cursor,
            status=status,
            warning=warning,
            barcode=barcode,
            variant_id=variant_id,
            document_number=document_number,
            history_id=history_id,
            search=search,
        )
    except (CostV2ReadValidationError, LookupError) as exc:
        raise _v2_http_error(exc) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/v2/receptions/{history_id}")
def get_v2_reception(
    history_id: int,
    company_id: int = Query(..., ge=1),
    office_id: int = Query(..., ge=1),
    _user: dict = Depends(require_staff_user),
):
    """Detalle read-only Costos V2 por history_id."""
    try:
        return v2svc.get_v2_reception(
            company_id=company_id,
            office_id=office_id,
            history_id=history_id,
        )
    except (CostV2ReadValidationError, LookupError) as exc:
        raise _v2_http_error(exc) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/v2/summary")
def v2_summary(
    company_id: int = Query(..., ge=1),
    office_id: int = Query(..., ge=1),
    date_from: date = Query(...),
    date_to: date = Query(...),
    status: list[str] | None = Query(None),
    warning: list[str] | None = Query(None),
    barcode: str | None = Query(None, max_length=64),
    variant_id: int | None = Query(None, ge=1),
    document_number: int | None = Query(None),
    history_id: int | None = Query(None, ge=1),
    search: str | None = Query(None, max_length=120),
    _user: dict = Depends(require_staff_user),
):
    """Resumen agregable sin sumar costos unitarios ni impacto monetario."""
    try:
        return v2svc.summarize_v2(
            company_id=company_id,
            office_id=office_id,
            date_from=date_from,
            date_to=date_to,
            status=status,
            warning=warning,
            barcode=barcode,
            variant_id=variant_id,
            document_number=document_number,
            history_id=history_id,
            search=search,
        )
    except (CostV2ReadValidationError, LookupError) as exc:
        raise _v2_http_error(exc) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/v2/products")
def list_v2_products(
    company_id: int = Query(..., ge=1),
    office_id: int = Query(..., ge=1),
    date_from: date = Query(...),
    date_to: date = Query(...),
    status: list[str] | None = Query(None),
    warning: list[str] | None = Query(None),
    barcode: str | None = Query(None, max_length=64),
    search: str | None = Query(None, max_length=120),
    sort: str | None = Query("latest_reception"),
    only_with_changes: bool = Query(False),
    only_needs_review: bool = Query(False),
    min_abs_change_percent: float | None = Query(None, ge=0),
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
    cursor: str | None = Query(None, max_length=1024),
    _user: dict = Depends(require_staff_user),
):
    """Listado por producto/variante (último + penúltimo costo). Read-only."""
    try:
        return v2svc.list_v2_products(
            company_id=company_id,
            office_id=office_id,
            date_from=date_from,
            date_to=date_to,
            limit=limit,
            cursor=cursor,
            sort=sort,
            status=status,
            warning=warning,
            barcode=barcode,
            search=search,
            only_with_changes=only_with_changes,
            only_needs_review=only_needs_review,
            min_abs_change_percent=min_abs_change_percent,
        )
    except (CostV2ReadValidationError, LookupError, ValueError) as exc:
        raise _v2_http_error(exc) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/v2/products-summary")
def v2_products_summary(
    company_id: int = Query(..., ge=1),
    office_id: int = Query(..., ge=1),
    date_from: date = Query(...),
    date_to: date = Query(...),
    change_threshold_percent: float | None = Query(10, ge=0),
    status: list[str] | None = Query(None),
    warning: list[str] | None = Query(None),
    barcode: str | None = Query(None, max_length=64),
    search: str | None = Query(None, max_length=120),
    _user: dict = Depends(require_staff_user),
):
    """Resumen orientado a decisiones por producto. Sin sumas de costo unitario."""
    try:
        return v2svc.summarize_v2_products(
            company_id=company_id,
            office_id=office_id,
            date_from=date_from,
            date_to=date_to,
            change_threshold_percent=change_threshold_percent,
            status=status,
            warning=warning,
            barcode=barcode,
            search=search,
        )
    except (CostV2ReadValidationError, LookupError) as exc:
        raise _v2_http_error(exc) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/v2/products/{variant_id}")
def get_v2_product(
    variant_id: int,
    company_id: int = Query(..., ge=1),
    office_id: int = Query(..., ge=1),
    date_from: date = Query(...),
    date_to: date = Query(...),
    history_limit: int = Query(20, ge=1, le=MAX_LIMIT),
    _user: dict = Depends(require_staff_user),
):
    """Detalle de producto con historial de recepciones V2."""
    try:
        return v2svc.get_v2_product(
            company_id=company_id,
            office_id=office_id,
            variant_id=variant_id,
            date_from=date_from,
            date_to=date_to,
            history_limit=history_limit,
        )
    except (CostV2ReadValidationError, LookupError) as exc:
        raise _v2_http_error(exc) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# --- E.7.3 consolidado por empresa (paralelo; no reemplaza /v2/products*) ---


@router.get("/v2/company-products")
def list_v2_company_products(
    company_id: int = Query(..., ge=1),
    date_from: date = Query(...),
    date_to: date = Query(...),
    search: str | None = Query(None, max_length=120),
    barcode: str | None = Query(None, max_length=64),
    warning: str | None = Query(None, max_length=64),
    movement: str | None = Query(None, max_length=16),
    situation: str | None = Query(None, max_length=32),
    sort: str | None = Query(None, max_length=32),
    only_relevant_changes: bool = Query(False),
    min_abs_change_percent: float | None = Query(None),
    change_threshold_percent: float | None = Query(None),
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
    cursor: str | None = Query(None, max_length=512),
    _user: dict = Depends(require_staff_user),
):
    """Listado consolidado: una fila por variant_id a nivel empresa."""
    try:
        return v2company.list_company_products(
            company_id=company_id,
            date_from=date_from,
            date_to=date_to,
            limit=limit,
            cursor=cursor,
            sort=sort,
            search=search,
            barcode=barcode,
            warning=warning,
            movement=movement,
            situation=situation,
            only_relevant_changes=only_relevant_changes,
            min_abs_change_percent=min_abs_change_percent,
            change_threshold_percent=change_threshold_percent,
        )
    except Exception as exc:
        raise _company_http_error(
            exc,
            endpoint="company-products",
            company_id=company_id,
            date_from=date_from,
            date_to=date_to,
            sort=sort,
        ) from exc


@router.get("/v2/company-summary")
def get_v2_company_summary(
    company_id: int = Query(..., ge=1),
    date_from: date = Query(...),
    date_to: date = Query(...),
    change_threshold_percent: float | None = Query(None),
    _user: dict = Depends(require_staff_user),
):
    """KPIs de decisión a nivel empresa."""
    try:
        return v2company.summarize_company_products(
            company_id=company_id,
            date_from=date_from,
            date_to=date_to,
            change_threshold_percent=change_threshold_percent,
        )
    except Exception as exc:
        raise _company_http_error(
            exc,
            endpoint="company-summary",
            company_id=company_id,
            date_from=date_from,
            date_to=date_to,
        ) from exc


@router.get("/v2/company-products/{variant_id}")
def get_v2_company_product(
    variant_id: int,
    company_id: int = Query(..., ge=1),
    date_from: date = Query(...),
    date_to: date = Query(...),
    _user: dict = Depends(require_staff_user),
):
    """Detalle consolidado + desglose por oficinas."""
    try:
        return v2company.get_company_product(
            company_id=company_id,
            variant_id=variant_id,
            date_from=date_from,
            date_to=date_to,
        )
    except Exception as exc:
        raise _company_http_error(
            exc,
            endpoint="company-products-detail",
            company_id=company_id,
            date_from=date_from,
            date_to=date_to,
        ) from exc


@router.get("/v2/company-products/{variant_id}/history")
def get_v2_company_product_history(
    variant_id: int,
    company_id: int = Query(..., ge=1),
    date_from: date = Query(...),
    date_to: date = Query(...),
    office_id: int | None = Query(None, ge=1),
    limit: int = Query(200, ge=1, le=500),
    _user: dict = Depends(require_staff_user),
):
    """Historial cronológico consolidado (filtro opcional por oficina)."""
    try:
        return v2company.list_company_product_history(
            company_id=company_id,
            variant_id=variant_id,
            date_from=date_from,
            date_to=date_to,
            office_id=office_id,
            limit=limit,
        )
    except Exception as exc:
        raise _company_http_error(
            exc,
            endpoint="company-products-history",
            company_id=company_id,
            date_from=date_from,
            date_to=date_to,
        ) from exc

@router.get("/dashboard")
def cost_dashboard(
    company_id: int = Query(..., ge=1),
    office_id: int | None = Query(None, ge=1),
    date_from: datetime | None = Query(None),
    date_to: datetime | None = Query(None),
    _user: dict = Depends(require_staff_user),
):
    try:
        return svc.get_dashboard(
            company_id,
            office_id=office_id,
            date_from=date_from,
            date_to=date_to,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/offices")
def list_offices(
    company_id: int = Query(..., ge=1),
    _user: dict = Depends(require_staff_user),
):
    return svc.list_offices(company_id)


@router.get("/history/search")
def search_history(
    company_id: int = Query(..., ge=1),
    q: str = Query(..., min_length=2, max_length=120),
    limit: int = Query(50, ge=1, le=200),
    _user: dict = Depends(require_staff_user),
):
    try:
        return svc.search_cost_history(company_id, q, limit=limit)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/history")
def list_history(
    company_id: int = Query(..., ge=1),
    q: str | None = Query(None, max_length=120),
    variant_id: int | None = Query(None, ge=1),
    office_id: int | None = Query(None, ge=1),
    date_from: datetime | None = Query(None),
    date_to: datetime | None = Query(None),
    limit: int = Query(200, ge=1, le=500),
    offset: int = Query(0, ge=0),
    _user: dict = Depends(require_staff_user),
):
    return svc.list_history(
        company_id,
        q=q,
        variant_id=variant_id,
        office_id=office_id,
        date_from=date_from,
        date_to=date_to,
        limit=limit,
        offset=offset,
    )


@router.get("/history/variants/{variant_id}")
def variant_history(
    variant_id: int,
    company_id: int = Query(..., ge=1),
    limit: int = Query(200, ge=1, le=500),
    _user: dict = Depends(require_staff_user),
):
    return svc.get_variant_cost_history(company_id, variant_id, limit=limit)


@router.get("/receptions")
def list_receptions(
    company_id: int = Query(..., ge=1),
    date_from: datetime | None = Query(None),
    date_to: datetime | None = Query(None),
    office_id: int | None = Query(None, ge=1),
    document_type: str | None = Query(None, max_length=64),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    _user: dict = Depends(require_staff_user),
):
    return svc.list_receptions(
        company_id,
        date_from=date_from,
        date_to=date_to,
        office_id=office_id,
        document_type=document_type,
        limit=limit,
        offset=offset,
    )


@router.get("/receptions/{reception_id}")
def get_reception(
    reception_id: int,
    company_id: int = Query(..., ge=1),
    _user: dict = Depends(require_staff_user),
):
    try:
        return svc.get_reception(company_id, reception_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/alerts")
def cost_alerts(
    company_id: int = Query(..., ge=1),
    office_id: int | None = Query(None, ge=1),
    limit: int = Query(100, ge=1, le=500),
    _user: dict = Depends(require_staff_user),
):
    return svc.list_alerts(company_id, office_id=office_id, limit=limit)


@router.get("/products")
def list_products(
    company_id: int = Query(..., ge=1),
    q: str | None = Query(None, max_length=120),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    _user: dict = Depends(require_staff_user),
):
    return svc.list_products(company_id, q=q, limit=limit, offset=offset)


@router.get("/opportunities")
def list_opportunities(
    company_id: int = Query(..., ge=1),
    status: str | None = Query(
        None, pattern="^(oportunidad_compra|riesgo_comercial)$"
    ),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    _user: dict = Depends(require_staff_user),
):
    return svc.list_opportunities(
        company_id, status=status, limit=limit, offset=offset
    )


@router.get("/compare/branches")
def compare_branches(
    company_id: int = Query(..., ge=1),
    q: str | None = Query(None, max_length=120),
    limit: int = Query(20, ge=1, le=100),
    _user: dict = Depends(require_staff_user),
):
    return svc.list_branch_comparison(company_id, q=q, limit=limit)


@router.get("/margin-impact")
def margin_impact(
    company_id: int = Query(..., ge=1),
    variant_id: int = Query(..., ge=1),
    _user: dict = Depends(require_staff_user),
):
    return svc.get_margin_impact(company_id, variant_id)


def _user_email(user: dict) -> str:
    email = (user.get("email") or "").strip().lower()
    if not email:
        raise HTTPException(status_code=401, detail="Token sin email")
    return email


@router.get("/intelligence")
def cost_intelligence(
    company_id: int = Query(..., ge=1),
    user: dict = Depends(require_staff_user),
):
    return svc.get_intelligence(company_id, user_email=_user_email(user))


@router.get("/watchlist")
def get_watchlist(
    company_id: int = Query(..., ge=1),
    user: dict = Depends(require_staff_user),
):
    return svc.list_watchlist(company_id, user_email=_user_email(user))


@router.get("/watchlist/status")
def watchlist_variant_status(
    company_id: int = Query(..., ge=1),
    variant_id: int = Query(..., ge=1),
    user: dict = Depends(require_staff_user),
):
    return svc.watchlist_status_for_variant(
        company_id, variant_id, user_email=_user_email(user)
    )


@router.post("/watchlist")
def post_watchlist(
    company_id: int = Query(..., ge=1),
    variant_id: int = Query(..., ge=1),
    user: dict = Depends(require_staff_user),
):
    return svc.add_to_watchlist(
        company_id, variant_id, user_email=_user_email(user)
    )


@router.delete("/watchlist")
def delete_watchlist(
    company_id: int = Query(..., ge=1),
    variant_id: int = Query(..., ge=1),
    user: dict = Depends(require_staff_user),
):
    return svc.remove_from_watchlist(
        company_id, variant_id, user_email=_user_email(user)
    )


@router.get("/compare/offices")
def compare_offices(
    company_id: int = Query(..., ge=1),
    variant_id: int | None = Query(None, ge=1),
    q: str | None = Query(None, max_length=120),
    _user: dict = Depends(require_staff_user),
):
    try:
        return svc.compare_offices(company_id, variant_id=variant_id, q=q)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/sync")
def trigger_sync(
    company_id: int | None = Query(None, ge=1),
    lookback_days: int | None = Query(None, ge=1, le=365),
    _user: dict = Depends(require_staff_user),
):
    try:
        return sync_cost_receptions(
            company_id=company_id,
            lookback_days=lookback_days,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
