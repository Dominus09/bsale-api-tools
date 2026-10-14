"""Analítica V2 — Control de Costos, Márgenes, Promociones y Dashboard sobre BSALE RAW.

Costos y Márgenes son solo lectura. Promociones V2 escribe únicamente en sus tablas propias
(app.commercial_*_v2), con permisos separados por acción; nunca modifica precios de Bsale.
Separado de los módulos legacy (/cost-analytics, /margins, /promotions) hasta su validación.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from decimal import Decimal
from typing import Any, Callable

import psycopg2
from fastapi import APIRouter, Depends, HTTPException, Path, Query
from pydantic import BaseModel, ConfigDict, Field

from backend.services.analytics.validate_distribuidora_source import AnalyticsValidationError
from backend.services.analytics_v2.access import (
    AnalyticsV2AccessError,
    require_cost_control_v2,
    require_margins_v2,
    require_promotions_approve,
    require_promotions_cancel,
    require_promotions_create,
    require_promotions_edit,
    require_promotions_view,
)
from backend.services.analytics_v2.promotion_campaigns import (
    CampaignConflict,
    CampaignNotFound,
    CampaignValidationError,
)
from backend.services.analytics_v2.promotions_service import (
    PromotionsForbidden,
    PromotionsNotReady,
    PromotionsV2Service,
    run_promotions_read,
    run_promotions_write,
)
from backend.services.analytics_v2.contracts import DEFAULT_COST_BASIS, DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE
from backend.services.analytics_v2.cost_control_service import (
    CostControlNotFound,
    CostControlV2Service,
    CostControlValidationError,
    run_cost_control,
)
from backend.services.analytics_v2.dashboard_service import run_dashboard
from backend.services.analytics_v2.margins_service import (
    MarginsNotFound,
    MarginsV2Service,
    MarginsValidationError,
    run_margins,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/analytics-v2", tags=["Analítica V2"])

CompanyQuery = Query(..., ge=1, description="Empresa; se verifica contra el usuario en backend")
PageQuery = Query(1, ge=1)
PageSizeQuery = Query(DEFAULT_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE)


def _run(
    endpoint: str,
    user: dict[str, Any],
    company_id: int,
    fn: Callable[[CostControlV2Service], Any],
) -> Any:
    return _guard(endpoint, company_id, lambda: run_cost_control(user, company_id, fn))


def _run_margins(
    endpoint: str,
    user: dict[str, Any],
    company_id: int,
    fn: Callable[[MarginsV2Service], Any],
) -> Any:
    return _guard(endpoint, company_id, lambda: run_margins(user, company_id, fn))


def _guard(endpoint: str, company_id: int, call: Callable[[], Any]) -> Any:
    try:
        return call()
    except AnalyticsV2AccessError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc
    except (CostControlValidationError, MarginsValidationError, CampaignValidationError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except (CostControlNotFound, MarginsNotFound, CampaignNotFound) as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except CampaignConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except PromotionsForbidden as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except PromotionsNotReady as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except psycopg2.IntegrityError as exc:
        logger.warning("analytics_v2_integrity endpoint=%s company_id=%s", endpoint, company_id)
        raise HTTPException(status_code=409, detail="Conflicto con datos existentes; recargue e intente de nuevo") from exc
    except AnalyticsValidationError as exc:
        logger.warning(
            "analytics_v2_query_error endpoint=%s company_id=%s error_type=%s",
            endpoint,
            company_id,
            exc.error_type,
        )
        if exc.error_type == "statement_timeout":
            raise HTTPException(status_code=503, detail="Consulta demoró demasiado; reintente") from exc
        raise HTTPException(status_code=500, detail="Error interno de Analítica V2") from exc
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception(
            "analytics_v2_endpoint_error endpoint=%s company_id=%s exc_type=%s",
            endpoint,
            company_id,
            type(exc).__name__,
        )
        raise HTTPException(status_code=500, detail="Error interno de Analítica V2") from exc


@router.get("/freshness")
def raw_freshness(
    company_id: int = CompanyQuery,
    user: dict = Depends(require_cost_control_v2),
):
    return _run("freshness", user, company_id, lambda s: s.freshness())


@router.get("/costs/summary")
def costs_summary(
    company_id: int = CompanyQuery,
    user: dict = Depends(require_cost_control_v2),
):
    return _run("costs_summary", user, company_id, lambda s: s.summary())


@router.get("/costs/tax-coverage")
def costs_tax_coverage(
    company_id: int = CompanyQuery,
    user: dict = Depends(require_cost_control_v2),
):
    """Inventario de impuestos RAW, combinaciones por producto, cobertura del costo bruto y anomalías."""
    return _run("costs_tax_coverage", user, company_id, lambda s: s.tax_coverage())


@router.get("/costs/filters")
def costs_filters(
    company_id: int = CompanyQuery,
    user: dict = Depends(require_cost_control_v2),
):
    return _run("costs_filters", user, company_id, lambda s: s.filters())


@router.get("/costs/products")
def costs_products(
    company_id: int = CompanyQuery,
    q: str | None = Query(None, max_length=100),
    product_type_id: int | None = Query(None, ge=1),
    variant_state: str = Query("active"),
    reliability: str | None = Query(None),
    tax_status: str | None = Query(None),
    last_movement_class: str | None = Query(None),
    anomaly_code: str | None = Query(None),
    office_id: int | None = Query(None, ge=1),
    min_abs_change_pct: Decimal | None = Query(None, ge=0, le=100000),
    sort: str = Query("reliability"),
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    user: dict = Depends(require_cost_control_v2),
):
    return _run(
        "costs_products",
        user,
        company_id,
        lambda s: s.list_products(
            q=q.strip() if q else None,
            product_type_id=product_type_id,
            variant_state=variant_state,
            reliability=reliability,
            tax_status=tax_status,
            last_movement_class=last_movement_class,
            anomaly_code=anomaly_code,
            office_id=office_id,
            min_abs_change_pct=min_abs_change_pct,
            sort=sort,
            page=page,
            page_size=page_size,
        ),
    )


@router.get("/costs/products/{variant_id}")
def costs_product(
    variant_id: int,
    company_id: int = CompanyQuery,
    user: dict = Depends(require_cost_control_v2),
):
    return _run("costs_product", user, company_id, lambda s: s.get_product(variant_id))


@router.get("/costs/products/{variant_id}/history")
def costs_product_history(
    variant_id: int,
    company_id: int = CompanyQuery,
    office_id: int | None = Query(None, ge=1),
    date_from: date | None = Query(None),
    date_to: date | None = Query(None),
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    user: dict = Depends(require_cost_control_v2),
):
    return _run(
        "costs_product_history",
        user,
        company_id,
        lambda s: s.product_history(
            variant_id,
            office_id=office_id,
            date_from=date_from,
            date_to=date_to,
            page=page,
            page_size=page_size,
        ),
    )


@router.get("/costs/alerts")
def costs_alerts(
    company_id: int = CompanyQuery,
    date_from: date | None = Query(None),
    date_to: date | None = Query(None),
    anomaly_code: str | None = Query(None),
    severity: str | None = Query(None),
    office_id: int | None = Query(None, ge=1),
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    user: dict = Depends(require_cost_control_v2),
):
    end = date_to or date.today()
    start = date_from or (end - timedelta(days=90))
    return _run(
        "costs_alerts",
        user,
        company_id,
        lambda s: s.list_alerts(
            date_from=start,
            date_to=end,
            anomaly_code=anomaly_code,
            severity=severity,
            office_id=office_id,
            page=page,
            page_size=page_size,
        ),
    )


@router.get("/costs/receptions/{reception_id}")
def costs_reception(
    reception_id: int,
    company_id: int = CompanyQuery,
    user: dict = Depends(require_cost_control_v2),
):
    return _run("costs_reception", user, company_id, lambda s: s.get_reception(reception_id))


# ---------------------------------------------------------------- Márgenes V2

CostBasisQuery = Query(DEFAULT_COST_BASIS.value, max_length=40)


@router.get("/margins/summary")
def margins_summary(
    company_id: int = CompanyQuery,
    cost_basis: str = CostBasisQuery,
    user: dict = Depends(require_margins_v2),
):
    return _run_margins("margins_summary", user, company_id, lambda s: s.summary(cost_basis=cost_basis))


@router.get("/margins/filters")
def margins_filters(
    company_id: int = CompanyQuery,
    user: dict = Depends(require_margins_v2),
):
    return _run_margins("margins_filters", user, company_id, lambda s: s.filters())


@router.get("/margins/scope")
def margins_scope(
    company_id: int = CompanyQuery,
    user: dict = Depends(require_margins_v2),
):
    return _run_margins("margins_scope", user, company_id, lambda s: s.scope())


@router.get("/margins/rows")
def margins_rows(
    company_id: int = CompanyQuery,
    q: str | None = Query(None, max_length=100),
    price_list_id: int | None = Query(None, ge=1),
    product_type_id: int | None = Query(None, ge=1),
    status: str | None = Query(None, max_length=60),
    qualifier: str | None = Query(None, max_length=60),
    reliability: str | None = Query(None, max_length=60),
    cost_basis: str = CostBasisQuery,
    variant_state: str = Query("active"),
    sort: str = Query("status"),
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    user: dict = Depends(require_margins_v2),
):
    return _run_margins(
        "margins_rows",
        user,
        company_id,
        lambda s: s.list_rows(
            q=q.strip() if q else None,
            price_list_id=price_list_id,
            product_type_id=product_type_id,
            status=status,
            qualifier=qualifier,
            reliability=reliability,
            cost_basis=cost_basis,
            variant_state=variant_state,
            sort=sort,
            page=page,
            page_size=page_size,
        ),
    )


@router.get("/margins/products/{variant_id}")
def margins_product(
    variant_id: int,
    company_id: int = CompanyQuery,
    cost_basis: str = CostBasisQuery,
    user: dict = Depends(require_margins_v2),
):
    return _run_margins("margins_product", user, company_id, lambda s: s.product(variant_id, cost_basis=cost_basis))


@router.get("/margins/policy-review")
def margins_policy_review(
    company_id: int = CompanyQuery,
    only_issues: bool = Query(False),
    user: dict = Depends(require_margins_v2),
):
    return _run_margins("margins_policy_review", user, company_id, lambda s: s.policy_review(only_issues=only_issues))


# ---------------------------------------------------------------- Dashboard V2


@router.get("/dashboard")
def dashboard(
    company_id: int = CompanyQuery,
    cost_basis: str = CostBasisQuery,
    refresh: bool = Query(False, description="Ignora la caché corta del dashboard"),
    user: dict = Depends(require_margins_v2),
):
    """Costos, márgenes, listas y promociones en una llamada; cada bloque respeta el permiso de su módulo."""
    return _guard(
        "dashboard",
        company_id,
        lambda: run_dashboard(user, company_id, cost_basis=cost_basis, refresh=refresh),
    )


# ---------------------------------------------------------------- Promociones V2

MonthQuery = Query(None, max_length=7, description="AAAA-MM; por defecto el mes actual")


def _promo_read(endpoint: str, user: dict[str, Any], company_id: int, fn: Callable[[PromotionsV2Service], Any]) -> Any:
    return _guard(endpoint, company_id, lambda: run_promotions_read(user, company_id, fn))


def _promo_write(endpoint: str, user: dict[str, Any], company_id: int, fn: Callable[[PromotionsV2Service], Any]) -> Any:
    return _guard(endpoint, company_id, lambda: run_promotions_write(user, company_id, fn))


class _StrictBody(BaseModel):
    # Campos no declarados (p. ej. precio normal o stock) se rechazan: se leen en backend desde RAW.
    model_config = ConfigDict(extra="forbid")


class CampaignCreateBody(_StrictBody):
    kind: str = Field(..., max_length=20)
    name: str = Field(..., max_length=200)
    starts_on: date
    ends_on: date
    responsible: str | None = Field(None, max_length=200)
    notes: str | None = Field(None, max_length=2000)


class CampaignUpdateBody(_StrictBody):
    version: int = Field(..., ge=1)
    name: str | None = Field(None, max_length=200)
    starts_on: date | None = None
    ends_on: date | None = None
    responsible: str | None = Field(None, max_length=200)
    notes: str | None = Field(None, max_length=2000)


class ItemCreateBody(_StrictBody):
    version: int = Field(..., ge=1)
    variant_id: int = Field(..., ge=1)
    price_list_id: int = Field(..., ge=1)
    promo_gross_price: str | int
    clearance_reason: str | None = Field(None, max_length=40)
    notes: str | None = Field(None, max_length=2000)


class ItemUpdateBody(_StrictBody):
    version: int = Field(..., ge=1)
    promo_gross_price: str | int | None = None
    clearance_reason: str | None = Field(None, max_length=40)
    notes: str | None = Field(None, max_length=2000)
    reason: str | None = Field(None, max_length=1000)


class ReasonBody(_StrictBody):
    version: int = Field(..., ge=1)
    reason: str | None = Field(None, max_length=1000)


def _set_fields(body: BaseModel) -> dict[str, Any]:
    return {k: v for k, v in body.model_dump(exclude_unset=True).items() if not (v is None and k == "promo_gross_price")}


@router.get("/promotions/summary")
def promotions_summary(
    company_id: int = CompanyQuery,
    month: str | None = MonthQuery,
    user: dict = Depends(require_promotions_view),
):
    return _promo_read("promotions_summary", user, company_id, lambda s: s.summary(month=month))


@router.get("/promotions/filters")
def promotions_filters(company_id: int = CompanyQuery, user: dict = Depends(require_promotions_view)):
    return _promo_read("promotions_filters", user, company_id, lambda s: s.filters())


@router.get("/promotions/campaigns")
def promotions_campaigns(
    company_id: int = CompanyQuery,
    month: str | None = MonthQuery,
    kind: str | None = Query(None, max_length=20),
    approval: str | None = Query(None, max_length=40),
    timeline: str | None = Query(None, max_length=40),
    price_list_id: int | None = Query(None, ge=1),
    q: str | None = Query(None, max_length=100),
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    user: dict = Depends(require_promotions_view),
):
    return _promo_read(
        "promotions_campaigns",
        user,
        company_id,
        lambda s: s.list_campaigns(
            month=month,
            kind=kind,
            approval=approval,
            timeline=timeline,
            price_list_id=price_list_id,
            q=q.strip() if q else None,
            page=page,
            page_size=page_size,
        ),
    )


@router.get("/promotions/campaigns/{campaign_id}")
def promotions_campaign(
    campaign_id: int = Path(..., ge=1),
    company_id: int = CompanyQuery,
    user: dict = Depends(require_promotions_view),
):
    return _promo_read("promotions_campaign", user, company_id, lambda s: s.campaign_detail(campaign_id))


@router.get("/promotions/items")
def promotions_items(
    company_id: int = CompanyQuery,
    section: str = Query("vigentes", max_length=20),
    month: str | None = MonthQuery,
    kind: str | None = Query(None, max_length=20),
    approval: str | None = Query(None, max_length=40),
    price_list_id: int | None = Query(None, ge=1),
    q: str | None = Query(None, max_length=100),
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    user: dict = Depends(require_promotions_view),
):
    return _promo_read(
        "promotions_items",
        user,
        company_id,
        lambda s: s.list_items(
            section=section,
            month=month,
            kind=kind,
            approval=approval,
            price_list_id=price_list_id,
            q=q.strip() if q else None,
            page=page,
            page_size=page_size,
        ),
    )


@router.get("/promotions/history")
def promotions_history(
    company_id: int = CompanyQuery,
    month: str | None = MonthQuery,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    user: dict = Depends(require_promotions_view),
):
    return _promo_read("promotions_history", user, company_id, lambda s: s.history(month=month, page=page, page_size=page_size))


@router.get("/promotions/legacy")
def promotions_legacy(company_id: int = CompanyQuery, user: dict = Depends(require_promotions_view)):
    return _promo_read("promotions_legacy", user, company_id, lambda s: s.legacy())


@router.get("/promotions/products/search")
def promotions_product_search(
    company_id: int = CompanyQuery,
    q: str = Query(..., min_length=2, max_length=60),
    user: dict = Depends(require_promotions_edit),
):
    return _promo_read("promotions_product_search", user, company_id, lambda s: s.search_products(q=q))


@router.post("/promotions/campaigns")
def promotions_create_campaign(
    body: CampaignCreateBody,
    company_id: int = CompanyQuery,
    user: dict = Depends(require_promotions_create),
):
    return _promo_write("promotions_create", user, company_id, lambda s: s.create_campaign(body.model_dump()))


@router.patch("/promotions/campaigns/{campaign_id}")
def promotions_update_campaign(
    body: CampaignUpdateBody,
    campaign_id: int = Path(..., ge=1),
    company_id: int = CompanyQuery,
    user: dict = Depends(require_promotions_edit),
):
    return _promo_write("promotions_update", user, company_id, lambda s: s.update_campaign(campaign_id, _set_fields(body)))


@router.post("/promotions/campaigns/{campaign_id}/items")
def promotions_add_item(
    body: ItemCreateBody,
    campaign_id: int = Path(..., ge=1),
    company_id: int = CompanyQuery,
    user: dict = Depends(require_promotions_edit),
):
    return _promo_write("promotions_add_item", user, company_id, lambda s: s.add_item(campaign_id, body.model_dump()))


@router.patch("/promotions/campaigns/{campaign_id}/items/{item_id}")
def promotions_update_item(
    body: ItemUpdateBody,
    campaign_id: int = Path(..., ge=1),
    item_id: int = Path(..., ge=1),
    company_id: int = CompanyQuery,
    user: dict = Depends(require_promotions_edit),
):
    return _promo_write(
        "promotions_update_item", user, company_id, lambda s: s.update_item(campaign_id, item_id, _set_fields(body))
    )


@router.post("/promotions/campaigns/{campaign_id}/items/{item_id}/remove")
def promotions_remove_item(
    body: ReasonBody,
    campaign_id: int = Path(..., ge=1),
    item_id: int = Path(..., ge=1),
    company_id: int = CompanyQuery,
    user: dict = Depends(require_promotions_edit),
):
    return _promo_write(
        "promotions_remove_item", user, company_id, lambda s: s.remove_item(campaign_id, item_id, body.model_dump())
    )


@router.post("/promotions/campaigns/{campaign_id}/submit")
def promotions_submit(
    body: ReasonBody,
    campaign_id: int = Path(..., ge=1),
    company_id: int = CompanyQuery,
    user: dict = Depends(require_promotions_edit),
):
    return _promo_write("promotions_submit", user, company_id, lambda s: s.transition(campaign_id, "submit", body.model_dump()))


@router.post("/promotions/campaigns/{campaign_id}/approve")
def promotions_approve(
    body: ReasonBody,
    campaign_id: int = Path(..., ge=1),
    company_id: int = CompanyQuery,
    user: dict = Depends(require_promotions_approve),
):
    return _promo_write("promotions_approve", user, company_id, lambda s: s.transition(campaign_id, "approve", body.model_dump()))


@router.post("/promotions/campaigns/{campaign_id}/reject")
def promotions_reject(
    body: ReasonBody,
    campaign_id: int = Path(..., ge=1),
    company_id: int = CompanyQuery,
    user: dict = Depends(require_promotions_approve),
):
    return _promo_write("promotions_reject", user, company_id, lambda s: s.transition(campaign_id, "reject", body.model_dump()))


@router.post("/promotions/campaigns/{campaign_id}/cancel")
def promotions_cancel(
    body: ReasonBody,
    campaign_id: int = Path(..., ge=1),
    company_id: int = CompanyQuery,
    user: dict = Depends(require_promotions_cancel),
):
    return _promo_write("promotions_cancel", user, company_id, lambda s: s.transition(campaign_id, "cancel", body.model_dump()))


@router.post("/promotions/legacy/{promotion_id}/import")
def promotions_import_legacy(
    promotion_id: int = Path(..., ge=1),
    company_id: int = CompanyQuery,
    user: dict = Depends(require_promotions_create),
):
    return _promo_write("promotions_import_legacy", user, company_id, lambda s: s.import_legacy(promotion_id))
