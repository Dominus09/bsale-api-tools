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
from fastapi.responses import StreamingResponse
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
from backend.services.analytics_v2 import excel_export
from backend.services.analytics_v2.catalog_service import CatalogValidationError, run_catalog
from backend.services.analytics_v2.contracts import (
    DEFAULT_COST_BASIS,
    DEFAULT_PAGE_SIZE,
    HISTORY_START,
    MAX_PAGE_SIZE,
)
from backend.services.analytics_v2.cost_control_service import (
    CostControlNotFound,
    CostControlV2Service,
    CostControlValidationError,
    run_cost_control,
)
from backend.services.analytics_v2.dashboard_service import run_dashboard
from backend.services.analytics_v2.packs_service import PacksV2Service, run_packs
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
    except (
        CostControlValidationError,
        MarginsValidationError,
        CampaignValidationError,
        CatalogValidationError,
    ) as exc:
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
    kind: str = Query("all", max_length=20),
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
            kind=kind,
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
    stock_filter: str | None = Query(None, max_length=20),
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
            stock_filter=stock_filter,
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


# ------------------------------------------- Catálogo Maestro y exportadores Excel
#
# Todo es solo lectura sobre BSALE RAW (o sobre los motores V2 existentes) y se
# acota por empresa validada en backend. Las exportaciones devuelven TODAS las
# filas que cumplen los filtros, no solo la página visible.

_XLSX_MEDIA = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
_EXPORT_ALL = 1_000_000
# Topes de la hoja de recepciones dentro del Excel de costos. Superarlos NUNCA
# produce una exportación silenciosamente incompleta: la cobertura queda
# declarada en el archivo y el historial completo se descarga por segmentos
# con /costs/receptions/export.xlsx (mismos filtros).
_RECEPTION_VARIANT_LIMIT = 300
_RECEPTION_LINE_LIMIT = 20_000


def _reception_lines_for(
    s: Any,
    *,
    company_id: int,
    items: list[dict[str, Any]],
    variant_offset: int = 0,
    variant_limit: int = _RECEPTION_VARIANT_LIMIT,
    line_limit: int = _RECEPTION_LINE_LIMIT,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Líneas de recepción originales de un tramo de variantes del universo.

    Devuelve (líneas, cobertura). La cobertura declara con exactitud si el
    tramo quedó completo o parcial y por qué límite.
    """
    total = len(items)
    tramo = items[variant_offset : variant_offset + variant_limit]
    by_id = {int(it["variant_id"]): it for it in tramo}
    lines: list[dict[str, Any]] = []
    truncated_lines = False
    for vid in by_id:
        if len(lines) >= line_limit:
            truncated_lines = True
            break
        for raw in s._repo.variant_lines(
            company_id=company_id, variant_id=vid, history_start=HISTORY_START
        ):
            if len(lines) >= line_limit:
                truncated_lines = True
                break
            it = by_id[vid]
            lines.append(
                {
                    "variant_id": vid,
                    "code": it.get("code"),
                    "product_name": it.get("product_name"),
                    "variant_name": it.get("variant_name") or it.get("description"),
                    "reception_id": raw.get("reception_id"),
                    "detail_id": raw.get("detail_id"),
                    "admission_date": raw.get("admission_date"),
                    "office_id": raw.get("office_id"),
                    "movement_class": str(raw.get("movement_class")),
                    "document": raw.get("document"),
                    "document_number": raw.get("document_number"),
                    "quantity": raw.get("quantity"),
                    "cost": raw.get("cost"),
                    "comparable": bool(raw.get("comparable")),
                }
            )
    variants_covered = variant_offset + len(tramo) >= total
    complete = variants_covered and not truncated_lines
    reasons: list[str] = []
    if not variants_covered:
        reasons.append(
            f"se incluyeron {len(tramo)} de {total} variantes del universo filtrado "
            f"(tope de {_RECEPTION_VARIANT_LIMIT} variantes por archivo)"
        )
    if truncated_lines:
        reasons.append(f"se alcanzó el tope de {line_limit} líneas de recepción")
    coverage = {
        "complete": complete,
        "lines_truncated": truncated_lines,
        "reason": "; ".join(reasons) if reasons else None,
        "variants_total": total,
        "variants_included": len(tramo),
        "limits": f"máx. {variant_limit} variantes y {line_limit} líneas por archivo",
        "segmented_hint": (
            "Descargue el historial completo por segmentos con el exportador independiente "
            "«Historial de recepciones» de esta misma pantalla (mismos filtros; cada segmento "
            f"cubre hasta {variant_limit} variantes)."
        ),
    }
    return lines, coverage


def _xlsx(content: bytes, filename: str) -> StreamingResponse:
    from io import BytesIO

    return StreamingResponse(
        BytesIO(content),
        media_type=_XLSX_MEDIA,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


def _filters_label(**parts: Any) -> str:
    activos = [f"{k}={v}" for k, v in parts.items() if v not in (None, "", "all")]
    return "; ".join(activos) if activos else "Sin filtros adicionales"


@router.get("/catalog/meta")
def catalog_meta(
    company_id: int = CompanyQuery,
    user: dict = Depends(require_margins_v2),
):
    return _guard("catalog_meta", company_id, lambda: run_catalog(user, company_id, lambda s: s.meta()))


@router.get("/catalog/rows")
def catalog_rows(
    company_id: int = CompanyQuery,
    q: str | None = Query(None, max_length=100),
    product_type_id: int | None = Query(None, ge=1),
    state: str = Query("all"),
    cost: str = Query("all"),
    tax: str = Query("all"),
    price_list_id: int | None = Query(None, ge=1),
    price_presence: str | None = Query(None),
    price_list_ids: str | None = Query(None, max_length=100),
    sort: str = Query("product"),
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    user: dict = Depends(require_margins_v2),
):
    f = {
        "q": q.strip() if q else None,
        "product_type_id": product_type_id,
        "state": state,
        "cost": cost,
        "tax": tax,
        "price_list_id": price_list_id,
        "price_presence": price_presence,
        "price_list_ids": [int(x) for x in price_list_ids.split(",") if x.strip()] if price_list_ids else None,
        "sort": sort,
    }
    return _guard(
        "catalog_rows", company_id, lambda: run_catalog(user, company_id, lambda s: s.list_rows(f, page, page_size))
    )


@router.get("/catalog/export.xlsx")
def catalog_export(
    company_id: int = CompanyQuery,
    scope: str = Query("full"),
    q: str | None = Query(None, max_length=100),
    product_type_id: int | None = Query(None, ge=1),
    state: str = Query("all"),
    cost: str = Query("all"),
    tax: str = Query("all"),
    price_list_id: int | None = Query(None, ge=1),
    price_presence: str | None = Query(None),
    price_list_ids: str | None = Query(None, max_length=100),
    sort: str = Query("product"),
    user: dict = Depends(require_margins_v2),
):
    if scope not in ("full", "filtered"):
        raise HTTPException(status_code=422, detail="scope debe ser full o filtered")
    f: dict[str, Any] = {
        "price_list_ids": [int(x) for x in price_list_ids.split(",") if x.strip()] if price_list_ids else None,
        "sort": sort,
    }
    if scope == "filtered":
        f.update(
            {
                "q": q.strip() if q else None,
                "product_type_id": product_type_id,
                "state": state,
                "cost": cost,
                "tax": tax,
                "price_list_id": price_list_id,
                "price_presence": price_presence,
            }
        )

    def build(s: Any) -> tuple[bytes, dict[str, Any]]:
        meta = s.meta()
        names = {pl["id"]: pl["name"] for pl in meta["price_lists"]}
        # iter_rows valida las listas contra la empresa y levanta 422 si no
        # están vigentes; aquí solo se resuelven las columnas a exportar.
        list_ids = f.get("price_list_ids") or [pl["id"] for pl in meta["price_lists"]]
        content, count, _codes, _bars = excel_export.catalog_xlsx(
            rows=s.iter_rows(f),
            meta=meta,
            list_ids=list_ids,
            list_names=names,
            scope_label="Catálogo completo de la empresa" if scope == "full" else "Resultados filtrados",
            filters_label=_filters_label(
                q=f.get("q"), tipo=f.get("product_type_id"), estado=f.get("state"),
                costo=f.get("cost"), impuestos=f.get("tax"),
            ),
        )
        return content, {"count": count}

    content, _info = _guard("catalog_export", company_id, lambda: run_catalog(user, company_id, build))
    return _xlsx(content, f"catalogo-maestro-empresa-{company_id}.xlsx")


@router.get("/costs/export.xlsx")
def costs_export(
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
    kind: str = Query("all", max_length=20),
    sort: str = Query("reliability"),
    user: dict = Depends(require_cost_control_v2),
):
    def build(s: Any) -> bytes:
        result = s.list_products(
            q=q.strip() if q else None,
            product_type_id=product_type_id,
            variant_state=variant_state,
            reliability=reliability,
            tax_status=tax_status,
            last_movement_class=last_movement_class,
            anomaly_code=anomaly_code,
            office_id=office_id,
            min_abs_change_pct=min_abs_change_pct,
            kind=kind,
            sort=sort,
            page=1,
            page_size=_EXPORT_ALL,
        )
        items = result["items"]
        meta = dict(result.get("meta") or {})
        meta.setdefault("company_id", company_id)
        summary = s.summary()
        priority = [
            it
            for it in items
            if (it.get("reliability") or {}).get("status") == "requiere_revision"
            or any(a.get("severity") == "alta" for a in (it.get("anomalies") or []))
        ]
        # Recepciones del universo filtrado (mismo repositorio del motor de
        # costos): se preserva la clasificación de movimiento de cada línea,
        # sin presentar transferencias internas como compras. La cobertura
        # (completa o parcial y por qué) queda declarada en el archivo.
        lines, coverage = _reception_lines_for(s, company_id=company_id, items=items)
        return excel_export.costs_xlsx(
            meta=meta,
            summary=summary,
            items=items,
            priority=priority,
            reception_lines=lines,
            reception_coverage=coverage,
            filters_label=_filters_label(
                q=q, tipo=product_type_id, estado=variant_state, confiabilidad=reliability,
                impuestos=tax_status, movimiento=last_movement_class, anomalia=anomaly_code,
                oficina=office_id, tipo_sku=kind,
            ),
        )

    content = _run("costs_export", user, company_id, build)
    return _xlsx(content, f"control-costos-empresa-{company_id}.xlsx")


@router.get("/costs/receptions/export.xlsx")
def costs_receptions_export(
    company_id: int = CompanyQuery,
    segment: int = Query(1, ge=1),
    segment_size: int = Query(_RECEPTION_VARIANT_LIMIT, ge=1, le=_RECEPTION_VARIANT_LIMIT),
    q: str | None = Query(None, max_length=100),
    product_type_id: int | None = Query(None, ge=1),
    variant_state: str = Query("active"),
    reliability: str | None = Query(None),
    tax_status: str | None = Query(None),
    last_movement_class: str | None = Query(None),
    anomaly_code: str | None = Query(None),
    office_id: int | None = Query(None, ge=1),
    min_abs_change_pct: Decimal | None = Query(None, ge=0, le=100000),
    kind: str = Query("all", max_length=20),
    sort: str = Query("reliability"),
    user: dict = Depends(require_cost_control_v2),
):
    """Historial de recepciones por segmentos de variantes (mismos filtros que
    el exportador de costos). Cada archivo declara su segmento y cobertura."""

    def build(s: Any) -> bytes:
        result = s.list_products(
            q=q.strip() if q else None,
            product_type_id=product_type_id,
            variant_state=variant_state,
            reliability=reliability,
            tax_status=tax_status,
            last_movement_class=last_movement_class,
            anomaly_code=anomaly_code,
            office_id=office_id,
            min_abs_change_pct=min_abs_change_pct,
            kind=kind,
            sort=sort,
            page=1,
            page_size=_EXPORT_ALL,
        )
        items = result["items"]
        meta = dict(result.get("meta") or {})
        meta.setdefault("company_id", company_id)
        total = len(items)
        segments_total = max(1, -(-total // segment_size)) if total else 1
        offset = (segment - 1) * segment_size
        lines, coverage = _reception_lines_for(
            s,
            company_id=company_id,
            items=items,
            variant_offset=offset,
            variant_limit=segment_size,
        )
        return excel_export.receptions_xlsx(
            meta=meta,
            reception_lines=lines,
            filters_label=_filters_label(
                segmento=segment, q=q, tipo=product_type_id, estado=variant_state,
                confiabilidad=reliability, impuestos=tax_status, movimiento=last_movement_class,
                anomalia=anomaly_code, oficina=office_id, tipo_sku=kind,
            ),
            segment=segment,
            segment_size=segment_size,
            variants_total=total,
            variants_included=coverage["variants_included"],
            segments_total=segments_total,
            lines_truncated=bool(coverage["lines_truncated"]),
        )

    content = _run("costs_receptions_export", user, company_id, build)
    return _xlsx(content, f"recepciones-empresa-{company_id}-segmento-{segment}.xlsx")


@router.get("/margins/export.xlsx")
def margins_export(
    company_id: int = CompanyQuery,
    mode: str = Query("completo"),
    q: str | None = Query(None, max_length=100),
    price_list_id: int | None = Query(None, ge=1),
    product_type_id: int | None = Query(None, ge=1),
    status: str | None = Query(None, max_length=60),
    qualifier: str | None = Query(None, max_length=60),
    reliability: str | None = Query(None, max_length=60),
    stock_filter: str | None = Query(None, max_length=20),
    cost_basis: str = CostBasisQuery,
    variant_state: str = Query("active"),
    user: dict = Depends(require_margins_v2),
):
    if mode not in ("bajo_costo", "bajo_politica", "completo"):
        raise HTTPException(status_code=422, detail="mode debe ser bajo_costo, bajo_politica o completo")

    def build(s: Any) -> bytes:
        result = s.list_rows(
            q=q.strip() if q else None,
            price_list_id=price_list_id,
            product_type_id=product_type_id,
            status=status,
            qualifier=qualifier,
            reliability=reliability,
            stock_filter=stock_filter,
            cost_basis=cost_basis,
            variant_state=variant_state,
            sort="status",
            page=1,
            page_size=_EXPORT_ALL,
        )
        items = result["items"]
        sheets: dict[str, list[dict[str, Any]]] = {}
        if mode in ("bajo_costo", "completo"):
            sheets["bajo_costo"] = [r for r in items if "venta_bajo_costo" in (r.get("risks") or [])]
        if mode in ("bajo_politica", "completo"):
            sheets["bajo_politica"] = [r for r in items if r.get("status") == "bajo_politica"]
        if mode == "completo":
            sheets["completo"] = items
        meta = dict(result.get("meta") or {})
        meta.setdefault("company_id", company_id)
        return excel_export.margins_xlsx(
            meta=meta,
            sheets=sheets,
            filters_label=_filters_label(
                modalidad=mode, q=q, lista=price_list_id, tipo=product_type_id,
                estado=status, confiabilidad=reliability, base_costo=cost_basis,
                existencias=stock_filter,
            ),
        )

    content = _run_margins("margins_export", user, company_id, build)
    return _xlsx(content, f"margenes-empresa-{company_id}.xlsx")


# ---------------------------------------------------------------- Packs Bsale


def _run_packs(endpoint: str, user: dict[str, Any], company_id: int, fn: Callable[[PacksV2Service], Any]) -> Any:
    return _guard(endpoint, company_id, lambda: run_packs(user, company_id, fn))


@router.get("/packs/summary")
def packs_summary(
    company_id: int = CompanyQuery,
    cost_basis: str = CostBasisQuery,
    user: dict = Depends(require_margins_v2),
):
    return _run_packs("packs_summary", user, company_id, lambda s: s.summary(cost_basis=cost_basis))


@router.get("/packs/rows")
def packs_rows(
    company_id: int = CompanyQuery,
    q: str | None = Query(None, max_length=100),
    price_list_id: int | None = Query(None, ge=1),
    pack_status: str | None = Query(None, max_length=60),
    margin_status: str | None = Query(None, max_length=60),
    problem: str | None = Query(None, max_length=60),
    pack_state: str = Query("active", max_length=20),
    cost_basis: str = CostBasisQuery,
    sort: str = Query("status", max_length=20),
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    user: dict = Depends(require_margins_v2),
):
    return _run_packs(
        "packs_rows",
        user,
        company_id,
        lambda s: s.list_rows(
            q=q.strip() if q else None,
            price_list_id=price_list_id,
            pack_status=pack_status,
            margin_status=margin_status,
            problem=problem,
            pack_state=pack_state,
            cost_basis=cost_basis,
            sort=sort,
            page=page,
            page_size=page_size,
        ),
    )


@router.get("/packs/{variant_id}")
def packs_detail(
    variant_id: int = Path(..., ge=1),
    company_id: int = CompanyQuery,
    cost_basis: str = CostBasisQuery,
    user: dict = Depends(require_margins_v2),
):
    return _run_packs("packs_detail", user, company_id, lambda s: s.detail(variant_id, cost_basis=cost_basis))


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
