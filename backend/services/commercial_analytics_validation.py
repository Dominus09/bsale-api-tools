"""Validación pre-deploy del motor comercial — reutiliza sales_base del bundle."""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

from backend.config.commercial_scope import (
    DOC_BOLETA,
    DOC_FACTURA,
    DOC_NC,
    ENGINE_VERSION,
    SALES_SCOPE_VERSION,
    validation_scope_payload,
)
from backend.services.commercial_analytics_engine import CommercialReadSession, SalesScope
from backend.services.commercial_analytics_service import CommercialFilters


def _float(v: Any) -> float:
    from decimal import Decimal

    if v is None:
        return 0.0
    if isinstance(v, Decimal):
        return float(v)
    return float(v)


def _int(v: Any) -> int:
    if v is None:
        return 0
    return int(v)


def build_commercial_validation(filters: CommercialFilters) -> dict[str, Any]:
    """Auditoría del universo analítico usando el mismo sales_base que el bundle."""
    scope = SalesScope.from_filters(filters)
    f = scope.filters
    t0 = time.perf_counter()

    base_cte, base_params = scope.sales_base_cte()
    period_params = list(base_params) + [f.date_from, f.date_to]

    totals_sql = f"""
        WITH {base_cte},
        period AS (
            SELECT sb.*
            FROM sales_base sb
            WHERE sb.sale_day BETWEEN %s AND %s
        )
        SELECT
            (SELECT MIN(p.sale_day) FROM period p) AS first_document_date,
            (SELECT MAX(p.sale_day) FROM period p) AS last_document_date,
            (SELECT COUNT(DISTINCT p.document_id) FROM period p)::bigint AS doc_total,
            (SELECT COUNT(DISTINCT p.document_id) FROM period p WHERE p.document_type_id = {DOC_FACTURA})::bigint AS doc_facturas,
            (SELECT COUNT(DISTINCT p.document_id) FROM period p WHERE p.document_type_id = {DOC_BOLETA})::bigint AS doc_boletas,
            (SELECT COUNT(DISTINCT p.document_id) FROM period p WHERE p.document_type_id = {DOC_NC})::bigint AS doc_nc,
            (SELECT COUNT(DISTINCT p.client_id) FROM period p)::bigint AS unique_clients_all,
            (SELECT COUNT(DISTINCT p.client_id) FROM period p WHERE p.is_sale = 1)::bigint AS active_clients,
            (SELECT COUNT(DISTINCT dd.variant_id) FROM period p
                INNER JOIN distribuidora.document_details dd ON dd.document_id = p.document_id)::bigint AS unique_products,
            (SELECT COUNT(*) FROM period p
                INNER JOIN distribuidora.document_details dd ON dd.document_id = p.document_id)::bigint AS total_lines,
            (SELECT COALESCE(SUM(p.total_amount_net) FILTER (WHERE p.document_type_id = {DOC_FACTURA}), 0) FROM period p) AS facturas,
            (SELECT COALESCE(SUM(p.total_amount_net) FILTER (WHERE p.document_type_id = {DOC_BOLETA}), 0) FROM period p) AS boletas,
            (SELECT COALESCE(ABS(SUM(p.total_amount_net) FILTER (WHERE p.document_type_id = {DOC_NC})), 0) FROM period p) AS notas_credito,
            (SELECT COALESCE(SUM(p.total_amount_net), 0) FROM period p) AS ventas_netas
    """
    totals_bound = tuple(period_params)

    sellers_sql = f"""
        WITH {base_cte},
        period AS (
            SELECT sb.*
            FROM sales_base sb
            WHERE sb.sale_day BETWEEN %s AND %s
        )
        SELECT
            p.seller_name AS seller,
            MAX(p.seller_id) AS seller_id,
            COUNT(DISTINCT p.document_id)::bigint AS documents,
            COUNT(DISTINCT p.client_id) FILTER (WHERE p.is_sale = 1)::bigint AS clients,
            COALESCE(SUM(p.total_amount_net), 0) AS ventas_netas
        FROM period p
        GROUP BY p.seller_name
        ORDER BY ventas_netas DESC NULLS LAST, seller
    """

    with CommercialReadSession("commercial-validation") as session:
        totals = session.query_one("validation_totals", totals_sql, totals_bound) or {}
        seller_rows = session.query_all("validation_sellers", sellers_sql, tuple(period_params))

    first_date = totals.get("first_document_date")
    last_date = totals.get("last_document_date")
    days_covered = 0
    if first_date and last_date:
        days_covered = (last_date - first_date).days + 1

    unique_all = _int(totals.get("unique_clients_all"))
    active_clients = _int(totals.get("active_clients"))
    facturas_amt = _float(totals.get("facturas"))
    boletas_amt = _float(totals.get("boletas"))
    nc_amt = _float(totals.get("notas_credito"))
    ventas_netas = _float(totals.get("ventas_netas"))

    execution_ms = (time.perf_counter() - t0) * 1000

    return {
        "scope": validation_scope_payload(),
        "period": {
            "from": f.date_from.isoformat(),
            "to": f.date_to.isoformat(),
        },
        "temporal_coverage": {
            "first_document_date": str(first_date) if first_date else None,
            "last_document_date": str(last_date) if last_date else None,
            "days_covered": days_covered,
        },
        "documents": {
            "total": _int(totals.get("doc_total")),
            "facturas": _int(totals.get("doc_facturas")),
            "boletas": _int(totals.get("doc_boletas")),
            "notas_credito": _int(totals.get("doc_nc")),
        },
        "clients": {
            "unique_clients": unique_all,
            "active_clients": active_clients,
            "inactive_clients": max(0, unique_all - active_clients),
        },
        "products": {
            "unique_products": _int(totals.get("unique_products")),
            "total_lines": _int(totals.get("total_lines")),
        },
        "ventas_netas": {
            "facturas": round(facturas_amt, 2),
            "boletas": round(boletas_amt, 2),
            "notas_credito": round(nc_amt, 2),
            "ventas_netas": round(ventas_netas, 2),
            "formula_check": round(facturas_amt + boletas_amt - nc_amt, 2),
        },
        "seller_distribution": [
            {
                "seller": r.get("seller"),
                "seller_id": _int(r.get("seller_id")),
                "documents": _int(r.get("documents")),
                "clients": _int(r.get("clients")),
                "ventas_netas": round(_float(r.get("ventas_netas")), 2),
            }
            for r in seller_rows
        ],
        "validation": {
            "status": "OK",
            "engine_version": ENGINE_VERSION,
            "sales_scope_version": SALES_SCOPE_VERSION,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "execution_ms": round(execution_ms, 1),
        },
    }
