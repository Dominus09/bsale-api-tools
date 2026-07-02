"""Validación pre-deploy del motor comercial — Etapa 1: cuadratura ERP vs Bsale."""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

from backend.config.commercial_scope import (
    ACTIVE_SELLERS,
    DD_SIGNED_QTY,
    DOC_BOLETA,
    DOC_FACTURA,
    DOC_NC,
    ENGINE_VERSION,
    SALES_SCOPE_VERSION,
    SELLER_NAME_BY_ID,
    validation_scope_payload,
)
from backend.services.commercial_analytics_engine import CommercialReadSession, SalesScope
from backend.services.commercial_analytics_service import CommercialFilters
from backend.utils.commercial_period import compare_period_meta


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


def _delta(current: float, previous: float) -> dict[str, Any]:
    diff = current - previous
    if previous == 0:
        pct = 100.0 if current > 0 else 0.0
    else:
        pct = (diff / abs(previous)) * 100.0
    return {
        "current": round(current, 2),
        "previous": round(previous, 2),
        "delta_abs": round(diff, 2),
        "delta_pct": round(pct, 1),
    }


def _metric_block(row: dict[str, Any], prefix: str) -> dict[str, Any]:
    return {
        "first_document_date": str(row.get(f"{prefix}_first_date")) if row.get(f"{prefix}_first_date") else None,
        "last_document_date": str(row.get(f"{prefix}_last_date")) if row.get(f"{prefix}_last_date") else None,
        "documents_total": _int(row.get(f"{prefix}_doc_total")),
        "documents_facturas": _int(row.get(f"{prefix}_doc_facturas")),
        "documents_boletas": _int(row.get(f"{prefix}_doc_boletas")),
        "documents_notas_credito": _int(row.get(f"{prefix}_doc_nc")),
        "venta_neta": round(_float(row.get(f"{prefix}_venta_neta")), 2),
        "venta_facturas": round(_float(row.get(f"{prefix}_venta_facturas")), 2),
        "venta_boletas": round(_float(row.get(f"{prefix}_venta_boletas")), 2),
        "notas_credito_monto": round(_float(row.get(f"{prefix}_nc_monto")), 2),
        "clientes_unicos": _int(row.get(f"{prefix}_clientes")),
        "productos_unicos": _int(row.get(f"{prefix}_productos")),
        "lineas_documento": _int(row.get(f"{prefix}_lineas")),
        "unidades_netas": round(_float(row.get(f"{prefix}_unidades")), 2),
        "ticket_promedio": round(_float(row.get(f"{prefix}_ticket")), 2),
        "documentos_emitidos": _int(row.get(f"{prefix}_docs_emitidos")),
    }


def _period_agg_sql(alias: str, period_cte: str) -> str:
    """Agregados de un período (CTE ya filtrado por fechas)."""
    return f"""
        SELECT
            MIN(p.sale_day) AS {alias}_first_date,
            MAX(p.sale_day) AS {alias}_last_date,
            COUNT(DISTINCT p.document_id)::bigint AS {alias}_doc_total,
            COUNT(DISTINCT p.document_id) FILTER (WHERE p.document_type_id = {DOC_FACTURA})::bigint AS {alias}_doc_facturas,
            COUNT(DISTINCT p.document_id) FILTER (WHERE p.document_type_id = {DOC_BOLETA})::bigint AS {alias}_doc_boletas,
            COUNT(DISTINCT p.document_id) FILTER (WHERE p.document_type_id = {DOC_NC})::bigint AS {alias}_doc_nc,
            COUNT(DISTINCT p.client_id) FILTER (WHERE p.is_sale = 1)::bigint AS {alias}_clientes,
            COALESCE(SUM(p.total_amount_net), 0) AS {alias}_venta_neta,
            COALESCE(SUM(p.total_amount_net) FILTER (WHERE p.document_type_id = {DOC_FACTURA}), 0) AS {alias}_venta_facturas,
            COALESCE(SUM(p.total_amount_net) FILTER (WHERE p.document_type_id = {DOC_BOLETA}), 0) AS {alias}_venta_boletas,
            COALESCE(ABS(SUM(p.total_amount_net) FILTER (WHERE p.document_type_id = {DOC_NC})), 0) AS {alias}_nc_monto,
            COALESCE(SUM(p.is_sale), 0)::bigint AS {alias}_docs_emitidos,
            COALESCE(
                SUM(p.total_amount_sales) FILTER (WHERE p.is_sale = 1)
                / NULLIF(SUM(p.is_sale)::numeric, 0),
                0
            ) AS {alias}_ticket,
            (SELECT COUNT(DISTINCT dd.variant_id) FROM {period_cte} px
                INNER JOIN distribuidora.document_details dd ON dd.document_id = px.document_id) AS {alias}_productos,
            (SELECT COUNT(*) FROM {period_cte} px
                INNER JOIN distribuidora.document_details dd ON dd.document_id = px.document_id) AS {alias}_lineas,
            (SELECT COALESCE(SUM({DD_SIGNED_QTY.replace("sb.", "px.")}), 0) FROM {period_cte} px
                INNER JOIN distribuidora.document_details dd ON dd.document_id = px.document_id) AS {alias}_unidades
        FROM {period_cte} p
    """


def _build_audit_checks(
    curr: dict[str, Any],
    prev: dict[str, Any],
    period_meta: dict[str, Any],
    sellers: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []

    formula_diff = abs(
        curr["venta_facturas"] + curr["venta_boletas"] - curr["notas_credito_monto"] - curr["venta_neta"]
    )
    if formula_diff >= 1:
        checks.append({
            "severity": "error",
            "metric": "venta_neta",
            "message": "La venta neta no cuadra con Facturas + Boletas − NC",
            "delta_abs": round(formula_diff, 2),
            "delta_pct": None,
            "possible_cause": "Revisar signo de notas de crédito en v_sales o líneas duplicadas",
            "seller": None,
            "document_type": "mixto",
        })
    else:
        checks.append({
            "severity": "ok",
            "metric": "venta_neta",
            "message": "Fórmula venta neta (F + B − NC) consistente",
            "delta_abs": 0,
            "delta_pct": None,
            "possible_cause": None,
            "seller": None,
            "document_type": None,
        })

    if not period_meta.get("same_length"):
        checks.append({
            "severity": "warning",
            "metric": "compare_period",
            "message": "El período anterior no tiene el mismo largo que el actual",
            "delta_abs": abs(
                int(period_meta["current"]["days"]) - int(period_meta["previous"]["days"])
            ),
            "delta_pct": None,
            "possible_cause": "Ajuste de fin de mes; verificar método de comparación",
            "seller": None,
            "document_type": None,
        })

    if curr["documents_notas_credito"] > 0 and curr["notas_credito_monto"] == 0:
        checks.append({
            "severity": "warning",
            "metric": "notas_credito",
            "message": "Hay NC registradas pero el monto neto es cero",
            "delta_abs": curr["documents_notas_credito"],
            "delta_pct": None,
            "possible_cause": "Montos en cero en documento o vista v_sales",
            "seller": None,
            "document_type": "nota_credito",
        })

    for s in sellers:
        if s.get("seller_id") and s["seller_id"] not in {int(x["id"]) for x in ACTIVE_SELLERS}:
            checks.append({
                "severity": "warning",
                "metric": "seller_scope",
                "message": f"Vendedor fuera del scope operativo: {s.get('seller')}",
                "delta_abs": s.get("ventas_netas", {}).get("current", 0),
                "delta_pct": None,
                "possible_cause": "Documento asignado a vendedor no operativo",
                "seller": s.get("seller"),
                "document_type": None,
            })

    venta_delta = _delta(curr["venta_neta"], prev["venta_neta"])
    if abs(venta_delta["delta_pct"]) >= 40 and abs(venta_delta["delta_abs"]) >= 500_000:
        checks.append({
            "severity": "info",
            "metric": "venta_neta",
            "message": "Variación fuerte vs período anterior — validar en Bsale",
            "delta_abs": venta_delta["delta_abs"],
            "delta_pct": venta_delta["delta_pct"],
            "possible_cause": "Cambio real de operación, NC concentradas o período incompleto en Bsale",
            "seller": None,
            "document_type": None,
        })

    return checks


def build_commercial_validation(filters: CommercialFilters) -> dict[str, Any]:
    """Auditoría Etapa 1 usando el mismo sales_base que el bundle."""
    scope = SalesScope.from_filters(filters)
    f = scope.filters
    prev_from, prev_to = scope.prev_from, scope.prev_to
    period_meta = compare_period_meta(
        f.date_from,
        f.date_to,
        compare_date_from=f.compare_date_from,
        compare_date_to=f.compare_date_to,
    )
    t0 = time.perf_counter()

    base_cte, base_params = scope.sales_base_cte()
    bound = tuple(
        list(base_params)
        + [f.date_from, f.date_to, prev_from, prev_to]
    )

    totals_sql = f"""
        WITH {base_cte},
        period_curr AS (
            SELECT sb.* FROM sales_base sb
            WHERE sb.sale_day BETWEEN %s AND %s
        ),
        period_prev AS (
            SELECT sb.* FROM sales_base sb
            WHERE sb.sale_day BETWEEN %s AND %s
        ),
        curr AS ({_period_agg_sql("c", "period_curr")}),
        prev AS ({_period_agg_sql("p", "period_prev")})
        SELECT c.*, p.*
        FROM curr c
        CROSS JOIN prev p
    """

    sellers_sql = f"""
        WITH {base_cte},
        period_curr AS (
            SELECT sb.* FROM sales_base sb WHERE sb.sale_day BETWEEN %s AND %s
        ),
        period_prev AS (
            SELECT sb.* FROM sales_base sb WHERE sb.sale_day BETWEEN %s AND %s
        ),
        curr AS (
            SELECT
                p.seller_id,
                MAX(p.seller_name) AS seller_name,
                COUNT(DISTINCT p.document_id)::bigint AS documents,
                COUNT(DISTINCT p.client_id) FILTER (WHERE p.is_sale = 1)::bigint AS clients,
                COUNT(DISTINCT p.document_id) FILTER (WHERE p.document_type_id = {DOC_NC})::bigint AS notas_credito,
                COALESCE(SUM(p.total_amount_net), 0) AS ventas_netas,
                COALESCE(
                    SUM(p.total_amount_sales) FILTER (WHERE p.is_sale = 1)
                    / NULLIF(SUM(p.is_sale)::numeric, 0),
                    0
                ) AS ticket_promedio
            FROM period_curr p
            GROUP BY p.seller_id
        ),
        prev AS (
            SELECT
                p.seller_id,
                COALESCE(SUM(p.total_amount_net), 0) AS ventas_netas,
                COUNT(DISTINCT p.document_id)::bigint AS documents,
                COUNT(DISTINCT p.client_id) FILTER (WHERE p.is_sale = 1)::bigint AS clients
            FROM period_prev p
            GROUP BY p.seller_id
        )
        SELECT
            COALESCE(c.seller_id, pr.seller_id) AS seller_id,
            COALESCE(c.seller_name, '') AS seller,
            COALESCE(c.documents, 0)::bigint AS documents,
            COALESCE(c.clients, 0)::bigint AS clients,
            COALESCE(c.notas_credito, 0)::bigint AS notas_credito,
            COALESCE(c.ventas_netas, 0) AS ventas_netas,
            COALESCE(c.ticket_promedio, 0) AS ticket_promedio,
            COALESCE(pr.ventas_netas, 0) AS ventas_netas_prev,
            COALESCE(pr.documents, 0)::bigint AS documents_prev,
            COALESCE(pr.clients, 0)::bigint AS clients_prev
        FROM curr c
        FULL OUTER JOIN prev pr ON pr.seller_id = c.seller_id
        ORDER BY COALESCE(c.ventas_netas, pr.ventas_netas) DESC NULLS LAST
    """

    with CommercialReadSession("commercial-validation") as session:
        totals_row = session.query_one("validation_totals", totals_sql, bound) or {}
        seller_rows = session.query_all("validation_sellers", sellers_sql, bound)

    curr = _metric_block(totals_row, "c")
    prev = _metric_block(totals_row, "p")

    seller_by_id: dict[int, dict[str, Any]] = {}
    for r in seller_rows:
        sid = _int(r.get("seller_id"))
        if sid:
            seller_by_id[sid] = r

    seller_distribution: list[dict[str, Any]] = []
    for active in ACTIVE_SELLERS:
        sid = int(active["id"])
        row = seller_by_id.get(sid, {})
        name = str(active["name"])
        vn_curr = round(_float(row.get("ventas_netas")), 2)
        vn_prev = round(_float(row.get("ventas_netas_prev")), 2)
        seller_distribution.append({
            "seller": name,
            "seller_id": sid,
            "documents": _int(row.get("documents")),
            "clients": _int(row.get("clients")),
            "notas_credito": _int(row.get("notas_credito")),
            "ticket_promedio": round(_float(row.get("ticket_promedio")), 2),
            "ventas_netas": _delta(vn_curr, vn_prev),
        })

    for sid, row in seller_by_id.items():
        if sid not in {int(s["id"]) for s in ACTIVE_SELLERS}:
            seller_distribution.append({
                "seller": row.get("seller") or SELLER_NAME_BY_ID.get(sid, f"ID {sid}"),
                "seller_id": sid,
                "documents": _int(row.get("documents")),
                "clients": _int(row.get("clients")),
                "notas_credito": _int(row.get("notas_credito")),
                "ticket_promedio": round(_float(row.get("ticket_promedio")), 2),
                "ventas_netas": _delta(
                    round(_float(row.get("ventas_netas")), 2),
                    round(_float(row.get("ventas_netas_prev")), 2),
                ),
                "out_of_scope": True,
            })

    comparison = {
        "period_meta": period_meta,
        "current": curr,
        "previous": prev,
        "deltas": {
            "venta_neta": _delta(curr["venta_neta"], prev["venta_neta"]),
            "clientes_unicos": _delta(float(curr["clientes_unicos"]), float(prev["clientes_unicos"])),
            "documentos_total": _delta(float(curr["documents_total"]), float(prev["documents_total"])),
            "unidades_netas": _delta(curr["unidades_netas"], prev["unidades_netas"]),
            "ticket_promedio": _delta(curr["ticket_promedio"], prev["ticket_promedio"]),
        },
    }

    audit_checks = _build_audit_checks(curr, prev, period_meta, seller_distribution)
    has_error = any(c["severity"] == "error" for c in audit_checks)
    has_warning = any(c["severity"] == "warning" for c in audit_checks)

    execution_ms = (time.perf_counter() - t0) * 1000

    return {
        "scope": validation_scope_payload(),
        "period": {
            "from": f.date_from.isoformat(),
            "to": f.date_to.isoformat(),
        },
        "compare_period": period_meta,
        "comparison": comparison,
        "temporal_coverage": {
            "first_document_date": curr["first_document_date"],
            "last_document_date": curr["last_document_date"],
            "days_covered": (
                (totals_row.get("c_last_date") - totals_row.get("c_first_date")).days + 1
                if totals_row.get("c_first_date") and totals_row.get("c_last_date")
                else 0
            ),
        },
        "documents": {
            "total": curr["documents_total"],
            "facturas": curr["documents_facturas"],
            "boletas": curr["documents_boletas"],
            "notas_credito": curr["documents_notas_credito"],
        },
        "clients": {
            "unique_clients": curr["clientes_unicos"],
            "active_clients": curr["clientes_unicos"],
            "inactive_clients": 0,
        },
        "products": {
            "unique_products": curr["productos_unicos"],
            "total_lines": curr["lineas_documento"],
            "unidades_netas": curr["unidades_netas"],
        },
        "ventas_netas": {
            "facturas": curr["venta_facturas"],
            "boletas": curr["venta_boletas"],
            "notas_credito": curr["notas_credito_monto"],
            "ventas_netas": curr["venta_neta"],
            "ticket_promedio": curr["ticket_promedio"],
            "formula_check": round(
                curr["venta_facturas"] + curr["venta_boletas"] - curr["notas_credito_monto"], 2
            ),
        },
        "seller_distribution": seller_distribution,
        "audit_checks": audit_checks,
        "validation": {
            "status": "ERROR" if has_error else ("WARNING" if has_warning else "OK"),
            "engine_version": ENGINE_VERSION,
            "sales_scope_version": SALES_SCOPE_VERSION,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "execution_ms": round(execution_ms, 1),
        },
    }
