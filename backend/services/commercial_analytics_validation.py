"""Validación pre-deploy del motor comercial — Etapa 1: cuadratura ERP vs Bsale."""

from __future__ import annotations

import time
from datetime import date, datetime, timezone
from typing import Any

from backend.config.commercial_scope import (
    ACTIVE_SELLERS,
    ACTIVE_SELLER_IDS,
    COMPANY_ID,
    DD_SIGNED_AMOUNT,
    DD_SIGNED_QTY,
    DOC_BOLETA,
    DOC_FACTURA,
    DOC_NC,
    ENGINE_VERSION,
    OFFICE_ID,
    SALES_SCOPE_VERSION,
    SELLER_NAME_BY_ID,
    validation_scope_payload,
)
from backend.services.commercial_analytics_engine import CommercialReadSession, SalesScope
from backend.services.commercial_analytics_service import CommercialFilters
from backend.utils.commercial_period import compare_period_meta

AUDIT_VALIDATION_VERSION = "2.1"
COMPARISON_METHOD = "document_id · venta neta (F + B − NC)"
MONEY_TOLERANCE = 1.0
COUNT_TOLERANCE = 0

DOC_TYPE_LABEL = {DOC_BOLETA: "Boleta", DOC_FACTURA: "Factura", DOC_NC: "Nota de crédito"}


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


def _reconcile_status(delta: float, *, is_money: bool = True) -> str:
    tol = MONEY_TOLERANCE if is_money else COUNT_TOLERANCE
    return "ok" if abs(delta) <= tol else "error"


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


def _erp_base_cte(scope: SalesScope) -> tuple[str, list[Any]]:
    """Documentos ERP (v_documents_latest) con la misma semántica de montos que v_sales."""
    extra_erp = scope.extra_sql.replace("sb.", "e.")
    params: list[Any] = [
        COMPANY_ID,
        OFFICE_ID,
        scope.doc_types,
        scope.seller_ids,
        scope.hist_from,
        scope.date_to,
    ]
    params.extend(scope.extra_params)
    sql = f"""
    erp_base AS (
        SELECT
            d.document_id,
            d.number,
            d.emission_date,
            (d.emission_date AT TIME ZONE 'UTC')::date AS sale_day,
            d.document_type_id,
            d.state,
            d.client_id,
            d.seller_id,
            COALESCE(NULLIF(TRIM(d.seller_name), ''), 'Sin vendedor') AS seller_name,
            CASE
                WHEN d.document_type_id = {DOC_NC} THEN -ABS(COALESCE(d.total_amount, 0::numeric))
                ELSE COALESCE(d.total_amount, 0::numeric)
            END AS total_amount_net,
            CASE
                WHEN d.document_type_id IN ({DOC_BOLETA}, {DOC_FACTURA}) THEN COALESCE(d.total_amount, 0::numeric)
                ELSE 0::numeric
            END AS total_amount_sales,
            CASE WHEN d.document_type_id IN ({DOC_BOLETA}, {DOC_FACTURA}) THEN 1 ELSE 0 END::integer AS is_sale,
            COALESCE(
                NULLIF(TRIM(d.municipality), ''),
                NULLIF(TRIM(c.municipality), '')
            ) AS municipality,
            COALESCE(
                NULLIF(TRIM(
                    CONCAT_WS(
                        ' ',
                        NULLIF(TRIM(c.company), ''),
                        NULLIF(TRIM(c.first_name), ''),
                        NULLIF(TRIM(c.last_name), '')
                    )
                ), ''),
                'Cliente ' || COALESCE(d.client_id::text, '0')
            ) AS client_name,
            d.url_public_view
        FROM distribuidora.v_documents_latest d
        LEFT JOIN bsale.clients c
            ON c.company_id = d.company_id
           AND c.bsale_id = d.client_id
        WHERE d.company_id = %s
          AND d.office_id = %s
          AND d.document_type_id IN %s
          AND d.seller_id IN %s
          AND (d.emission_date AT TIME ZONE 'UTC')::date >= %s
          AND (d.emission_date AT TIME ZONE 'UTC')::date <= %s
          AND COALESCE(d.state, 0) = 0
          {extra_erp}
    )"""
    return sql, params


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


def _build_data_coverage(erp: dict[str, Any], bsale: dict[str, Any]) -> list[dict[str, Any]]:
    def row(metric: str, erp_val: Any, bsale_val: Any, *, is_money: bool = False) -> dict[str, Any]:
        if is_money:
            match = abs(_float(erp_val) - _float(bsale_val)) <= MONEY_TOLERANCE
        elif isinstance(erp_val, (int, float)) or isinstance(bsale_val, (int, float)):
            match = _int(erp_val) == _int(bsale_val)
        else:
            match = str(erp_val or "") == str(bsale_val or "")
        return {
            "metric": metric,
            "erp": erp_val,
            "bsale": bsale_val,
            "match": match,
        }

    days_erp = 0
    if erp.get("first_document_date") and erp.get("last_document_date"):
        d0 = date.fromisoformat(str(erp["first_document_date"]))
        d1 = date.fromisoformat(str(erp["last_document_date"]))
        days_erp = (d1 - d0).days + 1
    days_bsale = 0
    if bsale.get("first_document_date") and bsale.get("last_document_date"):
        d0 = date.fromisoformat(str(bsale["first_document_date"]))
        d1 = date.fromisoformat(str(bsale["last_document_date"]))
        days_bsale = (d1 - d0).days + 1

    return [
        row("Primer documento", erp.get("first_document_date"), bsale.get("first_document_date")),
        row("Último documento", erp.get("last_document_date"), bsale.get("last_document_date")),
        row("Días cubiertos", days_erp, days_bsale),
        row("Documentos únicos", erp.get("documents_total"), bsale.get("documents_total")),
        row("Facturas", erp.get("documents_facturas"), bsale.get("documents_facturas")),
        row("Boletas", erp.get("documents_boletas"), bsale.get("documents_boletas")),
        row("Notas de crédito", erp.get("documents_notas_credito"), bsale.get("documents_notas_credito")),
        row("Clientes únicos", erp.get("clientes_unicos"), bsale.get("clientes_unicos")),
        row("Productos únicos", erp.get("productos_unicos"), bsale.get("productos_unicos")),
        row("Líneas", erp.get("lineas_documento"), bsale.get("lineas_documento")),
    ]


def _build_commercial_reconciliation(erp: dict[str, Any], bsale: dict[str, Any]) -> list[dict[str, Any]]:
    def concept_row(concept: str, erp_val: float, bsale_val: float, *, is_money: bool = True) -> dict[str, Any]:
        delta = round(erp_val - bsale_val, 2 if is_money else 0)
        return {
            "concept": concept,
            "erp": round(erp_val, 2 if is_money else 0),
            "bsale": round(bsale_val, 2 if is_money else 0),
            "delta": delta,
            "status": _reconcile_status(delta, is_money=is_money),
        }

    return [
        concept_row("Facturas", erp["venta_facturas"], bsale["venta_facturas"]),
        concept_row("Boletas", erp["venta_boletas"], bsale["venta_boletas"]),
        concept_row("Notas crédito", erp["notas_credito_monto"], bsale["notas_credito_monto"]),
        concept_row("Venta neta", erp["venta_neta"], bsale["venta_neta"]),
        concept_row("Ticket promedio", erp["ticket_promedio"], bsale["ticket_promedio"]),
        concept_row("Clientes únicos", float(erp["clientes_unicos"]), float(bsale["clientes_unicos"]), is_money=False),
        concept_row("Productos únicos", float(erp["productos_unicos"]), float(bsale["productos_unicos"]), is_money=False),
        concept_row("Unidades", erp["unidades_netas"], bsale["unidades_netas"]),
    ]


def _build_auto_audit_rules(
    session: CommercialReadSession,
    *,
    base_cte: str,
    erp_cte: str,
    date_from: date,
    date_to: date,
    bound: tuple[Any, ...],
) -> list[dict[str, Any]]:
    """12 reglas automáticas de calidad de datos."""
    audit_sql = f"""
        WITH {base_cte},
        {erp_cte},
        period_b AS (
            SELECT sb.* FROM sales_base sb WHERE sb.sale_day BETWEEN %s AND %s
        ),
        period_e AS (
            SELECT e.* FROM erp_base e WHERE e.sale_day BETWEEN %s AND %s
        ),
        scope_docs AS (
            SELECT d.*
            FROM distribuidora.v_documents_latest d
            WHERE d.company_id = %s
              AND d.office_id = %s
              AND d.document_type_id IN ({DOC_BOLETA}, {DOC_FACTURA}, {DOC_NC})
              AND COALESCE(d.state, 0) = 0
              AND (d.emission_date AT TIME ZONE 'UTC')::date BETWEEN %s AND %s
        ),
        sold_variants AS (
            SELECT DISTINCT dd.variant_id
            FROM period_b sb
            INNER JOIN distribuidora.document_details dd ON dd.document_id = sb.document_id
            WHERE dd.variant_id IS NOT NULL
        ),
        sold_clients AS (
            SELECT DISTINCT sb.client_id
            FROM period_b sb
            WHERE sb.client_id IS NOT NULL AND sb.is_sale = 1
        )
        SELECT
            (SELECT COUNT(*)::bigint FROM (
                SELECT sd.number, sd.document_type_id
                FROM scope_docs sd
                WHERE sd.number IS NOT NULL
                GROUP BY sd.number, sd.document_type_id
                HAVING COUNT(DISTINCT sd.document_id) > 1
            ) dup) AS duplicate_documents,
            (SELECT COUNT(*)::bigint FROM scope_docs sd WHERE sd.seller_id IS NULL) AS docs_sin_vendedor,
            (SELECT COUNT(*)::bigint FROM scope_docs sd
                WHERE sd.seller_id IS NOT NULL AND sd.seller_id NOT IN %s) AS sellers_out_scope,
            (SELECT COUNT(*)::bigint FROM distribuidora.documents d
                WHERE (d.emission_date AT TIME ZONE 'UTC')::date BETWEEN %s AND %s
                  AND d.document_type_id IN (1, 6, 9)
                  AND COALESCE(d.state, 0) = 0
                  AND d.company_id <> %s) AS wrong_company,
            (SELECT COUNT(*)::bigint FROM distribuidora.documents d
                WHERE (d.emission_date AT TIME ZONE 'UTC')::date BETWEEN %s AND %s
                  AND d.document_type_id IN (1, 6, 9)
                  AND COALESCE(d.state, 0) = 0
                  AND d.company_id = %s AND d.office_id <> %s) AS wrong_office,
            (SELECT COUNT(*)::bigint FROM period_e nc
                WHERE nc.document_type_id = {DOC_NC}
                  AND NOT EXISTS (
                    SELECT 1 FROM distribuidora.document_details dd
                    INNER JOIN distribuidora.document_related dr ON dr.detail_id = dd.detail_id
                    WHERE dd.document_id = nc.document_id
                  )) AS nc_sin_documento,
            (SELECT COUNT(*)::bigint FROM scope_docs sd
                WHERE sd.municipality IS NULL OR BTRIM(sd.municipality) = '') AS comunas_vacias,
            (SELECT COUNT(*)::bigint FROM sold_clients sc
                INNER JOIN bsale.rutero r ON r.company_id = %s AND r.bsale_id = sc.client_id
                WHERE r.lat IS NULL AND r.lon IS NULL) AS clientes_sin_georef,
            (SELECT COUNT(*)::bigint FROM sold_variants sv
                INNER JOIN bsale.variants v ON v.bsale_id = sv.variant_id AND v.company_id = %s
                LEFT JOIN bsale.products p ON p.company_id = v.company_id AND p.bsale_id = v.product_id
                LEFT JOIN bsale.product_types pt ON pt.company_id = p.company_id AND pt.bsale_id = p.product_type_id
                WHERE pt.bsale_id IS NULL) AS productos_sin_categoria,
            (SELECT COUNT(*)::bigint FROM sold_variants sv
                INNER JOIN bsale.variants v ON v.bsale_id = sv.variant_id AND v.company_id = %s
                LEFT JOIN bsale.variant_cost vc ON vc.company_id = v.company_id AND vc.variant_id = v.bsale_id
                WHERE COALESCE(vc.average_cost_net, 0) = 0) AS productos_sin_costo,
            (SELECT COUNT(*)::bigint FROM sold_variants sv
                INNER JOIN bsale.variants v ON v.bsale_id = sv.variant_id AND v.company_id = %s
                LEFT JOIN bsale.products_master pm ON pm.barcode = BTRIM(v.bar_code)
                WHERE pm.id IS NULL OR COALESCE(pm.weight_box_kg, 0) = 0) AS productos_sin_peso,
            (SELECT COUNT(*)::bigint FROM sold_variants sv
                INNER JOIN bsale.variants v ON v.bsale_id = sv.variant_id AND v.company_id = %s
                WHERE v.bar_code IS NULL OR BTRIM(v.bar_code) = '') AS variantes_sin_barcode,
            (SELECT COUNT(*)::bigint FROM (
                SELECT c.code
                FROM bsale.clients c
                WHERE c.company_id = %s AND c.code IS NOT NULL AND BTRIM(c.code) <> ''
                GROUP BY c.code
                HAVING COUNT(*) > 1
            ) dup_cli) AS clientes_repetidos
    """
    extra_params: list[Any] = [
        date_from,
        date_to,
        date_from,
        date_to,
        COMPANY_ID,
        OFFICE_ID,
        date_from,
        date_to,
        tuple(ACTIVE_SELLER_IDS),
        date_from,
        date_to,
        COMPANY_ID,
        date_from,
        date_to,
        COMPANY_ID,
        OFFICE_ID,
        COMPANY_ID,
        COMPANY_ID,
        COMPANY_ID,
        COMPANY_ID,
        COMPANY_ID,
        COMPANY_ID,
    ]
    row = session.query_one("validation_auto_audit", audit_sql, bound + tuple(extra_params)) or {}

    def rule(
        rule_id: str,
        label: str,
        count: int,
        *,
        warning_threshold: int = 0,
        explanation_ok: str,
        explanation_issue: str,
    ) -> dict[str, Any]:
        if count == 0:
            return {
                "rule_id": rule_id,
                "label": label,
                "severity": "ok",
                "count": 0,
                "message": explanation_ok,
            }
        severity = "error" if count > warning_threshold else "warning"
        return {
            "rule_id": rule_id,
            "label": label,
            "severity": severity,
            "count": count,
            "message": explanation_issue.format(count=count),
        }

    return [
        rule(
            "duplicate_documents",
            "Documentos duplicados",
            _int(row.get("duplicate_documents")),
            warning_threshold=0,
            explanation_ok="Sin duplicados por número y tipo en el período",
            explanation_issue="{count} grupos de documentos duplicados (mismo número y tipo)",
        ),
        rule(
            "docs_sin_vendedor",
            "Documentos sin vendedor",
            _int(row.get("docs_sin_vendedor")),
            explanation_ok="Todos los documentos tienen vendedor asignado",
            explanation_issue="{count} documentos sin seller_id",
        ),
        rule(
            "sellers_out_scope",
            "Vendedores fuera del scope",
            _int(row.get("sellers_out_scope")),
            explanation_ok="Todos los vendedores están en el scope operativo",
            explanation_issue="{count} documentos con vendedor fuera del scope (89, 80, 85, 59)",
        ),
        rule(
            "wrong_company",
            "Company distinta",
            _int(row.get("wrong_company")),
            explanation_ok="Sin documentos de venta con company_id distinta a 3",
            explanation_issue="{count} documentos de venta con company_id ≠ 3 en el período",
        ),
        rule(
            "wrong_office",
            "Office distinta",
            _int(row.get("wrong_office")),
            explanation_ok="Sin documentos con office_id distinta a 1",
            explanation_issue="{count} documentos con office_id ≠ 1 en el período",
        ),
        rule(
            "nc_sin_documento",
            "NC sin documento",
            _int(row.get("nc_sin_documento")),
            explanation_ok="Todas las NC tienen documento relacionado",
            explanation_issue="{count} notas de crédito sin enlace en document_related",
        ),
        rule(
            "comunas_vacias",
            "Comunas vacías",
            _int(row.get("comunas_vacias")),
            warning_threshold=5,
            explanation_ok="Comunas completas en documentos del período",
            explanation_issue="{count} documentos sin comuna en documento ni cliente",
        ),
        rule(
            "clientes_sin_georef",
            "Clientes sin georef",
            _int(row.get("clientes_sin_georef")),
            warning_threshold=10,
            explanation_ok="Clientes activos con georreferencia operacional",
            explanation_issue="{count} clientes vendidos sin georef en rutero",
        ),
        rule(
            "productos_sin_categoria",
            "Productos sin categoría",
            _int(row.get("productos_sin_categoria")),
            warning_threshold=5,
            explanation_ok="Variantes vendidas con categoría Bsale",
            explanation_issue="{count} variantes vendidas sin product_type",
        ),
        rule(
            "productos_sin_costo",
            "Productos sin costo",
            _int(row.get("productos_sin_costo")),
            warning_threshold=5,
            explanation_ok="Variantes vendidas con costo promedio",
            explanation_issue="{count} variantes vendidas sin costo en variant_cost",
        ),
        rule(
            "productos_sin_peso",
            "Productos sin peso",
            _int(row.get("productos_sin_peso")),
            warning_threshold=5,
            explanation_ok="Productos vendidos con peso en products_master",
            explanation_issue="{count} variantes vendidas sin weight_box_kg en products_master",
        ),
        rule(
            "variantes_sin_barcode",
            "Variantes sin código barras",
            _int(row.get("variantes_sin_barcode")),
            warning_threshold=3,
            explanation_ok="Variantes vendidas con código de barras",
            explanation_issue="{count} variantes vendidas sin bar_code",
        ),
        rule(
            "clientes_repetidos",
            "Clientes repetidos",
            _int(row.get("clientes_repetidos")),
            warning_threshold=0,
            explanation_ok="Sin códigos de cliente duplicados en Bsale",
            explanation_issue="{count} códigos de cliente duplicados en bsale.clients",
        ),
    ]


def _compute_precision(erp_venta: float, bsale_venta: float, abs_doc_delta: float) -> float:
    base = max(abs(bsale_venta), 1.0)
    if abs_doc_delta > 0:
        return round(max(0.0, 100.0 * (1.0 - abs_doc_delta / base)), 2)
    if abs(erp_venta - bsale_venta) <= MONEY_TOLERANCE:
        return 100.0
    return round(max(0.0, 100.0 * (1.0 - abs(erp_venta - bsale_venta) / base)), 2)


def _compute_audit_state(
    *,
    precision: float,
    progress: float,
    commercial_reconciliation: list[dict[str, Any]],
    auto_rules: list[dict[str, Any]],
) -> dict[str, Any]:
    has_commercial_error = any(r["status"] == "error" for r in commercial_reconciliation)
    has_rule_error = any(r["severity"] == "error" for r in auto_rules)
    has_rule_warning = any(r["severity"] == "warning" for r in auto_rules)

    if progress >= 100.0 and precision >= 99.99 and not has_commercial_error and not has_rule_error:
        state = "VALIDATED"
        label = "VALIDADO"
        emoji = "🟢"
    elif precision >= 99.5 and not has_rule_error and not has_commercial_error:
        state = "MINOR_DIFFERENCES"
        label = "DIFERENCIAS MENORES"
        emoji = "🟡"
    elif has_rule_warning and not has_commercial_error and not has_rule_error:
        state = "MINOR_DIFFERENCES"
        label = "DIFERENCIAS MENORES"
        emoji = "🟡"
    else:
        state = "MAJOR_DIFFERENCES"
        label = "DIFERENCIAS IMPORTANTES"
        emoji = "🔴"

    return {
        "state": state,
        "label": label,
        "emoji": emoji,
        "precision_percent": precision,
        "progress_percent": round(progress, 1),
        "progress_label": "Auditoría ERP",
        "validated": progress >= 100.0 and state == "VALIDATED",
    }


def _build_difference_items(
    *,
    commercial_reconciliation: list[dict[str, Any]],
    seller_reconciliation: list[dict[str, Any]],
    client_reconciliation: list[dict[str, Any]],
    daily_reconciliation: list[dict[str, Any]],
    auto_rules: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []

    for row in commercial_reconciliation:
        if row["status"] != "ok":
            items.append({
                "priority": "high" if row["concept"] == "Venta neta" else "medium",
                "type": "Ventas" if "Venta" in row["concept"] or row["concept"] in ("Facturas", "Boletas") else row["concept"],
                "description": f'{row["concept"]} difiere {row["delta"]:,.0f}'.replace(",", "."),
                "impact": abs(_float(row["delta"])),
                "anchor": "cuadratura-comercial",
            })

    diff_sellers = [s for s in seller_reconciliation if s["status"] != "ok"]
    if diff_sellers:
        items.append({
            "priority": "high",
            "type": "Vendedores",
            "description": f"{len(diff_sellers)} vendedor(es) con diferencia ERP vs Bsale",
            "impact": sum(abs(_float(s["delta"])) for s in diff_sellers),
            "anchor": "cuadratura-vendedor",
        })

    if client_reconciliation:
        items.append({
            "priority": "medium",
            "type": "Clientes",
            "description": f"{len(client_reconciliation)} cliente(s) con diferencia",
            "impact": sum(abs(_float(c["delta"])) for c in client_reconciliation),
            "anchor": "cuadratura-cliente",
        })

    diff_days = [d for d in daily_reconciliation if d["status"] != "ok"]
    if diff_days:
        items.append({
            "priority": "high" if len(diff_days) > 3 else "medium",
            "type": "Diario",
            "description": f"{len(diff_days)} día(s) con diferencia de cuadratura",
            "impact": sum(abs(_float(d["delta"])) for d in diff_days),
            "anchor": "cuadratura-diaria",
        })

    for rule in auto_rules:
        if rule["severity"] != "ok":
            items.append({
                "priority": "high" if rule["severity"] == "error" else "medium",
                "type": "Calidad",
                "description": rule["message"],
                "impact": float(rule["count"]),
                "anchor": "revision-automatica",
            })

    priority_order = {"high": 0, "medium": 1, "low": 2}
    items.sort(key=lambda x: (priority_order.get(x["priority"], 9), -x["impact"]))
    return items


def _build_audit_checks(
    curr: dict[str, Any],
    prev: dict[str, Any],
    period_meta: dict[str, Any],
    sellers: list[dict[str, Any]],
    auto_rules: list[dict[str, Any]],
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

    for rule in auto_rules:
        if rule["severity"] == "ok":
            continue
        checks.append({
            "severity": rule["severity"],
            "metric": rule["rule_id"],
            "message": rule["message"],
            "delta_abs": float(rule["count"]),
            "delta_pct": None,
            "possible_cause": rule["label"],
            "seller": None,
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
    erp_cte, erp_params = _erp_base_cte(scope)
    dual_base = list(base_params) + list(erp_params)
    period_params = [f.date_from, f.date_to, prev_from, prev_to, f.date_from, f.date_to]
    reconcile_period = [f.date_from, f.date_to, f.date_from, f.date_to]
    sellers_bound = tuple(base_params + [f.date_from, f.date_to, prev_from, prev_to])
    totals_bound = tuple(dual_base + period_params)
    reconcile_bound = tuple(dual_base + reconcile_period + [tuple(ACTIVE_SELLER_IDS)])
    period_only_bound = tuple(dual_base + reconcile_period)

    totals_sql = f"""
        WITH {base_cte},
        {erp_cte},
        period_curr AS (
            SELECT sb.* FROM sales_base sb
            WHERE sb.sale_day BETWEEN %s AND %s
        ),
        period_prev AS (
            SELECT sb.* FROM sales_base sb
            WHERE sb.sale_day BETWEEN %s AND %s
        ),
        period_erp AS (
            SELECT e.* FROM erp_base e
            WHERE e.sale_day BETWEEN %s AND %s
        ),
        curr AS ({_period_agg_sql("c", "period_curr")}),
        prev AS ({_period_agg_sql("p", "period_prev")}),
        erp AS ({_period_agg_sql("e", "period_erp")})
        SELECT c.*, p.*, e.*
        FROM curr c
        CROSS JOIN prev p
        CROSS JOIN erp e
    """

    reconcile_sql = f"""
        WITH {base_cte},
        {erp_cte},
        period_b AS (
            SELECT sb.* FROM sales_base sb WHERE sb.sale_day BETWEEN %s AND %s
        ),
        period_e AS (
            SELECT e.* FROM erp_base e WHERE e.sale_day BETWEEN %s AND %s
        ),
        merged AS (
            SELECT
                COALESCE(b.document_id, e.document_id) AS document_id,
                COALESCE(b.sale_day, e.sale_day) AS sale_day,
                COALESCE(b.document_type_id, e.document_type_id) AS document_type_id,
                COALESCE(b.client_id, e.client_id) AS client_id,
                COALESCE(b.client_name, e.client_name) AS client_name,
                COALESCE(b.seller_id, e.seller_id) AS seller_id,
                COALESCE(b.seller_name, e.seller_name) AS seller_name,
                COALESCE(e.total_amount_net, 0) AS erp_amount,
                COALESCE(b.total_amount_net, 0) AS bsale_amount,
                e.url_public_view,
                e.number
            FROM period_b b
            FULL OUTER JOIN period_e e ON e.document_id = b.document_id
        ),
        daily AS (
            SELECT
                m.sale_day,
                COALESCE(SUM(m.erp_amount), 0) AS erp,
                COALESCE(SUM(m.bsale_amount), 0) AS bsale,
                COALESCE(SUM(m.erp_amount), 0) - COALESCE(SUM(m.bsale_amount), 0) AS delta
            FROM merged m
            GROUP BY m.sale_day
        ),
        diff_days AS (
            SELECT sale_day FROM daily WHERE ABS(delta) > {MONEY_TOLERANCE}
        )
        SELECT
            'daily' AS section,
            d.sale_day::text AS k1,
            NULL::bigint AS k2,
            NULL::text AS k3,
            d.erp,
            d.bsale,
            d.delta,
            NULL::bigint AS n1,
            NULL::bigint AS n2,
            NULL::bigint AS n3,
            NULL::bigint AS n4,
            NULL::text AS n5
        FROM daily d
        UNION ALL
        SELECT
            'seller' AS section,
            COALESCE(m.seller_id::text, '0') AS k1,
            NULL::bigint AS k2,
            MAX(m.seller_name) AS k3,
            COALESCE(SUM(m.erp_amount), 0),
            COALESCE(SUM(m.bsale_amount), 0),
            COALESCE(SUM(m.erp_amount), 0) - COALESCE(SUM(m.bsale_amount), 0),
            COUNT(DISTINCT m.document_id) FILTER (WHERE m.document_type_id = {DOC_FACTURA}),
            COUNT(DISTINCT m.document_id) FILTER (WHERE m.document_type_id = {DOC_BOLETA}),
            COUNT(DISTINCT m.document_id) FILTER (WHERE m.document_type_id = {DOC_NC}),
            COUNT(DISTINCT m.client_id) FILTER (WHERE m.document_type_id IN ({DOC_BOLETA}, {DOC_FACTURA})),
            COALESCE(
                SUM(CASE WHEN m.document_type_id IN ({DOC_BOLETA}, {DOC_FACTURA}) THEN m.bsale_amount ELSE 0 END)
                / NULLIF(COUNT(*) FILTER (WHERE m.document_type_id IN ({DOC_BOLETA}, {DOC_FACTURA})), 0),
                0
            )::text
        FROM merged m
        WHERE m.seller_id IN %s
        GROUP BY m.seller_id
        UNION ALL
        SELECT
            'client' AS section,
            m.client_id::text AS k1,
            NULL::bigint AS k2,
            MAX(m.client_name) AS k3,
            COALESCE(SUM(m.erp_amount), 0),
            COALESCE(SUM(m.bsale_amount), 0),
            COALESCE(SUM(m.erp_amount), 0) - COALESCE(SUM(m.bsale_amount), 0),
            COUNT(DISTINCT m.document_id),
            NULL::bigint,
            NULL::bigint,
            NULL::bigint,
            MAX(m.sale_day)::text
        FROM merged m
        WHERE m.client_id IS NOT NULL
        GROUP BY m.client_id
        HAVING ABS(COALESCE(SUM(m.erp_amount), 0) - COALESCE(SUM(m.bsale_amount), 0)) > {MONEY_TOLERANCE}
        ORDER BY section, k1, k2
    """

    signed_qty_m = DD_SIGNED_QTY.replace("sb.", "m.")
    signed_amt_m = DD_SIGNED_AMOUNT.replace("sb.", "m.")
    signed_qty_e = DD_SIGNED_QTY.replace("sb.", "e.")
    signed_amt_e = DD_SIGNED_AMOUNT.replace("sb.", "e.")

    product_sql = f"""
        WITH {base_cte},
        {erp_cte},
        period_b AS (
            SELECT sb.* FROM sales_base sb WHERE sb.sale_day BETWEEN %s AND %s
        ),
        period_e AS (
            SELECT e.* FROM erp_base e WHERE e.sale_day BETWEEN %s AND %s
        ),
        erp_lines AS (
            SELECT
                dd.variant_id,
                COALESCE(SUM({signed_qty_e}), 0) AS qty_erp,
                COALESCE(SUM({signed_amt_e}), 0) AS amount_erp
            FROM period_e e
            INNER JOIN distribuidora.document_details dd ON dd.document_id = e.document_id
            GROUP BY dd.variant_id
        ),
        bsale_lines AS (
            SELECT
                dd.variant_id,
                COALESCE(SUM({signed_qty_m.replace("m.", "b.")}), 0) AS qty_bsale,
                COALESCE(SUM({signed_amt_m.replace("m.", "b.")}), 0) AS amount_bsale
            FROM period_b b
            INNER JOIN distribuidora.document_details dd ON dd.document_id = b.document_id
            GROUP BY dd.variant_id
        )
        SELECT
            COALESCE(el.variant_id, bl.variant_id) AS variant_id,
            COALESCE(p.name, v.description, 'Producto ' || COALESCE(el.variant_id, bl.variant_id)::text) AS product_name,
            COALESCE(el.qty_erp, 0) AS qty_erp,
            COALESCE(bl.qty_bsale, 0) AS qty_bsale,
            COALESCE(el.amount_erp, 0) AS amount_erp,
            COALESCE(bl.amount_bsale, 0) AS amount_bsale
        FROM erp_lines el
        FULL OUTER JOIN bsale_lines bl ON bl.variant_id = el.variant_id
        LEFT JOIN bsale.variants v ON v.bsale_id = COALESCE(el.variant_id, bl.variant_id) AND v.company_id = {COMPANY_ID}
        LEFT JOIN bsale.products p ON p.company_id = v.company_id AND p.bsale_id = v.product_id
        WHERE ABS(COALESCE(el.amount_erp, 0) - COALESCE(bl.amount_bsale, 0)) > {MONEY_TOLERANCE}
           OR ABS(COALESCE(el.qty_erp, 0) - COALESCE(bl.qty_bsale, 0)) > 0.001
        ORDER BY ABS(COALESCE(el.amount_erp, 0) - COALESCE(bl.amount_bsale, 0)) DESC
        LIMIT 500
    """

    doc_meta_sql = f"""
        WITH {base_cte},
        {erp_cte},
        period_b AS (
            SELECT sb.* FROM sales_base sb WHERE sb.sale_day BETWEEN %s AND %s
        ),
        period_e AS (
            SELECT e.* FROM erp_base e WHERE e.sale_day BETWEEN %s AND %s
        ),
        merged AS (
            SELECT
                COALESCE(b.document_id, e.document_id) AS document_id,
                COALESCE(b.sale_day, e.sale_day) AS sale_day,
                COALESCE(b.document_type_id, e.document_type_id) AS document_type_id,
                COALESCE(b.client_name, e.client_name) AS client_name,
                COALESCE(b.seller_name, e.seller_name) AS seller_name,
                COALESCE(e.total_amount_net, 0) AS erp_amount,
                COALESCE(b.total_amount_net, 0) AS bsale_amount,
                e.url_public_view,
                e.number
            FROM period_b b
            FULL OUTER JOIN period_e e ON e.document_id = b.document_id
        ),
        daily AS (
            SELECT sale_day, SUM(erp_amount) - SUM(bsale_amount) AS delta
            FROM merged GROUP BY sale_day
        ),
        diff_days AS (SELECT sale_day FROM daily WHERE ABS(delta) > {MONEY_TOLERANCE})
        SELECT
            m.sale_day::text AS sale_day,
            m.document_id,
            m.number,
            m.document_type_id,
            m.client_name,
            m.seller_name,
            m.erp_amount AS erp,
            m.bsale_amount AS bsale,
            m.erp_amount - m.bsale_amount AS delta,
            m.url_public_view
        FROM merged m
        WHERE m.sale_day IN (SELECT sale_day FROM diff_days)
        ORDER BY m.sale_day, m.number NULLS LAST, m.document_id
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
        totals_row = session.query_one("validation_totals", totals_sql, totals_bound) or {}
        seller_rows = session.query_all("validation_sellers", sellers_sql, sellers_bound)
        reconcile_rows = session.query_all("validation_reconcile", reconcile_sql, reconcile_bound)
        product_rows = session.query_all("validation_products", product_sql, period_only_bound)
        doc_rows = session.query_all("validation_doc_details", doc_meta_sql, period_only_bound)
        auto_rules = _build_auto_audit_rules(
            session,
            base_cte=base_cte,
            erp_cte=erp_cte,
            date_from=f.date_from,
            date_to=f.date_to,
            bound=tuple(dual_base),
        )

    curr = _metric_block(totals_row, "c")
    prev = _metric_block(totals_row, "p")
    erp_curr = _metric_block(totals_row, "e")

    daily_reconciliation: list[dict[str, Any]] = []
    seller_reconcile_raw: dict[int, dict[str, Any]] = {}
    client_reconciliation: list[dict[str, Any]] = []

    for r in reconcile_rows:
        section = str(r.get("section") or "")
        if section == "daily":
            delta = round(_float(r.get("delta")), 2)
            daily_reconciliation.append({
                "date": str(r.get("k1")),
                "erp": round(_float(r.get("erp")), 2),
                "bsale": round(_float(r.get("bsale")), 2),
                "delta": delta,
                "status": _reconcile_status(delta),
            })
        elif section == "seller":
            sid = _int(r.get("k1"))
            delta = round(_float(r.get("delta")), 2)
            seller_reconcile_raw[sid] = {
                "seller_id": sid,
                "seller": str(r.get("k3") or SELLER_NAME_BY_ID.get(sid, f"ID {sid}")),
                "facturas": _int(r.get("n1")),
                "boletas": _int(r.get("n2")),
                "notas_credito": _int(r.get("n3")),
                "venta_erp": round(_float(r.get("erp")), 2),
                "venta_bsale": round(_float(r.get("bsale")), 2),
                "delta": delta,
                "clientes": _int(r.get("n4")),
                "ticket": round(_float(r.get("n5")), 2),
                "status": _reconcile_status(delta),
            }
        elif section == "client":
            delta = round(_float(r.get("delta")), 2)
            client_reconciliation.append({
                "client_id": _int(r.get("k1")),
                "client": str(r.get("k3") or ""),
                "erp": round(_float(r.get("erp")), 2),
                "bsale": round(_float(r.get("bsale")), 2),
                "delta": delta,
                "documentos": _int(r.get("n1")),
                "ticket": round(
                    _float(r.get("bsale")) / max(_int(r.get("n1")), 1),
                    2,
                ),
                "ultima_compra": str(r.get("n5") or "") if r.get("n5") else None,
                "status": _reconcile_status(delta),
            })

    daily_reconciliation.sort(key=lambda x: x["date"])

    product_reconciliation: list[dict[str, Any]] = []
    for r in product_rows:
        qty_erp = round(_float(r.get("qty_erp")), 2)
        qty_bsale = round(_float(r.get("qty_bsale")), 2)
        amount_erp = round(_float(r.get("amount_erp")), 2)
        amount_bsale = round(_float(r.get("amount_bsale")), 2)
        delta = round(amount_erp - amount_bsale, 2)
        product_reconciliation.append({
            "variant_id": _int(r.get("variant_id")),
            "product": str(r.get("product_name") or ""),
            "qty_erp": qty_erp,
            "qty_bsale": qty_bsale,
            "amount_erp": amount_erp,
            "amount_bsale": amount_bsale,
            "delta": delta,
            "status": _reconcile_status(delta),
        })

    documents_by_day: dict[str, list[dict[str, Any]]] = {}
    abs_doc_delta = 0.0
    for r in doc_rows:
        sale_day = str(r.get("sale_day") or "")
        erp_amt = round(_float(r.get("erp")), 2)
        bsale_amt = round(_float(r.get("bsale")), 2)
        delta = round(_float(r.get("delta")), 2)
        abs_doc_delta += abs(delta)
        doc_type_id = _int(r.get("document_type_id"))
        documents_by_day.setdefault(sale_day, []).append({
            "document_id": _int(r.get("document_id")),
            "number": r.get("number"),
            "document_type": DOC_TYPE_LABEL.get(doc_type_id, f"Tipo {doc_type_id}"),
            "document_type_id": doc_type_id,
            "client": str(r.get("client_name") or ""),
            "seller": str(r.get("seller_name") or ""),
            "erp": erp_amt,
            "bsale": bsale_amt,
            "delta": delta,
            "status": _reconcile_status(delta),
            "url": r.get("url_public_view"),
        })

    seller_reconciliation: list[dict[str, Any]] = []
    for active in ACTIVE_SELLERS:
        sid = int(active["id"])
        row = seller_reconcile_raw.get(sid, {
            "seller_id": sid,
            "seller": str(active["name"]),
            "facturas": 0,
            "boletas": 0,
            "notas_credito": 0,
            "venta_erp": 0.0,
            "venta_bsale": 0.0,
            "delta": 0.0,
            "clientes": 0,
            "ticket": 0.0,
            "status": "ok",
        })
        row["seller"] = str(active["name"])
        seller_reconciliation.append(row)

    data_coverage = _build_data_coverage(erp_curr, curr)
    commercial_reconciliation = _build_commercial_reconciliation(erp_curr, curr)
    precision = _compute_precision(erp_curr["venta_neta"], curr["venta_neta"], abs_doc_delta)
    ok_rules = sum(1 for r in auto_rules if r["severity"] == "ok")
    progress = (ok_rules / len(auto_rules)) * 100.0 if auto_rules else 0.0
    audit_status = _compute_audit_state(
        precision=precision,
        progress=progress,
        commercial_reconciliation=commercial_reconciliation,
        auto_rules=auto_rules,
    )
    difference_items = _build_difference_items(
        commercial_reconciliation=commercial_reconciliation,
        seller_reconciliation=seller_reconciliation,
        client_reconciliation=client_reconciliation,
        daily_reconciliation=daily_reconciliation,
        auto_rules=auto_rules,
    )

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

    audit_checks = _build_audit_checks(curr, prev, period_meta, seller_distribution, auto_rules)
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
        "audit_status": audit_status,
        "data_coverage": data_coverage,
        "commercial_reconciliation": commercial_reconciliation,
        "seller_reconciliation": seller_reconciliation,
        "daily_reconciliation": daily_reconciliation,
        "documents_by_day": documents_by_day,
        "client_reconciliation": client_reconciliation,
        "product_reconciliation": product_reconciliation,
        "auto_audit_rules": auto_rules,
        "difference_items": difference_items,
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
            "audit_engine_version": AUDIT_VALIDATION_VERSION,
            "sales_scope_version": SALES_SCOPE_VERSION,
            "comparison_method": COMPARISON_METHOD,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "execution_ms": round(execution_ms, 1),
        },
    }
