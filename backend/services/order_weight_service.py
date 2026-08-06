"""Peso oficial de órdenes de compra — cálculo, persistencia y búsqueda."""

from __future__ import annotations

import csv
import io
import logging
import time
from datetime import date, datetime
from decimal import Decimal
from typing import Any, TypedDict

from backend.db import get_connection
from backend.utils.distribuidora_oc_sql import OC_PURCHASE_NOT_INVOICED_BY_RELATED_SQL
from backend.utils.order_weight_calc import (
    aggregate_order_summary,
    compute_line_from_row,
    coverage_semaphore,
    enrich_lines_peso_pct,
)

logger = logging.getLogger(__name__)


class OrderWeightSummary(TypedDict):
    """Fuente única de verdad para métricas de peso de una OC."""

    total_weight: float
    coverage_percent: float
    missing_products: int
    manual_products: int
    automatic_products: int
    estimated_products: int


class SnapshotLineConflictError(RuntimeError):
    """Un mismo detail_id llegó con dos representaciones de peso distintas."""


class LogisticsMatchConflictError(RuntimeError):
    """Una línea de OC tiene más de una coincidencia logística aplicable."""

    def __init__(
        self,
        *,
        variant_id: int | None,
        detail_id: int,
        match_type: str,
        products_master_ids: list[int],
    ) -> None:
        self.variant_id = variant_id
        self.detail_id = detail_id
        self.match_type = match_type
        self.products_master_ids = products_master_ids
        super().__init__(
            "Coincidencia logística ambigua para "
            f"variant_id={variant_id} detail_id={detail_id} "
            f"match_type={match_type} products_master_ids={products_master_ids}"
        )


def metrics_to_order_weight_summary(metrics: dict[str, Any]) -> OrderWeightSummary:
    return {
        "total_weight": float(metrics["total_weight"]),
        "coverage_percent": float(metrics["coverage_percent"]),
        "missing_products": int(metrics["missing_products"]),
        "manual_products": int(metrics["manual_products"]),
        "automatic_products": int(metrics["automatic_products"]),
        "estimated_products": int(metrics["estimated_products"]),
    }


def _log_planning_weight(
    *,
    order_id: int,
    weight_source: str,
    summary: OrderWeightSummary,
) -> None:
    logger.info(
        "[PLANNING_WEIGHT] order_id=%s weight_source=%s total_weight=%s coverage=%s missing=%s",
        order_id,
        weight_source,
        summary["total_weight"],
        summary["coverage_percent"],
        summary["missing_products"],
    )


def _log_popup_weight(*, order_id: int, summary: OrderWeightSummary) -> None:
    logger.info(
        "[POPUP_WEIGHT] order_id=%s total_weight=%s coverage=%s missing=%s",
        order_id,
        summary["total_weight"],
        summary["coverage_percent"],
        summary["missing_products"],
    )


def apply_order_weight_summary_to_row(
    row: dict[str, Any],
    summary: OrderWeightSummary,
    *,
    weight_source: str = "order_weight_summary",
    extras: dict[str, Any] | None = None,
    weight_status: str | None = None,
    weight_reason: str | None = None,
) -> None:
    status = weight_status or "calculated"
    value = summary["total_weight"]
    # No presentar 0 como peso real cuando el cálculo no está disponible.
    if status in {"unavailable", "error"}:
        row["weight_kg"] = None
        row["peso_total_kg"] = None
    else:
        row["weight_kg"] = value
        row["peso_total_kg"] = value
    row["productos_sin_peso"] = summary["missing_products"]
    row["porcentaje_cobertura_peso"] = summary["coverage_percent"]
    row["porcentaje_cobertura"] = summary["coverage_percent"]
    row["productos_manuales"] = summary["manual_products"]
    row["productos_estimados"] = summary["estimated_products"]
    row["peso_fuente"] = weight_source
    row["weight"] = {
        "value_kg": None if status in {"unavailable", "error"} else value,
        "status": status,
        "source": weight_source,
        "reason": weight_reason,
    }
    if extras:
        row.update(extras)


def get_order_weight_summary(
    document_id: int,
    *,
    company_id: int = 3,
    office_id: int = 1,
    user_email: str | None = None,
    persist_cache: bool = False,
) -> OrderWeightSummary | None:
    """Métricas de peso — siempre desde calculate_order_weight (sin snapshot/SQL lateral)."""
    result = calculate_order_weight(
        document_id,
        company_id=company_id,
        office_id=office_id,
        user_email=user_email,
        persist_cache=persist_cache,
    )
    if not result:
        return None
    return metrics_to_order_weight_summary(_build_weight_metrics(result))


def get_order_weight_summaries_batch(
    document_ids: list[int],
    *,
    company_id: int = 3,
    office_id: int = 1,
    user_email: str | None = None,
    persist_cache: bool = True,
    log_planning: bool = True,
) -> dict[int, dict[str, Any]]:
    """
    Batch para planificación: summary + campos auxiliares por OC.
    Errores por orden no abortan el lote completo.
    """
    if not document_ids:
        return {}
    ids = list(dict.fromkeys(int(x) for x in document_ids))
    out: dict[int, dict[str, Any]] = {}
    for doc_id in ids:
        try:
            result = calculate_order_weight(
                doc_id,
                company_id=company_id,
                office_id=office_id,
                user_email=user_email,
                persist_cache=persist_cache,
            )
            if not result:
                logger.warning("[PLANNING_WEIGHT] order_id=%s weight_source=missing_header", doc_id)
                continue
            metrics = _build_weight_metrics(result)
            summary = metrics_to_order_weight_summary(metrics)
            if log_planning:
                _log_planning_weight(
                    order_id=doc_id,
                    weight_source="order_weight_summary",
                    summary=summary,
                )
            out[doc_id] = weight_dict_for_planning(
                metrics,
                result,
                result.get("lines") or [],
            )
        except Exception:
            logger.exception("[PLANNING_WEIGHT] order_id=%s weight_source=error", doc_id)
            # No omitir: el overlay debe marcar unavailable (nunca 0 kg falso).
            out[doc_id] = {
                "total_weight": 0.0,
                "missing_products": 0,
                "coverage_percent": 0.0,
                "manual_products": 0,
                "automatic_products": 0,
                "estimated_products": 0,
                "peso_total_kg": None,
                "weight_kg": None,
                "weight": {
                    "value_kg": None,
                    "status": "unavailable",
                    "source": "product_lines",
                    "reason": "products_load_failed",
                },
                "productos_sin_peso": 0,
                "porcentaje_cobertura_peso": 0.0,
                "porcentaje_cobertura": 0.0,
                "productos_manuales": 0,
                "productos_estimados": 0,
                "productos_totales": 0,
                "cantidad_unidades": 0,
                "cantidad_cajas": 0,
            }
    return out

OC_PURCHASE_INVOICED_BY_RELATED_SQL = """
EXISTS (
    SELECT 1
    FROM distribuidora.document_related dr
    INNER JOIN distribuidora.document_details dd ON dd.detail_id = dr.detail_id
    INNER JOIN distribuidora.documents inv
        ON inv.document_id = dr.related_document_id
       AND inv.document_type_id IN (1, 6)
       AND inv.company_id = d.company_id
       AND inv.office_id = d.office_id
    WHERE dd.document_id = d.document_id
)
""".strip()

_ORDER_HEADER_SQL = """
SELECT
    d.document_id,
    d.number AS oc,
    d.company_id,
    d.office_id,
    d.emission_date,
    d.total_amount,
    co.name AS empresa,
    COALESCE(
        NULLIF(BTRIM(c.nombre_fantasia), ''),
        NULLIF(BTRIM(c.company), ''),
        CONCAT_WS(
            ' ',
            NULLIF(BTRIM(c.first_name), ''),
            NULLIF(BTRIM(c.last_name), '')
        )
    ) AS cliente,
    c.bsale_id AS codigo_cliente,
    COALESCE(
        NULLIF(BTRIM(d.municipality), ''),
        NULLIF(BTRIM(c.municipality), '')
    ) AS comuna
FROM distribuidora.documents d
LEFT JOIN bsale.companies co ON co.company_id = d.company_id
LEFT JOIN bsale.clients c
    ON c.company_id = d.company_id
   AND c.bsale_id = d.client_id
WHERE d.document_id = %s
  AND d.company_id = %s
  AND d.document_type_id = 33
LIMIT 1
"""

_ORDER_LINES_SQL = """
SELECT
    dd.detail_id,
    dd.line_number,
    dd.variant_id,
    NULLIF(BTRIM(dd.variant_code), '') AS codigo,
    NULLIF(BTRIM(dd.variant_description), '') AS producto,
    dd.quantity::numeric AS cantidad_unitaria,
    CASE
        WHEN pl_v.match_count = 1 THEN pl_v.units_per_box
        WHEN pl_b.match_count = 1 THEN pl_b.units_per_box
        ELSE v.units_per_box
    END AS units_per_box,
    CASE
        WHEN pl_v.match_count = 1 THEN pl_v.weight_unit_kg
        WHEN pl_b.match_count = 1 THEN pl_b.weight_unit_kg
    END AS peso_unitario_kg,
    CASE
        WHEN pl_v.match_count = 1 THEN pl_v.weight_box_kg
        WHEN pl_b.match_count = 1 THEN pl_b.weight_box_kg
    END AS peso_caja_kg,
    CASE
        WHEN pl_v.match_count = 1 THEN pl_v.products_master_id
        WHEN pl_b.match_count = 1 THEN pl_b.products_master_id
    END AS products_master_id,
    CASE
        WHEN pl_v.match_count = 1 THEN pl_v.product_name
        WHEN pl_b.match_count = 1 THEN pl_b.product_name
    END AS product_name,
    CASE
        WHEN pl_v.match_count = 1 THEN pl_v.variant_name
        WHEN pl_b.match_count = 1 THEN pl_b.variant_name
    END AS variante,
    p.name AS bsale_product_name,
    CASE
        WHEN pl_v.match_count = 1 THEN pl_v.logistics_completed
        WHEN pl_b.match_count = 1 THEN pl_b.logistics_completed
    END AS logistics_completed,
    CASE
        WHEN pl_v.match_count = 1 THEN pl_v.updated_at
        WHEN pl_b.match_count = 1 THEN pl_b.updated_at
    END AS pm_updated_at,
    CASE
        WHEN pl_v.match_count = 1 THEN pl_v.last_bsale_sync_at
        WHEN pl_b.match_count = 1 THEN pl_b.last_bsale_sync_at
    END AS last_bsale_sync_at,
    CASE
        WHEN pl_v.match_count = 1 THEN pl_v.height_cm
        WHEN pl_b.match_count = 1 THEN pl_b.height_cm
    END AS height_cm,
    CASE
        WHEN pl_v.match_count = 1 THEN pl_v.width_cm
        WHEN pl_b.match_count = 1 THEN pl_b.width_cm
    END AS width_cm,
    CASE
        WHEN pl_v.match_count = 1 THEN pl_v.length_cm
        WHEN pl_b.match_count = 1 THEN pl_b.length_cm
    END AS length_cm,
    v.product_id,
    NULLIF(BTRIM(v.bar_code), '') AS barcode,
    NULLIF(BTRIM(v.code), '') AS codigo_interno,
    (
        pl_v.match_count = 1
        AND pl_v.weight_unit_kg IS NOT NULL
        AND pl_v.weight_unit_kg > 0
    ) AS join_variant_ok,
    (
        pl_v.match_count <> 1
        AND pl_b.match_count = 1
        AND pl_b.weight_unit_kg IS NOT NULL
        AND pl_b.weight_unit_kg > 0
    ) AS join_barcode_ok,
    (
        pl_v.match_count = 1
        OR pl_b.match_count = 1
    ) AS exists_in_pm,
    CASE
        WHEN pl_v.match_count = 1 THEN 'matched_variant'
        WHEN pl_b.match_count = 1 AND COALESCE(pl_v.match_count, 0) > 1
            THEN 'matched_barcode_after_variant_conflict'
        WHEN pl_b.match_count = 1 THEN 'matched_barcode'
        WHEN COALESCE(pl_v.match_count, 0) > 1 THEN 'conflict_variant'
        WHEN COALESCE(pl_b.match_count, 0) > 1 THEN 'conflict_barcode'
        ELSE 'pending'
    END AS logistics_match_status,
    CASE
        WHEN COALESCE(pl_v.match_count, 0) > 1 AND COALESCE(pl_b.match_count, 0) <> 1
            THEN pl_v.products_master_ids
        WHEN COALESCE(pl_v.match_count, 0) <> 1 AND COALESCE(pl_b.match_count, 0) > 1
            THEN pl_b.products_master_ids
        ELSE ARRAY[]::bigint[]
    END AS conflicting_products_master_ids
FROM distribuidora.document_details dd
LEFT JOIN bsale.variants v
    ON v.company_id = %s
   AND v.bsale_id = dd.variant_id
LEFT JOIN bsale.products p
    ON p.company_id = v.company_id
   AND p.bsale_id = v.product_id
LEFT JOIN LATERAL (
    SELECT
        COUNT(*)::integer AS match_count,
        ARRAY_AGG(pl.products_master_id ORDER BY pl.products_master_id)
            AS products_master_ids,
        MIN(pl.products_master_id) AS products_master_id,
        MIN(pl.units_per_box) AS units_per_box,
        MIN(pl.weight_unit_kg) AS weight_unit_kg,
        MIN(pl.weight_box_kg) AS weight_box_kg,
        MIN(pl.product_name) AS product_name,
        MIN(pl.variant_name) AS variant_name,
        BOOL_AND(pl.logistics_completed) AS logistics_completed,
        MAX(pl.updated_at) AS updated_at,
        MAX(pl.last_bsale_sync_at) AS last_bsale_sync_at,
        MIN(pl.height_cm) AS height_cm,
        MIN(pl.width_cm) AS width_cm,
        MIN(pl.length_cm) AS length_cm
    FROM bsale.v_product_logistics pl
    WHERE pl.is_active = TRUE
      AND pl.variant_id = dd.variant_id
) pl_v ON TRUE
LEFT JOIN LATERAL (
    SELECT
        COUNT(*)::integer AS match_count,
        ARRAY_AGG(pl.products_master_id ORDER BY pl.products_master_id)
            AS products_master_ids,
        MIN(pl.products_master_id) AS products_master_id,
        MIN(pl.units_per_box) AS units_per_box,
        MIN(pl.weight_unit_kg) AS weight_unit_kg,
        MIN(pl.weight_box_kg) AS weight_box_kg,
        MIN(pl.product_name) AS product_name,
        MIN(pl.variant_name) AS variant_name,
        BOOL_AND(pl.logistics_completed) AS logistics_completed,
        MAX(pl.updated_at) AS updated_at,
        MAX(pl.last_bsale_sync_at) AS last_bsale_sync_at,
        MIN(pl.height_cm) AS height_cm,
        MIN(pl.width_cm) AS width_cm,
        MIN(pl.length_cm) AS length_cm
    FROM bsale.v_product_logistics pl
    WHERE pl.is_active = TRUE
      AND NULLIF(BTRIM(v.bar_code), '') IS NOT NULL
      AND BTRIM(pl.barcode) = BTRIM(v.bar_code)
) pl_b ON TRUE
WHERE dd.document_id = %s
ORDER BY dd.line_number NULLS LAST, dd.detail_id
"""

_SEARCH_ORDERS_SQL = f"""
SELECT
    d.document_id,
    d.number AS oc,
    d.emission_date,
    d.total_amount,
    co.name AS empresa,
    COALESCE(
        NULLIF(BTRIM(c.nombre_fantasia), ''),
        NULLIF(BTRIM(c.company), ''),
        CONCAT_WS(
            ' ',
            NULLIF(BTRIM(c.first_name), ''),
            NULLIF(BTRIM(c.last_name), '')
        )
    ) AS cliente,
    c.bsale_id AS codigo_cliente,
    COALESCE(
        NULLIF(BTRIM(d.municipality), ''),
        NULLIF(BTRIM(c.municipality), '')
    ) AS comuna,
    ows.peso_total_kg,
    ows.porcentaje_cobertura,
    ows.productos_sin_peso,
    ows.calculated_at AS ultimo_calculo,
    COALESCE(ows.estado_cached, 'pendiente') AS estado,
    (NOT ({OC_PURCHASE_NOT_INVOICED_BY_RELATED_SQL})) AS facturada
FROM distribuidora.v_documents_latest d
LEFT JOIN bsale.companies co ON co.company_id = d.company_id
LEFT JOIN bsale.clients c
    ON c.company_id = d.company_id
   AND c.bsale_id = d.client_id
LEFT JOIN LATERAL (
    SELECT
        s.peso_total_kg,
        s.porcentaje_cobertura,
        s.productos_sin_peso,
        s.calculated_at,
        s.productos_con_peso,
        s.productos_totales,
        CASE
            WHEN s.porcentaje_cobertura >= 100 AND s.peso_total_kg > 0 THEN 'completa'
            ELSE 'incompleta'
        END AS estado_cached
    FROM distribuidora.order_weight_snapshots s
    WHERE s.document_id = d.document_id
    LIMIT 1
) ows ON TRUE
WHERE d.company_id = %s
  AND d.office_id = %s
  AND d.document_type_id = 33
  AND COALESCE(d.state, 0) = 0
  AND (
        %s = 'todas'
        OR (%s = 'pendientes' AND {OC_PURCHASE_NOT_INVOICED_BY_RELATED_SQL})
        OR (%s = 'facturadas' AND {OC_PURCHASE_INVOICED_BY_RELATED_SQL})
  )
  AND (%s IS NULL OR d.number = %s)
  AND (%s IS NULL OR (
        c.nombre_fantasia ILIKE %s
        OR c.company ILIKE %s
        OR CONCAT_WS(' ', c.first_name, c.last_name) ILIKE %s
        OR c.bsale_id::text ILIKE %s
  ))
  AND (%s IS NULL OR c.bsale_id::text ILIKE %s)
  AND (%s IS NULL OR d.emission_date >= %s::date)
  AND (%s IS NULL OR d.emission_date < (%s::date + interval '1 day'))
  AND (%s IS NULL OR COALESCE(ows.estado_cached, 'incompleta') = %s)
ORDER BY
    CASE COALESCE(ows.estado_cached, 'incompleta') WHEN 'incompleta' THEN 0 ELSE 1 END,
    COALESCE(ows.productos_sin_peso, 999) DESC,
    d.number DESC
LIMIT %s
"""

_SNAPSHOT_BY_DOC_SQL = """
SELECT
    id, document_id, peso_total_kg, productos_totales, productos_con_peso,
    productos_sin_peso, productos_manuales, productos_estimados,
    porcentaje_cobertura, calculated_at, calculated_by
FROM distribuidora.order_weight_snapshots
WHERE document_id = %s
LIMIT 1
"""

_SNAPSHOT_LINES_SQL = """
SELECT
    detail_id, line_number, codigo, producto, variante,
    cantidad_unitaria, cantidad_cajas, units_per_box,
    peso_unitario_kg, peso_caja_kg, peso_linea_kg,
    fuente_peso, estado_linea, products_master_id, variant_id, join_debug
FROM distribuidora.order_weight_snapshot_lines
WHERE snapshot_id = %s
ORDER BY line_number NULLS LAST, detail_id
"""

_WEIGHTS_BY_DOCS_SQL = """
SELECT
    ows.document_id,
    ows.peso_total_kg,
    ows.productos_sin_peso,
    ows.porcentaje_cobertura,
    ows.productos_manuales,
    ows.productos_estimados,
    COALESCE(line_agg.cantidad_unidades, 0) AS cantidad_unidades,
    COALESCE(line_agg.cantidad_cajas, 0) AS cantidad_cajas
FROM distribuidora.order_weight_snapshots ows
LEFT JOIN LATERAL (
    SELECT
        SUM(l.cantidad_unitaria) AS cantidad_unidades,
        SUM(COALESCE(l.cantidad_cajas, 0)) AS cantidad_cajas
    FROM distribuidora.order_weight_snapshot_lines l
    WHERE l.snapshot_id = ows.id
) line_agg ON TRUE
WHERE ows.document_id = ANY(%s::bigint[])
"""


def _serialize(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return value


def _row_dict(cur, row: tuple) -> dict[str, Any]:
    cols = [d[0] for d in cur.description]
    return {k: _serialize(v) for k, v in zip(cols, row)}


def _table_exists(cur, table: str) -> bool:
    cur.execute(
        """
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'distribuidora' AND table_name = %s
        )
        """,
        (table,),
    )
    return bool(cur.fetchone()[0])


def compute_order_lines(cur, *, document_id: int, company_id: int) -> list[dict[str, Any]]:
    cur.execute(_ORDER_LINES_SQL, (company_id, document_id))
    rows = [_row_dict(cur, row) for row in cur.fetchall()]
    lines: list[dict[str, Any]] = []
    for row in rows:
        match_status = row.get("logistics_match_status")
        line_warnings: list[str] = []
        if match_status in {"conflict_variant", "conflict_barcode"}:
            products_master_ids = [
                int(value)
                for value in (row.get("conflicting_products_master_ids") or [])
            ]
            match_type = "variant_id" if match_status == "conflict_variant" else "barcode"
            logger.warning(
                "order_weight_logistics_match_conflict_soft "
                "variant_id=%s detail_id=%s match_type=%s products_master_ids=%s "
                "action=continue_line_without_weight",
                row.get("variant_id"),
                row.get("detail_id"),
                match_type,
                products_master_ids,
            )
            # No abortar el pedido completo: la línea sigue visible sin peso.
            row = dict(row)
            row["peso_unitario_kg"] = None
            row["peso_caja_kg"] = None
            row["products_master_id"] = None
            row["join_variant_ok"] = False
            row["join_barcode_ok"] = False
            row["exists_in_pm"] = False
            line_warnings.append(
                f"logistics_match_conflict:{match_type}:{products_master_ids}"
            )
        elif match_status == "matched_barcode_after_variant_conflict":
            logger.info(
                "order_weight_variant_conflict_resolved_by_barcode "
                "variant_id=%s detail_id=%s products_master_id=%s",
                row.get("variant_id"),
                row.get("detail_id"),
                row.get("products_master_id"),
            )
            line_warnings.append("variant_conflict_resolved_by_barcode")
        line = compute_line_from_row(row)
        if line_warnings:
            line["warnings"] = line_warnings
        lines.append(line)
    deduplicated, duplicate_counts = _deduplicate_snapshot_lines(
        lines,
        snapshot_id=None,
    )
    for detail_id, count in sorted(duplicate_counts.items()):
        logger.warning(
            "order_weight_line_deduplicated detail_id=%s occurrences=%s",
            detail_id,
            count,
        )
    return deduplicated


def build_weight_payload(summary: dict[str, Any], *, lines: list[dict[str, Any]]) -> dict[str, Any]:
    """Contrato de peso: distingue 0 real vs no calculable."""
    total_lines = int(summary.get("productos_totales") or 0)
    missing = int(summary.get("productos_sin_peso") or 0)
    peso = summary.get("peso_total_kg")
    peso_f = float(peso) if peso is not None else None
    has_conflict = any(
        "logistics_match_conflict" in (ln.get("warnings") or [])
        for ln in lines
    )
    if total_lines <= 0:
        status = "unavailable"
        reason = "no_product_lines"
        value = None
    elif missing == 0 and peso_f is not None:
        status = "calculated"
        reason = None
        value = peso_f
    elif missing > 0 and (peso_f or 0) > 0:
        status = "partial"
        reason = "missing_unit_weights"
        value = peso_f
    elif has_conflict and (peso_f or 0) == 0:
        status = "partial" if total_lines > missing else "unavailable"
        reason = "logistics_match_conflict"
        value = peso_f if (peso_f or 0) > 0 else None
    elif missing == total_lines:
        status = "unavailable"
        reason = "all_lines_without_weight"
        value = None
    else:
        status = "calculated"
        reason = None
        value = peso_f
    return {
        "value_kg": value,
        "status": status,
        "source": "product_lines",
        "reason": reason,
        "lines_total": total_lines,
        "lines_missing_weight": missing,
    }

def _build_weight_metrics(summary: dict[str, Any]) -> dict[str, Any]:
    automatic = max(
        0,
        int(summary.get("productos_con_peso") or 0)
        - int(summary.get("productos_manuales") or 0)
        - int(summary.get("productos_estimados") or 0),
    )
    return {
        "total_weight": float(summary.get("peso_total_kg") or 0),
        "missing_products": int(summary.get("productos_sin_peso") or 0),
        "coverage_percent": float(summary.get("porcentaje_cobertura") or 0),
        "manual_products": int(summary.get("productos_manuales") or 0),
        "automatic_products": automatic,
        "estimated_products": int(summary.get("productos_estimados") or 0),
    }


def weight_dict_for_planning(
    metrics: dict[str, Any],
    summary: dict[str, Any],
    lines: list[dict[str, Any]],
) -> dict[str, Any]:
    unidades = sum(float(ln.get("cantidad_unitaria") or 0) for ln in lines)
    cajas = sum(float(ln.get("cantidad_cajas") or 0) for ln in lines)
    weight = summary.get("weight")
    if not isinstance(weight, dict):
        weight = build_weight_payload(summary, lines=lines)
    value = weight.get("value_kg")
    return {
        **metrics,
        "peso_total_kg": value,
        "weight_kg": value,
        "weight": weight,
        "productos_sin_peso": metrics["missing_products"],
        "porcentaje_cobertura_peso": metrics["coverage_percent"],
        "porcentaje_cobertura": metrics["coverage_percent"],
        "productos_manuales": metrics["manual_products"],
        "productos_estimados": metrics["estimated_products"],
        "productos_totales": int(summary.get("productos_totales") or 0),
        "cantidad_unidades": unidades,
        "cantidad_cajas": cajas,
    }


def _log_order_weight(
    *,
    order_id: int,
    old_weight: float | None,
    new_weight: float,
    coverage: float,
    missing_products: int,
    calculation_ms: float,
) -> None:
    logger.info(
        "[ORDER_WEIGHT] order_id=%s old_weight=%s new_weight=%s coverage=%s "
        "missing_products=%s calculation_ms=%.1f",
        order_id,
        old_weight,
        new_weight,
        coverage,
        missing_products,
        calculation_ms,
    )


def _snapshot_weight_only(cur, document_id: int) -> float | None:
    if not _table_exists(cur, "order_weight_snapshots"):
        return None
    cur.execute(
        "SELECT peso_total_kg FROM distribuidora.order_weight_snapshots WHERE document_id = %s",
        (int(document_id),),
    )
    row = cur.fetchone()
    if not row or row[0] is None:
        return None
    return float(row[0])


def invalidate_order_weight_cache(document_id: int) -> None:
    """Invalida snapshot cacheado (p. ej. tras sync o cambio logístico)."""
    invalidate_order_weight_cache_batch([int(document_id)])


def invalidate_order_weight_cache_batch(document_ids: list[int]) -> int:
    """Elimina snapshots cacheados para las OCs indicadas."""
    ids = list(dict.fromkeys(int(x) for x in document_ids if x))
    if not ids:
        return 0
    conn = get_connection()
    try:
        cur = conn.cursor()
        if not _table_exists(cur, "order_weight_snapshots"):
            cur.close()
            return 0
        cur.execute(
            "DELETE FROM distribuidora.order_weight_snapshots WHERE document_id = ANY(%s::bigint[])",
            (ids,),
        )
        deleted = int(cur.rowcount or 0)
        conn.commit()
        cur.close()
        if deleted:
            logger.info("[ORDER_WEIGHT] cache_invalidated count=%s document_ids=%s", deleted, ids)
        return deleted
    finally:
        conn.close()


def invalidate_order_weight_cache_for_products_master(products_master_id: int) -> int:
    """Invalida snapshots de OCs que incluyen el producto logístico."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        if not _table_exists(cur, "order_weight_snapshots"):
            cur.close()
            return 0
        cur.execute(
            """
            DELETE FROM distribuidora.order_weight_snapshots ows
            WHERE ows.document_id IN (
                SELECT DISTINCT dd.document_id
                FROM distribuidora.document_details dd
                INNER JOIN distribuidora.documents d
                    ON d.document_id = dd.document_id
                   AND d.document_type_id = 33
                LEFT JOIN bsale.variants v
                    ON v.bsale_id = dd.variant_id
                   AND v.company_id = d.company_id
                INNER JOIN bsale.products_master pm ON pm.id = %s
                WHERE (
                    pm.variant_id IS NOT NULL
                    AND pm.variant_id = dd.variant_id
                ) OR (
                    pm.barcode IS NOT NULL
                    AND NULLIF(BTRIM(v.bar_code), '') IS NOT NULL
                    AND pm.barcode = BTRIM(v.bar_code)
                )
            )
            """,
            (int(products_master_id),),
        )
        deleted = int(cur.rowcount or 0)
        conn.commit()
        cur.close()
        if deleted:
            logger.info(
                "[ORDER_WEIGHT] cache_invalidated products_master_id=%s count=%s",
                products_master_id,
                deleted,
            )
        return deleted
    finally:
        conn.close()


def calculate_order_weight(
    document_id: int,
    *,
    company_id: int = 3,
    office_id: int = 1,
    user_email: str | None = None,
    persist_cache: bool = False,
) -> dict[str, Any]:
    """
    Calcula peso real desde document_details + logística (manual > automático).
    Nunca lee snapshot cacheado — siempre recalcula desde fuente.
    """
    t0 = time.perf_counter()
    conn = get_connection()
    try:
        cur = conn.cursor()
        can_persist = persist_cache and _table_exists(cur, "order_weight_snapshots")
        old_weight = _snapshot_weight_only(cur, int(document_id)) if can_persist else None

        cur.execute(_ORDER_HEADER_SQL, (document_id, company_id))
        header_row = cur.fetchone()
        if not header_row:
            cur.close()
            return {}
        header = _row_dict(cur, header_row)
        if header.get("office_id") is None:
            header["office_id"] = office_id

        lines = compute_order_lines(cur, document_id=document_id, company_id=company_id)
        summary = aggregate_order_summary(lines)
        enrich_lines_peso_pct(lines, summary["peso_total_kg"])

        calculated_at = None
        calculated_by = user_email
        if can_persist:
            _persist_snapshot(
                cur,
                header=header,
                lines=lines,
                summary=summary,
                user_email=user_email,
            )
            conn.commit()
            cur.execute(_SNAPSHOT_BY_DOC_SQL, (document_id,))
            snap = cur.fetchone()
            if snap:
                calculated_at = _serialize(snap[9])
                calculated_by = snap[10]
        metrics = _build_weight_metrics(summary)
        semaforo = coverage_semaphore(summary["porcentaje_cobertura"])
        cur.close()
    finally:
        conn.close()

    calculation_ms = (time.perf_counter() - t0) * 1000
    new_weight = float(metrics["total_weight"])
    _log_order_weight(
        order_id=int(document_id),
        old_weight=old_weight,
        new_weight=new_weight,
        coverage=float(metrics["coverage_percent"]),
        missing_products=int(metrics["missing_products"]),
        calculation_ms=calculation_ms,
    )

    estado = _order_estado_label(summary["porcentaje_cobertura"], summary["peso_total_kg"])
    weight = build_weight_payload(summary, lines=lines)
    return {
        **header,
        **summary,
        **metrics,
        "peso_total_kg": new_weight if weight["value_kg"] is not None else None,
        "weight": weight,
        "estado": estado,
        "semaforo": semaforo,
        "ultimo_calculo": calculated_at or datetime.utcnow().isoformat(),
        "calculated_by": calculated_by,
        "calculation_ms": round(calculation_ms, 1),
        "lines": lines,
    }


def recalculate_order_weight_in_transaction(
    cur,
    *,
    document_id: int,
    company_id: int = 3,
    office_id: int = 1,
    user_email: str | None = None,
    persist: bool = True,
) -> dict[str, Any]:
    """Recalcula y persiste el snapshot usando la transacción/cursor del caller.

    Se usa cuando el mismo TX acaba de reemplazar ``document_details``: el cálculo
    ve esas líneas aún no confirmadas y evita abrir conexiones laterales.
    No hace ``commit`` ni ``rollback``.
    """
    cur.execute(_ORDER_HEADER_SQL, (int(document_id), int(company_id)))
    header_row = cur.fetchone()
    if not header_row:
        return {}
    header = _row_dict(cur, header_row)
    if header.get("office_id") is None:
        header["office_id"] = int(office_id)

    lines = compute_order_lines(
        cur,
        document_id=int(document_id),
        company_id=int(company_id),
    )
    summary = aggregate_order_summary(lines)
    enrich_lines_peso_pct(lines, summary["peso_total_kg"])
    if persist and _table_exists(cur, "order_weight_snapshots"):
        _persist_snapshot(
            cur,
            header=header,
            lines=lines,
            summary=summary,
            user_email=user_email,
        )
    metrics = _build_weight_metrics(summary)

    weight = build_weight_payload(summary, lines=lines)
    return {
        **header,
        **summary,
        **metrics,
        "peso_total_kg": (
            float(metrics["total_weight"]) if weight["value_kg"] is not None else None
        ),
        "weight": weight,
        "estado": _order_estado_label(
            float(summary["porcentaje_cobertura"]),
            float(summary["peso_total_kg"]),
        ),
        "semaforo": coverage_semaphore(float(summary["porcentaje_cobertura"])),
        "lines": lines,
    }


def calculate_order_weights_batch(
    document_ids: list[int],
    *,
    company_id: int = 3,
    office_id: int = 1,
    user_email: str | None = None,
    persist_cache: bool = True,
) -> dict[int, dict[str, Any]]:
    """Recalcula peso real para varias OCs (planificación / live refresh)."""
    return get_order_weight_summaries_batch(
        document_ids,
        company_id=company_id,
        office_id=office_id,
        user_email=user_email,
        persist_cache=persist_cache,
        log_planning=True,
    )


_SNAPSHOT_LINE_FIELDS = (
    "line_number",
    "codigo",
    "producto",
    "variante",
    "cantidad_unitaria",
    "cantidad_cajas",
    "units_per_box",
    "peso_unitario_kg",
    "peso_caja_kg",
    "peso_linea_kg",
    "fuente_peso",
    "estado_linea",
    "products_master_id",
    "variant_id",
    "join_debug",
)


def _snapshot_comparable(value: Any) -> Any:
    if isinstance(value, Decimal):
        return ("number", value.normalize())
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return ("number", Decimal(str(value)).normalize())
    if isinstance(value, dict):
        return tuple(
            sorted((str(key), _snapshot_comparable(item)) for key, item in value.items())
        )
    if isinstance(value, (list, tuple)):
        return tuple(_snapshot_comparable(item) for item in value)
    return value


def _deduplicate_snapshot_lines(
    lines: list[dict[str, Any]],
    *,
    snapshot_id: int | None,
) -> tuple[list[dict[str, Any]], dict[int, int]]:
    """Deduplica iguales y aborta si un detail_id tiene valores divergentes."""
    by_detail: dict[int, list[tuple[tuple[Any, ...], dict[str, Any]]]] = {}
    output: list[dict[str, Any]] = []
    for line in lines:
        raw_detail_id = line.get("detail_id")
        if raw_detail_id is None:
            output.append(line)
            continue
        detail_id = int(raw_detail_id)
        signature = tuple(
            _snapshot_comparable(line.get(field)) for field in _SNAPSHOT_LINE_FIELDS
        )
        group = by_detail.setdefault(detail_id, [])
        if not group:
            output.append(line)
        group.append((signature, line))

    duplicates: dict[int, int] = {}
    for detail_id, group in by_detail.items():
        if len(group) <= 1:
            continue
        duplicates[detail_id] = len(group)
        first_signature = group[0][0]
        if any(signature != first_signature for signature, _line in group[1:]):
            logger.error(
                "snapshot_line_conflict snapshot_id=%s detail_id=%s occurrences=%s",
                snapshot_id,
                detail_id,
                len(group),
            )
            raise SnapshotLineConflictError(
                "Líneas de peso conflictivas para "
                f"snapshot_id={snapshot_id} detail_id={detail_id} "
                f"occurrences={len(group)}"
            )
    return output, duplicates


def _persist_snapshot(
    cur,
    *,
    header: dict[str, Any],
    lines: list[dict[str, Any]],
    summary: dict[str, Any],
    user_email: str | None,
) -> int:
    old_weight = None
    cur.execute(_SNAPSHOT_BY_DOC_SQL, (int(header["document_id"]),))
    prev = cur.fetchone()
    previous_snapshot_id = int(prev[0]) if prev else None
    if prev:
        old_weight = float(prev[2]) if prev[2] is not None else None

    deduplicated, duplicate_counts = _deduplicate_snapshot_lines(
        lines,
        snapshot_id=previous_snapshot_id,
    )
    if len(deduplicated) != len(lines):
        lines[:] = deduplicated
        corrected_summary = aggregate_order_summary(lines)
        enrich_lines_peso_pct(lines, corrected_summary["peso_total_kg"])
        summary.clear()
        summary.update(corrected_summary)

    cur.execute(
        """
        INSERT INTO distribuidora.order_weight_snapshots (
            document_id, company_id, office_id, oc_number,
            peso_total_kg, productos_totales, productos_con_peso, productos_sin_peso,
            productos_manuales, productos_estimados, porcentaje_cobertura,
            calculated_at, calculated_by, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s, NOW())
        ON CONFLICT (document_id) DO UPDATE SET
            company_id = EXCLUDED.company_id,
            office_id = EXCLUDED.office_id,
            oc_number = EXCLUDED.oc_number,
            peso_total_kg = EXCLUDED.peso_total_kg,
            productos_totales = EXCLUDED.productos_totales,
            productos_con_peso = EXCLUDED.productos_con_peso,
            productos_sin_peso = EXCLUDED.productos_sin_peso,
            productos_manuales = EXCLUDED.productos_manuales,
            productos_estimados = EXCLUDED.productos_estimados,
            porcentaje_cobertura = EXCLUDED.porcentaje_cobertura,
            calculated_at = NOW(),
            calculated_by = EXCLUDED.calculated_by,
            updated_at = NOW()
        RETURNING id
        """,
        (
            int(header["document_id"]),
            int(header["company_id"]),
            int(header.get("office_id") or 1),
            int(header["oc"]) if header.get("oc") is not None else None,
            summary["peso_total_kg"],
            summary["productos_totales"],
            summary["productos_con_peso"],
            summary["productos_sin_peso"],
            summary["productos_manuales"],
            summary["productos_estimados"],
            summary["porcentaje_cobertura"],
            user_email,
        ),
    )
    snapshot_id = int(cur.fetchone()[0])
    for detail_id, count in sorted(duplicate_counts.items()):
        logger.warning(
            "snapshot_line_deduplicated snapshot_id=%s detail_id=%s occurrences=%s",
            snapshot_id,
            detail_id,
            count,
        )
    cur.execute(
        "DELETE FROM distribuidora.order_weight_snapshot_lines WHERE snapshot_id = %s",
        (snapshot_id,),
    )
    for ln in lines:
        cur.execute(
            """
            INSERT INTO distribuidora.order_weight_snapshot_lines (
                snapshot_id, detail_id, line_number, codigo, producto, variante,
                cantidad_unitaria, cantidad_cajas, units_per_box,
                peso_unitario_kg, peso_caja_kg, peso_linea_kg,
                fuente_peso, estado_linea, products_master_id, variant_id, join_debug
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb
            )
            """,
            (
                snapshot_id,
                ln["detail_id"],
                ln.get("line_number"),
                ln.get("codigo"),
                ln.get("producto"),
                ln.get("variante"),
                ln.get("cantidad_unitaria"),
                ln.get("cantidad_cajas"),
                ln.get("units_per_box"),
                ln.get("peso_unitario_kg"),
                ln.get("peso_caja_kg"),
                ln.get("peso_linea_kg"),
                ln.get("fuente_peso"),
                ln.get("estado_linea"),
                ln.get("products_master_id"),
                ln.get("variant_id"),
                __import__("json").dumps(ln.get("join_debug") or {}),
            ),
        )

    new_weight = summary["peso_total_kg"]
    if old_weight is None or abs(old_weight - new_weight) > 0.001:
        cur.execute(
            """
            INSERT INTO distribuidora.order_weight_history (
                document_id, user_email, peso_anterior_kg, peso_nuevo_kg, productos_modificados
            ) VALUES (%s, %s, %s, %s, %s)
            """,
            (
                int(header["document_id"]),
                user_email,
                old_weight,
                new_weight,
                summary.get("productos_sin_peso", 0),
            ),
        )
    return snapshot_id


def recalculate_order_weight(
    *,
    document_id: int,
    company_id: int = 3,
    office_id: int = 1,
    user_email: str | None = None,
    persist: bool = True,
) -> dict[str, Any]:
    return calculate_order_weight(
        document_id,
        company_id=company_id,
        office_id=office_id,
        user_email=user_email,
        persist_cache=persist,
    )


def get_order_weight(
    *,
    document_id: int,
    company_id: int = 3,
    office_id: int = 1,
    line_filter: str | None = None,
    use_cache: bool = True,
) -> dict[str, Any]:
    """Detalle de peso para popup — misma fuente que planificación (calculate_order_weight)."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        has_snapshots = _table_exists(cur, "order_weight_snapshots")
        cur.close()
    finally:
        conn.close()

    result = calculate_order_weight(
        document_id,
        company_id=company_id,
        office_id=office_id,
        persist_cache=use_cache and has_snapshots,
    )
    if not result:
        return {}

    summary = metrics_to_order_weight_summary(_build_weight_metrics(result))
    _log_popup_weight(order_id=int(document_id), summary=summary)

    all_lines = result.get("lines") or []
    if line_filter and line_filter != "all":
        result = dict(result)
        result["lines"] = [ln for ln in all_lines if ln.get("estado_linea") == line_filter]
    else:
        result["lines"] = all_lines

    result["semaforo"] = coverage_semaphore(float(result.get("porcentaje_cobertura") or 0))
    result["estado"] = _order_estado_label(
        float(result.get("porcentaje_cobertura") or 0),
        float(result.get("peso_total_kg") or 0),
    )
    result["ultimo_calculo"] = result.get("calculated_at") or result.get("ultimo_calculo")
    return result


def _order_estado_label(porcentaje: float, peso_total: float) -> str:
    if porcentaje >= 100 and peso_total > 0:
        return "completa"
    return "incompleta"


def search_orders(
    *,
    company_id: int = 3,
    office_id: int = 1,
    oc: int | None = None,
    cliente: str | None = None,
    codigo_cliente: str | None = None,
    date_from: date | None = None,
    date_to: date | None = None,
    estado: str | None = None,
    billing_filter: str = "pendientes",
    limit: int = 150,
) -> list[dict[str, Any]]:
    cliente_term = f"%{cliente.strip()}%" if cliente and cliente.strip() else None
    codigo_term = f"%{codigo_cliente.strip()}%" if codigo_cliente and codigo_cliente.strip() else None
    billing = (billing_filter or "pendientes").strip().lower()
    if billing not in ("pendientes", "facturadas", "todas"):
        billing = "pendientes"
    estado_norm = None
    if estado and estado.strip().lower() in ("completa", "incompleta"):
        estado_norm = estado.strip().lower()

    conn = get_connection()
    try:
        cur = conn.cursor()
        if not _table_exists(cur, "order_weight_snapshots"):
            pendientes_only = billing == "pendientes"
            facturadas_only = billing == "facturadas"
            billing_sql = "TRUE"
            if pendientes_only:
                billing_sql = OC_PURCHASE_NOT_INVOICED_BY_RELATED_SQL
            elif facturadas_only:
                billing_sql = OC_PURCHASE_INVOICED_BY_RELATED_SQL
            cur.execute(
                f"""
                SELECT d.document_id, d.number AS oc, d.emission_date, d.total_amount,
                       co.name AS empresa,
                       COALESCE(NULLIF(BTRIM(c.nombre_fantasia), ''), NULLIF(BTRIM(c.company), '')) AS cliente,
                       c.bsale_id AS codigo_cliente,
                       COALESCE(NULLIF(BTRIM(d.municipality), ''), NULLIF(BTRIM(c.municipality), '')) AS comuna,
                       NULL::numeric AS peso_total_kg,
                       NULL::numeric AS porcentaje_cobertura,
                       NULL::int AS productos_sin_peso,
                       NULL::timestamptz AS ultimo_calculo,
                       'incompleta' AS estado,
                       (NOT ({OC_PURCHASE_NOT_INVOICED_BY_RELATED_SQL})) AS facturada
                FROM distribuidora.v_documents_latest d
                LEFT JOIN bsale.companies co ON co.company_id = d.company_id
                LEFT JOIN bsale.clients c ON c.company_id = d.company_id AND c.bsale_id = d.client_id
                WHERE d.company_id = %s AND d.office_id = %s AND d.document_type_id = 33
                  AND ({billing_sql})
                  AND (%s IS NULL OR d.number = %s)
                ORDER BY d.number DESC LIMIT %s
                """,
                (company_id, office_id, oc, oc, limit),
            )
        else:
            cur.execute(
                _SEARCH_ORDERS_SQL,
                (
                    company_id,
                    office_id,
                    billing,
                    billing,
                    billing,
                    oc,
                    oc,
                    cliente_term,
                    cliente_term,
                    cliente_term,
                    cliente_term,
                    cliente_term,
                    codigo_term,
                    codigo_term,
                    date_from,
                    date_from,
                    date_to,
                    date_to,
                    estado_norm,
                    estado_norm,
                    limit,
                ),
            )
        rows = [_row_dict(cur, r) for r in cur.fetchall()]
        for row in rows:
            if not row.get("estado"):
                row["estado"] = _order_estado_label(
                    float(row.get("porcentaje_cobertura") or 0),
                    float(row.get("peso_total_kg") or 0),
                )
        cur.close()
        return rows
    finally:
        conn.close()


def ensure_order_weights(
    document_ids: list[int],
    *,
    company_id: int = 3,
    office_id: int = 1,
    user_email: str | None = None,
) -> dict[int, dict[str, Any]]:
    """Recalcula peso real para planificación (nunca usa snapshot sin recalcular)."""
    return calculate_order_weights_batch(
        document_ids,
        company_id=company_id,
        office_id=office_id,
        user_email=user_email,
        persist_cache=True,
    )


def fetch_weights_by_document_ids(document_ids: list[int]) -> dict[int, dict[str, Any]]:
    if not document_ids:
        return {}
    conn = get_connection()
    try:
        cur = conn.cursor()
        if not _table_exists(cur, "order_weight_snapshots"):
            cur.close()
            return {}
        cur.execute(_WEIGHTS_BY_DOCS_SQL, (list(dict.fromkeys(int(x) for x in document_ids)),))
        out: dict[int, dict[str, Any]] = {}
        for row in cur.fetchall():
            out[int(row[0])] = {
                "peso_total_kg": float(row[1] or 0),
                "productos_sin_peso": int(row[2] or 0),
                "porcentaje_cobertura_peso": float(row[3] or 0),
                "weight_kg": float(row[1] or 0),
                "productos_manuales": int(row[4] or 0),
                "productos_estimados": int(row[5] or 0),
                "cantidad_unidades": float(row[6] or 0),
                "cantidad_cajas": float(row[7] or 0),
            }
        cur.close()
        return out
    finally:
        conn.close()


def create_logistics_from_variant(
    *,
    variant_id: int,
    company_id: int = 3,
) -> dict[str, Any]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO bsale.products_master (
                barcode, sku, product_id, variant_id, product_name, variant_name,
                product_type, companies, units_per_box, is_active, created_at, updated_at, last_bsale_sync_at
            )
            SELECT
                NULLIF(BTRIM(v.bar_code), ''),
                NULLIF(BTRIM(v.code), ''),
                v.product_id,
                v.bsale_id,
                p.name,
                v.description,
                pt.name,
                jsonb_build_array(v.company_id),
                NULLIF(v.units_per_box, 0),
                TRUE, NOW(), NOW(), NOW()
            FROM bsale.variants v
            LEFT JOIN bsale.products p
                ON p.company_id = v.company_id AND p.bsale_id = v.product_id
            LEFT JOIN bsale.product_types pt
                ON pt.company_id = p.company_id AND pt.bsale_id = p.product_type_id
            WHERE v.company_id = %s AND v.bsale_id = %s
              AND NULLIF(BTRIM(v.bar_code), '') IS NOT NULL
            ON CONFLICT (barcode) DO UPDATE SET
                variant_id = EXCLUDED.variant_id,
                product_name = EXCLUDED.product_name,
                variant_name = EXCLUDED.variant_name,
                updated_at = NOW()
            RETURNING id, barcode, variant_id, product_name, variant_name
            """,
            (company_id, variant_id),
        )
        row = cur.fetchone()
        if not row:
            conn.rollback()
            raise ValueError("Variante sin barcode — no se puede crear ficha automática")
        conn.commit()
        cols = [d[0] for d in cur.description]
        cur.close()
        return dict(zip(cols, row))
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def export_order_csv(*, document_id: int, company_id: int = 3) -> str:
    data = get_order_weight(document_id=document_id, company_id=company_id, use_cache=False)
    buf = io.StringIO()
    writer = csv.writer(buf, delimiter=";")
    writer.writerow(
        [
            "Código",
            "Producto",
            "Variante",
            "Cantidad unitaria",
            "Cantidad cajas",
            "Peso unitario kg",
            "Peso caja kg",
            "Peso línea kg",
            "Estado",
            "Fuente",
        ]
    )
    for ln in data.get("lines") or []:
        writer.writerow(
            [
                ln.get("codigo") or "",
                ln.get("producto") or "",
                ln.get("variante") or "",
                ln.get("cantidad_unitaria") or 0,
                ln.get("cantidad_cajas") or "",
                ln.get("peso_unitario_kg") or "",
                ln.get("peso_caja_kg") or "",
                ln.get("peso_linea_kg") or 0,
                ln.get("estado_linea") or "",
                ln.get("fuente_peso") or "",
            ]
        )
    return buf.getvalue()


def get_order_history(document_id: int, *, limit: int = 20) -> list[dict[str, Any]]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        if not _table_exists(cur, "order_weight_history"):
            cur.close()
            return []
        cur.execute(
            """
            SELECT user_email, peso_anterior_kg, peso_nuevo_kg, productos_modificados, created_at
            FROM distribuidora.order_weight_history
            WHERE document_id = %s
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (document_id, limit),
        )
        rows = [_row_dict(cur, r) for r in cur.fetchall()]
        cur.close()
        return rows
    finally:
        conn.close()


def recalculate_orders_batch(
    *,
    document_ids: list[int],
    company_id: int = 3,
    office_id: int = 1,
    user_email: str | None = None,
    plan_session_id: str | None = None,
    motivo: str | None = None,
) -> dict[str, Any]:
    ids = list(dict.fromkeys(int(x) for x in document_ids if x))
    if not ids:
        return {"recalculated": 0, "peso_anterior_kg": 0.0, "peso_nuevo_kg": 0.0, "items": []}

    before = fetch_weights_by_document_ids(ids)
    peso_antes = sum(float(w.get("peso_total_kg") or 0) for w in before.values())

    after_map = calculate_order_weights_batch(
        ids,
        company_id=company_id,
        office_id=office_id,
        user_email=user_email,
        persist_cache=True,
    )
    items = [
        {
            "document_id": doc_id,
            "peso_total_kg": w.get("peso_total_kg"),
            "porcentaje_cobertura": w.get("porcentaje_cobertura_peso"),
        }
        for doc_id, w in after_map.items()
    ]
    peso_nuevo = sum(float(w.get("peso_total_kg") or 0) for w in after_map.values())

    if plan_session_id or motivo:
        conn = get_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = 'distribuidora'
                      AND table_name = 'dispatch_plan_weight_audit'
                )
                """
            )
            if cur.fetchone()[0]:
                from backend.repositories.distribuidora import dispatch_plan_repo as dp_repo

                dp_repo.insert_plan_weight_audit(
                    cur,
                    dispatch_plan_id=None,
                    plan_session_id=plan_session_id,
                    user_email=user_email,
                    peso_anterior_kg=peso_antes,
                    peso_nuevo_kg=peso_nuevo,
                    motivo=motivo or "recalculate_batch",
                )
                conn.commit()
            cur.close()
        finally:
            conn.close()

    return {
        "recalculated": len(items),
        "peso_anterior_kg": round(peso_antes, 3),
        "peso_nuevo_kg": round(peso_nuevo, 3),
        "items": items,
    }
