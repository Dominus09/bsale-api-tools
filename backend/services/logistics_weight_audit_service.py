"""Auditoría de peso: maestro products_master y órdenes de compra abiertas."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from backend.db import get_connection
from backend.services.distribuidora.orders_service import OC_PURCHASE_NOT_INVOICED_BY_RELATED_SQL
from backend.utils.planning_sql_fragments import PLANNING_WEIGHT_LATERAL, PLANNING_WEIGHT_SELECT

_MASTER_AUDIT_SQL = """
WITH pm AS (
    SELECT
        product_id,
        variant_id,
        (
            weight_box_kg IS NOT NULL AND weight_box_kg > 0
            AND units_per_box IS NOT NULL AND units_per_box > 0
        ) AS tiene_peso
    FROM bsale.products_master
    WHERE is_active = TRUE
),
prod AS (
    SELECT
        product_id,
        BOOL_OR(tiene_peso) AS tiene_peso
    FROM pm
    GROUP BY product_id
)
SELECT
    (SELECT COUNT(DISTINCT product_id) FROM pm)::bigint AS productos_erp,
    (SELECT COUNT(*)::bigint FROM prod WHERE tiene_peso) AS productos_con_peso,
    (SELECT COUNT(*)::bigint FROM prod WHERE NOT tiene_peso) AS productos_sin_peso,
    (SELECT COUNT(*)::bigint FROM pm WHERE tiene_peso) AS variantes_con_peso,
    (SELECT COUNT(*)::bigint FROM pm WHERE NOT tiene_peso) AS variantes_sin_peso,
    (SELECT COUNT(*)::bigint FROM pm) AS variantes_total
"""

_OPEN_ORDERS_SQL = f"""
SELECT
    d.document_id,
    d.number AS oc,
    COALESCE(
        NULLIF(BTRIM(c.nombre_fantasia), ''),
        NULLIF(BTRIM(c.company), ''),
        CONCAT_WS(
            ' ',
            NULLIF(BTRIM(c.first_name), ''),
            NULLIF(BTRIM(c.last_name), '')
        )
    ) AS cliente,
    COUNT(*) FILTER (WHERE COALESCE(dd.quantity, 0) > 0)::int AS productos_totales,
    COUNT(*) FILTER (
        WHERE COALESCE(dd.quantity, 0) > 0
          AND pl.weight_unit_kg IS NOT NULL
          AND pl.weight_unit_kg > 0
    )::int AS productos_con_peso,
    COUNT(*) FILTER (
        WHERE COALESCE(dd.quantity, 0) > 0
          AND (pl.weight_unit_kg IS NULL OR pl.weight_unit_kg <= 0)
    )::int AS productos_sin_peso,
    {PLANNING_WEIGHT_SELECT}
FROM distribuidora.v_documents_latest d
LEFT JOIN bsale.clients c
    ON c.company_id = d.company_id
   AND c.bsale_id = d.client_id
LEFT JOIN distribuidora.document_details dd
    ON dd.document_id = d.document_id
LEFT JOIN bsale.v_product_logistics pl
    ON pl.variant_id = dd.variant_id
{PLANNING_WEIGHT_LATERAL}
WHERE d.company_id = %s
  AND d.office_id = %s
  AND d.document_type_id = 33
  AND {OC_PURCHASE_NOT_INVOICED_BY_RELATED_SQL}
GROUP BY d.document_id, d.number, c.nombre_fantasia, c.company, c.first_name, c.last_name,
         w.peso_total_kg, w.productos_sin_peso, w.porcentaje_cobertura_peso
ORDER BY d.number DESC
"""

_ORDER_DETAIL_HEADER_SQL = """
SELECT
    d.document_id,
    d.number AS oc,
    d.company_id,
    COALESCE(
        NULLIF(BTRIM(c.nombre_fantasia), ''),
        NULLIF(BTRIM(c.company), ''),
        CONCAT_WS(
            ' ',
            NULLIF(BTRIM(c.first_name), ''),
            NULLIF(BTRIM(c.last_name), '')
        )
    ) AS cliente
FROM distribuidora.v_documents_latest d
LEFT JOIN bsale.clients c
    ON c.company_id = d.company_id
   AND c.bsale_id = d.client_id
WHERE d.document_id = %s
  AND d.company_id = %s
  AND d.office_id = %s
  AND d.document_type_id = 33
LIMIT 1
"""

_ORDER_DETAIL_LINES_SQL = """
SELECT
    dd.detail_id,
    dd.line_number,
    dd.variant_id,
    NULLIF(BTRIM(dd.variant_code), '') AS codigo,
    NULLIF(BTRIM(dd.variant_description), '') AS producto,
    dd.quantity::numeric AS cantidad,
    pl_v.weight_unit_kg AS peso_unitario_variant_join,
    pl_b.weight_unit_kg AS peso_unitario_barcode_join,
    COALESCE(pl_v.weight_unit_kg, pl_b.weight_unit_kg) AS peso_unitario_kg,
    ROUND(
        (dd.quantity * COALESCE(pl_v.weight_unit_kg, pl_b.weight_unit_kg, 0))::numeric,
        3
    ) AS peso_total_kg,
    v.product_id,
    NULLIF(BTRIM(v.bar_code), '') AS barcode,
    NULLIF(BTRIM(v.code), '') AS codigo_interno,
    pm_v.id AS pm_id_variant,
    pm_b.id AS pm_id_barcode,
    pm_v.weight_box_kg AS pm_weight_box_variant,
    pm_v.units_per_box AS pm_units_variant,
    pm_b.weight_box_kg AS pm_weight_box_barcode,
    pm_b.units_per_box AS pm_units_barcode,
    CASE
        WHEN COALESCE(dd.quantity, 0) <= 0 THEN 'sin_cantidad'
        WHEN pl_v.weight_unit_kg IS NOT NULL AND pl_v.weight_unit_kg > 0 THEN 'ok_variant_join'
        WHEN pl_b.weight_unit_kg IS NOT NULL AND pl_b.weight_unit_kg > 0 THEN 'ok_barcode_join'
        WHEN dd.variant_id IS NULL THEN 'sin_variant_id'
        WHEN pm_v.id IS NULL AND pm_b.id IS NULL THEN 'sin_match_pm'
        WHEN pm_v.id IS NOT NULL OR pm_b.id IS NOT NULL THEN 'pm_sin_peso'
        ELSE 'sin_peso'
    END AS join_status
FROM distribuidora.document_details dd
LEFT JOIN bsale.variants v
    ON v.company_id = %s
   AND v.bsale_id = dd.variant_id
LEFT JOIN bsale.v_product_logistics pl_v
    ON pl_v.variant_id = dd.variant_id
LEFT JOIN bsale.products_master pm_v
    ON pm_v.variant_id = dd.variant_id
   AND pm_v.is_active = TRUE
LEFT JOIN bsale.products_master pm_b
    ON pm_b.is_active = TRUE
   AND NULLIF(BTRIM(v.bar_code), '') IS NOT NULL
   AND pm_b.barcode = BTRIM(v.bar_code)
LEFT JOIN bsale.v_product_logistics pl_b
    ON pl_b.products_master_id = pm_b.id
WHERE dd.document_id = %s
ORDER BY dd.line_number NULLS LAST, dd.detail_id
"""


def classify_order_weight_estado(
    *,
    productos_totales: int,
    productos_con_peso: int,
    porcentaje_cobertura: float,
    peso_total_kg: float,
) -> str:
    if productos_totales <= 0 or peso_total_kg <= 0:
        if productos_totales > 0 and productos_con_peso > 0:
            return "parcial"
        return "sin_peso"
    if porcentaje_cobertura >= 100:
        return "completo"
    if productos_con_peso > 0:
        return "parcial"
    return "sin_peso"


def _serialize_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def _row_to_dict(cur, row: tuple) -> dict[str, Any]:
    cols = [d[0] for d in cur.description]
    return {k: _serialize_value(v) for k, v in zip(cols, row)}


def _coverage_pct(con: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return round(100.0 * con / total, 1)


def get_master_weight_audit() -> dict[str, Any]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(_MASTER_AUDIT_SQL)
        row = _row_to_dict(cur, cur.fetchone())
        cur.close()
    finally:
        conn.close()

    total_var = int(row.get("variantes_total") or 0)
    var_con = int(row.get("variantes_con_peso") or 0)
    row["porcentaje_cobertura"] = _coverage_pct(var_con, total_var)
    return row


def get_open_orders_weight_audit(
    *,
    company_id: int = 3,
    office_id: int = 1,
) -> dict[str, Any]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(_OPEN_ORDERS_SQL, (company_id, office_id))
        orders: list[dict[str, Any]] = []
        summary = {"completo": 0, "parcial": 0, "sin_peso": 0, "total": 0}
        for raw in cur.fetchall():
            item = _row_to_dict(cur, raw)
            productos_totales = int(item.get("productos_totales") or 0)
            productos_con_peso = int(item.get("productos_con_peso") or 0)
            productos_sin_peso = int(item.get("productos_sin_peso") or 0)
            peso = float(item.get("peso_total_kg") or 0)
            cobertura = float(item.get("porcentaje_cobertura_peso") or 0)
            estado = classify_order_weight_estado(
                productos_totales=productos_totales,
                productos_con_peso=productos_con_peso,
                porcentaje_cobertura=cobertura,
                peso_total_kg=peso,
            )
            item["estado"] = estado
            item["porcentaje_cobertura"] = cobertura
            orders.append(item)
            summary["total"] += 1
            summary[estado] = summary.get(estado, 0) + 1
        cur.close()
    finally:
        conn.close()

    return {
        "orders_summary": {
            "ordenes_total": summary["total"],
            "ordenes_completo": summary.get("completo", 0),
            "ordenes_parcial": summary.get("parcial", 0),
            "ordenes_sin_peso": summary.get("sin_peso", 0),
        },
        "orders": orders,
    }


def get_weight_audit(
    *,
    company_id: int = 3,
    office_id: int = 1,
) -> dict[str, Any]:
    master = get_master_weight_audit()
    orders_block = get_open_orders_weight_audit(company_id=company_id, office_id=office_id)
    return {
        "master": master,
        **orders_block,
    }


def get_order_weight_detail(
    *,
    document_id: int,
    company_id: int = 3,
    office_id: int = 1,
) -> dict[str, Any]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(_ORDER_DETAIL_HEADER_SQL, (document_id, company_id, office_id))
        header_row = cur.fetchone()
        if not header_row:
            cur.close()
            return {}
        header = _row_to_dict(cur, header_row)

        cur.execute(_ORDER_DETAIL_LINES_SQL, (company_id, document_id))
        lines: list[dict[str, Any]] = []
        productos_totales = 0
        productos_con_peso = 0
        productos_sin_peso = 0
        peso_total = 0.0

        for raw in cur.fetchall():
            line = _row_to_dict(cur, raw)
            qty = float(line.get("cantidad") or 0)
            if qty > 0:
                productos_totales += 1
                unit = line.get("peso_unitario_kg")
                if unit is not None and float(unit) > 0:
                    productos_con_peso += 1
                    peso_total += float(line.get("peso_total_kg") or 0)
                else:
                    productos_sin_peso += 1

            join_status = line.get("join_status") or "sin_peso"
            unit = line.get("peso_unitario_kg")
            tiene_peso = unit is not None and float(unit or 0) > 0
            line["estado"] = "tiene_peso" if tiene_peso else "sin_peso"
            line["join_debug"] = {
                "variant_id": line.pop("variant_id", None),
                "product_id": line.pop("product_id", None),
                "barcode": line.pop("barcode", None),
                "codigo_interno": line.pop("codigo_interno", None),
                "weight_unit_kg": line.get("peso_unitario_kg"),
                "join_status": join_status,
                "pm_id_variant": line.pop("pm_id_variant", None),
                "pm_id_barcode": line.pop("pm_id_barcode", None),
                "peso_unitario_variant_join": line.pop("peso_unitario_variant_join", None),
                "peso_unitario_barcode_join": line.pop("peso_unitario_barcode_join", None),
            }
            for k in (
                "pm_weight_box_variant",
                "pm_units_variant",
                "pm_weight_box_barcode",
                "pm_units_barcode",
            ):
                line.pop(k, None)
            lines.append(line)

        cobertura = _coverage_pct(productos_con_peso, productos_totales)
        estado = classify_order_weight_estado(
            productos_totales=productos_totales,
            productos_con_peso=productos_con_peso,
            porcentaje_cobertura=cobertura,
            peso_total_kg=peso_total,
        )
        cur.close()
    finally:
        conn.close()

    return {
        **header,
        "productos_totales": productos_totales,
        "productos_con_peso": productos_con_peso,
        "productos_sin_peso": productos_sin_peso,
        "peso_total_kg": round(peso_total, 3),
        "porcentaje_cobertura": cobertura,
        "estado": estado,
        "lines": lines,
    }
