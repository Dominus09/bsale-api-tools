"""Peso oficial de órdenes de compra — cálculo, persistencia y búsqueda."""

from __future__ import annotations

import csv
import io
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from backend.db import get_connection
from backend.services.distribuidora.orders_service import OC_PURCHASE_NOT_INVOICED_BY_RELATED_SQL
from backend.utils.order_weight_calc import (
    aggregate_order_summary,
    compute_line_from_row,
    coverage_semaphore,
)

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
    c.bsale_id AS codigo_cliente
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
    COALESCE(pm_v.units_per_box, pm_b.units_per_box, v.units_per_box) AS units_per_box,
    COALESCE(pl_v.weight_unit_kg, pl_b.weight_unit_kg) AS peso_unitario_kg,
    COALESCE(pm_v.weight_box_kg, pm_b.weight_box_kg) AS peso_caja_kg,
    COALESCE(pm_v.id, pm_b.id) AS products_master_id,
    COALESCE(pm_v.product_name, pm_b.product_name) AS product_name,
    COALESCE(pm_v.variant_name, pm_b.variant_name) AS variante,
    COALESCE(pm_v.logistics_completed, pm_b.logistics_completed) AS logistics_completed,
    COALESCE(pm_v.updated_at, pm_b.updated_at) AS pm_updated_at,
    COALESCE(pm_v.last_bsale_sync_at, pm_b.last_bsale_sync_at) AS last_bsale_sync_at,
    COALESCE(pm_v.height_cm, pm_b.height_cm) AS height_cm,
    COALESCE(pm_v.width_cm, pm_b.width_cm) AS width_cm,
    COALESCE(pm_v.length_cm, pm_b.length_cm) AS length_cm,
    v.product_id,
    NULLIF(BTRIM(v.bar_code), '') AS barcode,
    NULLIF(BTRIM(v.code), '') AS codigo_interno,
    (pl_v.weight_unit_kg IS NOT NULL AND pl_v.weight_unit_kg > 0) AS join_variant_ok,
    (pl_b.weight_unit_kg IS NOT NULL AND pl_b.weight_unit_kg > 0) AS join_barcode_ok,
    (pm_v.id IS NOT NULL OR pm_b.id IS NOT NULL) AS exists_in_pm
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
    ows.peso_total_kg,
    ows.porcentaje_cobertura,
    ows.calculated_at AS ultimo_calculo,
    COALESCE(ows.estado_cached, 'pendiente') AS estado
FROM distribuidora.v_documents_latest d
LEFT JOIN bsale.companies co ON co.company_id = d.company_id
LEFT JOIN bsale.clients c
    ON c.company_id = d.company_id
   AND c.bsale_id = d.client_id
LEFT JOIN LATERAL (
    SELECT
        s.peso_total_kg,
        s.porcentaje_cobertura,
        s.calculated_at,
        s.productos_con_peso,
        s.productos_totales,
        CASE
            WHEN s.porcentaje_cobertura >= 100 AND s.peso_total_kg > 0 THEN 'completo'
            WHEN s.productos_con_peso > 0 THEN 'parcial'
            WHEN s.productos_totales > 0 THEN 'sin_peso'
            ELSE 'pendiente'
        END AS estado_cached
    FROM distribuidora.order_weight_snapshots s
    WHERE s.document_id = d.document_id
    LIMIT 1
) ows ON TRUE
WHERE d.company_id = %s
  AND d.office_id = %s
  AND d.document_type_id = 33
  AND (%s = FALSE OR {OC_PURCHASE_NOT_INVOICED_BY_RELATED_SQL})
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
  AND (%s IS NULL OR COALESCE(ows.estado_cached, 'pendiente') = %s)
ORDER BY d.number DESC
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
SELECT document_id, peso_total_kg, productos_sin_peso, porcentaje_cobertura
FROM distribuidora.order_weight_snapshots
WHERE document_id = ANY(%s::bigint[])
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
    return [compute_line_from_row(_row_dict(cur, r)) for r in cur.fetchall()]


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
    if prev:
        old_weight = float(prev[2]) if prev[2] is not None else None

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
    conn = get_connection()
    try:
        cur = conn.cursor()
        if persist and not _table_exists(cur, "order_weight_snapshots"):
            persist = False

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
        semaforo = coverage_semaphore(summary["porcentaje_cobertura"])

        calculated_at = None
        calculated_by = user_email
        if persist:
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
        cur.close()
    finally:
        conn.close()

    estado = "completo"
    if summary["porcentaje_cobertura"] < 100 or summary["peso_total_kg"] <= 0:
        estado = "parcial" if summary["productos_con_peso"] > 0 else "sin_peso"

    return {
        **header,
        **summary,
        "estado": estado,
        "semaforo": semaforo,
        "ultimo_calculo": calculated_at or datetime.utcnow().isoformat(),
        "calculated_by": calculated_by,
        "lines": lines,
    }


def get_order_weight(
    *,
    document_id: int,
    company_id: int = 3,
    office_id: int = 1,
    line_filter: str | None = None,
    use_cache: bool = True,
) -> dict[str, Any]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        has_snapshots = _table_exists(cur, "order_weight_snapshots")
        cached = None
        if use_cache and has_snapshots:
            cur.execute(_SNAPSHOT_BY_DOC_SQL, (document_id,))
            snap_row = cur.fetchone()
            if snap_row:
                snap = _row_dict(cur, snap_row)
                cur.execute(_SNAPSHOT_LINES_SQL, (snap["id"],))
                lines = [_row_dict(cur, r) for r in cur.fetchall()]
                cur.execute(_ORDER_HEADER_SQL, (document_id, company_id))
                hdr = cur.fetchone()
                header = _row_dict(cur, hdr) if hdr else {}
                cached = {**header, **snap, "lines": lines}

        if cached:
            result = dict(cached)
            all_lines = result.get("lines") or []
            if line_filter and line_filter != "all":
                result["lines"] = [ln for ln in all_lines if ln.get("estado_linea") == line_filter]
            else:
                result["lines"] = all_lines
        else:
            cur.close()
            conn.close()
            return recalculate_order_weight(
                document_id=document_id,
                company_id=company_id,
                office_id=office_id,
                persist=has_snapshots,
            )

        lines = result.get("lines") or []
        result["semaforo"] = coverage_semaphore(float(result.get("porcentaje_cobertura") or 0))
        estado = "completo"
        pct = float(result.get("porcentaje_cobertura") or 0)
        peso = float(result.get("peso_total_kg") or 0)
        if pct < 100 or peso <= 0:
            estado = "parcial" if (result.get("productos_con_peso") or 0) > 0 else "sin_peso"
        result["estado"] = estado
        result["ultimo_calculo"] = result.get("calculated_at")
        cur.close()
        return result
    finally:
        conn.close()


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
    only_open: bool = True,
    limit: int = 100,
) -> list[dict[str, Any]]:
    cliente_term = f"%{cliente.strip()}%" if cliente and cliente.strip() else None
    codigo_term = f"%{codigo_cliente.strip()}%" if codigo_cliente and codigo_cliente.strip() else None

    conn = get_connection()
    try:
        cur = conn.cursor()
        if not _table_exists(cur, "order_weight_snapshots"):
            cur.execute(
                f"""
                SELECT d.document_id, d.number AS oc, d.emission_date, d.total_amount,
                       co.name AS empresa,
                       COALESCE(NULLIF(BTRIM(c.nombre_fantasia), ''), NULLIF(BTRIM(c.company), '')) AS cliente,
                       c.bsale_id AS codigo_cliente,
                       NULL::numeric AS peso_total_kg,
                       NULL::numeric AS porcentaje_cobertura,
                       NULL::timestamptz AS ultimo_calculo,
                       'pendiente' AS estado
                FROM distribuidora.v_documents_latest d
                LEFT JOIN bsale.companies co ON co.company_id = d.company_id
                LEFT JOIN bsale.clients c ON c.company_id = d.company_id AND c.bsale_id = d.client_id
                WHERE d.company_id = %s AND d.office_id = %s AND d.document_type_id = 33
                  AND (%s = FALSE OR {OC_PURCHASE_NOT_INVOICED_BY_RELATED_SQL})
                  AND (%s IS NULL OR d.number = %s)
                ORDER BY d.number DESC LIMIT %s
                """,
                (company_id, office_id, only_open, oc, oc, limit),
            )
        else:
            cur.execute(
                _SEARCH_ORDERS_SQL,
                (
                    company_id,
                    office_id,
                    only_open,
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
                    estado,
                    estado,
                    limit,
                ),
            )
        rows = [_row_dict(cur, r) for r in cur.fetchall()]
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
    """Calcula y persiste peso para OCs sin snapshot (planificación)."""
    if not document_ids:
        return {}
    ids = list(dict.fromkeys(int(x) for x in document_ids))
    conn = get_connection()
    try:
        cur = conn.cursor()
        if not _table_exists(cur, "order_weight_snapshots"):
            cur.close()
            return {}
        cur.execute(
            """
            SELECT document_id FROM distribuidora.order_weight_snapshots
            WHERE document_id = ANY(%s::bigint[])
            """,
            (ids,),
        )
        existing = {int(r[0]) for r in cur.fetchall()}
        cur.close()
    finally:
        conn.close()

    for doc_id in ids:
        if doc_id not in existing:
            recalculate_order_weight(
                document_id=doc_id,
                company_id=company_id,
                office_id=office_id,
                user_email=user_email,
                persist=True,
            )
    return fetch_weights_by_document_ids(ids)


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
