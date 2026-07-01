"""Analítica comercial vendedores — Company 3 / Office 1, solo facturas (6) y boletas (1)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from backend.db import get_connection
from backend.services.distribuidora.orders_service import _row_to_dict, _serialize_row

COMPANY_ID = 3
OFFICE_ID = 1
DOC_BOLETA = 1
DOC_FACTURA = 6
DOC_NC = 9
SALE_DOC_TYPES = (DOC_BOLETA, DOC_FACTURA)

MAX_ROWS = 5000


@dataclass
class CommercialFilters:
    date_from: date
    date_to: date
    compare_date_from: date | None = None
    compare_date_to: date | None = None
    seller: str | None = None
    city: str | None = None
    client_id: int | None = None
    document_type: str | None = None  # factura | boleta | all | None

    def compare_period(self) -> tuple[date, date]:
        if self.compare_date_from and self.compare_date_to:
            return self.compare_date_from, self.compare_date_to
        days = (self.date_to - self.date_from).days + 1
        prev_to = self.date_from - timedelta(days=1)
        prev_from = prev_to - timedelta(days=days - 1)
        return prev_from, prev_to


def _conn_query_all(sql: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        rows = cur.fetchall()
        out = [_serialize_row(_row_to_dict(cur, r)) for r in rows]
        cur.close()
        return out
    finally:
        conn.close()


def _conn_query_one(sql: str, params: tuple[Any, ...]) -> dict[str, Any] | None:
    rows = _conn_query_all(sql, params)
    return rows[0] if rows else None


def _float(v: Any) -> float:
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
    if abs(diff) < 0.01:
        trend = "flat"
    elif diff > 0:
        trend = "up"
    else:
        trend = "down"
    return {
        "current": current,
        "previous": previous,
        "delta_abs": round(diff, 2),
        "delta_pct": round(pct, 1),
        "trend": trend,
    }



def _period_kpis(filters: CommercialFilters, d_from: date, d_to: date) -> dict[str, Any]:
    where_parts: list[str] = ["v.is_sale = 1"]
    p: list[Any] = []
    doc = (filters.document_type or "all").lower().strip()
    if doc == "factura":
        where_parts.append("v.document_type_id = %s")
        p.append(DOC_FACTURA)
    elif doc == "boleta":
        where_parts.append("v.document_type_id = %s")
        p.append(DOC_BOLETA)
    else:
        where_parts.append("v.document_type_id IN %s")
        p.append(SALE_DOC_TYPES)
    p.extend([d_from, d_to])
    where_parts.append("(v.emission_date AT TIME ZONE 'UTC')::date >= %s")
    where_parts.append("(v.emission_date AT TIME ZONE 'UTC')::date <= %s")
    if filters.seller and str(filters.seller).strip():
        p.append(filters.seller.strip())
        where_parts.append("v.seller_name = %s")
    if filters.city and str(filters.city).strip():
        p.append(filters.city.strip())
        where_parts.append("v.municipality = %s")
    if filters.client_id is not None:
        p.append(int(filters.client_id))
        where_parts.append("v.client_id = %s")
    where_sql = " AND ".join(where_parts)

    sql_sales = f"""
        SELECT
            COALESCE(SUM(v.total_amount_net), 0) AS venta_neta,
            COUNT(DISTINCT v.client_id)::bigint AS clientes_unicos,
            COALESCE(SUM(v.is_sale), 0)::bigint AS documentos_emitidos,
            COALESCE(
                SUM(v.total_amount_sales) / NULLIF(SUM(v.is_sale)::numeric, 0),
                0
            ) AS ticket_promedio
        FROM distribuidora.v_sales v
        WHERE {where_sql}
    """
    row = _conn_query_one(sql_sales, tuple(p)) or {}

    sql_lines = f"""
        WITH sale_docs AS (
            SELECT DISTINCT v.document_id
            FROM distribuidora.v_sales v
            WHERE {where_sql}
        )
        SELECT
            COALESCE(SUM(dd.quantity), 0) AS unidades_vendidas,
            COUNT(DISTINCT dd.variant_id)::bigint AS productos_distintos
        FROM distribuidora.document_details dd
        INNER JOIN sale_docs sd ON sd.document_id = dd.document_id
    """
    lines = _conn_query_one(sql_lines, tuple(p)) or {}

    sql_margin = f"""
        WITH sale_docs AS (
            SELECT DISTINCT v.document_id
            FROM distribuidora.v_sales v
            WHERE {where_sql}
        ),
        lines AS (
            SELECT
                dd.total_amount AS line_revenue,
                dd.quantity,
                vc.average_cost_net,
                COALESCE(NULLIF(p.tax_factor, 0), 1)::numeric AS tax_factor
            FROM distribuidora.document_details dd
            INNER JOIN sale_docs sd ON sd.document_id = dd.document_id
            LEFT JOIN bsale.variants v2
                ON v2.company_id = {COMPANY_ID} AND v2.bsale_id = dd.variant_id
            LEFT JOIN bsale.products p
                ON p.company_id = v2.company_id AND p.bsale_id = v2.product_id
            LEFT JOIN bsale.variant_cost vc
                ON vc.company_id = {COMPANY_ID} AND vc.variant_id = dd.variant_id
        )
        SELECT
            COALESCE(SUM(line_revenue), 0) AS revenue,
            COALESCE(SUM(average_cost_net * tax_factor * quantity), 0) AS cost,
            COUNT(*)::bigint AS lines_total,
            COUNT(*) FILTER (WHERE average_cost_net IS NOT NULL)::bigint AS lines_with_cost
        FROM lines
    """
    margin_row = _conn_query_one(sql_margin, tuple(p)) or {}
    revenue = _float(margin_row.get("revenue"))
    cost = _float(margin_row.get("cost"))
    lines_total = _int(margin_row.get("lines_total"))
    lines_with_cost = _int(margin_row.get("lines_with_cost"))
    margin_est = None
    if lines_with_cost > 0 and lines_with_cost == lines_total:
        margin_est = revenue - cost

    return {
        "venta_neta": _float(row.get("venta_neta")),
        "clientes_unicos": _int(row.get("clientes_unicos")),
        "documentos_emitidos": _int(row.get("documentos_emitidos")),
        "ticket_promedio": _float(row.get("ticket_promedio")),
        "unidades_vendidas": _float(lines.get("unidades_vendidas")),
        "productos_distintos": _int(lines.get("productos_distintos")),
        "margen_estimado": margin_est,
        "margen_parcial": lines_with_cost > 0 and lines_with_cost < lines_total,
    }


def _client_classification(
    filters: CommercialFilters,
    d_from: date,
    d_to: date,
    prev_from: date,
    prev_to: date,
) -> dict[str, int]:
    doc = (filters.document_type or "all").lower().strip()
    if doc == "factura":
        doc_filter = "v.document_type_id = %s"
        doc_params: list[Any] = [DOC_FACTURA]
    elif doc == "boleta":
        doc_filter = "v.document_type_id = %s"
        doc_params = [DOC_BOLETA]
    else:
        doc_filter = "v.document_type_id IN %s"
        doc_params = [SALE_DOC_TYPES]

    extra: list[str] = []
    extra_params: list[Any] = []
    if filters.seller and str(filters.seller).strip():
        extra.append("v.seller_name = %s")
        extra_params.append(filters.seller.strip())
    if filters.city and str(filters.city).strip():
        extra.append("v.municipality = %s")
        extra_params.append(filters.city.strip())
    if filters.client_id is not None:
        extra.append("v.client_id = %s")
        extra_params.append(int(filters.client_id))
    extra_sql = (" AND " + " AND ".join(extra)) if extra else ""

    sql = f"""
        WITH curr AS (
            SELECT DISTINCT v.client_id
            FROM distribuidora.v_sales v
            WHERE v.is_sale = 1 AND {doc_filter}
              AND (v.emission_date AT TIME ZONE 'UTC')::date >= %s
              AND (v.emission_date AT TIME ZONE 'UTC')::date <= %s
              {extra_sql}
        ),
        prev AS (
            SELECT DISTINCT v.client_id
            FROM distribuidora.v_sales v
            WHERE v.is_sale = 1 AND {doc_filter}
              AND (v.emission_date AT TIME ZONE 'UTC')::date >= %s
              AND (v.emission_date AT TIME ZONE 'UTC')::date <= %s
              {extra_sql}
        ),
        last_purchase AS (
            SELECT
                v.client_id,
                MAX((v.emission_date AT TIME ZONE 'UTC')::date) AS last_d
            FROM distribuidora.v_sales v
            WHERE v.is_sale = 1 AND {doc_filter}
              AND (v.emission_date AT TIME ZONE 'UTC')::date < %s
              {extra_sql}
            GROUP BY v.client_id
        ),
        classified AS (
            SELECT
                c.client_id,
                CASE
                    WHEN p.client_id IS NOT NULL THEN 'activo'
                    WHEN lp.last_d IS NULL OR lp.last_d < (%s::date - INTERVAL '90 days')::date
                        THEN 'nuevo'
                    WHEN lp.last_d < (%s::date - INTERVAL '60 days')::date
                        THEN 'recuperado'
                    ELSE 'nuevo'
                END AS status
            FROM curr c
            LEFT JOIN prev p ON p.client_id = c.client_id
            LEFT JOIN last_purchase lp ON lp.client_id = c.client_id
        )
        SELECT status, COUNT(*)::bigint AS n
        FROM classified
        GROUP BY status
    """
    params: list[Any] = []
    for _ in range(2):
        params.extend(doc_params)
        params.extend([d_from, d_to])
        params.extend(extra_params)
    params.extend(doc_params)
    params.append(d_from)
    params.extend(extra_params)
    params.extend([d_from, d_from])

    rows = _conn_query_all(sql, tuple(params))
    counts = {r["status"]: _int(r["n"]) for r in rows}

    lost_sql = f"""
        SELECT COUNT(DISTINCT p.client_id)::bigint AS n
        FROM (
            SELECT DISTINCT v.client_id
            FROM distribuidora.v_sales v
            WHERE v.is_sale = 1 AND {doc_filter}
              AND (v.emission_date AT TIME ZONE 'UTC')::date >= %s
              AND (v.emission_date AT TIME ZONE 'UTC')::date <= %s
              {extra_sql}
        ) p
        LEFT JOIN (
            SELECT DISTINCT v.client_id
            FROM distribuidora.v_sales v
            WHERE v.is_sale = 1 AND {doc_filter}
              AND (v.emission_date AT TIME ZONE 'UTC')::date >= %s
              AND (v.emission_date AT TIME ZONE 'UTC')::date <= %s
              {extra_sql}
        ) c ON c.client_id = p.client_id
        WHERE c.client_id IS NULL
    """
    lp = list(doc_params) + [prev_from, prev_to] + extra_params
    lp += list(doc_params) + [d_from, d_to] + extra_params
    lost_row = _conn_query_one(lost_sql, tuple(lp)) or {}
    counts["perdido"] = _int(lost_row.get("n"))

    risk_sql = f"""
        WITH hist AS (
            SELECT
                v.client_id,
                COUNT(DISTINCT (v.emission_date AT TIME ZONE 'UTC')::date) AS visit_days,
                MAX((v.emission_date AT TIME ZONE 'UTC')::date) AS last_d,
                MIN((v.emission_date AT TIME ZONE 'UTC')::date) AS first_d
            FROM distribuidora.v_sales v
            WHERE v.is_sale = 1 AND {doc_filter}
              AND (v.emission_date AT TIME ZONE 'UTC')::date >= (%s::date - INTERVAL '180 days')::date
              {extra_sql}
            GROUP BY v.client_id
            HAVING COUNT(*) >= 3
        ),
        curr AS (
            SELECT DISTINCT v.client_id
            FROM distribuidora.v_sales v
            WHERE v.is_sale = 1 AND {doc_filter}
              AND (v.emission_date AT TIME ZONE 'UTC')::date >= %s
              AND (v.emission_date AT TIME ZONE 'UTC')::date <= %s
              {extra_sql}
        )
        SELECT COUNT(*)::bigint AS n
        FROM hist h
        LEFT JOIN curr c ON c.client_id = h.client_id
        WHERE c.client_id IS NULL
          AND (%s::date - h.last_d) > GREATEST(
              14,
              ((h.last_d - h.first_d)::numeric / NULLIF(h.visit_days - 1, 0)) * 1.5
          )
    """
    rp = list(doc_params) + [d_from] + extra_params
    rp += list(doc_params) + [d_from, d_to] + extra_params
    rp.append(d_to)
    risk_row = _conn_query_one(risk_sql, tuple(rp)) or {}
    counts["en_riesgo"] = _int(risk_row.get("n"))

    return {
        "activos": counts.get("activo", 0),
        "nuevos": counts.get("nuevo", 0),
        "recuperados": counts.get("recuperado", 0),
        "perdidos": counts.get("perdido", 0),
        "en_riesgo": counts.get("en_riesgo", 0),
    }


def get_dashboard(filters: CommercialFilters) -> dict[str, Any]:
    prev_from, prev_to = filters.compare_period()
    curr_kpi = _period_kpis(filters, filters.date_from, filters.date_to)
    prev_kpi = _period_kpis(filters, prev_from, prev_to)
    clients = _client_classification(
        filters, filters.date_from, filters.date_to, prev_from, prev_to
    )

    kpis = {
        "venta_neta": _delta(curr_kpi["venta_neta"], prev_kpi["venta_neta"]),
        "clientes_unicos": _delta(curr_kpi["clientes_unicos"], prev_kpi["clientes_unicos"]),
        "clientes_nuevos": {"current": clients["nuevos"], "previous": 0, **{"delta_abs": clients["nuevos"], "delta_pct": 0.0, "trend": "up" if clients["nuevos"] else "flat"}},
        "clientes_recuperados": {"current": clients["recuperados"], "previous": 0, "delta_abs": clients["recuperados"], "delta_pct": 0.0, "trend": "up" if clients["recuperados"] else "flat"},
        "clientes_perdidos": {"current": clients["perdidos"], "previous": 0, "delta_abs": clients["perdidos"], "delta_pct": 0.0, "trend": "down" if clients["perdidos"] else "flat"},
        "ticket_promedio": _delta(curr_kpi["ticket_promedio"], prev_kpi["ticket_promedio"]),
        "documentos_emitidos": _delta(curr_kpi["documentos_emitidos"], prev_kpi["documentos_emitidos"]),
        "unidades_vendidas": _delta(curr_kpi["unidades_vendidas"], prev_kpi["unidades_vendidas"]),
        "productos_distintos": _delta(curr_kpi["productos_distintos"], prev_kpi["productos_distintos"]),
        "margen_estimado": _delta(
            curr_kpi["margen_estimado"] or 0,
            prev_kpi["margen_estimado"] or 0,
        ) if curr_kpi["margen_estimado"] is not None else None,
    }

    daily = sales_daily_in_period(filters, filters.date_from, filters.date_to)

    return {
        "period": {"from": filters.date_from.isoformat(), "to": filters.date_to.isoformat()},
        "compare_period": {"from": prev_from.isoformat(), "to": prev_to.isoformat()},
        "document_types": {"boleta": DOC_BOLETA, "factura": DOC_FACTURA, "nota_credito_excluded": DOC_NC},
        "kpis": kpis,
        "client_classification": clients,
        "daily_sales": daily,
        "margen_parcial": curr_kpi.get("margen_parcial", False),
    }


def sales_daily_in_period(filters: CommercialFilters, d_from: date, d_to: date) -> list[dict[str, Any]]:
    where_parts: list[str] = ["v.is_sale = 1"]
    p: list[Any] = []
    doc = (filters.document_type or "all").lower().strip()
    if doc == "factura":
        where_parts.append("v.document_type_id = %s")
        p.append(DOC_FACTURA)
    elif doc == "boleta":
        where_parts.append("v.document_type_id = %s")
        p.append(DOC_BOLETA)
    else:
        where_parts.append("v.document_type_id IN %s")
        p.append(SALE_DOC_TYPES)
    p.extend([d_from, d_to])
    where_parts.append("(v.emission_date AT TIME ZONE 'UTC')::date >= %s")
    where_parts.append("(v.emission_date AT TIME ZONE 'UTC')::date <= %s")
    if filters.seller and str(filters.seller).strip():
        p.append(filters.seller.strip())
        where_parts.append("v.seller_name = %s")
    if filters.city and str(filters.city).strip():
        p.append(filters.city.strip())
        where_parts.append("v.municipality = %s")
    if filters.client_id is not None:
        p.append(int(filters.client_id))
        where_parts.append("v.client_id = %s")
    where_sql = " AND ".join(where_parts)
    sql = f"""
        SELECT
            (v.emission_date AT TIME ZONE 'UTC')::date AS day,
            COALESCE(SUM(v.total_amount_net), 0) AS venta_neta,
            COUNT(DISTINCT v.client_id)::bigint AS clientes
        FROM distribuidora.v_sales v
        WHERE {where_sql}
        GROUP BY 1
        ORDER BY 1
    """
    rows = _conn_query_all(sql, tuple(p))
    return [
        {"day": str(r["day"]), "venta_neta": _float(r["venta_neta"]), "clientes": _int(r["clientes"])}
        for r in rows
    ]


def get_seller_performance(filters: CommercialFilters, *, limit: int = 50) -> dict[str, Any]:
    prev_from, prev_to = filters.compare_period()
    cap = min(max(1, limit), MAX_ROWS)

    def _seller_agg(d_from: date, d_to: date) -> list[dict[str, Any]]:
        where_parts: list[str] = ["v.is_sale = 1"]
        p: list[Any] = []
        doc = (filters.document_type or "all").lower().strip()
        if doc == "factura":
            where_parts.append("v.document_type_id = %s")
            p.append(DOC_FACTURA)
        elif doc == "boleta":
            where_parts.append("v.document_type_id = %s")
            p.append(DOC_BOLETA)
        else:
            where_parts.append("v.document_type_id IN %s")
            p.append(SALE_DOC_TYPES)
        p.extend([d_from, d_to])
        where_parts.append("(v.emission_date AT TIME ZONE 'UTC')::date >= %s")
        where_parts.append("(v.emission_date AT TIME ZONE 'UTC')::date <= %s")
        if filters.city and str(filters.city).strip():
            p.append(filters.city.strip())
            where_parts.append("v.municipality = %s")
        if filters.client_id is not None:
            p.append(int(filters.client_id))
            where_parts.append("v.client_id = %s")
        where_sql = " AND ".join(where_parts)
        sql = f"""
            SELECT
                v.seller_name,
                v.seller_id,
                COALESCE(SUM(v.total_amount_net), 0) AS venta,
                COUNT(DISTINCT v.client_id)::bigint AS clientes,
                COALESCE(
                    SUM(v.total_amount_sales) / NULLIF(SUM(v.is_sale)::numeric, 0),
                    0
                ) AS ticket_promedio
            FROM distribuidora.v_sales v
            WHERE {where_sql}
            GROUP BY v.seller_name, v.seller_id
        """
        return _conn_query_all(sql, tuple(p))

    curr = {r["seller_name"]: r for r in _seller_agg(filters.date_from, filters.date_to)}
    prev = {r["seller_name"]: r for r in _seller_agg(prev_from, prev_to)}
    clients = _client_classification(filters, filters.date_from, filters.date_to, prev_from, prev_to)

    # Per-seller client metrics
    doc = (filters.document_type or "all").lower().strip()
    if doc == "factura":
        doc_filter = "v.document_type_id = %s"
        doc_params: list[Any] = [DOC_FACTURA]
    elif doc == "boleta":
        doc_filter = "v.document_type_id = %s"
        doc_params = [DOC_BOLETA]
    else:
        doc_filter = "v.document_type_id IN %s"
        doc_params = [SALE_DOC_TYPES]

    seller_client_sql = f"""
        WITH seller_clients AS (
            SELECT
                v.seller_name,
                v.client_id,
                MAX((v.emission_date AT TIME ZONE 'UTC')::date) FILTER (
                    WHERE (v.emission_date AT TIME ZONE 'UTC')::date BETWEEN %s AND %s
                ) AS curr_last,
                BOOL_OR(
                    (v.emission_date AT TIME ZONE 'UTC')::date BETWEEN %s AND %s
                ) AS in_curr,
                BOOL_OR(
                    (v.emission_date AT TIME ZONE 'UTC')::date BETWEEN %s AND %s
                ) AS in_prev
            FROM distribuidora.v_sales v
            WHERE v.is_sale = 1 AND {doc_filter}
            GROUP BY v.seller_name, v.client_id
        ),
        last_before AS (
            SELECT v.seller_name, v.client_id,
                   MAX((v.emission_date AT TIME ZONE 'UTC')::date) AS last_d
            FROM distribuidora.v_sales v
            WHERE v.is_sale = 1 AND {doc_filter}
              AND (v.emission_date AT TIME ZONE 'UTC')::date < %s
            GROUP BY v.seller_name, v.client_id
        )
        SELECT
            sc.seller_name,
            COUNT(*) FILTER (WHERE sc.in_curr) AS clientes_curr,
            COUNT(*) FILTER (WHERE sc.in_prev) AS clientes_prev,
            COUNT(*) FILTER (
                WHERE sc.in_curr AND NOT sc.in_prev
                AND (lb.last_d IS NULL OR lb.last_d < (%s::date - INTERVAL '90 days')::date)
            ) AS nuevos,
            COUNT(*) FILTER (
                WHERE sc.in_curr AND NOT sc.in_prev
                AND lb.last_d IS NOT NULL AND lb.last_d < (%s::date - INTERVAL '60 days')::date
            ) AS recuperados,
            COUNT(*) FILTER (WHERE sc.in_prev AND NOT sc.in_curr) AS perdidos
        FROM seller_clients sc
        LEFT JOIN last_before lb
            ON lb.seller_name = sc.seller_name AND lb.client_id = sc.client_id
        GROUP BY sc.seller_name
    """
    sc_params: list[Any] = (
        [filters.date_from, filters.date_to, filters.date_from, filters.date_to,
         prev_from, prev_to]
        + doc_params
        + doc_params
        + [filters.date_from]
        + doc_params
        + [filters.date_from, filters.date_from]
    )
    seller_clients_rows = _conn_query_all(seller_client_sql, tuple(sc_params))
    sc_map = {r["seller_name"]: r for r in seller_clients_rows}

    # Categories per seller
    cat_sql = f"""
        WITH sale_docs AS (
            SELECT DISTINCT v.document_id, v.seller_name
            FROM distribuidora.v_sales v
            WHERE v.is_sale = 1 AND {doc_filter}
              AND (v.emission_date AT TIME ZONE 'UTC')::date >= %s
              AND (v.emission_date AT TIME ZONE 'UTC')::date <= %s
        )
        SELECT
            sd.seller_name,
            COUNT(DISTINCT COALESCE(pt.name, pm.product_type, 'Sin categoría'))::bigint AS categorias
        FROM sale_docs sd
        INNER JOIN distribuidora.document_details dd ON dd.document_id = sd.document_id
        LEFT JOIN bsale.variants v2 ON v2.company_id = {COMPANY_ID} AND v2.bsale_id = dd.variant_id
        LEFT JOIN bsale.products p ON p.company_id = v2.company_id AND p.bsale_id = v2.product_id
        LEFT JOIN bsale.product_types pt ON pt.company_id = p.company_id AND pt.bsale_id = p.product_type_id
        LEFT JOIN bsale.products_master pm ON pm.company_id = {COMPANY_ID} AND pm.variant_id = dd.variant_id
        GROUP BY sd.seller_name
    """
    cat_rows = _conn_query_all(
        cat_sql, tuple(doc_params + [filters.date_from, filters.date_to])
    )
    cat_map = {r["seller_name"]: _int(r["categorias"]) for r in cat_rows}

    prod_sql = f"""
        WITH sale_docs AS (
            SELECT DISTINCT v.document_id, v.seller_name
            FROM distribuidora.v_sales v
            WHERE v.is_sale = 1 AND {doc_filter}
              AND (v.emission_date AT TIME ZONE 'UTC')::date >= %s
              AND (v.emission_date AT TIME ZONE 'UTC')::date <= %s
        )
        SELECT sd.seller_name, COUNT(DISTINCT dd.variant_id)::bigint AS productos
        FROM sale_docs sd
        INNER JOIN distribuidora.document_details dd ON dd.document_id = sd.document_id
        GROUP BY sd.seller_name
    """
    prod_rows = _conn_query_all(
        prod_sql, tuple(doc_params + [filters.date_from, filters.date_to])
    )
    prod_map = {r["seller_name"]: _int(r["productos"]) for r in prod_rows}

    all_sellers = set(curr) | set(prev) | set(sc_map)
    items: list[dict[str, Any]] = []
    for sn in all_sellers:
        c = curr.get(sn, {})
        p = prev.get(sn, {})
        sc = sc_map.get(sn, {})
        venta_c = _float(c.get("venta"))
        venta_p = _float(p.get("venta"))
        items.append({
            "seller_name": sn,
            "seller_id": c.get("seller_id") or p.get("seller_id"),
            "venta_actual": venta_c,
            "venta_anterior": venta_p,
            "variacion_pct": round(_delta(venta_c, venta_p)["delta_pct"], 1),
            "clientes_unicos_actual": _int(sc.get("clientes_curr") or c.get("clientes")),
            "clientes_unicos_anterior": _int(sc.get("clientes_prev") or p.get("clientes")),
            "clientes_nuevos": _int(sc.get("nuevos")),
            "clientes_perdidos": _int(sc.get("perdidos")),
            "clientes_recuperados": _int(sc.get("recuperados")),
            "ticket_promedio": _float(c.get("ticket_promedio")),
            "productos_distintos": prod_map.get(sn, 0),
            "categorias_vendidas": cat_map.get(sn, 0),
        })

    items.sort(key=lambda x: x["venta_actual"], reverse=True)
    items = items[:cap]

    rankings = {
        "mayor_venta": [x["seller_name"] for x in sorted(items, key=lambda x: x["venta_actual"], reverse=True)[:5]],
        "mayor_crecimiento": [x["seller_name"] for x in sorted(items, key=lambda x: x["variacion_pct"], reverse=True)[:5]],
        "mayor_recuperacion": [x["seller_name"] for x in sorted(items, key=lambda x: x["clientes_recuperados"], reverse=True)[:5]],
        "mayor_perdida": [x["seller_name"] for x in sorted(items, key=lambda x: x["clientes_perdidos"], reverse=True)[:5]],
        "mejor_cobertura": [x["seller_name"] for x in sorted(items, key=lambda x: x["clientes_unicos_actual"], reverse=True)[:5]],
    }

    return {
        "items": items,
        "rankings": rankings,
        "client_classification_total": clients,
        "period": {"from": filters.date_from.isoformat(), "to": filters.date_to.isoformat()},
        "compare_period": {"from": prev_from.isoformat(), "to": prev_to.isoformat()},
    }


def get_unique_clients(filters: CommercialFilters, *, limit: int = 500) -> dict[str, Any]:
    prev_from, prev_to = filters.compare_period()
    cap = min(max(1, limit), MAX_ROWS)
    doc = (filters.document_type or "all").lower().strip()
    if doc == "factura":
        doc_filter = "v.document_type_id = %s"
        doc_params: list[Any] = [DOC_FACTURA]
    elif doc == "boleta":
        doc_filter = "v.document_type_id = %s"
        doc_params = [DOC_BOLETA]
    else:
        doc_filter = "v.document_type_id IN %s"
        doc_params = [SALE_DOC_TYPES]

    extra: list[str] = []
    extra_params: list[Any] = []
    if filters.seller and str(filters.seller).strip():
        extra.append("v.seller_name = %s")
        extra_params.append(filters.seller.strip())
    if filters.city and str(filters.city).strip():
        extra.append("v.municipality = %s")
        extra_params.append(filters.city.strip())
    extra_sql = (" AND " + " AND ".join(extra)) if extra else ""

    sql = f"""
        WITH client_period AS (
            SELECT
                v.client_id,
                MAX(v.client_name) AS client_name,
                MAX(v.municipality) AS municipality,
                (
                    ARRAY_AGG(v.seller_name ORDER BY v.emission_date DESC)
                    FILTER (WHERE (v.emission_date AT TIME ZONE 'UTC')::date BETWEEN %s AND %s)
                )[1] AS seller_name,
                COALESCE(SUM(v.total_amount_net) FILTER (
                    WHERE (v.emission_date AT TIME ZONE 'UTC')::date BETWEEN %s AND %s
                ), 0) AS venta_actual,
                BOOL_OR((v.emission_date AT TIME ZONE 'UTC')::date BETWEEN %s AND %s) AS in_curr,
                BOOL_OR((v.emission_date AT TIME ZONE 'UTC')::date BETWEEN %s AND %s) AS in_prev,
                MAX((v.emission_date AT TIME ZONE 'UTC')::date) FILTER (
                    WHERE v.is_sale = 1 AND (v.emission_date AT TIME ZONE 'UTC')::date < %s
                ) AS last_before
            FROM distribuidora.v_sales v
            WHERE v.is_sale = 1 AND {doc_filter}
              {extra_sql}
            GROUP BY v.client_id
        )
        SELECT
            client_id, client_name, municipality, seller_name, venta_actual,
            CASE
                WHEN in_curr AND in_prev THEN 'activo'
                WHEN in_curr AND NOT in_prev AND (
                    last_before IS NULL OR last_before < (%s::date - INTERVAL '90 days')::date
                ) THEN 'nuevo'
                WHEN in_curr AND NOT in_prev AND last_before < (%s::date - INTERVAL '60 days')::date
                    THEN 'recuperado'
                WHEN in_prev AND NOT in_curr THEN 'perdido'
                ELSE 'en_riesgo'
            END AS status
        FROM client_period
        WHERE in_curr OR in_prev
        ORDER BY venta_actual DESC NULLS LAST
        LIMIT %s
    """
    params: list[Any] = [
        filters.date_from, filters.date_to,
        filters.date_from, filters.date_to,
        filters.date_from, filters.date_to,
        prev_from, prev_to,
        filters.date_from,
    ]
    params.extend(doc_params)
    params.extend(extra_params)
    params.extend([filters.date_from, filters.date_from, cap])
    rows = _conn_query_all(sql, tuple(params))

    summary: dict[str, int] = {}
    for r in rows:
        st = r.get("status") or "otro"
        summary[st] = summary.get(st, 0) + 1

    return {
        "items": [
            {
                "client_id": _int(r["client_id"]),
                "client_name": r.get("client_name"),
                "municipality": r.get("municipality"),
                "seller_name": r.get("seller_name"),
                "venta_actual": _float(r.get("venta_actual")),
                "status": r.get("status"),
            }
            for r in rows
        ],
        "summary": summary,
    }


def get_lost_clients(filters: CommercialFilters, *, limit: int = 200) -> dict[str, Any]:
    prev_from, prev_to = filters.compare_period()
    cap = min(max(1, limit), MAX_ROWS)
    doc = (filters.document_type or "all").lower().strip()
    if doc == "factura":
        doc_filter = "v.document_type_id = %s"
        doc_params: list[Any] = [DOC_FACTURA]
    elif doc == "boleta":
        doc_filter = "v.document_type_id = %s"
        doc_params = [DOC_BOLETA]
    else:
        doc_filter = "v.document_type_id IN %s"
        doc_params = [SALE_DOC_TYPES]

    extra: list[str] = []
    extra_params: list[Any] = []
    if filters.seller and str(filters.seller).strip():
        extra.append("v.seller_name = %s")
        extra_params.append(filters.seller.strip())
    if filters.city and str(filters.city).strip():
        extra.append("v.municipality = %s")
        extra_params.append(filters.city.strip())
    extra_sql = (" AND " + " AND ".join(extra)) if extra else ""

    sql = f"""
        WITH prev_clients AS (
            SELECT DISTINCT v.client_id
            FROM distribuidora.v_sales v
            WHERE v.is_sale = 1 AND {doc_filter}
              AND (v.emission_date AT TIME ZONE 'UTC')::date BETWEEN %s AND %s
              {extra_sql}
        ),
        curr_clients AS (
            SELECT DISTINCT v.client_id
            FROM distribuidora.v_sales v
            WHERE v.is_sale = 1 AND {doc_filter}
              AND (v.emission_date AT TIME ZONE 'UTC')::date BETWEEN %s AND %s
        ),
        lost AS (
            SELECT p.client_id FROM prev_clients p
            LEFT JOIN curr_clients c ON c.client_id = p.client_id
            WHERE c.client_id IS NULL
        ),
        stats AS (
            SELECT
                v.client_id,
                MAX(v.client_name) AS client_name,
                MAX(v.municipality) AS municipality,
                (
                    ARRAY_AGG(v.seller_name ORDER BY v.emission_date DESC)
                )[1] AS seller_name,
                MAX((v.emission_date AT TIME ZONE 'UTC')::date) FILTER (WHERE v.is_sale = 1) AS ultima_compra,
                (CURRENT_DATE - MAX((v.emission_date AT TIME ZONE 'UTC')::date) FILTER (WHERE v.is_sale = 1))::int
                    AS dias_sin_comprar,
                COALESCE(SUM(v.total_amount_net) FILTER (WHERE v.is_sale = 1), 0) AS valor_historico,
                COALESCE(SUM(v.is_sale), 0)::bigint AS total_compras,
                COALESCE(
                    SUM(v.total_amount_sales) / NULLIF(SUM(v.is_sale)::numeric, 0),
                    0
                ) AS ticket_promedio
            FROM distribuidora.v_sales v
            INNER JOIN lost l ON l.client_id = v.client_id
            WHERE v.is_sale = 1 AND {doc_filter}
            GROUP BY v.client_id
        ),
        top_products AS (
            SELECT
                v.client_id,
                ARRAY_AGG(DISTINCT COALESCE(dd.variant_description, dd.variant_code, 'Producto')
                    ORDER BY COALESCE(dd.variant_description, dd.variant_code)) AS productos
            FROM distribuidora.v_sales v
            INNER JOIN lost l ON l.client_id = v.client_id
            INNER JOIN distribuidora.document_details dd ON dd.document_id = v.document_id
            WHERE v.is_sale = 1 AND {doc_filter}
            GROUP BY v.client_id
        )
        SELECT
            s.*,
            tp.productos[1:5] AS productos_habituales,
            CASE
                WHEN s.valor_historico >= 500000 AND s.dias_sin_comprar >= 30 THEN 'alta'
                WHEN s.total_compras >= 5 AND s.dias_sin_comprar >= 21 THEN 'media'
                ELSE 'baja'
            END AS prioridad
        FROM stats s
        LEFT JOIN top_products tp ON tp.client_id = s.client_id
        ORDER BY
            CASE
                WHEN s.valor_historico >= 500000 THEN 1
                WHEN s.total_compras >= 5 THEN 2
                ELSE 3
            END,
            s.valor_historico DESC NULLS LAST
        LIMIT %s
    """
    params: list[Any] = (
        doc_params + [prev_from, prev_to] + extra_params
        + doc_params + [filters.date_from, filters.date_to]
        + doc_params + doc_params + [cap]
    )
    rows = _conn_query_all(sql, tuple(params))

    action_map = {"alta": "Visitar", "media": "Llamar", "baja": "Ofrecer productos habituales"}
    return {
        "items": [
            {
                "client_id": _int(r["client_id"]),
                "client_name": r.get("client_name"),
                "seller_name": r.get("seller_name"),
                "municipality": r.get("municipality"),
                "ultima_compra": str(r.get("ultima_compra")) if r.get("ultima_compra") else None,
                "dias_sin_comprar": _int(r.get("dias_sin_comprar")),
                "promedio_compra_mensual": round(
                    _float(r.get("valor_historico")) / max(1, _int(r.get("total_compras"))), 0
                ),
                "ticket_promedio": _float(r.get("ticket_promedio")),
                "productos_habituales": list(r.get("productos_habituales") or []),
                "prioridad": r.get("prioridad"),
                "accion_sugerida": action_map.get(str(r.get("prioridad")), "Revisar si cambió de proveedor"),
            }
            for r in rows
        ],
    }


def get_recovered_clients(filters: CommercialFilters, *, limit: int = 200) -> dict[str, Any]:
    data = get_unique_clients(filters, limit=limit * 2)
    items = [x for x in data["items"] if x.get("status") == "recuperado"][:limit]
    return {"items": items, "total": len(items)}


def get_product_performance(filters: CommercialFilters, *, seller: str | None = None, limit: int = 100) -> dict[str, Any]:
    prev_from, prev_to = filters.compare_period()
    cap = min(max(1, limit), MAX_ROWS)
    target_seller = seller or filters.seller
    doc = (filters.document_type or "all").lower().strip()
    if doc == "factura":
        doc_filter = "v.document_type_id = %s"
        doc_params: list[Any] = [DOC_FACTURA]
    elif doc == "boleta":
        doc_filter = "v.document_type_id = %s"
        doc_params = [DOC_BOLETA]
    else:
        doc_filter = "v.document_type_id IN %s"
        doc_params = [SALE_DOC_TYPES]

    seller_extra = ""
    seller_params: list[Any] = []
    if target_seller and str(target_seller).strip():
        seller_extra = "AND v.seller_name = %s"
        seller_params.append(target_seller.strip())

    sql_top = f"""
        WITH sale_docs AS (
            SELECT v.document_id, v.client_id, v.seller_name
            FROM distribuidora.v_sales v
            WHERE v.is_sale = 1 AND {doc_filter}
              AND (v.emission_date AT TIME ZONE 'UTC')::date BETWEEN %s AND %s
              {seller_extra}
        )
        SELECT
            COALESCE(dd.variant_description, dd.variant_code, 'Sin nombre') AS producto,
            dd.variant_id,
            COALESCE(SUM(dd.quantity), 0) AS unidades,
            COUNT(DISTINCT sd.client_id)::bigint AS clientes,
            COALESCE(SUM(dd.total_amount), 0) AS venta
        FROM sale_docs sd
        INNER JOIN distribuidora.document_details dd ON dd.document_id = sd.document_id
        GROUP BY dd.variant_id, dd.variant_description, dd.variant_code
        ORDER BY venta DESC NULLS LAST
        LIMIT %s
    """
    top = _conn_query_all(
        sql_top,
        tuple(doc_params + [filters.date_from, filters.date_to] + seller_params + [cap]),
    )

    # Company-wide product reach vs seller
    gap_sql = f"""
        WITH company_prod AS (
            SELECT
                dd.variant_id,
                COALESCE(dd.variant_description, dd.variant_code, 'Sin nombre') AS producto,
                COUNT(DISTINCT v.client_id)::bigint AS clientes_empresa
            FROM distribuidora.v_sales v
            INNER JOIN distribuidora.document_details dd ON dd.document_id = v.document_id
            WHERE v.is_sale = 1 AND {doc_filter}
              AND (v.emission_date AT TIME ZONE 'UTC')::date BETWEEN %s AND %s
            GROUP BY dd.variant_id, dd.variant_description, dd.variant_code
            HAVING COUNT(DISTINCT v.client_id) >= 10
        ),
        seller_prod AS (
            SELECT
                dd.variant_id,
                COUNT(DISTINCT v.client_id)::bigint AS clientes_vendedor
            FROM distribuidora.v_sales v
            INNER JOIN distribuidora.document_details dd ON dd.document_id = v.document_id
            WHERE v.is_sale = 1 AND {doc_filter}
              AND (v.emission_date AT TIME ZONE 'UTC')::date BETWEEN %s AND %s
              {seller_extra}
            GROUP BY dd.variant_id
        )
        SELECT
            cp.producto,
            cp.variant_id,
            cp.clientes_empresa,
            COALESCE(sp.clientes_vendedor, 0)::bigint AS clientes_vendedor,
            cp.clientes_empresa - COALESCE(sp.clientes_vendedor, 0) AS brecha
        FROM company_prod cp
        LEFT JOIN seller_prod sp ON sp.variant_id = cp.variant_id
        WHERE COALESCE(sp.clientes_vendedor, 0) < cp.clientes_empresa * 0.3
        ORDER BY brecha DESC NULLS LAST
        LIMIT %s
    """
    gaps = _conn_query_all(
        gap_sql,
        tuple(
            doc_params + [filters.date_from, filters.date_to]
            + doc_params + [filters.date_from, filters.date_to]
            + seller_params
            + [min(50, cap)]
        ),
    )

    # Period comparison for drops/growth
    cmp_sql = f"""
        WITH curr AS (
            SELECT dd.variant_id,
                   COALESCE(SUM(dd.total_amount), 0) AS venta
            FROM distribuidora.v_sales v
            INNER JOIN distribuidora.document_details dd ON dd.document_id = v.document_id
            WHERE v.is_sale = 1 AND {doc_filter}
              AND (v.emission_date AT TIME ZONE 'UTC')::date BETWEEN %s AND %s
              {seller_extra}
            GROUP BY dd.variant_id
        ),
        prev AS (
            SELECT dd.variant_id,
                   COALESCE(SUM(dd.total_amount), 0) AS venta
            FROM distribuidora.v_sales v
            INNER JOIN distribuidora.document_details dd ON dd.document_id = v.document_id
            WHERE v.is_sale = 1 AND {doc_filter}
              AND (v.emission_date AT TIME ZONE 'UTC')::date BETWEEN %s AND %s
              {seller_extra}
            GROUP BY dd.variant_id
        )
        SELECT
            COALESCE(c.variant_id, p.variant_id) AS variant_id,
            COALESCE(c.venta, 0) AS venta_actual,
            COALESCE(p.venta, 0) AS venta_anterior
        FROM curr c
        FULL OUTER JOIN prev p ON p.variant_id = c.variant_id
        WHERE COALESCE(p.venta, 0) > 0
        ORDER BY (COALESCE(c.venta, 0) - COALESCE(p.venta, 0)) ASC
        LIMIT %s
    """
    cmp_rows = _conn_query_all(
        cmp_sql,
        tuple(
            doc_params + [filters.date_from, filters.date_to] + seller_params
            + doc_params + [prev_from, prev_to] + seller_params
            + [30]
        ),
    )

    return {
        "seller": target_seller,
        "top_products": [
            {
                "producto": r.get("producto"),
                "variant_id": _int(r.get("variant_id")) if r.get("variant_id") else None,
                "unidades": _float(r.get("unidades")),
                "clientes": _int(r.get("clientes")),
                "venta": _float(r.get("venta")),
            }
            for r in top
        ],
        "oportunidades": [
            {
                "producto": r.get("producto"),
                "variant_id": _int(r.get("variant_id")) if r.get("variant_id") else None,
                "clientes_empresa": _int(r.get("clientes_empresa")),
                "clientes_vendedor": _int(r.get("clientes_vendedor")),
                "brecha": _int(r.get("brecha")),
            }
            for r in gaps
        ],
        "caida_fuerte": [
            {
                "variant_id": _int(r.get("variant_id")) if r.get("variant_id") else None,
                "venta_actual": _float(r.get("venta_actual")),
                "venta_anterior": _float(r.get("venta_anterior")),
                "variacion_pct": round(_delta(_float(r.get("venta_actual")), _float(r.get("venta_anterior")))["delta_pct"], 1),
            }
            for r in cmp_rows
            if _float(r.get("venta_actual")) < _float(r.get("venta_anterior")) * 0.5
        ],
    }


# Cross-sell rules: category bought -> recommended category
CROSS_SELL_RULES: list[tuple[str, str, str, str]] = [
    ("cerveza", "hielo", "Compra cerveza pero no hielo", "alta"),
    ("whisky", "energetica", "Compra whisky pero no energética", "media"),
    ("bebida", "snack", "Compra bebidas pero no snacks", "media"),
    ("aseo", "papel", "Compra aseo pero no papel", "baja"),
]


def get_cross_selling(filters: CommercialFilters, *, limit: int = 100) -> dict[str, Any]:
    cap = min(max(1, limit), MAX_ROWS)
    doc = (filters.document_type or "all").lower().strip()
    if doc == "factura":
        doc_filter = "v.document_type_id = %s"
        doc_params: list[Any] = [DOC_FACTURA]
    elif doc == "boleta":
        doc_filter = "v.document_type_id = %s"
        doc_params = [DOC_BOLETA]
    else:
        doc_filter = "v.document_type_id IN %s"
        doc_params = [SALE_DOC_TYPES]

    extra: list[str] = []
    extra_params: list[Any] = []
    if filters.seller and str(filters.seller).strip():
        extra.append("v.seller_name = %s")
        extra_params.append(filters.seller.strip())
    if filters.city and str(filters.city).strip():
        extra.append("v.municipality = %s")
        extra_params.append(filters.city.strip())
    extra_sql = (" AND " + " AND ".join(extra)) if extra else ""

    sql = f"""
        WITH client_cats AS (
            SELECT
                v.client_id,
                MAX(v.client_name) AS client_name,
                (
                    ARRAY_AGG(v.seller_name ORDER BY v.emission_date DESC)
                )[1] AS seller_name,
                LOWER(COALESCE(pt.name, pm.product_type, '')) AS categoria
            FROM distribuidora.v_sales v
            INNER JOIN distribuidora.document_details dd ON dd.document_id = v.document_id
            LEFT JOIN bsale.variants v2 ON v2.company_id = {COMPANY_ID} AND v2.bsale_id = dd.variant_id
            LEFT JOIN bsale.products p ON p.company_id = v2.company_id AND p.bsale_id = v2.product_id
            LEFT JOIN bsale.product_types pt ON pt.company_id = p.company_id AND pt.bsale_id = p.product_type_id
            LEFT JOIN bsale.products_master pm ON pm.company_id = {COMPANY_ID} AND pm.variant_id = dd.variant_id
            WHERE v.is_sale = 1 AND {doc_filter}
              AND (v.emission_date AT TIME ZONE 'UTC')::date BETWEEN %s AND %s
              {extra_sql}
            GROUP BY v.client_id, LOWER(COALESCE(pt.name, pm.product_type, ''))
        ),
        client_cat_set AS (
            SELECT client_id, MAX(client_name) AS client_name, MAX(seller_name) AS seller_name,
                   ARRAY_AGG(DISTINCT categoria) AS cats
            FROM client_cats
            GROUP BY client_id
        )
        SELECT client_id, client_name, seller_name, cats
        FROM client_cat_set
        LIMIT %s
    """
    params = doc_params + [filters.date_from, filters.date_to] + extra_params + [cap * 3]
    rows = _conn_query_all(sql, tuple(params))

    opportunities: list[dict[str, Any]] = []
    for row in rows:
        cats = [str(c).lower() for c in (row.get("cats") or []) if c]
        cats_joined = " ".join(cats)
        for bought_kw, rec_kw, motivo, prioridad in CROSS_SELL_RULES:
            if bought_kw in cats_joined and rec_kw not in cats_joined:
                opportunities.append({
                    "client_id": _int(row["client_id"]),
                    "client_name": row.get("client_name"),
                    "seller_name": row.get("seller_name"),
                    "producto_comprado": bought_kw,
                    "producto_recomendado": rec_kw,
                    "motivo": motivo,
                    "prioridad": prioridad,
                })
                break
        if len(opportunities) >= cap:
            break

    return {"items": opportunities, "total": len(opportunities)}


def get_client_profile(filters: CommercialFilters, client_id: int) -> dict[str, Any]:
    doc = (filters.document_type or "all").lower().strip()
    if doc == "factura":
        doc_filter = "v.document_type_id = %s"
        doc_params: list[Any] = [DOC_FACTURA]
    elif doc == "boleta":
        doc_filter = "v.document_type_id = %s"
        doc_params = [DOC_BOLETA]
    else:
        doc_filter = "v.document_type_id IN %s"
        doc_params = [SALE_DOC_TYPES]

    six_months_ago = filters.date_to - timedelta(days=180)

    sql_client = f"""
        SELECT
            v.client_id,
            MAX(v.client_name) AS client_name,
            MAX(v.municipality) AS municipality,
            (
                ARRAY_AGG(v.seller_name ORDER BY v.emission_date DESC)
            )[1] AS seller_name,
            MAX((v.emission_date AT TIME ZONE 'UTC')::date) FILTER (WHERE v.is_sale = 1) AS ultima_compra,
            COALESCE(SUM(v.is_sale), 0)::bigint AS total_compras,
            COALESCE(SUM(v.total_amount_net), 0) AS venta_total,
            COALESCE(
                SUM(v.total_amount_sales) / NULLIF(SUM(v.is_sale)::numeric, 0),
                0
            ) AS ticket_promedio
        FROM distribuidora.v_sales v
        WHERE v.client_id = %s AND v.is_sale = 1 AND {doc_filter}
        GROUP BY v.client_id
    """
    client_row = _conn_query_one(sql_client, tuple([client_id] + doc_params)) or {}

    sql_monthly = f"""
        SELECT
            TO_CHAR((v.emission_date AT TIME ZONE 'UTC')::date, 'YYYY-MM') AS mes,
            COALESCE(SUM(v.total_amount_net), 0) AS venta
        FROM distribuidora.v_sales v
        WHERE v.client_id = %s AND v.is_sale = 1 AND {doc_filter}
          AND (v.emission_date AT TIME ZONE 'UTC')::date >= %s
        GROUP BY 1
        ORDER BY 1
    """
    monthly = _conn_query_all(sql_monthly, tuple([client_id] + doc_params + [six_months_ago]))

    sql_products = f"""
        SELECT
            COALESCE(dd.variant_description, dd.variant_code, 'Producto') AS producto,
            COALESCE(SUM(dd.quantity), 0) AS unidades,
            COALESCE(SUM(dd.total_amount), 0) AS venta,
            MAX((v.emission_date AT TIME ZONE 'UTC')::date) AS ultima_compra
        FROM distribuidora.v_sales v
        INNER JOIN distribuidora.document_details dd ON dd.document_id = v.document_id
        WHERE v.client_id = %s AND v.is_sale = 1 AND {doc_filter}
        GROUP BY dd.variant_description, dd.variant_code
        ORDER BY venta DESC NULLS LAST
        LIMIT 20
    """
    products = _conn_query_all(sql_products, tuple([client_id] + doc_params))

    sql_cats = f"""
        SELECT
            COALESCE(pt.name, pm.product_type, 'Sin categoría') AS categoria,
            COALESCE(SUM(dd.total_amount), 0) AS venta
        FROM distribuidora.v_sales v
        INNER JOIN distribuidora.document_details dd ON dd.document_id = v.document_id
        LEFT JOIN bsale.variants v2 ON v2.company_id = {COMPANY_ID} AND v2.bsale_id = dd.variant_id
        LEFT JOIN bsale.products p ON p.company_id = v2.company_id AND p.bsale_id = v2.product_id
        LEFT JOIN bsale.product_types pt ON pt.company_id = p.company_id AND pt.bsale_id = p.product_type_id
        LEFT JOIN bsale.products_master pm ON pm.company_id = {COMPANY_ID} AND pm.variant_id = dd.variant_id
        WHERE v.client_id = %s AND v.is_sale = 1 AND {doc_filter}
        GROUP BY 1
        ORDER BY venta DESC NULLS LAST
    """
    categories = _conn_query_all(sql_cats, tuple([client_id] + doc_params))

    cross = get_cross_selling(
        CommercialFilters(
            date_from=filters.date_from,
            date_to=filters.date_to,
            client_id=client_id,
            document_type=filters.document_type,
        ),
        limit=10,
    )

    freq_days = None
    if _int(client_row.get("total_compras")) > 1 and client_row.get("ultima_compra"):
        freq_days = max(1, 180 // _int(client_row.get("total_compras")))

    return {
        "client": {
            "client_id": client_id,
            "client_name": client_row.get("client_name"),
            "municipality": client_row.get("municipality"),
            "seller_name": client_row.get("seller_name"),
            "ultima_compra": str(client_row.get("ultima_compra")) if client_row.get("ultima_compra") else None,
            "frecuencia_dias": freq_days,
            "ticket_promedio": _float(client_row.get("ticket_promedio")),
            "venta_total": _float(client_row.get("venta_total")),
            "total_compras": _int(client_row.get("total_compras")),
        },
        "venta_mensual": [{"mes": r["mes"], "venta": _float(r["venta"])} for r in monthly],
        "productos_habituales": [
            {
                "producto": r.get("producto"),
                "unidades": _float(r.get("unidades")),
                "venta": _float(r.get("venta")),
                "ultima_compra": str(r.get("ultima_compra")) if r.get("ultima_compra") else None,
            }
            for r in products
        ],
        "categorias": [{"categoria": r.get("categoria"), "venta": _float(r.get("venta"))} for r in categories],
        "oportunidades": cross.get("items", []),
    }


def get_summary(filters: CommercialFilters) -> dict[str, Any]:
    dash = get_dashboard(filters)
    sellers = get_seller_performance(filters, limit=20)
    cross = get_cross_selling(filters, limit=500)
    lost = get_lost_clients(filters, limit=500)

    kpis = dash["kpis"]
    bullets: list[str] = []

    venta_pct = kpis["venta_neta"]["delta_pct"]
    if venta_pct > 0:
        bullets.append(f"La venta creció {venta_pct:.1f}% vs período anterior.")
    elif venta_pct < 0:
        bullets.append(f"La venta bajó {abs(venta_pct):.1f}% vs período anterior.")
    else:
        bullets.append("La venta se mantuvo estable vs período anterior.")

    perdidos = dash["client_classification"]["perdidos"]
    recuperados = dash["client_classification"]["recuperados"]
    bullets.append(f"Se perdieron {perdidos} clientes únicos.")
    bullets.append(f"Se recuperaron {recuperados} clientes.")

    for item in sellers.get("items", [])[:3]:
        var = item.get("variacion_pct", 0)
        if var >= 10:
            bullets.append(f"{item['seller_name']} aumentó {var:.0f}% en venta.")
        elif var <= -10:
            bullets.append(f"{item['seller_name']} bajó {abs(var):.0f}% en venta.")

    for item in sellers.get("items", []):
        cu_var = _delta(
            item.get("clientes_unicos_actual", 0),
            item.get("clientes_unicos_anterior", 0),
        )["delta_pct"]
        if cu_var <= -10:
            bullets.append(f"{item['seller_name']} bajó {abs(cu_var):.0f}% en clientes únicos.")
            break

    bullets.append(f"Hay {cross.get('total', 0)} oportunidades de cross-selling.")

    important_lost = [x for x in lost.get("items", []) if x.get("prioridad") == "alta" and _int(x.get("dias_sin_comprar")) > 30]
    bullets.append(f"{len(important_lost)} clientes importantes llevan más de 30 días sin comprar.")

    month_label = filters.date_to.strftime("%B %Y")
    return {
        "title": f"Resumen Comercial — {month_label}",
        "bullets": bullets[:10],
        "period": dash["period"],
        "compare_period": dash["compare_period"],
    }


def list_filter_options() -> dict[str, Any]:
    sellers = _conn_query_all(
        """
        SELECT DISTINCT seller_name
        FROM distribuidora.v_sales
        WHERE is_sale = 1 AND seller_name IS NOT NULL
        ORDER BY seller_name
        """,
        (),
    )
    cities = _conn_query_all(
        """
        SELECT DISTINCT municipality
        FROM distribuidora.v_sales
        WHERE is_sale = 1 AND municipality IS NOT NULL AND TRIM(municipality) <> ''
        ORDER BY municipality
        """,
        (),
    )
    return {
        "sellers": [r["seller_name"] for r in sellers],
        "cities": [r["municipality"] for r in cities],
        "document_types": [
            {"id": "all", "label": "Todos"},
            {"id": "factura", "label": "Factura", "document_type_id": DOC_FACTURA},
            {"id": "boleta", "label": "Boleta", "document_type_id": DOC_BOLETA},
        ],
    }


def current_month_range() -> tuple[date, date]:
    today = datetime.now(timezone.utc).date()
    first = today.replace(day=1)
    return first, today


def previous_month_range() -> tuple[date, date]:
    today = datetime.now(timezone.utc).date()
    first_this = today.replace(day=1)
    last_prev = first_this - timedelta(days=1)
    first_prev = last_prev.replace(day=1)
    return first_prev, last_prev
