"""Análisis de clientes a partir de ``distribuidora.v_sales`` (montos netos con NC)."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

from backend.db import get_connection
from backend.services.distribuidora.orders_service import _row_to_dict, _serialize_row

MAX_ANALYTICS_ROWS = 5000
CONSOLIDATED_DEFAULT_LIMIT = 1000


def _sales_from() -> str:
    return "FROM distribuidora.v_sales v"


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


def _sales_filters(
    *,
    start_date: date | None,
    end_date: date | None,
    seller: str | None,
    municipality: str | None,
) -> tuple[str, list[Any]]:
    where: list[str] = ["TRUE"]
    params: list[Any] = []
    if start_date is not None:
        params.append(start_date)
        where.append("v.emission_date::date >= %s")
    if end_date is not None:
        params.append(end_date)
        where.append("v.emission_date::date <= %s")
    if seller is not None and str(seller).strip() != "":
        params.append(seller.strip())
        where.append("v.seller_name = %s")
    if municipality is not None and str(municipality).strip() != "":
        params.append(municipality.strip())
        where.append("v.municipality = %s")
    return " AND ".join(where), params


def list_clients_consolidated(
    *,
    start_date: date | None,
    end_date: date | None,
    seller: str | None,
    municipality: str | None,
    limit: int,
    offset: int,
) -> list[dict[str, Any]]:
    where_sql, params = _sales_filters(
        start_date=start_date,
        end_date=end_date,
        seller=seller,
        municipality=municipality,
    )
    cap = min(max(1, limit), MAX_ANALYTICS_ROWS)
    params2 = list(params)
    params2.extend([cap, offset])
    sql = f"""
        SELECT
            v.client_id,
            MAX(v.client_name) AS client_name,
            COUNT(*)::bigint AS total_compras,
            COALESCE(SUM(v.total_amount), 0) AS total_comprado,
            COALESCE(AVG(v.total_amount), 0) AS ticket_promedio,
            MIN(v.emission_date) AS primera_compra,
            MAX(v.emission_date) AS ultima_compra
        {_sales_from()}
        WHERE {where_sql}
        GROUP BY v.client_id
        ORDER BY SUM(v.total_amount) DESC NULLS LAST
        LIMIT %s OFFSET %s
    """
    return _conn_query_all(sql, tuple(params2))


def list_clients_frequency(
    *,
    start_date: date | None,
    end_date: date | None,
    seller: str | None,
    municipality: str | None,
    limit: int,
) -> list[dict[str, Any]]:
    where_sql, params = _sales_filters(
        start_date=start_date,
        end_date=end_date,
        seller=seller,
        municipality=municipality,
    )
    cap = min(max(1, limit), MAX_ANALYTICS_ROWS)
    params2 = list(params)
    params2.append(cap)
    sql = f"""
        SELECT
            v.client_id,
            MAX(v.client_name) AS client_name,
            COUNT(*)::bigint AS compras,
            (
                (MAX((v.emission_date AT TIME ZONE 'UTC')::date)
                 - MIN((v.emission_date AT TIME ZONE 'UTC')::date))::numeric
                / NULLIF(COUNT(*)::numeric, 0)
            ) AS frecuencia_dias
        {_sales_from()}
        WHERE {where_sql}
        GROUP BY v.client_id
        ORDER BY frecuencia_dias ASC NULLS LAST
        LIMIT %s
    """
    return _conn_query_all(sql, tuple(params2))


def list_clients_inactive(*, days: int, limit: int) -> list[dict[str, Any]]:
    d = max(1, days)
    cap = min(max(1, limit), MAX_ANALYTICS_ROWS)
    sql = f"""
        SELECT
            v.client_id,
            MAX(v.client_name) AS client_name,
            MAX(v.emission_date) AS ultima_compra,
            (
                CURRENT_DATE
                - (MAX(v.emission_date) AT TIME ZONE 'UTC')::date
            )::int AS dias_sin_comprar
        {_sales_from()}
        GROUP BY v.client_id
        HAVING (CURRENT_DATE - (MAX(v.emission_date) AT TIME ZONE 'UTC')::date) > %s
        ORDER BY dias_sin_comprar DESC NULLS LAST
        LIMIT %s
    """
    return _conn_query_all(sql, (d, cap))


def list_clients_top(*, limit: int) -> list[dict[str, Any]]:
    cap = min(max(1, limit), MAX_ANALYTICS_ROWS)
    sql = f"""
        SELECT
            v.client_id,
            MAX(v.client_name) AS client_name,
            COALESCE(SUM(v.total_amount), 0) AS total
        {_sales_from()}
        GROUP BY v.client_id
        ORDER BY SUM(v.total_amount) DESC NULLS LAST
        LIMIT %s
    """
    return _conn_query_all(sql, (cap,))


def summary_clients_by_seller(*, limit: int) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    cap = min(max(1, limit), MAX_ANALYTICS_ROWS)
    sql = """
        SELECT
            v.seller_name,
            COUNT(DISTINCT v.client_id)::bigint AS clientes,
            COALESCE(SUM(v.total_amount), 0) AS ventas,
            COALESCE(AVG(v.total_amount), 0) AS ticket_promedio
        FROM distribuidora.v_sales v
        GROUP BY v.seller_name
        ORDER BY SUM(v.total_amount) DESC NULLS LAST
        LIMIT %s
    """
    items_raw = _conn_query_all(sql, (cap,))
    items: list[dict[str, Any]] = []
    ventas_total = Decimal("0")
    for row in items_raw:
        vta = row.get("ventas")
        if isinstance(vta, Decimal):
            vd = vta
        elif vta is None:
            vd = Decimal("0")
        else:
            vd = Decimal(str(vta))
        ventas_total += vd
        items.append(
            {
                "seller_name": row.get("seller_name"),
                "clientes": int(row.get("clientes") or 0),
                "ventas": float(vd),
                "ticket_promedio": float(row.get("ticket_promedio") or 0),
            }
        )
    totals = {
        "sellers": len(items),
        "ventas_total": float(ventas_total),
    }
    return items, totals
