"""Consultas sobre ``distribuidora.v_orders`` y ``distribuidora.v_sales`` (planificación / análisis)."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

from backend.db import get_connection
from backend.services.distribuidora.orders_service import _row_to_dict, _serialize_row

MAX_LIST_ROWS = 5000
DEFAULT_LIST_LIMIT = 500


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


def list_orders_view(
    *,
    start_date: date | None,
    end_date: date | None,
    seller: str | None,
    municipality: str | None,
    is_invoiced: bool | None,
    limit: int,
    offset: int,
) -> tuple[list[dict[str, Any]], int]:
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
    if is_invoiced is not None:
        params.append(is_invoiced)
        where.append("v.is_invoiced = %s")

    where_sql = " AND ".join(where)
    cap = min(max(1, limit), MAX_LIST_ROWS)

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            f"SELECT COUNT(*) FROM distribuidora.v_orders v WHERE {where_sql}",
            tuple(params),
        )
        total = int(cur.fetchone()[0])

        params2 = list(params)
        params2.extend([cap, offset])
        cur.execute(
            f"""
            SELECT v.*
            FROM distribuidora.v_orders v
            WHERE {where_sql}
            ORDER BY v.emission_date DESC NULLS LAST, v.document_id DESC
            LIMIT %s OFFSET %s
            """,
            tuple(params2),
        )
        items = [_serialize_row(_row_to_dict(cur, r)) for r in cur.fetchall()]
        cur.close()
        return items, total
    finally:
        conn.close()


def _summary_date_where(
    *,
    start_date: date | None,
    end_date: date | None,
    is_invoiced: bool | None,
) -> tuple[str, list[Any]]:
    where: list[str] = ["TRUE"]
    params: list[Any] = []
    if start_date is not None:
        params.append(start_date)
        where.append("v.emission_date::date >= %s")
    if end_date is not None:
        params.append(end_date)
        where.append("v.emission_date::date <= %s")
    if is_invoiced is not None:
        params.append(is_invoiced)
        where.append("v.is_invoiced = %s")
    return " AND ".join(where), params


def summary_orders_by_seller(
    *,
    start_date: date | None,
    end_date: date | None,
    is_invoiced: bool | None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    where_sql, params_list = _summary_date_where(
        start_date=start_date,
        end_date=end_date,
        is_invoiced=is_invoiced,
    )
    params = list(params_list)
    params.append(MAX_LIST_ROWS)
    sql = f"""
        SELECT
            v.seller_name,
            COUNT(*)::bigint AS total_orders,
            COALESCE(SUM(v.total_amount), 0) AS total_amount
        FROM distribuidora.v_orders v
        WHERE {where_sql}
        GROUP BY v.seller_name
        ORDER BY SUM(v.total_amount) DESC NULLS LAST
        LIMIT %s
    """
    items_raw = _conn_query_all(sql, tuple(params))
    items: list[dict[str, Any]] = []
    tot_orders = 0
    tot_amount = Decimal("0")
    for row in items_raw:
        n = int(row.get("total_orders") or 0)
        amt = row.get("total_amount")
        if isinstance(amt, Decimal):
            amt_d = amt
        elif amt is None:
            amt_d = Decimal("0")
        else:
            amt_d = Decimal(str(amt))
        tot_orders += n
        tot_amount += amt_d
        items.append(
            {
                "seller_name": row.get("seller_name"),
                "total_orders": n,
                "total_amount": float(amt_d),
            }
        )
    totals = {"total_orders": tot_orders, "total_amount": float(tot_amount)}
    return items, totals


def summary_orders_by_city(
    *,
    start_date: date | None,
    end_date: date | None,
    is_invoiced: bool | None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    where_sql, params_list = _summary_date_where(
        start_date=start_date,
        end_date=end_date,
        is_invoiced=is_invoiced,
    )
    params = list(params_list)
    params.append(MAX_LIST_ROWS)
    sql = f"""
        SELECT
            v.municipality,
            COUNT(*)::bigint AS total_orders,
            COALESCE(SUM(v.total_amount), 0) AS total_amount
        FROM distribuidora.v_orders v
        WHERE {where_sql}
        GROUP BY v.municipality
        ORDER BY SUM(v.total_amount) DESC NULLS LAST
        LIMIT %s
    """
    items_raw = _conn_query_all(sql, tuple(params))
    items = []
    tot_orders = 0
    tot_amount = Decimal("0")
    for row in items_raw:
        n = int(row.get("total_orders") or 0)
        amt = row.get("total_amount")
        if isinstance(amt, Decimal):
            amt_d = amt
        elif amt is None:
            amt_d = Decimal("0")
        else:
            amt_d = Decimal(str(amt))
        tot_orders += n
        tot_amount += amt_d
        items.append(
            {
                "municipality": row.get("municipality"),
                "total_orders": n,
                "total_amount": float(amt_d),
            }
        )
    totals = {"total_orders": tot_orders, "total_amount": float(tot_amount)}
    return items, totals


def list_sales_view(
    *,
    start_date: date | None,
    end_date: date | None,
    seller: str | None,
    municipality: str | None,
    limit: int,
    offset: int,
) -> tuple[list[dict[str, Any]], int]:
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

    where_sql = " AND ".join(where)
    cap = min(max(1, limit), MAX_LIST_ROWS)

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            f"SELECT COUNT(*) FROM distribuidora.v_sales v WHERE {where_sql}",
            tuple(params),
        )
        total = int(cur.fetchone()[0])

        params2 = list(params)
        params2.extend([cap, offset])
        cur.execute(
            f"""
            SELECT v.*
            FROM distribuidora.v_sales v
            WHERE {where_sql}
            ORDER BY v.emission_date DESC NULLS LAST, v.document_id DESC
            LIMIT %s OFFSET %s
            """,
            tuple(params2),
        )
        items = [_serialize_row(_row_to_dict(cur, r)) for r in cur.fetchall()]
        cur.close()
        return items, total
    finally:
        conn.close()
