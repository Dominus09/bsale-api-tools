"""Análisis desde ``distribuidora.v_sales``: total neto con NC; ticket = solo tipos 1 y 6 (Bsale)."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
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
            COALESCE(SUM(v.is_sale), 0)::bigint AS total_compras,
            COALESCE(SUM(v.total_amount_net), 0) AS total_comprado,
            COALESCE(
                SUM(v.total_amount_sales) / NULLIF(SUM(v.is_sale)::numeric, 0),
                0
            ) AS ticket_promedio,
            MIN(v.emission_date) FILTER (WHERE v.is_sale = 1) AS primera_compra,
            MAX(v.emission_date) FILTER (WHERE v.is_sale = 1) AS ultima_compra,
            (
                ARRAY_AGG(v.seller_name ORDER BY v.emission_date DESC NULLS LAST)
                FILTER (WHERE v.is_sale = 1)
            )[1] AS vendedor
        {_sales_from()}
        WHERE {where_sql}
        GROUP BY v.client_id
        ORDER BY SUM(v.total_amount_net) DESC NULLS LAST
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
            COALESCE(SUM(v.is_sale), 0)::bigint AS compras,
            (
                (
                    MAX((v.emission_date AT TIME ZONE 'UTC')::date)
                    FILTER (WHERE v.is_sale = 1)
                    - MIN((v.emission_date AT TIME ZONE 'UTC')::date)
                    FILTER (WHERE v.is_sale = 1)
                )::numeric
                / NULLIF(SUM(v.is_sale)::numeric, 0)
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
            MAX(v.emission_date) FILTER (WHERE v.is_sale = 1) AS ultima_compra,
            (
                CURRENT_DATE
                - (MAX(v.emission_date) FILTER (WHERE v.is_sale = 1) AT TIME ZONE 'UTC')::date
            )::int AS dias_sin_comprar,
            (
                ARRAY_AGG(v.seller_name ORDER BY v.emission_date DESC NULLS LAST)
                FILTER (WHERE v.is_sale = 1)
            )[1] AS vendedor
        {_sales_from()}
        GROUP BY v.client_id
        HAVING SUM(v.is_sale) > 0
           AND (
               CURRENT_DATE
               - (MAX(v.emission_date) FILTER (WHERE v.is_sale = 1) AT TIME ZONE 'UTC')::date
           ) >= %s
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
            COALESCE(SUM(v.total_amount_net), 0) AS total
        {_sales_from()}
        GROUP BY v.client_id
        ORDER BY SUM(v.total_amount_net) DESC NULLS LAST
        LIMIT %s
    """
    return _conn_query_all(sql, (cap,))


def summary_clients_by_seller(
    *,
    limit: int,
    start_date: date | None = None,
    end_date: date | None = None,
    seller_ids: list[int] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    cap = min(max(1, limit), MAX_ANALYTICS_ROWS)
    where_extra: list[str] = []
    params: list[Any] = []
    if start_date is not None:
        params.append(start_date)
        where_extra.append("v.emission_date::date >= %s")
    if end_date is not None:
        params.append(end_date)
        where_extra.append("v.emission_date::date <= %s")
    if seller_ids:
        params.append(tuple(int(x) for x in seller_ids))
        where_extra.append("v.seller_id IN %s")
    where_sql = (" AND " + " AND ".join(where_extra)) if where_extra else ""
    params.append(cap)

    sql = f"""
        WITH filt AS (
            SELECT *
            FROM distribuidora.v_sales v
            WHERE 1=1{where_sql}
        ),
        agg AS (
            SELECT
                seller_name,
                COUNT(DISTINCT client_id)::bigint AS clientes,
                COALESCE(SUM(total_amount_net), 0) AS ventas,
                COALESCE(
                    SUM(total_amount_sales) / NULLIF(SUM(is_sale)::numeric, 0),
                    0
                ) AS ticket_promedio
            FROM filt
            GROUP BY seller_name
        ),
        inact AS (
            SELECT ult.seller_name AS sn, COUNT(DISTINCT ult.client_id)::bigint AS n
            FROM (
                SELECT DISTINCT ON (v2.client_id)
                    v2.client_id,
                    v2.seller_name,
                    (v2.emission_date AT TIME ZONE 'UTC')::date AS last_d
                FROM distribuidora.v_sales v2
                WHERE v2.is_sale = 1
                ORDER BY v2.client_id, v2.emission_date DESC NULLS LAST, v2.document_id DESC
            ) ult
            WHERE (CURRENT_DATE - ult.last_d) > 30
            GROUP BY ult.seller_name
        )
        SELECT
            a.seller_name,
            a.clientes,
            a.ventas,
            a.ticket_promedio,
            COALESCE(i.n, 0)::bigint AS clientes_inactivos
        FROM agg a
        LEFT JOIN inact i ON i.sn IS NOT DISTINCT FROM a.seller_name
        ORDER BY a.ventas DESC NULLS LAST
        LIMIT %s
    """
    items_raw = _conn_query_all(sql, tuple(params))
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
                "clientes_inactivos": int(row.get("clientes_inactivos") or 0),
            }
        )
    totals = {
        "sellers": len(items),
        "ventas_total": float(ventas_total),
    }
    return items, totals


def sales_daily_totals(*, start_date: date, end_date: date) -> list[dict[str, Any]]:
    sql = """
        SELECT
            (v.emission_date AT TIME ZONE 'UTC')::date AS day,
            COALESCE(SUM(v.total_amount_net), 0) AS total_net
        FROM distribuidora.v_sales v
        WHERE (v.emission_date AT TIME ZONE 'UTC')::date >= %s
          AND (v.emission_date AT TIME ZONE 'UTC')::date <= %s
        GROUP BY 1
        ORDER BY 1
    """
    return _conn_query_all(sql, (start_date, end_date))


def month_dashboard_kpis(*, year: int, month: int) -> dict[str, Any]:
    first = date(year, month, 1)
    if month == 12:
        next_first = date(year + 1, 1, 1)
    else:
        next_first = date(year, month + 1, 1)
    first_dt = datetime(first.year, first.month, first.day, 0, 0, 0, tzinfo=timezone.utc)
    next_dt = datetime(next_first.year, next_first.month, next_first.day, 0, 0, 0, tzinfo=timezone.utc)
    sql = """
        SELECT
            COALESCE(SUM(total_amount_net), 0) AS ventas_mes,
            COALESCE(
                SUM(total_amount_sales) / NULLIF(SUM(is_sale)::numeric, 0),
                0
            ) AS ticket_mes,
            COUNT(DISTINCT client_id) FILTER (WHERE is_sale = 1)::bigint AS clientes_activos
        FROM distribuidora.v_sales
        WHERE emission_date >= %s
          AND emission_date < %s
    """
    rows = _conn_query_all(sql, (first_dt, next_dt))
    return dict(rows[0]) if rows else {}


def recover_clients_top(*, min_days: int = 7, limit: int = 10) -> list[dict[str, Any]]:
    lim = min(max(1, limit), 200)
    d = max(1, min_days)
    sql = f"""
        SELECT
            s.client_id,
            MAX(s.client_name) AS client_name,
            (
                ARRAY_AGG(s.seller_name ORDER BY s.emission_date DESC NULLS LAST)
                FILTER (WHERE s.is_sale = 1)
            )[1] AS vendedor,
            MAX(s.emission_date) FILTER (WHERE s.is_sale = 1) AS ultima_compra,
            (
                CURRENT_DATE
                - (MAX(s.emission_date) FILTER (WHERE s.is_sale = 1) AT TIME ZONE 'UTC')::date
            )::int AS dias_sin_comprar,
            COALESCE(SUM(s.total_amount_net), 0) AS valor_historico_neto
        FROM distribuidora.v_sales s
        GROUP BY s.client_id
        HAVING SUM(s.is_sale) > 0
           AND (
               CURRENT_DATE
               - (MAX(s.emission_date) FILTER (WHERE s.is_sale = 1) AT TIME ZONE 'UTC')::date
           ) >= %s
        ORDER BY dias_sin_comprar DESC NULLS LAST, valor_historico_neto DESC NULLS LAST
        LIMIT %s
    """
    return _conn_query_all(sql, (d, lim))


def clients_commercial_dashboard(
    *,
    chart_days: int = 30,
    kpi_year: int | None = None,
    kpi_month: int | None = None,
    recover_min_days: int = 7,
) -> dict[str, Any]:
    today = datetime.now(timezone.utc).date()
    chart_days = max(7, min(chart_days, 120))
    start_chart = today - timedelta(days=chart_days - 1)
    y = kpi_year if kpi_year is not None else today.year
    m = kpi_month if kpi_month is not None else today.month
    m = max(1, min(12, m))
    if y < 2000 or y > 2100:
        y = today.year
    daily = sales_daily_totals(start_date=start_chart, end_date=today)
    sellers, st = summary_clients_by_seller(
        limit=30,
        start_date=start_chart,
        end_date=today,
        seller_ids=None,
    )
    kpis = month_dashboard_kpis(year=y, month=m)
    recover = recover_clients_top(min_days=recover_min_days, limit=10)
    return {
        "chart_range": {"start": start_chart.isoformat(), "end": today.isoformat(), "days": chart_days},
        "kpi_month": {"year": y, "month": m, "label": f"{y}-{m:02d}"},
        "daily_sales": daily,
        "sales_by_seller": sellers,
        "seller_totals": st,
        "kpis": kpis,
        "recover_clients": recover,
    }
