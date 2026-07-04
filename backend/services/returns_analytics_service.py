"""Análisis de Notas de Crédito — dashboard, rankings, ficha, mapa, timeline, insights."""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any

from backend.config.returns_scope import (
    COMPANY_ID,
    COMPANY_NAME,
    HISTORY_DATE_FROM,
    HISTORY_DATE_TO,
    MODULE_VERSION,
    OFFICE_ID,
    OFFICE_NAME,
)
from backend.db import get_connection
from backend.repositories import returns_analytics_repo as repo
from backend.services.distribuidora.orders_service import _row_to_dict, _serialize_row

logger = logging.getLogger(__name__)

SCOPE_WHERE = "r.company_id = %s AND r.office_id = %s"
DOC_TYPES_SALE = (1, 6)


def _float(v: Any) -> float:
    if v is None:
        return 0.0
    return float(v)


def _int(v: Any) -> int:
    if v is None:
        return 0
    return int(v)


def _period_bounds(
    date_from: date | None,
    date_to: date | None,
) -> tuple[datetime, datetime]:
    today = datetime.now(timezone.utc).date()
    d0 = date_from or today.replace(day=1)
    d1 = date_to or today
    start = datetime.combine(d0, datetime.min.time()).replace(tzinfo=timezone.utc)
    end = datetime.combine(d1, datetime.max.time()).replace(tzinfo=timezone.utc)
    return start, end


def _query_all(sql: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
    conn = get_connection()
    conn.autocommit = True
    cur = conn.cursor()
    try:
        cur.execute(sql, params)
        rows = cur.fetchall()
        return [_serialize_row(_row_to_dict(cur, r)) for r in rows]
    finally:
        cur.close()
        conn.close()


def _query_one(sql: str, params: tuple[Any, ...]) -> dict[str, Any] | None:
    rows = _query_all(sql, params)
    return rows[0] if rows else None


def _sales_net_period(start: datetime, end: datetime) -> float:
    row = _query_one(
        """
        SELECT COALESCE(SUM(v.total_amount_net), 0) AS sales_net
        FROM distribuidora.v_sales v
        WHERE v.document_type_id IN %s
          AND (v.emission_date AT TIME ZONE 'UTC') >= %s
          AND (v.emission_date AT TIME ZONE 'UTC') <= %s
        """,
        (DOC_TYPES_SALE, start, end),
    )
    return round(_float(row.get("sales_net") if row else 0), 2)


def get_sync_status() -> dict[str, Any]:
    conn = get_connection()
    cur = conn.cursor()
    try:
        repo.ensure_returns_schema(cur)
        conn.commit()
        state = repo.get_sync_state(cur, company_id=COMPANY_ID, office_id=OFFICE_ID)
        history = repo.get_completed_history_sync(
            cur,
            company_id=COMPANY_ID,
            office_id=OFFICE_ID,
        )
        resumable = repo.get_resumable_history_sync(
            cur,
            company_id=COMPANY_ID,
            office_id=OFFICE_ID,
        )
        runs = repo.list_sync_runs(cur, company_id=COMPANY_ID, office_id=OFFICE_ID, limit=8)
        last_history = next((r for r in runs if r.get("sync_type") == "history"), None)
    finally:
        cur.close()
        conn.close()

    def _iso(v: Any) -> str | None:
        if v is None:
            return None
        if hasattr(v, "isoformat"):
            return v.isoformat()
        return str(v)

    return {
        "company_id": COMPANY_ID,
        "office_id": OFFICE_ID,
        "bootstrap": {
            "scope": "full_catalog",
            "company_id": COMPANY_ID,
            "office_id": OFFICE_ID,
            "completed": history is not None,
            "completed_at": _iso(history.get("finished_at")) if history else None,
            "records_processed": _int(history.get("records_processed")) if history else 0,
            "resumable": resumable is not None,
            "resumable_sync_id": resumable.get("id") if resumable else None,
            "pages_processed": _int(resumable.get("pages_processed")) if resumable else 0,
            "last_history_status": last_history.get("status") if last_history else None,
            "last_history_sync_id": last_history.get("id") if last_history else None,
        },
        "cursor": {
            "last_sync_at": _iso(state.get("last_sync_at")) if state else None,
            "records_total": _int(state.get("records_total")) if state else 0,
            "last_return_ts": state.get("last_return_ts") if state else None,
        },
        "recent_runs": [
            {
                "id": r.get("id"),
                "sync_type": r.get("sync_type"),
                "status": r.get("status"),
                "date_from": _iso(r.get("date_from")),
                "date_to": _iso(r.get("date_to")),
                "pages_processed": _int(r.get("pages_processed")),
                "records_processed": _int(r.get("records_processed")),
                "started_at": _iso(r.get("started_at")),
                "finished_at": _iso(r.get("finished_at")),
                "duration_ms": r.get("duration_ms"),
                "error_message": r.get("error_message"),
            }
            for r in runs
        ],
    }


def get_dashboard(
    *,
    date_from: date | None = None,
    date_to: date | None = None,
) -> dict[str, Any]:
    start, end = _period_bounds(date_from, date_to)
    period_sql, period_params = repo._period_filter(start, end)

    agg = _query_one(
        f"""
        SELECT
            COUNT(*)::bigint AS total_nc,
            COALESCE(SUM(r.amount), 0) AS total_amount,
            COUNT(DISTINCT r.client_id) FILTER (WHERE r.client_id IS NOT NULL) AS clients_affected,
            COUNT(DISTINCT rd.variant_id) FILTER (WHERE rd.variant_id IS NOT NULL) AS products_affected
        FROM bsale.returns r
        LEFT JOIN bsale.return_details rd
            ON rd.company_id = r.company_id AND rd.return_id = r.bsale_id
        WHERE {SCOPE_WHERE}
        {period_sql}
        """,
        (COMPANY_ID, OFFICE_ID, *period_params),
    ) or {}

    total_nc = _int(agg.get("total_nc"))
    total_amount = round(_float(agg.get("total_amount")), 2)
    sales_net = _sales_net_period(start, end)
    pct_sales = round((total_amount / sales_net * 100.0) if sales_net > 0 else 0.0, 2)
    ticket_avg = round(total_amount / total_nc, 2) if total_nc > 0 else 0.0

    return {
        "scope": {
            "company_id": COMPANY_ID,
            "company_name": COMPANY_NAME,
            "office_id": OFFICE_ID,
            "office_name": OFFICE_NAME,
            "module_version": MODULE_VERSION,
        },
        "period": {"from": start.date().isoformat(), "to": end.date().isoformat()},
        "kpis": {
            "total_nc": total_nc,
            "total_amount": total_amount,
            "pct_over_sales": pct_sales,
            "sales_net_period": sales_net,
            "ticket_promedio_nc": ticket_avg,
            "clients_affected": _int(agg.get("clients_affected")),
            "products_affected": _int(agg.get("products_affected")),
        },
        "sync": get_sync_status(),
    }


def _ranking_motives(start: datetime, end: datetime) -> list[dict[str, Any]]:
    period_sql, period_params = repo._period_filter(start, end)
    rows = _query_all(
        f"""
        WITH base AS (
            SELECT
                COALESCE(NULLIF(BTRIM(r.motive), ''), 'Sin motivo') AS motive,
                r.amount
            FROM bsale.returns r
            WHERE {SCOPE_WHERE}
            {period_sql}
        ),
        totals AS (SELECT COALESCE(SUM(amount), 0) AS grand FROM base)
        SELECT
            b.motive,
            COUNT(*)::bigint AS quantity,
            COALESCE(SUM(b.amount), 0) AS amount,
            ROUND(
                COALESCE(SUM(b.amount), 0) * 100.0 / NULLIF(t.grand, 0),
                2
            ) AS pct
        FROM base b
        CROSS JOIN totals t
        GROUP BY b.motive, t.grand
        ORDER BY amount DESC
        """,
        (COMPANY_ID, OFFICE_ID, *period_params),
    )
    return [
        {
            "motive": r["motive"],
            "quantity": _int(r["quantity"]),
            "amount": round(_float(r["amount"]), 2),
            "pct": round(_float(r["pct"]), 2),
        }
        for r in rows
    ]


def _ranking_sellers(start: datetime, end: datetime, sales_net: float) -> list[dict[str, Any]]:
    period_sql, period_params = repo._period_filter(start, end)
    rows = _query_all(
        f"""
        SELECT
            COALESCE(r.seller_id, 0) AS seller_id,
            COALESCE(NULLIF(BTRIM(r.seller_name), ''), 'Sin vendedor') AS seller,
            COUNT(*)::bigint AS quantity,
            COALESCE(SUM(r.amount), 0) AS amount,
            ARRAY_AGG(DISTINCT COALESCE(NULLIF(BTRIM(r.motive), ''), 'Sin motivo'))
                FILTER (WHERE r.motive IS NOT NULL) AS motives
        FROM bsale.returns r
        WHERE {SCOPE_WHERE}
        {period_sql}
        GROUP BY r.seller_id, r.seller_name
        ORDER BY amount DESC
        """,
        (COMPANY_ID, OFFICE_ID, *period_params),
    )
    out: list[dict[str, Any]] = []
    for r in rows:
        amt = round(_float(r["amount"]), 2)
        motives = r.get("motives") or []
        if isinstance(motives, str):
            motives = [motives]
        out.append({
            "seller_id": _int(r["seller_id"]),
            "seller": str(r["seller"]),
            "quantity": _int(r["quantity"]),
            "amount": amt,
            "pct_over_sales": round((amt / sales_net * 100.0) if sales_net > 0 else 0.0, 2),
            "motives": list(motives)[:8],
        })
    return out


def _ranking_clients(start: datetime, end: datetime) -> list[dict[str, Any]]:
    period_sql, period_params = repo._period_filter(start, end)
    rows = _query_all(
        f"""
        SELECT
            r.client_id,
            COALESCE(NULLIF(BTRIM(r.client_name), ''), 'Cliente ' || r.client_id::text) AS client,
            COUNT(*)::bigint AS quantity,
            COALESCE(SUM(r.amount), 0) AS amount,
            MAX(r.return_date) AS last_return
        FROM bsale.returns r
        WHERE {SCOPE_WHERE}
          AND r.client_id IS NOT NULL
        {period_sql}
        GROUP BY r.client_id, r.client_name
        ORDER BY amount DESC
        LIMIT 100
        """,
        (COMPANY_ID, OFFICE_ID, *period_params),
    )
    return [
        {
            "client_id": _int(r["client_id"]),
            "client": str(r["client"]),
            "quantity": _int(r["quantity"]),
            "amount": round(_float(r["amount"]), 2),
            "last_return": str(r["last_return"])[:10] if r.get("last_return") else None,
        }
        for r in rows
    ]


def _ranking_products(start: datetime, end: datetime) -> list[dict[str, Any]]:
    period_sql, period_params = repo._period_filter(start, end)
    rows = _query_all(
        f"""
        SELECT
            rd.variant_id,
            COALESCE(NULLIF(BTRIM(rd.product_name), ''), rd.variant_description, 'Variante ' || rd.variant_id::text) AS product,
            COALESCE(SUM(rd.quantity), 0) AS quantity,
            COALESCE(SUM(rd.total_amount), 0) AS amount,
            COUNT(DISTINCT r.bsale_id)::bigint AS return_count
        FROM bsale.returns r
        INNER JOIN bsale.return_details rd
            ON rd.company_id = r.company_id AND rd.return_id = r.bsale_id
        WHERE {SCOPE_WHERE}
        {period_sql}
        GROUP BY rd.variant_id, rd.product_name, rd.variant_description
        ORDER BY amount DESC
        LIMIT 100
        """,
        (COMPANY_ID, OFFICE_ID, *period_params),
    )
    return [
        {
            "variant_id": _int(r["variant_id"]),
            "product": str(r["product"]),
            "quantity": round(_float(r["quantity"]), 2),
            "amount": round(_float(r["amount"]), 2),
            "return_count": _int(r["return_count"]),
        }
        for r in rows
    ]


def get_rankings(
    *,
    date_from: date | None = None,
    date_to: date | None = None,
) -> dict[str, Any]:
    start, end = _period_bounds(date_from, date_to)
    sales_net = _sales_net_period(start, end)
    motives = _ranking_motives(start, end)

    prev_days = (end.date() - start.date()).days + 1
    prev_end = start - timedelta(seconds=1)
    prev_start = prev_end - timedelta(days=max(prev_days - 1, 0))
    prev_motives = {m["motive"]: m for m in _ranking_motives(prev_start, prev_end)}

    motives_with_trend: list[dict[str, Any]] = []
    for m in motives:
        prev = prev_motives.get(m["motive"], {})
        prev_amt = _float(prev.get("amount"))
        delta = m["amount"] - prev_amt
        motives_with_trend.append({
            **m,
            "trend_delta": round(delta, 2),
            "trend_pct": round((delta / prev_amt * 100.0) if prev_amt else 0.0, 1),
        })

    return {
        "motives": motives_with_trend,
        "sellers": _ranking_sellers(start, end, sales_net),
        "clients": _ranking_clients(start, end),
        "products": _ranking_products(start, end),
    }


def list_returns(
    *,
    date_from: date | None = None,
    date_to: date | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    start, end = _period_bounds(date_from, date_to)
    period_sql, period_params = repo._period_filter(start, end)
    lim = max(1, min(limit, 200))
    rows = _query_all(
        f"""
        SELECT
            r.bsale_id AS return_id,
            r.credit_note_number,
            r.return_date,
            r.motive,
            r.amount,
            r.client_name,
            r.seller_name,
            r.municipality
        FROM bsale.returns r
        WHERE {SCOPE_WHERE}
        {period_sql}
        ORDER BY r.return_date DESC NULLS LAST, r.bsale_id DESC
        LIMIT %s
        """,
        (COMPANY_ID, OFFICE_ID, *period_params, lim),
    )
    return [
        {
            "return_id": _int(r["return_id"]),
            "credit_note_number": r.get("credit_note_number"),
            "return_date": str(r["return_date"])[:10] if r.get("return_date") else None,
            "motive": r.get("motive"),
            "amount": round(_float(r["amount"]), 2),
            "client": r.get("client_name"),
            "seller": r.get("seller_name"),
            "municipality": r.get("municipality"),
        }
        for r in rows
    ]


def get_return_detail(return_id: int) -> dict[str, Any]:
    header = _query_one(
        f"""
        SELECT r.*,
               cn.url_public_view AS credit_note_url
        FROM bsale.returns r
        LEFT JOIN distribuidora.v_documents_latest cn
            ON cn.document_id = r.credit_note_id
        WHERE r.company_id = %s AND r.bsale_id = %s
        """,
        (COMPANY_ID, return_id),
    )
    if not header:
        raise ValueError(f"Devolución {return_id} no encontrada")

    details = _query_all(
        """
        SELECT
            rd.*,
            COALESCE(vc.average_cost_net, 0) AS unit_cost,
            COALESCE(rd.total_amount, 0) - COALESCE(vc.average_cost_net, 0) * COALESCE(rd.quantity, 0) AS margin_estimated
        FROM bsale.return_details rd
        LEFT JOIN bsale.variant_cost vc
            ON vc.company_id = rd.company_id AND vc.variant_id = rd.variant_id
        WHERE rd.company_id = %s AND rd.return_id = %s
        ORDER BY rd.bsale_detail_id
        """,
        (COMPANY_ID, return_id),
    )

    lines: list[dict[str, Any]] = []
    total_margin = 0.0
    for d in details:
        margin = round(_float(d.get("margin_estimated")), 2)
        total_margin += margin
        lines.append({
            "variant_id": _int(d.get("variant_id")),
            "product": d.get("product_name") or d.get("variant_description"),
            "quantity": round(_float(d.get("quantity")), 2),
            "unit_value": round(_float(d.get("unit_value")), 2),
            "total_amount": round(_float(d.get("total_amount")), 2),
            "unit_cost": round(_float(d.get("unit_cost")), 2),
            "margin_estimated": margin,
        })

    return {
        "return_id": return_id,
        "header": {
            "number": header.get("credit_note_number") or header.get("code"),
            "return_date": str(header.get("return_date"))[:10] if header.get("return_date") else None,
            "motive": header.get("motive"),
            "amount": round(_float(header.get("amount")), 2),
            "client": header.get("client_name"),
            "seller": header.get("seller_name"),
            "municipality": header.get("municipality"),
            "reference_document": {
                "id": header.get("reference_document_id"),
                "number": header.get("reference_document_number"),
                "type_id": header.get("reference_document_type_id"),
                "emission": str(header.get("reference_emission"))[:10] if header.get("reference_emission") else None,
            },
            "credit_note": {
                "id": header.get("credit_note_id"),
                "number": header.get("credit_note_number"),
                "emission": str(header.get("credit_note_emission"))[:10] if header.get("credit_note_emission") else None,
                "url": header.get("credit_note_url"),
            },
        },
        "lines": lines,
        "margin_estimated_total": round(total_margin, 2),
    }


def get_map_data(
    *,
    date_from: date | None = None,
    date_to: date | None = None,
) -> list[dict[str, Any]]:
    start, end = _period_bounds(date_from, date_to)
    period_sql, period_params = repo._period_filter(start, end)
    rows = _query_all(
        f"""
        SELECT
            COALESCE(NULLIF(BTRIM(r.municipality), ''), 'Sin comuna') AS municipality,
            COUNT(*)::bigint AS quantity,
            COALESCE(SUM(r.amount), 0) AS amount,
            MODE() WITHIN GROUP (ORDER BY COALESCE(NULLIF(BTRIM(r.motive), ''), 'Sin motivo')) AS top_motive
        FROM bsale.returns r
        WHERE {SCOPE_WHERE}
        {period_sql}
        GROUP BY COALESCE(NULLIF(BTRIM(r.municipality), ''), 'Sin comuna')
        ORDER BY amount DESC
        """,
        (COMPANY_ID, OFFICE_ID, *period_params),
    )
    return [
        {
            "municipality": str(r["municipality"]),
            "quantity": _int(r["quantity"]),
            "amount": round(_float(r["amount"]), 2),
            "top_motive": str(r.get("top_motive") or ""),
        }
        for r in rows
    ]


def get_timeline(
    *,
    date_from: date | None = None,
    date_to: date | None = None,
    grain: str = "day",
) -> list[dict[str, Any]]:
    start, end = _period_bounds(date_from, date_to)
    period_sql, period_params = repo._period_filter(start, end)
    trunc = {
        "day": "day",
        "week": "week",
        "month": "month",
        "year": "year",
    }.get(grain, "day")

    rows = _query_all(
        f"""
        SELECT
            DATE_TRUNC(%s, r.return_date AT TIME ZONE 'UTC')::date AS bucket,
            COUNT(*)::bigint AS quantity,
            COALESCE(SUM(r.amount), 0) AS amount
        FROM bsale.returns r
        WHERE {SCOPE_WHERE}
          AND r.return_date IS NOT NULL
        {period_sql}
        GROUP BY 1
        ORDER BY 1
        """,
        (trunc, COMPANY_ID, OFFICE_ID, *period_params),
    )
    return [
        {
            "bucket": str(r["bucket"]),
            "quantity": _int(r["quantity"]),
            "amount": round(_float(r["amount"]), 2),
        }
        for r in rows
    ]


def get_insights(
    *,
    date_from: date | None = None,
    date_to: date | None = None,
) -> dict[str, Any]:
    start, end = _period_bounds(date_from, date_to)
    rankings = get_rankings(date_from=start.date(), date_to=end.date())
    motives = rankings["motives"]
    sellers = rankings["sellers"]
    clients = rankings["clients"]
    products = rankings["products"]
    sales_net = _sales_net_period(start, end)

    insights: list[dict[str, Any]] = []
    recommendations: list[str] = []

    if motives:
        top = motives[0]
        insights.append({
            "type": "motive",
            "severity": "high",
            "title": "Motivo con mayor pérdida",
            "description": f"«{top['motive']}» concentra {formatCLP(top['amount'])} ({top['pct']}% del total NC).",
            "impact": top["amount"],
        })
        recommendations.append(
            f"Revisar proceso operativo para «{top['motive']}» con el equipo de bodega y ventas."
        )

    if sellers:
        worst = max(sellers, key=lambda s: s.get("pct_over_sales", 0))
        if worst["pct_over_sales"] > 0:
            insights.append({
                "type": "seller",
                "severity": "medium",
                "title": "Vendedor con mayor % NC / ventas",
                "description": (
                    f"{worst['seller']}: {worst['pct_over_sales']}% sobre ventas netas del período."
                ),
                "impact": worst["amount"],
            })
            recommendations.append(f"Auditar cartera y motivos de devolución de {worst['seller']}.")

    repeat_clients = [c for c in clients if c["quantity"] >= 2]
    if repeat_clients:
        rc = repeat_clients[0]
        insights.append({
            "type": "client",
            "severity": "medium",
            "title": "Cliente reincidente",
            "description": f"{rc['client']}: {rc['quantity']} NC por {formatCLP(rc['amount'])}.",
            "impact": rc["amount"],
        })
        recommendations.append(f"Contactar a {rc['client']} para entender causas recurrentes.")

    if products:
        tp = products[0]
        insights.append({
            "type": "product",
            "severity": "medium",
            "title": "Producto con más devoluciones",
            "description": f"«{tp['product']}»: {tp['return_count']} devoluciones, {formatCLP(tp['amount'])}.",
            "impact": tp["amount"],
        })
        recommendations.append(f"Verificar calidad, caducidad o picking de «{tp['product']}».")

    if sales_net > 0 and motives:
        total_nc = sum(m["amount"] for m in motives)
        if total_nc / sales_net > 0.03:
            recommendations.append(
                "El ratio NC/ventas supera 3% — priorizar reunión de pérdidas con gerencia."
            )

    return {
        "insights": insights,
        "recommendations": recommendations,
        "generated_by": "returns-analytics-rules-v1",
    }


def formatCLP(n: float) -> str:
    return f"${n:,.0f}".replace(",", ".")
