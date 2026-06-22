"""Consultas Analítica → Costos."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from backend.db import get_connection
from backend.repositories import cost_analytics_repo as repo
from backend.utils.cost_analytics_calc import (
    alert_semaphore,
    branch_spread_pct,
    classify_cost_alert,
)
from backend.utils.json_safe import serialize_value


def get_dashboard(
    company_id: int,
    *,
    office_id: int | None = None,
    date_from: datetime | None = None,
    date_to: datetime | None = None,
) -> dict[str, Any]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        kpis = repo.dashboard_kpis(
            cur,
            company_id,
            office_id=office_id,
            date_from=date_from,
            date_to=date_to,
        )
        sync = repo.get_sync_state(cur, company_id)
        offices = repo.list_offices(cur, company_id)
        cur.close()
    finally:
        conn.close()
    return serialize_value(
        {
            "company_id": company_id,
            "kpis": kpis,
            "last_sync": sync,
            "offices": offices,
        }
    )


def list_offices(company_id: int) -> dict[str, Any]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        offices = repo.list_offices(cur, company_id)
        cur.close()
    finally:
        conn.close()
    return serialize_value({"company_id": company_id, "items": offices})


def search_cost_history(company_id: int, q: str, *, limit: int = 50) -> dict[str, Any]:
    q = (q or "").strip()
    if len(q) < 2:
        raise ValueError("Ingrese al menos 2 caracteres para buscar.")
    conn = get_connection()
    try:
        cur = conn.cursor()
        hits = repo.search_variants(cur, company_id, q=q, limit=limit)
        cur.close()
    finally:
        conn.close()
    return serialize_value({"company_id": company_id, "q": q, "items": hits})


def list_history(
    company_id: int,
    *,
    q: str | None = None,
    variant_id: int | None = None,
    office_id: int | None = None,
    date_from: datetime | None = None,
    date_to: datetime | None = None,
    limit: int = 200,
    offset: int = 0,
) -> dict[str, Any]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        rows, total = repo.list_history_rows(
            cur,
            company_id,
            q=q,
            variant_id=variant_id,
            office_id=office_id,
            date_from=date_from,
            date_to=date_to,
            limit=limit,
            offset=offset,
        )
        cur.close()
    finally:
        conn.close()
    return serialize_value(
        {
            "company_id": company_id,
            "total": total,
            "limit": limit,
            "offset": offset,
            "items": rows,
        }
    )


def get_variant_cost_history(
    company_id: int, variant_id: int, *, limit: int = 200
) -> dict[str, Any]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        rows, _ = repo.list_history_rows(
            cur, company_id, variant_id=variant_id, limit=limit
        )
        ctx = repo.variant_tax_context(cur, company_id, variant_id)
        cur.execute(
            """
            SELECT average_cost_net, average_cost_gross
            FROM bsale.variant_cost
            WHERE company_id = %s AND variant_id = %s
            """,
            (company_id, variant_id),
        )
        vc_row = cur.fetchone()
        vc = None
        if vc_row:
            cols = [d[0] for d in cur.description]
            vc = dict(zip(cols, vc_row))
        cur.close()
    finally:
        conn.close()
    return serialize_value(
        {
            "company_id": company_id,
            "variant_id": variant_id,
            "product_name": ctx.get("product_name"),
            "variant_name": ctx.get("variant_name"),
            "barcode": ctx.get("barcode"),
            "average_cost": vc.get("average_cost_net") if vc else None,
            "average_cost_gross": vc.get("average_cost_gross") if vc else None,
            "items": rows,
        }
    )


def list_receptions(
    company_id: int,
    *,
    date_from: datetime | None = None,
    date_to: datetime | None = None,
    office_id: int | None = None,
    document_type: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> dict[str, Any]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        items, total = repo.list_receptions(
            cur,
            company_id,
            date_from=date_from,
            date_to=date_to,
            office_id=office_id,
            document_type=document_type,
            limit=limit,
            offset=offset,
        )
        cur.close()
    finally:
        conn.close()
    return serialize_value(
        {"company_id": company_id, "total": total, "limit": limit, "offset": offset, "items": items}
    )


def get_reception(company_id: int, reception_id: int) -> dict[str, Any]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        row = repo.get_reception_detail(cur, company_id, reception_id)
        cur.close()
    finally:
        conn.close()
    if not row:
        raise ValueError("Recepción no encontrada.")
    return serialize_value(row)


def list_alerts(
    company_id: int,
    *,
    office_id: int | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        raw = repo.list_cost_alerts(cur, company_id, office_id=office_id, limit=limit)
        cur.close()
    finally:
        conn.close()

    items: list[dict[str, Any]] = []
    for r in raw:
        spread = r.get("cross_branch_spread")
        kinds = classify_cost_alert(
            has_history=True,
            has_cost_row=not bool(r.get("missing_cost")),
            average_cost=float(r["average_cost"]) if r.get("average_cost") is not None else None,
            cost_net=float(r["cost_net"]) if r.get("cost_net") is not None else None,
            variation_pct=float(r["variation_pct"]) if r.get("variation_pct") is not None else None,
            cross_branch_spread=float(spread) if spread is not None else None,
            suspicious_reception=bool(r.get("suspicious_reception")),
        )
        if not kinds:
            continue
        r["alert_types"] = kinds
        r["semaphore"] = alert_semaphore(kinds)
        items.append(r)

    return serialize_value({"company_id": company_id, "items": items})


def compare_offices(
    company_id: int,
    *,
    variant_id: int | None = None,
    q: str | None = None,
) -> dict[str, Any]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        if variant_id is not None:
            data = repo.compare_offices_by_variant(cur, company_id, variant_id)
            if not data:
                raise ValueError("Sin datos de sucursales para esta variante.")
            spread = branch_spread_pct(
                float(data["min_cost_net"] or 0),
                float(data["max_cost_net"] or 0),
            )
            data["max_spread_pct"] = spread
            cur.close()
            return serialize_value({"company_id": company_id, "comparison": data})

        if not q or len(q.strip()) < 2:
            raise ValueError("Indique variant_id o búsqueda de al menos 2 caracteres.")
        variants = repo.search_compare_variants(cur, company_id, q.strip())
        comparisons: list[dict[str, Any]] = []
        for v in variants:
            cmp_data = repo.compare_offices_by_variant(cur, company_id, int(v["variant_id"]))
            if not cmp_data or len(cmp_data.get("offices") or []) < 2:
                continue
            spread = branch_spread_pct(
                float(cmp_data["min_cost_net"] or 0),
                float(cmp_data["max_cost_net"] or 0),
            )
            cmp_data["max_spread_pct"] = spread
            comparisons.append(cmp_data)
        comparisons.sort(
            key=lambda x: float(x.get("max_spread_pct") or 0),
            reverse=True,
        )
        cur.close()
    finally:
        conn.close()
    return serialize_value(
        {"company_id": company_id, "q": q, "items": comparisons}
    )
