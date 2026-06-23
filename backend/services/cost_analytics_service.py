"""Consultas Analítica → Costos."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from backend.db import get_connection
from backend.repositories import cost_analytics_repo as repo
from backend.utils.cost_analytics_calc import (
    COMMERCIAL_SCORE_LABELS,
    OPPORTUNITY_LABELS,
    WATCHLIST_STATUS_LABELS,
    alert_semaphore,
    alert_to_action,
    branch_spread_pct,
    classify_cost_alert,
    commercial_score,
    spread_semaphore,
    watchlist_status,
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
        opp_counts = repo.list_purchase_opportunities(
            cur, company_id, limit=10000, offset=0
        )[2]
        kpis["opportunities_buy"] = opp_counts.get("oportunidad_compra", 0)
        kpis["opportunities_risk"] = opp_counts.get("riesgo_comercial", 0)
        kpis["opportunities_detected"] = (
            kpis["opportunities_buy"] + kpis["opportunities_risk"]
        )
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
            "product_id": ctx.get("product_id"),
            "average_cost": vc.get("average_cost_net") if vc else None,
            "average_cost_gross": vc.get("average_cost_gross") if vc else None,
            "items": rows,
            "chart_series": [
                {
                    "date": r.get("admission_date"),
                    "cost_net": r.get("cost_net"),
                    "cost_bruto_erp": r.get("cost_bruto_erp"),
                    "average_cost": r.get("average_cost"),
                    "office_name": r.get("office_name"),
                }
                for r in rows
            ],
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


def list_products(
    company_id: int,
    *,
    q: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> dict[str, Any]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        rows, total = repo.list_products(
            cur, company_id, q=q, limit=limit, offset=offset
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


def list_opportunities(
    company_id: int,
    *,
    status: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> dict[str, Any]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        items, total, counts = repo.list_purchase_opportunities(
            cur, company_id, status=status, limit=limit, offset=offset
        )
        enriched = []
        for row in items:
            r = dict(row)
            st = r.get("status")
            r["status_label"] = OPPORTUNITY_LABELS.get(st, "") if st else None
            r["semaphore"] = (
                "green" if st == "oportunidad_compra"
                else "red" if st == "riesgo_comercial"
                else "yellow"
            )
            enriched.append(r)
        cur.close()
    finally:
        conn.close()
    return serialize_value(
        {
            "company_id": company_id,
            "total": total,
            "counts": counts,
            "limit": limit,
            "offset": offset,
            "items": enriched,
        }
    )


def list_branch_comparison(
    company_id: int,
    *,
    q: str | None = None,
    limit: int = 20,
) -> dict[str, Any]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        rows = repo.list_branch_comparison_ranked(
            cur, company_id, q=q, limit=limit
        )
        for r in rows:
            r["semaphore"] = spread_semaphore(
                float(r["internal_variation_pct"])
                if r.get("internal_variation_pct") is not None
                else None
            )
        cur.close()
    finally:
        conn.close()
    return serialize_value({"company_id": company_id, "items": rows})


def get_margin_impact(company_id: int, variant_id: int) -> dict[str, Any]:
    """Stub preparado para Política de Márgenes — sin implementar cálculo."""
    return serialize_value(
        {
            "company_id": company_id,
            "variant_id": variant_id,
            "status": "not_implemented",
            "message": "Endpoint preparado para integración con Política de Márgenes.",
            "current_price": None,
            "current_margin_pct": None,
            "target_margin_pct": None,
            "suggested_price": None,
        }
    )


def _enrich_opportunity_row(row: dict[str, Any]) -> dict[str, Any]:
    r = dict(row)
    cur_cost = float(r.get("current_cost") or 0)
    avg90 = float(r.get("avg_90d") or 0)
    r["savings_vs_avg"] = round(avg90 - cur_cost, 2) if avg90 > 0 else None
    st = r.get("status")
    score_key, score_sem = commercial_score(
        variation_pct_90d=float(r["variation_pct_90d"])
        if r.get("variation_pct_90d") is not None
        else None,
        anomalous=abs(float(r.get("variation_pct_90d") or 0)) >= 50,
    )
    r["commercial_score"] = score_key
    r["commercial_score_label"] = COMMERCIAL_SCORE_LABELS.get(score_key, score_key)
    r["commercial_score_semaphore"] = score_sem
    r["status_label"] = OPPORTUNITY_LABELS.get(st, "") if st else None
    r["semaphore"] = (
        "green" if st == "oportunidad_compra"
        else "red" if st == "riesgo_comercial"
        else "yellow"
    )
    return r


def get_intelligence(
    company_id: int,
    *,
    user_email: str,
) -> dict[str, Any]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        summary_24h = repo.intelligence_summary_24h(cur, company_id)
        opp_rows = repo.fetch_opportunity_base_rows(cur, company_id)
        opp_counts = {
            "oportunidad_compra": sum(
                1 for r in opp_rows if r.get("status") == "oportunidad_compra"
            ),
            "riesgo_comercial": sum(
                1 for r in opp_rows if r.get("status") == "riesgo_comercial"
            ),
        }
        summary_24h["opportunities_detected"] = opp_counts["oportunidad_compra"]

        enriched = [_enrich_opportunity_row(r) for r in opp_rows]
        buy_ops = [
            r for r in enriched if r.get("status") == "oportunidad_compra"
        ]
        risks = [r for r in enriched if r.get("status") == "riesgo_comercial"]
        buy_ops.sort(
            key=lambda x: float(x.get("savings_vs_avg") or 0), reverse=True
        )
        risks.sort(
            key=lambda x: float(x.get("variation_pct_90d") or 0), reverse=True
        )

        featured = buy_ops[0] if buy_ops else (enriched[0] if enriched else None)

        raw_alerts = repo.list_cost_alerts(cur, company_id, limit=50)
        actionable: list[dict[str, Any]] = []
        for r in raw_alerts:
            spread = r.get("cross_branch_spread")
            kinds = classify_cost_alert(
                has_history=True,
                has_cost_row=not bool(r.get("missing_cost")),
                average_cost=float(r["average_cost"])
                if r.get("average_cost") is not None
                else None,
                cost_net=float(r["cost_net"]) if r.get("cost_net") is not None else None,
                variation_pct=float(r["variation_pct"])
                if r.get("variation_pct") is not None
                else None,
                cross_branch_spread=float(spread) if spread is not None else None,
                suspicious_reception=bool(r.get("suspicious_reception")),
            )
            if not kinds:
                continue
            primary = kinds[0]
            action = alert_to_action(primary)
            actionable.append(
                {
                    **r,
                    "alert_types": kinds,
                    "semaphore": alert_semaphore(kinds),
                    "action": action["action"],
                    "action_semaphore": action["semaphore"],
                }
            )
        actionable.sort(
            key=lambda x: (
                0 if x.get("action_semaphore") == "red" else 1,
                -abs(float(x.get("variation_pct") or 0)),
            )
        )

        watchlist_raw = repo.list_watchlist_enriched(
            cur, user_email=user_email, company_id=company_id
        )
        watchlist_items: list[dict[str, Any]] = []
        for w in watchlist_raw:
            wl_status, wl_sem = watchlist_status(
                float(w["variation_pct_90d"])
                if w.get("variation_pct_90d") is not None
                else None
            )
            watchlist_items.append(
                {
                    **w,
                    "watchlist_status": wl_status,
                    "watchlist_status_label": WATCHLIST_STATUS_LABELS.get(
                        wl_status, wl_status
                    ),
                    "watchlist_semaphore": wl_sem,
                }
            )

        cur.close()
    finally:
        conn.close()

    bullets = [
        f"{summary_24h.get('products_increased_10', 0)} productos aumentaron más de 10%.",
        f"{summary_24h.get('products_decreased_10', 0)} productos bajaron de costo.",
        f"{summary_24h.get('products_branch_diff', 0)} productos presentan diferencias entre sucursales.",
        f"{summary_24h.get('products_anomalous', 0)} productos muestran costo anómalo.",
        f"{summary_24h.get('opportunities_detected', 0)} oportunidades de compra detectadas.",
    ]

    return serialize_value(
        {
            "company_id": company_id,
            "auto_summary": {
                "period": "24h",
                "bullets": bullets,
                "metrics": summary_24h,
                "featured_product": featured,
            },
            "top_opportunities": buy_ops[:10],
            "top_risks": risks[:10],
            "actionable_alerts": actionable[:15],
            "watchlist": watchlist_items,
            "opportunity_counts": opp_counts,
        }
    )


def list_watchlist(company_id: int, *, user_email: str) -> dict[str, Any]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        items = repo.list_watchlist_enriched(
            cur, user_email=user_email, company_id=company_id
        )
        out = []
        for w in items:
            wl_status, wl_sem = watchlist_status(
                float(w["variation_pct_90d"])
                if w.get("variation_pct_90d") is not None
                else None
            )
            score_key, score_sem = commercial_score(
                variation_pct_90d=float(w["variation_pct_90d"])
                if w.get("variation_pct_90d") is not None
                else None,
            )
            out.append(
                {
                    **w,
                    "watchlist_status": wl_status,
                    "watchlist_status_label": WATCHLIST_STATUS_LABELS.get(
                        wl_status, wl_status
                    ),
                    "watchlist_semaphore": wl_sem,
                    "commercial_score": score_key,
                    "commercial_score_label": COMMERCIAL_SCORE_LABELS.get(
                        score_key, score_key
                    ),
                    "commercial_score_semaphore": score_sem,
                }
            )
        cur.close()
    finally:
        conn.close()
    return serialize_value(
        {"company_id": company_id, "items": out, "total": len(out)}
    )


def add_to_watchlist(
    company_id: int, variant_id: int, *, user_email: str
) -> dict[str, Any]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        row = repo.add_watchlist_item(
            cur,
            user_email=user_email,
            company_id=company_id,
            variant_id=variant_id,
        )
        conn.commit()
        cur.close()
    finally:
        conn.close()
    return serialize_value(row)


def remove_from_watchlist(
    company_id: int, variant_id: int, *, user_email: str
) -> dict[str, Any]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        ok = repo.remove_watchlist_item(
            cur,
            user_email=user_email,
            company_id=company_id,
            variant_id=variant_id,
        )
        conn.commit()
        cur.close()
    finally:
        conn.close()
    return serialize_value({"removed": ok, "company_id": company_id, "variant_id": variant_id})


def watchlist_status_for_variant(
    company_id: int, variant_id: int, *, user_email: str
) -> dict[str, Any]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        on_list = repo.is_on_watchlist(
            cur,
            user_email=user_email,
            company_id=company_id,
            variant_id=variant_id,
        )
        cur.close()
    finally:
        conn.close()
    return serialize_value(
        {
            "company_id": company_id,
            "variant_id": variant_id,
            "on_watchlist": on_list,
        }
    )
