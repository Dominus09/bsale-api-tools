"""Analítica comercial vendedores — equipo operativo La Quillotana (Company 3 / Office 1)."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from backend.config.commercial_scope import (
    COMPANY_ID,
    DD_SIGNED_AMOUNT,
    DD_SIGNED_QTY,
    filter_options_payload,
    profile_sales_where,
)
from backend.db import get_connection
from backend.services.distribuidora.orders_service import _row_to_dict, _serialize_row

logger = logging.getLogger(__name__)

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


def _conn_query_all(
    sql: str,
    params: tuple[Any, ...],
    *,
    endpoint: str = "legacy",
    label: str = "query",
) -> list[dict[str, Any]]:
    conn = get_connection()
    conn.autocommit = True
    logger.info("[COMMERCIAL_LOCK] transaction_start endpoint=%s readonly=1 autocommit=1", endpoint)
    t0 = time.perf_counter()
    try:
        cur = conn.cursor()
        try:
            cur.execute("SET default_transaction_read_only = on")
            cur.execute(sql, params)
            rows = cur.fetchall()
            out = [_serialize_row(_row_to_dict(cur, r)) for r in rows]
        finally:
            cur.close()
        ms = (time.perf_counter() - t0) * 1000
        logger.info(
            "[COMMERCIAL_SQL] endpoint=%s query=%s duration_ms=%.1f rows=%s",
            endpoint,
            label,
            ms,
            len(out),
        )
        return out
    finally:
        logger.info("[COMMERCIAL_LOCK] transaction_end endpoint=%s error=0", endpoint)
        conn.close()


def _conn_query_one(
    sql: str,
    params: tuple[Any, ...],
    *,
    endpoint: str = "legacy",
    label: str = "query",
) -> dict[str, Any] | None:
    rows = _conn_query_all(sql, params, endpoint=endpoint, label=label)
    return rows[0] if rows else None


def _commercial_bundle(filters: CommercialFilters, limits: Any | None = None) -> dict[str, Any]:
    from backend.services.commercial_analytics_engine import BundleLimits, build_commercial_bundle

    blim = limits if isinstance(limits, BundleLimits) else BundleLimits()
    return build_commercial_bundle(filters, blim)


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


def get_commercial_bundle(
    filters: CommercialFilters,
    *,
    seller_limit: int = 50,
    unique_limit: int = 300,
    lost_limit: int = 100,
    cross_limit: int = 100,
    product_limit: int = 50,
) -> dict[str, Any]:
    from backend.services.commercial_analytics_engine import BundleLimits

    return _commercial_bundle(
        filters,
        BundleLimits(
            seller=seller_limit,
            unique_clients=unique_limit,
            lost_clients=lost_limit,
            cross_selling=cross_limit,
            products=product_limit,
        ),
    )


def get_dashboard(filters: CommercialFilters) -> dict[str, Any]:
    return _commercial_bundle(filters)["dashboard"]


def get_seller_performance(filters: CommercialFilters, *, limit: int = 50) -> dict[str, Any]:
    from backend.services.commercial_analytics_engine import BundleLimits

    return _commercial_bundle(filters, BundleLimits(seller=limit))["seller_performance"]


def get_unique_clients(filters: CommercialFilters, *, limit: int = 500) -> dict[str, Any]:
    from backend.services.commercial_analytics_engine import BundleLimits

    return _commercial_bundle(filters, BundleLimits(unique_clients=limit))["unique_clients"]


def get_lost_clients(filters: CommercialFilters, *, limit: int = 200) -> dict[str, Any]:
    from backend.services.commercial_analytics_engine import BundleLimits

    return _commercial_bundle(filters, BundleLimits(lost_clients=limit))["lost_clients"]


def get_recovered_clients(filters: CommercialFilters, *, limit: int = 200) -> dict[str, Any]:
    from backend.services.commercial_analytics_engine import BundleLimits

    return _commercial_bundle(filters, BundleLimits(unique_clients=limit * 2))["recovered_clients"]


def get_product_performance(
    filters: CommercialFilters,
    *,
    seller: str | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    from backend.services.commercial_analytics_engine import BundleLimits

    f = filters
    if seller and str(seller).strip():
        f = CommercialFilters(
            date_from=filters.date_from,
            date_to=filters.date_to,
            compare_date_from=filters.compare_date_from,
            compare_date_to=filters.compare_date_to,
            seller=seller,
            city=filters.city,
            client_id=filters.client_id,
            document_type=filters.document_type,
        )
    return _commercial_bundle(f, BundleLimits(products=limit))["product_performance"]


def get_cross_selling(filters: CommercialFilters, *, limit: int = 100) -> dict[str, Any]:
    from backend.services.commercial_analytics_engine import BundleLimits

    return _commercial_bundle(filters, BundleLimits(cross_selling=limit))["cross_selling"]


def get_summary(filters: CommercialFilters) -> dict[str, Any]:
    from backend.services.commercial_analytics_engine import BundleLimits

    return _commercial_bundle(
        filters,
        BundleLimits(seller=20, lost_clients=500, cross_selling=500),
    )["summary"]


def get_client_profile(filters: CommercialFilters, client_id: int) -> dict[str, Any]:
    where_clause, where_params = profile_sales_where(
        client_id=client_id,
        document_type=filters.document_type,
    )
    six_months_ago = filters.date_to - timedelta(days=180)

    sql_client = f"""
        SELECT
            v.client_id,
            MAX(v.client_name) AS client_name,
            MAX(v.municipality) AS municipality,
            (
                ARRAY_AGG(v.seller_name ORDER BY v.emission_date DESC)
                FILTER (WHERE v.is_sale = 1)
            )[1] AS seller_name,
            MAX((v.emission_date AT TIME ZONE 'UTC')::date) FILTER (WHERE v.is_sale = 1) AS ultima_compra,
            COALESCE(SUM(v.is_sale), 0)::bigint AS total_compras,
            COALESCE(SUM(v.total_amount_net), 0) AS venta_total,
            COALESCE(
                SUM(v.total_amount_sales) FILTER (WHERE v.is_sale = 1)
                / NULLIF(SUM(v.is_sale)::numeric, 0),
                0
            ) AS ticket_promedio
        FROM distribuidora.v_sales v
        WHERE {where_clause}
        GROUP BY v.client_id
    """
    client_row = _conn_query_one(
        sql_client,
        tuple(where_params),
        endpoint="client-profile",
        label="client",
    ) or {}

    sql_monthly = f"""
        SELECT
            TO_CHAR((v.emission_date AT TIME ZONE 'UTC')::date, 'YYYY-MM') AS mes,
            COALESCE(SUM(v.total_amount_net), 0) AS venta
        FROM distribuidora.v_sales v
        WHERE {where_clause}
          AND (v.emission_date AT TIME ZONE 'UTC')::date >= %s
        GROUP BY 1
        ORDER BY 1
    """
    monthly = _conn_query_all(
        sql_monthly,
        tuple(where_params + [six_months_ago]),
        endpoint="client-profile",
        label="monthly",
    )

    signed_qty = DD_SIGNED_QTY.replace("sb.", "v.")
    signed_amount = DD_SIGNED_AMOUNT.replace("sb.", "v.")

    sql_products = f"""
        SELECT
            COALESCE(dd.variant_description, dd.variant_code, 'Producto') AS producto,
            COALESCE(SUM({signed_qty}), 0) AS unidades,
            COALESCE(SUM({signed_amount}), 0) AS venta,
            MAX((v.emission_date AT TIME ZONE 'UTC')::date) FILTER (WHERE v.is_sale = 1) AS ultima_compra
        FROM distribuidora.v_sales v
        INNER JOIN distribuidora.document_details dd ON dd.document_id = v.document_id
        WHERE {where_clause}
        GROUP BY dd.variant_description, dd.variant_code
        ORDER BY venta DESC NULLS LAST
        LIMIT 20
    """
    products = _conn_query_all(
        sql_products,
        tuple(where_params),
        endpoint="client-profile",
        label="products",
    )

    sql_cats = f"""
        SELECT
            COALESCE(pt.name, pm.product_type, 'Sin categoría') AS categoria,
            COALESCE(SUM({signed_amount}), 0) AS venta
        FROM distribuidora.v_sales v
        INNER JOIN distribuidora.document_details dd ON dd.document_id = v.document_id
        LEFT JOIN bsale.variants v2 ON v2.company_id = {COMPANY_ID} AND v2.bsale_id = dd.variant_id
        LEFT JOIN bsale.products p ON p.company_id = v2.company_id AND p.bsale_id = v2.product_id
        LEFT JOIN bsale.product_types pt ON pt.company_id = p.company_id AND pt.bsale_id = p.product_type_id
        LEFT JOIN bsale.products_master pm ON pm.company_id = {COMPANY_ID} AND pm.variant_id = dd.variant_id
        WHERE {where_clause}
        GROUP BY 1
        ORDER BY venta DESC NULLS LAST
    """
    categories = _conn_query_all(
        sql_cats,
        tuple(where_params),
        endpoint="client-profile",
        label="categories",
    )

    prev_from, prev_to = filters.compare_period()
    sql_period = f"""
        SELECT
            COALESCE(SUM(v.total_amount_net) FILTER (
                WHERE (v.emission_date AT TIME ZONE 'UTC')::date BETWEEN %s AND %s
            ), 0) AS venta_actual,
            COALESCE(SUM(v.total_amount_net) FILTER (
                WHERE (v.emission_date AT TIME ZONE 'UTC')::date BETWEEN %s AND %s
            ), 0) AS venta_anterior,
            BOOL_OR(v.is_sale = 1 AND (v.emission_date AT TIME ZONE 'UTC')::date BETWEEN %s AND %s) AS in_curr,
            BOOL_OR(v.is_sale = 1 AND (v.emission_date AT TIME ZONE 'UTC')::date BETWEEN %s AND %s) AS in_prev,
            (CURRENT_DATE - MAX((v.emission_date AT TIME ZONE 'UTC')::date) FILTER (WHERE v.is_sale = 1))::int AS dias_sin_comprar,
            COUNT(*) FILTER (
                WHERE v.is_sale = 1
                  AND (v.emission_date AT TIME ZONE 'UTC')::date >= (%s::date - INTERVAL '90 days')::date
            )::bigint AS compras_90d
        FROM distribuidora.v_sales v
        WHERE {where_clause}
    """
    period_row = _conn_query_one(
        sql_period,
        tuple(
            where_params
            + [
                filters.date_from, filters.date_to,
                prev_from, prev_to,
                filters.date_from, filters.date_to,
                prev_from, prev_to,
                filters.date_to,
            ]
        ),
        endpoint="client-profile",
        label="period",
    ) or {}

    sql_abandoned = f"""
        WITH hist AS (
            SELECT
                COALESCE(dd.variant_description, dd.variant_code, 'Producto') AS producto,
                MAX((v.emission_date AT TIME ZONE 'UTC')::date) AS ultima
            FROM distribuidora.v_sales v
            INNER JOIN distribuidora.document_details dd ON dd.document_id = v.document_id
            WHERE {where_clause}
              AND v.is_sale = 1
              AND (v.emission_date AT TIME ZONE 'UTC')::date >= %s
            GROUP BY 1
        )
        SELECT producto, ultima
        FROM hist
        WHERE ultima < (%s::date - INTERVAL '90 days')::date
        ORDER BY ultima DESC
        LIMIT 10
    """
    abandoned = _conn_query_all(
        sql_abandoned,
        tuple(where_params + [six_months_ago, filters.date_to]),
        endpoint="client-profile",
        label="abandoned",
    )

    from backend.services.commercial_analytics_intelligence import (
        CROSS_SELL_RULES,
        client_health_from_status,
        compute_client_score,
    )

    in_curr = bool(period_row.get("in_curr"))
    in_prev = bool(period_row.get("in_prev"))
    if in_curr and in_prev:
        status = "activo"
    elif in_curr and not in_prev:
        status = "nuevo"
    elif in_prev and not in_curr:
        status = "perdido"
    else:
        status = "en_riesgo"

    health_key, health_label = client_health_from_status(status)
    venta_actual = _float(period_row.get("venta_actual"))
    venta_anterior = _float(period_row.get("venta_anterior"))
    dias_sin = _int(period_row.get("dias_sin_comprar"))
    compras_90d = _int(period_row.get("compras_90d"))
    ticket = _float(client_row.get("ticket_promedio"))
    client_score = compute_client_score(
        status=status,
        venta_actual=venta_actual,
        venta_anterior=venta_anterior,
        dias_sin_comprar=dias_sin,
        compras_90d=compras_90d,
        ticket_promedio=ticket,
    )

    cats_joined = " ".join(str(c.get("categoria") or "").lower() for c in categories)
    oportunidades: list[dict[str, Any]] = []
    for bought_kw, rec_kw, motivo, prioridad in CROSS_SELL_RULES:
        if bought_kw in cats_joined and rec_kw not in cats_joined:
            oportunidades.append({
                "producto_recomendado": rec_kw,
                "motivo": motivo,
                "prioridad": prioridad,
            })

    freq_days = None
    if _int(client_row.get("total_compras")) > 1 and client_row.get("ultima_compra"):
        freq_days = max(1, 180 // _int(client_row.get("total_compras")))

    if status == "perdido":
        prob_abandono = min(98, 75 + min(dias_sin, 30))
        prob_recuperacion = max(15, min(85, 90 - dias_sin // 2))
    elif status == "en_riesgo":
        prob_abandono = min(85, 45 + dias_sin)
        prob_recuperacion = max(25, 70 - dias_sin // 3)
    elif status == "activo":
        prob_abandono = max(5, min(40, dias_sin * 2))
        prob_recuperacion = 10
    else:
        prob_abandono = max(10, min(50, dias_sin))
        prob_recuperacion = 50

    potencial_mensual = venta_anterior or venta_actual or (ticket * 2 if ticket else 0)
    if potencial_mensual <= 0 and freq_days:
        potencial_mensual = ticket * max(1, 30 // freq_days)

    productos_sugeridos = [
        {"producto": o["producto_recomendado"], "motivo": o["motivo"], "prioridad": o.get("prioridad", "media")}
        for o in oportunidades
    ]

    return {
        "client": {
            "client_id": client_id,
            "client_name": client_row.get("client_name"),
            "municipality": client_row.get("municipality"),
            "seller_name": client_row.get("seller_name"),
            "ultima_compra": str(client_row.get("ultima_compra")) if client_row.get("ultima_compra") else None,
            "frecuencia_dias": freq_days,
            "ticket_promedio": ticket,
            "venta_total": _float(client_row.get("venta_total")),
            "total_compras": _int(client_row.get("total_compras")),
            "client_score": client_score,
            "client_health": health_key,
            "client_health_label": health_label,
            "status": status,
            "dias_sin_comprar": dias_sin,
            "venta_periodo_actual": venta_actual,
            "venta_periodo_anterior": venta_anterior,
            "probabilidad_abandono": prob_abandono,
            "probabilidad_recuperacion": prob_recuperacion,
            "potencial_mensual": round(potencial_mensual, 0),
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
        "productos_abandonados": [
            {"producto": r.get("producto"), "ultima_compra": str(r.get("ultima"))}
            for r in abandoned
        ],
        "categorias": [{"categoria": r.get("categoria"), "venta": _float(r.get("venta"))} for r in categories],
        "oportunidades": oportunidades,
        "productos_sugeridos": productos_sugeridos,
    }


def get_seller_profile(filters: CommercialFilters, seller_name: str) -> dict[str, Any]:
    """Ficha inteligente del vendedor — on-demand, filtrado por vendedor."""
    name = str(seller_name or "").strip()
    if not name:
        return {"error": "seller_name requerido"}

    f = CommercialFilters(
        date_from=filters.date_from,
        date_to=filters.date_to,
        compare_date_from=filters.compare_date_from,
        compare_date_to=filters.compare_date_to,
        seller=name,
        city=filters.city,
        document_type=filters.document_type,
    )
    from backend.services.commercial_analytics_engine import BundleLimits

    bundle = get_commercial_bundle(
        f,
        seller_limit=5,
        unique_limit=400,
        lost_limit=150,
        cross_limit=80,
        product_limit=30,
    )
    crm = bundle.get("crm") or {}
    sellers = bundle["seller_performance"]["items"]
    seller = next((s for s in sellers if s.get("seller_name") == name), sellers[0] if sellers else {})

    from backend.services.commercial_crm_intelligence import explain_seller_score

    expl = explain_seller_score(seller) if seller else {"positives": [], "negatives": [], "stars": 0, "status_label": ""}
    ranking = crm.get("ranking") or []
    rank_pos = next((i + 1 for i, r in enumerate(ranking) if r.get("seller_name") == name), None)
    forecast = crm.get("forecast") or {}
    seller_aporte = next(
        (a for a in forecast.get("seller_aportes") or [] if a.get("seller_name") == name),
        None,
    )

    unique = bundle["unique_clients"]["items"]
    lost = bundle["lost_clients"]["items"]
    recovered = [c for c in unique if c.get("status") == "recuperado"]
    lost_mine = [c for c in lost if c.get("seller_name") == name][:20]

    where_clause, where_params = profile_sales_where(
        seller_name=name,
        document_type=filters.document_type,
    )
    sql_comunas = f"""
        SELECT
            COALESCE(v.municipality, 'Sin comuna') AS comuna,
            COUNT(DISTINCT v.client_id) FILTER (WHERE v.is_sale = 1)::bigint AS clientes,
            COALESCE(SUM(v.total_amount_net), 0) AS venta
        FROM distribuidora.v_sales v
        WHERE {where_clause}
          AND (v.emission_date AT TIME ZONE 'UTC')::date BETWEEN %s AND %s
        GROUP BY 1
        ORDER BY venta DESC NULLS LAST
        LIMIT 15
    """
    comunas = _conn_query_all(
        sql_comunas,
        tuple(where_params + [filters.date_from, filters.date_to]),
        endpoint="seller-profile",
        label="comunas",
    )

    products_payload = bundle.get("product_performance") or {}
    top_products = products_payload.get("top_products") or []
    oportunidades_prod = products_payload.get("oportunidades") or []

    acciones: list[str] = []
    if seller.get("accion_sugerida"):
        acciones.append(str(seller["accion_sugerida"]))
    if _int(seller.get("clientes_perdidos")) >= 3:
        acciones.append(f"Recuperar {_int(seller.get('clientes_perdidos'))} clientes perdidos prioritarios.")
    if seller_aporte and _float(seller_aporte.get("aporte_necesario")) > 0:
        acciones.append(
            f"Aportar {_float(seller_aporte.get('aporte_necesario')):,.0f} para cerrar meta del mes.".replace(",", ".")
        )
    for neg in expl.get("negatives", [])[:2]:
        acciones.append(f"Mejorar: {neg}.")

    return {
        "seller": {
            "seller_name": name,
            "seller_id": seller.get("seller_id"),
            "commercial_score": seller.get("commercial_score"),
            "score_status": seller.get("score_status"),
            "score_status_label": seller.get("score_status_label"),
            "score_explanation": expl,
            "venta_actual": _float(seller.get("venta_actual")),
            "venta_anterior": _float(seller.get("venta_anterior")),
            "variacion_pct": _float(seller.get("variacion_pct")),
            "clientes_unicos": _int(seller.get("clientes_unicos_actual")),
            "clientes_perdidos": _int(seller.get("clientes_perdidos")),
            "clientes_recuperados": _int(seller.get("clientes_recuperados")),
            "clientes_nuevos": _int(seller.get("clientes_nuevos")),
            "ticket_promedio": _float(seller.get("ticket_promedio")),
            "ranking_posicion": rank_pos,
            "ranking_total": len(ranking),
        },
        "forecast_personal": seller_aporte,
        "forecast_equipo": {
            "meta": forecast.get("meta"),
            "proyeccion": forecast.get("proyeccion"),
            "cumplimiento_pct": forecast.get("cumplimiento_pct"),
            "faltan": forecast.get("faltan"),
        },
        "comunas": [
            {"comuna": r.get("comuna"), "clientes": _int(r.get("clientes")), "venta": _float(r.get("venta"))}
            for r in comunas
        ],
        "productos_fuertes": top_products[:8],
        "productos_debiles": oportunidades_prod[:8],
        "clientes_recuperados": recovered[:15],
        "clientes_perdidos": lost_mine,
        "evolucion_mensual": (crm.get("timeline") or {}).get("meses") or [],
        "acciones_sugeridas": acciones[:6],
        "ia_narrativas": [
            n for n in (crm.get("ia_comercial") or []) if n.get("seller_name") in (name, None)
        ][:4],
    }


def list_filter_options() -> dict[str, Any]:
    from backend.config.commercial_scope import ACTIVE_SELLER_IDS

    payload = filter_options_payload()
    cities = _conn_query_all(
        """
        SELECT DISTINCT municipality
        FROM distribuidora.v_sales
        WHERE seller_id IN %s
          AND is_sale = 1
          AND municipality IS NOT NULL AND TRIM(municipality) <> ''
        ORDER BY municipality
        """,
        (ACTIVE_SELLER_IDS,),
        endpoint="filter-options",
        label="cities",
    )
    payload["cities"] = [r["municipality"] for r in cities]
    return payload


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


def get_commercial_map(filters: CommercialFilters, *, limit: int = 500) -> dict[str, Any]:
    from backend.services.commercial_analytics_engine import get_commercial_map_data

    return get_commercial_map_data(filters, limit=limit)


def get_commercial_validation(filters: CommercialFilters) -> dict[str, Any]:
    from backend.services.commercial_analytics_validation import build_commercial_validation

    return build_commercial_validation(filters)


def run_commercial_simulator(
    filters: CommercialFilters,
    *,
    scenario: str,
    seller: str | None = None,
    pct_recuperacion: float = 0.3,
    ticket_uplift_pct: float = 0.1,
    cross_clients: int = 10,
) -> dict[str, Any]:
    from backend.services.commercial_analytics_engine import BundleLimits, build_commercial_bundle
    from backend.services.commercial_predictive_intelligence import run_commercial_simulator as _sim

    bundle = build_commercial_bundle(
        filters,
        BundleLimits(seller=30, unique_clients=400, lost_clients=200, cross_selling=150),
    )
    crm = bundle.get("crm") or {}
    from backend.services.commercial_predictive_intelligence import (
        _cross_lookup,
        _peer_medians_by_comuna,
        enrich_client_predictive,
    )

    unique = bundle["unique_clients"]["items"]
    lost = bundle["lost_clients"]["items"]
    cross = bundle["cross_selling"]["items"]
    peer_medians = _peer_medians_by_comuna(unique + lost)
    cross_lookup = _cross_lookup(cross)
    enriched_unique = [
        enrich_client_predictive(c, peer_medians=peer_medians, cross_by_client=cross_lookup) for c in unique
    ]
    enriched_lost = [
        enrich_client_predictive(
            {**x, "status": "perdido", "venta_actual": _float(x.get("promedio_compra_mensual")), "compras_90d": 0},
            peer_medians=peer_medians,
            cross_by_client=cross_lookup,
        )
        for x in lost
    ]

    curr_kpi = {
        "venta_neta": bundle["dashboard"]["kpis"]["venta_neta"]["current"]
        if bundle.get("dashboard", {}).get("kpis", {}).get("venta_neta")
        else 0,
    }
    return _sim(
        scenario=scenario,
        sellers=bundle["seller_performance"]["items"],
        enriched_unique=enriched_unique,
        enriched_lost=enriched_lost,
        cross=cross,
        curr_kpi=curr_kpi,
        forecast=crm.get("forecast") or {},
        seller_filter=seller,
        pct_recuperacion=pct_recuperacion,
        ticket_uplift_pct=ticket_uplift_pct,
        cross_clients=cross_clients,
    )
