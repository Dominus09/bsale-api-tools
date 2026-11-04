"""Resumen de carga operativo para planificación de despacho."""

from __future__ import annotations

from typing import Any

from backend.utils.picking_readiness import evaluate_picking_readiness

OPERATIONAL_STATUSES = (
    "LISTO_PARA_CARGA",
    "FACTURACION_PENDIENTE",
    "PICKING_GENERADO",
    "DESPACHADO",
)


def _operational_status(
    plan: dict[str, Any],
    *,
    readiness_ready: bool,
    has_picking: bool,
) -> str:
    st = (plan.get("status") or "").strip().lower()
    if st in ("dispatched", "delivered"):
        return "DESPACHADO"
    if has_picking or st == "picking_generated":
        return "PICKING_GENERADO"
    if readiness_ready or st == "ready_for_picking":
        return "LISTO_PARA_CARGA"
    return "FACTURACION_PENDIENTE"


def _ors_metrics_calculated(plan: dict[str, Any]) -> bool:
    raw = plan.get("ors_metrics_calculated")
    if raw is None:
        # Planes previos a 050: asumir ORS calculó.
        return True
    return bool(raw)


def _route_coverage_meta(plan: dict[str, Any]) -> dict[str, Any]:
    geom = plan.get("route_geometry")
    if isinstance(geom, str):
        try:
            import json

            geom = json.loads(geom)
        except Exception:
            geom = None
    meta = {}
    if isinstance(geom, dict):
        raw = geom.get("quillotana_route_meta")
        if isinstance(raw, dict):
            meta = raw
    coverage = meta.get("route_coverage")
    if not coverage:
        if plan.get("ors_metrics_calculated") is False:
            coverage = "none"
        elif plan.get("ors_metrics_calculated") is True:
            coverage = "full"
        else:
            coverage = None
    return {
        "route_coverage": coverage,
        "clients_total": meta.get("clients_total"),
        "clients_georef": meta.get("clients_georef"),
        "clients_pending": meta.get("clients_pending"),
        "km_scope": meta.get("km_scope"),
    }


def _cost_block(plan: dict[str, Any]) -> dict[str, Any]:
    """Costos de ruta: si ORS no calculó, exponer null (no cero real)."""
    calculated = _ors_metrics_calculated(plan)
    cov = _route_coverage_meta(plan)
    if not calculated:
        return {
            "fuel_clp": None,
            "crew_clp": None,
            "tolls_clp": None,
            "ferry_clp": None,
            "extras_clp": None,
            "route_total_clp": None,
            "km_total": None,
            "duration_min": None,
            "liters_estimated": None,
            "ors_metrics_calculated": False,
            "route_coverage": "none",
            "note": "Métricas ORS no calculadas (planificación simple_list)",
            **{k: cov[k] for k in ("clients_total", "clients_georef", "clients_pending", "km_scope")},
        }
    note = None
    if cov.get("route_coverage") == "partial":
        note = (
            "Ruta parcial: km/costos solo de paradas georreferenciadas; "
            "no representan el despacho completo."
        )
    return {
        "fuel_clp": int(plan.get("fuel_cost_clp") or 0),
        "crew_clp": int(plan.get("crew_cost_clp") or 0),
        "tolls_clp": int(plan.get("toll_cost_clp") or 0),
        "ferry_clp": int(plan.get("ferry_cost_clp") or 0),
        "extras_clp": int(plan.get("extras_cost_clp") or 0),
        "route_total_clp": int(plan.get("total_route_cost_clp") or 0),
        "km_total": plan.get("km_total"),
        "duration_min": plan.get("duration_min"),
        "liters_estimated": plan.get("liters_estimated"),
        "ors_metrics_calculated": True,
        "route_coverage": cov.get("route_coverage") or "full",
        "note": note,
        **{k: cov[k] for k in ("clients_total", "clients_georef", "clients_pending", "km_scope")},
    }


def build_load_summary(
    *,
    plan: dict[str, Any],
    inv: dict[str, Any],
    invoicing_block: dict[str, Any],
    margin_block: dict[str, Any] | None,
    picking_meta: dict[str, Any] | None,
    picking_kpis: dict[str, Any] | None,
    header: dict[str, Any] | None,
    orders: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Bloque superior: cabecera, KPIs, facturación, costos y estado operacional."""
    inv_summary = inv.get("summary") if isinstance(inv.get("summary"), dict) else {}
    readiness = evaluate_picking_readiness(inv, include_probable=False)
    has_picking = picking_meta is not None

    confirmed = invoicing_block.get("confirmed") or {}
    probable = invoicing_block.get("probable") or {}
    pending = invoicing_block.get("pending") or {}

    auto_n = int(confirmed.get("auto_confirmed_count") or inv_summary.get("auto_confirmed") or 0)
    confirmed_n = int(confirmed.get("count") or inv_summary.get("confirmed") or 0)
    manual_confirmed = max(0, confirmed_n - auto_n)

    pk = picking_kpis or {}
    order_rows = orders or []
    oc_count = int(invoicing_block.get("total_orders") or len(order_rows) or 0)
    oc_clients = len(
        {
            (o.get("client_id"), (o.get("address") or "").strip().lower())
            for o in order_rows
            if o.get("client_id") is not None or (o.get("client_name") or "").strip()
        }
    )
    if not oc_clients and oc_count:
        oc_clients = oc_count

    picking_stops = int(
        pk.get("stops") or (picking_meta.get("stops_count") if picking_meta else 0) or 0
    )
    picking_clients = int(pk.get("clients") or 0)
    picking_docs = int(pk.get("documents") or 0)

    # KPIs principales: preferir facturación confirmada / OC del plan;
    # no usar 0 de picking como si no hubiera clientes/documentos.
    clients_kpi = picking_clients or picking_stops or oc_clients
    documents_kpi = picking_docs or confirmed_n or oc_count

    oc_total_clp = int(
        round(float(invoicing_block.get("total_oc_amount_clp") or 0))
    )
    confirmed_sales_clp = int(confirmed.get("amount_clp") or 0)
    picking_sales_clp = int(
        round(
            float(
                pk.get("sales_total_clp")
                or (picking_meta or {}).get("document_total_clp")
                or 0
            )
        )
    )

    out: dict[str, Any] = {
        "header": {
            "planning_code": plan.get("planning_code") or f"PLAN-{plan.get('id')}",
            "planning_date": (
                plan["planning_date"].isoformat()
                if hasattr(plan.get("planning_date"), "isoformat")
                else str(plan.get("planning_date") or "")
            ),
            "planning_name": plan.get("planning_name") or plan.get("route_name") or "",
            "truck_name": plan.get("truck_name") or plan.get("route_name") or "",
            "driver_name": (header or {}).get("driver_name") or "",
            "driver_label": (header or {}).get("driver_label") or "",
            "assistant_label": (header or {}).get("assistant_label") or "",
            "assistant_names": (header or {}).get("assistant_names") or [],
            "route_name": plan.get("route_name") or "",
            "communes": (header or {}).get("communes") or "",
            "sello": (header or {}).get("sello") or "",
        },
        "kpis": {
            "clients": clients_kpi,
            "documents": documents_kpi,
            "clients_from_oc": oc_clients,
            "clients_from_picking": picking_clients or None,
            "documents_from_invoices": confirmed_n,
            "documents_from_picking": picking_docs or None,
            "oc_count": oc_count,
            "oc_total_amount_clp": oc_total_clp,
            "confirmed_sales_clp": confirmed_sales_clp,
            "picking_sales_clp": picking_sales_clp,
            "sales_total_clp": picking_sales_clp or confirmed_sales_clp or oc_total_clp,
            "distinct_products": int(
                pk.get("distinct_products")
                or (picking_meta or {}).get("product_lines_count")
                or 0
            ),
            "total_units": float(pk.get("total_units") or 0),
            "estimated_boxes": float(pk.get("estimated_boxes") or 0),
        },
        "invoicing": {
            "confirmed_manual": manual_confirmed,
            "confirmed_auto": auto_n,
            "confirmed_total": confirmed_n,
            "probable": int(probable.get("count") or inv_summary.get("probable") or 0),
            "pending": int(pending.get("count") or inv_summary.get("missing") or 0),
            "oc_total_amount_clp": oc_total_clp,
            "confirmed_amount_clp": confirmed_sales_clp,
            "probable_amount_clp": int(probable.get("amount_clp") or 0),
            "pending_amount_clp": int(pending.get("amount_clp") or 0),
        },
        "planning_origin": plan.get("planning_origin") or "ors",
        "ors_metrics_calculated": bool(
            plan["ors_metrics_calculated"]
            if plan.get("ors_metrics_calculated") is not None
            else True
        ),
        "costs": _cost_block(plan),
        "results": {
            "commercial_margin_clp": (
                margin_block.get("commercial_margin_clp") if margin_block else None
            ),
            "net_operational_clp": (
                margin_block.get("net_operational_clp") if margin_block else None
            ),
            "margin_visible": bool(margin_block and margin_block.get("visible")),
            "margin_message": (margin_block or {}).get("message"),
        },
        "picking": {
            "has_snapshot": has_picking,
            "version": (picking_meta or {}).get("version"),
            "picking_id": (picking_meta or {}).get("picking_id"),
            "generated_at": (picking_meta or {}).get("generated_at"),
            "ready_to_generate": readiness["ready"],
            "ready_reason": readiness.get("reason"),
            "pending_invoices": not readiness["ready"],
            "not_load_ready": not has_picking,
        },
    }
    out["operational_status"] = _operational_status(
        plan,
        readiness_ready=readiness["ready"],
        has_picking=has_picking,
    )
    out["operational_status_label"] = _status_label(out["operational_status"])

    frozen_weight = plan.get("weight_total_kg") is not None
    cap = plan.get("truck_max_weight_kg")
    if frozen_weight:
        weight_total = float(plan.get("weight_total_kg") or 0)
        util_pct = plan.get("weight_utilization_pct")
        coverage = plan.get("weight_cobertura_pct")
        productos = plan.get("weight_productos_totales")
        unidades = plan.get("weight_unidades_totales")
        orders_count = plan.get("weight_orders_count")
    elif orders:
        weight_total = sum(float(o.get("peso_total_kg") or o.get("weight_kg") or 0) for o in orders)
        coverages = [
            float(o.get("cobertura_logistica") or o.get("porcentaje_cobertura_peso") or 0)
            for o in orders
        ]
        coverage = round(sum(coverages) / len(coverages), 1) if coverages else 0.0
        productos = sum(int(o.get("cantidad_productos") or 0) for o in orders)
        unidades = sum(float(o.get("cantidad_unidades") or 0) for o in orders)
        orders_count = len(orders)
        util_pct = None
        if cap and int(cap) > 0:
            util_pct = round((weight_total / int(cap)) * 100, 2)
    else:
        weight_total = 0.0
        util_pct = None
        coverage = 0.0
        productos = 0
        unidades = 0.0
        orders_count = 0

    out["weight"] = {
        "frozen": frozen_weight,
        "truck_max_weight_kg": int(cap) if cap is not None else None,
        "weight_total_kg": round(weight_total, 3),
        "utilization_pct": float(util_pct) if util_pct is not None else None,
        "orders_count": int(orders_count or 0),
        "productos_totales": int(productos or 0),
        "unidades_totales": float(unidades or 0),
        "cobertura_pct": float(coverage or 0),
        "calculated_at": (
            plan.get("weight_calculated_at").isoformat()
            if hasattr(plan.get("weight_calculated_at"), "isoformat")
            else plan.get("weight_calculated_at")
        ),
        "calc_version": plan.get("weight_calc_version"),
    }
    return out


def _status_label(code: str) -> str:
    return {
        "LISTO_PARA_CARGA": "Listo para carga",
        "FACTURACION_PENDIENTE": "Facturación pendiente",
        "PICKING_GENERADO": "Picking generado",
        "DESPACHADO": "Despachado",
    }.get(code, code.replace("_", " ").title())
