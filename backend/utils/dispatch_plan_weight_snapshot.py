"""Congelar peso logístico al confirmar planificación."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

WEIGHT_CALC_VERSION = "order_weight_v1"

_CONFIRMED_STATUSES = frozenset(
    {
        "planned",
        "invoicing",
        "ready_for_picking",
        "picking_generated",
        "dispatched",
        "delivered",
        "closed",
        "squared",
    }
)


def plan_weight_is_frozen(status: str | None) -> bool:
    return (status or "").strip().lower() in _CONFIRMED_STATUSES


def _round3(value: float) -> float:
    return round(float(value or 0), 3)


def _fetch_weights_extended(cur, document_ids: list[int]) -> dict[int, dict[str, Any]]:
    if not document_ids:
        return {}
    cur.execute(
        """
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'distribuidora'
              AND table_name = 'order_weight_snapshots'
        )
        """
    )
    if not cur.fetchone()[0]:
        return {}

    cur.execute(
        """
        SELECT
            ows.document_id,
            ows.peso_total_kg,
            ows.productos_totales,
            ows.productos_sin_peso,
            ows.porcentaje_cobertura,
            ows.calculated_at,
            COALESCE(line_agg.cantidad_unidades, 0) AS cantidad_unidades,
            COALESCE(line_agg.cantidad_cajas, 0) AS cantidad_cajas
        FROM distribuidora.order_weight_snapshots ows
        LEFT JOIN LATERAL (
            SELECT
                SUM(l.cantidad_unitaria) AS cantidad_unidades,
                SUM(COALESCE(l.cantidad_cajas, 0)) AS cantidad_cajas
            FROM distribuidora.order_weight_snapshot_lines l
            WHERE l.snapshot_id = ows.id
        ) line_agg ON TRUE
        WHERE ows.document_id = ANY(%s::bigint[])
        """,
        (list(dict.fromkeys(int(x) for x in document_ids)),),
    )
    cols = [d[0] for d in cur.description]
    out: dict[int, dict[str, Any]] = {}
    for row in cur.fetchall():
        item = dict(zip(cols, row))
        doc_id = int(item["document_id"])
        out[doc_id] = {
            "peso_total_kg": _round3(item.get("peso_total_kg") or 0),
            "weight_kg": _round3(item.get("peso_total_kg") or 0),
            "cantidad_productos": int(item.get("productos_totales") or 0),
            "productos_sin_peso": int(item.get("productos_sin_peso") or 0),
            "cobertura_logistica": float(item.get("porcentaje_cobertura") or 0),
            "porcentaje_cobertura_peso": float(item.get("porcentaje_cobertura") or 0),
            "cantidad_unidades": float(item.get("cantidad_unidades") or 0),
            "cantidad_cajas": float(item.get("cantidad_cajas") or 0),
            "peso_calculated_at": item.get("calculated_at"),
        }
    return out


def _ensure_order_weights(document_ids: list[int]) -> None:
    try:
        from backend.services.order_weight_service import ensure_order_weights

        ensure_order_weights(document_ids)
    except Exception:
        pass


def freeze_orders_weight(
    cur,
    orders: list[dict[str, Any]],
    *,
    calculated_at: datetime | None = None,
) -> list[dict[str, Any]]:
    """Calcula y aplica peso oficial a cada orden del plan (momento de confirmar)."""
    ids = [int(o["oc_document_id"]) for o in orders if o.get("oc_document_id")]
    if not ids:
        return [dict(o) for o in orders]
    _ensure_order_weights(ids)
    weights = _fetch_weights_extended(cur, ids)
    now = calculated_at or datetime.now(timezone.utc)
    out: list[dict[str, Any]] = []
    for order in orders:
        row = dict(order)
        doc_id = int(row["oc_document_id"])
        w = weights.get(doc_id) or {}
        peso = w.get("peso_total_kg", 0.0)
        row["peso_total_kg"] = peso
        row["weight_kg"] = peso
        row["cantidad_productos"] = w.get("cantidad_productos", 0)
        row["cantidad_unidades"] = w.get("cantidad_unidades", 0.0)
        row["cantidad_cajas"] = w.get("cantidad_cajas", 0.0)
        row["productos_sin_peso"] = w.get("productos_sin_peso", 0)
        row["cobertura_logistica"] = w.get("cobertura_logistica", 0.0)
        row["porcentaje_cobertura_peso"] = w.get("porcentaje_cobertura_peso", 0.0)
        row["peso_calculated_at"] = w.get("peso_calculated_at") or now
        row["weight_frozen"] = True
        out.append(row)
    return out


def build_stop_snapshots(orders: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Agrupa órdenes por cliente para snapshot de paradas."""
    groups: dict[tuple[Any, ...], dict[str, Any]] = {}
    for order in sorted(orders, key=lambda o: int(o.get("route_order") or 0)):
        client_id = order.get("client_id")
        address = (order.get("address") or "").strip().lower()
        key = (client_id, address)
        if key not in groups:
            groups[key] = {
                "client_id": client_id,
                "client_name": order.get("client_name") or order.get("fantasy_name"),
                "address": order.get("address"),
                "city": order.get("city"),
                "lat": order.get("lat"),
                "lng": order.get("lng"),
                "peso_total_kg": 0.0,
                "cantidad_cajas": 0.0,
                "cantidad_unidades": 0.0,
                "cantidad_productos": 0,
                "monto_total": 0.0,
                "oc_document_ids": [],
                "route_order": int(order.get("route_order") or 0),
            }
        g = groups[key]
        g["peso_total_kg"] += float(order.get("peso_total_kg") or 0)
        g["cantidad_cajas"] += float(order.get("cantidad_cajas") or 0)
        g["cantidad_unidades"] += float(order.get("cantidad_unidades") or 0)
        g["cantidad_productos"] += int(order.get("cantidad_productos") or 0)
        g["monto_total"] += float(order.get("oc_total_amount") or 0)
        g["oc_document_ids"].append(int(order["oc_document_id"]))
        g["route_order"] = min(int(g["route_order"]), int(order.get("route_order") or 0))

    stops = sorted(groups.values(), key=lambda s: int(s.get("route_order") or 0))
    for idx, stop in enumerate(stops):
        stop["stop_order"] = idx + 1
        stop["peso_total_kg"] = _round3(stop["peso_total_kg"])
        stop["monto_total"] = round(float(stop["monto_total"]), 2)
    return stops


def aggregate_plan_weight(
    orders: list[dict[str, Any]],
    *,
    truck_max_weight_kg: int | None,
    calculated_at: datetime | None = None,
) -> dict[str, Any]:
    """Totales del plan a persistir en dispatch_plan."""
    now = calculated_at or datetime.now(timezone.utc)
    total_kg = sum(float(o.get("peso_total_kg") or 0) for o in orders)
    productos = sum(int(o.get("cantidad_productos") or 0) for o in orders)
    unidades = sum(float(o.get("cantidad_unidades") or 0) for o in orders)
    sin_peso = sum(int(o.get("productos_sin_peso") or 0) for o in orders)
    coverages = [float(o.get("cobertura_logistica") or 0) for o in orders if o.get("cobertura_logistica")]
    avg_coverage = round(sum(coverages) / len(coverages), 1) if coverages else 0.0
    cap = int(truck_max_weight_kg) if truck_max_weight_kg else None
    util_pct = None
    if cap and cap > 0:
        util_pct = round((total_kg / cap) * 100, 2)
    return {
        "weight_total_kg": _round3(total_kg),
        "truck_max_weight_kg": cap,
        "weight_utilization_pct": util_pct,
        "weight_calculated_at": now,
        "weight_calc_version": WEIGHT_CALC_VERSION,
        "weight_orders_count": len(orders),
        "weight_productos_totales": productos,
        "weight_unidades_totales": round(unidades, 4),
        "weight_cobertura_pct": avg_coverage,
    }


def hydrate_frozen_order_row(row: dict[str, Any]) -> dict[str, Any]:
    """Mapea columnas DB a campos API sin recalcular."""
    out = dict(row)
    peso = out.get("peso_total_kg")
    if peso is not None:
        out["weight_kg"] = float(peso)
        out["peso_total_kg"] = float(peso)
    cov = out.get("cobertura_logistica")
    if cov is not None:
        out["porcentaje_cobertura_peso"] = float(cov)
    out["weight_frozen"] = peso is not None
    return out
