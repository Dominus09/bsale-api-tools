"""
Mapa de entregas MVP — destinos por cliente de un dispatch_plan.

Fuente de paradas: distribuidora.dispatch_plan_orders (agrupado por cliente).
Coordenadas: lat/lng del order; enriquecimiento opcional desde bsale.rutero / clients.
Estado entregado: eventos en dispatch_plan_order_events (sin migración nueva).
"""

from __future__ import annotations

import re
from typing import Any

from psycopg2.extras import RealDictCursor

from backend.db import get_connection
from backend.repositories.distribuidora import dispatch_plan_load_batch_repo as batch_repo
from backend.utils.rutero_coords_sql import R_LAT, R_LON

ACTION_DELIVERY_STOP = "delivery_stop_status"
VALID_STOP_STATUS = frozenset({"pending", "delivered"})


def _planning_code(plan: dict[str, Any]) -> str:
    raw = (plan.get("planning_code") or "").strip()
    if raw:
        return raw
    return f"PLAN-{int(plan['id']):05d}"


def _customer_key(client_id: Any, client_name: str | None) -> str:
    if client_id is not None:
        return f"id:{int(client_id)}"
    name = re.sub(r"\s+", " ", (client_name or "").strip().lower())
    return f"name:{name or 'sin-nombre'}"


def _valid_coords(lat: Any, lng: Any) -> tuple[float, float] | None:
    try:
        if lat is None or lng is None:
            return None
        la = float(lat)
        ln = float(lng)
    except (TypeError, ValueError):
        return None
    if not (-90 <= la <= 90 and -180 <= ln <= 180):
        return None
    if la == 0.0 and ln == 0.0:
        return None
    return la, ln


def resolve_plan_id_from_picking_number(picking_number: str) -> int | None:
    code = (picking_number or "").strip()
    if not code:
        return None
    conn = get_connection()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """
            SELECT id
            FROM distribuidora.dispatch_plan
            WHERE COALESCE(NULLIF(BTRIM(planning_code), ''),
                           'PLAN-' || LPAD(id::text, 5, '0')) = %s
            LIMIT 1
            """,
            (code,),
        )
        row = cur.fetchone()
        if row:
            return int(row["id"])
        # Fallback numérico PLAN-00044 → 44
        m = re.match(r"^PLAN-0*(\d+)$", code, re.I)
        if m:
            pid = int(m.group(1))
            cur.execute(
                "SELECT id FROM distribuidora.dispatch_plan WHERE id = %s",
                (pid,),
            )
            row = cur.fetchone()
            if row:
                return int(row["id"])
        return None
    finally:
        conn.close()


def resolve_plan_id_from_load_id(load_id: int) -> tuple[int, str]:
    """Retorna (plan_id, picking_number). Raises LookupError."""
    conn = get_connection()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """
            SELECT id, picking_number
            FROM distribuidora.loads
            WHERE id = %s AND status <> 'cancelled'
            """,
            (load_id,),
        )
        row = cur.fetchone()
        if not row:
            raise LookupError(f"Carga {load_id} no encontrada")
        picking = str(row["picking_number"] or "").strip()
        plan_id = resolve_plan_id_from_picking_number(picking)
        if plan_id is None:
            raise LookupError(
                f"No hay planificación asociada al picking '{picking}'. "
                "El mapa de entregas usa las órdenes de la planificación (PLAN-xxxxx)."
            )
        return plan_id, picking
    finally:
        conn.close()


def _fetch_plan(cur, plan_id: int) -> dict[str, Any]:
    cur.execute(
        """
        SELECT
            id,
            COALESCE(NULLIF(BTRIM(planning_code), ''),
                     'PLAN-' || LPAD(id::text, 5, '0')) AS planning_code,
            status,
            planning_date,
            truck_name,
            route_name,
            driver_name
        FROM distribuidora.dispatch_plan
        WHERE id = %s
        """,
        (plan_id,),
    )
    row = cur.fetchone()
    if not row:
        raise LookupError(f"Plan {plan_id} no encontrado")
    return dict(row)


def _latest_delivery_status_by_key(cur, plan_id: int) -> dict[str, dict[str, Any]]:
    cur.execute(
        """
        SELECT DISTINCT ON (payload->>'customer_key')
            payload->>'customer_key' AS customer_key,
            COALESCE(payload->>'status', 'pending') AS status,
            created_at,
            user_name
        FROM distribuidora.dispatch_plan_order_events
        WHERE dispatch_plan_id = %s
          AND action = %s
          AND COALESCE(payload->>'customer_key', '') <> ''
        ORDER BY payload->>'customer_key', created_at DESC, id DESC
        """,
        (plan_id, ACTION_DELIVERY_STOP),
    )
    out: dict[str, dict[str, Any]] = {}
    for r in cur.fetchall():
        key = str(r["customer_key"])
        st = str(r["status"] or "pending")
        if st not in VALID_STOP_STATUS:
            st = "pending"
        out[key] = {
            "status": st,
            "updated_at": r["created_at"].isoformat() if r["created_at"] else None,
            "updated_by": r["user_name"],
        }
    return out


def get_delivery_map(plan_id: int) -> dict[str, Any]:
    conn = get_connection()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        plan = _fetch_plan(cur, plan_id)
        status_map = _latest_delivery_status_by_key(cur, plan_id)

        # Una sola query: órdenes + coords efectivas (order → rutero → clients)
        cur.execute(
            f"""
            SELECT
                o.id AS order_row_id,
                o.oc_document_id,
                o.oc_number,
                o.route_order,
                o.client_id,
                o.client_name,
                o.fantasy_name,
                o.address,
                o.city,
                o.oc_total_amount,
                o.cantidad_unidades,
                o.cantidad_productos,
                o.document_type_to_generate,
                o.payment_method,
                o.seller_name,
                o.lat AS order_lat,
                o.lng AS order_lng,
                {R_LAT} AS rutero_lat,
                {R_LON} AS rutero_lng,
                c.lat AS client_lat,
                c.lon AS client_lng
            FROM distribuidora.dispatch_plan_orders o
            LEFT JOIN bsale.rutero r
              ON o.client_id IS NOT NULL AND r.bsale_id = o.client_id
            LEFT JOIN bsale.clients c
              ON o.client_id IS NOT NULL AND c.bsale_id = o.client_id
            WHERE o.dispatch_plan_id = %s
            ORDER BY o.route_order NULLS LAST, o.id
            """,
            (plan_id,),
        )
        rows = [dict(r) for r in cur.fetchall()]

        grouped: dict[str, dict[str, Any]] = {}
        for row in rows:
            key = _customer_key(row.get("client_id"), row.get("client_name"))
            coords = (
                _valid_coords(row.get("order_lat"), row.get("order_lng"))
                or _valid_coords(row.get("rutero_lat"), row.get("rutero_lng"))
                or _valid_coords(row.get("client_lat"), row.get("client_lng"))
            )
            coord_source = None
            if _valid_coords(row.get("order_lat"), row.get("order_lng")):
                coord_source = "dispatch_plan_orders.lat/lng"
            elif _valid_coords(row.get("rutero_lat"), row.get("rutero_lng")):
                coord_source = "bsale.rutero (operacional/sync)"
            elif _valid_coords(row.get("client_lat"), row.get("client_lng")):
                coord_source = "bsale.clients.lat/lon"

            order = {
                "oc_document_id": row.get("oc_document_id"),
                "oc_number": row.get("oc_number"),
                "document_type": row.get("document_type_to_generate"),
                "payment_method": row.get("payment_method"),
                "amount": float(row["oc_total_amount"])
                if row.get("oc_total_amount") is not None
                else None,
                "units": float(row["cantidad_unidades"])
                if row.get("cantidad_unidades") is not None
                else None,
                "route_order": row.get("route_order"),
            }

            if key not in grouped:
                st_info = status_map.get(key) or {}
                grouped[key] = {
                    "customer_key": key,
                    "customer_id": int(row["client_id"])
                    if row.get("client_id") is not None
                    else None,
                    "customer_name": row.get("client_name") or "Sin nombre",
                    "fantasy_name": row.get("fantasy_name"),
                    "address": row.get("address"),
                    "city": row.get("city"),
                    "latitude": coords[0] if coords else None,
                    "longitude": coords[1] if coords else None,
                    "has_coordinates": coords is not None,
                    "coordinates_source": coord_source,
                    "status": st_info.get("status") or "pending",
                    "status_updated_at": st_info.get("updated_at"),
                    "status_updated_by": st_info.get("updated_by"),
                    "orders": [],
                    "documents": [],
                    "amount": 0.0,
                    "items_count": 0,
                    "units_count": 0.0,
                    "min_route_order": row.get("route_order") or 10_000,
                }

            g = grouped[key]
            g["orders"].append(order)
            if order["oc_number"] is not None:
                g["documents"].append(str(order["oc_number"]))
            if order["amount"] is not None:
                g["amount"] += order["amount"]
            if order["units"] is not None:
                g["units_count"] += order["units"]
            g["items_count"] += 1
            if coords and not g["has_coordinates"]:
                g["latitude"], g["longitude"] = coords
                g["has_coordinates"] = True
                g["coordinates_source"] = coord_source
            if not g.get("address") and row.get("address"):
                g["address"] = row.get("address")
            if not g.get("city") and row.get("city"):
                g["city"] = row.get("city")
            ro = row.get("route_order")
            if ro is not None and ro < g["min_route_order"]:
                g["min_route_order"] = ro

        stops = list(grouped.values())
        stops.sort(key=lambda s: (s["min_route_order"], s["customer_name"] or ""))

        for s in stops:
            s.pop("min_route_order", None)
            s["amount"] = round(float(s["amount"]), 2)
            s["units_count"] = round(float(s["units_count"]), 3)
            s["search_text"] = " ".join(
                filter(
                    None,
                    [
                        s.get("customer_name"),
                        s.get("fantasy_name"),
                        s.get("address"),
                        s.get("city"),
                        " ".join(s.get("documents") or []),
                    ],
                )
            ).lower()

        with_coords = sum(1 for s in stops if s["has_coordinates"])
        delivered = sum(1 for s in stops if s["status"] == "delivered")
        pending = len(stops) - delivered

        return {
            "load": {
                "id": int(plan["id"]),
                "plan_id": int(plan["id"]),
                "picking_number": _planning_code(plan),
                "status": plan.get("status"),
                "planning_date": plan["planning_date"].isoformat()
                if plan.get("planning_date")
                else None,
                "truck_name": plan.get("truck_name"),
                "route_name": plan.get("route_name"),
                "driver_name": plan.get("driver_name"),
            },
            "summary": {
                "orders": len(rows),
                "customers": len(stops),
                "with_coordinates": with_coords,
                "without_coordinates": len(stops) - with_coords,
                "delivered": delivered,
                "pending": pending,
                "coordinates_primary_source": "distribuidora.dispatch_plan_orders.lat/lng",
            },
            "stops": stops,
        }
    finally:
        conn.close()


def set_delivery_stop_status(
    plan_id: int,
    customer_key: str,
    status: str,
    *,
    user_email: str,
) -> dict[str, Any]:
    status = (status or "").strip().lower()
    if status not in VALID_STOP_STATUS:
        raise ValueError("status debe ser 'pending' o 'delivered'")
    key = (customer_key or "").strip()
    if not key:
        raise ValueError("customer_key requerido")

    data = get_delivery_map(plan_id)
    stop = next((s for s in data["stops"] if s["customer_key"] == key), None)
    if not stop:
        raise LookupError(f"Cliente '{key}' no pertenece al plan {plan_id}")

    conn = get_connection()
    try:
        cur = conn.cursor()
        batch_repo.insert_order_event(
            cur,
            plan_id=plan_id,
            action=ACTION_DELIVERY_STOP,
            user_name=user_email,
            reason=status,
            oc_document_id=(stop["orders"][0].get("oc_document_id") if stop["orders"] else None),
            oc_number=(
                int(stop["orders"][0]["oc_number"])
                if stop["orders"] and stop["orders"][0].get("oc_number") is not None
                else None
            ),
            payload={
                "customer_key": key,
                "client_id": stop.get("customer_id"),
                "customer_name": stop.get("customer_name"),
                "status": status,
            },
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return get_delivery_map(plan_id)
