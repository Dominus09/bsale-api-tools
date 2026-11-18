"""Avance del recorrido del chofer (clientes únicos del plan).

Dos métricas independientes:
- route_progress: clientes con visita finalizada persistida / total asignados
- by_result: semáforo de entregas (completa / parcial / no realizada / …)

No cuenta documentos como clientes. Ignora visitas anuladas (vía attention).
"""

from __future__ import annotations

from typing import Any

MANAGED_STATUSES = frozenset({"completed", "partial", "not_delivered"})


def compute_route_progress(
    *,
    clients: list[dict[str, Any]],
    attention_by_key: dict[str, dict[str, Any]],
    origin_lat: float | None = None,
    origin_lng: float | None = None,
) -> dict[str, Any]:
    """Agrega avance a partir del índice de clientes + attention canónica."""
    total = len(clients)
    by_result = {
        "completed": 0,
        "partial": 0,
        "not_delivered": 0,
        "pending": 0,
        "in_progress": 0,
        "pending_review": 0,
    }
    managed = 0
    pending_keys: list[dict[str, Any]] = []
    inconsistencies: list[str] = []

    for c in clients:
        key = str(c.get("customer_key") or "")
        if not key:
            continue
        att = attention_by_key.get(key) or {}
        status = str(att.get("attention_status") or att.get("visit_status") or "pending")
        review = str(att.get("closure_review_status") or "")
        admin_dup = bool(att.get("admin_review_pending"))

        if status in MANAGED_STATUSES:
            managed += 1
            if status == "completed":
                by_result["completed"] += 1
            elif status == "partial":
                by_result["partial"] += 1
            else:
                by_result["not_delivered"] += 1
        elif status == "in_progress":
            by_result["in_progress"] += 1
            pending_keys.append(c)
        else:
            by_result["pending"] += 1
            pending_keys.append(c)

        if review == "pending_review" or admin_dup:
            by_result["pending_review"] += 1

        # Señal blanda: cierre + visita abierta concurrente
        if admin_dup and status in MANAGED_STATUSES:
            inconsistencies.append(
                f"{key}: cierre operativo con visita abierta (revisión admin)"
            )

    remaining = max(0, total - managed)
    percent = 0.0 if total <= 0 else round(100.0 * managed / total, 1)

    next_customer = _pick_next(
        pending_keys,
        attention_by_key=attention_by_key,
        origin_lat=origin_lat,
        origin_lng=origin_lng,
    )

    return {
        "total_clients": total,
        "managed_clients": managed,
        "remaining_clients": remaining,
        "percent": percent,
        "by_result": by_result,
        "next_customer": next_customer,
        "inconsistencies": inconsistencies,
        "formula_es": (
            "Avance = clientes con visita finalizada (completa/parcial/no realizada) "
            "/ clientes únicos del plan. Local cerrado cuenta como gestionado, "
            "no como entrega exitosa. pending_review es independiente del semáforo."
        ),
    }


def _pick_next(
    pending: list[dict[str, Any]],
    *,
    attention_by_key: dict[str, dict[str, Any]],
    origin_lat: float | None,
    origin_lng: float | None,
) -> dict[str, Any] | None:
    if not pending:
        return None

    def _pack(c: dict[str, Any], *, reason: str, distance_m: float | None) -> dict[str, Any]:
        key = str(c.get("customer_key") or "")
        att = attention_by_key.get(key) or {}
        return {
            "customer_key": key,
            "client_id": c.get("client_id"),
            "client_name": c.get("client_name"),
            "address": c.get("address") or "",
            "city": c.get("city") or "",
            "route_order": c.get("route_order"),
            "has_coordinates": bool(c.get("has_coordinates")),
            "latitude": c.get("latitude"),
            "longitude": c.get("longitude"),
            "distance_m": distance_m,
            "attention_status": att.get("attention_status") or "pending",
            "semaphore_color": att.get("semaphore_color") or "neutral",
            "document_count": c.get("document_count") or 0,
            "amount_total": c.get("amount_total"),
            "suggestion_reason": reason,
            "suggestion_reason_es": (
                "Más cercano" if reason == "nearest" else "Siguiente en ruta"
            ),
            "distance_basis": "haversine_m" if distance_m is not None else None,
        }

    # Preferir pendientes con coords + origen GPS → más cercano
    if origin_lat is not None and origin_lng is not None:
        from backend.services.distribuidora.delivery_semaphore import haversine_m

        with_coords: list[tuple[float, dict[str, Any]]] = []
        for c in pending:
            if not c.get("has_coordinates"):
                continue
            try:
                lat = float(c["latitude"])
                lng = float(c["longitude"])
            except (TypeError, ValueError, KeyError):
                continue
            dist = haversine_m(float(origin_lat), float(origin_lng), lat, lng)
            with_coords.append((dist, c))
        if with_coords:
            with_coords.sort(key=lambda x: x[0])
            dist, c = with_coords[0]
            return _pack(c, reason="nearest", distance_m=round(dist, 1))

    # Sin GPS usable → orden de ruta (route_order, luego nombre)
    ordered = sorted(
        pending,
        key=lambda c: (
            c.get("route_order") is None,
            int(c["route_order"]) if c.get("route_order") is not None else 10**9,
            str(c.get("client_name") or ""),
        ),
    )
    return _pack(ordered[0], reason="route_order", distance_m=None)


def get_plan_progress_for_driver(
    plan_id: int,
    *,
    origin_lat: float | None = None,
    origin_lng: float | None = None,
) -> dict[str, Any]:
    """Agregación servidor para un plan. Reutiliza índice + attention."""
    from backend.services.distribuidora import dispatch_plan_pwa_driver_read_service as read_svc

    bundle = read_svc._build_client_index(int(plan_id))  # noqa: SLF001
    attention = read_svc._visit_attention_by_customer(int(plan_id))  # noqa: SLF001
    clients = list(bundle.get("clients") or [])
    progress = compute_route_progress(
        clients=clients,
        attention_by_key=attention,
        origin_lat=origin_lat,
        origin_lng=origin_lng,
    )
    return {
        "dispatch_plan_id": int(plan_id),
        "progress": progress,
        "summary": bundle.get("summary") or {},
        "picking_ready": bundle.get("picking_ready"),
    }
