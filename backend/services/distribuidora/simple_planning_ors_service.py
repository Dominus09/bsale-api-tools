"""Cálculo ORS acotado a planificación simplificada (orden preservado).

No abre el endpoint ORS tradicional a capability-only.
Las coordenadas se leen de ``bsale.clients``; no se confía en lat/lng del frontend.
"""

from __future__ import annotations

import hashlib
import logging
import threading
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Any

from backend.db import get_connection
from backend.services.distribuidora.logistics_cost_service import (
    get_logistics_cost_settings,
    load_active_trucks,
)
from backend.services.distribuidora.oc_source_resolver import OC_DOCUMENT_TYPE_ID
from backend.services.distribuidora.planificacion_ors_service import (
    BODEGA_LAT,
    BODEGA_LNG,
    compute_ors_route_for_truck,
    depot_base,
)
from backend.services.distribuidora.system_config_service import (
    DEFAULT_DIESEL_CLP_PER_LITER,
    get_diesel_price_per_liter,
)
from backend.utils.json_safe import serialize_row

logger = logging.getLogger(__name__)

# Rate limit in-process (por worker): capability-only no debe martillar ORS.
_RATE_LOCK = threading.Lock()
_RATE_HITS: dict[int, deque[float]] = defaultdict(deque)
_RATE_MAX = 12
_RATE_WINDOW_SEC = 60.0


class SimplePlanningOrsError(Exception):
    def __init__(
        self,
        message: str,
        *,
        code: str = "ors_error",
        http_status: int = 400,
        details: dict[str, Any] | None = None,
    ):
        super().__init__(message)
        self.code = code
        self.http_status = http_status
        self.details = details or {}


def _check_rate_limit(user_id: int) -> None:
    now = time.monotonic()
    with _RATE_LOCK:
        q = _RATE_HITS[int(user_id)]
        while q and now - q[0] > _RATE_WINDOW_SEC:
            q.popleft()
        if len(q) >= _RATE_MAX:
            raise SimplePlanningOrsError(
                "Demasiadas solicitudes de ruta; espere un momento",
                code="rate_limited",
                http_status=429,
            )
        q.append(now)


def route_input_fingerprint(
    *,
    truck_id: int,
    stops: list[dict[str, Any]],
) -> str:
    """Huella de entradas ORS (orden + coords redondeadas)."""
    parts = [f"t{int(truck_id)}"]
    for s in stops:
        lat = round(float(s["lat"]), 5)
        lng = round(float(s["lng"]), 5)
        parts.append(f"{int(s['document_id'])}:{lat}:{lng}")
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def _load_stops_from_db(
    *,
    company_id: int,
    office_id: int,
    document_ids: list[int],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """
    Carga paradas en el orden de ``document_ids``.
    Consolida mismo cliente+dirección en una parada (primera OC define orden).
    """
    if not document_ids:
        raise SimplePlanningOrsError("Sin document_ids", code="empty_stops")
    ids = [int(x) for x in document_ids]
    if len(ids) != len(set(ids)):
        raise SimplePlanningOrsError(
            "document_ids duplicados",
            code="duplicate_document_ids",
        )
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                d.document_id,
                d.number AS folio,
                d.client_id,
                COALESCE(
                    NULLIF(BTRIM(d.municipality), ''),
                    NULLIF(BTRIM(c.municipality), ''),
                    NULLIF(BTRIM(d.city), ''),
                    NULLIF(BTRIM(c.city), '')
                ) AS municipality,
                COALESCE(
                    NULLIF(BTRIM(d.address), ''),
                    NULLIF(BTRIM(c.address), '')
                ) AS address,
                COALESCE(
                    NULLIF(BTRIM(c.nombre_fantasia), ''),
                    NULLIF(BTRIM(c.company), ''),
                    CONCAT_WS(
                        ' ',
                        NULLIF(BTRIM(c.first_name), ''),
                        NULLIF(BTRIM(c.last_name), '')
                    )
                ) AS client_name,
                c.lat::double precision AS lat,
                c.lon::double precision AS lng
            FROM distribuidora.documents d
            LEFT JOIN bsale.clients c
                ON c.company_id = d.company_id
               AND c.bsale_id = d.client_id
            WHERE d.company_id = %s
              AND d.office_id = %s
              AND d.document_type_id = %s
              AND COALESCE(d.state, 0) = 0
              AND d.document_id = ANY(%s::bigint[])
            """,
            (int(company_id), int(office_id), int(OC_DOCUMENT_TYPE_ID), ids),
        )
        cols = [d[0] for d in cur.description]
        by_id: dict[int, dict[str, Any]] = {}
        for row in cur.fetchall():
            item = dict(zip(cols, row))
            by_id[int(item["document_id"])] = item
        cur.close()
    finally:
        conn.close()

    missing = [i for i in ids if i not in by_id]
    if missing:
        raise SimplePlanningOrsError(
            f"OC no autorizadas o inexistentes en el alcance: {missing[:8]}",
            code="documents_out_of_scope",
            http_status=403,
            details={"missing_document_ids": missing},
        )

    pending: list[dict[str, Any]] = []
    geo_rows: list[dict[str, Any]] = []
    for doc_id in ids:
        row = by_id[doc_id]
        lat, lng = row.get("lat"), row.get("lng")
        base = {
            "document_id": doc_id,
            "folio": int(row["folio"]) if row.get("folio") is not None else None,
            "client_id": int(row["client_id"]) if row.get("client_id") is not None else None,
            "client_name": row.get("client_name"),
            "address": row.get("address"),
            "municipality": row.get("municipality"),
        }
        if lat is None or lng is None:
            pending.append(
                {
                    **base,
                    "has_georef": False,
                    "lat": None,
                    "lng": None,
                    "missing_georef_reason": "client_lat_lon_null",
                }
            )
            continue
        try:
            lat_f, lng_f = float(lat), float(lng)
        except (TypeError, ValueError):
            pending.append(
                {
                    **base,
                    "has_georef": False,
                    "lat": None,
                    "lng": None,
                    "missing_georef_reason": "client_lat_lon_invalid",
                }
            )
            continue
        # 0,0 y fuera de rango Chile operativo: no inventar parada.
        if (
            (lat_f == 0.0 and lng_f == 0.0)
            or not (-56.0 <= lat_f <= -17.0)
            or not (-76.0 <= lng_f <= -66.0)
        ):
            pending.append(
                {
                    **base,
                    "has_georef": False,
                    "lat": None,
                    "lng": None,
                    "missing_georef_reason": "client_lat_lon_out_of_range",
                }
            )
            continue
        geo_rows.append({**base, "has_georef": True, "lat": lat_f, "lng": lng_f})

    # Consolidar mismo cliente + dirección (conserva orden de primera OC).
    consolidated: list[dict[str, Any]] = []
    seen: set[tuple[Any, str]] = set()
    for row in geo_rows:
        key = (row.get("client_id"), (row.get("address") or "").strip().lower())
        if key in seen:
            # Adjuntar folio a la parada existente
            for stop in consolidated:
                sk = (stop.get("client_id"), (stop.get("address") or "").strip().lower())
                if sk == key:
                    stop.setdefault("oc_document_ids", [stop["document_id"]])
                    if row["document_id"] not in stop["oc_document_ids"]:
                        stop["oc_document_ids"].append(row["document_id"])
                    stop.setdefault("folios", [stop.get("folio")])
                    if row.get("folio") not in stop["folios"]:
                        stop["folios"].append(row.get("folio"))
                    break
            continue
        seen.add(key)
        consolidated.append(
            {
                **row,
                "oc_document_ids": [row["document_id"]],
                "folios": [row.get("folio")],
            }
        )
    return consolidated, pending, geo_rows


def calculate_simple_planning_route(
    *,
    user_id: int,
    company_id: int,
    office_id: int,
    truck_id: int,
    document_ids: list[int],
    camion: str | None = None,
) -> dict[str, Any]:
    """
    Calcula ruta ORS con orden preservado para OC del alcance.
    No escribe dispatch_plan; el resultado se vincula luego a la sesión.
    """
    _check_rate_limit(int(user_id))
    if int(truck_id) <= 0:
        raise SimplePlanningOrsError("truck_id inválido")

    stops_geo, pending, _all_geo = _load_stops_from_db(
        company_id=int(company_id),
        office_id=int(office_id),
        document_ids=[int(x) for x in document_ids],
    )

    truck_name = (camion or "").strip() or f"truck-{truck_id}"
    fingerprint = route_input_fingerprint(
        truck_id=int(truck_id),
        stops=[
            {"document_id": s["document_id"], "lat": s["lat"], "lng": s["lng"]}
            for s in stops_geo
        ],
    )

    pending_clients = _consolidate_pending_clients(pending)
    clients_georef = len(stops_geo)
    clients_pending = len(pending_clients)
    clients_total = clients_georef + clients_pending
    if clients_georef <= 0:
        route_coverage = "none"
    elif clients_pending > 0:
        route_coverage = "partial"
    else:
        route_coverage = "full"

    base_out: dict[str, Any] = {
        "ok": False,
        "ors_metrics_calculated": False,
        "preserve_order": True,
        "truck_id": int(truck_id),
        "route_fingerprint": fingerprint,
        "route_coverage": route_coverage,
        "clients_total": clients_total,
        "clients_georef": clients_georef,
        "clients_pending": clients_pending,
        "stops_georef_count": clients_georef,
        "stops_pending_georef": pending_clients,
        "stops_pending_count": clients_pending,
        "km_scope": "georeferenced_stops_only" if route_coverage == "partial" else (
            "full_dispatch" if route_coverage == "full" else None
        ),
        "stops": stops_geo,
        "depot": {"lat": BODEGA_LAT, "lng": BODEGA_LNG},
        "distance_km": None,
        "duration_min": None,
        "geometry": None,
        "liters_estimated": None,
        "fuel_cost_clp": None,
        "crew_cost_clp": None,
        "total_route_cost_clp": None,
        "cost_breakdown": None,
        "message": None,
        "calculated_at": datetime.now(timezone.utc).isoformat(),
    }

    if len(stops_geo) < 1:
        base_out["message"] = (
            f"Georreferenciados: 0 de {clients_total} clientes. "
            "Kilómetros y costos no disponibles."
        )
        return base_out

    conn = get_connection()
    try:
        try:
            diesel_clp = get_diesel_price_per_liter(conn)
            trucks_by_id, trucks_by_name = load_active_trucks(conn)
            logistics_settings = get_logistics_cost_settings(conn)
        except Exception:
            logger.exception("simple_planning_ors_config_fallback")
            diesel_clp = DEFAULT_DIESEL_CLP_PER_LITER
            trucks_by_id, trucks_by_name = {}, {}
            logistics_settings = get_logistics_cost_settings()
    finally:
        conn.close()

    if int(truck_id) not in trucks_by_id and trucks_by_id:
        # Si el catálogo cargó y el camión no está, rechazar (no inventar yield).
        raise SimplePlanningOrsError(
            "Camión no autorizado o inactivo",
            code="truck_invalid",
            http_status=403,
        )

    ors_stops = [
        {"document_id": int(s["document_id"]), "lat": float(s["lat"]), "lon": float(s["lng"])}
        for s in stops_geo
    ]
    try:
        row = compute_ors_route_for_truck(
            camion=truck_name,
            stops=ors_stops,
            truck_id=int(truck_id),
            trucks_by_id=trucks_by_id,
            trucks_by_name=trucks_by_name,
            diesel_clp=float(diesel_clp),
            logistics_settings=logistics_settings,
            driver_count=1,
            assistant_count=0,
            preserve_order=True,
        )
    except Exception as exc:
        logger.exception("simple_planning_ors_compute_failed")
        base_out["message"] = f"ORS no disponible: {exc}"
        return base_out

    if row is None:
        base_out["message"] = (
            "ORS no pudo calcular la geometría. Marcadores disponibles; "
            "kilómetros y costos no disponibles."
        )
        return base_out

    bd = row.get("cost_breakdown") or {}
    total_clp = bd.get("total_clp")
    if total_clp is None:
        total_clp = int(row.get("fuel_cost_clp") or 0) + int(row.get("crew_cost_clp") or 0)

    geometry = row.get("geometry")
    if isinstance(geometry, dict):
        # Miembros GeoJSON extranjeros: cobertura sin migración ni romper LineString.
        geometry = {
            **geometry,
            "quillotana_route_meta": {
                "route_coverage": route_coverage,
                "clients_total": clients_total,
                "clients_georef": clients_georef,
                "clients_pending": clients_pending,
                "km_scope": base_out["km_scope"],
            },
        }

    coverage_msg = (
        f"Ruta parcial: {clients_georef} de {clients_total} clientes georreferenciados. "
        "Km/costos solo del recorrido calculado (no representan todo el despacho)."
        if route_coverage == "partial"
        else None
    )

    return {
        **base_out,
        "ok": True,
        "ors_metrics_calculated": True,
        "distance_km": row.get("distance_km"),
        "duration_min": row.get("duration_min"),
        "geometry": geometry,
        "stops_ordered": row.get("stops_ordered"),
        "liters_estimated": row.get("liters_estimated"),
        "fuel_cost_clp": row.get("fuel_cost_clp"),
        "crew_cost_clp": row.get("crew_cost_clp"),
        "total_route_cost_clp": int(total_clp) if total_clp is not None else None,
        "cost_breakdown": serialize_row(bd) if isinstance(bd, dict) else bd,
        "diesel_price_per_liter": round(float(diesel_clp), 2),
        "depot": row.get("depot") or depot_base(),
        "message": coverage_msg,
        # Snapshot: métricas solo del backend (prepare/confirm las recalculan).
        "ors_snapshot": {
            "route_fingerprint": fingerprint,
            "truck_id": int(truck_id),
            "document_ids": [int(s["document_id"]) for s in stops_geo],
            "all_document_ids": [int(x) for x in document_ids],
            "route_coverage": route_coverage,
            "clients_total": clients_total,
            "clients_georef": clients_georef,
            "clients_pending": clients_pending,
            "km_scope": base_out["km_scope"],
            "distance_km": row.get("distance_km"),
            "duration_min": row.get("duration_min"),
            "liters_estimated": row.get("liters_estimated"),
            "diesel_price_per_liter": round(float(diesel_clp), 2),
            "fuel_cost_clp": int(row.get("fuel_cost_clp") or 0),
            "crew_cost_clp": int(row.get("crew_cost_clp") or 0),
            "ferry_cost_clp": 0,
            "toll_cost_clp": 0,
            "extras_cost_clp": 0,
            "total_route_cost_clp": int(total_clp) if total_clp is not None else 0,
            "route_geometry": geometry,
            "preserve_order": True,
            "calculated_at": base_out["calculated_at"],
            "depot": {"lat": BODEGA_LAT, "lng": BODEGA_LNG},
            "stop_coords": [
                {
                    "document_id": int(s["document_id"]),
                    "lat": float(s["lat"]),
                    "lng": float(s["lng"]),
                }
                for s in stops_geo
            ],
            "pending_clients": pending_clients,
        },
    }


def _consolidate_pending_clients(pending: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Agrupa OC sin geo por cliente+dirección; motivo explícito."""
    out: list[dict[str, Any]] = []
    seen: dict[tuple[Any, str], int] = {}
    for row in pending:
        key = (row.get("client_id"), (row.get("address") or "").strip().lower())
        idx = seen.get(key)
        if idx is not None:
            stop = out[idx]
            stop.setdefault("oc_document_ids", [])
            if row.get("document_id") not in stop["oc_document_ids"]:
                stop["oc_document_ids"].append(row["document_id"])
            stop.setdefault("folios", [])
            if row.get("folio") not in stop["folios"]:
                stop["folios"].append(row.get("folio"))
            continue
        seen[key] = len(out)
        out.append(
            {
                **row,
                "oc_document_ids": [row["document_id"]],
                "folios": [row.get("folio")],
                "missing_georef_reason": "client_lat_lon_null",
            }
        )
    return out


def recompute_ors_snapshot_for_session(
    *,
    user_id: int,
    company_id: int,
    office_id: int,
    truck_id: int,
    document_ids: list[int],
    camion: str | None = None,
    client_hint_fingerprint: str | None = None,
) -> dict[str, Any]:
    """
    Recalcula ORS en servidor. Ignora km/costos enviados por el frontend.
    Si ``client_hint_fingerprint`` no coincide, la asignación cambió → error.
    """
    result = calculate_simple_planning_route(
        user_id=int(user_id),
        company_id=int(company_id),
        office_id=int(office_id),
        truck_id=int(truck_id),
        document_ids=[int(x) for x in document_ids],
        camion=camion,
    )
    snap = result.get("ors_snapshot") if result.get("ok") else None
    if not snap:
        raise SimplePlanningOrsError(
            result.get("message") or "No se pudo recalcular la ruta ORS en servidor",
            code="ors_recompute_failed",
            http_status=409,
        )
    if client_hint_fingerprint and str(client_hint_fingerprint) != str(
        snap.get("route_fingerprint")
    ):
        raise SimplePlanningOrsError(
            "La ruta previa ya no coincide con OC/coordenadas actuales; recalcule.",
            code="ors_snapshot_stale",
            http_status=409,
        )
    return snap


def validate_ors_snapshot_for_orders(
    snapshot: dict[str, Any] | None,
    *,
    truck_id: int,
    ordered_document_ids: list[int],
    company_id: int,
    office_id: int,
) -> dict[str, Any] | None:
    """
    Revalida snapshot contra OC actuales (orden + coords DB).
    Si no coincide, retorna None — el confirm debe fallar (no degradar a sin ORS).
    """
    if not snapshot or not snapshot.get("ors_metrics_calculated", True):
        # snapshots store metrics implicitly when present
        pass
    if not isinstance(snapshot, dict):
        return None
    if int(snapshot.get("truck_id") or 0) != int(truck_id):
        return None
    stored_ids = [int(x) for x in (snapshot.get("document_ids") or [])]
    # Releer coords del conjunto georef actual en el mismo orden de sesión
    # filtrando solo las que tienen geo (como en el cálculo original).
    stops_geo, _pending, _ = _load_stops_from_db(
        company_id=int(company_id),
        office_id=int(office_id),
        document_ids=[int(x) for x in ordered_document_ids],
    )
    current_ids = [int(s["document_id"]) for s in stops_geo]
    if current_ids != stored_ids:
        return None
    fp = route_input_fingerprint(
        truck_id=int(truck_id),
        stops=[
            {"document_id": s["document_id"], "lat": s["lat"], "lng": s["lng"]}
            for s in stops_geo
        ],
    )
    if fp != str(snapshot.get("route_fingerprint") or ""):
        return None
    if snapshot.get("distance_km") is None or snapshot.get("route_geometry") is None:
        return None
    return snapshot
