"""Lectura PWA chofer: clientes, documentos, productos y mapa (solo lectura).

Fuentes canónicas (sin reconstruir picking ni inferir por nombre comercial):
- Clientes/agrupación: ``dispatch_plan_orders`` vía ``get_delivery_map``
- Documentos facturados: picking persistido ``get_picking_by_client``
- Grupos de carga: ``dispatch_plan_document_load_assignments``
- Productos/cantidades: ``compute_picking_products_for_related_documents``
- Coordenadas: order → rutero → clients (solo si ya existen; sin geocodificar)
"""

from __future__ import annotations

import math
import re
from typing import Any

from fastapi import HTTPException

from backend.auth.multiapp import company_allowed, get_scope
from backend.db import get_connection
from backend.repositories.distribuidora import dispatch_plan_load_batch_repo as batch_repo
from backend.services.distribuidora import delivery_map_service as map_svc
from backend.services.distribuidora import dispatch_plan_driver_service as driver_svc
from backend.services.distribuidora import dispatch_plan_service as plan_svc
from backend.utils.json_safe import serialize_value


FUTURE_CONTRACTS = {
    "facade_photo": {
        "status": "not_implemented",
        "phase": "P3+",
        "storage": "cloudflare_r2_private",
        "note_es": (
            "Fotografía permanente de fachada: subida privada a R2, "
            "asociada a client_id del despacho; requiere validación administrativa."
        ),
        "fields_planned": [
            "client_id",
            "object_key",
            "content_type",
            "captured_at",
            "captured_by_user_id",
            "validation_status",
        ],
    },
    "gps_correction_proposal": {
        "status": "not_implemented",
        "phase": "P3+",
        "note_es": (
            "Propuesta de corrección GPS del cliente: lat/lng + evidencia; "
            "no escribe en bsale.clients hasta aprobación admin."
        ),
        "fields_planned": [
            "client_id",
            "proposed_lat",
            "proposed_lng",
            "accuracy_m",
            "evidence_object_key",
            "proposed_by_user_id",
            "admin_validation_status",
        ],
    },
    "admin_validation": {
        "status": "not_implemented",
        "phase": "P3+",
        "note_es": "Cola administrativa para aprobar foto de fachada y GPS propuesto.",
    },
    "delivery_evidence_photo": {
        "status": "not_implemented",
        "phase": "P3+",
        "storage": "cloudflare_r2_private",
        "note_es": (
            "Evidencia fotográfica de entrega por documento/parada. "
            "Sin buckets, migraciones ni eventos de entrega en P2."
        ),
    },
}


def _uid(user: dict[str, Any]) -> int:
    raw = user.get("id") if user.get("id") is not None else user.get("uid")
    try:
        return int(raw)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=401, detail="Usuario sin id") from exc


def enforce_driver_plan_read_access(user: dict[str, Any], plan_id: int) -> int:
    """JWT+PWA+chofer (dep) + driver_user_id + empresa/oficina del plan."""
    uid = _uid(user)
    if not driver_svc.driver_has_access_to_plan(uid, int(plan_id)):
        raise HTTPException(status_code=404, detail="Despacho no encontrado")

    scope = get_scope(user)
    if scope.load_failed:
        raise HTTPException(
            status_code=503,
            detail="No se pudo verificar autorización multiaplicación",
        )

    conn = get_connection()
    try:
        cur = conn.cursor()
        company_id, office_id = driver_svc.resolve_plan_company_office(cur, int(plan_id))
        cur.close()
    finally:
        conn.close()

    # Misma semántica que elegibilidad de asignación: empresa si está acotada;
    # oficina solo si el usuario tiene oficinas cargadas (revocación inmediata).
    if company_id is not None:
        if getattr(scope, "companies_unverified", False):
            raise HTTPException(
                status_code=503,
                detail="No se pudo verificar empresas autorizadas",
            )
        if not company_allowed(scope, int(company_id)):
            raise HTTPException(status_code=403, detail="Empresa no autorizada")
        if office_id is not None:
            if getattr(scope, "offices_unverified", False):
                raise HTTPException(
                    status_code=503,
                    detail="No se pudo verificar oficinas autorizadas",
                )
            if scope.offices and (int(company_id), int(office_id)) not in scope.offices:
                raise HTTPException(status_code=403, detail="Oficina no autorizada")
    return uid


def _customer_key(client_id: Any, client_name: str | None) -> str:
    if client_id is not None:
        try:
            return f"id:{int(client_id)}"
        except (TypeError, ValueError):
            pass
    name = re.sub(r"\s+", " ", (client_name or "").strip().lower())
    return f"name:{name or 'sin-nombre'}"


def _norm_name(name: str | None) -> str:
    return re.sub(r"\s+", " ", (name or "").strip().lower())


def _num(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def _list_document_assignments(plan_id: int) -> list[dict[str, Any]]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        rows = batch_repo.list_document_assignments(cur, int(plan_id))
        cur.close()
        return rows
    finally:
        conn.close()


def _units_by_related_document(related_ids: list[int]) -> dict[int, float]:
    if not related_ids:
        return {}
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT document_id, COALESCE(SUM(quantity), 0)::float
            FROM distribuidora.document_details
            WHERE document_id = ANY(%s)
            GROUP BY document_id
            """,
            (list(related_ids),),
        )
        out = {int(r[0]): float(r[1]) for r in cur.fetchall()}
        cur.close()
        return out
    finally:
        conn.close()


def _oc_number_by_oc_id(plan_id: int) -> dict[int, Any]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT oc_document_id, oc_number
            FROM distribuidora.dispatch_plan_orders
            WHERE dispatch_plan_id = %s
            """,
            (int(plan_id),),
        )
        out = {int(r[0]): r[1] for r in cur.fetchall() if r[0] is not None}
        cur.close()
        return out
    finally:
        conn.close()


def _product_payload(row: dict[str, Any], *, load_group: dict[str, Any] | None) -> dict[str, Any]:
    sin_caja = bool(row.get("sin_unidad_caja"))
    cajas = row.get("cajas")
    cajas_out = None
    if not sin_caja and cajas is not None:
        try:
            cajas_val = float(cajas)
            if cajas_val > 0:
                cajas_out = cajas_val
        except (TypeError, ValueError):
            cajas_out = None
    return {
        "description": row.get("producto_variante")
        or row.get("display_name")
        or row.get("producto")
        or "",
        "producto": row.get("producto") or "",
        "variante": row.get("variante") or "",
        "product_id": row.get("product_id"),
        "variant_id": row.get("variant_id"),
        "sku_code": row.get("variant_id"),
        "codigo_barras": row.get("codigo_barras"),
        "unidades": _num(row.get("unidades")),
        "cajas_equivalentes": cajas_out,
        "units_per_box": None if sin_caja else row.get("units_per_box"),
        "sin_unidad_caja": sin_caja,
        "load_group": load_group,
    }


def _build_client_index(plan_id: int) -> dict[str, Any]:
    """Índice canónico de clientes del despacho (órdenes + picking + grupos)."""
    delivery = map_svc.get_delivery_map(int(plan_id))
    picking = plan_svc.get_picking_by_client(int(plan_id))
    assignments = _list_document_assignments(int(plan_id))
    oc_numbers = _oc_number_by_oc_id(int(plan_id))

    assign_by_rel: dict[int, dict[str, Any]] = {}
    for a in assignments:
        rel = a.get("related_document_id")
        if rel is None:
            continue
        assign_by_rel[int(rel)] = {
            "load_batch_id": int(a["load_batch_id"]) if a.get("load_batch_id") is not None else None,
            "load_batch_name": a.get("load_batch_name"),
            "load_batch_sort_order": a.get("load_batch_sort_order"),
            "oc_document_id": int(a["oc_document_id"]) if a.get("oc_document_id") is not None else None,
        }

    picking_rows = list(picking.get("clients") or [])
    related_ids = [
        int(r["related_document_id"])
        for r in picking_rows
        if r.get("related_document_id") is not None
    ]
    units_by_rel = _units_by_related_document(related_ids)

    # Mapear picking → stop por OC (canónico) o por related_document / nombre.
    stop_by_key: dict[str, dict[str, Any]] = {
        s["customer_key"]: s for s in (delivery.get("stops") or [])
    }
    stop_by_oc: dict[int, str] = {}
    for s in delivery.get("stops") or []:
        for o in s.get("orders") or []:
            oc_id = o.get("oc_document_id")
            if oc_id is not None:
                stop_by_oc[int(oc_id)] = s["customer_key"]

    clients: dict[str, dict[str, Any]] = {}

    def _ensure_from_stop(stop: dict[str, Any]) -> dict[str, Any]:
        key = stop["customer_key"]
        if key not in clients:
            clients[key] = {
                "customer_key": key,
                "client_id": stop.get("customer_id"),
                "client_name": stop.get("customer_name") or "Sin nombre",
                "fantasy_name": stop.get("fantasy_name") or "",
                "address": stop.get("address") or "",
                "city": stop.get("city") or "",
                "phone": "",
                "observations": "",
                "latitude": stop.get("latitude"),
                "longitude": stop.get("longitude"),
                "has_coordinates": bool(stop.get("has_coordinates")),
                "coordinates_source": stop.get("coordinates_source"),
                "route_order": None,
                "documents": [],
                "load_groups": [],
                "_group_ids": set(),
                "_oc_ids": set(),
                "_folios": [],
                "units_total": 0.0,
                "document_count": 0,
                "facade_photo": None,
                "search_text": "",
            }
        return clients[key]

    for stop in delivery.get("stops") or []:
        _ensure_from_stop(stop)

    for row in picking_rows:
        oc_id = int(row["oc_document_id"]) if row.get("oc_document_id") is not None else None
        rel_id = (
            int(row["related_document_id"]) if row.get("related_document_id") is not None else None
        )
        key = None
        if oc_id is not None and oc_id in stop_by_oc:
            key = stop_by_oc[oc_id]
        elif row.get("client_id") is not None:
            key = _customer_key(row.get("client_id"), row.get("client_name"))
            if key not in stop_by_key and key not in clients:
                # Picking sin client_id en mapa: intentar match por nombre normalizado.
                n = _norm_name(row.get("client_name"))
                for sk, st in stop_by_key.items():
                    if _norm_name(st.get("customer_name")) == n:
                        key = sk
                        break
        else:
            n = _norm_name(row.get("client_name"))
            for sk, st in stop_by_key.items():
                if _norm_name(st.get("customer_name")) == n:
                    key = sk
                    break
            if key is None:
                key = _customer_key(None, row.get("client_name"))

        if key not in clients:
            clients[key] = {
                "customer_key": key,
                "client_id": row.get("client_id"),
                "client_name": row.get("client_name") or "Sin nombre",
                "fantasy_name": row.get("fantasy_name") or "",
                "address": row.get("address") or "",
                "city": row.get("city") or "",
                "phone": row.get("phone") or "",
                "observations": row.get("observations") or "",
                "latitude": _num(row.get("lat")),
                "longitude": _num(row.get("lng")),
                "has_coordinates": _num(row.get("lat")) is not None
                and _num(row.get("lng")) is not None,
                "coordinates_source": "dispatch_plan_picking_clients"
                if _num(row.get("lat")) is not None
                else None,
                "route_order": row.get("route_order"),
                "documents": [],
                "load_groups": [],
                "_group_ids": set(),
                "_oc_ids": set(),
                "_folios": [],
                "units_total": 0.0,
                "document_count": 0,
                "facade_photo": None,
                "search_text": "",
            }

        c = clients[key]
        if not c.get("phone") and row.get("phone"):
            c["phone"] = row.get("phone") or ""
        if not c.get("address") and row.get("address"):
            c["address"] = row.get("address") or ""
        if not c.get("city") and row.get("city"):
            c["city"] = row.get("city") or ""
        if not c.get("observations") and row.get("observations"):
            c["observations"] = row.get("observations") or ""
        if c.get("client_id") is None and row.get("client_id") is not None:
            c["client_id"] = row.get("client_id")
        ro = row.get("route_order")
        if ro is not None and (c.get("route_order") is None or int(ro) < int(c["route_order"])):
            c["route_order"] = int(ro)

        grp = assign_by_rel.get(rel_id) if rel_id is not None else None
        load_group = None
        if grp and grp.get("load_batch_id") is not None:
            load_group = {
                "load_batch_id": grp["load_batch_id"],
                "name": grp.get("load_batch_name"),
                "sort_order": grp.get("load_batch_sort_order"),
            }
            gid = int(grp["load_batch_id"])
            if gid not in c["_group_ids"]:
                c["_group_ids"].add(gid)
                c["load_groups"].append(load_group)

        units = units_by_rel.get(rel_id, 0.0) if rel_id is not None else 0.0
        oc_number = oc_numbers.get(oc_id) if oc_id is not None else None
        doc = {
            "related_document_id": rel_id,
            "document_number": row.get("document_number"),
            "document_type": row.get("document_type") or "",
            "document_total": _num(row.get("document_total")),
            "oc_document_id": oc_id,
            "oc_number": oc_number,
            "payment_method": row.get("payment_method") or "",
            "units": units,
            "load_group": load_group,
            "relation_source": row.get("relation_source"),
        }
        # Evitar duplicar el mismo folio/related_document_id
        existing_rels = {
            int(d["related_document_id"])
            for d in c["documents"]
            if d.get("related_document_id") is not None
        }
        if rel_id is not None and rel_id in existing_rels:
            continue
        c["documents"].append(doc)
        c["document_count"] = len(c["documents"])
        c["units_total"] = round(float(c["units_total"]) + float(units or 0), 3)
        if oc_id is not None:
            c["_oc_ids"].add(oc_id)
        if row.get("document_number") is not None:
            c["_folios"].append(str(row.get("document_number")))

    # Clientes solo en mapa (sin picking aún): cards con OCs, sin facturas.
    for key, c in clients.items():
        stop = stop_by_key.get(key)
        if stop and not c["documents"]:
            for o in stop.get("orders") or []:
                oc_id = o.get("oc_document_id")
                if oc_id is not None:
                    c["_oc_ids"].add(int(oc_id))
            c["units_total"] = round(float(stop.get("units_count") or 0), 3)
        if stop and c.get("route_order") is None:
            orders = stop.get("orders") or []
            ros = [o.get("route_order") for o in orders if o.get("route_order") is not None]
            if ros:
                c["route_order"] = min(int(x) for x in ros)

        c["load_groups"].sort(
            key=lambda g: (
                int(g.get("sort_order") or 0),
                int(g.get("load_batch_id") or 0),
            )
        )
        c["documents"].sort(
            key=lambda d: (
                int(d.get("document_number") or 0),
                int(d.get("related_document_id") or 0),
            )
        )
        c["search_text"] = " ".join(
            filter(
                None,
                [
                    c.get("client_name"),
                    c.get("fantasy_name"),
                    c.get("address"),
                    c.get("city"),
                    c.get("phone"),
                    " ".join(c.get("_folios") or []),
                    " ".join(str(x) for x in (c.get("_oc_ids") or [])),
                    " ".join(g.get("name") or "" for g in c.get("load_groups") or []),
                ],
            )
        ).lower()
        c.pop("_group_ids", None)
        c.pop("_oc_ids", None)
        c.pop("_folios", None)

    ordered = sorted(
        clients.values(),
        key=lambda x: (
            x.get("route_order") is None,
            int(x.get("route_order") or 10_000),
            str(x.get("client_name") or ""),
        ),
    )
    return {
        "dispatch_plan_id": int(plan_id),
        "picking_ready": bool(picking.get("ready")),
        "picking_reason": picking.get("reason"),
        "picking_id": picking.get("picking_id"),
        "picking_version": picking.get("version"),
        "summary": {
            "clients": len(ordered),
            "documents": sum(int(c.get("document_count") or 0) for c in ordered),
            "with_coordinates": sum(1 for c in ordered if c.get("has_coordinates")),
            "without_coordinates": sum(1 for c in ordered if not c.get("has_coordinates")),
            "map_orders": (delivery.get("summary") or {}).get("orders"),
        },
        "clients": ordered,
        "future_contracts": FUTURE_CONTRACTS,
        "read_only": True,
        "sources": {
            "clients": "distribuidora.dispatch_plan_orders (delivery_map grouping)",
            "documents": "distribuidora.dispatch_plan_picking_clients (current picking)",
            "load_groups": "distribuidora.dispatch_plan_document_load_assignments",
            "units": "distribuidora.document_details.quantity (invoice/boleta)",
            "coordinates": "order → rutero → clients (verifiable only)",
        },
    }


def list_clients_for_driver(plan_id: int, *, q: str | None = None) -> dict[str, Any]:
    bundle = _build_client_index(int(plan_id))
    items = bundle["clients"]
    query = (q or "").strip().lower()
    if query:
        items = [c for c in items if query in (c.get("search_text") or "")]
    cards = [
        {
            "customer_key": c["customer_key"],
            "client_id": c.get("client_id"),
            "client_name": c.get("client_name"),
            "fantasy_name": c.get("fantasy_name") or "",
            "city": c.get("city") or "",
            "address": c.get("address") or "",
            "document_count": c.get("document_count") or 0,
            "units_total": c.get("units_total") or 0,
            "load_groups": [
                {"load_batch_id": g.get("load_batch_id"), "name": g.get("name")}
                for g in (c.get("load_groups") or [])
            ],
            "has_coordinates": bool(c.get("has_coordinates")),
            "route_order": c.get("route_order"),
        }
        for c in items
    ]
    return serialize_value(
        {
            "dispatch_plan_id": int(plan_id),
            "items": cards,
            "count": len(cards),
            "summary": bundle["summary"],
            "picking_ready": bundle["picking_ready"],
            "picking_reason": bundle.get("picking_reason"),
            "sources": bundle["sources"],
            "future_contracts": FUTURE_CONTRACTS,
            "read_only": True,
        }
    )


def get_client_ficha_for_driver(plan_id: int, customer_key: str) -> dict[str, Any]:
    key = (customer_key or "").strip()
    if not key:
        raise HTTPException(status_code=400, detail="customer_key requerido")
    bundle = _build_client_index(int(plan_id))
    client = next((c for c in bundle["clients"] if c["customer_key"] == key), None)
    if client is None:
        raise HTTPException(status_code=404, detail="Cliente no pertenece a este despacho")

    documents_out: list[dict[str, Any]] = []
    for doc in client.get("documents") or []:
        rel = doc.get("related_document_id")
        products_raw: list[dict[str, Any]] = []
        if rel is not None:
            products_raw = plan_svc.compute_picking_products_for_related_documents(
                int(plan_id), [int(rel)]
            )
        products = [
            _product_payload(p, load_group=doc.get("load_group")) for p in products_raw
        ]
        documents_out.append(
            {
                **doc,
                "products": products,
                "product_count": len(products),
            }
        )

    navigation = None
    if client.get("has_coordinates") and client.get("latitude") is not None:
        lat = float(client["latitude"])
        lng = float(client["longitude"])
        navigation = {
            "available": True,
            "latitude": lat,
            "longitude": lng,
            "google_maps_url": f"https://www.google.com/maps/dir/?api=1&destination={lat},{lng}",
            "geo_uri": f"geo:{lat},{lng}",
        }
    else:
        navigation = {"available": False}

    return serialize_value(
        {
            "dispatch_plan_id": int(plan_id),
            "customer_key": key,
            "client": {
                "customer_key": key,
                "client_id": client.get("client_id"),
                "client_name": client.get("client_name"),
                "fantasy_name": client.get("fantasy_name") or "",
                "address": client.get("address") or "",
                "city": client.get("city") or "",
                "phone": client.get("phone") or "",
                "observations": client.get("observations") or "",
                "latitude": client.get("latitude"),
                "longitude": client.get("longitude"),
                "has_coordinates": bool(client.get("has_coordinates")),
                "coordinates_source": client.get("coordinates_source"),
                "route_order": client.get("route_order"),
                "load_groups": client.get("load_groups") or [],
                "facade_photo": None,
                "facade_photo_placeholder": True,
            },
            "navigation": navigation,
            "documents": documents_out,
            "document_count": len(documents_out),
            "units_total": client.get("units_total") or 0,
            "future_contracts": FUTURE_CONTRACTS,
            "read_only": True,
            "sources": bundle["sources"],
        }
    )


def get_map_for_driver(
    plan_id: int,
    *,
    origin_lat: float | None = None,
    origin_lng: float | None = None,
) -> dict[str, Any]:
    bundle = _build_client_index(int(plan_id))
    markers = []
    for c in bundle["clients"]:
        if not c.get("has_coordinates"):
            continue
        lat = float(c["latitude"])
        lng = float(c["longitude"])
        dist = None
        if origin_lat is not None and origin_lng is not None:
            dist = round(_haversine_m(origin_lat, origin_lng, lat, lng), 1)
        markers.append(
            {
                "customer_key": c["customer_key"],
                "client_id": c.get("client_id"),
                "client_name": c.get("client_name"),
                "address": c.get("address") or "",
                "city": c.get("city") or "",
                "latitude": lat,
                "longitude": lng,
                "coordinates_source": c.get("coordinates_source"),
                "document_count": c.get("document_count") or 0,
                "units_total": c.get("units_total") or 0,
                "load_groups": [
                    {"load_batch_id": g.get("load_batch_id"), "name": g.get("name")}
                    for g in (c.get("load_groups") or [])
                ],
                "distance_m": dist,
                "navigation": {
                    "google_maps_url": (
                        f"https://www.google.com/maps/dir/?api=1&destination={lat},{lng}"
                    ),
                    "geo_uri": f"geo:{lat},{lng}",
                },
            }
        )
    if origin_lat is not None and origin_lng is not None:
        markers.sort(key=lambda m: (m.get("distance_m") is None, m.get("distance_m") or 0))
    else:
        markers.sort(key=lambda m: str(m.get("client_name") or ""))

    return serialize_value(
        {
            "dispatch_plan_id": int(plan_id),
            "markers": markers,
            "count": len(markers),
            "summary": {
                "clients": bundle["summary"]["clients"],
                "with_coordinates": len(markers),
                "without_coordinates": bundle["summary"]["without_coordinates"],
            },
            "device_location": {
                "requested": origin_lat is not None and origin_lng is not None,
                "latitude": origin_lat,
                "longitude": origin_lng,
                "tracking_persistent": False,
                "note_es": (
                    "Ubicación solo en memoria del dispositivo para proximidad. "
                    "P2 no guarda recorridos GPS."
                ),
            },
            "empty_state": None
            if markers
            else {
                "code": "no_georeferenced_clients",
                "message_es": (
                    "No hay clientes con coordenadas verificables en este despacho. "
                    "Continúa por la lista de clientes."
                ),
            },
            "future_contracts": FUTURE_CONTRACTS,
            "read_only": True,
            "sources": bundle["sources"],
        }
    )


def get_future_contracts() -> dict[str, Any]:
    return serialize_value({"contracts": FUTURE_CONTRACTS, "phase": "P2"})
