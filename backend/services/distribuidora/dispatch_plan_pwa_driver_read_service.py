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
        "status": "implemented_pending_r2",
        "phase": "P3",
        "storage": "cloudflare_r2_private",
        "note_es": (
            "Fotografía de fachada: metadatos en PG + bytes en R2 privado. "
            "Requiere R2_* en Coolify y migración 055."
        ),
    },
    "gps_correction_proposal": {
        "status": "implemented",
        "phase": "P3",
        "note_es": (
            "Propuesta GPS del chofer: no escribe bsale.clients; "
            "aprobación ERP publica client_verified_locations."
        ),
    },
    "admin_validation": {
        "status": "implemented",
        "phase": "P3",
        "note_es": "Cola ERP: ubicaciones de clientes pendientes.",
    },
    "delivery_evidence_photo": {
        "status": "not_implemented",
        "phase": "P4+",
        "storage": "cloudflare_r2_private",
        "note_es": "Evidencia de entrega: fuera de P3.",
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


def validate_origin_coords(
    origin_lat: float | None,
    origin_lng: float | None,
) -> tuple[float | None, float | None]:
    """Valida origen GPS opcional. No altera autorización del plan.

    - Ambos omitidos → (None, None)
    - Solo uno → 400
    - NaN / infinito / fuera de rango → 400
    """
    if origin_lat is None and origin_lng is None:
        return None, None
    if origin_lat is None or origin_lng is None:
        raise HTTPException(
            status_code=400,
            detail="origin_lat y origin_lng deben enviarse juntos",
        )
    try:
        lat = float(origin_lat)
        lng = float(origin_lng)
    except (TypeError, ValueError) as exc:
        raise HTTPException(
            status_code=400,
            detail="origin_lat y origin_lng deben ser números finitos",
        ) from exc
    if not math.isfinite(lat) or not math.isfinite(lng):
        raise HTTPException(
            status_code=400,
            detail="origin_lat y origin_lng deben ser números finitos (no NaN ni infinito)",
        )
    if not (-90.0 <= lat <= 90.0):
        raise HTTPException(
            status_code=400,
            detail="origin_lat debe estar entre -90 y 90",
        )
    if not (-180.0 <= lng <= 180.0):
        raise HTTPException(
            status_code=400,
            detail="origin_lng debe estar entre -180 y 180",
        )
    return lat, lng


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


# Solo destinos con evidencia confiable de falta de ruta terrestre continua.
# NO usar latitud sola ni Quellón/Castro/Ancud (hay conexión vial).
_MELINKA_CITY_HINTS = (
    "melinka",
    "repollal",
)


def likely_island_destination(city: str | None, lat: float | None = None) -> bool:
    """True solo si la ciudad identifica Melinka/Repollal.

    ``lat`` se ignora a propósito: coords australes fuera de Melinka no deben
    disparar advertencia de isla (p. ej. Quellón/Castro en un plan mixto).
    """
    _ = lat
    c = (city or "").strip().lower()
    if not c:
        return False
    return any(h in c for h in _MELINKA_CITY_HINTS)


def build_client_navigation(
    lat: float,
    lng: float,
    *,
    city: str | None = None,
    client_name: str | None = None,
) -> dict[str, Any]:
    """Enlaces Google Maps con destino = coords del cliente (nunca el GPS del chofer)."""
    if not math.isfinite(lat) or not math.isfinite(lng):
        raise ValueError("Coordenadas de destino inválidas")
    if not (-90.0 <= lat <= 90.0 and -180.0 <= lng <= 180.0):
        raise ValueError("Coordenadas de destino fuera de rango")
    query = f"{lat},{lng}"
    # Codificar coma como %2C para Maps URLs api=1
    encoded = query.replace(",", "%2C")
    place = f"https://www.google.com/maps/search/?api=1&query={encoded}"
    directions = (
        f"https://www.google.com/maps/dir/?api=1&destination={encoded}&travelmode=driving"
    )
    melinka = likely_island_destination(city, lat)
    city_known = bool((city or "").strip())
    # Sin ciudad: marcador exacto sin afirmar isla. Melinka: marcador + aviso.
    # Quellón/Castro/Ancud y resto continental: directions.
    prefer_exact = melinka or not city_known
    return {
        "available": True,
        "destination_latitude": lat,
        "destination_longitude": lng,
        "destination_label": client_name or None,
        "google_maps_place_url": place,
        "google_maps_directions_url": directions,
        "google_maps_url": place if prefer_exact else directions,
        "likely_no_road_route": melinka,
        "warning_es": (
            "Destino Melinka (archipiélago de las Guaitecas): se muestra la ubicación "
            "exacta; la ruta vehicular de Google puede detenerse en el continente."
            if melinka
            else None
        ),
        "note_es": (
            "El destino son las coordenadas del cliente del despacho. "
            "No se usa tu GPS como destino."
        ),
        "geo_uri": f"geo:{lat},{lng}",
    }


def _short_product_name(row: dict[str, Any]) -> str:
    raw = _commercial_product_name(row)
    text = re.sub(r"\s+", " ", str(raw)).strip()
    if len(text) <= 72:
        return text
    return text[:69].rstrip() + "…"


def _clean_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _commercial_product_name(row: dict[str, Any]) -> str:
    """Nombre comercial: producto Bsale + variante/formato. Sin inventar datos."""
    product = _clean_text(
        row.get("producto_base")
        or row.get("product_name")
        or row.get("producto_marca")
        or ""
    )
    variant = _clean_text(
        row.get("producto_variante")
        or row.get("display_name")
        or row.get("variante")
        or ""
    )
    # Evitar duplicar si la variante ya incluye el nombre del producto.
    if product and variant:
        if product.lower() in variant.lower():
            return variant
        return f"{product} — {variant}"
    return product or variant or _clean_text(row.get("producto")) or "Producto"


def _product_payload(row: dict[str, Any], *, load_group: dict[str, Any] | None) -> dict[str, Any]:
    """Producto para ficha. Cajas solo con units_per_box estructurado (no parsear SEC del nombre)."""
    sin_caja = bool(row.get("sin_unidad_caja"))
    upb = row.get("units_per_box_efectivo")
    if upb is None:
        upb = row.get("units_per_box")
    try:
        upb_f = float(upb) if upb is not None else 0.0
    except (TypeError, ValueError):
        upb_f = 0.0
    cajas = row.get("cajas_efectivas")
    if cajas is None:
        cajas = row.get("cajas")
    cajas_out = None
    if not sin_caja and upb_f > 0 and cajas is not None:
        try:
            cajas_val = float(cajas)
            if cajas_val > 0:
                cajas_out = cajas_val
        except (TypeError, ValueError):
            cajas_out = None
    # Si hay factor estructurado válido, mostrar cajas aunque el picking no traiga
    # ``cajas`` precalculado (líneas de document_details).
    units_num = _num(row.get("unidades"))
    if cajas_out is None and not sin_caja and upb_f > 0 and units_num is not None:
        try:
            cajas_out = round(float(units_num) / upb_f, 2)
        except (TypeError, ValueError, ZeroDivisionError):
            cajas_out = None
    verified = upb_f > 0 and not sin_caja
    commercial = _commercial_product_name(row)
    product_name = _clean_text(
        row.get("producto_base") or row.get("product_name") or row.get("producto") or ""
    ) or None
    variant_name = _clean_text(
        row.get("producto_variante") or row.get("variante") or row.get("display_name") or ""
    ) or None
    category_name = _clean_text(row.get("category_name") or row.get("product_type") or "") or None
    description = variant_name or product_name or commercial or ""
    source_detail_id = row.get("source_detail_id")
    if source_detail_id is None:
        source_detail_id = row.get("detail_id")
    barcode = row.get("codigo_barras")
    if barcode is not None:
        barcode = str(barcode).strip() or None
    return {
        "description": description,
        "commercial_name": commercial,
        "name_short": _short_product_name(row),
        "category_name": category_name,
        "product_name": product_name,
        "variant_name": variant_name,
        "producto": product_name or "",
        "variante": variant_name or "",
        "product_id": row.get("product_id"),
        "variant_id": row.get("variant_id"),
        "sku_code": row.get("variant_id"),
        "codigo_barras": barcode,
        "barcode": barcode,
        "unidades": _num(row.get("unidades")),
        "cajas_equivalentes": cajas_out,
        "units_per_box": upb_f if upb_f > 0 and not sin_caja else None,
        "sin_unidad_caja": sin_caja or upb_f <= 0,
        "equivalence_status": "verified" if verified else "pending_validation",
        "equivalence_label": None if verified else "Equivalencia por validar",
        "load_group": load_group,
        "source_detail_id": int(source_detail_id) if source_detail_id is not None else None,
        "line_number": (
            int(row["line_number"]) if row.get("line_number") is not None else None
        ),
    }


def _document_detail_lines(related_document_id: int) -> list[dict[str, Any]]:
    """Una fila por detail_id (no agrupa SKUs repetidos). Sin mutar picking."""
    from backend.utils.picking_sql import (
        BARCODE_EXPR,
        BSALE_PRODUCT_JOIN,
        VARIANTS_JOIN,
    )

    conn = get_connection()
    try:
        cur = conn.cursor()
        # Joins canónicos Bsale (bsale_id + company_id). Sin v.id/p.id (no existen).
        try:
            cur.execute(
                f"""
                SELECT
                    dd.detail_id AS source_detail_id,
                    dd.line_number,
                    dd.variant_id,
                    p.bsale_id AS product_id,
                    COALESCE(
                        NULLIF(BTRIM(p.name), ''),
                        NULLIF(BTRIM(dd.variant_description), ''),
                        NULLIF(BTRIM(dd.variant_code), ''),
                        'Producto'
                    ) AS producto,
                    NULLIF(BTRIM(p.name), '') AS producto_base,
                    NULLIF(BTRIM(p.name), '') AS product_name,
                    NULLIF(BTRIM(pt.name), '') AS category_name,
                    NULLIF(BTRIM(pt.name), '') AS product_type,
                    COALESCE(
                        NULLIF(BTRIM(dd.variant_description), ''),
                        NULLIF(BTRIM(v.description), ''),
                        NULLIF(BTRIM(dd.variant_code), '')
                    ) AS producto_variante,
                    NULLIF(BTRIM(dd.variant_code), '') AS variante,
                    ({BARCODE_EXPR}) AS codigo_barras,
                    dd.quantity AS unidades,
                    v.units_per_box AS units_per_box,
                    CASE
                        WHEN v.units_per_box IS NOT NULL AND v.units_per_box > 0
                        THEN ROUND(dd.quantity / v.units_per_box::numeric, 2)
                        ELSE NULL
                    END AS cajas,
                    CASE
                        WHEN v.units_per_box IS NULL OR v.units_per_box <= 0
                        THEN TRUE
                        ELSE FALSE
                    END AS sin_unidad_caja
                FROM distribuidora.document_details dd
                {VARIANTS_JOIN}
                {BSALE_PRODUCT_JOIN}
                WHERE dd.document_id = %s
                ORDER BY dd.line_number NULLS LAST, dd.detail_id
                """,
                (int(related_document_id),),
            )
        except Exception:
            conn.rollback()
            cur.execute(
                """
                SELECT
                    dd.detail_id AS source_detail_id,
                    dd.line_number,
                    dd.variant_id,
                    COALESCE(
                        NULLIF(BTRIM(dd.variant_description), ''),
                        NULLIF(BTRIM(dd.variant_code), ''),
                        'Producto'
                    ) AS producto,
                    NULLIF(BTRIM(dd.variant_description), '') AS producto_variante,
                    NULLIF(BTRIM(dd.variant_code), '') AS variante,
                    NULLIF(BTRIM(dd.variant_code), '') AS codigo_barras,
                    dd.quantity AS unidades
                FROM distribuidora.document_details dd
                WHERE dd.document_id = %s
                ORDER BY dd.line_number NULLS LAST, dd.detail_id
                """,
                (int(related_document_id),),
            )
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        cur.close()
        return rows
    except Exception:
        return []
    finally:
        conn.close()


def _client_commercial_by_ids(client_ids: list[int]) -> dict[int, dict[str, Any]]:
    """RUT comercial y vendedor maestro desde bsale.clients (company_id=3)."""
    ids = sorted({int(x) for x in client_ids if x is not None})
    if not ids:
        return {}
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT bsale_id, code, rut_clean, vendedor
            FROM bsale.clients
            WHERE company_id = 3
              AND bsale_id = ANY(%s)
            """,
            (ids,),
        )
        out: dict[int, dict[str, Any]] = {}
        for bsale_id, code, rut_clean, vendedor in cur.fetchall():
            rut = (rut_clean or "").strip() or (code or "").strip() or None
            out[int(bsale_id)] = {
                "rut": rut,
                "vendedor_master": (vendedor or "").strip() or None,
            }
        cur.close()
        return out
    finally:
        conn.close()


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
                "delivery_notes": "",
                "rut": None,
                "sellers": [],
                "_sellers": set(),
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
                "observations": (row.get("observations") or row.get("delivery_notes") or ""),
                "delivery_notes": row.get("delivery_notes") or "",
                "rut": None,
                "sellers": [],
                "_sellers": set(),
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
        row_obs = (row.get("observations") or row.get("delivery_notes") or "").strip()
        if row_obs and not (c.get("observations") or "").strip():
            c["observations"] = row_obs
        if not (c.get("delivery_notes") or "").strip() and row.get("delivery_notes"):
            c["delivery_notes"] = row.get("delivery_notes") or ""
        if c.get("client_id") is None and row.get("client_id") is not None:
            c["client_id"] = row.get("client_id")
        seller = (row.get("seller_name") or "").strip()
        if seller:
            c.setdefault("_sellers", set()).add(seller)
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
            "seller_name": seller or None,
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

    master = _client_commercial_by_ids(
        [int(c["client_id"]) for c in clients.values() if c.get("client_id") is not None]
    )

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

        cid = c.get("client_id")
        if cid is not None and int(cid) in master:
            m = master[int(cid)]
            c["rut"] = m.get("rut")
            # Vendedor del documento/OC prevalece; maestro solo si no hay sellers de picking.
            if not c.get("_sellers") and m.get("vendedor_master"):
                c.setdefault("_sellers", set()).add(m["vendedor_master"])

        sellers = sorted(c.pop("_sellers", set()) or [])
        c["sellers"] = sellers
        c["seller_name"] = ", ".join(sellers) if sellers else None

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
        # Monto cliente: Σ document_total (c/IVA) una vez por related_document_id.
        amount = 0.0
        amount_known = False
        seen_docs: set[int] = set()
        for d in c.get("documents") or []:
            rel = d.get("related_document_id")
            if rel is not None:
                rid = int(rel)
                if rid in seen_docs:
                    continue
                seen_docs.add(rid)
            tot = d.get("document_total")
            if tot is None:
                continue
            try:
                amount += float(tot)
                amount_known = True
            except (TypeError, ValueError):
                continue
        c["amount_total"] = round(amount, 2) if amount_known else None
        # Observación operativa: observations; si viene vacía, delivery_notes del picking.
        obs = (c.get("observations") or "").strip()
        if not obs:
            obs = (c.get("delivery_notes") or "").strip()
        c["observations"] = obs
        c["search_text"] = " ".join(
            filter(
                None,
                [
                    c.get("client_name"),
                    c.get("fantasy_name"),
                    c.get("address"),
                    c.get("city"),
                    c.get("phone"),
                    c.get("rut"),
                    c.get("seller_name"),
                    obs,
                    " ".join(c.get("_folios") or []),
                    " ".join(str(x) for x in (c.get("_oc_ids") or [])),
                    " ".join(g.get("name") or "" for g in c.get("load_groups") or []),
                ],
            )
        ).lower()
        c.pop("_group_ids", None)
        c.pop("_oc_ids", None)
        c.pop("_folios", None)
        c.pop("delivery_notes", None)

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
    attention = _visit_attention_by_customer(int(plan_id))
    cards = []
    for c in items:
        att = attention.get(str(c["customer_key"])) or {
            "visit_id": None,
            "visit_status": None,
            "attention_status": "pending",
            "attention_label_es": "Pendiente",
            "admin_review_pending": False,
        }
        cards.append(
            {
                "customer_key": c["customer_key"],
                "client_id": c.get("client_id"),
                "client_name": c.get("client_name"),
                "fantasy_name": c.get("fantasy_name") or "",
                "city": c.get("city") or "",
                "address": c.get("address") or "",
                "observations": c.get("observations") or "",
                "document_count": c.get("document_count") or 0,
                "units_total": c.get("units_total") or 0,
                "amount_total": c.get("amount_total"),
                "load_groups": [
                    {"load_batch_id": g.get("load_batch_id"), "name": g.get("name")}
                    for g in (c.get("load_groups") or [])
                ],
                "has_coordinates": bool(c.get("has_coordinates")),
                "route_order": c.get("route_order"),
                "visit_id": att.get("visit_id"),
                "visit_status": att.get("visit_status"),
                "result_reason": att.get("result_reason"),
                "attention_status": att.get("attention_status") or "pending",
                "attention_label_es": att.get("attention_label_es") or "Pendiente",
                "semaphore_color": att.get("semaphore_color") or "neutral",
                "semaphore_code": att.get("semaphore_code") or "pending",
                "closure_review_status": att.get("closure_review_status"),
                "admin_review_pending": bool(att.get("admin_review_pending")),
            }
        )
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

    # Conflictos OC↔grupo: informar sin autocorregir.
    oc_groups: dict[int, set[int]] = {}
    for d0 in client.get("documents") or []:
        oc0 = d0.get("oc_document_id")
        lg0 = d0.get("load_group") or {}
        bid0 = lg0.get("load_batch_id")
        if oc0 is None or bid0 is None:
            continue
        oc_groups.setdefault(int(oc0), set()).add(int(bid0))

    documents_out: list[dict[str, Any]] = []
    for doc in client.get("documents") or []:
        rel = doc.get("related_document_id")
        products_raw: list[dict[str, Any]] = []
        line_identity = "empty"
        quantities_registrable = False
        if rel is not None:
            # Entrega: líneas exactas (detail_id). Nunca fabricar IDs.
            products_raw = _document_detail_lines(int(rel))
            if products_raw:
                line_identity = "reliable"
                quantities_registrable = True
            else:
                # Solo lectura: consolidado picking SIN source_detail_id.
                products_raw = plan_svc.compute_picking_products_for_related_documents(
                    int(plan_id), [int(rel)]
                )
                if products_raw:
                    line_identity = "ambiguous_display_only"
                    quantities_registrable = False
        doc_group = doc.get("load_group")
        # No repetir grupo en cada línea si todo el documento comparte el mismo.
        products = [_product_payload(p, load_group=None) for p in products_raw]
        if line_identity == "ambiguous_display_only":
            # Garantizar que el fallback no invente identidad de línea.
            for p in products:
                p["source_detail_id"] = None
                p["line_number"] = None
        conflict = None
        oc_id = doc.get("oc_document_id")
        if oc_id is not None and int(oc_id) in oc_groups and len(oc_groups[int(oc_id)]) > 1:
            conflict = {
                "code": "oc_load_group_mismatch",
                "message_es": (
                    "La OC vinculada aparece en más de un grupo de carga. "
                    "Revisar en ERP; no se corrige automáticamente."
                ),
                "oc_document_id": int(oc_id),
                "load_batch_ids": sorted(oc_groups[int(oc_id)]),
            }
        documents_out.append(
            {
                **doc,
                "load_group": doc_group,
                "products": products,
                "product_count": len(products),
                "line_identity": line_identity,
                "quantities_registrable": quantities_registrable,
                "group_assignment_conflict": conflict,
            }
        )

    from backend.services.distribuidora import client_location_service as loc_svc

    client_payload = {
        "customer_key": key,
        "client_id": client.get("client_id"),
        "client_name": client.get("client_name"),
        "fantasy_name": client.get("fantasy_name") or "",
        "rut": client.get("rut"),
        "seller_name": client.get("seller_name"),
        "sellers": client.get("sellers") or [],
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
    }
    try:
        client_payload = loc_svc.enrich_client_location_fields(client_payload)
    except Exception:
        # Ficha no debe caerse si 055/R2 aún no están listos.
        client_payload.setdefault(
            "location_registration",
            {"status": "unknown", "label_es": "Estado no disponible", "can_register": False},
        )

    navigation: dict[str, Any]
    if client_payload.get("has_coordinates") and client_payload.get("latitude") is not None:
        lat = float(client_payload["latitude"])
        lng = float(client_payload["longitude"])
        navigation = build_client_navigation(
            lat,
            lng,
            city=client_payload.get("city"),
            client_name=client_payload.get("client_name"),
        )
    else:
        navigation = {"available": False}

    try:
        p3_status = loc_svc.schema_status()
    except Exception:
        p3_status = {
            "tables_ready": False,
            "r2": {"configured": False},
            "note_es": "Estado P3 no disponible",
        }

    from backend.services.distribuidora import delivery_visit_service as visit_svc

    try:
        p4_status = visit_svc.schema_status()
    except Exception:
        p4_status = {
            "tables_ready": False,
            "mode": "online_only",
            "note_es": "Estado P4 no disponible",
        }
    try:
        active_visit = visit_svc.get_active_visit_for_client(
            plan_id=int(plan_id), customer_key=key
        )
    except Exception:
        active_visit = None
    try:
        latest_visit = visit_svc.get_latest_visit_for_client(
            plan_id=int(plan_id), customer_key=key
        )
    except Exception:
        latest_visit = active_visit
    try:
        last_closed_visit = visit_svc.get_last_closed_visit_for_client(
            plan_id=int(plan_id), customer_key=key
        )
    except Exception:
        last_closed_visit = None

    closed_status = str((last_closed_visit or {}).get("status") or "")
    blocking_closed = closed_status in {"completed", "partial", "not_delivered"}
    has_active = bool(
        active_visit and str(active_visit.get("status") or "") == "in_progress"
    )
    # Duplicada abierta tras cierre válido: no retomar; solo resultado histórico.
    admin_review_pending = bool(blocking_closed and has_active)
    can_resume_active = has_active and (not admin_review_pending)
    can_create_new = (not blocking_closed) and (not has_active)
    can_operate = can_resume_active or can_create_new
    note_es = None
    if admin_review_pending:
        note_es = (
            "Existe una visita anterior pendiente de revisión administrativa. "
            "Consulte el resultado de la entrega finalizada."
        )
    elif blocking_closed and not has_active:
        note_es = (
            "Visita ya finalizada en este despacho. "
            "Solo consulta del resultado histórico."
        )

    return serialize_value(
        {
            "dispatch_plan_id": int(plan_id),
            "customer_key": key,
            "client": client_payload,
            "navigation": navigation,
            "p3": p3_status,
            "p4": p4_status,
            "active_visit": active_visit,
            "latest_visit": latest_visit,
            "last_closed_visit": last_closed_visit,
            "documents": documents_out,
            "document_count": len(documents_out),
            "units_total": client.get("units_total") or 0,
            "amount_total": client.get("amount_total"),
            "future_contracts": FUTURE_CONTRACTS,
            "read_only": False,
            "capabilities": {
                "start_delivery_visit": can_operate,
                "resume_active_visit": can_resume_active,
                "create_new_visit_after_close": can_create_new,
                "admin_review_pending": admin_review_pending,
                "operational_visit_id": (
                    int(last_closed_visit["id"])
                    if blocking_closed and last_closed_visit
                    else (
                        int(active_visit["id"])
                        if has_active and active_visit
                        else None
                    )
                ),
                "mode": "online_only",
                "note_es": note_es,
            },
            "sources": {
                **bundle["sources"],
                "rut": "bsale.clients.code / rut_clean (empresa del documento)",
                "seller": "dispatch_plan_picking_clients.seller_name (documento/OC)",
                "boxes": (
                    "units_per_box estructurado (variants/products_master); "
                    "sin inferir SEC del nombre"
                ),
            },
        }
    )


def resolve_visit_attention_rows(
    rows: list[tuple[Any, ...]],
) -> dict[str, dict[str, Any]]:
    """Estado operativo por cliente.

    Filas: (customer_key, id, status, result_reason, ended_at
            [, admin_classification, voided_at, voided_by, void_reason
             [, closure_review_status]])
    Compat: filas antiguas sin result_reason (len<=4 con ended_at en [3]).

    Semáforo (color) es resultado de entrega; closure_review_status es validación
    independiente (nunca convierte rojo→amarillo).
    """
    from backend.services.distribuidora.delivery_semaphore import resolve_semaphore

    valid_close = {"completed", "partial", "not_delivered"}

    def _voided(
        admin_class: Any,
        voided_at: Any,
        voided_by: Any = None,
        void_reason: Any = None,
    ) -> bool:
        if str(admin_class or "") != "voided_admin" or voided_at is None:
            return False
        try:
            actor = int(voided_by) if voided_by is not None else 0
        except (TypeError, ValueError):
            actor = 0
        return actor > 0 and len(str(void_reason or "").strip()) > 0

    def _parse_row(
        row: tuple[Any, ...],
    ) -> tuple[str, int, str, str | None, Any, str | None] | None:
        """Soporta:
        - Canónico: key,id,status,result_reason,ended_at[,admin...][,closure_review]
        - Legacy tests: key,id,status,ended_at[,admin...] (sin result_reason)
        """
        n = len(row)
        # Legacy: [3] es ended_at (datetime/None) y no hay hueco de reason.
        legacy = n == 4 or (
            n >= 5
            and (row[3] is None or hasattr(row[3], "year"))
            and not isinstance(row[3], str)
        )
        if legacy:
            admin_class = row[4] if n > 4 else None
            voided_at = row[5] if n > 5 else None
            voided_by = row[6] if n > 6 else None
            void_reason = row[7] if n > 7 else None
            review = (str(row[8]).strip() if n > 8 and row[8] else None) or None
            if _voided(admin_class, voided_at, voided_by, void_reason):
                return None
            return (
                str(row[0] or ""),
                int(row[1]),
                str(row[2] or ""),
                None,
                row[3] if n > 3 else None,
                review,
            )
        admin_class = row[5] if n > 5 else None
        voided_at = row[6] if n > 6 else None
        voided_by = row[7] if n > 7 else None
        void_reason = row[8] if n > 8 else None
        review = (str(row[9]).strip() if n > 9 and row[9] else None) or None
        if _voided(admin_class, voided_at, voided_by, void_reason):
            return None
        reason = row[3]
        return (
            str(row[0] or ""),
            int(row[1]),
            str(row[2] or ""),
            (str(reason).strip() if reason else None) or None,
            row[4],
            review,
        )

    by_key: dict[str, list[tuple[int, str, str | None, Any, str | None]]] = {}
    for row in rows:
        parsed = _parse_row(tuple(row))
        if not parsed:
            continue
        key, visit_id, status, reason, ended_at, review = parsed
        if not key:
            continue
        by_key.setdefault(key, []).append(
            (visit_id, status, reason, ended_at, review)
        )

    def _pack(
        *,
        visit_id: int | None,
        status: str | None,
        reason: str | None,
        admin_pending: bool,
        open_visit_id: int | None,
        closure_review: str | None = None,
    ) -> dict[str, Any]:
        sem = resolve_semaphore(status=status, result_reason=reason)
        label = sem["label_es"]
        if admin_pending:
            label = f"{label} · revisión admin"
        return {
            "visit_id": visit_id,
            "visit_status": status,
            "result_reason": reason,
            "attention_status": status or "pending",
            "attention_label_es": label,
            "semaphore_color": sem["color"],
            "semaphore_code": sem["code"],
            "closure_review_status": closure_review,
            "admin_review_pending": admin_pending,
            "open_visit_id": open_visit_id,
            "operational_visit_id": visit_id,
        }

    out: dict[str, dict[str, Any]] = {}
    for key, visits in by_key.items():
        visits_sorted = sorted(visits, key=lambda x: x[0])
        closed_valid = [
            v for v in visits_sorted if v[1] in valid_close and v[3] is not None
        ]
        active = [v for v in visits_sorted if v[1] == "in_progress"]
        other_closed = [
            v for v in visits_sorted if v[1] != "in_progress" and v not in closed_valid
        ]

        if closed_valid:
            op = closed_valid[-1]
            out[key] = _pack(
                visit_id=op[0],
                status=op[1],
                reason=op[2],
                admin_pending=bool(active),
                open_visit_id=active[-1][0] if active else None,
                closure_review=op[4],
            )
        elif active:
            op = active[-1]
            out[key] = _pack(
                visit_id=op[0],
                status="in_progress",
                reason=None,
                admin_pending=False,
                open_visit_id=op[0],
                closure_review=None,
            )
        elif other_closed:
            op = other_closed[-1]
            out[key] = _pack(
                visit_id=op[0],
                status=op[1] or None,
                reason=op[2],
                admin_pending=False,
                open_visit_id=None,
                closure_review=op[4],
            )
        else:
            out[key] = _pack(
                visit_id=None,
                status=None,
                reason=None,
                admin_pending=False,
                open_visit_id=None,
                closure_review=None,
            )
    return out


def _visit_attention_by_customer(plan_id: int) -> dict[str, dict[str, Any]]:
    """Una lectura batch: estado operativo por customer_key del plan."""
    from backend.db import get_connection
    from backend.repositories.distribuidora import delivery_visit_repo as visit_repo

    conn = get_connection()
    try:
        cur = conn.cursor()
        if not visit_repo.tables_ready(cur):
            cur.close()
            return {}
        if visit_repo.admin_void_columns_ready(cur):
            cur.execute(
                """
                SELECT customer_key, id, status, result_reason, ended_at,
                       admin_classification, voided_at,
                       voided_by_user_id, void_reason,
                       closure_review_status
                FROM distribuidora.delivery_visits
                WHERE dispatch_plan_id = %s
                ORDER BY customer_key, id
                """,
                (int(plan_id),),
            )
        else:
            cur.execute(
                """
                SELECT customer_key, id, status, result_reason, ended_at,
                       NULL::text, NULL::timestamptz, NULL::int, NULL::text,
                       closure_review_status
                FROM distribuidora.delivery_visits
                WHERE dispatch_plan_id = %s
                ORDER BY customer_key, id
                """,
                (int(plan_id),),
            )
        rows = cur.fetchall()
        cur.close()
        return resolve_visit_attention_rows(list(rows))
    except Exception:
        return {}
    finally:
        try:
            conn.close()
        except Exception:
            pass


def get_map_for_driver(
    plan_id: int,
    *,
    origin_lat: float | None = None,
    origin_lng: float | None = None,
) -> dict[str, Any]:
    bundle = _build_client_index(int(plan_id))
    attention = _visit_attention_by_customer(int(plan_id))
    markers = []
    for c in bundle["clients"]:
        if not c.get("has_coordinates"):
            continue
        lat = float(c["latitude"])
        lng = float(c["longitude"])
        dist = None
        if origin_lat is not None and origin_lng is not None:
            dist = round(_haversine_m(origin_lat, origin_lng, lat, lng), 1)
        att = attention.get(str(c["customer_key"])) or {
            "visit_id": None,
            "visit_status": None,
            "attention_status": "pending",
            "attention_label_es": "Pendiente",
            "admin_review_pending": False,
        }
        markers.append(
            {
                "customer_key": c["customer_key"],
                "client_id": c.get("client_id"),
                "client_name": c.get("client_name"),
                "address": c.get("address") or "",
                "city": c.get("city") or "",
                "observations": c.get("observations") or "",
                "route_order": c.get("route_order"),
                "latitude": lat,
                "longitude": lng,
                "coordinates_source": c.get("coordinates_source"),
                "document_count": c.get("document_count") or 0,
                "units_total": c.get("units_total") or 0,
                "amount_total": c.get("amount_total"),
                "visit_id": att.get("visit_id"),
                "visit_status": att.get("visit_status"),
                "result_reason": att.get("result_reason"),
                "attention_status": att.get("attention_status") or "pending",
                "attention_label_es": att.get("attention_label_es") or "Pendiente",
                "semaphore_color": att.get("semaphore_color") or "neutral",
                "semaphore_code": att.get("semaphore_code") or "pending",
                "closure_review_status": att.get("closure_review_status"),
                "admin_review_pending": bool(att.get("admin_review_pending")),
                "open_visit_id": att.get("open_visit_id"),
                "operational_visit_id": att.get("operational_visit_id"),
                "load_groups": [
                    {"load_batch_id": g.get("load_batch_id"), "name": g.get("name")}
                    for g in (c.get("load_groups") or [])
                ],
                "distance_m": dist,
                "navigation": build_client_navigation(
                    lat,
                    lng,
                    city=c.get("city"),
                    client_name=c.get("client_name"),
                ),
            }
        )
    if origin_lat is not None and origin_lng is not None:
        markers.sort(key=lambda m: (m.get("distance_m") is None, m.get("distance_m") or 0))
    else:
        markers.sort(
            key=lambda m: (
                m.get("route_order") is None,
                int(m.get("route_order") or 10_000),
                str(m.get("client_name") or ""),
            )
        )

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
