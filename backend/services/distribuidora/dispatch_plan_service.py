"""Planificación por camión: confirmación, snapshot, facturación y picking."""

from __future__ import annotations

import json
import re
import unicodedata
from datetime import date, datetime, timezone
from decimal import Decimal
from io import BytesIO
from typing import Any

import pandas as pd

from backend.db import get_connection
from backend.repositories.distribuidora import dispatch_plan_repo as repo

VALID_STATUSES = frozenset(
    {
        "draft",
        "planned",
        "invoicing",
        "ready_for_picking",
        "picking_generated",
        "dispatched",
    }
)


def _enrich_orders_from_purchase(cur, orders: list[dict[str, Any]]) -> list[dict[str, Any]]:
    ids = [int(o["oc_document_id"]) for o in orders if o.get("oc_document_id")]
    if not ids:
        return orders
    cur.execute(
        """
        SELECT
            document_id,
            number,
            client_id,
            forma_pago,
            tipo_documento_a_generar,
            COALESCE(NULLIF(BTRIM(nombre_fantasia), ''), '') AS nombre_fantasia,
            municipality,
            city,
            address,
            seller_name,
            total_amount
        FROM distribuidora.v_orders_purchase
        WHERE document_id = ANY(%s)
        """,
        (ids,),
    )
    cols = [c[0] for c in cur.description]
    by_id = {int(r[0]): dict(zip(cols, r)) for r in cur.fetchall()}
    out: list[dict[str, Any]] = []
    for o in orders:
        row = dict(o)
        src = by_id.get(int(row["oc_document_id"]))
        if src:
            row.setdefault("oc_number", src.get("number"))
            row.setdefault("client_id", src.get("client_id"))
            row.setdefault("client_name", src.get("nombre_fantasia") or row.get("client_name"))
            row.setdefault("payment_method", src.get("forma_pago"))
            row.setdefault("document_type_to_generate", src.get("tipo_documento_a_generar"))
            row.setdefault("city", src.get("city") or src.get("municipality") or row.get("city"))
            row.setdefault("address", src.get("address") or row.get("address"))
            row.setdefault("seller_name", src.get("seller_name") or row.get("seller_name"))
            row.setdefault("oc_total_amount", src.get("total_amount") or row.get("oc_total_amount"))
        out.append(row)
    return out


def _serialize(d: dict[str, Any]) -> dict[str, Any]:
    out = dict(d)
    for k, v in list(out.items()):
        if isinstance(v, Decimal):
            out[k] = float(v)
        elif hasattr(v, "isoformat"):
            out[k] = v.isoformat()
    return out


def _slug_filename_part(text: str) -> str:
    s = unicodedata.normalize("NFKD", text or "")
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r"[^a-zA-Z0-9]+", "_", s).strip("_").lower()
    return s[:48] or "ruta"


def list_session_plans(plan_session_id: str) -> list[dict[str, Any]]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        rows = repo.list_plans_by_session(cur, plan_session_id)
        cur.close()
        return [_serialize(r) for r in rows]
    finally:
        conn.close()


def get_dispatch_plan(plan_id: int) -> dict[str, Any] | None:
    conn = get_connection()
    try:
        cur = conn.cursor()
        plan = repo.get_plan_by_id(cur, plan_id)
        if not plan:
            cur.close()
            return None
        orders = repo.list_plan_orders(cur, plan_id)
        cur.close()
        return {"plan": _serialize(plan), "orders": [_serialize(o) for o in orders]}
    finally:
        conn.close()


def confirm_dispatch_plan(
    *,
    plan_session_id: str,
    truck_id: int,
    route_name: str,
    driver_count: int,
    assistant_count: int,
    driver_cost_clp: int,
    assistant_cost_clp: int,
    diesel_price_per_liter: float,
    km_total: float,
    duration_min: float,
    liters_estimated: float,
    fuel_cost_clp: int,
    ferry_cost_clp: int,
    toll_cost_clp: int,
    extras_cost_clp: int,
    crew_cost_clp: int,
    total_route_cost_clp: int,
    route_geometry: dict[str, Any] | None,
    orders: list[dict[str, Any]],
    planning_date: date | None = None,
) -> dict[str, Any]:
    if not orders:
        raise ValueError("Se requiere al menos una OC para confirmar el plan.")
    pname = (route_name or "").strip() or f"Camión {truck_id}"
    now = datetime.now(timezone.utc)
    pdate = planning_date or date.today()

    conn = get_connection()
    try:
        cur = conn.cursor()
        existing = repo.get_latest_plan_for_truck_session(
            cur, plan_session_id=plan_session_id, truck_id=truck_id
        )
        if existing and existing.get("status") in ("planned", "invoicing", "ready_for_picking", "picking_generated", "dispatched"):
            raise ValueError(
                f"Ya existe un plan confirmado para este camión (id={existing['id']}, "
                f"estado={existing['status']})."
            )

        fields = {
            "plan_session_id": plan_session_id.strip(),
            "planning_date": pdate,
            "truck_id": truck_id,
            "route_name": pname,
            "status": "planned",
            "driver_count": max(0, int(driver_count)),
            "assistant_count": max(0, int(assistant_count)),
            "driver_cost_clp": int(driver_cost_clp),
            "assistant_cost_clp": int(assistant_cost_clp),
            "diesel_price_per_liter": round(float(diesel_price_per_liter), 2),
            "km_total": round(float(km_total), 3),
            "duration_min": round(float(duration_min), 2),
            "liters_estimated": round(float(liters_estimated), 3),
            "fuel_cost_clp": int(fuel_cost_clp),
            "ferry_cost_clp": int(ferry_cost_clp),
            "toll_cost_clp": int(toll_cost_clp),
            "extras_cost_clp": int(extras_cost_clp),
            "crew_cost_clp": int(crew_cost_clp),
            "total_route_cost_clp": int(total_route_cost_clp),
            "route_geometry": json.dumps(route_geometry) if route_geometry else None,
            "confirmed_at": now,
        }
        enriched = _enrich_orders_from_purchase(cur, orders)
        plan_id = repo.insert_dispatch_plan(cur, fields)
        repo.insert_plan_orders(cur, plan_id, enriched)
        conn.commit()
        cur.close()
    finally:
        conn.close()

    return get_dispatch_plan(plan_id) or {"plan": {"id": plan_id}}


def update_dispatch_plan_status(plan_id: int, status: str) -> dict[str, Any]:
    st = status.strip().lower()
    if st not in VALID_STATUSES:
        raise ValueError(f"Estado inválido: {status}")
    conn = get_connection()
    try:
        cur = conn.cursor()
        if not repo.get_plan_by_id(cur, plan_id):
            raise ValueError("Plan no encontrado")
        repo.update_plan_status(cur, plan_id, st)
        conn.commit()
        cur.close()
    finally:
        conn.close()
    data = get_dispatch_plan(plan_id)
    if not data:
        raise ValueError("Plan no encontrado")
    return data


def get_invoiced_documents(plan_id: int) -> dict[str, Any]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        plan = repo.get_plan_by_id(cur, plan_id)
        if not plan:
            raise ValueError("Plan no encontrado")
        rows = repo.list_invoiced_documents(cur, plan_id)
        cur.close()
    finally:
        conn.close()

    items = [_serialize(r) for r in rows]
    summary = {
        "confirmed": sum(1 for x in items if x.get("status") == "confirmed"),
        "probable": sum(1 for x in items if x.get("status") == "probable"),
        "missing": sum(1 for x in items if x.get("status") == "missing"),
        "total": len(items),
    }
    warnings = [
        {
            "oc_document_id": x["oc_document_id"],
            "oc_number": x.get("oc_number"),
            "message": "OC aún sin documento facturado asociado",
        }
        for x in items
        if x.get("status") == "missing"
    ]
    probable_notes = [
        {
            "oc_document_id": x["oc_document_id"],
            "oc_number": x.get("oc_number"),
            "message": "Coincidencia probable — no usar para picking hasta confirmar en Bsale",
            "probable_document_number": x.get("probable_document_number"),
            "probable_score": x.get("probable_score"),
        }
        for x in items
        if x.get("status") == "probable"
    ]
    ready = summary["missing"] == 0 and summary["confirmed"] > 0
    return {
        "dispatch_plan_id": plan_id,
        "items": items,
        "summary": summary,
        "warnings": warnings,
        "probable_notes": probable_notes,
        "ready_for_picking": ready and summary["confirmed"] == summary["total"],
    }


def build_billing_excel_bytes(plan_id: int) -> tuple[bytes, str]:
    data = get_dispatch_plan(plan_id)
    if not data:
        raise ValueError("Plan no encontrado")
    plan = data["plan"]
    orders = data["orders"]
    if plan.get("status") == "draft":
        raise ValueError("El plan debe estar confirmado (planned) para exportar facturación.")

    crew_label = (
        f"{plan.get('driver_count', 1)} chofer / "
        f"{plan.get('assistant_count', 0)} peoneta(s)"
    )
    rows = []
    for o in orders:
        rows.append(
            {
                "orden_ruta": o.get("route_order"),
                "numero_orden": o.get("oc_number") or o.get("oc_document_id"),
                "forma_pago": o.get("payment_method"),
                "tipo_documento_generar": o.get("document_type_to_generate"),
                "cliente": o.get("client_name"),
                "total_oc": o.get("oc_total_amount"),
                "vendedor": o.get("seller_name"),
                "ciudad": o.get("city"),
                "direccion": o.get("address"),
                "camion": plan.get("truck_name") or plan.get("route_name"),
                "tripulacion": crew_label,
            }
        )
    df = pd.DataFrame(rows)
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Facturacion", index=False)
        meta = pd.DataFrame(
            [
                {"campo": "plan_id", "valor": plan_id},
                {"campo": "ruta", "valor": plan.get("route_name")},
                {"campo": "km_total", "valor": plan.get("km_total")},
                {"campo": "costo_total_ruta", "valor": plan.get("total_route_cost_clp")},
            ]
        )
        meta.to_excel(writer, sheet_name="Resumen", index=False)
    buf.seek(0)
    slug = _slug_filename_part(str(plan.get("truck_name") or plan.get("route_name")))
    fname = f"facturacion_{slug}_{date.today().strftime('%Y%m%d')}.xlsx"
    return buf.getvalue(), fname


def _validate_picking_ready(plan_id: int) -> dict[str, Any]:
    inv = get_invoiced_documents(plan_id)
    if inv["summary"]["confirmed"] == 0:
        raise ValueError("No hay documentos facturados confirmados para generar picking.")
    return inv


def get_picking_by_client(plan_id: int, *, validate: bool = True) -> dict[str, Any]:
    inv_check = _validate_picking_ready(plan_id) if validate else get_invoiced_documents(plan_id)

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                dpo.route_order,
                dpo.client_id,
                dpo.client_name,
                dpo.address,
                dpo.city,
                NULLIF(BTRIM(cl.phone), '') AS phone,
                inv.related_document_id,
                inv.related_document_number,
                inv.related_document_type_label,
                d_pay.forma_pago,
                d_pay.tipo_documento_a_generar,
                COALESCE(NULLIF(BTRIM(d.seller_name), ''), dpo.seller_name) AS seller_name,
                d.total_amount AS document_total,
                d.document_type_id
            FROM distribuidora.dispatch_plan_orders dpo
            INNER JOIN distribuidora.v_dispatch_plan_invoiced_documents inv
                ON inv.dispatch_plan_id = dpo.dispatch_plan_id
               AND inv.oc_document_id = dpo.oc_document_id
               AND inv.status = 'confirmed'
            INNER JOIN distribuidora.v_documents_latest d
                ON d.document_id = inv.related_document_id
            LEFT JOIN distribuidora.v_orders_purchase d_pay
                ON d_pay.document_id = dpo.oc_document_id
            LEFT JOIN bsale.clients cl
                ON cl.company_id = 3 AND cl.bsale_id = dpo.client_id
            WHERE dpo.dispatch_plan_id = %s
            ORDER BY dpo.route_order ASC, dpo.oc_document_id ASC
            """,
            (plan_id,),
        )
        cols = [c[0] for c in cur.description]
        stops = [dict(zip(cols, r)) for r in cur.fetchall()]

        lines_by_doc: dict[int, list[dict[str, Any]]] = {}
        for stop in stops:
            doc_id = int(stop["related_document_id"])
            if doc_id in lines_by_doc:
                continue
            cur.execute(
                """
                SELECT
                    dd.line_number,
                    dd.variant_description AS producto,
                    dd.variant_description AS variante,
                    NULLIF(BTRIM(dd.variant_code), '') AS codigo_barras,
                    dd.quantity AS unidades,
                    CASE
                        WHEN pm.units_per_box IS NOT NULL AND pm.units_per_box > 0
                        THEN CEIL(dd.quantity / pm.units_per_box::numeric)
                        ELSE NULL
                    END AS cajas,
                    dd.total_amount AS monto_linea,
                    pm.product_type_name AS tipo_producto
                FROM distribuidora.document_details dd
                LEFT JOIN bsale.products_master pm
                    ON pm.barcode = NULLIF(BTRIM(dd.variant_code), '')
                WHERE dd.document_id = %s
                ORDER BY dd.line_number ASC NULLS LAST, dd.detail_id ASC
                """,
                (doc_id,),
            )
            lcols = [c[0] for c in cur.description]
            lines_by_doc[doc_id] = [dict(zip(lcols, r)) for r in cur.fetchall()]
        cur.close()
    finally:
        conn.close()

    clients = []
    for stop in stops:
        doc_id = int(stop["related_document_id"])
        clients.append(
            {
                **_serialize(stop),
                "lines": [_serialize(x) for x in lines_by_doc.get(doc_id, [])],
            }
        )

    return {
        "dispatch_plan_id": plan_id,
        "clients": clients,
        "validation": inv_check if validate else None,
    }


def get_picking_by_product(plan_id: int, *, validate: bool = True) -> dict[str, Any]:
    if validate:
        _validate_picking_ready(plan_id)

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                COALESCE(pm.product_type_name, 'Sin tipo') AS tipo_producto,
                dd.variant_description AS producto,
                dd.variant_description AS variante,
                NULLIF(BTRIM(dd.variant_code), '') AS codigo_barras,
                SUM(dd.quantity) AS unidades,
                CASE
                    WHEN MAX(pm.units_per_box) IS NOT NULL AND MAX(pm.units_per_box) > 0
                    THEN CEIL(SUM(dd.quantity) / MAX(pm.units_per_box)::numeric)
                    ELSE NULL
                END AS cajas,
                SUM(dd.total_amount) AS total_monto
            FROM distribuidora.dispatch_plan_orders dpo
            INNER JOIN distribuidora.v_dispatch_plan_invoiced_documents inv
                ON inv.dispatch_plan_id = dpo.dispatch_plan_id
               AND inv.oc_document_id = dpo.oc_document_id
               AND inv.status = 'confirmed'
            INNER JOIN distribuidora.document_details dd
                ON dd.document_id = inv.related_document_id
            LEFT JOIN bsale.products_master pm
                ON pm.barcode = NULLIF(BTRIM(dd.variant_code), '')
            WHERE dpo.dispatch_plan_id = %s
            GROUP BY
                COALESCE(pm.product_type_name, 'Sin tipo'),
                dd.variant_description,
                NULLIF(BTRIM(dd.variant_code), '')
            ORDER BY tipo_producto, producto, codigo_barras NULLS LAST
            """,
            (plan_id,),
        )
        cols = [c[0] for c in cur.description]
        items = [_serialize(dict(zip(cols, r))) for r in cur.fetchall()]
        cur.close()
    finally:
        conn.close()

    return {"dispatch_plan_id": plan_id, "items": items}


def mark_picking_generated(plan_id: int) -> dict[str, Any]:
    return update_dispatch_plan_status(plan_id, "picking_generated")
