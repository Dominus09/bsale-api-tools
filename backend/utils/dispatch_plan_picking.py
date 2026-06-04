"""Cabecera, normalización y exportación Excel de picking por plan."""

from __future__ import annotations

from datetime import date
from io import BytesIO
from typing import Any

import pandas as pd

from backend.repositories.distribuidora import dispatch_plan_repo as repo
from backend.db import get_connection


def inv_status_sql_filter(*, include_probable: bool, alias: str = "inv") -> str:
    if include_probable:
        return f"AND {alias}.status IN ('confirmed', 'probable')"
    return f"AND {alias}.status = 'confirmed'"


def _slug_filename_part(s: str) -> str:
    import re

    t = re.sub(r"[^\w\-]+", "_", (s or "plan").strip(), flags=re.UNICODE)
    return t[:48] or "plan"


def fetch_picking_header(plan_id: int) -> dict[str, Any]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        plan = repo.get_plan_by_id(cur, plan_id)
        if not plan:
            raise ValueError("Plan no encontrado")
        cur.execute(
            """
            SELECT STRING_AGG(x.city, ', ' ORDER BY x.min_ord) AS communes
            FROM (
                SELECT NULLIF(BTRIM(o.city), '') AS city,
                       MIN(o.route_order) AS min_ord
                FROM distribuidora.dispatch_plan_orders o
                WHERE o.dispatch_plan_id = %s
                  AND NULLIF(BTRIM(o.city), '') IS NOT NULL
                GROUP BY NULLIF(BTRIM(o.city), '')
            ) x
            """,
            (plan_id,),
        )
        communes_row = cur.fetchone()
        communes = (communes_row[0] if communes_row else None) or ""

        sello = ""
        driver_name = ""
        assistant_names: list[str] = []
        truck_key = (plan.get("truck_name") or plan.get("route_name") or "").strip()
        pdate = plan.get("planning_date")
        if truck_key and pdate:
            cur.execute(
                """
                SELECT general_observation, driver, assistant_1, assistant_2
                FROM distribuidora.route_planning_summary
                WHERE planning_date = %s
                  AND upper(btrim(truck)) = upper(btrim(%s))
                LIMIT 1
                """,
                (pdate, truck_key),
            )
            srow = cur.fetchone()
            if srow:
                sello = (srow[0] or "").strip()
                driver_name = (srow[1] or "").strip()
                for a in (srow[2], srow[3]):
                    name = (a or "").strip()
                    if name:
                        assistant_names.append(name)
        cur.close()
    finally:
        conn.close()

    driver_n = int(plan.get("driver_count") or 1)
    assistant_n = int(plan.get("assistant_count") or 0)
    return {
        "plan_id": plan_id,
        "planning_number": plan.get("planning_code") or f"PLAN-{plan_id}",
        "planning_name": plan.get("planning_name") or plan.get("route_name"),
        "delivery_date": (
            plan["planning_date"].isoformat()
            if hasattr(plan.get("planning_date"), "isoformat")
            else str(plan.get("planning_date") or "")
        ),
        "route_name": plan.get("route_name") or "",
        "communes": communes,
        "truck_name": plan.get("truck_name") or plan.get("route_name") or "",
        "driver_name": driver_name,
        "driver_label": driver_name or f"{driver_n} chofer{'es' if driver_n != 1 else ''}",
        "assistant_label": (
            ", ".join(assistant_names)
            if assistant_names
            else f"{assistant_n} peoneta{'s' if assistant_n != 1 else ''}"
        ),
        "assistant_names": assistant_names,
        "sello": sello,
    }


def normalize_client_stop(raw: dict[str, Any]) -> dict[str, Any]:
    rel = (raw.get("relation_source") or "").strip()
    status = (raw.get("invoicing_status") or raw.get("status") or "").strip()
    score = raw.get("probable_score")
    is_probable = status == "probable" or rel == "probable_match"
    inclusion = (
        "probable"
        if is_probable
        else "auto_match"
        if rel == "auto_match"
        else "bsale"
        if rel == "relateddetailid"
        else "confirmed"
    )
    return {
        "route_order": raw.get("route_order"),
        "oc_document_id": raw.get("oc_document_id"),
        "lat": raw.get("lat"),
        "lng": raw.get("lng"),
        "city": raw.get("city") or "",
        "client_name": raw.get("client_name") or "",
        "fantasy_name": raw.get("fantasy_name") or raw.get("client_name") or "",
        "address": raw.get("address") or "",
        "phone": raw.get("phone") or "",
        "document_number": raw.get("related_document_number"),
        "document_type": raw.get("related_document_type_label") or "",
        "payment_method": raw.get("forma_pago") or "",
        "seller_name": raw.get("seller_name") or "",
        "observations": raw.get("observaciones") or "",
        "document_total": raw.get("document_total"),
        "related_document_id": raw.get("related_document_id"),
        "relation_source": rel or None,
        "inclusion": inclusion,
        "is_probable_included": is_probable,
        "probable_score": score,
    }


def normalize_product_row(raw: dict[str, Any]) -> dict[str, Any]:
    upb = raw.get("units_per_box")
    sin_caja = raw.get("sin_unidad_caja") is True or upb is None or (
        isinstance(upb, (int, float)) and float(upb) <= 0
    )
    cajas = raw.get("cajas")
    if sin_caja:
        cajas_out: int | float | None = 0
    else:
        cajas_out = cajas
    producto = (raw.get("producto") or "").strip()
    variante = (raw.get("variante") or "").strip()
    prod_var = producto if producto == variante else f"{producto} — {variante}".strip(" —")
    return {
        "product_id": raw.get("product_id"),
        "variant_id": raw.get("variant_id"),
        "sucursal_bodega": raw.get("sucursal_bodega") or "Centro de despacho",
        "unidades": raw.get("unidades"),
        "tipo_producto": raw.get("tipo_producto") or "Sin tipo",
        "producto": producto,
        "variante": variante,
        "cajas": cajas_out,
        "sin_unidad_caja": sin_caja,
        "units_per_box": upb,
        "producto_variante": prod_var,
        "codigo_barras": raw.get("codigo_barras"),
        "total_monto": raw.get("total_monto"),
    }


def picking_warnings_from_stops(stops: list[dict[str, Any]]) -> list[str]:
    warnings: list[str] = []
    for s in stops:
        if s.get("is_probable_included"):
            sc = s.get("probable_score")
            doc = s.get("document_number") or s.get("related_document_id")
            warnings.append(
                f"OC incluida por coincidencia probable (score {sc}) — documento {doc}; "
                "no confirmada en Bsale."
            )
    return warnings


def build_picking_client_excel(
    plan_id: int,
    header: dict[str, Any],
    clients: list[dict[str, Any]],
) -> tuple[bytes, str]:
    rows = [
        {
            "orden_ruta": c.get("route_order"),
            "ciudad": c.get("city"),
            "cliente": c.get("client_name"),
            "nombre_fantasia": c.get("fantasy_name"),
            "direccion": c.get("address"),
            "celular": c.get("phone"),
            "numero_documento": c.get("document_number"),
            "tipo_documento": c.get("document_type"),
            "forma_pago": c.get("payment_method"),
            "vendedor": c.get("seller_name"),
            "observaciones": c.get("observations"),
            "lat": c.get("lat"),
            "lng": c.get("lng"),
            "total_documento": c.get("document_total"),
            "origen": c.get("relation_source"),
            "inclusion": c.get("inclusion"),
        }
        for c in clients
    ]
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        pd.DataFrame(rows).to_excel(writer, sheet_name="Picking cliente", index=False)
        meta = pd.DataFrame(
            [
                {"campo": k, "valor": v}
                for k, v in header.items()
                if k != "plan_id"
            ]
        )
        meta.to_excel(writer, sheet_name="Encabezado", index=False)
        if any(c.get("is_probable_included") for c in clients):
            pd.DataFrame({"advertencia": picking_warnings_from_stops(clients)}).to_excel(
                writer, sheet_name="Advertencias", index=False
            )
    buf.seek(0)
    slug = _slug_filename_part(str(header.get("truck_name")))
    fname = f"picking_cliente_{slug}_{date.today().strftime('%Y%m%d')}.xlsx"
    return buf.getvalue(), fname


def build_picking_product_excel(
    plan_id: int,
    header: dict[str, Any],
    items: list[dict[str, Any]],
) -> tuple[bytes, str]:
    rows = [
        {
            "sucursal_bodega": i.get("sucursal_bodega"),
            "unidades": i.get("unidades"),
            "tipo_producto": i.get("tipo_producto"),
            "cajas": i.get("cajas"),
            "sin_unidad_caja": "Sí" if i.get("sin_unidad_caja") else "",
            "producto_variante": i.get("producto_variante"),
            "codigo_barras": i.get("codigo_barras"),
            "total_monto": i.get("total_monto"),
        }
        for i in items
    ]
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        pd.DataFrame(rows).to_excel(writer, sheet_name="Picking producto", index=False)
        pd.DataFrame(
            [{"campo": k, "valor": v} for k, v in header.items() if k != "plan_id"]
        ).to_excel(writer, sheet_name="Encabezado", index=False)
    buf.seek(0)
    slug = _slug_filename_part(str(header.get("truck_name")))
    fname = f"picking_producto_{slug}_{date.today().strftime('%Y%m%d')}.xlsx"
    return buf.getvalue(), fname
