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


def _crew_from_frozen_plan(plan: dict[str, Any]) -> tuple[str, list[str], bool]:
    """Dotación congelada en dispatch_plan al confirmar."""
    driver = (plan.get("driver_name") or "").strip()
    raw_asst = (plan.get("assistant_names") or "").strip()
    assistants = [a.strip() for a in raw_asst.split(",") if a.strip()] if raw_asst else []
    if driver or assistants:
        return driver, assistants, True
    return "", [], False


def resolve_crew_from_route_summary(
    cur,
    *,
    planning_date: date | Any,
    truck_key: str,
) -> tuple[str, list[str], str]:
    """Chofer/peoneta/sello desde planificación confirmada (route_planning_summary)."""
    sello = ""
    driver_name = ""
    assistant_names: list[str] = []
    truck = (truck_key or "").strip()
    if not truck or not planning_date:
        return driver_name, assistant_names, sello
    cur.execute(
        """
        SELECT general_observation, driver, assistant_1, assistant_2
        FROM distribuidora.route_planning_summary
        WHERE planning_date = %s
          AND upper(btrim(truck)) = upper(btrim(%s))
        LIMIT 1
        """,
        (planning_date, truck),
    )
    srow = cur.fetchone()
    if srow:
        sello = (srow[0] or "").strip()
        driver_name = (srow[1] or "").strip()
        for a in (srow[2], srow[3]):
            name = (a or "").strip()
            if name:
                assistant_names.append(name)
    return driver_name, assistant_names, sello


def build_picking_header_dict(
    plan: dict[str, Any],
    *,
    plan_id: int,
    communes: str = "",
    sello: str = "",
    driver_name: str = "",
    assistant_names: list[str] | None = None,
) -> dict[str, Any]:
    assistants = assistant_names or []
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
            ", ".join(assistants)
            if assistants
            else f"{assistant_n} peoneta{'s' if assistant_n != 1 else ''}"
        ),
        "assistant_names": assistants,
        "sello": sello,
    }


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

        driver_name, assistant_names, frozen = _crew_from_frozen_plan(plan)
        sello = ""
        if not frozen:
            truck_key = (plan.get("truck_name") or plan.get("route_name") or "").strip()
            driver_name, assistant_names, sello = resolve_crew_from_route_summary(
                cur,
                planning_date=plan.get("planning_date"),
                truck_key=truck_key,
            )
        else:
            truck_key = (plan.get("truck_name") or plan.get("route_name") or "").strip()
            _, _, sello = resolve_crew_from_route_summary(
                cur,
                planning_date=plan.get("planning_date"),
                truck_key=truck_key,
            )
        cur.close()
    finally:
        conn.close()

    return build_picking_header_dict(
        plan,
        plan_id=plan_id,
        communes=communes,
        sello=sello,
        driver_name=driver_name,
        assistant_names=assistant_names,
    )


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
        "tipo_producto": raw.get("tipo_producto") or "OTROS",
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


def _style_sheet(
    ws,
    *,
    money_cols: set[str] | None = None,
    text_cols: set[str] | None = None,
    number_cols: set[str] | None = None,
    min_widths: dict[str, float] | None = None,
) -> None:
    from openpyxl.styles import Alignment, Font, numbers
    from openpyxl.utils import get_column_letter

    money_cols = money_cols or set()
    text_cols = text_cols or set()
    number_cols = number_cols or set()
    min_widths = min_widths or {}
    if ws.max_row < 1:
        return
    headers = {cell.value: cell.column for cell in ws[1] if cell.value}
    ws.freeze_panes = "A2"
    ws.auto_filter.ref = ws.dimensions
    bold = Font(bold=True)
    for cell in ws[1]:
        cell.font = bold
        cell.alignment = Alignment(wrap_text=True, vertical="center")
    for name, col_idx in headers.items():
        letter = get_column_letter(col_idx)
        max_len = len(str(name or ""))
        for row in ws.iter_rows(min_row=2, min_col=col_idx, max_col=col_idx):
            val = row[0].value
            if val is None:
                continue
            max_len = max(max_len, min(len(str(val)), 48))
            if name in money_cols and isinstance(val, (int, float)):
                row[0].number_format = '#,##0'
            if name in number_cols and isinstance(val, (int, float)):
                row[0].number_format = '#,##0.##'
                row[0].alignment = Alignment(horizontal="right", vertical="center")
            if name in text_cols:
                row[0].number_format = numbers.FORMAT_TEXT
                row[0].value = str(val)
        base_w = max(12, min(max_len + 2, 42))
        ws.column_dimensions[letter].width = max(base_w, float(min_widths.get(str(name), 0) or 0))
    ws.print_title_rows = "1:1"
    ws.page_setup.orientation = "landscape"
    ws.page_setup.fitToPage = True
    ws.page_setup.fitToWidth = 1
    ws.page_setup.fitToHeight = 0


def build_picking_client_excel(
    plan_id: int,
    header: dict[str, Any],
    clients: list[dict[str, Any]],
) -> tuple[bytes, str]:
    from backend.utils.picking_presentation import enrich_client_presentation

    presented = enrich_client_presentation(clients)
    # Hoja legible (bodega/chofer). Conserva orden de entrega.
    # Hoja legible alineada al formato operativo de chofer (sin campos E3/E4 de ruta).
    present_rows = [
        {
            "Parada": c.get("stop_number"),
            "Ciudad": (
                f"{c.get('display_commune')} · {c.get('display_sector')}"
                if c.get("display_sector")
                else (c.get("display_commune") or c.get("city") or "")
            ),
            "Cliente": c.get("client_name"),
            "Dirección": c.get("address"),
            "Celular cliente": c.get("phone"),
            "Número documento": (
                str(c.get("document_number")) if c.get("document_number") is not None else ""
            ),
            "Formato de pago": c.get("payment_method"),
            "Nombre de fantasía": c.get("fantasy_name"),
            "OBS": c.get("observations"),
            "Vendedor": c.get("seller_name"),
            "Total": c.get("document_total"),
        }
        for c in presented
    ]
    # Hoja técnica compatible con integraciones existentes.
    tech_rows = [
        {
            "orden_ruta": c.get("route_order"),
            "parada_visible": c.get("stop_number"),
            "ciudad": c.get("city"),
            "comuna": c.get("display_commune"),
            "sector": c.get("display_sector"),
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
        for c in presented
    ]
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        pd.DataFrame(present_rows).to_excel(writer, sheet_name="Picking cliente", index=False)
        _style_sheet(
            writer.sheets["Picking cliente"],
            money_cols={"Total"},
            text_cols={"Número documento", "Celular cliente"},
        )
        pd.DataFrame(tech_rows).to_excel(writer, sheet_name="Datos técnicos", index=False)
        _style_sheet(
            writer.sheets["Datos técnicos"],
            money_cols={"total_documento"},
            text_cols={"numero_documento", "celular"},
        )
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
    from backend.utils.picking_presentation import commercial_pack_label

    present_rows = []
    tech_rows = []
    for i in items:
        pack = commercial_pack_label(
            units_per_box=i.get("units_per_box_efectivo") or i.get("units_per_box"),
            unidades=i.get("unidades"),
        )
        # Cajas = número de equivalencia comercial verificada; vacío si no hay conversión.
        # Control permanece vacío para anotación manual.
        cajas_val: int | float | str = ""
        if not i.get("sin_unidad_caja"):
            try:
                upb = float(i.get("units_per_box_efectivo") or i.get("units_per_box") or 0)
                u = float(i.get("unidades") or 0)
                if upb > 0 and u > 0:
                    boxes = round(u / upb, 2)
                    cajas_val = int(boxes) if float(boxes).is_integer() else boxes
            except (TypeError, ValueError):
                cajas_val = ""
        present_rows.append(
            {
                "Sucursal origen": i.get("sucursal_bodega"),
                "Categoría": i.get("tipo_producto"),
                "Producto": i.get("display_name") or i.get("producto_variante"),
                "Código de barras": (
                    str(i.get("codigo_barras")) if i.get("codigo_barras") is not None else ""
                ),
                "Unidades": i.get("unidades"),
                "Cajas": cajas_val,
                "CONTROL": "",
                "Total": i.get("total_monto"),
                "Camión": header.get("truck_name"),
                "Nº picking": header.get("planning_number"),
                "Sello": header.get("sello") or "",
            }
        )
        tech_rows.append(
            {
                "sucursal_bodega": i.get("sucursal_bodega"),
                "unidades": i.get("unidades"),
                "tipo_producto": i.get("tipo_producto"),
                "cajas": i.get("cajas"),
                "sin_unidad_caja": "Sí" if i.get("sin_unidad_caja") else "",
                "units_per_box": i.get("units_per_box"),
                "presentacion_comercial": pack,
                "producto_variante": i.get("display_name") or i.get("producto_variante"),
                "codigo_barras": i.get("codigo_barras"),
                "total_monto": i.get("total_monto"),
            }
        )
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        pd.DataFrame(present_rows).to_excel(writer, sheet_name="Picking producto", index=False)
        _style_sheet(
            writer.sheets["Picking producto"],
            money_cols={"Total"},
            text_cols={"Código de barras"},
            number_cols={"Unidades", "Cajas"},
            min_widths={"CONTROL": 14, "Control": 14, "Código de barras": 18, "Producto": 36},
        )
        pd.DataFrame(tech_rows).to_excel(writer, sheet_name="Datos técnicos", index=False)
        _style_sheet(
            writer.sheets["Datos técnicos"],
            money_cols={"total_monto"},
            text_cols={"codigo_barras"},
        )
        pd.DataFrame(
            [{"campo": k, "valor": v} for k, v in header.items() if k != "plan_id"]
        ).to_excel(writer, sheet_name="Encabezado", index=False)
    buf.seek(0)
    slug = _slug_filename_part(str(header.get("truck_name")))
    fname = f"picking_producto_{slug}_{date.today().strftime('%Y%m%d')}.xlsx"
    return buf.getvalue(), fname
