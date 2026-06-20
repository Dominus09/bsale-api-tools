"""Construcción de filas cuadratura v2 desde snapshots congelados de picking."""

from __future__ import annotations

from typing import Any

from backend.utils.dispatch_plan_cuadratura_v2 import guess_medio_from_payment_method


def build_documents_from_picking_clients(
    clients: list[dict[str, Any]],
    *,
    saved_documents: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Una fila por documento facturado en picking cliente congelado."""
    saved_by_doc: dict[str, dict[str, Any]] = {}
    for row in saved_documents or []:
        key = _doc_key(row)
        if key:
            saved_by_doc[key] = row

    out: list[dict[str, Any]] = []
    for client in clients or []:
        doc_id = client.get("related_document_id")
        doc_num = client.get("document_number")
        if doc_id is None and doc_num is None:
            continue
        key = str(doc_id or doc_num)
        prev = saved_by_doc.get(key) or {}
        try:
            monto = int(round(float(client.get("document_total") or 0)))
        except (TypeError, ValueError):
            monto = 0
        medio = prev.get("medio_pago") or guess_medio_from_payment_method(
            client.get("payment_method")
        )
        out.append(
            {
                "related_document_id": doc_id,
                "document_number": doc_num,
                "oc_document_id": client.get("oc_document_id"),
                "client_name": client.get("fantasy_name")
                or client.get("client_name")
                or "",
                "monto_clp": monto,
                "medio_pago": medio,
                "observacion": (prev.get("observacion") or "").strip(),
                "route_order": client.get("route_order"),
            }
        )
    out.sort(key=lambda r: (r.get("route_order") or 0, str(r.get("document_number") or "")))
    return out


def build_product_catalog_from_picking(
    products: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    catalog: list[dict[str, Any]] = []
    for item in products or []:
        try:
            unidades = float(item.get("unidades") or 0)
        except (TypeError, ValueError):
            unidades = 0.0
        try:
            total = float(item.get("total_monto") or 0)
        except (TypeError, ValueError):
            total = 0.0
        unit_price = round(total / unidades, 2) if unidades > 0 else 0.0
        label = (
            item.get("producto_variante")
            or item.get("display_name")
            or item.get("producto")
            or ""
        ).strip()
        catalog.append(
            {
                "product_id": item.get("product_id"),
                "variant_id": item.get("variant_id"),
                "producto": label,
                "codigo_barras": item.get("codigo_barras"),
                "unidades_snapshot": unidades,
                "total_monto_snapshot": total,
                "unit_price_clp": unit_price,
            }
        )
    return catalog


def estimate_not_loaded_monto(
    *,
    producto: str,
    cantidad: float,
    catalog: list[dict[str, Any]],
    product_id: Any = None,
    variant_id: Any = None,
) -> int:
    qty = max(0.0, float(cantidad or 0))
    if qty <= 0:
        return 0
    needle = (producto or "").strip().lower()
    match = None
    for row in catalog:
        if product_id and row.get("product_id") == product_id:
            if variant_id is None or row.get("variant_id") == variant_id:
                match = row
                break
        if needle and needle == (row.get("producto") or "").strip().lower():
            match = row
            break
    if not match and needle:
        for row in catalog:
            if needle in (row.get("producto") or "").lower():
                match = row
                break
    if not match:
        return 0
    unit = float(match.get("unit_price_clp") or 0)
    return int(round(unit * qty))


def enrich_not_loaded_rows(
    rows: list[dict[str, Any]],
    catalog: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows or []:
        item = dict(row)
        if item.get("monto_clp") is None or int(item.get("monto_clp") or 0) == 0:
            item["monto_clp"] = estimate_not_loaded_monto(
                producto=item.get("producto") or "",
                cantidad=item.get("cantidad") or 0,
                catalog=catalog,
                product_id=item.get("product_id"),
                variant_id=item.get("variant_id"),
            )
        out.append(item)
    return out


def _doc_key(row: dict[str, Any]) -> str:
    if row.get("related_document_id") is not None:
        return str(row["related_document_id"])
    if row.get("document_number") is not None:
        return str(row["document_number"])
    return ""
