"""Construcción de filas cuadratura v2 desde snapshots congelados de picking."""

from __future__ import annotations

from typing import Any

from backend.utils.dispatch_plan_cuadratura_v2 import guess_medio_from_payment_method


def build_documents_from_picking_clients(
    clients: list[dict[str, Any]],
    *,
    saved_documents: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
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
        producto = (item.get("producto") or "").strip()
        variante = (item.get("variante") or "").strip()
        label = (
            item.get("producto_variante")
            or item.get("display_name")
            or (producto if producto == variante else f"{producto} — {variante}".strip(" —"))
            or producto
            or variante
        ).strip()
        catalog.append(
            {
                "product_id": item.get("product_id"),
                "variant_id": item.get("variant_id"),
                "producto": producto or label,
                "variante": variante,
                "producto_variante": label,
                "codigo_barras": item.get("codigo_barras"),
            }
        )
    return catalog


def normalize_credit_note_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "documento": str(row.get("documento") or row.get("documento_venta") or "").strip(),
        "nota_credito": str(
            row.get("nota_credito") or row.get("numero_nc") or row.get("nota_credito") or ""
        ).strip(),
        "monto": int(round(float(row.get("monto") or 0))),
        "observacion": str(row.get("observacion") or row.get("motivo") or "").strip(),
    }


def normalize_credit_notes(rows: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    return [normalize_credit_note_row(r) for r in rows or []]


def resolve_product_from_catalog(
    query: str,
    catalog: list[dict[str, Any]],
) -> dict[str, Any] | None:
    q = (query or "").strip()
    if not q:
        return None
    q_lower = q.lower()
    for row in catalog:
        bc = str(row.get("codigo_barras") or "").strip()
        if bc and bc == q:
            return row
    for row in catalog:
        label = (row.get("producto_variante") or "").strip().lower()
        if label and label == q_lower:
            return row
    for row in catalog:
        label = (row.get("producto_variante") or "").lower()
        if q_lower in label:
            return row
    for row in catalog:
        prod = (row.get("producto") or "").lower()
        var = (row.get("variante") or "").lower()
        if q_lower in prod or q_lower in var:
            return row
    return None


def normalize_not_loaded_row(
    row: dict[str, Any],
    catalog: list[dict[str, Any]],
) -> dict[str, Any]:
    query = (row.get("producto") or row.get("producto_variante") or "").strip()
    resolved = resolve_product_from_catalog(query, catalog)
    if resolved:
        return {
            "producto": resolved.get("producto_variante") or query,
            "producto_variante": resolved.get("producto_variante") or query,
            "codigo_barras": resolved.get("codigo_barras"),
            "product_id": resolved.get("product_id"),
            "variant_id": resolved.get("variant_id"),
            "cantidad": float(row.get("cantidad") or 0),
            "motivo": (row.get("motivo") or "").strip(),
        }
    return {
        "producto": query,
        "producto_variante": query,
        "codigo_barras": row.get("codigo_barras"),
        "product_id": row.get("product_id"),
        "variant_id": row.get("variant_id"),
        "cantidad": float(row.get("cantidad") or 0),
        "motivo": (row.get("motivo") or "").strip(),
    }


def normalize_not_loaded_rows(
    rows: list[dict[str, Any]] | None,
    catalog: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    return [normalize_not_loaded_row(r, catalog) for r in rows or []]


def _doc_key(row: dict[str, Any]) -> str:
    if row.get("related_document_id") is not None:
        return str(row["related_document_id"])
    if row.get("document_number") is not None:
        return str(row["document_number"])
    return ""
