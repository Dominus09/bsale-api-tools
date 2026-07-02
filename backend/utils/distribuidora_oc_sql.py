"""Fragmentos SQL compartidos para órdenes de compra (tipo 33)."""

from __future__ import annotations

# OC (tipo 33): pendiente si no hay document_related hacia boleta/factura (1 o 6).
OC_PURCHASE_NOT_INVOICED_BY_RELATED_SQL = """
NOT EXISTS (
    SELECT 1
    FROM distribuidora.document_related dr
    INNER JOIN distribuidora.document_details dd ON dd.detail_id = dr.detail_id
    INNER JOIN distribuidora.documents inv
        ON inv.document_id = dr.related_document_id
       AND inv.document_type_id IN (1, 6)
       AND inv.company_id = d.company_id
       AND inv.office_id = d.office_id
    WHERE dd.document_id = d.document_id
)
""".strip()
