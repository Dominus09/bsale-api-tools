"""Fragmentos SQL compartidos para órdenes de compra (tipo 33)."""

from __future__ import annotations

# OC (tipo 33): pendiente solo si NO hay arista confirmada a boleta/factura.
#
# Fuente de verdad del tipo: ``document_related.related_document_type``.
# NO exigir JOIN a ``documents``: puede existir related huérfano (doc aún no
# sincronizado) y la OC seguiría apareciendo como "para facturar" (canario 68677).
OC_PURCHASE_IS_INVOICED_BY_RELATED_SQL = """
EXISTS (
    SELECT 1
    FROM distribuidora.document_details dd
    INNER JOIN distribuidora.document_related dr ON dr.detail_id = dd.detail_id
    WHERE dd.document_id = d.document_id
      AND dr.related_document_type IN (1, 6)
)
""".strip()

OC_PURCHASE_NOT_INVOICED_BY_RELATED_SQL = f"""
NOT ({OC_PURCHASE_IS_INVOICED_BY_RELATED_SQL})
""".strip()
