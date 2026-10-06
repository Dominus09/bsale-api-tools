"""Fragmentos SQL compartidos para órdenes de compra (tipo 33)."""

from __future__ import annotations

# OC (tipo 33): pendiente solo si NO hay arista confirmada a boleta/factura.
#
# Fuente de verdad del tipo: ``document_related.related_document_type``.
# NO exigir JOIN a ``documents``: puede existir related huérfano (doc aún no
# sincronizado) y la OC seguiría apareciendo como "para facturar" (canario 68677).
#
# El detalle de la OC puede ser vigente o histórico (reemisión Bsale): la relación
# se resuelve al ``document_id`` local estable vía ``v_document_related_resolved``.
DOCUMENT_RELATED_RESOLVED_VIEW = "distribuidora.v_document_related_resolved"

OC_PURCHASE_IS_INVOICED_BY_RELATED_SQL = f"""
EXISTS (
    SELECT 1
    FROM {DOCUMENT_RELATED_RESOLVED_VIEW} dr
    WHERE dr.origin_document_id = d.document_id
      AND dr.related_document_type IN (1, 6)
)
""".strip()

OC_PURCHASE_NOT_INVOICED_BY_RELATED_SQL = f"""
NOT ({OC_PURCHASE_IS_INVOICED_BY_RELATED_SQL})
""".strip()
