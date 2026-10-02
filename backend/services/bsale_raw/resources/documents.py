"""Recursos de documentos (crítico; prioridad empresa 3 / document_type_id 33 = OC de vendedores)."""

from __future__ import annotations

from backend.services.bsale_raw.core.registry import REGISTRY, KeyKind, Priority, ResourceSpec

PRIORITY_DOCUMENT_SCOPES: tuple[tuple[int, int], ...] = ((3, 33),)

DOCUMENTS = REGISTRY.register(
    ResourceSpec(
        name="documents",
        list_endpoint="/v1/documents.json",
        item_endpoint="/v1/documents/{id}.json",
        raw_table="bsale_raw.documents",
        key_kind=KeyKind.ENTITY,
        priority=Priority.CRITICAL,
        webhook_topics=("document",),
        state_filter=True,
        incremental_filter="emissiondaterange",
        point_filters=("documenttypeid", "officeid", "number", "clientid"),
        expand=("details", "references", "sellers", "document_taxes", "attributes"),
        freshness_sla_seconds=5 * 60,
        needs_live_verification=(
            "generationdaterange en documents.json NO está documentado (sólo en summary.json)",
            "webhook document sólo documenta action=post: ¿llegan PUT al anular/modificar?",
            "expand=[details] ¿trunca detalles >25? ¿hay que paginar /details.json?",
            "documentos con state=1 (anulados) ¿aparecen sin filtro state?",
        ),
    )
)

DOCUMENT_DETAILS = REGISTRY.register(
    ResourceSpec(
        name="document_details",
        list_endpoint="/v1/documents/{parent_id}/details.json",
        item_endpoint="/v1/documents/{parent_id}/details/{id}.json",
        raw_table="bsale_raw.document_details",
        key_kind=KeyKind.CHILD,
        priority=Priority.CRITICAL,
        parent="documents",
        freshness_sla_seconds=5 * 60,
    )
)

DOCUMENT_REFERENCES = REGISTRY.register(
    ResourceSpec(
        name="document_references",
        list_endpoint="/v1/documents/{parent_id}/references.json",
        item_endpoint="/v1/documents/{parent_id}/references/{id}.json",
        raw_table="bsale_raw.document_references",
        key_kind=KeyKind.CHILD,
        priority=Priority.CRITICAL,
        parent="documents",
        freshness_sla_seconds=5 * 60,
        needs_live_verification=("sólo retorna referencias electrónicas (XML) según docs",),
    )
)

DOCUMENT_SELLERS = REGISTRY.register(
    ResourceSpec(
        name="document_sellers",
        list_endpoint="/v1/documents/{parent_id}/sellers.json",
        item_endpoint=None,
        raw_table="bsale_raw.document_sellers",
        key_kind=KeyKind.CHILD,
        priority=Priority.CRITICAL,
        parent="documents",
        freshness_sla_seconds=5 * 60,
        needs_live_verification=("el item es un usuario; el id no identifica la relación por sí solo",),
    )
)
