"""Recursos de documentos (crítico; prioridad empresa 3 / document_type_id 33 = OC de vendedores).

Observado en fase 2:
- count global sin filtros (C3) = 3.880.542 → PROHIBIDO full scan global.
- ``generationdaterange`` en ``/v1/documents.json`` → HTTP 403 (REJECTED). No se usa.
- ``documenttypeid=33`` + ``emissiondaterange`` funciona (32 OC en la ventana de prueba).
- El documento trae links details/sellers/references, sin stock directo.
- ``expand=[details]`` INCONCLUSIVE: la integridad sale de ``/documents/{id}/details.json`` paginado.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from backend.services.bsale_raw.core.document_version import (  # noqa: F401  (API pública previa)
    DOCUMENT_REFRESH_PARTS,
    DocumentVersion,
    affected_variants,
    changed_parts,
)
from backend.services.bsale_raw.core.models import SyncMode
from backend.services.bsale_raw.core.rate_limit import RequestPriority
from backend.services.bsale_raw.core.registry import (
    REGISTRY,
    KeyKind,
    Priority,
    ResourceSpec,
    TypedColumn,
    document_type_scope,
    optional_int,
    optional_numeric,
    optional_relation_id,
    optional_text,
    optional_unix_date,
    optional_unix_datetime,
)

PRIORITY_DOCUMENT_SCOPES: tuple[tuple[int, int], ...] = ((3, 33),)
FORBIDDEN_DOCUMENT_FILTERS: frozenset[str] = frozenset({"generationdaterange"})
P1 = RequestPriority.P1_OC33

# Fase 4E1: el refresh POINT de documentos sólo acepta estos document_type_id (identidad por id,
# nunca por nombre). Un documento de otro tipo falla sin escribir.
POINT_DOCUMENT_TYPE_IDS: frozenset[int] = frozenset({33})


class WatchAction(str, Enum):
    ACTIVE = "ACTIVE"  # no terminal: refresh por ID con la frecuencia normal
    GRACE = "GRACE"  # terminal detectado: seguir verificando por ID durante la gracia
    CLOSE = "CLOSE"  # gracia cumplida con lecturas estables: cerrar watch (watch_closed_at)


@dataclass(frozen=True)
class OpenDocumentWatch:
    """Refresh periódico POR ID de documentos mutables (el webhook sólo garantiza creación).

    Una OC 33 cambia después de creada (líneas, cantidades, descuentos, vendedor, cliente,
    atributos, estado, facturación, anulación, references). Candidatos: ``bsale_raw.documents``
    de (company_id, document_type_id) con emisión dentro de ``lookback_days`` y
    ``watch_closed_at IS NULL``, ordenados por ``api_fetched_at`` (``ix_raw_documents_watch``).

    Qué estado es terminal se decide aquí, nunca en SQL. Los conjuntos quedan vacíos hasta
    confirmarlos en vivo, de modo que ningún documento sale del watch por estado.
    Al detectar un estado terminal no se cierra de inmediato: gracia de ``grace_seconds`` con
    lecturas por ID cada ``grace_read_interval_seconds``; sólo tras ``grace_min_stable_reads``
    lecturas sin cambio de ``version_hash`` se cierra. Un cambio durante la gracia reinicia el
    conteo. ``children_full_refresh_seconds`` fuerza el refresh de hijos aunque el header no
    cambie (sellers/attributes pueden cambiar sin alterar el header).
    """

    company_id: int
    document_type_id: int
    lookback_days: int
    min_refresh_interval_seconds: int
    grace_seconds: int = 45 * 60
    grace_read_interval_seconds: int = 10 * 60
    grace_min_stable_reads: int = 3
    children_full_refresh_seconds: int = 60 * 60
    terminal_states: frozenset[int] = frozenset()
    terminal_commercial_states: frozenset[str] = frozenset()

    @property
    def scope(self) -> str:
        return document_type_scope(self.document_type_id)

    def is_terminal(self, state: int | None, commercial_state: str | None) -> bool:
        if state is not None and state in self.terminal_states:
            return True
        return commercial_state is not None and commercial_state in self.terminal_commercial_states

    def decide(
        self,
        *,
        state: int | None,
        commercial_state: str | None,
        terminal_seen_at: datetime | None,
        stable_reads: int,
        now: datetime,
    ) -> WatchAction:
        if not self.is_terminal(state, commercial_state):
            return WatchAction.ACTIVE
        if terminal_seen_at is None:
            return WatchAction.GRACE
        grace_over = (now - terminal_seen_at).total_seconds() >= self.grace_seconds
        if grace_over and stable_reads >= self.grace_min_stable_reads:
            return WatchAction.CLOSE
        return WatchAction.GRACE


OPEN_DOCUMENT_WATCHES: tuple[OpenDocumentWatch, ...] = (
    OpenDocumentWatch(company_id=3, document_type_id=33, lookback_days=45, min_refresh_interval_seconds=15 * 60),
)


DOCUMENTS = REGISTRY.register(
    ResourceSpec(
        name="documents",
        list_endpoint="/v1/documents.json",
        item_endpoint="/v1/documents/{id}.json",
        raw_table="bsale_raw.documents",
        key_kind=KeyKind.ENTITY,
        priority=Priority.CRITICAL,
        request_priority=P1,
        webhook_topics=("document",),
        state_filter=True,
        incremental_filter="emissiondaterange",
        full_scan_global_allowed=False,
        reconcile_window_days=45,
        point_filters=("documenttypeid", "officeid", "number", "clientid"),
        freshness_sla_seconds=5 * 60,
        needs_live_verification=(
            "webhook document sólo documenta action=post: ¿llegan PUT al anular/modificar?",
            "documentos con state=1 (anulados) ¿aparecen sin filtro state?",
        ),
        # attributes: /v1/documents/{id}/attributes.json (LIVE VERIFIED C3, paginado) → la colección
        # completa va a documents.attributes_payload = {"count", "items"}; no hay tabla de attributes.
        # details_count, details_complete, children_fetched_at, attributes_payload y hashes los
        # calcula core/document_engine.py desde el bundle completo.
        typed_columns=(
            TypedColumn("state", "state", optional_int),
            TypedColumn("commercial_state", "commercialState", optional_text),
            TypedColumn("document_type_id", "document_type", optional_relation_id),
            TypedColumn("office_id", "office", optional_relation_id),
            TypedColumn("client_id", "client", optional_relation_id),
            TypedColumn("user_id", "user", optional_relation_id),
            TypedColumn("number", "number", optional_int),
            TypedColumn("emission_date", "emissionDate", optional_unix_date),
            TypedColumn("generation_date", "generationDate", optional_unix_datetime),
            TypedColumn("total_amount", "totalAmount", optional_numeric),
            TypedColumn("informed_sii", "informedSii", optional_int),
        ),
        # Sólo POINT por id técnico (fase 4E1). Sin FULL_RECONCILE: el full scan global está prohibido.
        pipeline_enabled=True,
        pipeline_modes=(SyncMode.POINT,),
        point_key="document",
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
        request_priority=P1,
        parent="documents",
        freshness_sla_seconds=5 * 60,
        typed_columns=(
            TypedColumn("variant_id", "variant", optional_relation_id),
            TypedColumn("line_number", "lineNumber", optional_int),
            TypedColumn("quantity", "quantity", optional_numeric),
            TypedColumn("related_detail_id", "relatedDetailId", optional_int),
        ),
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
        request_priority=P1,
        parent="documents",
        freshness_sla_seconds=5 * 60,
        needs_live_verification=("sólo retorna referencias electrónicas (XML) según docs",),
        typed_columns=(
            TypedColumn("number", "number", optional_text),
            TypedColumn("dte_code_id", "dte_code", optional_relation_id),
            TypedColumn("reference_date", "referenceDate", optional_unix_date),
        ),
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
        request_priority=P1,
        parent="documents",
        freshness_sla_seconds=5 * 60,
    )
)
