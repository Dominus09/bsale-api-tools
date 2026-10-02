"""Recursos de documentos (crítico; prioridad empresa 3 / document_type_id 33 = OC de vendedores).

Observado en fase 2:
- count global sin filtros (C3) = 3.880.542 → PROHIBIDO full scan global.
- ``generationdaterange`` en ``/v1/documents.json`` → HTTP 403 (REJECTED). No se usa.
- ``documenttypeid=33`` + ``emissiondaterange`` funciona (32 OC en la ventana de prueba).
- El documento trae links details/sellers/references, sin stock directo.
- ``expand=[details]`` INCONCLUSIVE: la integridad sale de ``/documents/{id}/details.json`` paginado.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any

from backend.services.bsale_raw.core.models import payload_hash, relation_id
from backend.services.bsale_raw.core.rate_limit import RequestPriority
from backend.services.bsale_raw.core.registry import (
    REGISTRY,
    KeyKind,
    Priority,
    ResourceSpec,
    document_type_scope,
)

PRIORITY_DOCUMENT_SCOPES: tuple[tuple[int, int], ...] = ((3, 33),)
FORBIDDEN_DOCUMENT_FILTERS: frozenset[str] = frozenset({"generationdaterange"})
P1 = RequestPriority.P1_OC33


# Partes de un refresh completo; todas se persisten en UNA transacción con el mismo version_hash.
DOCUMENT_REFRESH_PARTS: tuple[str, ...] = ("header", "details", "references", "sellers", "attributes")


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


def _sorted_by_id(items: Sequence[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    return sorted(items, key=lambda item: (str(item.get("id", "")), payload_hash(item)))


@dataclass(frozen=True)
class DocumentVersion:
    """Versión observada completa de un documento: todas sus partes de un mismo refresh."""

    header: Mapping[str, Any]
    details: Sequence[Mapping[str, Any]]
    references: Sequence[Mapping[str, Any]]
    sellers: Sequence[Mapping[str, Any]]
    attributes: Any = None

    def part_hashes(self) -> dict[str, str]:
        return {
            "header": payload_hash(self.header),
            "details": payload_hash(_sorted_by_id(self.details)),
            "references": payload_hash(_sorted_by_id(self.references)),
            "sellers": payload_hash(_sorted_by_id(self.sellers)),
            "attributes": payload_hash(self.attributes),
        }

    @property
    def children_hash(self) -> str:
        hashes = self.part_hashes()
        return payload_hash({k: v for k, v in hashes.items() if k != "header"})

    @property
    def version_hash(self) -> str:
        return payload_hash(self.part_hashes())

    def variant_ids(self) -> frozenset[int]:
        ids = set()
        for detail in self.details:
            vid = relation_id(dict(detail), "variant")
            if vid is not None:
                ids.add(vid)
        return frozenset(ids)


def changed_parts(previous: Mapping[str, str] | None, current: DocumentVersion) -> dict[str, bool]:
    """Partes cambiadas respecto de los hashes anteriores (None = primera observación)."""
    hashes = current.part_hashes()
    if previous is None:
        return {part: True for part in DOCUMENT_REFRESH_PARTS}
    return {part: previous.get(part) != hashes[part] for part in DOCUMENT_REFRESH_PARTS}


def affected_variants(previous_variants: Iterable[int], current_variants: Iterable[int]) -> frozenset[int]:
    """previous ∪ current: cubre líneas agregadas, quitadas, cambiadas, facturación y anulación."""
    return frozenset(previous_variants) | frozenset(current_variants)

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
