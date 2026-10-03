"""Hash de versión de un documento Bsale (puro). Re-exportado por ``resources/documents.py``."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from backend.services.bsale_raw.core.models import payload_hash, relation_id

# Partes de un refresh completo; todas se persisten en UNA transacción con el mismo version_hash.
DOCUMENT_REFRESH_PARTS: tuple[str, ...] = ("header", "details", "references", "sellers", "attributes")


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
