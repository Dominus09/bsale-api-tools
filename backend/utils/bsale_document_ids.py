"""Resolución local_document_id vs bsale_source_document_id.

Tras ``ON CONFLICT`` por folio, la PK de PostgreSQL puede diferir del ``id``
vigente en Bsale. Los GET de hijos (details/attributes/…) deben usar el id
Bsale; la persistencia usa siempre la PK local.
"""

from __future__ import annotations

from typing import Any


def coerce_positive_document_id(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        n = int(value)
    except (TypeError, ValueError):
        return None
    return n if n > 0 else None


def resolve_bsale_source_document_id(
    *,
    local_document_id: int,
    raw_document: dict[str, Any] | None = None,
    raw_data_id: Any = None,
) -> int:
    """
    Id a usar en URLs ``/documents/{id}/…`` de Bsale.

    Preferencia: ``raw_document["id"]`` → ``raw_data_id`` (p. ej. ``raw_data->>'id'``)
    → ``local_document_id`` (mismo id cuando no hubo reemisión).
    """
    candidates: list[Any] = []
    if isinstance(raw_document, dict):
        candidates.append(raw_document.get("id"))
    candidates.append(raw_data_id)
    for c in candidates:
        sid = coerce_positive_document_id(c)
        if sid is not None:
            return sid
    return int(local_document_id)


def ids_differ(local_document_id: int, bsale_source_document_id: int) -> bool:
    return int(local_document_id) != int(bsale_source_document_id)
