"""Lectura de la revisión Bsale vigente de ``distribuidora.documents``.

Todos los flujos que leen/escriben hijos (details, attributes, references, sellers,
related) deben resolver la revisión con ``load_current_source`` y verificarla con
``children_source_is_current`` antes de persistir.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from backend.utils.bsale_document_ids import (
    DocumentSourceEvidence,
    coerce_folio_number,
    coerce_positive_document_id,
    resolve_current_source_document_id,
)

logger = logging.getLogger(__name__)

# ``to_jsonb(d)`` tolera instalaciones sin las columnas de 041/044.
SOURCE_EVIDENCE_COLUMNS_SQL = """
    d.document_id,
    d.number,
    to_jsonb(d)->>'source_document_id',
    (to_jsonb(d)->>'source_updated_at')::timestamptz,
    d.raw_data->>'id',
    d.raw_data->>'number',
    (to_jsonb(d)->>'bsale_modified_at')::timestamptz
"""


def _as_datetime(v: Any) -> datetime | None:
    return v if isinstance(v, datetime) else None


def source_evidence_from_row(row: tuple | list) -> DocumentSourceEvidence:
    """Fila con el orden de ``SOURCE_EVIDENCE_COLUMNS_SQL``."""
    return DocumentSourceEvidence(
        local_document_id=int(row[0]),
        folio=coerce_folio_number(row[1]),
        source_document_id=coerce_positive_document_id(row[2]),
        source_updated_at=_as_datetime(row[3]),
        raw_data_id=coerce_positive_document_id(row[4]),
        raw_number=coerce_folio_number(row[5]),
        raw_revision_at=_as_datetime(row[6]),
    )


def load_source_evidence(
    cur,
    local_document_id: int,
    *,
    for_update: bool = False,
) -> DocumentSourceEvidence | None:
    lock = "FOR UPDATE OF d" if for_update else ""
    cur.execute(
        f"""
        SELECT {SOURCE_EVIDENCE_COLUMNS_SQL}
        FROM distribuidora.documents d
        WHERE d.document_id = %s
        {lock}
        """,
        (int(local_document_id),),
    )
    row = cur.fetchone()
    if row is None:
        return None
    return source_evidence_from_row(row)


def load_current_source(cur, local_document_id: int) -> tuple[int, int | None]:
    """``(bsale_source_document_id, folio)``; sin fila local retorna la propia PK."""
    ev = load_source_evidence(cur, local_document_id)
    if ev is None:
        return int(local_document_id), None
    return resolve_current_source_document_id(ev), ev.folio


def children_source_is_current(
    cur,
    local_document_id: int,
    fetched_source_document_id: int,
) -> bool:
    """
    Bloquea el header (``FOR UPDATE``) y confirma que los hijos descargados desde
    ``fetched_source_document_id`` siguen correspondiendo a la revisión vigente.

    Debe llamarse dentro de la TX que persiste los hijos: el lock se mantiene hasta
    el commit, por lo que un upsert concurrente de una revisión más nueva espera.
    """
    ev = load_source_evidence(cur, local_document_id, for_update=True)
    if ev is None:
        logger.warning(
            "children_source_guard local_document_id=%s bsale_source_document_id=%s "
            "status=skip reason=header_missing",
            local_document_id,
            fetched_source_document_id,
        )
        return False
    current = resolve_current_source_document_id(ev)
    if int(current) != int(fetched_source_document_id):
        logger.warning(
            "children_source_guard local_document_id=%s bsale_source_document_id=%s "
            "current_source_document_id=%s status=skip reason=stale_source",
            local_document_id,
            fetched_source_document_id,
            current,
        )
        return False
    return True
