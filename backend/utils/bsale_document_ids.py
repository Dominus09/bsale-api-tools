"""Resolución local_document_id vs bsale_source_document_id.

Tras ``ON CONFLICT`` por folio, la PK de PostgreSQL puede diferir del ``id``
vigente en Bsale. Los GET de hijos (details/attributes/…) deben usar el id
Bsale; la persistencia usa siempre la PK local.

Modelo de identidad:

* ``documents.document_id`` (PK local) = identidad comercial estable.
* ``id`` Bsale = revisión técnica; una reemisión crea un id nuevo con el mismo folio.
* ``source_document_id`` = revisión Bsale vigente según la última escritura coordinada.

``resolve_current_source_document_id`` es el único criterio para decidir qué revisión
Bsale usar para leer/escribir hijos. ``is_revision_not_older`` es la política de
frescura compartida (header y children); el upsert SQL replica el mismo orden
``(bsale_modified_at, id)``.

Semántica conocida de ``state = 8888``:

* Bsale: revisión técnica reemplazada (``number = 0`` + ``state = 8888``). No es folio
  comercial; se descarta por ``number <= 0`` (ver ``positive_folio_number``), no por state.
* Local: marcador de cancelación escrito por la reconciliación OC sobre la fila
  comercial (folio positivo) cuando el folio ya no tiene revisión activa en Bsale.
* ``number > 0`` con ``state = 8888`` no implica "reemplazado": sin evidencia de
  continuidad (otra revisión del mismo folio) se persiste igual que cualquier otro
  documento. El state solo no descarta.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any


def coerce_positive_document_id(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        n = int(value)
    except (TypeError, ValueError):
        return None
    return n if n > 0 else None


def coerce_folio_number(raw: Any) -> int | None:
    """Folio entero tal como viene de Bsale/PG (puede ser 0 o negativo); None si no es entero."""
    if raw is None or isinstance(raw, bool):
        return None
    if isinstance(raw, int):
        return raw
    if isinstance(raw, float):
        try:
            i = int(raw)
        except (TypeError, ValueError, OverflowError):
            return None
        return i if i == raw else None
    s = str(raw).strip()
    if not s:
        return None
    try:
        return int(s, 10)
    except ValueError:
        return None


def positive_folio_number(raw: Any) -> int | None:
    """Folio comercial candidato: solo enteros > 0. ``number <= 0`` nunca es folio."""
    n = coerce_folio_number(raw)
    return n if n is not None and n > 0 else None


def is_non_positive_folio(raw: Any) -> bool:
    n = coerce_folio_number(raw)
    return n is not None and n <= 0


def resolve_bsale_source_document_id(
    *,
    local_document_id: int,
    raw_document: dict[str, Any] | None = None,
    raw_data_id: Any = None,
) -> int:
    """
    Id a usar en URLs ``/documents/{id}/…`` cuando el caller ya trae el payload Bsale
    que acaba de persistir (``raw_document``). Para filas leídas desde PG usar
    ``resolve_current_source_document_id``.
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


def is_revision_not_older(
    incoming_at: datetime | None,
    incoming_id: int,
    current_at: datetime | None,
    current_id: int | None,
) -> bool:
    """
    ``True`` si la revisión entrante es igual o más nueva que la vigente.

    Orden: ``(bsale_modified_at, id)`` cuando ambos timestamps existen; si falta
    alguno, solo ``id`` (Bsale asigna ids crecientes a cada reemisión). Sin revisión
    vigente conocida, siempre ``True``.
    """
    if current_id is None:
        return True
    if incoming_at is not None and current_at is not None:
        return (incoming_at, int(incoming_id)) >= (current_at, int(current_id))
    return int(incoming_id) >= int(current_id)


@dataclass(frozen=True)
class DocumentSourceEvidence:
    """Evidencia persistida en ``distribuidora.documents`` para elegir la revisión Bsale."""

    local_document_id: int
    folio: int | None = None
    source_document_id: int | None = None
    source_updated_at: datetime | None = None
    raw_data_id: int | None = None
    raw_number: int | None = None
    raw_revision_at: datetime | None = None


def _raw_candidate_eligible(ev: DocumentSourceEvidence) -> bool:
    if ev.raw_data_id is None:
        return False
    if ev.raw_number is not None and ev.raw_number <= 0:
        return False
    if ev.folio is not None and ev.raw_number is not None and ev.raw_number != ev.folio:
        return False
    return True


def resolve_current_source_document_id(ev: DocumentSourceEvidence) -> int:
    """
    Revisión Bsale vigente para hijos de ``ev.local_document_id``.

    * ``raw_data.id`` es candidata solo si su ``number`` es el folio local y > 0.
    * ``source_document_id`` es candidata si es > 0.
    * Si ambas difieren gana la más nueva por ``is_revision_not_older``; nunca se
      prefiere un ``source_document_id`` stale frente a un ``raw_data`` más reciente.
    * Sin candidatas: la PK local (documento sin reemisión conocida).
    """
    raw = ev.raw_data_id if _raw_candidate_eligible(ev) else None
    src = ev.source_document_id
    if raw is not None and src is not None:
        if raw == src:
            return raw
        if is_revision_not_older(ev.raw_revision_at, raw, ev.source_updated_at, src):
            return raw
        return src
    if raw is not None:
        return raw
    if src is not None:
        return src
    return int(ev.local_document_id)
