"""Adaptadores de fuente documental — contrato sin I/O pesado (Etapa 1).

company_id=3 / office_id=1 usará DistribuidoraLiveDocumentSource en Etapa 2+.
Offices 3/4/5 podrán registrar otro adaptador sin duplicar fórmulas.
"""

from __future__ import annotations

from datetime import date
from typing import Protocol, runtime_checkable

from backend.services.analytics.schemas import DocumentHeader, DocumentLine


# Tipos de documento canónicos (distribuidora / Bsale Quillotana).
DOC_TYPE_BOLETA = 1
DOC_TYPE_FACTURA = 6
DOC_TYPE_CREDIT_NOTE = 9

SALE_DOCUMENT_TYPES: frozenset[int] = frozenset({DOC_TYPE_BOLETA, DOC_TYPE_FACTURA})
CREDIT_NOTE_DOCUMENT_TYPES: frozenset[int] = frozenset({DOC_TYPE_CREDIT_NOTE})


@runtime_checkable
class DocumentSource(Protocol):
    """Interfaz de lectura analítica. Implementaciones reales en etapas posteriores."""

    source_name: str

    def list_headers(
        self,
        *,
        company_id: int,
        office_id: int | None,
        date_from: date,
        date_to: date,
        active_only: bool = True,
    ) -> list[DocumentHeader]:
        """Lista encabezados en el rango. No implementado en Etapa 1."""
        ...

    def list_lines(
        self,
        *,
        company_id: int,
        office_id: int | None,
        date_from: date,
        date_to: date,
        active_only: bool = True,
    ) -> list[DocumentLine]:
        """Lista líneas en el rango. No implementado en Etapa 1."""
        ...


class UnboundDocumentSource:
    """Stub registrado: permite cablear el motor sin tocar PG todavía."""

    source_name = "unbound"

    def list_headers(
        self,
        *,
        company_id: int,
        office_id: int | None,
        date_from: date,
        date_to: date,
        active_only: bool = True,
    ) -> list[DocumentHeader]:
        raise NotImplementedError(
            "DocumentSource.list_headers: conectar adaptador distribuidora "
            "en Etapa 2 (sin queries en Etapa 1)."
        )

    def list_lines(
        self,
        *,
        company_id: int,
        office_id: int | None,
        date_from: date,
        date_to: date,
        active_only: bool = True,
    ) -> list[DocumentLine]:
        raise NotImplementedError(
            "DocumentSource.list_lines: conectar adaptador distribuidora "
            "en Etapa 2 (sin queries en Etapa 1)."
        )


def classify_document_type(document_type_id: int) -> str:
    """Clasificación liviana sin DB. Preferir AnalyticsDocumentKind en Etapa 2+."""
    if document_type_id in SALE_DOCUMENT_TYPES:
        return "sale"
    if document_type_id in CREDIT_NOTE_DOCUMENT_TYPES:
        return "credit_note"
    return "unsupported"
