"""Clasificación de recepciones Bsale por tipo de movimiento.

Fuente: ``bsale_raw.stock_receptions.document`` (etiqueta libre de Bsale) e ``internal_dispatch_id``.
Etiquetas observadas en company 3 (2026): "Factura", "GUÍA DE DESPACHO ELECTRÓNICA",
"NOTA DE CRÉDITO ELECTRÓNICA T", "Sin Documento", "Guía",
"GUÍA DE DESPACHO ELECTRÓNICA <n> Ingreso por despacho anulado" y NULL.

Una sola tabla de reglas genera la función Python y el CASE SQL (mismo orden, mismo resultado).
Solo factura y guía de despacho son candidatas a costo comercial; nada se asume compra real.
"""

from __future__ import annotations

import unicodedata
from dataclasses import dataclass

from backend.services.analytics_v2.contracts import COMPARABLE_MOVEMENTS, MovementClass

# (texto contenido en la etiqueta normalizada, clase, motivo). El orden es la prioridad.
DOCUMENT_RULES: tuple[tuple[str, MovementClass, str], ...] = (
    ("DESPACHO ANULADO", MovementClass.AJUSTE, "Reingreso por despacho anulado"),
    ("NOTA DE CREDITO", MovementClass.NOTA_CREDITO, "Nota de crédito"),
    ("AJUSTE", MovementClass.AJUSTE, "Ajuste de inventario"),
    ("FACTURA", MovementClass.FACTURA, "Factura"),
    ("GUIA", MovementClass.GUIA_DESPACHO, "Guía de despacho"),
    ("SIN DOCUMENTO", MovementClass.SIN_DOCUMENTO, "Recepción sin documento"),
)

_SQL_ACCENTS_FROM = "áéíóúüÁÉÍÓÚÜ"
_SQL_ACCENTS_TO = "aeiouuAEIOUU"


@dataclass(frozen=True, slots=True)
class MovementResult:
    movement_class: MovementClass
    reason: str

    @property
    def comparable(self) -> bool:
        return self.movement_class in COMPARABLE_MOVEMENTS


def normalize_label(value: str | None) -> str:
    if not value:
        return ""
    decomposed = unicodedata.normalize("NFD", value)
    stripped = "".join(ch for ch in decomposed if unicodedata.category(ch) != "Mn")
    return " ".join(stripped.upper().split())


def classify_movement(document: str | None, internal_dispatch_id: int | None) -> MovementResult:
    if internal_dispatch_id not in (None, 0):
        return MovementResult(MovementClass.TRANSFERENCIA_INTERNA, "Despacho interno Bsale")
    label = normalize_label(document)
    if not label:
        return MovementResult(MovementClass.NO_DETERMINADO, "Recepción sin etiqueta de documento")
    for token, movement_class, reason in DOCUMENT_RULES:
        if token in label:
            return MovementResult(movement_class, reason)
    return MovementResult(MovementClass.NO_DETERMINADO, "Etiqueta de documento no reconocida")


def movement_class_sql(document_expr: str, internal_dispatch_expr: str) -> str:
    """CASE SQL equivalente a ``classify_movement`` (expresiones de columnas, sin parámetros)."""
    normalized = (
        f"upper(regexp_replace(translate(coalesce({document_expr}, ''), "
        f"'{_SQL_ACCENTS_FROM}', '{_SQL_ACCENTS_TO}'), '\\s+', ' ', 'g'))"
    )
    branches = [
        f"WHEN coalesce({internal_dispatch_expr}, 0) <> 0 THEN '{MovementClass.TRANSFERENCIA_INTERNA.value}'",
        f"WHEN btrim({normalized}) = '' THEN '{MovementClass.NO_DETERMINADO.value}'",
    ]
    for token, movement_class, _reason in DOCUMENT_RULES:
        branches.append(
            f"WHEN strpos({normalized}, '{token}') > 0 THEN '{movement_class.value}'"
        )
    body = "\n        ".join(branches)
    return f"CASE\n        {body}\n        ELSE '{MovementClass.NO_DETERMINADO.value}'\n    END"


def comparable_sql_list() -> str:
    return ", ".join(f"'{m.value}'" for m in sorted(COMPARABLE_MOVEMENTS))
