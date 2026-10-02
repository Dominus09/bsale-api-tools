"""Modelos en memoria de ``bsale_raw`` (sin acceso a BD ni a la API).

Identidad: toda entidad Bsale se identifica por ``(company_id, bsale_id)``. Los ids de
variante, producto, documento, sucursal, etc. NO son globales entre empresas.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class SyncMode(str, Enum):
    WEBHOOK = "WEBHOOK"
    POINT = "POINT"
    INCREMENTAL = "INCREMENTAL"
    FULL_RECONCILE = "FULL_RECONCILE"
    SCANNER = "SCANNER"
    WINDOW_RECONCILE = "WINDOW_RECONCILE"


class RunStatus(str, Enum):
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    PARTIAL = "PARTIAL"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"


class WebhookStatus(str, Enum):
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    DONE = "DONE"
    RETRY = "RETRY"
    FAILED_FINAL = "FAILED_FINAL"
    COALESCED = "COALESCED"  # absorbido por otro evento en cola con el mismo refresh_key


class ResponseEnvelope(str, Enum):
    """Forma de la respuesta del GET exacto de un webhook (evidencia en webhook_resource_responses)."""

    V2_CODE_DATA = "V2_CODE_DATA"
    OTHER = "OTHER"
    NO_JSON = "NO_JSON"
    NETWORK_ERROR = "NETWORK_ERROR"


class DocumentChangeKind(str, Enum):
    """Tipo de fila en document_change_log. Facturación/anulación se leen de state, no se codifican aquí."""

    CREATED = "CREATED"
    MODIFIED = "MODIFIED"


def payload_hash(payload: Any) -> str:
    """SHA-256 del JSON canónico (claves ordenadas, sin espacios). Detecta cambios reales."""
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def relation_id(payload: dict[str, Any], key: str) -> int | None:
    """Id de un nodo relación Bsale (``{"href": ..., "id": "12"}``). Bsale entrega ids como string."""
    node = payload.get(key)
    if not isinstance(node, dict) or node.get("id") in (None, ""):
        return None
    return int(node["id"])


@dataclass(frozen=True)
class RawRecord:
    """Entidad normal: espejo fiel del payload; columnas de búsqueda derivadas sin reglas de negocio."""

    company_id: int
    bsale_id: int
    payload: dict[str, Any] = field(repr=False)
    search: dict[str, Any] = field(default_factory=dict)

    @property
    def key(self) -> tuple[int, int]:
        return (self.company_id, self.bsale_id)

    @property
    def payload_hash(self) -> str:
        return payload_hash(self.payload)

    @property
    def state(self) -> int | None:
        value = self.payload.get("state")
        return None if value is None else int(value)


@dataclass(frozen=True)
class StockRecord:
    """Estado ACTUAL de stock. Clave operativa ``(company_id, variant_id, office_id)``."""

    company_id: int
    variant_id: int
    office_id: int
    stock_id: int | None
    quantity: float | None
    quantity_reserved: float | None
    quantity_available: float | None
    payload: dict[str, Any] = field(repr=False)

    @property
    def key(self) -> tuple[int, int, int]:
        return (self.company_id, self.variant_id, self.office_id)

    @property
    def payload_hash(self) -> str:
        return payload_hash(self.payload)

    @classmethod
    def from_payload(cls, company_id: int, payload: dict[str, Any]) -> "StockRecord":
        variant_id = relation_id(payload, "variant")
        office_id = relation_id(payload, "office")
        if variant_id is None or office_id is None:
            raise ValueError("stock sin variant.id u office.id")

        def _num(key: str) -> float | None:
            value = payload.get(key)
            return None if value is None else float(value)

        return cls(
            company_id=company_id,
            variant_id=variant_id,
            office_id=office_id,
            stock_id=int(payload["id"]) if payload.get("id") is not None else None,
            quantity=_num("quantity"),
            quantity_reserved=_num("quantityReserved"),
            quantity_available=_num("quantityAvailable"),
            payload=payload,
        )
