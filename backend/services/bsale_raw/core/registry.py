"""Registro central de recursos ``bsale_raw``.

Cada recurso se declara una sola vez como ``ResourceSpec`` (datos, no código) en
``backend/services/bsale_raw/resources/*``. El motor único recorre el registro; no existe un
script por endpoint. La fuente de verdad de cada campo es ``docs/BSALE_RAW_ENDPOINT_MATRIX.md``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Any, Callable

from backend.services.bsale_raw.core.models import SyncMode
from backend.services.bsale_raw.core.rate_limit import RequestPriority


# Scopes canónicos de sync_state / sync_cursors (TEXT sin CHECK en SQL). Nuevos scopes se
# definen sólo aquí.
GLOBAL_SCOPE = "global"


def office_scope(office_id: int) -> str:
    return f"office:{int(office_id)}"


def price_list_scope(price_list_id: int) -> str:
    return f"price_list:{int(price_list_id)}"


def document_type_scope(document_type_id: int) -> str:
    return f"document_type:{int(document_type_id)}"


# POINT (refresh dirigido de stock). El scope detallado va SÓLO a sync_entity_runs (historial por
# corrida); sync_state guarda una única fila agregada POINT_STATE_SCOPE por empresa/recurso, para no
# crear una fila por variante y no marcar como fresca una sucursal entera por refrescar una variante.
POINT_STATE_SCOPE = "point"


def variant_scope(variant_id: int, office_id: int | None = None) -> str:
    base = f"variant:{int(variant_id)}"
    return base if office_id is None else f"{base}:office:{int(office_id)}"


def point_scope(variant_ids: list[int], office_id: int | None = None) -> str:
    """Una variante → ``variant:<v>[:office:<o>]``; varias → ``variants:<n>[:office:<o>]`` (lista en el summary)."""
    if len(variant_ids) == 1:
        return variant_scope(variant_ids[0], office_id)
    base = f"variants:{len(variant_ids)}"
    return base if office_id is None else f"{base}:office:{int(office_id)}"


class Priority(str, Enum):
    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


class KeyKind(str, Enum):
    ENTITY = "ENTITY"  # (company_id, bsale_id)
    STOCK = "STOCK"  # (company_id, variant_id, office_id)
    CHILD = "CHILD"  # (company_id, bsale_id) del hijo + id del padre como columna


def optional_int(value: Any) -> int | None:
    """Entero Bsale (int o string numérico). Cualquier otro valor invalida el snapshot."""
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip().lstrip("-").isdigit():
        return int(value.strip())
    raise ValueError(f"valor no entero: {type(value).__name__}")


def optional_text(value: Any) -> str | None:
    return None if value is None else str(value)


def optional_numeric(value: Any) -> Decimal | None:
    """Número Bsale (int, float o string numérico, p. ej. ``"19.0"``) como ``Decimal`` exacto."""
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        raise ValueError("valor no numérico: bool")
    if isinstance(value, (int, float, str)):
        try:
            number = Decimal(str(value).strip())
        except InvalidOperation:
            raise ValueError(f"valor no numérico: {type(value).__name__}") from None
        if number.is_finite():
            return number
    raise ValueError(f"valor no numérico: {type(value).__name__}")


def optional_relation_id(value: Any) -> int | None:
    """Id de un nodo relación Bsale (``{"href": ..., "id": "1"}``); ausente → None."""
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ValueError(f"relación no es objeto: {type(value).__name__}")
    return optional_int(value.get("id"))


@dataclass(frozen=True)
class TypedColumn:
    """Columna de búsqueda derivada del payload (el payload se guarda siempre completo, sin cambios)."""

    column: str
    payload_key: str
    convert: Callable[[Any], Any]

    def extract(self, payload: dict[str, Any]) -> Any:
        return self.convert(payload.get(self.payload_key))


@dataclass(frozen=True)
class ResourceSpec:
    name: str
    list_endpoint: str
    item_endpoint: str | None
    raw_table: str
    key_kind: KeyKind
    priority: Priority
    request_priority: RequestPriority
    parent: str | None = None
    webhook_topics: tuple[str, ...] = ()
    # El filtro `state` existe, pero el full scan normal va SIN `state` (devuelve activos e
    # inactivos, observado en fase 2); `state=0/1` queda sólo para auditoría.
    state_filter: bool = False
    incremental_filter: str | None = None
    full_reconcile: bool = True
    full_scan_global_allowed: bool = True
    reconcile_window_days: int | None = None
    partition_by_office: bool = False
    point_filters: tuple[str, ...] = ()
    expand: tuple[str, ...] = ()
    freshness_sla_seconds: int | None = None
    needs_live_verification: tuple[str, ...] = field(default_factory=tuple)
    typed_columns: tuple[TypedColumn, ...] = ()
    # Habilitación explícita y gradual del motor productivo (fase 4A: sólo offices).
    pipeline_enabled: bool = False
    # Modos que el motor acepta para el recurso (deben existir en los CHECK de sync_runs).
    pipeline_modes: tuple[SyncMode, ...] = (SyncMode.FULL_RECONCILE,)


class ResourceRegistry:
    def __init__(self) -> None:
        self._specs: dict[str, ResourceSpec] = {}

    def register(self, spec: ResourceSpec) -> ResourceSpec:
        if spec.name in self._specs:
            raise ValueError(f"recurso duplicado en registry: {spec.name}")
        if not spec.full_scan_global_allowed and spec.reconcile_window_days is None:
            raise ValueError(f"{spec.name}: sin full scan global requiere reconcile_window_days")
        if spec.parent is not None and spec.parent not in self._specs:
            raise ValueError(f"{spec.name}: padre {spec.parent} no registrado antes")
        self._specs[spec.name] = spec
        return spec

    def get(self, name: str) -> ResourceSpec:
        return self._specs[name]

    def all(self) -> list[ResourceSpec]:
        return list(self._specs.values())

    def by_webhook_topic(self, topic: str) -> list[ResourceSpec]:
        return [s for s in self._specs.values() if topic in s.webhook_topics]

    def names(self) -> list[str]:
        return list(self._specs)

    def pipeline_names(self) -> list[str]:
        return [name for name, spec in self._specs.items() if spec.pipeline_enabled]


REGISTRY = ResourceRegistry()
