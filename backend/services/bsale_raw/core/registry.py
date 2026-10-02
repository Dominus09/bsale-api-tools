"""Registro central de recursos ``bsale_raw``.

Cada recurso se declara una sola vez como ``ResourceSpec`` (datos, no código) en
``backend/services/bsale_raw/resources/*``. El motor único recorre el registro; no existe un
script por endpoint. La fuente de verdad de cada campo es ``docs/BSALE_RAW_ENDPOINT_MATRIX.md``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

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


class Priority(str, Enum):
    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


class KeyKind(str, Enum):
    ENTITY = "ENTITY"  # (company_id, bsale_id)
    STOCK = "STOCK"  # (company_id, variant_id, office_id)
    CHILD = "CHILD"  # (company_id, bsale_id) del hijo + id del padre como columna


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


REGISTRY = ResourceRegistry()
