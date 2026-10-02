"""Registro central de recursos ``bsale_raw``.

Cada recurso se declara una sola vez como ``ResourceSpec`` (datos, no código) en
``backend/services/bsale_raw/resources/*``. El motor único recorre el registro; no existe un
script por endpoint. La fuente de verdad de cada campo es ``docs/BSALE_RAW_ENDPOINT_MATRIX.md``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


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
    parent: str | None = None
    webhook_topics: tuple[str, ...] = ()
    state_filter: bool = False
    incremental_filter: str | None = None
    full_reconcile: bool = True
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
