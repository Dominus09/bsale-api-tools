"""Fetch completo y validado de un listado Bsale + filas RAW en memoria (sin BD).

Paginación estricta sobre ``BsaleHttpClient.get_json`` (reintentos 408/425/429/5xx, Retry-After,
validación de host y token fuera de mensajes los aporta el cliente). ``fetch_all_items`` del
cliente compartido no valida ``count`` ni expone cuántas páginas leyó, por eso no se usa aquí.

``api_fetched_at`` = instante en que llegó la respuesta HTTP de la página que contenía el ítem:
todos los ítems de una página comparten el mismo valor.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable

from backend.services.bsale_raw.core.models import payload_hash
from backend.services.bsale_raw.core.registry import ResourceSpec, optional_relation_id

Clock = Callable[[], datetime]

BSALE_PAGE_LIMIT = 50
DEFAULT_MAX_PAGES = 5000


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class SnapshotValidationError(RuntimeError):
    """Respuesta incompleta, inconsistente o inválida: el snapshot no se acepta y no se escribe nada."""


@dataclass(frozen=True)
class FetchedItem:
    payload: dict[str, Any] = field(repr=False)
    fetched_at: datetime


@dataclass(frozen=True)
class Snapshot:
    items: list[FetchedItem]
    api_count: int  # último count observado (== count_first si el snapshot es estricto)
    pages: int
    count_first: int | None = None
    count_tolerance: int = 0


@dataclass(frozen=True)
class CountDrift:
    """Tolerancia de ``count`` para listados naturalmente mutables (stock). ``STRICT`` = catálogo."""

    pct: float = 0.0
    minimum: int = 0

    def tolerance(self, first_count: int) -> int:
        return max(self.minimum, math.ceil(first_count * self.pct / 100.0))


STRICT = CountDrift()


def fetch_snapshot(
    client: Any,
    endpoint: str,
    *,
    params: dict[str, Any] | None = None,
    limit: int = BSALE_PAGE_LIMIT,
    max_pages: int = DEFAULT_MAX_PAGES,
    clock: Clock = utc_now,
    drift: CountDrift = STRICT,
) -> Snapshot:
    """
    Descarga TODAS las páginas y exige coherencia con ``count``:

    - ``count`` entero en cada página; ``items`` lista de objetos;
    - estricto (default): ``count`` igual en todas las páginas y total recibido == ``count``;
    - con ``drift``: ``count`` puede variar a lo sumo ``tolerance`` respecto del primero, y el total
      recibido debe quedar a lo sumo ``tolerance`` del último ``count`` observado;
    - página vacía antes de alcanzar ``count - tolerance`` = truncado.
    """
    base = dict(params or {})
    items: list[FetchedItem] = []
    first: int | None = None
    last = 0
    tolerance = 0
    offset = 0
    pages = 0
    while True:
        if pages >= max_pages:
            raise SnapshotValidationError(f"{endpoint}: se excedió max_pages={max_pages}")
        data = client.get_json(endpoint, {**base, "limit": limit, "offset": offset})
        fetched_at = clock()
        pages += 1

        if not isinstance(data, dict):
            raise SnapshotValidationError(f"{endpoint}: respuesta no es objeto en offset {offset}")
        count = data.get("count")
        if isinstance(count, bool) or not isinstance(count, int) or count < 0:
            raise SnapshotValidationError(f"{endpoint}: 'count' ausente o inválido en offset {offset}")
        if first is None:
            first = count
            tolerance = drift.tolerance(count)
        elif abs(count - first) > tolerance:
            raise SnapshotValidationError(
                f"{endpoint}: count cambió durante el snapshot ({first} -> {count}; tolerancia {tolerance})"
            )
        last = count

        page = data.get("items")
        if not isinstance(page, list):
            raise SnapshotValidationError(f"{endpoint}: 'items' ausente o no es lista en offset {offset}")
        if any(not isinstance(it, dict) for it in page):
            raise SnapshotValidationError(f"{endpoint}: item no es objeto en offset {offset}")
        items.extend(FetchedItem(it, fetched_at) for it in page)

        if len(items) >= last:
            break
        if not page:
            if len(items) >= last - tolerance:
                break
            raise SnapshotValidationError(
                f"{endpoint}: respuesta truncada ({len(items)} de {last} ítems)"
            )
        offset += len(page)

    if abs(len(items) - last) > tolerance:
        raise SnapshotValidationError(f"{endpoint}: recibidos {len(items)} ítems, count={last}")
    return Snapshot(items=items, api_count=last, pages=pages, count_first=first, count_tolerance=tolerance)


@dataclass(frozen=True)
class RawRow:
    company_id: int
    bsale_id: int
    typed: dict[str, Any]
    payload: dict[str, Any] = field(repr=False)
    payload_hash: str
    api_fetched_at: datetime


def _bsale_id(payload: dict[str, Any]) -> int:
    value = payload.get("id")
    if isinstance(value, bool):
        raise SnapshotValidationError("id inválido (bool)")
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip().isdigit():
        return int(value.strip())
    raise SnapshotValidationError("item sin id numérico")


def build_rows(spec: ResourceSpec, company_id: int, snapshot: Snapshot) -> list[RawRow]:
    """Una fila por ítem. Cualquier id duplicado invalida el snapshot (no se elige uno arbitrariamente)."""
    rows: list[RawRow] = []
    seen: dict[int, str] = {}
    duplicates: list[int] = []
    for item in snapshot.items:
        bsale_id = _bsale_id(item.payload)
        digest = payload_hash(item.payload)
        if bsale_id in seen:
            duplicates.append(bsale_id)
            continue
        seen[bsale_id] = digest
        try:
            typed = {col.column: col.extract(item.payload) for col in spec.typed_columns}
        except ValueError as exc:
            raise SnapshotValidationError(f"{spec.name} id={bsale_id}: {exc}") from exc
        rows.append(
            RawRow(
                company_id=company_id,
                bsale_id=bsale_id,
                typed=typed,
                payload=item.payload,
                payload_hash=digest,
                api_fetched_at=item.fetched_at,
            )
        )
    if duplicates:
        sample = sorted(set(duplicates))[:5]
        raise SnapshotValidationError(
            f"{spec.name}: {len(duplicates)} ids duplicados en el snapshot (p. ej. {sample}); "
            "posible desplazamiento de páginas"
        )
    return rows


@dataclass(frozen=True)
class StockRow:
    """Fila de stock: identidad ``(company_id, variant_id, office_id)``; ``bsale_stock_id`` es sólo dato."""

    company_id: int
    variant_id: int
    office_id: int
    typed: dict[str, Any]
    payload: dict[str, Any] = field(repr=False)
    payload_hash: str
    api_fetched_at: datetime

    @property
    def key(self) -> tuple[int, int]:
        return (self.variant_id, self.office_id)


def stock_key(row: StockRow) -> tuple[int, int]:
    return row.key


def _required_relation(payload: dict[str, Any], name: str) -> int:
    try:
        value = optional_relation_id(payload.get(name))
    except ValueError as exc:
        raise SnapshotValidationError(f"stock con {name} inválido: {exc}") from exc
    if value is None:
        raise SnapshotValidationError(f"stock sin {name}.id")
    return value


def build_stock_rows(
    spec: ResourceSpec, company_id: int, snapshot: Snapshot, *, office_id: int | None = None
) -> list[StockRow]:
    """
    Una fila por ítem. Invalida el snapshot completo: ``variant.id`` / ``office.id`` ausentes o no
    numéricos, sucursal distinta de ``office_id`` (filtro ignorado), clave repetida o cantidad no
    numérica. Las cantidades se guardan tal cual (``0`` explícito = 0; ausente = NULL; nunca se
    recalculan ni se fabrican).
    """
    rows: list[StockRow] = []
    seen: set[tuple[int, int]] = set()
    duplicates: list[tuple[int, int]] = []
    wrong_office: set[int] = set()
    for item in snapshot.items:
        variant_id = _required_relation(item.payload, "variant")
        item_office = _required_relation(item.payload, "office")
        if office_id is not None and item_office != office_id:
            wrong_office.add(item_office)
            continue
        key = (variant_id, item_office)
        if key in seen:
            duplicates.append(key)
            continue
        seen.add(key)
        try:
            typed = {col.column: col.extract(item.payload) for col in spec.typed_columns}
        except ValueError as exc:
            raise SnapshotValidationError(
                f"{spec.name} variant={variant_id} office={item_office}: {exc}"
            ) from exc
        rows.append(
            StockRow(
                company_id=company_id,
                variant_id=variant_id,
                office_id=item_office,
                typed=typed,
                payload=item.payload,
                payload_hash=payload_hash(item.payload),
                api_fetched_at=item.fetched_at,
            )
        )
    if wrong_office:
        raise SnapshotValidationError(
            f"{spec.name}: filas de sucursales {sorted(wrong_office)[:5]} en un snapshot de office={office_id}"
        )
    if duplicates:
        raise SnapshotValidationError(
            f"{spec.name}: {len(duplicates)} claves (variant, office) duplicadas en el snapshot "
            f"(p. ej. {sorted(set(duplicates))[:5]}); posible desplazamiento de páginas"
        )
    return rows
