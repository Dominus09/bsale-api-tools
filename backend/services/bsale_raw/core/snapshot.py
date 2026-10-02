"""Fetch completo y validado de un listado Bsale + filas RAW en memoria (sin BD).

Paginación estricta sobre ``BsaleHttpClient.get_json`` (reintentos 408/425/429/5xx, Retry-After,
validación de host y token fuera de mensajes los aporta el cliente). ``fetch_all_items`` del
cliente compartido no valida ``count`` ni expone cuántas páginas leyó, por eso no se usa aquí.

``api_fetched_at`` = instante en que llegó la respuesta HTTP de la página que contenía el ítem:
todos los ítems de una página comparten el mismo valor.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable

from backend.services.bsale_raw.core.models import payload_hash
from backend.services.bsale_raw.core.registry import ResourceSpec

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
    api_count: int
    pages: int


def fetch_snapshot(
    client: Any,
    endpoint: str,
    *,
    params: dict[str, Any] | None = None,
    limit: int = BSALE_PAGE_LIMIT,
    max_pages: int = DEFAULT_MAX_PAGES,
    clock: Clock = utc_now,
) -> Snapshot:
    """
    Descarga TODAS las páginas y exige coherencia con ``count``:

    - ``count`` entero en cada página e igual en todas (si cambia durante el barrido, se rechaza);
    - ``items`` lista de objetos;
    - página vacía antes de alcanzar ``count`` = truncado;
    - total recibido == ``count`` (ni más ni menos).
    """
    base = dict(params or {})
    items: list[FetchedItem] = []
    api_count: int | None = None
    offset = 0
    pages = 0
    while True:
        if pages >= max_pages:
            raise SnapshotValidationError(f"{endpoint}: se excedió max_pages={max_pages}")
        data = client.get_json(endpoint, {**base, "limit": limit, "offset": offset})
        fetched_at = clock()
        pages += 1

        count = data.get("count")
        if isinstance(count, bool) or not isinstance(count, int) or count < 0:
            raise SnapshotValidationError(f"{endpoint}: 'count' ausente o inválido en offset {offset}")
        if api_count is None:
            api_count = count
        elif count != api_count:
            raise SnapshotValidationError(
                f"{endpoint}: count cambió durante el snapshot ({api_count} -> {count})"
            )

        page = data.get("items")
        if not isinstance(page, list):
            raise SnapshotValidationError(f"{endpoint}: 'items' ausente o no es lista en offset {offset}")
        if any(not isinstance(it, dict) for it in page):
            raise SnapshotValidationError(f"{endpoint}: item no es objeto en offset {offset}")
        items.extend(FetchedItem(it, fetched_at) for it in page)

        if len(items) >= api_count:
            break
        if not page:
            raise SnapshotValidationError(
                f"{endpoint}: respuesta truncada ({len(items)} de {api_count} ítems)"
            )
        offset += len(page)

    if len(items) != api_count:
        raise SnapshotValidationError(f"{endpoint}: recibidos {len(items)} ítems, count={api_count}")
    return Snapshot(items=items, api_count=api_count, pages=pages)


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
