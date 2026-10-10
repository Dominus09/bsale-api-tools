"""Motor ``bsale_raw.product_taxes`` (clave ``(company_id, product_id)``).

Fuentes (``source``):

- ``expand`` (default, LIVE VALIDATED C3: probe ``EXPAND_COMPLETE``): snapshot estricto de
  ``GET /v1/products.json?expand=[product_taxes]`` (activos e inactivos, ~1 request cada 50 productos).
  Identidad del producto y relación salen del MISMO snapshot. Cada nodo ``product_taxes`` se valida
  (objeto, ``items`` lista, ``count`` entero == ``len(items)``, sin ``next`` ni ``offset`` > 0, ítems del
  mismo producto con ``tax.id``, sin ítems repetidos). Un nodo ausente, sin expandir, incompleto, con
  paginación interna o inválido NO es "cero impuestos": ese producto pasa al endpoint individual, con
  tope ``MAX_INDIVIDUAL_FALLBACK`` por corrida (el resto queda "no consultado", PARTIAL).
- ``individual``: ``GET /v1/products/{id}/product_taxes.json`` por cada producto vigente de
  ``bsale_raw.products`` (un request por producto). Alternativa explícita si ``expand`` deja de servir.

El paso ``products`` del catálogo sigue leyendo el listado SIN ``expand``: compartir el snapshot
expandido cambiaría el payload (y el hash) que ``bsale_raw.products`` guarda hoy. El costo es un segundo
listado (~41 requests en C3) en vez de ~2.000 consultas individuales.

RAW guarda EXCLUSIVAMENTE lo que entrega Bsale: ``payload`` es un array JSON con lo recibido tal cual —
el nodo ``product_taxes`` expandido (``last_source = 'FULL_RECONCILE'``) o las páginas del endpoint
individual (``last_source = 'POINT'``) —, los ``tax.id`` en su orden (``tax_ids``) y la cantidad de ítems.
Nunca calcula IVA, ILA, factores ni montos.

Estados por producto:

- consultado sin impuestos: fila con ``items_count = 0`` (``count = 0`` y ``items`` vacía explícitos);
- consultado con impuestos: fila con ``tax_ids``;
- no consultado: sin fila;
- consulta fallida: entrada en ``sync_cursors`` (``product_taxes`` / ``global`` / ``failures``); la fila
  anterior, si existe, queda intacta. Un 404 o cualquier error NUNCA se guarda como lista vacía.

Orden: fuente → lock ``(company, product_taxes, global)`` → impuestos / relaciones vigentes → run →
HTTP SIN transacción → cada ``WRITE_BATCH`` relaciones una transacción corta (UPSERT con frescura) →
transacción final (ausencias con fusible + fallas + run/state). Si el snapshot expandido falla (HTTP,
truncado, ``count`` inestable, ids repetidos) no se escribe ninguna relación ni se marca ausencia.

Cuota: limitador propio conservador (``BSALE_RAW_PRODUCT_TAX_RPS``, 1 rps por defecto, máximo 2) y
corte ante 429 sostenidos o errores consecutivos: los limitadores de otros procesos (stock, costos,
precios, legacy) no se coordinan con éste.
"""

from __future__ import annotations

import logging
import os
import time
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Iterator, Protocol

from backend.services.bsale.companies import BsaleCompany
from backend.services.bsale.http_client import BsaleHttpError
from backend.services.bsale_raw.core.client import build_company_client
from backend.services.bsale_raw.core.engine import (
    TRIGGER_MANUAL,
    UnsupportedSyncError,
    _collect_request_stats,
    _fail,
    api_path,
    read_token,
    sanitize_error,
)
from backend.services.bsale_raw.core.models import RunStatus, SyncMode, payload_hash
from backend.services.bsale_raw.core.rate_limit import (
    CompanyRateLimiters,
    PriorityRateLimiter,
    RateLimitConfig,
    TokenBucket,
)
from backend.services.bsale_raw.core.reconcile import ExistingRow, max_missing_pct
from backend.services.bsale_raw.core.registry import GLOBAL_SCOPE, REGISTRY, ResourceSpec, optional_relation_id
from backend.services.bsale_raw.core.snapshot import (
    BSALE_PAGE_LIMIT,
    Clock,
    FetchedItem,
    Snapshot,
    SnapshotValidationError,
    _bsale_id,
    fetch_snapshot,
    utc_now,
)
from backend.services.bsale_raw.core.store import (
    EntityOutcome,
    LockBusyError,
    PgRawStore,
    PgRawTx,
    RunHandle,
    SourceConfig,
)

logger = logging.getLogger(__name__)

RESOURCE = "product_taxes"
FAILURES_CURSOR = "failures"
DEFAULT_TAX_RPS = 1.0
MAX_TAX_RPS = 2.0
WRITE_BATCH = 200
MAX_CONSECUTIVE_FAILURES = 10
MAX_HTTP_429 = 5
MAX_SAMPLE = 50
MAX_INDIVIDUAL_FALLBACK = 100

SOURCE_EXPAND = "expand"
SOURCE_INDIVIDUAL = "individual"
SOURCES = (SOURCE_EXPAND, SOURCE_INDIVIDUAL)
PRODUCTS_ENDPOINT = "/v1/products.json"
EXPAND_PARAMS = {"expand": "[product_taxes]"}
LAST_SOURCE_EXPAND = SyncMode.FULL_RECONCILE.value
LAST_SOURCE_INDIVIDUAL = SyncMode.POINT.value

TaxClientFactory = Callable[[SourceConfig, str, ResourceSpec], Any]


class ProductTaxPayloadError(ValueError):
    """Respuesta con forma inesperada: el producto no se escribe (queda como falla)."""


def product_tax_spec() -> ResourceSpec:
    import backend.services.bsale_raw.resources  # noqa: F401  (registra los recursos)

    return REGISTRY.get(RESOURCE)


# --- ritmo propio ---------------------------------------------------------------------------------


def product_tax_rate_config(getenv: Callable[[str], str | None] = os.getenv) -> RateLimitConfig:
    raw = (getenv("BSALE_RAW_PRODUCT_TAX_RPS") or "").strip()
    rps = float(raw) if raw else DEFAULT_TAX_RPS
    if not 0 < rps <= MAX_TAX_RPS:
        raise ValueError(f"BSALE_RAW_PRODUCT_TAX_RPS inválido: {rps} (rango (0, {MAX_TAX_RPS}])")
    return RateLimitConfig(requests_per_second=rps, burst=1)


_TAX_LIMITERS = CompanyRateLimiters(lambda cid: PriorityRateLimiter(TokenBucket(product_tax_rate_config())))


def default_tax_client_factory(source: SourceConfig, token: str, spec: ResourceSpec) -> Any:
    company = BsaleCompany(company_id=source.company_id, name=source.name, token_env=source.token_env, token=token)
    return build_company_client(company, _TAX_LIMITERS, spec.request_priority)


# --- filas ----------------------------------------------------------------------------------------


@dataclass(frozen=True)
class ProductTaxRow:
    company_id: int
    product_id: int
    tax_ids: tuple[int, ...]
    items_count: int
    payload: list[Any] = field(repr=False)
    payload_hash: str
    api_fetched_at: datetime
    last_source: str = LAST_SOURCE_INDIVIDUAL


class _RecordingClient:
    """Guarda cada página tal cual llega, para persistir el payload original completo."""

    def __init__(self, client: Any) -> None:
        self._client = client
        self.pages: list[Any] = []

    def get_json(self, endpoint: str, params: dict[str, Any] | None = None) -> Any:
        body = self._client.get_json(endpoint, params)
        self.pages.append(body)
        return body


def build_product_tax_row(
    company_id: int,
    product_id: int,
    items: list[dict[str, Any]],
    pages: list[Any],
    fetched_at: datetime,
    last_source: str = LAST_SOURCE_INDIVIDUAL,
) -> ProductTaxRow:
    tax_ids: list[int] = []
    item_ids: set[Any] = set()
    for item in items:
        if not isinstance(item, dict):
            raise ProductTaxPayloadError("ítem product_tax no es objeto")
        item_id = item.get("id")
        if item_id is not None:
            if str(item_id) in item_ids:
                raise ProductTaxPayloadError(f"ítem product_tax repetido id={item_id}")
            item_ids.add(str(item_id))
        try:
            owner = optional_relation_id(item.get("product"))
            tax_id = optional_relation_id(item.get("tax"))
        except ValueError as exc:
            raise ProductTaxPayloadError(f"product/tax inválido: {exc}") from None
        if owner is not None and owner != product_id:
            raise ProductTaxPayloadError(f"ítem de otro producto ({owner})")
        if tax_id is None:
            raise ProductTaxPayloadError("ítem sin tax.id")
        tax_ids.append(tax_id)
    return ProductTaxRow(
        company_id=company_id,
        product_id=product_id,
        tax_ids=tuple(tax_ids),
        items_count=len(items),
        payload=pages,
        payload_hash=payload_hash(pages),
        api_fetched_at=fetched_at,
        last_source=last_source,
    )


def fetch_product_taxes(client: Any, spec: ResourceSpec, company_id: int, product_id: int, *, clock: Clock) -> ProductTaxRow:
    """Snapshot estricto (``count`` estable, total exacto) de la relación de UN producto."""
    recorder = _RecordingClient(client)
    snapshot = fetch_snapshot(recorder, api_path(spec.list_endpoint.format(parent_id=product_id)), clock=clock)
    fetched_at = max((i.fetched_at for i in snapshot.items), default=None) or clock()
    return build_product_tax_row(
        company_id, product_id, [i.payload for i in snapshot.items], recorder.pages, fetched_at, LAST_SOURCE_INDIVIDUAL
    )


# --- expand ---------------------------------------------------------------------------------------


def expanded_node_problem(node: Any) -> str | None:
    """``None`` = relación completa en el nodo; si no, el motivo (nunca se interpreta como lista vacía)."""
    if node is None:
        return "producto sin product_taxes"
    if not isinstance(node, dict):
        return "product_taxes no es objeto"
    items = node.get("items")
    if not isinstance(items, list):
        return "relación no expandida (sin items)"
    count = node.get("count")
    if isinstance(count, bool) or not isinstance(count, int) or count < 0:
        return "count ausente o inválido"
    if node.get("next"):
        return "paginación interna (next)"
    offset = node.get("offset")
    if offset not in (None, 0):
        return f"offset {offset} distinto de 0"
    if count != len(items):
        return f"relación incompleta ({len(items)} de {count})"
    return None


def fetch_expanded_listing(client: Any, *, clock: Clock, max_items: int | None = None) -> Snapshot:
    """Listado ``products.json?expand=[product_taxes]``. Sin ``max_items``: snapshot estricto completo.

    Con ``max_items`` (sólo dry-run con límite): las primeras páginas hasta cubrirlo, con ``count``
    estable; el resultado es parcial y no sirve para ausencias.
    """
    endpoint = api_path(PRODUCTS_ENDPOINT)
    if max_items is None:
        return fetch_snapshot(client, endpoint, params=EXPAND_PARAMS, clock=clock)
    items: list[FetchedItem] = []
    first: int | None = None
    offset = pages = 0
    while len(items) < max_items:
        data = client.get_json(endpoint, {**EXPAND_PARAMS, "limit": BSALE_PAGE_LIMIT, "offset": offset})
        fetched_at = clock()
        pages += 1
        count = data.get("count") if isinstance(data, dict) else None
        page = data.get("items") if isinstance(data, dict) else None
        if isinstance(count, bool) or not isinstance(count, int) or count < 0:
            raise SnapshotValidationError(f"{endpoint}: 'count' ausente o inválido en offset {offset}")
        if not isinstance(page, list) or any(not isinstance(it, dict) for it in page):
            raise SnapshotValidationError(f"{endpoint}: 'items' inválido en offset {offset}")
        if first is None:
            first = count
        elif count != first:
            raise SnapshotValidationError(f"{endpoint}: count cambió durante el snapshot ({first} -> {count})")
        items.extend(FetchedItem(it, fetched_at) for it in page)
        if not page or len(items) >= count:
            break
        offset += len(page)
    return Snapshot(items=items[:max_items], api_count=first or 0, pages=pages, count_first=first)


@dataclass
class ExpandedResult:
    order: list[int] = field(default_factory=list)
    rows: list[ProductTaxRow] = field(default_factory=list)
    fallback: dict[int, str] = field(default_factory=dict)


def collect_expanded(company_id: int, snapshot: Snapshot) -> ExpandedResult:
    """Valida cada producto del snapshot. Identidad inválida o repetida invalida el snapshot completo."""
    out = ExpandedResult()
    seen: set[int] = set()
    duplicates: list[int] = []
    for item in snapshot.items:
        product_id = _bsale_id(item.payload)
        if product_id in seen:
            duplicates.append(product_id)
            continue
        seen.add(product_id)
        out.order.append(product_id)
        node = item.payload.get("product_taxes")
        problem = expanded_node_problem(node)
        if problem is None:
            try:
                out.rows.append(
                    build_product_tax_row(company_id, product_id, node["items"], [node], item.fetched_at, LAST_SOURCE_EXPAND)
                )
            except ProductTaxPayloadError as exc:
                problem = f"relación expandida inválida: {exc}"
        if problem is not None:
            out.fallback[product_id] = problem
    if duplicates:
        raise SnapshotValidationError(
            f"products expand: {len(duplicates)} ids duplicados en el snapshot (p. ej. {sorted(set(duplicates))[:5]}); "
            "posible desplazamiento de páginas"
        )
    return out


# --- HTTP individual ------------------------------------------------------------------------------


@dataclass
class TaxFetch:
    rows: list[ProductTaxRow] = field(default_factory=list)
    failed: dict[int, str] = field(default_factory=dict)
    attempted: list[int] = field(default_factory=list)
    aborted: str | None = None
    fallback: dict[int, str] = field(default_factory=dict)
    fallback_skipped: list[int] = field(default_factory=list)


def _answered(exc: BaseException) -> bool:
    """La API respondió para ESTE producto (404 / forma inválida): no indica caída ni cuota."""
    return isinstance(exc, (ProductTaxPayloadError, SnapshotValidationError)) or (
        isinstance(exc, BsaleHttpError) and exc.status == 404
    )


def _http_429(client: Any) -> int:
    stats = getattr(getattr(client, "session", None), "stats", None)
    return int(getattr(stats, "http_429", 0) or 0)


def fetch_batch(
    client: Any,
    spec: ResourceSpec,
    company_id: int,
    product_ids: list[int],
    result: TaxFetch,
    *,
    clock: Clock,
    secrets: list[str],
    streak: int,
) -> int:
    """GET por producto (sin transacción). Nunca lanza; devuelve la racha de errores no respondidos."""
    for product_id in product_ids:
        if streak >= MAX_CONSECUTIVE_FAILURES:
            result.aborted = f"{MAX_CONSECUTIVE_FAILURES} errores seguidos (caída, token o cuota)"
            break
        if _http_429(client) >= MAX_HTTP_429:
            result.aborted = f"{MAX_HTTP_429} respuestas 429: se detiene para no agotar la cuota compartida"
            break
        result.attempted.append(product_id)
        try:
            result.rows.append(fetch_product_taxes(client, spec, company_id, product_id, clock=clock))
        except Exception as exc:
            result.failed[product_id] = sanitize_error(exc, secrets)
            streak = 0 if _answered(exc) else streak + 1
        else:
            streak = 0
    return streak


# --- fallas persistentes --------------------------------------------------------------------------


def next_failures(
    previous: dict[int, dict[str, Any]], fetched: TaxFetch, present: set[int], now: datetime
) -> dict[int, dict[str, Any]]:
    """Éxito → sale; falla → entra/actualiza; no intentado → se conserva; producto no vigente → sale."""
    ok = {r.product_id for r in fetched.rows}
    out = {pid: dict(v) for pid, v in previous.items() if pid in present and pid not in ok}
    stamp = now.isoformat()
    for product_id, error in fetched.failed.items():
        prev = out.get(product_id, {})
        out[product_id] = {
            "attempts": int(prev.get("attempts", 0)) + 1,
            "first_failed_at": prev.get("first_failed_at", stamp),
            "last_attempt_at": stamp,
            "error": error,
        }
    return out


def failures_value(failures: dict[int, dict[str, Any]]) -> dict[str, Any]:
    return {"products": {str(pid): failures[pid] for pid in sorted(failures)}}


def failures_from_value(value: Any) -> dict[int, dict[str, Any]]:
    products = value.get("products") if isinstance(value, dict) else None
    if not isinstance(products, dict):
        return {}
    return {int(k): dict(v) for k, v in products.items() if str(k).isdigit() and isinstance(v, dict)}


# --- persistencia ---------------------------------------------------------------------------------

TAX_TABLE = "bsale_raw.product_taxes"
TAX_COLUMNS = (
    "company_id", "product_id", "tax_ids", "items_count", "payload", "payload_hash", "first_seen_at",
    "last_seen_at", "last_changed_at", "api_fetched_at", "missing_since", "last_source", "sync_run_id",
)
TAX_TEMPLATE = "(%s, %s, %s::bigint[], %s, %s, %s, now(), now(), now(), %s, NULL, %s, %s)"

UPSERT_PRODUCT_TAXES_SQL = (
    f"INSERT INTO {TAX_TABLE} AS t ({', '.join(TAX_COLUMNS)}) VALUES %s\n"
    "ON CONFLICT (company_id, product_id) DO UPDATE SET\n    "
    + ",\n    ".join([
        "tax_ids = EXCLUDED.tax_ids",
        "items_count = EXCLUDED.items_count",
        "payload = EXCLUDED.payload",
        "payload_hash = EXCLUDED.payload_hash",
        "last_seen_at = EXCLUDED.last_seen_at",
        "last_changed_at = CASE WHEN t.payload_hash IS DISTINCT FROM EXCLUDED.payload_hash "
        "THEN EXCLUDED.last_changed_at ELSE t.last_changed_at END",
        "api_fetched_at = EXCLUDED.api_fetched_at",
        "missing_since = NULL",
        "last_source = EXCLUDED.last_source",
        "sync_run_id = EXCLUDED.sync_run_id",
    ])
    + "\nWHERE t.api_fetched_at <= EXCLUDED.api_fetched_at\nRETURNING product_id"
)

MARK_PRODUCT_TAXES_MISSING_SQL = (
    f"UPDATE {TAX_TABLE} SET missing_since = now() "
    "WHERE company_id = %s AND product_id = ANY(%s) AND missing_since IS NULL AND api_fetched_at <= %s"
)

SELECT_EXISTING_PRODUCT_TAXES_SQL = (
    f"SELECT product_id, payload_hash, api_fetched_at, missing_since FROM {TAX_TABLE} WHERE company_id = %s"
)

SELECT_PRODUCTS_SQL = (
    "SELECT bsale_id FROM bsale_raw.products WHERE company_id = %s AND missing_since IS NULL ORDER BY bsale_id"
)

SELECT_TAXES_SQL = "SELECT bsale_id FROM bsale_raw.taxes WHERE company_id = %s AND missing_since IS NULL"

SELECT_FAILURES_SQL = (
    "SELECT cursor_value FROM bsale_raw.sync_cursors "
    "WHERE company_id = %s AND resource = %s AND scope = %s AND cursor_name = %s"
)

UPSERT_FAILURES_SQL = """
INSERT INTO bsale_raw.sync_cursors AS c
    (company_id, resource, scope, cursor_name, cursor_value, cycle_started_at, updated_at, sync_run_id)
VALUES (%s, %s, %s, %s, %s, %s, now(), %s)
ON CONFLICT (company_id, resource, scope, cursor_name) DO UPDATE SET
    cursor_value = EXCLUDED.cursor_value,
    cycle_started_at = EXCLUDED.cycle_started_at,
    updated_at = now(),
    sync_run_id = EXCLUDED.sync_run_id
"""


def _existing(rows: list[tuple]) -> dict[int, ExistingRow]:
    return {
        int(r[0]): ExistingRow(bsale_id=int(r[0]), payload_hash=r[1], api_fetched_at=r[2], missing_since=r[3])
        for r in rows
    }


class ProductTaxTx(Protocol):
    def upsert_product_taxes(self, rows: list[ProductTaxRow], *, sync_run_id: int) -> set[int]: ...

    def mark_product_taxes_missing(self, company_id: int, product_ids: list[int], snapshot_started_at: datetime) -> int: ...

    def write_failures(
        self, company_id: int, failures: dict[int, dict[str, Any]], *, started_at: datetime, sync_run_id: int
    ) -> None: ...

    def finish_success(self, handle: RunHandle, outcome: EntityOutcome) -> None: ...


class ProductTaxStore(Protocol):
    def resolve_source(self, company_id: int) -> SourceConfig: ...

    def advisory_lock(self, company_id: int, resource: str, scope: str) -> Any: ...

    def start_run(
        self, *, mode: str, trigger: str, host: str | None, company_id: int, resource: str, scope: str,
        state_scope: str | None = None,
    ) -> RunHandle: ...

    def select_products(self, company_id: int) -> list[int]: ...

    def select_taxes(self, company_id: int) -> set[int]: ...

    def read_existing_product_taxes(self, company_id: int) -> dict[int, ExistingRow]: ...

    def read_failures(self, company_id: int) -> dict[int, dict[str, Any]]: ...

    def product_tax_transaction(self) -> Any: ...

    def finish_failed(self, handle: RunHandle, outcome: EntityOutcome) -> None: ...


@dataclass
class PgProductTaxTx(PgRawTx):
    def upsert_product_taxes(self, rows: list[ProductTaxRow], *, sync_run_id: int) -> set[int]:
        if not rows:
            return set()
        from psycopg2.extras import Json, execute_values

        values = [
            (
                r.company_id, r.product_id, list(r.tax_ids), r.items_count, Json(r.payload), r.payload_hash,
                r.api_fetched_at, r.last_source, sync_run_id,
            )
            for r in rows
        ]
        returned = execute_values(
            self.cur, UPSERT_PRODUCT_TAXES_SQL, values, template=TAX_TEMPLATE, page_size=500, fetch=True
        )
        return {int(r[0]) for r in returned}

    def mark_product_taxes_missing(self, company_id: int, product_ids: list[int], snapshot_started_at: datetime) -> int:
        if not product_ids:
            return 0
        self.cur.execute(MARK_PRODUCT_TAXES_MISSING_SQL, (company_id, list(product_ids), snapshot_started_at))
        return int(self.cur.rowcount or 0)

    def write_failures(
        self, company_id: int, failures: dict[int, dict[str, Any]], *, started_at: datetime, sync_run_id: int
    ) -> None:
        from psycopg2.extras import Json

        self.cur.execute(
            UPSERT_FAILURES_SQL,
            (company_id, RESOURCE, GLOBAL_SCOPE, FAILURES_CURSOR, Json(failures_value(failures)), started_at, sync_run_id),
        )


class PgProductTaxStore(PgRawStore):
    def _rows(self, sql: str, params: tuple) -> list[tuple]:
        cur = self._work().cursor()
        try:
            cur.execute(sql, params)
            return cur.fetchall()
        finally:
            cur.close()

    def select_products(self, company_id: int) -> list[int]:
        return [int(r[0]) for r in self._rows(SELECT_PRODUCTS_SQL, (company_id,))]

    def select_taxes(self, company_id: int) -> set[int]:
        return {int(r[0]) for r in self._rows(SELECT_TAXES_SQL, (company_id,))}

    def read_existing_product_taxes(self, company_id: int) -> dict[int, ExistingRow]:
        return _existing(self._rows(SELECT_EXISTING_PRODUCT_TAXES_SQL, (company_id,)))

    def read_failures(self, company_id: int) -> dict[int, dict[str, Any]]:
        rows = self._rows(SELECT_FAILURES_SQL, (company_id, RESOURCE, GLOBAL_SCOPE, FAILURES_CURSOR))
        return failures_from_value(rows[0][0]) if rows else {}

    @contextmanager
    def product_tax_transaction(self) -> Iterator[PgProductTaxTx]:
        with self.transaction() as tx:
            yield PgProductTaxTx(tx.cur)


# --- orquestación ---------------------------------------------------------------------------------


def _classify(existing: dict[int, ExistingRow], rows: list[ProductTaxRow], applied: set[int] | None) -> dict[str, int]:
    out = {"inserted": 0, "updated": 0, "unchanged": 0, "skipped_newer": 0}
    for row in rows:
        prev = existing.get(row.product_id)
        if applied is not None and row.product_id not in applied:
            kind = "skipped_newer"
        elif applied is None and prev is not None and prev.api_fetched_at > row.api_fetched_at:
            kind = "skipped_newer"
        elif prev is None:
            kind = "inserted"
        elif prev.payload_hash == row.payload_hash and prev.missing_since is None:
            kind = "unchanged"
        else:
            kind = "updated"
        out[kind] += 1
    return out


def _add_counts(outcome: EntityOutcome, counts: dict[str, int]) -> None:
    outcome.rows_inserted += counts["inserted"]
    outcome.rows_updated += counts["updated"]
    outcome.rows_unchanged += counts["unchanged"]
    outcome.rows_skipped_newer += counts["skipped_newer"]


def _missing_plan(
    existing: dict[int, ExistingRow], present: set[int], started: datetime, threshold: float
) -> tuple[list[int], dict[str, Any]]:
    current = [pid for pid, e in existing.items() if e.missing_since is None]
    absent = [pid for pid in current if pid not in present]
    missing = sorted(pid for pid in absent if existing[pid].api_fetched_at <= started)
    pct = round(len(missing) * 100.0 / len(current), 2) if current else 0.0
    tripped = pct > threshold
    fuse = {
        "threshold_pct": threshold, "present_existing": len(current), "would_mark_missing": len(missing),
        "protected_newer": len(absent) - len(missing), "missing_pct": pct, "tripped": tripped,
        "reason": f"{len(missing)}/{len(current)} relaciones pasarían a ausentes ({pct}%) > umbral {threshold}%"
        if tripped else None,
    }
    return missing, fuse


def _status(outcome: EntityOutcome, fetched: TaxFetch, targets: int, unknown: dict[int, list[int]]) -> tuple[str, str | None]:
    problems = []
    if outcome.point.get("expand_ignored"):
        problems.append("expand no entregó la relación de ningún producto")
    if fetched.failed:
        problems.append(f"{len(fetched.failed)} productos con error (p. ej. {sorted(fetched.failed)[:10]})")
    not_attempted = targets - len(fetched.attempted)
    if not_attempted:
        reason = fetched.aborted or (
            f"tope de {MAX_INDIVIDUAL_FALLBACK} consultas individuales por relaciones no expandidas"
            if fetched.fallback_skipped else "corte"
        )
        problems.append(f"{not_attempted} productos no consultados: {reason}")
    if unknown:
        problems.append(f"{len(unknown)} productos con tax.id ausente de bsale_raw.taxes (p. ej. {sorted(unknown)[:10]})")
    if outcome.fuse and outcome.fuse.get("tripped"):
        problems.append(f"fusible de ausencias: {outcome.fuse['reason']}; no se marcó ninguna")
    if not problems:
        return RunStatus.SUCCESS.value, None
    return RunStatus.PARTIAL.value, "; ".join(problems)


def run_product_tax_sync(
    *,
    store: ProductTaxStore,
    company_id: int,
    dry_run: bool = False,
    limit: int | None = None,
    source: str = SOURCE_EXPAND,
    client_factory: TaxClientFactory = default_tax_client_factory,
    clock: Clock = utc_now,
    monotonic: Callable[[], float] = time.perf_counter,
    getenv: Callable[[str], str | None] = os.getenv,
    trigger: str = TRIGGER_MANUAL,
    host: str | None = None,
) -> EntityOutcome:
    """Nunca lanza por fallas de API/BD: devuelve el ``EntityOutcome`` con status y error saneado."""
    spec = product_tax_spec()
    if source not in SOURCES:
        raise UnsupportedSyncError(f"fuente de product_taxes inválida: {source!r} (válidas {SOURCES})")
    if limit is not None and (isinstance(limit, bool) or not isinstance(limit, int) or limit < 1):
        raise UnsupportedSyncError(f"límite inválido: {limit!r}")
    if limit is not None and not dry_run:
        raise UnsupportedSyncError("el límite de productos sólo se acepta en dry-run")
    mode = SyncMode.FULL_RECONCILE
    outcome = EntityOutcome(
        company_id=company_id, resource=RESOURCE, scope=GLOBAL_SCOPE, mode=mode.value, dry_run=dry_run,
        point={"source": source},
    )
    t0 = monotonic()

    def elapsed_ms() -> int:
        return int((monotonic() - t0) * 1000)

    try:
        src = store.resolve_source(company_id)
        token = read_token(src, getenv)
        threshold = max_missing_pct(RESOURCE, getenv)
        rate = product_tax_rate_config(getenv)
    except Exception as exc:
        return _fail(store, None, outcome, exc, [], elapsed_ms())
    secrets = [token]
    outcome.point["rps"] = rate.requests_per_second

    lock = nullcontext() if dry_run else store.advisory_lock(company_id, RESOURCE, GLOBAL_SCOPE)
    try:
        with lock:
            return _run_locked(
                store=store, spec=spec, source=src, token=token, threshold=threshold, outcome=outcome,
                mode=mode, limit=limit, tax_source=source, client_factory=client_factory, clock=clock,
                elapsed_ms=elapsed_ms, trigger=trigger, host=host, secrets=secrets,
            )
    except LockBusyError as exc:
        outcome.status = RunStatus.SKIPPED.value
        outcome.error = sanitize_error(exc, secrets)
        outcome.duration_ms = elapsed_ms()
        logger.warning("[BSALE_RAW] %s", outcome.error)
        return outcome
    except Exception as exc:
        return _fail(store, None, outcome, exc, secrets, elapsed_ms())


def _run_locked(
    *,
    store: ProductTaxStore,
    spec: ResourceSpec,
    source: SourceConfig,
    token: str,
    threshold: float,
    outcome: EntityOutcome,
    mode: SyncMode,
    limit: int | None,
    tax_source: str,
    client_factory: TaxClientFactory,
    clock: Clock,
    elapsed_ms: Callable[[], int],
    trigger: str,
    host: str | None,
    secrets: list[str],
) -> EntityOutcome:
    company_id = source.company_id
    try:
        products = store.select_products(company_id)
        known_taxes = store.select_taxes(company_id)
        existing = store.read_existing_product_taxes(company_id)
        previous_failures = store.read_failures(company_id)
    except Exception as exc:
        return _fail(store, None, outcome, exc, secrets, elapsed_ms())
    if tax_source == SOURCE_INDIVIDUAL and not products:
        return _fail(
            store, None, outcome,
            RuntimeError(f"bsale_raw.products sin productos vigentes para company_id={company_id}"),
            secrets, elapsed_ms(),
        )

    handle: RunHandle | None = None
    if not outcome.dry_run:
        try:
            handle = store.start_run(
                mode=mode.value, trigger=trigger, host=host, company_id=company_id, resource=RESOURCE,
                scope=outcome.scope,
            )
        except Exception as exc:
            return _fail(store, None, outcome, exc, secrets, elapsed_ms())
        outcome.sync_run_id = handle.run_id

    started = clock()
    outcome.snapshot_started_at = started
    fetched = TaxFetch()
    client = None

    def write(rows: list[ProductTaxRow]) -> None:
        for i in range(0, len(rows), WRITE_BATCH):
            chunk = rows[i:i + WRITE_BATCH]
            if outcome.dry_run:
                _add_counts(outcome, _classify(existing, chunk, None))
            elif chunk:
                assert handle is not None
                with store.product_tax_transaction() as tx:
                    applied = tx.upsert_product_taxes(chunk, sync_run_id=handle.run_id)
                _add_counts(outcome, _classify(existing, chunk, applied))

    def fetch_individual(product_ids: list[int]) -> None:
        streak = 0
        for i in range(0, len(product_ids), WRITE_BATCH):
            chunk_start = len(fetched.rows)
            streak = fetch_batch(
                client, spec, company_id, product_ids[i:i + WRITE_BATCH], fetched, clock=clock, secrets=secrets,
                streak=streak,
            )
            write(fetched.rows[chunk_start:])
            if fetched.aborted:
                break

    missing_evaluable = True
    try:
        client = client_factory(source, token, spec)
        if tax_source == SOURCE_EXPAND:
            snapshot = fetch_expanded_listing(client, clock=clock, max_items=limit)
            outcome.api_count = snapshot.api_count
            outcome.pages = snapshot.pages
            expanded = collect_expanded(company_id, snapshot)
            present = set(expanded.order)
            missing_evaluable = limit is None
            targets = len(expanded.order)
            raw_products = set(products)
            outcome.point.update(
                products=len(expanded.order), targets=targets, limit=limit, expanded=len(expanded.rows),
                not_in_raw_products=len(present - raw_products),
                raw_products_not_in_listing=len(raw_products - present) if missing_evaluable else None,
                expand_ignored=bool(expanded.order) and not expanded.rows,
            )
            fetched.rows.extend(expanded.rows)
            fetched.attempted.extend(r.product_id for r in expanded.rows)
            fetched.fallback = expanded.fallback
            write(expanded.rows)
            pending = [pid for pid in expanded.order if pid in expanded.fallback]
            fetched.fallback_skipped = pending[MAX_INDIVIDUAL_FALLBACK:]
            fetch_individual(pending[:MAX_INDIVIDUAL_FALLBACK])
        else:
            present = set(products)
            target_ids = products[:limit] if limit is not None else products
            targets = len(target_ids)
            outcome.api_count = targets
            outcome.point.update(products=len(products), targets=targets, limit=limit)
            fetch_individual(target_ids)
    except Exception as exc:
        _collect_request_stats(client, outcome)
        _record(outcome, fetched, known_taxes)
        saved = outcome.rows_inserted + outcome.rows_updated + outcome.rows_unchanged
        failure = RuntimeError(
            f"{sanitize_error(exc, secrets)}; {saved} relaciones de lotes anteriores quedaron guardadas"
        )
        return _fail(store, handle, outcome, failure, secrets, elapsed_ms())
    _collect_request_stats(client, outcome)
    unknown = _record(outcome, fetched, known_taxes)

    if missing_evaluable:
        missing_ids, fuse = _missing_plan(existing, present, started, threshold)
        failures_scope = present
    else:
        missing_ids, fuse = [], None
        failures_scope = present | set(previous_failures)
    outcome.fuse = fuse
    outcome.point["missing_evaluated"] = missing_evaluable
    failures = next_failures(previous_failures, fetched, failures_scope, clock())
    outcome.point["failures_pending"] = len(failures)
    if not fetched.rows:
        first = next(iter(fetched.failed.values()), fetched.aborted or "sin respuesta")
        failure = RuntimeError(f"ningún producto con impuestos válidos ({len(fetched.failed)} con error; p. ej. {first})")
        if handle is not None:
            try:
                with store.product_tax_transaction() as tx:
                    tx.write_failures(company_id, failures, started_at=started, sync_run_id=handle.run_id)
            except Exception as exc:
                return _fail(store, handle, outcome, exc, secrets, elapsed_ms())
        return _fail(store, handle, outcome, failure, secrets, elapsed_ms())

    if outcome.dry_run:
        outcome.rows_missing = 0 if fuse is None or fuse["tripped"] else len(missing_ids)
        outcome.status, outcome.error = _status(outcome, fetched, targets, unknown)
        outcome.duration_ms = elapsed_ms()
        _effective_rps(outcome)
        return outcome

    assert handle is not None and fuse is not None
    try:
        with store.product_tax_transaction() as tx:
            if not fuse["tripped"]:
                outcome.rows_missing = tx.mark_product_taxes_missing(company_id, missing_ids, started)
            tx.write_failures(company_id, failures, started_at=started, sync_run_id=handle.run_id)
            outcome.status, outcome.error = _status(outcome, fetched, targets, unknown)
            outcome.duration_ms = elapsed_ms()
            _effective_rps(outcome)
            tx.finish_success(handle, outcome)
    except Exception as exc:
        outcome.rows_missing = 0
        failure = RuntimeError(
            f"{sanitize_error(exc, secrets)}; relaciones de los lotes ya guardadas, ausencias y fallas no registradas"
        )
        return _fail(store, handle, outcome, failure, secrets, elapsed_ms())
    _log(outcome)
    return outcome


def _record(outcome: EntityOutcome, fetched: TaxFetch, known_taxes: set[int]) -> dict[int, list[int]]:
    unknown = {
        r.product_id: sorted({t for t in r.tax_ids if t not in known_taxes})
        for r in fetched.rows if any(t not in known_taxes for t in r.tax_ids)
    }
    outcome.rows_received = len(fetched.rows)
    failed = sorted(fetched.failed)
    fallback = sorted(fetched.fallback)
    outcome.point.update(
        attempted=len(fetched.attempted),
        fetched=len(fetched.rows),
        with_taxes=sum(1 for r in fetched.rows if r.items_count),
        without_taxes=sum(1 for r in fetched.rows if not r.items_count),
        failed=len(failed),
        not_attempted=outcome.point.get("targets", 0) - len(fetched.attempted),
        aborted=fetched.aborted,
        failed_sample={str(p): fetched.failed[p] for p in failed[:MAX_SAMPLE]},
        unknown_tax_products={str(p): unknown[p] for p in sorted(unknown)[:MAX_SAMPLE]},
        unknown_tax_count=len(unknown),
    )
    if outcome.point.get("source") == SOURCE_EXPAND:
        outcome.point.update(
            fallback_needed=len(fallback),
            fallback_ok=sum(1 for r in fetched.rows if r.last_source == LAST_SOURCE_INDIVIDUAL),
            fallback_skipped=len(fetched.fallback_skipped),
            fallback_sample={str(p): fetched.fallback[p] for p in fallback[:MAX_SAMPLE]},
        )
    return unknown


def _effective_rps(outcome: EntityOutcome) -> None:
    if outcome.duration_ms:
        outcome.point["effective_rps"] = round(outcome.requests * 1000.0 / outcome.duration_ms, 3)


def _log(outcome: EntityOutcome) -> None:
    p = outcome.point or {}
    logger.info(
        "[BSALE_RAW] company=%s resource=%s source=%s status=%s targets=%s fetched=%s expanded=%s "
        "fallback_needed=%s fallback_ok=%s with_taxes=%s without_taxes=%s failed=%s not_attempted=%s "
        "inserted=%s updated=%s unchanged=%s skipped_newer=%s missing=%s requests=%s http_429=%s duration_ms=%s",
        outcome.company_id, outcome.resource, p.get("source"), outcome.status, p.get("targets"), p.get("fetched"),
        p.get("expanded"), p.get("fallback_needed"), p.get("fallback_ok"), p.get("with_taxes"),
        p.get("without_taxes"), p.get("failed"), p.get("not_attempted"), outcome.rows_inserted,
        outcome.rows_updated, outcome.rows_unchanged, outcome.rows_skipped_newer, outcome.rows_missing,
        outcome.requests, outcome.http_429, outcome.duration_ms,
    )
