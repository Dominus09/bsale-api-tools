"""Motor de precios ``bsale_raw.variant_prices`` (clave ``(company_id, price_list_id, variant_id)``).

Fuente: ``GET /v1/price_lists/{id}/details.json`` paginado (``limit``/``offset``), una lista a la vez.
``bsale_detail_id`` (``id`` del detalle) es sólo metadata: la identidad es la variante dentro de la
lista. RAW guarda EXCLUSIVAMENTE lo que entrega Bsale:

- ``payload``: el detalle completo, sin cambios;
- ``variant_value`` = ``variantValue`` y ``variant_value_with_taxes`` = ``variantValueWithTaxes``,
  como ``Decimal`` exacto del valor recibido (``0`` explícito = 0; ``null`` = NULL). Nunca se
  recalcula IVA, impuestos adicionales ni márgenes, y nunca se redondea.

``sync-prices`` (``sync_prices``), por empresa:

1. lock ``(company, variant_prices, sync-prices)``: una sola corrida por empresa (SKIPPED si no);
2. refresco de ``bsale_raw.price_lists`` con el motor de entidades (FULL_RECONCILE + fusible); si
   falla, se usan las listas guardadas pero la corrida queda como máximo PARTIAL y lo informa;
3. listas a barrer, leídas de ``bsale_raw.price_lists`` (nunca hardcodeadas): activas
   (``state = 0``), inactivas o todas; las ausentes en Bsale (``missing_since``) no se barren;
4. por lista, con lock ``(company, variant_prices, price_list:N)`` y su propio ``sync_runs``:
   ``snapshot_started_at`` → fetch completo y ESTRICTO SIN transacción → validación (count estable,
   total exacto, sin variantes ni detalles duplicados) → UNA transacción corta: UPSERT con frescura
   (``missing_since = NULL`` al reaparecer) + ``missing_since`` para los precios conocidos que el
   snapshot completo ya no trae + run/state. Fusible de ausencias por lista
   (``BSALE_RAW_MAX_MISSING_PCT_VARIANT_PRICES``, 20 % por defecto; snapshot vacío con precios
   presentes = fusible). Nunca hay DELETE.

Una lista que falla no escribe nada y no afecta a las demás: SUCCESS si todas las listas (y la
metadata) quedaron OK, PARTIAL si alguna falló, FAILED si ninguna, SKIPPED si el lock está ocupado.

Los precios de listas inactivas se guardan igual que los de las activas; el carácter "vigente" lo
decide la capa de negocio con ``bsale_raw.price_lists.state``.

``refresh-prices`` (``refresh_prices``): POINT de variantes explícitas en listas explícitas o en las
activas guardadas; ``details.json?variantid=V`` por (lista, variante) con prioridad P0, sin lock (la
frescura por fila decide). 0 filas = ``NO_ROWS``: no se escribe nada (ni precio 0 ni ausencia).

Ritmo propio (``BSALE_RAW_PRICE_RPS``, 2 rps por defecto): los scanners de stock y costos corren
en otros procesos con sus propios limitadores y comparten la cuota del token.
"""

from __future__ import annotations

import dataclasses
import logging
import os
import time
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Iterable, Iterator, Protocol

from backend.services.bsale.companies import BsaleCompany
from backend.services.bsale_raw.core.client import build_company_client
from backend.services.bsale_raw.core.engine import (
    TRIGGER_MANUAL,
    FuseTrippedError,
    UnsupportedSyncError,
    _collect_request_stats,
    _fail,
    _reset_write_counts,
    api_path,
    read_token,
    run_entity_sync,
    sanitize_error,
)
from backend.services.bsale_raw.core.models import RunStatus, SyncMode, payload_hash
from backend.services.bsale_raw.core.rate_limit import (
    BSALE_DOCUMENTED_MAX_REQUESTS,
    BSALE_DOCUMENTED_WINDOW_SECONDS,
    DEFAULT_BUDGET_FRACTION,
    CompanyRateLimiters,
    PriorityRateLimiter,
    RateLimitConfig,
    RequestPriority,
    TokenBucket,
)
from backend.services.bsale_raw.core.reconcile import ExistingRow, ReconcilePlan, max_missing_pct, plan_reconcile
from backend.services.bsale_raw.core.registry import (
    GLOBAL_SCOPE,
    POINT_STATE_SCOPE,
    REGISTRY,
    ResourceSpec,
    optional_int,
    optional_numeric,
    optional_relation_id,
    point_scope,
    price_list_scope,
)
from backend.services.bsale_raw.core.snapshot import (
    Clock,
    Snapshot,
    SnapshotValidationError,
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

RESOURCE = "variant_prices"
LISTS_RESOURCE = "price_lists"
SYNC_LOCK_SCOPE = "sync-prices"
PRICE_LIST_STATE_ACTIVE = 0
VARIANT_FILTER = "variantid"
MAX_POINT_VARIANTS = 50
MAX_SAMPLE = 20
DEFAULT_PRICE_RPS = 2.0
POINT_PRIORITY = RequestPriority.P0_TARGETED
PRICE_FIELDS = ("variantValue", "variantValueWithTaxes")

PriceClientFactory = Callable[[SourceConfig, str, ResourceSpec], Any]


class ListSelection(str, Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"
    ALL = "all"


def price_spec() -> ResourceSpec:
    import backend.services.bsale_raw.resources  # noqa: F401  (registra los recursos)

    return REGISTRY.get(RESOURCE)


# --- ritmo propio ---------------------------------------------------------------------------------


def price_rate_config(getenv: Callable[[str], str | None] = os.getenv) -> RateLimitConfig:
    """``BSALE_RAW_PRICE_RPS`` (default 2). Tope: el presupuesto por empresa del limitador general."""
    ceiling = BSALE_DOCUMENTED_MAX_REQUESTS / BSALE_DOCUMENTED_WINDOW_SECONDS * DEFAULT_BUDGET_FRACTION
    raw = (getenv("BSALE_RAW_PRICE_RPS") or "").strip()
    rps = float(raw) if raw else DEFAULT_PRICE_RPS
    if not 0 < rps <= ceiling:
        raise ValueError(f"BSALE_RAW_PRICE_RPS inválido: {rps} (rango (0, {ceiling}])")
    return RateLimitConfig(requests_per_second=rps, burst=max(1, int(rps)))


_PRICE_LIMITERS = CompanyRateLimiters(lambda cid: PriorityRateLimiter(TokenBucket(price_rate_config())))


def default_price_client_factory(source: SourceConfig, token: str, spec: ResourceSpec) -> Any:
    company = BsaleCompany(company_id=source.company_id, name=source.name, token_env=source.token_env, token=token)
    return build_company_client(company, _PRICE_LIMITERS, spec.request_priority)


# --- listas y filas -------------------------------------------------------------------------------


@dataclass(frozen=True)
class PriceListInfo:
    bsale_id: int
    name: str | None
    state: int | None
    missing_since: datetime | None = None

    @property
    def present(self) -> bool:
        return self.missing_since is None

    @property
    def active(self) -> bool:
        return self.present and self.state == PRICE_LIST_STATE_ACTIVE


@dataclass(frozen=True)
class PriceRow:
    company_id: int
    price_list_id: int
    variant_id: int
    bsale_detail_id: int
    variant_value: Any
    variant_value_with_taxes: Any
    payload: dict[str, Any] = field(repr=False)
    payload_hash: str
    api_fetched_at: datetime


def variant_key(row: PriceRow) -> int:
    return row.variant_id


def pair_key(row: PriceRow) -> tuple[int, int]:
    return (row.price_list_id, row.variant_id)


def build_price_rows(
    company_id: int, price_list_id: int, snapshot: Snapshot, *, variant_id: int | None = None
) -> list[PriceRow]:
    """
    Una fila por detalle. Invalida el snapshot completo de la lista: ``id`` o ``variant.id`` ausente o
    no numérico, ``variantValue``/``variantValueWithTaxes`` ausente o no numérico, la misma variante
    en más de un detalle (no se elige uno), ``id`` de detalle repetido (desplazamiento de páginas) o
    una variante distinta de ``variant_id`` (filtro ignorado por la API).
    """
    rows: list[PriceRow] = []
    seen_variants: set[int] = set()
    seen_details: set[int] = set()
    duplicate_variants: list[int] = []
    duplicate_details: list[int] = []
    wrong_variant: set[int] = set()
    where = f"lista {price_list_id}"
    for item in snapshot.items:
        payload = item.payload
        try:
            detail_id = optional_int(payload.get("id"))
            item_variant = optional_relation_id(payload.get("variant"))
        except ValueError as exc:
            raise SnapshotValidationError(f"{where}: detalle con id o variant inválido: {exc}") from None
        if detail_id is None:
            raise SnapshotValidationError(f"{where}: detalle sin id")
        if item_variant is None:
            raise SnapshotValidationError(f"{where}: detalle {detail_id} sin variant.id")
        missing = [k for k in PRICE_FIELDS if k not in payload]
        if missing:
            raise SnapshotValidationError(f"{where}: detalle {detail_id} sin {', '.join(missing)}")
        try:
            value = optional_numeric(payload["variantValue"])
            with_taxes = optional_numeric(payload["variantValueWithTaxes"])
        except ValueError as exc:
            raise SnapshotValidationError(f"{where}: detalle {detail_id}: {exc}") from None
        if variant_id is not None and item_variant != variant_id:
            wrong_variant.add(item_variant)
            continue
        if detail_id in seen_details:
            duplicate_details.append(detail_id)
            continue
        seen_details.add(detail_id)
        if item_variant in seen_variants:
            duplicate_variants.append(item_variant)
            continue
        seen_variants.add(item_variant)
        rows.append(
            PriceRow(
                company_id=company_id,
                price_list_id=price_list_id,
                variant_id=item_variant,
                bsale_detail_id=detail_id,
                variant_value=value,
                variant_value_with_taxes=with_taxes,
                payload=payload,
                payload_hash=payload_hash(payload),
                api_fetched_at=item.fetched_at,
            )
        )
    if wrong_variant:
        raise SnapshotValidationError(
            f"{where}: detalles de variantes {sorted(wrong_variant)[:5]} en un refresh de variant={variant_id}"
        )
    if duplicate_variants:
        raise SnapshotValidationError(
            f"{where}: {len(duplicate_variants)} variantes con más de un detalle "
            f"(p. ej. {sorted(set(duplicate_variants))[:5]}); no se elige uno"
        )
    if duplicate_details:
        raise SnapshotValidationError(
            f"{where}: {len(duplicate_details)} ids de detalle duplicados "
            f"(p. ej. {sorted(set(duplicate_details))[:5]}); posible desplazamiento de páginas"
        )
    return rows


# --- persistencia ---------------------------------------------------------------------------------

PRICE_TABLE = "bsale_raw.variant_prices"
PRICE_COLUMNS = (
    "company_id", "price_list_id", "variant_id", "bsale_detail_id", "variant_value", "variant_value_with_taxes",
    "payload", "payload_hash", "first_seen_at", "last_seen_at", "last_changed_at", "api_fetched_at",
    "missing_since", "last_source", "sync_run_id",
)
PRICE_TEMPLATE = "(%s, %s, %s, %s, %s, %s, %s, %s, now(), now(), now(), %s, NULL, %s, %s)"

UPSERT_PRICES_SQL = (
    f"INSERT INTO {PRICE_TABLE} AS t ({', '.join(PRICE_COLUMNS)}) VALUES %s\n"
    "ON CONFLICT (company_id, price_list_id, variant_id) DO UPDATE SET\n    "
    + ",\n    ".join([
        "bsale_detail_id = EXCLUDED.bsale_detail_id",
        "variant_value = EXCLUDED.variant_value",
        "variant_value_with_taxes = EXCLUDED.variant_value_with_taxes",
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
    + "\nWHERE t.api_fetched_at <= EXCLUDED.api_fetched_at\nRETURNING price_list_id, variant_id"
)

# Re-chequea la frescura al marcar: un precio refrescado (POINT) después del inicio del snapshot
# nunca queda ausente.
MARK_PRICES_MISSING_SQL = (
    f"UPDATE {PRICE_TABLE} SET missing_since = now() "
    "WHERE company_id = %s AND price_list_id = %s AND variant_id = ANY(%s) "
    "AND missing_since IS NULL AND api_fetched_at <= %s"
)

SELECT_EXISTING_PRICES_SQL = (
    f"SELECT variant_id, payload_hash, api_fetched_at, missing_since FROM {PRICE_TABLE} "
    "WHERE company_id = %s AND price_list_id = %s"
)

SELECT_EXISTING_PRICE_PAIRS_SQL = (
    f"SELECT price_list_id, variant_id, payload_hash, api_fetched_at, missing_since FROM {PRICE_TABLE} "
    "WHERE company_id = %s AND variant_id = ANY(%s)"
)

SELECT_PRICE_LISTS_SQL = (
    "SELECT bsale_id, name, state, missing_since FROM bsale_raw.price_lists WHERE company_id = %s ORDER BY bsale_id"
)

SELECT_CATALOGUED_VARIANTS_SQL = (
    "SELECT bsale_id FROM bsale_raw.variants WHERE company_id = %s AND missing_since IS NULL AND bsale_id = ANY(%s)"
)

SELECT_LAST_SUCCESS_SQL = (
    "SELECT scope, last_success_at FROM bsale_raw.sync_state "
    "WHERE company_id = %s AND resource = %s AND scope = ANY(%s)"
)


def _existing_prices(rows: list[tuple]) -> dict[int, ExistingRow]:
    return {
        int(r[0]): ExistingRow(bsale_id=int(r[0]), payload_hash=r[1], api_fetched_at=r[2], missing_since=r[3])
        for r in rows
    }


def _existing_pairs(rows: list[tuple]) -> dict[tuple[int, int], ExistingRow]:
    return {
        (int(r[0]), int(r[1])): ExistingRow(bsale_id=int(r[1]), payload_hash=r[2], api_fetched_at=r[3],
                                            missing_since=r[4])
        for r in rows
    }


def _price_lists(rows: list[tuple]) -> list[PriceListInfo]:
    return [
        PriceListInfo(bsale_id=int(r[0]), name=r[1], state=None if r[2] is None else int(r[2]), missing_since=r[3])
        for r in rows
    ]


class PriceTx(Protocol):
    def read_existing_prices(self, company_id: int, price_list_id: int) -> dict[int, ExistingRow]: ...

    def read_existing_price_pairs(
        self, company_id: int, variant_ids: list[int]
    ) -> dict[tuple[int, int], ExistingRow]: ...

    def upsert_prices(self, rows: list[PriceRow], *, sync_run_id: int, last_source: str) -> set[tuple[int, int]]: ...

    def mark_prices_missing(
        self, company_id: int, price_list_id: int, variant_ids: list[int], snapshot_started_at: datetime
    ) -> int: ...

    def finish_success(self, handle: RunHandle, outcome: EntityOutcome) -> None: ...


class PriceStore(Protocol):
    def resolve_source(self, company_id: int) -> SourceConfig: ...

    def advisory_lock(self, company_id: int, resource: str, scope: str) -> Any: ...

    def start_run(
        self, *, mode: str, trigger: str, host: str | None, company_id: int, resource: str, scope: str,
        state_scope: str | None = None,
    ) -> RunHandle: ...

    def read_price_lists(self, company_id: int) -> list[PriceListInfo]: ...

    def read_catalogued_variants(self, company_id: int, variant_ids: list[int]) -> set[int]: ...

    def read_existing_prices(self, company_id: int, price_list_id: int) -> dict[int, ExistingRow]: ...

    def read_existing_price_pairs(
        self, company_id: int, variant_ids: list[int]
    ) -> dict[tuple[int, int], ExistingRow]: ...

    def read_last_success(self, company_id: int, resource: str, scopes: list[str]) -> dict[str, datetime | None]: ...

    def price_transaction(self) -> Any: ...

    def finish_failed(self, handle: RunHandle, outcome: EntityOutcome) -> None: ...


@dataclass
class PgPriceTx(PgRawTx):
    def read_existing_prices(self, company_id: int, price_list_id: int) -> dict[int, ExistingRow]:
        """Lectura MVCC sin ``FOR UPDATE``: no bloquea un POINT; la frescura la re-chequea la SQL."""
        self.cur.execute(SELECT_EXISTING_PRICES_SQL, (company_id, price_list_id))
        return _existing_prices(self.cur.fetchall())

    def read_existing_price_pairs(
        self, company_id: int, variant_ids: list[int]
    ) -> dict[tuple[int, int], ExistingRow]:
        if not variant_ids:
            return {}
        self.cur.execute(SELECT_EXISTING_PRICE_PAIRS_SQL, (company_id, list(variant_ids)))
        return _existing_pairs(self.cur.fetchall())

    def upsert_prices(self, rows: list[PriceRow], *, sync_run_id: int, last_source: str) -> set[tuple[int, int]]:
        if not rows:
            return set()
        from psycopg2.extras import Json, execute_values

        values = [
            (
                r.company_id, r.price_list_id, r.variant_id, r.bsale_detail_id, r.variant_value,
                r.variant_value_with_taxes, Json(r.payload), r.payload_hash, r.api_fetched_at, last_source,
                sync_run_id,
            )
            for r in rows
        ]
        returned = execute_values(
            self.cur, UPSERT_PRICES_SQL, values, template=PRICE_TEMPLATE, page_size=500, fetch=True
        )
        return {(int(r[0]), int(r[1])) for r in returned}

    def mark_prices_missing(
        self, company_id: int, price_list_id: int, variant_ids: list[int], snapshot_started_at: datetime
    ) -> int:
        if not variant_ids:
            return 0
        self.cur.execute(MARK_PRICES_MISSING_SQL, (company_id, price_list_id, list(variant_ids), snapshot_started_at))
        return int(self.cur.rowcount or 0)


class PgPriceStore(PgRawStore):
    def _select(self, sql: str, params: tuple) -> list[tuple]:
        cur = self._work().cursor()
        try:
            cur.execute(sql, params)
            return cur.fetchall()
        finally:
            cur.close()

    def read_price_lists(self, company_id: int) -> list[PriceListInfo]:
        return _price_lists(self._select(SELECT_PRICE_LISTS_SQL, (company_id,)))

    def read_catalogued_variants(self, company_id: int, variant_ids: list[int]) -> set[int]:
        if not variant_ids:
            return set()
        return {int(r[0]) for r in self._select(SELECT_CATALOGUED_VARIANTS_SQL, (company_id, list(variant_ids)))}

    def read_existing_prices(self, company_id: int, price_list_id: int) -> dict[int, ExistingRow]:
        return _existing_prices(self._select(SELECT_EXISTING_PRICES_SQL, (company_id, price_list_id)))

    def read_existing_price_pairs(
        self, company_id: int, variant_ids: list[int]
    ) -> dict[tuple[int, int], ExistingRow]:
        if not variant_ids:
            return {}
        return _existing_pairs(self._select(SELECT_EXISTING_PRICE_PAIRS_SQL, (company_id, list(variant_ids))))

    def read_last_success(self, company_id: int, resource: str, scopes: list[str]) -> dict[str, datetime | None]:
        if not scopes:
            return {}
        return {r[0]: r[1] for r in self._select(SELECT_LAST_SUCCESS_SQL, (company_id, resource, list(scopes)))}

    @contextmanager
    def price_transaction(self) -> Iterator[PgPriceTx]:
        with self.transaction() as tx:
            yield PgPriceTx(tx.cur)


# --- utilidades -----------------------------------------------------------------------------------


def _validate_id(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise UnsupportedSyncError(f"{name} inválido: {value!r}")
    return value


def _apply_counts(outcome: EntityOutcome, counts: dict[str, int]) -> None:
    outcome.rows_inserted = counts["inserted"]
    outcome.rows_updated = counts["updated"]
    outcome.rows_unchanged = counts["unchanged"]
    outcome.rows_skipped_newer = counts["skipped_newer"]


def _log(outcome: EntityOutcome) -> None:
    logger.info(
        "[BSALE_RAW] company=%s resource=%s scope=%s mode=%s status=%s api_count=%s received=%s inserted=%s "
        "updated=%s unchanged=%s skipped_newer=%s missing=%s requests=%s duration_ms=%s",
        outcome.company_id, outcome.resource, outcome.scope, outcome.mode, outcome.status, outcome.api_count,
        outcome.rows_received, outcome.rows_inserted, outcome.rows_updated, outcome.rows_unchanged,
        outcome.rows_skipped_newer, outcome.rows_missing, outcome.requests, outcome.duration_ms,
    )


# --- una lista: snapshot completo + reconcile no destructivo ------------------------------------


def _list_fuse(plan: ReconcilePlan, extra: dict[str, Any]) -> dict[str, Any]:
    return {**plan.fuse_json(), **extra, "deletes": False}


def _sync_list(
    *,
    store: PriceStore,
    spec: ResourceSpec,
    source: SourceConfig,
    token: str,
    info: PriceListInfo,
    threshold: float,
    dry_run: bool,
    client_factory: PriceClientFactory,
    clock: Clock,
    monotonic: Callable[[], float],
    trigger: str,
    host: str | None,
    secrets: list[str],
) -> EntityOutcome:
    """Nunca lanza por fallas de API/BD: devuelve el ``EntityOutcome`` de la lista."""
    scope = price_list_scope(info.bsale_id)
    outcome = EntityOutcome(
        company_id=source.company_id, resource=RESOURCE, scope=scope, mode=SyncMode.FULL_RECONCILE.value,
        dry_run=dry_run,
        point={"price_list_id": info.bsale_id, "name": info.name, "state": info.state, "active": info.active},
    )
    t0 = monotonic()

    def elapsed_ms() -> int:
        return int((monotonic() - t0) * 1000)

    lock = nullcontext() if dry_run else store.advisory_lock(source.company_id, RESOURCE, scope)
    try:
        with lock:
            return _sync_list_locked(
                store=store, spec=spec, source=source, token=token, info=info, threshold=threshold,
                outcome=outcome, client_factory=client_factory, clock=clock, elapsed_ms=elapsed_ms,
                trigger=trigger, host=host, secrets=secrets,
            )
    except LockBusyError as exc:
        outcome.status = RunStatus.SKIPPED.value
        outcome.error = sanitize_error(exc, secrets)
        outcome.duration_ms = elapsed_ms()
        logger.warning("[BSALE_RAW] %s", outcome.error)
        return outcome
    except Exception as exc:
        return _fail(store, None, outcome, exc, secrets, elapsed_ms())


def _sync_list_locked(
    *,
    store: PriceStore,
    spec: ResourceSpec,
    source: SourceConfig,
    token: str,
    info: PriceListInfo,
    threshold: float,
    outcome: EntityOutcome,
    client_factory: PriceClientFactory,
    clock: Clock,
    elapsed_ms: Callable[[], int],
    trigger: str,
    host: str | None,
    secrets: list[str],
) -> EntityOutcome:
    company_id = source.company_id
    handle: RunHandle | None = None
    if not outcome.dry_run:
        try:
            handle = store.start_run(
                mode=outcome.mode, trigger=trigger, host=host, company_id=company_id, resource=RESOURCE,
                scope=outcome.scope,
            )
        except Exception as exc:
            return _fail(store, None, outcome, exc, secrets, elapsed_ms())
        outcome.sync_run_id = handle.run_id

    snapshot_started_at = clock()
    outcome.snapshot_started_at = snapshot_started_at
    client = None
    try:
        client = client_factory(source, token, spec)
        snapshot = fetch_snapshot(
            client, api_path(spec.list_endpoint.format(parent_id=info.bsale_id)), clock=clock
        )
        outcome.api_count = snapshot.api_count
        outcome.pages = snapshot.pages
        rows = build_price_rows(company_id, info.bsale_id, snapshot)
        outcome.rows_received = len(rows)
    except Exception as exc:
        _collect_request_stats(client, outcome)
        return _fail(store, handle, outcome, exc, secrets, elapsed_ms())
    _collect_request_stats(client, outcome)

    try:
        variant_ids = sorted(r.variant_id for r in rows)
        catalogued = store.read_catalogued_variants(company_id, variant_ids)
    except Exception as exc:
        return _fail(store, handle, outcome, exc, secrets, elapsed_ms())
    # Se guardan igual: el detalle es válido aunque el catálogo RAW aún no tenga la variante.
    uncatalogued = [v for v in variant_ids if v not in catalogued]
    outcome.point.update(uncatalogued=len(uncatalogued), uncatalogued_sample=uncatalogued[:MAX_SAMPLE])
    diagnostics = {"count_first": snapshot.count_first, "count_last": snapshot.api_count}

    def plan_for(existing: dict[int, ExistingRow]) -> ReconcilePlan:
        return plan_reconcile(
            existing, rows, snapshot_started_at=snapshot_started_at, threshold_pct=threshold, key=variant_key
        )

    if outcome.dry_run:
        try:
            plan = plan_for(store.read_existing_prices(company_id, info.bsale_id))
        except Exception as exc:
            return _fail(store, None, outcome, exc, secrets, elapsed_ms())
        outcome.fuse = _list_fuse(plan, diagnostics)
        _apply_counts(outcome, plan.predicted)
        outcome.rows_missing = len(plan.missing_ids)
        outcome.status = RunStatus.FAILED.value if plan.fuse_tripped else RunStatus.SUCCESS.value
        outcome.error = f"fusible: {plan.fuse_reason}" if plan.fuse_tripped else None
        outcome.duration_ms = elapsed_ms()
        return outcome

    assert handle is not None
    try:
        with store.price_transaction() as tx:
            plan = plan_for(tx.read_existing_prices(company_id, info.bsale_id))
            outcome.fuse = _list_fuse(plan, diagnostics)
            if plan.fuse_tripped:
                raise FuseTrippedError(f"fusible: {plan.fuse_reason}; no se escribió nada")
            applied = tx.upsert_prices(rows, sync_run_id=handle.run_id, last_source=outcome.mode)
            _apply_counts(outcome, plan.counts(key[1] for key in applied))
            outcome.rows_missing = tx.mark_prices_missing(
                company_id, info.bsale_id, list(plan.missing_ids), snapshot_started_at
            )
            outcome.rows_deleted = 0
            outcome.status = RunStatus.SUCCESS.value
            outcome.duration_ms = elapsed_ms()
            tx.finish_success(handle, outcome)
    except Exception as exc:
        _reset_write_counts(outcome)
        return _fail(store, handle, outcome, exc, secrets, elapsed_ms())

    _log(outcome)
    return outcome


# --- sync-prices: metadata + listas ---------------------------------------------------------------


@dataclass
class ListResult:
    price_list_id: int
    name: str | None
    state: int | None
    active: bool
    outcome: EntityOutcome
    last_success_at: datetime | None = None


@dataclass
class PriceSyncReport:
    company_id: int
    selection: str
    dry_run: bool
    requested_lists: list[int] | None = None
    status: str = RunStatus.RUNNING.value
    error: str | None = None
    metadata: EntityOutcome | None = None
    metadata_last_success_at: datetime | None = None
    lists: list[ListResult] = field(default_factory=list)
    new_lists: list[int] = field(default_factory=list)
    deactivated_lists: list[int] = field(default_factory=list)
    reactivated_lists: list[int] = field(default_factory=list)
    removed_lists: list[int] = field(default_factory=list)
    duration_ms: int = 0

    @property
    def requests(self) -> int:
        return (self.metadata.requests if self.metadata else 0) + sum(r.outcome.requests for r in self.lists)


MetadataRunner = Callable[..., EntityOutcome]


def default_metadata_runner(**kwargs: Any) -> EntityOutcome:
    """``price_lists`` con el motor de entidades: FULL_RECONCILE, fusible, ``missing_since`` y su lock."""
    return run_entity_sync(resource=LISTS_RESOURCE, mode=SyncMode.FULL_RECONCILE, **kwargs)


def _list_changes(report: PriceSyncReport, before: list[PriceListInfo], after: list[PriceListInfo]) -> None:
    prev = {p.bsale_id: p for p in before}
    for info in after:
        old = prev.get(info.bsale_id)
        if not info.present:
            if old is None or old.present:
                report.removed_lists.append(info.bsale_id)
        elif old is None:
            report.new_lists.append(info.bsale_id)
        elif old.active and not info.active:
            report.deactivated_lists.append(info.bsale_id)
        elif not old.active and info.active:
            report.reactivated_lists.append(info.bsale_id)


def select_lists(
    lists: list[PriceListInfo], selection: ListSelection, requested: list[int] | None
) -> tuple[list[PriceListInfo], list[tuple[int, str]]]:
    """(listas a barrer, [(lista pedida no disponible, motivo)]). Nunca barre listas ausentes en Bsale."""
    wanted = {
        ListSelection.ACTIVE: lambda p: p.active,
        ListSelection.INACTIVE: lambda p: p.present and not p.active,
        ListSelection.ALL: lambda p: p.present,
    }[selection]
    chosen = [p for p in lists if wanted(p)]
    if requested is None:
        return chosen, []
    by_id = {p.bsale_id: p for p in lists}
    chosen_ids = {p.bsale_id for p in chosen}
    unavailable: list[tuple[int, str]] = []
    for list_id in requested:
        info = by_id.get(list_id)
        if info is None:
            unavailable.append((list_id, "no existe en bsale_raw.price_lists"))
        elif not info.present:
            unavailable.append((list_id, "ausente en Bsale (price_lists.missing_since)"))
        elif list_id not in chosen_ids:
            kind = "activa" if info.active else "inactiva"
            unavailable.append((list_id, f"lista {kind} fuera de --lists {selection.value}"))
    return [p for p in chosen if p.bsale_id in set(requested)], unavailable


def _last_success(
    store: PriceStore, company_id: int, resource: str, scopes: list[str], secrets: list[str]
) -> dict[str, datetime | None]:
    """Sólo informativo: una falla de lectura no cambia el resultado de la corrida."""
    try:
        return store.read_last_success(company_id, resource, scopes)
    except Exception as exc:
        logger.warning("[BSALE_RAW] no se pudo leer last_success_at: %s", sanitize_error(exc, secrets))
        return {}


def _overall(report: PriceSyncReport) -> tuple[str, str | None]:
    statuses = [r.outcome.status for r in report.lists]
    errors: list[str] = []
    metadata_ok = report.metadata is not None and report.metadata.status == RunStatus.SUCCESS.value
    if report.metadata is not None and not metadata_ok:
        last = report.metadata_last_success_at.isoformat() if report.metadata_last_success_at else "nunca"
        errors.append(
            f"metadata price_lists {report.metadata.status}: {report.metadata.error}; se usaron las listas "
            f"guardadas (último refresco exitoso: {last})"
        )
    if not statuses:
        errors.append(f"sin listas que sincronizar (--lists {report.selection})")
        return RunStatus.FAILED.value, "; ".join(errors)
    failed = [r.price_list_id for r in report.lists if r.outcome.status != RunStatus.SUCCESS.value]
    if failed:
        errors.append(f"listas sin sincronizar: {failed}")
    ok = len(statuses) - len(failed)
    if ok == len(statuses):
        status = RunStatus.SUCCESS.value if metadata_ok else RunStatus.PARTIAL.value
    elif ok == 0:
        all_skipped = all(s == RunStatus.SKIPPED.value for s in statuses)
        status = RunStatus.SKIPPED.value if all_skipped else RunStatus.FAILED.value
    else:
        status = RunStatus.PARTIAL.value
    return status, "; ".join(errors) or None


def sync_prices(
    *,
    store: PriceStore,
    company_id: int,
    selection: ListSelection | str = ListSelection.ACTIVE,
    price_list_ids: Iterable[int] | None = None,
    dry_run: bool = False,
    client_factory: PriceClientFactory = default_price_client_factory,
    metadata_runner: MetadataRunner = default_metadata_runner,
    clock: Clock = utc_now,
    monotonic: Callable[[], float] = time.perf_counter,
    getenv: Callable[[str], str | None] = os.getenv,
    trigger: str = TRIGGER_MANUAL,
    host: str | None = None,
) -> PriceSyncReport:
    """Nunca lanza por fallas de API/BD: devuelve el ``PriceSyncReport``."""
    selection = ListSelection(selection)
    requested = sorted({_validate_id(v, "price_list_id") for v in price_list_ids}) if price_list_ids else None
    spec = price_spec()
    report = PriceSyncReport(company_id=company_id, selection=selection.value, dry_run=dry_run,
                             requested_lists=requested)
    t0 = monotonic()

    def finish(status: str, error: str | None) -> PriceSyncReport:
        report.status, report.error = status, error
        report.duration_ms = int((monotonic() - t0) * 1000)
        log = logger.info if status == RunStatus.SUCCESS.value else logger.warning
        log("[BSALE_RAW] sync-prices company=%s selection=%s status=%s lists=%s requests=%s duration_ms=%s error=%s",
            company_id, selection.value, status, len(report.lists), report.requests, report.duration_ms, error)
        return report

    try:
        source = store.resolve_source(company_id)
        token = read_token(source, getenv)
        threshold = max_missing_pct(RESOURCE, getenv)
    except Exception as exc:
        return finish(RunStatus.FAILED.value, sanitize_error(exc, []))
    secrets = [token]

    lock = nullcontext() if dry_run else store.advisory_lock(company_id, RESOURCE, SYNC_LOCK_SCOPE)
    try:
        with lock:
            before = store.read_price_lists(company_id)
            report.metadata = metadata_runner(
                store=store, company_id=company_id, dry_run=dry_run, clock=clock, monotonic=monotonic,
                getenv=getenv, trigger=trigger, host=host,
            )
            after = before if dry_run else store.read_price_lists(company_id)
            _list_changes(report, before, after)
            report.metadata_last_success_at = _last_success(
                store, company_id, LISTS_RESOURCE, [GLOBAL_SCOPE], secrets
            ).get(GLOBAL_SCOPE)

            targets, unavailable = select_lists(after, selection, requested)
            for list_id, reason in unavailable:
                outcome = EntityOutcome(
                    company_id=company_id, resource=RESOURCE, scope=price_list_scope(list_id),
                    mode=SyncMode.FULL_RECONCILE.value, dry_run=dry_run, status=RunStatus.FAILED.value,
                    error=reason, point={"price_list_id": list_id},
                )
                report.lists.append(ListResult(list_id, None, None, False, outcome))
            for info in targets:
                outcome = _sync_list(
                    store=store, spec=spec, source=source, token=token, info=info, threshold=threshold,
                    dry_run=dry_run, client_factory=client_factory, clock=clock, monotonic=monotonic,
                    trigger=trigger, host=host, secrets=secrets,
                )
                report.lists.append(ListResult(info.bsale_id, info.name, info.state, info.active, outcome))
            report.lists.sort(key=lambda r: r.price_list_id)

            last = _last_success(
                store, company_id, RESOURCE, [price_list_scope(r.price_list_id) for r in report.lists], secrets
            )
            for result in report.lists:
                result.last_success_at = last.get(price_list_scope(result.price_list_id))
    except LockBusyError as exc:
        return finish(
            RunStatus.SKIPPED.value,
            f"sync-prices company_id={company_id} ya en ejecución (lock {RESOURCE}/{SYNC_LOCK_SCOPE} ocupado); "
            f"no se inicia otra: {sanitize_error(exc, secrets)}",
        )
    except Exception as exc:
        return finish(RunStatus.FAILED.value, sanitize_error(exc, secrets))

    return finish(*_overall(report))


# --- refresh-prices: POINT ------------------------------------------------------------------------


def refresh_prices(
    *,
    store: PriceStore,
    company_id: int,
    variant_ids: Iterable[int],
    price_list_ids: Iterable[int] | None = None,
    dry_run: bool = False,
    client_factory: PriceClientFactory = default_price_client_factory,
    clock: Clock = utc_now,
    monotonic: Callable[[], float] = time.perf_counter,
    getenv: Callable[[str], str | None] = os.getenv,
    trigger: str = TRIGGER_MANUAL,
    host: str | None = None,
) -> EntityOutcome:
    """
    POINT: ``details.json?variantid=V`` por (lista, variante). Sin ``price_list_ids`` usa las listas
    activas guardadas en ``bsale_raw.price_lists`` (no refresca la metadata). Una sola ``sync_runs``
    por llamada; el detalle por par queda en ``summary.point.results``. Nunca lanza por fallas de API/BD.
    """
    spec = dataclasses.replace(price_spec(), request_priority=POINT_PRIORITY)
    variants = sorted({_validate_id(v, "variant_id") for v in variant_ids})
    if not variants:
        raise UnsupportedSyncError("refresh de precios sin variantes")
    if len(variants) > MAX_POINT_VARIANTS:
        raise UnsupportedSyncError(f"refresh de precios con {len(variants)} variantes > máximo {MAX_POINT_VARIANTS}")
    requested = sorted({_validate_id(v, "price_list_id") for v in price_list_ids}) if price_list_ids else None
    outcome = EntityOutcome(
        company_id=company_id, resource=RESOURCE, scope=point_scope(variants), mode=SyncMode.POINT.value,
        dry_run=dry_run, state_scope=POINT_STATE_SCOPE,
        point={"variant_ids": variants, "price_list_ids": [], "results": {}},
    )
    t0 = monotonic()

    def elapsed_ms() -> int:
        return int((monotonic() - t0) * 1000)

    try:
        source = store.resolve_source(company_id)
        token = read_token(source, getenv)
    except Exception as exc:
        return _fail(store, None, outcome, exc, [], elapsed_ms())
    secrets = [token]

    try:
        lists = store.read_price_lists(company_id)
    except Exception as exc:
        return _fail(store, None, outcome, exc, secrets, elapsed_ms())
    if requested is None:
        targets = [p.bsale_id for p in lists if p.active]
        if not targets:
            return _fail(store, None, outcome, RuntimeError("sin listas activas en bsale_raw.price_lists"),
                         secrets, elapsed_ms())
    else:
        _, unavailable = select_lists(lists, ListSelection.ALL, requested)
        if unavailable:
            detail = ", ".join(f"{lid}: {why}" for lid, why in unavailable)
            return _fail(store, None, outcome, UnsupportedSyncError(f"listas no disponibles: {detail}"),
                         secrets, elapsed_ms())
        targets = requested
    outcome.point["price_list_ids"] = targets

    handle: RunHandle | None = None
    if not dry_run:
        try:
            handle = store.start_run(
                mode=SyncMode.POINT.value, trigger=trigger, host=host, company_id=company_id, resource=RESOURCE,
                scope=outcome.scope, state_scope=POINT_STATE_SCOPE,
            )
        except Exception as exc:
            return _fail(store, None, outcome, exc, secrets, elapsed_ms())
        outcome.sync_run_id = handle.run_id

    outcome.snapshot_started_at = clock()
    results: dict[str, dict[str, Any]] = outcome.point["results"]
    rows: list[PriceRow] = []
    client = None
    try:
        client = client_factory(source, token, spec)
    except Exception as exc:
        return _fail(store, handle, outcome, exc, secrets, elapsed_ms())

    api_count = 0
    for list_id in targets:
        path = api_path(spec.list_endpoint.format(parent_id=list_id))
        for variant in variants:
            key = f"{list_id}:{variant}"
            try:
                snapshot = fetch_snapshot(client, path, params={VARIANT_FILTER: variant}, clock=clock)
                pair_rows = build_price_rows(company_id, list_id, snapshot, variant_id=variant)
            except Exception as exc:
                results[key] = {"status": "FAILED", "error": sanitize_error(exc, secrets)}
                continue
            api_count += snapshot.api_count
            outcome.pages += snapshot.pages
            results[key] = {"status": "FETCHED" if pair_rows else "NO_ROWS"}
            rows.extend(pair_rows)
    _collect_request_stats(client, outcome)
    outcome.api_count = api_count
    outcome.rows_received = len(rows)

    failed = sorted(k for k, r in results.items() if r["status"] == "FAILED")
    if len(failed) == len(results):
        first = results[failed[0]]["error"]
        return _fail(
            store, handle, outcome,
            RuntimeError(f"refresh de precios falló en todos los pares lista:variante ({len(failed)}); p. ej. {first}"),
            secrets, elapsed_ms(),
        )
    status = RunStatus.PARTIAL.value if failed else RunStatus.SUCCESS.value
    error = f"{len(failed)} de {len(results)} pares lista:variante fallaron: {failed[:10]}" if failed else None
    fetched_variants = sorted({r.variant_id for r in rows})

    def plan_for(existing: dict) -> ReconcilePlan:
        return plan_reconcile(
            existing, rows, snapshot_started_at=outcome.snapshot_started_at, threshold_pct=100.0, key=pair_key
        )

    def record(classes: dict) -> None:
        for (list_id, variant), kind in classes.items():
            results[f"{list_id}:{variant}"]["class"] = kind

    if dry_run:
        try:
            plan = plan_for(store.read_existing_price_pairs(company_id, fetched_variants))
        except Exception as exc:
            return _fail(store, None, outcome, exc, secrets, elapsed_ms())
        record(plan.predicted_classes())
        _apply_counts(outcome, plan.predicted)
        outcome.status, outcome.error, outcome.duration_ms = status, error, elapsed_ms()
        return outcome

    assert handle is not None
    try:
        with store.price_transaction() as tx:
            plan = plan_for(tx.read_existing_price_pairs(company_id, fetched_variants))
            applied = tx.upsert_prices(rows, sync_run_id=handle.run_id, last_source=SyncMode.POINT.value)
            record(plan.classify(applied))
            _apply_counts(outcome, plan.counts(applied))
            outcome.status, outcome.error, outcome.duration_ms = status, error, elapsed_ms()
            tx.finish_success(handle, outcome)
    except Exception as exc:
        _reset_write_counts(outcome)
        for result in results.values():
            result.pop("class", None)
        return _fail(store, handle, outcome, exc, secrets, elapsed_ms())

    _log(outcome)
    return outcome


# --- salida ---------------------------------------------------------------------------------------


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, (list, tuple)):
        return ",".join(str(v) for v in value)
    return str(value)


_LIST_FIELDS = (
    ("api_count", "api_count"),
    ("received", "rows_received"),
    ("inserted", "rows_inserted"),
    ("updated", "rows_updated"),
    ("unchanged", "rows_unchanged"),
    ("skipped_newer", "rows_skipped_newer"),
    ("missing", "rows_missing"),
    ("requests", "requests"),
    ("http_429", "http_429"),
    ("duration_ms", "duration_ms"),
    ("sync_run_id", "sync_run_id"),
)


def _list_line(result: ListResult) -> str:
    o = result.outcome
    parts = [f"list={result.price_list_id}", f"status={o.status}", f"state={_text(result.state)}",
             f"active={_text(result.active)}"]
    parts += [f"{label}={_text(getattr(o, attr))}" for label, attr in _LIST_FIELDS]
    parts.append(f"uncatalogued={_text((o.point or {}).get('uncatalogued'))}")
    parts.append(f"last_success_at={_text(result.last_success_at)}")
    if o.error:
        parts.append(f"error={o.error}")
    parts.append(f"name={_text(result.name)}")
    return " ".join(parts)


def format_price_report(report: PriceSyncReport) -> str:
    """key=value por línea (una línea por lista); nunca payload ni token."""
    lines = ["dry_run=true"] if report.dry_run else []
    lines += [f"company={report.company_id}", "command=sync-prices", f"selection={report.selection}"]
    if report.requested_lists:
        lines.append(f"requested_lists={_text(report.requested_lists)}")
    meta = report.metadata
    if meta is not None:
        lines += [
            f"metadata_status={meta.status}",
            f"metadata_received={meta.rows_received}",
            f"metadata_inserted={meta.rows_inserted}",
            f"metadata_updated={meta.rows_updated}",
            f"metadata_missing={meta.rows_missing}",
        ]
        if meta.error:
            lines.append(f"metadata_error={meta.error}")
    lines.append(f"metadata_last_success_at={_text(report.metadata_last_success_at)}")
    lines += [
        f"new_lists={_text(report.new_lists)}",
        f"deactivated_lists={_text(report.deactivated_lists)}",
        f"reactivated_lists={_text(report.reactivated_lists)}",
        f"removed_lists={_text(report.removed_lists)}",
    ]
    statuses = [r.outcome.status for r in report.lists]
    lines += [
        f"lists={len(report.lists)}",
        f"lists_ok={statuses.count(RunStatus.SUCCESS.value)}",
        f"lists_failed={statuses.count(RunStatus.FAILED.value)}",
        f"lists_skipped={statuses.count(RunStatus.SKIPPED.value)}",
    ]
    for label, attr in (("received", "rows_received"), ("inserted", "rows_inserted"), ("updated", "rows_updated"),
                        ("unchanged", "rows_unchanged"), ("skipped_newer", "rows_skipped_newer"),
                        ("missing", "rows_missing")):
        lines.append(f"{label}={sum(getattr(r.outcome, attr) for r in report.lists)}")
    lines.append(f"uncatalogued={sum((r.outcome.point or {}).get('uncatalogued', 0) for r in report.lists)}")
    lines += [f"requests={report.requests}", f"duration_ms={report.duration_ms}"]
    lines += [_list_line(r) for r in report.lists]
    lines.append(f"status={report.status}")
    if report.error:
        lines.append(f"error={report.error}")
    return "\n".join(lines)


def format_price_point(outcome: EntityOutcome) -> str:
    point = outcome.point or {}
    results = point.get("results", {})
    statuses = [r.get("status") for r in results.values()]
    lines = ["dry_run=true"] if outcome.dry_run else []
    lines += [
        f"company={outcome.company_id}",
        "command=refresh-prices",
        f"scope={outcome.scope}",
        f"variants={_text(point.get('variant_ids'))}",
        f"price_lists={_text(point.get('price_list_ids'))}",
        f"pairs={len(results)}",
        f"fetched={statuses.count('FETCHED')}",
        f"no_rows={statuses.count('NO_ROWS')}",
        f"failed={statuses.count('FAILED')}",
        f"received={outcome.rows_received}",
        f"inserted={outcome.rows_inserted}",
        f"updated={outcome.rows_updated}",
        f"unchanged={outcome.rows_unchanged}",
        f"skipped_newer={outcome.rows_skipped_newer}",
        f"requests={outcome.requests}",
        f"duration_ms={outcome.duration_ms}",
    ]
    for key in sorted(results):
        r = results[key]
        line = f"pair={key} status={r['status']}"
        if r.get("class"):
            line += f" class={r['class']}"
        if r.get("error"):
            line += f" error={r['error']}"
        lines.append(line)
    if outcome.sync_run_id is not None:
        lines.append(f"sync_run_id={outcome.sync_run_id}")
    lines.append(f"status={outcome.status}")
    if outcome.error:
        lines.append(f"error={outcome.error}")
    return "\n".join(lines)
