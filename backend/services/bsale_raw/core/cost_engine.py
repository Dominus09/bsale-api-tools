"""Motor de costos ``bsale_raw.variant_costs`` (clave ``(company_id, variant_id)``).

Fuente: ``GET /v1/variants/{id}/costs.json``, un request por variante, sin filtro por sucursal: el
costo es de la variante en la empresa, no de una sucursal.

RAW guarda EXCLUSIVAMENTE lo que entrega Bsale:

- ``payload``: la respuesta completa, sin cambios (``averageCost``, ``totalCost``, ``history``...);
- ``average_cost`` = ``averageCost`` (costo NETO de Bsale) y ``total_cost`` = ``totalCost``, como
  ``Decimal`` exacto del valor recibido (``0`` explícito = 0; ``null`` = NULL);
- ``history_count`` / ``last_admission_date``: conteo y fecha máxima de ``history`` (sin recalcular
  costos); ``history_complete`` siempre false (``history`` llega sin metadata de paginación).

Aquí nunca se calcula costo bruto, impuestos ni factores: eso es de la capa de negocio, con
``bsale_raw.taxes`` y las reglas de cada impuesto del producto.

Modos (existentes en los CHECK de ``sync_runs``):

- ``SCANNER``: un lote de variantes por corrida desde ``bsale_raw.variants`` (``missing_since IS
  NULL``, activas e inactivas) en orden de ``bsale_id``, con cursor en ``sync_cursors``; al llegar al
  final la vuelta siguiente reinicia. Lock ``(company, variant_costs, global)``. No borra nada.
- ``POINT``: variantes explícitas con prioridad P0, sin lock (la frescura por fila decide).

Orden: fuente → [lock] → lote → run RUNNING → GET por variante SIN transacción → UNA transacción
corta (UPSERT con frescura + cursor + run/state). Una variante con error no se escribe ni invalida a
las demás (PARTIAL). Ritmo propio (``BSALE_RAW_COST_RPS``, 2 rps por defecto) para no competir con
el scanner de stock, que corre en otro proceso con su propio limitador.
"""

from __future__ import annotations

import dataclasses
import logging
import os
import time
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Callable, Iterable, Iterator, Protocol

from backend.services.bsale.companies import BsaleCompany
from backend.services.bsale.http_client import BsaleHttpError
from backend.services.bsale_raw.core.client import build_company_client
from backend.services.bsale_raw.core.engine import (
    TRIGGER_MANUAL,
    UnsupportedSyncError,
    _collect_request_stats,
    _fail,
    _reset_write_counts,
    api_path,
    read_token,
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
from backend.services.bsale_raw.core.reconcile import ExistingRow, ReconcilePlan, plan_reconcile
from backend.services.bsale_raw.core.registry import (
    GLOBAL_SCOPE,
    POINT_STATE_SCOPE,
    REGISTRY,
    ResourceSpec,
    optional_numeric,
    optional_unix_date,
    point_scope,
    variant_range_scope,
)
from backend.services.bsale_raw.core.snapshot import Clock, utc_now
from backend.services.bsale_raw.core.store import (
    EntityOutcome,
    LockBusyError,
    PgRawStore,
    PgRawTx,
    RunHandle,
    SourceConfig,
)

logger = logging.getLogger(__name__)

RESOURCE = "variant_costs"
CURSOR_NAME = "scanner"
SCANNER_STATE_SCOPE = "scanner"
DEFAULT_BATCH = 1000
MAX_BATCH = 5000
MAX_POINT_VARIANTS = 500
# Tras N variantes seguidas con error de API se deja de pedir el resto del lote (caída, token
# revocado, 429 sostenido); el cursor queda en la última variante intentada.
MAX_CONSECUTIVE_FAILURES = 10
# Reintentos: hasta RETRY_PER_RUN variantes fallidas al inicio de cada corrida, MAX_RETRY_ATTEMPTS
# veces cada una; la cola guarda como máximo MAX_RETRY_TRACKED (el resto lo retoma la vuelta).
RETRY_PER_RUN = 100
MAX_RETRY_ATTEMPTS = 3
MAX_RETRY_TRACKED = 1000
MAX_FAILED_IN_SUMMARY = 50
DEFAULT_COST_RPS = 2.0
POINT_PRIORITY = RequestPriority.P0_TARGETED

CostClientFactory = Callable[[SourceConfig, str, ResourceSpec], Any]


class CostPayloadError(ValueError):
    """Respuesta de costos con forma inesperada: la variante no se escribe."""


def cost_spec() -> ResourceSpec:
    import backend.services.bsale_raw.resources  # noqa: F401  (registra los recursos)

    return REGISTRY.get(RESOURCE)


# --- ritmo propio ---------------------------------------------------------------------------------


def cost_rate_config(getenv: Callable[[str], str | None] = os.getenv) -> RateLimitConfig:
    """``BSALE_RAW_COST_RPS`` (default 2). Tope: el presupuesto por empresa del limitador general."""
    ceiling = BSALE_DOCUMENTED_MAX_REQUESTS / BSALE_DOCUMENTED_WINDOW_SECONDS * DEFAULT_BUDGET_FRACTION
    raw = (getenv("BSALE_RAW_COST_RPS") or "").strip()
    rps = float(raw) if raw else DEFAULT_COST_RPS
    if not 0 < rps <= ceiling:
        raise ValueError(f"BSALE_RAW_COST_RPS inválido: {rps} (rango (0, {ceiling}])")
    return RateLimitConfig(requests_per_second=rps, burst=max(1, int(rps)))


_COST_LIMITERS = CompanyRateLimiters(lambda cid: PriorityRateLimiter(TokenBucket(cost_rate_config())))


def default_cost_client_factory(source: SourceConfig, token: str, spec: ResourceSpec) -> Any:
    company = BsaleCompany(company_id=source.company_id, name=source.name, token_env=source.token_env, token=token)
    return build_company_client(company, _COST_LIMITERS, spec.request_priority)


# --- filas ----------------------------------------------------------------------------------------


@dataclass(frozen=True)
class CostRow:
    company_id: int
    variant_id: int
    average_cost: Any
    total_cost: Any
    history_count: int | None
    last_admission_date: date | None
    payload: dict[str, Any] = field(repr=False)
    payload_hash: str
    api_fetched_at: datetime


def cost_key(row: CostRow) -> int:
    return row.variant_id


def build_cost_row(company_id: int, variant_id: int, payload: Any, fetched_at: datetime) -> CostRow:
    """Valida la forma y extrae columnas de búsqueda; el payload se guarda tal cual."""
    if not isinstance(payload, dict):
        raise CostPayloadError(f"respuesta no es objeto: {type(payload).__name__}")
    if "averageCost" not in payload:
        raise CostPayloadError("respuesta sin averageCost")
    try:
        average_cost = optional_numeric(payload.get("averageCost"))
        total_cost = optional_numeric(payload.get("totalCost"))
    except ValueError as exc:
        raise CostPayloadError(f"averageCost/totalCost: {exc}") from None

    history = payload.get("history")
    history_count: int | None = None
    last_admission: date | None = None
    if history is not None:
        if not isinstance(history, list) or any(not isinstance(h, dict) for h in history):
            raise CostPayloadError("history no es lista de objetos")
        history_count = len(history)
        dates = []
        for entry in history:
            try:
                admitted = optional_unix_date(entry.get("admissionDate"))
            except ValueError as exc:
                raise CostPayloadError(f"history.admissionDate: {exc}") from None
            if admitted is not None:
                dates.append(admitted)
        last_admission = max(dates) if dates else None

    return CostRow(
        company_id=company_id,
        variant_id=variant_id,
        average_cost=average_cost,
        total_cost=total_cost,
        history_count=history_count,
        last_admission_date=last_admission,
        payload=payload,
        payload_hash=payload_hash(payload),
        api_fetched_at=fetched_at,
    )


@dataclass
class CostFetch:
    rows: list[CostRow] = field(default_factory=list)
    failed: dict[int, str] = field(default_factory=dict)
    # Fallas propias de la variante (404 o respuesta inválida): la API respondió, no es una caída.
    answered_failures: set[int] = field(default_factory=set)
    attempted: list[int] = field(default_factory=list)
    aborted: bool = False


def _variant_level(exc: BaseException) -> bool:
    return isinstance(exc, CostPayloadError) or (isinstance(exc, BsaleHttpError) and exc.status == 404)


def fetch_costs(
    client: Any,
    spec: ResourceSpec,
    company_id: int,
    variant_ids: list[int],
    *,
    clock: Clock,
    secrets: list[str],
    max_consecutive_failures: int = MAX_CONSECUTIVE_FAILURES,
) -> CostFetch:
    """GET por variante (sin transacción abierta). Nunca lanza: los errores quedan por variante."""
    result = CostFetch()
    streak = 0
    for variant_id in variant_ids:
        if streak >= max_consecutive_failures:
            result.aborted = True
            break
        result.attempted.append(variant_id)
        try:
            data = client.get_json(api_path(spec.list_endpoint.format(parent_id=variant_id)))
            result.rows.append(build_cost_row(company_id, variant_id, data, clock()))
        except Exception as exc:
            result.failed[variant_id] = sanitize_error(exc, secrets)
            if _variant_level(exc):
                result.answered_failures.add(variant_id)
                streak = 0
            else:
                streak += 1
        else:
            streak = 0
    return result


# --- cursor del scanner ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CostCursor:
    """Posición del scanner + cola de reintentos ``{variant_id: intentos fallidos}``.

    Una variante que falla se reintenta al inicio de las corridas siguientes (no espera la vuelta
    completa); tras ``MAX_RETRY_ATTEMPTS`` fallas sale de la cola y la vuelve a tomar la vuelta
    siguiente. Nunca se olvida: sigue en ``bsale_raw.variants`` y el recorrido la incluye.
    """

    last_variant_id: int = 0
    lap: int = 1
    lap_started_at: datetime | None = None
    last_completed_lap: int | None = None
    last_completed_lap_at: str | None = None
    retry: dict[int, int] = field(default_factory=dict)

    def value(self) -> dict[str, Any]:
        return {
            "last_variant_id": self.last_variant_id,
            "lap": self.lap,
            "last_completed_lap": self.last_completed_lap,
            "last_completed_lap_at": self.last_completed_lap_at,
            "retry": {str(v): n for v, n in sorted(self.retry.items())},
        }

    @classmethod
    def from_value(cls, value: Any, cycle_started_at: datetime | None) -> "CostCursor":
        if not isinstance(value, dict):
            return cls(lap_started_at=cycle_started_at)
        last = value.get("last_variant_id")
        lap = value.get("lap")
        completed = value.get("last_completed_lap")
        retry: dict[int, int] = {}
        raw_retry = value.get("retry")
        if isinstance(raw_retry, dict):
            for key, attempts in raw_retry.items():
                if str(key).isdigit() and int(key) > 0 and isinstance(attempts, int) and attempts > 0:
                    retry[int(key)] = attempts
        return cls(
            last_variant_id=last if isinstance(last, int) and last >= 0 else 0,
            lap=lap if isinstance(lap, int) and lap >= 1 else 1,
            lap_started_at=cycle_started_at,
            last_completed_lap=completed if isinstance(completed, int) else None,
            last_completed_lap_at=value.get("last_completed_lap_at"),
            retry=retry,
        )


def next_retry(previous: dict[int, int], fetched: CostFetch) -> tuple[dict[int, int], list[int]]:
    """Cola siguiente y variantes que agotaron sus reintentos (las retoma la vuelta siguiente)."""
    attempted = set(fetched.attempted)
    queue = {v: n for v, n in previous.items() if v not in attempted}  # no intentadas: se conservan
    dropped: list[int] = []
    for variant_id in sorted(fetched.failed):
        attempts = previous.get(variant_id, 0) + 1
        if attempts >= MAX_RETRY_ATTEMPTS:
            dropped.append(variant_id)
        else:
            queue[variant_id] = attempts
    if len(queue) > MAX_RETRY_TRACKED:
        ordered = sorted(queue)
        dropped.extend(ordered[MAX_RETRY_TRACKED:])
        queue = {v: queue[v] for v in ordered[:MAX_RETRY_TRACKED]}
    return queue, sorted(dropped)


# --- persistencia ---------------------------------------------------------------------------------

COST_TABLE = "bsale_raw.variant_costs"
COST_COLUMNS = (
    "company_id", "variant_id", "average_cost", "total_cost", "history_count", "last_admission_date",
    "history_complete", "payload", "payload_hash", "first_seen_at", "last_seen_at", "last_changed_at",
    "api_fetched_at", "last_source", "sync_run_id",
)
COST_TEMPLATE = "(%s, %s, %s, %s, %s, %s, false, %s, %s, now(), now(), now(), %s, %s, %s)"

UPSERT_COSTS_SQL = (
    f"INSERT INTO {COST_TABLE} AS t ({', '.join(COST_COLUMNS)}) VALUES %s\n"
    "ON CONFLICT (company_id, variant_id) DO UPDATE SET\n    "
    + ",\n    ".join([
        "average_cost = EXCLUDED.average_cost",
        "total_cost = EXCLUDED.total_cost",
        "history_count = EXCLUDED.history_count",
        "last_admission_date = EXCLUDED.last_admission_date",
        "history_complete = EXCLUDED.history_complete",
        "payload = EXCLUDED.payload",
        "payload_hash = EXCLUDED.payload_hash",
        "last_seen_at = EXCLUDED.last_seen_at",
        "last_changed_at = CASE WHEN t.payload_hash IS DISTINCT FROM EXCLUDED.payload_hash "
        "THEN EXCLUDED.last_changed_at ELSE t.last_changed_at END",
        "api_fetched_at = EXCLUDED.api_fetched_at",
        "last_source = EXCLUDED.last_source",
        "sync_run_id = EXCLUDED.sync_run_id",
    ])
    + "\nWHERE t.api_fetched_at <= EXCLUDED.api_fetched_at\nRETURNING variant_id"
)

SELECT_EXISTING_COSTS_SQL = (
    f"SELECT variant_id, payload_hash, api_fetched_at FROM {COST_TABLE} "
    "WHERE company_id = %s AND variant_id = ANY(%s)"
)

SELECT_VARIANT_BATCH_SQL = (
    "SELECT bsale_id FROM bsale_raw.variants "
    "WHERE company_id = %s AND missing_since IS NULL AND bsale_id > %s "
    "ORDER BY bsale_id LIMIT %s"
)

SELECT_CURSOR_SQL = (
    "SELECT cursor_value, cycle_started_at FROM bsale_raw.sync_cursors "
    "WHERE company_id = %s AND resource = %s AND scope = %s AND cursor_name = %s"
)

UPSERT_CURSOR_SQL = """
INSERT INTO bsale_raw.sync_cursors AS c
    (company_id, resource, scope, cursor_name, cursor_value, cycle_started_at, updated_at, sync_run_id)
VALUES (%s, %s, %s, %s, %s, %s, now(), %s)
ON CONFLICT (company_id, resource, scope, cursor_name) DO UPDATE SET
    cursor_value = EXCLUDED.cursor_value,
    cycle_started_at = EXCLUDED.cycle_started_at,
    updated_at = now(),
    sync_run_id = EXCLUDED.sync_run_id
"""


def _existing_costs(rows: list[tuple]) -> dict[int, ExistingRow]:
    return {
        int(r[0]): ExistingRow(bsale_id=int(r[0]), payload_hash=r[1], api_fetched_at=r[2], missing_since=None)
        for r in rows
    }


class CostTx(Protocol):
    def read_existing_costs(self, company_id: int, variant_ids: list[int]) -> dict[int, ExistingRow]: ...

    def upsert_costs(self, rows: list[CostRow], *, sync_run_id: int, last_source: str) -> set[int]: ...

    def write_cursor(self, company_id: int, cursor: CostCursor, *, sync_run_id: int) -> None: ...

    def finish_success(self, handle: RunHandle, outcome: EntityOutcome) -> None: ...


class CostStore(Protocol):
    def resolve_source(self, company_id: int) -> SourceConfig: ...

    def advisory_lock(self, company_id: int, resource: str, scope: str) -> Any: ...

    def start_run(
        self, *, mode: str, trigger: str, host: str | None, company_id: int, resource: str, scope: str,
        state_scope: str | None = None,
    ) -> RunHandle: ...

    def read_cursor(self, company_id: int) -> CostCursor: ...

    def select_variants(self, company_id: int, after_variant_id: int, limit: int) -> list[int]: ...

    def read_existing_costs(self, company_id: int, variant_ids: list[int]) -> dict[int, ExistingRow]: ...

    def cost_transaction(self) -> Any: ...

    def finish_failed(self, handle: RunHandle, outcome: EntityOutcome) -> None: ...


@dataclass
class PgCostTx(PgRawTx):
    def read_existing_costs(self, company_id: int, variant_ids: list[int]) -> dict[int, ExistingRow]:
        if not variant_ids:
            return {}
        self.cur.execute(SELECT_EXISTING_COSTS_SQL, (company_id, list(variant_ids)))
        return _existing_costs(self.cur.fetchall())

    def upsert_costs(self, rows: list[CostRow], *, sync_run_id: int, last_source: str) -> set[int]:
        if not rows:
            return set()
        from psycopg2.extras import Json, execute_values

        values = [
            (
                r.company_id, r.variant_id, r.average_cost, r.total_cost, r.history_count,
                r.last_admission_date, Json(r.payload), r.payload_hash, r.api_fetched_at,
                last_source, sync_run_id,
            )
            for r in rows
        ]
        returned = execute_values(
            self.cur, UPSERT_COSTS_SQL, values, template=COST_TEMPLATE, page_size=500, fetch=True
        )
        return {int(r[0]) for r in returned}

    def write_cursor(self, company_id: int, cursor: CostCursor, *, sync_run_id: int) -> None:
        from psycopg2.extras import Json

        self.cur.execute(
            UPSERT_CURSOR_SQL,
            (company_id, RESOURCE, GLOBAL_SCOPE, CURSOR_NAME, Json(cursor.value()), cursor.lap_started_at, sync_run_id),
        )


class PgCostStore(PgRawStore):
    def read_cursor(self, company_id: int) -> CostCursor:
        cur = self._work().cursor()
        try:
            cur.execute(SELECT_CURSOR_SQL, (company_id, RESOURCE, GLOBAL_SCOPE, CURSOR_NAME))
            row = cur.fetchone()
        finally:
            cur.close()
        return CostCursor() if row is None else CostCursor.from_value(row[0], row[1])

    def select_variants(self, company_id: int, after_variant_id: int, limit: int) -> list[int]:
        cur = self._work().cursor()
        try:
            cur.execute(SELECT_VARIANT_BATCH_SQL, (company_id, after_variant_id, limit))
            return [int(r[0]) for r in cur.fetchall()]
        finally:
            cur.close()

    def read_existing_costs(self, company_id: int, variant_ids: list[int]) -> dict[int, ExistingRow]:
        if not variant_ids:
            return {}
        cur = self._work().cursor()
        try:
            cur.execute(SELECT_EXISTING_COSTS_SQL, (company_id, list(variant_ids)))
            return _existing_costs(cur.fetchall())
        finally:
            cur.close()

    @contextmanager
    def cost_transaction(self) -> Iterator[PgCostTx]:
        with self.transaction() as tx:
            yield PgCostTx(tx.cur)


# --- orquestación ---------------------------------------------------------------------------------


def _validate_id(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise UnsupportedSyncError(f"{name} inválido: {value!r}")
    return value


def _plan(existing: dict[int, ExistingRow], rows: list[CostRow], started: datetime) -> ReconcilePlan:
    return plan_reconcile(existing, rows, snapshot_started_at=started, threshold_pct=100.0, key=cost_key)


def _apply_counts(outcome: EntityOutcome, counts: dict[str, int]) -> None:
    outcome.rows_inserted = counts["inserted"]
    outcome.rows_updated = counts["updated"]
    outcome.rows_unchanged = counts["unchanged"]
    outcome.rows_skipped_newer = counts["skipped_newer"]


def _record_fetch(outcome: EntityOutcome, fetched: CostFetch, requested: int) -> None:
    outcome.api_count = len(fetched.attempted)
    outcome.rows_received = len(fetched.rows)
    failed_ids = sorted(fetched.failed)
    outcome.point.update(
        variants=requested,
        attempted=len(fetched.attempted),
        fetched=len(fetched.rows),
        failed=len(failed_ids),
        not_attempted=requested - len(fetched.attempted),
        failed_sample={str(v): fetched.failed[v] for v in failed_ids[:MAX_FAILED_IN_SUMMARY]},
    )


def _status(fetched: CostFetch, requested: int) -> tuple[str, str | None]:
    failed = len(fetched.failed)
    skipped = requested - len(fetched.attempted)
    if not failed and not skipped:
        return RunStatus.SUCCESS.value, None
    parts = []
    if failed:
        parts.append(f"{failed} variantes con error (p. ej. {sorted(fetched.failed)[:10]})")
    if skipped:
        parts.append(f"{skipped} variantes no intentadas tras {MAX_CONSECUTIVE_FAILURES} errores seguidos")
    return RunStatus.PARTIAL.value, "; ".join(parts)


def _log(outcome: EntityOutcome) -> None:
    logger.info(
        "[BSALE_RAW] company=%s resource=%s scope=%s mode=%s status=%s attempted=%s received=%s inserted=%s "
        "updated=%s unchanged=%s skipped_newer=%s failed=%s requests=%s duration_ms=%s",
        outcome.company_id, outcome.resource, outcome.scope, outcome.mode, outcome.status, outcome.api_count,
        outcome.rows_received, outcome.rows_inserted, outcome.rows_updated, outcome.rows_unchanged,
        outcome.rows_skipped_newer, (outcome.point or {}).get("failed"), outcome.requests, outcome.duration_ms,
    )


def _execute(
    *,
    store: CostStore,
    spec: ResourceSpec,
    source: SourceConfig,
    token: str,
    variant_ids: list[int],
    outcome: EntityOutcome,
    mode: SyncMode,
    client_factory: CostClientFactory,
    clock: Clock,
    elapsed_ms: Callable[[], int],
    trigger: str,
    host: str | None,
    secrets: list[str],
    next_cursor: Callable[[CostFetch], CostCursor] | None,
) -> EntityOutcome:
    company_id = source.company_id
    handle: RunHandle | None = None
    if not outcome.dry_run:
        try:
            handle = store.start_run(
                mode=mode.value, trigger=trigger, host=host, company_id=company_id,
                resource=RESOURCE, scope=outcome.scope, state_scope=outcome.state_scope,
            )
        except Exception as exc:
            return _fail(store, None, outcome, exc, secrets, elapsed_ms())
        outcome.sync_run_id = handle.run_id

    outcome.snapshot_started_at = clock()
    client = None
    try:
        client = client_factory(source, token, spec)
        fetched = fetch_costs(client, spec, company_id, variant_ids, clock=clock, secrets=secrets)
    except Exception as exc:
        _collect_request_stats(client, outcome)
        return _fail(store, handle, outcome, exc, secrets, elapsed_ms())
    _collect_request_stats(client, outcome)
    _record_fetch(outcome, fetched, len(variant_ids))

    if not fetched.rows:
        first = next(iter(fetched.failed.values()), "sin respuesta")
        failure = RuntimeError(f"ninguna variante con costo válido ({len(fetched.failed)} con error; p. ej. {first})")
        # Si la API respondió (404 / respuesta inválida) el cursor y la cola avanzan igual: un lote
        # de variantes sin costo obtenible no puede bloquear el recorrido. Caída total: no avanza.
        if next_cursor is not None and fetched.answered_failures and handle is not None:
            try:
                with store.cost_transaction() as tx:
                    cursor = next_cursor(fetched)
                    tx.write_cursor(company_id, cursor, sync_run_id=handle.run_id)
                    outcome.point["cursor"] = cursor.value()
            except Exception as exc:
                outcome.point.pop("cursor", None)
                return _fail(store, handle, outcome, exc, secrets, elapsed_ms())
        return _fail(store, handle, outcome, failure, secrets, elapsed_ms())
    status, error = _status(fetched, len(variant_ids))
    received = [r.variant_id for r in fetched.rows]

    if outcome.dry_run:
        try:
            plan = _plan(store.read_existing_costs(company_id, received), fetched.rows, outcome.snapshot_started_at)
        except Exception as exc:
            return _fail(store, None, outcome, exc, secrets, elapsed_ms())
        _apply_counts(outcome, plan.predicted)
        outcome.status, outcome.error, outcome.duration_ms = status, error, elapsed_ms()
        return outcome

    assert handle is not None
    try:
        with store.cost_transaction() as tx:
            plan = _plan(tx.read_existing_costs(company_id, received), fetched.rows, outcome.snapshot_started_at)
            applied = tx.upsert_costs(fetched.rows, sync_run_id=handle.run_id, last_source=mode.value)
            _apply_counts(outcome, plan.counts(applied))
            if next_cursor is not None:
                cursor = next_cursor(fetched)
                tx.write_cursor(company_id, cursor, sync_run_id=handle.run_id)
                outcome.point["cursor"] = cursor.value()
            outcome.status, outcome.error, outcome.duration_ms = status, error, elapsed_ms()
            tx.finish_success(handle, outcome)
    except Exception as exc:
        _reset_write_counts(outcome)
        outcome.point.pop("cursor", None)
        return _fail(store, handle, outcome, exc, secrets, elapsed_ms())

    _log(outcome)
    return outcome


def run_cost_scan(
    *,
    store: CostStore,
    company_id: int,
    batch_size: int = DEFAULT_BATCH,
    dry_run: bool = False,
    client_factory: CostClientFactory = default_cost_client_factory,
    clock: Clock = utc_now,
    monotonic: Callable[[], float] = time.perf_counter,
    getenv: Callable[[str], str | None] = os.getenv,
    trigger: str = TRIGGER_MANUAL,
    host: str | None = None,
) -> EntityOutcome:
    """SCANNER de un lote. Nunca lanza por fallas de API/BD: devuelve el ``EntityOutcome``."""
    spec = cost_spec()
    if isinstance(batch_size, bool) or not isinstance(batch_size, int) or not 1 <= batch_size <= MAX_BATCH:
        raise UnsupportedSyncError(f"batch inválido: {batch_size!r} (1..{MAX_BATCH})")
    outcome = EntityOutcome(
        company_id=company_id, resource=RESOURCE, scope=GLOBAL_SCOPE, mode=SyncMode.SCANNER.value,
        dry_run=dry_run, state_scope=SCANNER_STATE_SCOPE, point={"batch_size": batch_size},
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

    lock = nullcontext() if dry_run else store.advisory_lock(company_id, RESOURCE, GLOBAL_SCOPE)
    try:
        with lock:
            try:
                cursor = store.read_cursor(company_id)
                # Siempre queda al menos un lugar para el recorrido: los reintentos no lo bloquean.
                retried = sorted(cursor.retry)[: min(RETRY_PER_RUN, batch_size - 1)]
                regular_limit = batch_size - len(retried)
                regular = store.select_variants(company_id, cursor.last_variant_id, regular_limit)
                wrapped = False
                if not regular and cursor.last_variant_id > 0:
                    regular = store.select_variants(company_id, 0, regular_limit)
                    wrapped = True
            except Exception as exc:
                return _fail(store, None, outcome, exc, secrets, elapsed_ms())
            if not regular:
                return _fail(
                    store, None, outcome,
                    RuntimeError(f"bsale_raw.variants sin variantes vigentes para company_id={company_id}"),
                    secrets, elapsed_ms(),
                )

            lap = cursor.lap + 1 if wrapped else cursor.lap
            lap_started_at = None if wrapped else cursor.lap_started_at
            reaches_end = len(regular) < regular_limit
            retried_set = set(retried)
            variant_ids = retried + [v for v in regular if v not in retried_set]
            outcome.scope = variant_range_scope(regular[0], regular[-1])
            outcome.point.update(
                first_variant_id=regular[0], last_variant_id=regular[-1], lap=lap, wrapped=wrapped,
                retried=len(retried),
            )

            def next_cursor(fetched: CostFetch) -> CostCursor:
                queue, dropped = next_retry(cursor.retry, fetched)
                outcome.point["retry_pending"] = len(queue)
                outcome.point["retry_exhausted"] = dropped[:MAX_FAILED_IN_SUMMARY]
                attempted = set(fetched.attempted)
                covered = 0
                for variant_id in regular:  # sólo avanza sobre el prefijo efectivamente intentado
                    if variant_id not in attempted:
                        break
                    covered += 1
                if covered == 0:
                    outcome.point["lap_completed"] = False
                    return dataclasses.replace(cursor, retry=queue)
                completed = reaches_end and covered == len(regular)
                outcome.point["lap_completed"] = completed
                return CostCursor(
                    last_variant_id=regular[covered - 1],
                    lap=lap,
                    lap_started_at=lap_started_at or outcome.snapshot_started_at,
                    last_completed_lap=lap if completed else cursor.last_completed_lap,
                    last_completed_lap_at=clock().isoformat() if completed else cursor.last_completed_lap_at,
                    retry=queue,
                )

            return _execute(
                store=store, spec=spec, source=source, token=token, variant_ids=variant_ids, outcome=outcome,
                mode=SyncMode.SCANNER, client_factory=client_factory, clock=clock, elapsed_ms=elapsed_ms,
                trigger=trigger, host=host, secrets=secrets, next_cursor=next_cursor,
            )
    except LockBusyError as exc:
        outcome.status = RunStatus.SKIPPED.value
        outcome.error = (
            f"scanner de costos company_id={company_id} ya en ejecución (lock {RESOURCE}/{GLOBAL_SCOPE} "
            f"ocupado); no se inicia otro: {sanitize_error(exc, secrets)}"
        )
        outcome.duration_ms = elapsed_ms()
        logger.warning("[BSALE_RAW] %s", outcome.error)
        return outcome
    except Exception as exc:
        return _fail(store, None, outcome, exc, secrets, elapsed_ms())


def refresh_costs(
    *,
    store: CostStore,
    company_id: int,
    variant_ids: Iterable[int],
    dry_run: bool = False,
    client_factory: CostClientFactory = default_cost_client_factory,
    clock: Clock = utc_now,
    monotonic: Callable[[], float] = time.perf_counter,
    getenv: Callable[[str], str | None] = os.getenv,
    trigger: str = TRIGGER_MANUAL,
    host: str | None = None,
) -> EntityOutcome:
    """POINT: costos de variantes explícitas (P0), sin lock ni cursor. Nunca lanza por fallas de API/BD."""
    spec = dataclasses.replace(cost_spec(), request_priority=POINT_PRIORITY)
    variants = sorted({_validate_id(v, "variant_id") for v in variant_ids})
    if not variants:
        raise UnsupportedSyncError("refresh de costos sin variantes")
    if len(variants) > MAX_POINT_VARIANTS:
        raise UnsupportedSyncError(f"refresh de costos con {len(variants)} variantes > máximo {MAX_POINT_VARIANTS}")
    outcome = EntityOutcome(
        company_id=company_id, resource=RESOURCE, scope=point_scope(variants), mode=SyncMode.POINT.value,
        dry_run=dry_run, state_scope=POINT_STATE_SCOPE, point={"variant_ids": variants},
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
        return _execute(
            store=store, spec=spec, source=source, token=token, variant_ids=variants, outcome=outcome,
            mode=SyncMode.POINT, client_factory=client_factory, clock=clock, elapsed_ms=elapsed_ms,
            trigger=trigger, host=host, secrets=secrets, next_cursor=None,
        )
    except Exception as exc:
        return _fail(store, None, outcome, exc, secrets, elapsed_ms())


# --- salida ---------------------------------------------------------------------------------------

_OUTPUT_FIELDS = (
    ("company", "company_id"),
    ("resource", "resource"),
    ("scope", "scope"),
    ("mode", "mode"),
    ("received", "rows_received"),
    ("inserted", "rows_inserted"),
    ("updated", "rows_updated"),
    ("unchanged", "rows_unchanged"),
    ("skipped_newer", "rows_skipped_newer"),
    ("requests", "requests"),
    ("http_429", "http_429"),
    ("http_5xx", "http_5xx"),
    ("duration_ms", "duration_ms"),
)
_POINT_FIELDS = (
    "batch_size", "first_variant_id", "last_variant_id", "lap", "wrapped", "lap_completed",
    "variants", "retried", "attempted", "failed", "not_attempted", "retry_pending",
)


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def format_cost_outcome(outcome: EntityOutcome) -> str:
    """key=value por línea; nunca payload, token ni datos del cliente."""
    lines = ["dry_run=true"] if outcome.dry_run else []
    lines += [f"{label}={_text(getattr(outcome, attr))}" for label, attr in _OUTPUT_FIELDS]
    point = outcome.point or {}
    lines += [f"{key}={_text(point[key])}" for key in _POINT_FIELDS if key in point]
    if outcome.sync_run_id is not None:
        lines.append(f"sync_run_id={outcome.sync_run_id}")
    lines.append(f"status={outcome.status}")
    if outcome.error:
        lines.append(f"error={outcome.error}")
    return "\n".join(lines)
