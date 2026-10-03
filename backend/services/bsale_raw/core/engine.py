"""Motor genérico ``bsale_raw`` (fase 4A: entidades + FULL_RECONCILE; habilitado sólo ``offices``).

Orden obligatorio (nunca hay una transacción PostgreSQL abierta durante requests HTTP):

 1. resolver fuente (``bsale_raw.sources``) y token desde la variable de entorno;
 2. advisory lock (company, resource, scope) en conexión dedicada;
 3. ``sync_runs`` / ``sync_entity_runs`` RUNNING (transacción corta);
 4. ``snapshot_started_at`` = ahora UTC, ANTES del primer GET;
 5. fetch COMPLETO + validación (count, páginas, duplicados) + filas en memoria;
 6. transacción corta: lock de filas existentes → fusible → UPSERT con frescura → marcar
    ``missing_since`` → cerrar entity run / run / sync_state → COMMIT;
 7. liberar lock.

Si falla antes del paso 6 ninguna fila RAW cambia; la falla se registra en una transacción aparte.
"""

from __future__ import annotations

import logging
import os
import time
from contextlib import nullcontext
from typing import Any, Callable

from backend.services.bsale.companies import BsaleCompany
from backend.services.bsale_raw.core.client import build_company_client
from backend.services.bsale_raw.core.models import RunStatus, SyncMode
from backend.services.bsale_raw.core.rate_limit import CompanyRateLimiters
from backend.services.bsale_raw.core.reconcile import max_missing_pct, plan_reconcile
from backend.services.bsale_raw.core.registry import GLOBAL_SCOPE, REGISTRY, KeyKind, ResourceSpec
from backend.services.bsale_raw.core.snapshot import Clock, build_rows, fetch_snapshot, utc_now
from backend.services.bsale_raw.core.store import (
    EntityOutcome,
    LockBusyError,
    RawStore,
    RunHandle,
    SourceConfig,
)

logger = logging.getLogger(__name__)

TRIGGER_MANUAL = "MANUAL"
MAX_ERROR_CHARS = 2000

ClientFactory = Callable[[SourceConfig, str, ResourceSpec], Any]

_LIMITERS = CompanyRateLimiters()


class UnsupportedSyncError(ValueError):
    """Recurso o modo no habilitado en el motor."""


class TokenConfigError(RuntimeError):
    """Variable de entorno del token ausente o vacía (el mensaje sólo contiene el NOMBRE)."""


class FuseTrippedError(RuntimeError):
    """Fusible de faltantes: la corrida falla sin escribir."""


def pipeline_spec(resource: str) -> ResourceSpec:
    import backend.services.bsale_raw.resources  # noqa: F401  (registra los recursos)

    if resource not in REGISTRY.pipeline_names():
        raise UnsupportedSyncError(
            f"recurso no habilitado en el motor: {resource} (habilitados: {REGISTRY.pipeline_names()})"
        )
    spec = REGISTRY.get(resource)
    if spec.key_kind is not KeyKind.ENTITY:
        raise UnsupportedSyncError(f"{resource}: key_kind {spec.key_kind.value} aún no soportado")
    return spec


def read_token(source: SourceConfig, getenv: Callable[[str], str | None] = os.getenv) -> str:
    token = (getenv(source.token_env) or "").strip()
    if not token:
        raise TokenConfigError(f"variable de entorno {source.token_env} no definida o vacía")
    return token


def redact(text: str, secrets: list[str]) -> str:
    for secret in secrets:
        if secret:
            text = text.replace(secret, "***")
    return text


def sanitize_error(exc: BaseException, secrets: list[str]) -> str:
    return redact(f"{type(exc).__name__}: {exc}", secrets)[:MAX_ERROR_CHARS]


def api_path(endpoint: str) -> str:
    """El registry documenta rutas como ``/v1/offices.json``; ``BsaleHttpClient`` ya usa base ``…/v1``."""
    path = endpoint.lstrip("/")
    return path[len("v1/"):] if path.startswith("v1/") else path


def default_client_factory(source: SourceConfig, token: str, spec: ResourceSpec) -> Any:
    company = BsaleCompany(
        company_id=source.company_id, name=source.name, token_env=source.token_env, token=token
    )
    return build_company_client(company, _LIMITERS, spec.request_priority)


def _collect_request_stats(client: Any, outcome: EntityOutcome) -> None:
    stats = getattr(getattr(client, "session", None), "stats", None)
    if stats is not None:
        outcome.requests = stats.requests
        outcome.http_429 = stats.http_429
        outcome.http_5xx = stats.http_5xx


def _reset_write_counts(outcome: EntityOutcome) -> None:
    outcome.rows_inserted = outcome.rows_updated = outcome.rows_unchanged = 0
    outcome.rows_skipped_newer = outcome.rows_missing = outcome.rows_deleted = 0


def _fail(
    store: RawStore,
    handle: RunHandle | None,
    outcome: EntityOutcome,
    exc: BaseException,
    secrets: list[str],
    duration_ms: int,
) -> EntityOutcome:
    outcome.status = RunStatus.FAILED.value
    outcome.error = sanitize_error(exc, secrets)
    outcome.duration_ms = duration_ms
    if handle is not None:
        try:
            store.finish_failed(handle, outcome)
        except Exception as record_exc:  # la falla original es la que importa
            note = sanitize_error(record_exc, secrets)
            logger.error("[BSALE_RAW] no se pudo registrar la falla: %s", note)
            outcome.error = f"{outcome.error} | registro de falla no guardado: {note}"[:MAX_ERROR_CHARS]
    logger.error(
        "[BSALE_RAW] company=%s resource=%s mode=%s status=FAILED error=%s",
        outcome.company_id,
        outcome.resource,
        outcome.mode,
        outcome.error,
    )
    return outcome


def run_entity_sync(
    *,
    store: RawStore,
    company_id: int,
    resource: str,
    mode: SyncMode = SyncMode.FULL_RECONCILE,
    dry_run: bool = False,
    client_factory: ClientFactory = default_client_factory,
    clock: Clock = utc_now,
    monotonic: Callable[[], float] = time.perf_counter,
    getenv: Callable[[str], str | None] = os.getenv,
    trigger: str = TRIGGER_MANUAL,
    host: str | None = None,
) -> EntityOutcome:
    """Nunca lanza por fallas de API/BD: devuelve el ``EntityOutcome`` con status y error saneado."""
    spec = pipeline_spec(resource)
    if mode is not SyncMode.FULL_RECONCILE:
        raise UnsupportedSyncError(f"modo no habilitado en fase 4A: {mode.value}")
    if mode not in spec.pipeline_modes or not spec.full_scan_global_allowed:
        raise UnsupportedSyncError(f"{resource}: barrido completo no habilitado (modos {spec.pipeline_modes})")
    scope = GLOBAL_SCOPE
    outcome = EntityOutcome(company_id=company_id, resource=resource, scope=scope, mode=mode.value, dry_run=dry_run)
    t0 = monotonic()

    try:
        source = store.resolve_source(company_id)
        token = read_token(source, getenv)
        threshold = max_missing_pct(resource, getenv)
    except Exception as exc:  # fuente, token, umbral o conexión: sin run, sin escrituras
        return _fail(store, None, outcome, exc, [], int((monotonic() - t0) * 1000))

    secrets = [token]
    lock = nullcontext() if dry_run else store.advisory_lock(company_id, resource, scope)
    try:
        with lock:
            return _run_locked(
                store=store,
                spec=spec,
                source=source,
                token=token,
                threshold=threshold,
                outcome=outcome,
                mode=mode,
                client_factory=client_factory,
                clock=clock,
                monotonic=monotonic,
                t0=t0,
                trigger=trigger,
                host=host,
                secrets=secrets,
            )
    except LockBusyError as exc:
        outcome.status = RunStatus.SKIPPED.value
        outcome.error = sanitize_error(exc, secrets)
        outcome.duration_ms = int((monotonic() - t0) * 1000)
        logger.warning("[BSALE_RAW] %s", outcome.error)
        return outcome
    except Exception as exc:  # p. ej. sin conexión para el lock: no se llegó a escribir nada
        return _fail(store, None, outcome, exc, secrets, int((monotonic() - t0) * 1000))


def _run_locked(
    *,
    store: RawStore,
    spec: ResourceSpec,
    source: SourceConfig,
    token: str,
    threshold: float,
    outcome: EntityOutcome,
    mode: SyncMode,
    client_factory: ClientFactory,
    clock: Clock,
    monotonic: Callable[[], float],
    t0: float,
    trigger: str,
    host: str | None,
    secrets: list[str],
) -> EntityOutcome:
    company_id = source.company_id

    def elapsed_ms() -> int:
        return int((monotonic() - t0) * 1000)

    handle: RunHandle | None = None
    if not outcome.dry_run:
        try:
            handle = store.start_run(
                mode=mode.value, trigger=trigger, host=host, company_id=company_id,
                resource=spec.name, scope=outcome.scope,
            )
        except Exception as exc:
            return _fail(store, None, outcome, exc, secrets, elapsed_ms())
        outcome.sync_run_id = handle.run_id

    snapshot_started_at = clock()
    outcome.snapshot_started_at = snapshot_started_at
    client = None
    try:
        client = client_factory(source, token, spec)
        snapshot = fetch_snapshot(client, api_path(spec.list_endpoint), clock=clock)
        outcome.api_count = snapshot.api_count
        outcome.pages = snapshot.pages
        rows = build_rows(spec, company_id, snapshot)
        outcome.rows_received = len(rows)
    except Exception as exc:
        _collect_request_stats(client, outcome)
        return _fail(store, handle, outcome, exc, secrets, elapsed_ms())
    _collect_request_stats(client, outcome)

    if outcome.dry_run:
        try:
            existing = store.read_existing(spec, company_id)
        except Exception as exc:
            return _fail(store, None, outcome, exc, secrets, elapsed_ms())
        plan = plan_reconcile(existing, rows, snapshot_started_at=snapshot_started_at, threshold_pct=threshold)
        outcome.fuse = plan.fuse_json()
        outcome.rows_inserted = plan.predicted["inserted"]
        outcome.rows_updated = plan.predicted["updated"]
        outcome.rows_unchanged = plan.predicted["unchanged"]
        outcome.rows_skipped_newer = plan.predicted["skipped_newer"]
        outcome.rows_missing = len(plan.missing_ids)
        outcome.status = RunStatus.FAILED.value if plan.fuse_tripped else RunStatus.SUCCESS.value
        outcome.error = f"fusible: {plan.fuse_reason}" if plan.fuse_tripped else None
        outcome.duration_ms = elapsed_ms()
        return outcome

    assert handle is not None
    try:
        with store.transaction() as tx:
            existing = tx.lock_existing(spec, company_id)
            plan = plan_reconcile(existing, rows, snapshot_started_at=snapshot_started_at, threshold_pct=threshold)
            outcome.fuse = plan.fuse_json()
            if plan.fuse_tripped:
                raise FuseTrippedError(f"fusible: {plan.fuse_reason}; no se escribió nada")
            applied = tx.upsert(spec, rows, sync_run_id=handle.run_id, last_source=mode.value)
            counts = plan.counts(applied)
            outcome.rows_inserted = counts["inserted"]
            outcome.rows_updated = counts["updated"]
            outcome.rows_unchanged = counts["unchanged"]
            outcome.rows_skipped_newer = counts["skipped_newer"]
            outcome.rows_missing = tx.mark_missing(spec, company_id, plan.missing_ids, snapshot_started_at)
            outcome.rows_deleted = 0  # entidades: nunca se borra
            outcome.status = RunStatus.SUCCESS.value
            outcome.duration_ms = elapsed_ms()
            tx.finish_success(handle, outcome)
    except Exception as exc:
        _reset_write_counts(outcome)
        return _fail(store, handle, outcome, exc, secrets, elapsed_ms())

    logger.info(
        "[BSALE_RAW] company=%s resource=%s mode=%s status=%s api_count=%s received=%s inserted=%s "
        "updated=%s unchanged=%s skipped_newer=%s missing=%s requests=%s duration_ms=%s",
        company_id, spec.name, mode.value, outcome.status, outcome.api_count, outcome.rows_received,
        outcome.rows_inserted, outcome.rows_updated, outcome.rows_unchanged, outcome.rows_skipped_newer,
        outcome.rows_missing, outcome.requests, outcome.duration_ms,
    )
    return outcome
