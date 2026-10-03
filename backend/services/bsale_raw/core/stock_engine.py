"""Motor de stock ``bsale_raw`` por empresa + sucursal (scope ``office:<id>``).

Stock no es una entidad: clave ``(company_id, variant_id, office_id)``, estado actual mutable y
partición por sucursal. Comparte con ``engine.py`` fuente, token, cliente + limitador de la
empresa (prioridad P2), saneo de errores, ``sync_runs`` / ``sync_entity_runs`` / ``sync_state``,
advisory lock y hashing. Orden: fuente → lock (company, stocks, office:N) → run RUNNING →
``snapshot_started_at`` → fetch completo de la sucursal SIN transacción → validación → filas en
memoria → UNA transacción corta (UPSERT por lotes con frescura [+ DELETE stale] + run/state).

Modos (existentes en los CHECK de ``sync_runs``; no se inventa ninguno):

- ``SCANNER``: NO destructivo. UPSERT de lo recibido; lo que no vino no se toca (ni se borra ni
  se pone en 0). Tolera que ``count`` varíe durante el barrido dentro de ``STOCK_SCAN_DRIFT``.
- ``FULL_RECONCILE``: snapshot ESTRICTO (count estable y total exacto, como catálogo) + fusible;
  borra sólo filas de ESA sucursal ausentes del snapshot y con ``api_fetched_at <=
  snapshot_started_at`` (una fila refrescada durante el barrido nunca se borra). Nunca fabrica 0.

Las tres cantidades (``quantity``, ``quantityReserved``, ``quantityAvailable``) se guardan tal
cual las entrega Bsale; nunca se recalculan (las OC 33 reservan antes de la salida física).
"""

from __future__ import annotations

import logging
import os
import time
from contextlib import nullcontext
from typing import Any, Callable

from backend.services.bsale_raw.core.engine import (
    TRIGGER_MANUAL,
    ClientFactory,
    FuseTrippedError,
    UnsupportedSyncError,
    _collect_request_stats,
    _fail,
    _reset_write_counts,
    api_path,
    default_client_factory,
    read_token,
    sanitize_error,
)
from backend.services.bsale_raw.core.models import RunStatus, SyncMode
from backend.services.bsale_raw.core.reconcile import ReconcilePlan, max_missing_pct, plan_reconcile
from backend.services.bsale_raw.core.registry import REGISTRY, KeyKind, ResourceSpec, office_scope
from backend.services.bsale_raw.core.snapshot import (
    STRICT,
    Clock,
    CountDrift,
    build_stock_rows,
    fetch_snapshot,
    stock_key,
    utc_now,
)
from backend.services.bsale_raw.core.store import EntityOutcome, LockBusyError, RawStore, RunHandle, SourceConfig

logger = logging.getLogger(__name__)

# Scanner: el count puede moverse por altas/bajas de filas durante el barrido (endpoint mutable).
# Más allá de max(10, 1 %) del primer count la corrida se considera inconsistente y falla.
STOCK_SCAN_DRIFT = CountDrift(pct=1.0, minimum=10)
OFFICE_FILTER = "officeid"


def stock_pipeline_spec(resource: str) -> ResourceSpec:
    import backend.services.bsale_raw.resources  # noqa: F401  (registra los recursos)

    if resource not in REGISTRY.pipeline_names():
        raise UnsupportedSyncError(f"recurso no habilitado en el motor: {resource}")
    spec = REGISTRY.get(resource)
    if spec.key_kind is not KeyKind.STOCK or not spec.partition_by_office:
        raise UnsupportedSyncError(f"{resource}: no es un recurso de stock por sucursal")
    return spec


def _validate_office(office_id: Any) -> int:
    if isinstance(office_id, bool) or not isinstance(office_id, int) or office_id <= 0:
        raise UnsupportedSyncError(f"office_id inválido: {office_id!r}")
    return office_id


def run_stock_sync(
    *,
    store: RawStore,
    company_id: int,
    office_id: int,
    resource: str = "stocks",
    mode: SyncMode = SyncMode.SCANNER,
    dry_run: bool = False,
    client_factory: ClientFactory = default_client_factory,
    clock: Clock = utc_now,
    monotonic: Callable[[], float] = time.perf_counter,
    getenv: Callable[[str], str | None] = os.getenv,
    trigger: str = TRIGGER_MANUAL,
    host: str | None = None,
) -> EntityOutcome:
    """Nunca lanza por fallas de API/BD: devuelve el ``EntityOutcome`` con status y error saneado."""
    spec = stock_pipeline_spec(resource)
    if mode not in spec.pipeline_modes:
        raise UnsupportedSyncError(f"{resource}: modo no habilitado: {mode.value}")
    office_id = _validate_office(office_id)
    scope = office_scope(office_id)
    outcome = EntityOutcome(company_id=company_id, resource=resource, scope=scope, mode=mode.value, dry_run=dry_run)
    t0 = monotonic()

    try:
        source = store.resolve_source(company_id)
        token = read_token(source, getenv)
        threshold = max_missing_pct(resource, getenv)
    except Exception as exc:
        return _fail(store, None, outcome, exc, [], int((monotonic() - t0) * 1000))

    secrets = [token]
    # Lock por sucursal: otra sucursal puede escanearse en paralelo; un refresh dirigido no toma
    # este lock (lo protege la frescura por fila del UPSERT).
    lock = nullcontext() if dry_run else store.advisory_lock(company_id, resource, scope)
    try:
        with lock:
            return _run_locked(
                store=store, spec=spec, source=source, token=token, office_id=office_id,
                threshold=threshold, outcome=outcome, mode=mode, client_factory=client_factory,
                clock=clock, monotonic=monotonic, t0=t0, trigger=trigger, host=host, secrets=secrets,
            )
    except LockBusyError as exc:
        outcome.status = RunStatus.SKIPPED.value
        outcome.error = sanitize_error(exc, secrets)
        outcome.duration_ms = int((monotonic() - t0) * 1000)
        logger.warning("[BSALE_RAW] %s", outcome.error)
        return outcome
    except Exception as exc:
        return _fail(store, None, outcome, exc, secrets, int((monotonic() - t0) * 1000))


def _fuse_json(plan: ReconcilePlan, reconcile: bool, diagnostics: dict[str, Any]) -> dict[str, Any]:
    if reconcile:
        return {**plan.fuse_json(), **diagnostics, "deletes": True}
    return {
        **diagnostics,
        "deletes": False,
        "absent_not_seen": len(plan.missing_ids) + plan.protected_newer,
        "tripped": False,
        "reason": None,
    }


def _run_locked(
    *,
    store: RawStore,
    spec: ResourceSpec,
    source: SourceConfig,
    token: str,
    office_id: int,
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
    reconcile = mode is SyncMode.FULL_RECONCILE

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
        snapshot = fetch_snapshot(
            client,
            api_path(spec.list_endpoint),
            params={OFFICE_FILTER: office_id},
            clock=clock,
            drift=STRICT if reconcile else STOCK_SCAN_DRIFT,
        )
        outcome.api_count = snapshot.api_count
        outcome.pages = snapshot.pages
        rows = build_stock_rows(spec, company_id, snapshot, office_id=office_id)
        outcome.rows_received = len(rows)
    except Exception as exc:
        _collect_request_stats(client, outcome)
        return _fail(store, handle, outcome, exc, secrets, elapsed_ms())
    _collect_request_stats(client, outcome)

    diagnostics = {
        "policy": mode.value,
        "count_first": snapshot.count_first,
        "count_last": snapshot.api_count,
        "count_tolerance": snapshot.count_tolerance,
    }

    def plan_for(existing: dict) -> ReconcilePlan:
        return plan_reconcile(
            existing, rows, snapshot_started_at=snapshot_started_at, threshold_pct=threshold, key=stock_key
        )

    if outcome.dry_run:
        try:
            plan = plan_for(store.read_existing_stock(spec, company_id, office_id))
        except Exception as exc:
            return _fail(store, None, outcome, exc, secrets, elapsed_ms())
        outcome.fuse = _fuse_json(plan, reconcile, diagnostics)
        outcome.rows_inserted = plan.predicted["inserted"]
        outcome.rows_updated = plan.predicted["updated"]
        outcome.rows_unchanged = plan.predicted["unchanged"]
        outcome.rows_skipped_newer = plan.predicted["skipped_newer"]
        outcome.rows_deleted = len(plan.missing_ids) if reconcile else 0
        tripped = reconcile and plan.fuse_tripped
        outcome.status = RunStatus.FAILED.value if tripped else RunStatus.SUCCESS.value
        outcome.error = f"fusible: {plan.fuse_reason}" if tripped else None
        outcome.duration_ms = elapsed_ms()
        return outcome

    assert handle is not None
    try:
        with store.transaction() as tx:
            plan = plan_for(tx.read_existing_stock(spec, company_id, office_id))
            outcome.fuse = _fuse_json(plan, reconcile, diagnostics)
            if reconcile and plan.fuse_tripped:
                raise FuseTrippedError(f"fusible: {plan.fuse_reason}; no se escribió nada")
            applied = tx.upsert_stock(spec, rows, sync_run_id=handle.run_id, last_source=mode.value)
            counts = plan.counts(applied)
            outcome.rows_inserted = counts["inserted"]
            outcome.rows_updated = counts["updated"]
            outcome.rows_unchanged = counts["unchanged"]
            outcome.rows_skipped_newer = counts["skipped_newer"]
            outcome.rows_missing = 0
            outcome.rows_deleted = (
                tx.delete_stale_stock(
                    spec, company_id, office_id, [variant_id for variant_id, _ in plan.missing_ids],
                    snapshot_started_at,
                )
                if reconcile
                else 0
            )
            outcome.status = RunStatus.SUCCESS.value
            outcome.duration_ms = elapsed_ms()
            tx.finish_success(handle, outcome)
    except Exception as exc:
        _reset_write_counts(outcome)
        return _fail(store, handle, outcome, exc, secrets, elapsed_ms())

    logger.info(
        "[BSALE_RAW] company=%s resource=%s scope=%s mode=%s status=%s api_count=%s received=%s inserted=%s "
        "updated=%s unchanged=%s skipped_newer=%s deleted=%s requests=%s duration_ms=%s",
        company_id, spec.name, outcome.scope, mode.value, outcome.status, outcome.api_count,
        outcome.rows_received, outcome.rows_inserted, outcome.rows_updated, outcome.rows_unchanged,
        outcome.rows_skipped_newer, outcome.rows_deleted, outcome.requests, outcome.duration_ms,
    )
    return outcome
