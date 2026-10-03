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

``POINT`` (refresh dirigido, ``refresh_stock_variants`` / ``refresh_stock_point``): un request
``stocks.json?variantid=V[&officeid=O]`` por variante (Bsale no documenta listas de ids), prioridad
P0 en el MISMO limitador de la empresa, mismo UPSERT con frescura sobre las mismas filas. Nunca
borra ni fabrica 0 (0 filas = ``NO_ROWS``, sin escritura). No toma el lock de sucursal del scanner.
"""

from __future__ import annotations

import dataclasses
import logging
import os
import time
from contextlib import nullcontext
from typing import Any, Callable, Iterable

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
from backend.services.bsale_raw.core.rate_limit import RequestPriority
from backend.services.bsale_raw.core.reconcile import ReconcilePlan, max_missing_pct, plan_reconcile
from backend.services.bsale_raw.core.registry import (
    POINT_STATE_SCOPE,
    REGISTRY,
    KeyKind,
    ResourceSpec,
    office_scope,
    point_scope,
)
from backend.services.bsale_raw.core.snapshot import (
    STRICT,
    Clock,
    CountDrift,
    SnapshotValidationError,
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
VARIANT_FILTER = "variantid"
POINT_PRIORITY = RequestPriority.P0_TARGETED
MAX_POINT_VARIANTS = 500


class PointRefreshError(RuntimeError):
    """Todas las variantes de un refresh POINT fallaron."""


def stock_pipeline_spec(resource: str) -> ResourceSpec:
    import backend.services.bsale_raw.resources  # noqa: F401  (registra los recursos)

    if resource not in REGISTRY.pipeline_names():
        raise UnsupportedSyncError(f"recurso no habilitado en el motor: {resource}")
    spec = REGISTRY.get(resource)
    if spec.key_kind is not KeyKind.STOCK or not spec.partition_by_office:
        raise UnsupportedSyncError(f"{resource}: no es un recurso de stock por sucursal")
    return spec


def _validate_id(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise UnsupportedSyncError(f"{name} inválido: {value!r}")
    return value


def _validate_office(office_id: Any) -> int:
    return _validate_id(office_id, "office_id")


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
    if mode not in spec.pipeline_modes or mode is SyncMode.POINT:
        raise UnsupportedSyncError(f"{resource}: modo no habilitado en el barrido por sucursal: {mode.value}")
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

    _log_written(outcome)
    return outcome


def _log_written(outcome: EntityOutcome) -> None:
    logger.info(
        "[BSALE_RAW] company=%s resource=%s scope=%s mode=%s status=%s api_count=%s received=%s inserted=%s "
        "updated=%s unchanged=%s skipped_newer=%s deleted=%s requests=%s duration_ms=%s",
        outcome.company_id, outcome.resource, outcome.scope, outcome.mode, outcome.status, outcome.api_count,
        outcome.rows_received, outcome.rows_inserted, outcome.rows_updated, outcome.rows_unchanged,
        outcome.rows_skipped_newer, outcome.rows_deleted, outcome.requests, outcome.duration_ms,
    )


# --- POINT: refresh dirigido -----------------------------------------------------------------


def refresh_stock_point(
    *, store: RawStore, company_id: int, variant_id: int, office_id: int | None = None, **kwargs: Any
) -> EntityOutcome:
    """Una variante: ``variant + office`` (0 o 1 fila) o ``variant`` sola (todas sus sucursales)."""
    return refresh_stock_variants(
        store=store, company_id=company_id, variant_ids=[variant_id], office_id=office_id, **kwargs
    )


def refresh_stock_variants(
    *,
    store: RawStore,
    company_id: int,
    variant_ids: Iterable[int],
    office_id: int | None = None,
    resource: str = "stocks",
    dry_run: bool = False,
    client_factory: ClientFactory = default_client_factory,
    clock: Clock = utc_now,
    monotonic: Callable[[], float] = time.perf_counter,
    getenv: Callable[[str], str | None] = os.getenv,
    trigger: str = TRIGGER_MANUAL,
    host: str | None = None,
) -> EntityOutcome:
    """
    Refresh POINT de un conjunto de variantes (API interna para la futura OC 33: ``affected_variants``).

    Un ``sync_runs`` + un ``sync_entity_runs`` por llamada (no uno por variante); el detalle por
    variante (``FETCHED`` / ``NO_ROWS`` / ``FAILED`` y la clase por sucursal) queda en
    ``summary.point``. Una variante inválida no invalida a las demás: status ``PARTIAL``; si fallan
    todas, ``FAILED``. Nunca lanza por fallas de API/BD.
    """
    spec = stock_pipeline_spec(resource)
    if SyncMode.POINT not in spec.pipeline_modes:
        raise UnsupportedSyncError(f"{resource}: modo POINT no habilitado")
    variants = sorted({_validate_id(v, "variant_id") for v in variant_ids})
    if not variants:
        raise UnsupportedSyncError("refresh POINT sin variantes")
    if len(variants) > MAX_POINT_VARIANTS:
        raise UnsupportedSyncError(f"refresh POINT con {len(variants)} variantes > máximo {MAX_POINT_VARIANTS}")
    office = None if office_id is None else _validate_office(office_id)

    outcome = EntityOutcome(
        company_id=company_id,
        resource=resource,
        scope=point_scope(variants, office),
        mode=SyncMode.POINT.value,
        dry_run=dry_run,
        state_scope=POINT_STATE_SCOPE,
        point={"office_id": office, "variant_ids": variants, "results": {}},
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

    # Sin advisory lock: dos refresh de la misma variante sólo duplican un GET; la frescura del
    # UPSERT decide cuál queda. Un try-lock haría SKIP de un refresh posterior al cambio.
    handle: RunHandle | None = None
    if not dry_run:
        try:
            handle = store.start_run(
                mode=SyncMode.POINT.value, trigger=trigger, host=host, company_id=company_id,
                resource=resource, scope=outcome.scope, state_scope=POINT_STATE_SCOPE,
            )
        except Exception as exc:
            return _fail(store, None, outcome, exc, secrets, elapsed_ms())
        outcome.sync_run_id = handle.run_id

    outcome.snapshot_started_at = clock()
    results: dict[str, dict[str, Any]] = outcome.point["results"]
    rows = []
    client = None
    try:
        client = client_factory(source, token, dataclasses.replace(spec, request_priority=POINT_PRIORITY))
    except Exception as exc:
        return _fail(store, handle, outcome, exc, secrets, elapsed_ms())

    api_count = 0
    for variant in variants:
        params: dict[str, Any] = {VARIANT_FILTER: variant}
        if office is not None:
            params[OFFICE_FILTER] = office
        try:
            snapshot = fetch_snapshot(client, api_path(spec.list_endpoint), params=params, clock=clock)
            if office is not None and len(snapshot.items) > 1:
                raise SnapshotValidationError(
                    f"respuesta ambigua: {len(snapshot.items)} filas para variant={variant} office={office}"
                )
            variant_rows = build_stock_rows(spec, company_id, snapshot, office_id=office, variant_id=variant)
        except Exception as exc:
            results[str(variant)] = {"status": "FAILED", "error": sanitize_error(exc, secrets)}
            continue
        api_count += snapshot.api_count
        outcome.pages += snapshot.pages
        results[str(variant)] = {"status": "FETCHED" if variant_rows else "NO_ROWS", "offices": {}}
        rows.extend(variant_rows)
    _collect_request_stats(client, outcome)
    outcome.api_count = api_count
    outcome.rows_received = len(rows)

    failed = sorted(int(v) for v, r in results.items() if r["status"] == "FAILED")
    if len(failed) == len(variants):
        first = results[str(failed[0])]["error"]
        return _fail(
            store, handle, outcome,
            PointRefreshError(f"refresh POINT falló en todas las variantes ({len(failed)}); p. ej. {first}"),
            secrets, elapsed_ms(),
        )
    status = RunStatus.PARTIAL.value if failed else RunStatus.SUCCESS.value
    error = f"{len(failed)} de {len(variants)} variantes fallaron: {failed[:10]}" if failed else None
    fetched = [v for v in variants if results[str(v)]["status"] == "FETCHED"]

    def plan_for(existing: dict) -> ReconcilePlan:
        return plan_reconcile(
            existing, rows, snapshot_started_at=outcome.snapshot_started_at, threshold_pct=100.0, key=stock_key
        )

    def record(classes: dict) -> None:
        for (variant, row_office), kind in classes.items():
            results[str(variant)]["offices"][str(row_office)] = kind

    if dry_run:
        try:
            plan = plan_for(store.read_existing_stock_variants(spec, company_id, fetched))
        except Exception as exc:
            return _fail(store, None, outcome, exc, secrets, elapsed_ms())
        record(plan.predicted_classes())
        outcome.rows_inserted = plan.predicted["inserted"]
        outcome.rows_updated = plan.predicted["updated"]
        outcome.rows_unchanged = plan.predicted["unchanged"]
        outcome.rows_skipped_newer = plan.predicted["skipped_newer"]
        outcome.status, outcome.error, outcome.duration_ms = status, error, elapsed_ms()
        return outcome

    assert handle is not None
    try:
        with store.transaction() as tx:
            plan = plan_for(tx.read_existing_stock_variants(spec, company_id, fetched))
            applied = tx.upsert_stock(spec, rows, sync_run_id=handle.run_id, last_source=SyncMode.POINT.value)
            classes = plan.classify(applied)
            counts = plan.counts(applied)
            record(classes)
            outcome.rows_inserted = counts["inserted"]
            outcome.rows_updated = counts["updated"]
            outcome.rows_unchanged = counts["unchanged"]
            outcome.rows_skipped_newer = counts["skipped_newer"]
            outcome.status, outcome.error, outcome.duration_ms = status, error, elapsed_ms()
            tx.finish_success(handle, outcome)
    except Exception as exc:
        _reset_write_counts(outcome)
        for result in results.values():
            result.get("offices", {}).clear()
        return _fail(store, handle, outcome, exc, secrets, elapsed_ms())

    _log_written(outcome)
    return outcome
