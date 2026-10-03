"""Refresh POINT de UN documento Bsale por id técnico (fase 4E1: sólo document_type_id 33 = OC).

``document_id`` es el ``id`` técnico del documento en Bsale (``/v1/documents/{id}.json``); NO es el
folio (``number``), ni un SKU, ni una referencia. No hay búsqueda por folio.

Orden (nunca hay una transacción PostgreSQL abierta durante HTTP):

 1. fuente + token;
 2. run RUNNING (``sync_runs`` / ``sync_entity_runs`` scope ``document:<id>``; ``sync_state``
    agregado ``(company, documents, point)``);
 3. header ``/v1/documents/{id}.json`` (sin ``expand``) → guard ``document_type_id`` ∈ permitidos;
 4. details / references / sellers / attributes por sus endpoints, paginados COMPLETOS (los links
    del header deben coincidir exacto con esas rutas en ``https://api.bsale.io``);
 5. bundle validado + hashes (payload / children / version);
 6. UNA transacción corta: advisory xact lock del documento + ``FOR UPDATE`` → versión vigente →
    frescura → UPSERT header → REPLACE details / references / sellers → change log → COMMIT;
 7. DESPUÉS del COMMIT: ``refresh_stock_variants`` (P0) de previous ∪ current (+ pendientes) en la
    sucursal del documento;
 8. transacción corta final: marcar ``stock_refresh_done_at`` (sólo si el stock terminó bien) +
    cerrar la corrida.

Una falla en 3–6 no toca la versión RAW anterior. Una falla en 7 NO revierte el documento: la
corrida queda PARTIAL y la fila del change log sigue pendiente; el próximo refresh del mismo
documento la reintenta aunque ``version_hash`` no cambie (sin fabricar un MODIFIED).
Sin detección de estados terminales (las columnas watch_* no se tocan).
"""

from __future__ import annotations

import dataclasses
import logging
import os
import time
from typing import Any, Callable

from backend.services.bsale_raw.core.document_bundle import (
    ATTRIBUTES_LINK,
    CHILD_KINDS,
    CHILD_SPEC_NAMES,
    EMPTY_STORED,
    DocumentBundle,
    DocumentPlan,
    StoredDocument,
    attributes_path,
    build_attribute_items,
    build_child_rows,
    build_header,
    check_child_link,
    child_path,
    make_bundle,
    plan_document,
)
from backend.services.bsale_raw.core.engine import (
    TRIGGER_MANUAL,
    ClientFactory,
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
from backend.services.bsale_raw.core.registry import POINT_STATE_SCOPE, REGISTRY, KeyKind, ResourceSpec, document_scope
from backend.services.bsale_raw.core.snapshot import Clock, fetch_snapshot, utc_now
from backend.services.bsale_raw.core.stock_engine import MAX_POINT_VARIANTS, refresh_stock_variants
from backend.services.bsale_raw.core.store import EntityOutcome, RawStore, RunHandle

logger = logging.getLogger(__name__)

DOCUMENT_PRIORITY = RequestPriority.P0_TARGETED

STOCK_SUCCESS = "SUCCESS"
STOCK_PARTIAL = "PARTIAL"
STOCK_FAILED = "FAILED"
STOCK_NOT_NEEDED = "NOT_NEEDED"
STOCK_SKIPPED_STALE = "SKIPPED_STALE"
STOCK_DRY_RUN = "DRY_RUN"
STOCK_NOT_RUN = "NOT_RUN"


class StaleDocumentError(RuntimeError):
    """El UPSERT del header no aplicó pese al lock (no debería ocurrir): se revierte el bundle."""


def document_pipeline_spec(resource: str) -> tuple[ResourceSpec, dict[str, ResourceSpec]]:
    import backend.services.bsale_raw.resources  # noqa: F401  (registra los recursos)

    if resource not in REGISTRY.pipeline_names():
        raise UnsupportedSyncError(f"recurso no habilitado en el motor: {resource}")
    spec = REGISTRY.get(resource)
    if spec.key_kind is not KeyKind.ENTITY or spec.point_key != "document" or SyncMode.POINT not in spec.pipeline_modes:
        raise UnsupportedSyncError(f"{resource}: no admite refresh POINT de documento")
    return spec, {kind: REGISTRY.get(name) for kind, name in CHILD_SPEC_NAMES.items()}


def _summary(outcome: EntityOutcome, **values: Any) -> None:
    assert outcome.document is not None
    outcome.document.update(values)


def _bundle_summary(bundle: DocumentBundle) -> dict[str, Any]:
    return {
        "document_type_id": bundle.document_type_id,
        "office_id": bundle.office_id,
        "state": bundle.typed.get("state"),
        "details": len(bundle.details),
        "references": len(bundle.references),
        "sellers": len(bundle.sellers),
        "attributes": len(bundle.attributes),
        "details_without_variant": sum(1 for r in bundle.details if r.variant_id is None),
        "version_hash": bundle.version_hash,
    }


def _plan_summary(plan: DocumentPlan, stored: StoredDocument) -> dict[str, Any]:
    return {
        "stale": plan.stale,
        "change_kind": plan.change_kind,
        "version_changed": plan.version_changed,
        "previous_version_hash": stored.version_hash,
        "changed": plan.changed,
        "previous_variants": plan.previous_variant_ids,
        "current_variants": plan.current_variant_ids,
        "affected_variants": plan.affected_variant_ids,
        "pending_change_ids": plan.pending_change_ids,
        "stock_variants": plan.stock_variant_ids,
        "stock_office_id": plan.stock_office_id,
    }


def _apply_counts(outcome: EntityOutcome, plan: DocumentPlan) -> None:
    _reset_write_counts(outcome)
    if plan.stale:
        outcome.rows_skipped_newer = 1
    elif plan.change_kind == "CREATED":
        outcome.rows_inserted = 1
    elif plan.change_kind == "MODIFIED":
        outcome.rows_updated = 1
    else:
        outcome.rows_unchanged = 1


def refresh_document_point(
    *,
    store: RawStore,
    company_id: int,
    document_id: int,
    resource: str = "documents",
    dry_run: bool = False,
    client_factory: ClientFactory = default_client_factory,
    clock: Clock = utc_now,
    monotonic: Callable[[], float] = time.perf_counter,
    getenv: Callable[[str], str | None] = os.getenv,
    trigger: str = TRIGGER_MANUAL,
    host: str | None = None,
) -> EntityOutcome:
    """Nunca lanza por fallas de API/BD: devuelve el ``EntityOutcome`` con status y error saneado."""
    spec, child_specs = document_pipeline_spec(resource)
    if isinstance(document_id, bool) or not isinstance(document_id, int) or document_id <= 0:
        raise UnsupportedSyncError(f"document_id inválido: {document_id!r}")
    from backend.services.bsale_raw.resources.documents import POINT_DOCUMENT_TYPE_IDS

    outcome = EntityOutcome(
        company_id=company_id,
        resource=resource,
        scope=document_scope(document_id),
        mode=SyncMode.POINT.value,
        dry_run=dry_run,
        state_scope=POINT_STATE_SCOPE,
        document={"document_id": document_id, "stock_refresh": STOCK_NOT_RUN},
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

    # Sin advisory lock de sesión durante HTTP: la exclusión es por documento y sólo dentro de la
    # transacción de escritura (pg_advisory_xact_lock + FOR UPDATE).
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
    client = None
    try:
        client = client_factory(source, token, dataclasses.replace(spec, request_priority=DOCUMENT_PRIORITY))
        header = client.get_json(api_path(spec.item_endpoint.format(id=document_id)))
        header_fetched_at = clock()
        outcome.pages = 1
        typed = build_header(spec, document_id, header, POINT_DOCUMENT_TYPE_IDS)
        _summary(outcome, document_type_id=typed.get("document_type_id"), office_id=typed.get("office_id"))
        children = {}
        for kind in CHILD_KINDS:
            check_child_link(header, kind, document_id)
            snapshot = fetch_snapshot(client, child_path(child_specs[kind], document_id), clock=clock)
            outcome.pages += snapshot.pages
            children[kind] = build_child_rows(kind, child_specs[kind], document_id, snapshot)
        check_child_link(header, ATTRIBUTES_LINK, document_id)
        snapshot = fetch_snapshot(client, attributes_path(document_id), clock=clock)
        outcome.pages += snapshot.pages
        attributes = build_attribute_items(document_id, snapshot)
        bundle = make_bundle(
            company_id=company_id, document_id=document_id, header=header, typed=typed, children=children,
            attributes=attributes, api_fetched_at=header_fetched_at, children_fetched_at=clock(),
        )
    except Exception as exc:
        _collect_request_stats(client, outcome)
        return _fail(store, handle, outcome, exc, secrets, elapsed_ms())
    _collect_request_stats(client, outcome)
    outcome.rows_received = 1
    _summary(outcome, **_bundle_summary(bundle))

    if dry_run:
        try:
            stored = store.read_document(spec, child_specs, company_id, document_id)
        except Exception as exc:
            return _fail(store, None, outcome, exc, secrets, elapsed_ms())
        plan = plan_document(stored, bundle)
        _apply_counts(outcome, plan)
        _summary(outcome, **_plan_summary(plan, stored), stock_refresh=STOCK_DRY_RUN)
        outcome.status, outcome.duration_ms = RunStatus.SUCCESS.value, elapsed_ms()
        return outcome

    assert handle is not None
    stored: StoredDocument = EMPTY_STORED
    change_id: int | None = None
    try:
        with store.transaction() as tx:
            stored = tx.lock_document(spec, child_specs, company_id, document_id)
            plan = plan_document(stored, bundle)
            removed = dict.fromkeys(CHILD_KINDS, 0)
            if not plan.stale:
                if not tx.upsert_document(spec, bundle, sync_run_id=handle.run_id, last_source=SyncMode.POINT.value):
                    raise StaleDocumentError(f"documento {document_id}: UPSERT no aplicado; se revierte el bundle")
                for kind in CHILD_KINDS:
                    removed[kind] = tx.replace_document_children(
                        child_specs[kind], kind, bundle, sync_run_id=handle.run_id, last_source=SyncMode.POINT.value
                    )
                if plan.change_kind is not None:
                    change_id = tx.insert_document_change(
                        bundle, stored, plan, sync_run_id=handle.run_id, detected_by=SyncMode.POINT.value
                    )
    except Exception as exc:
        _reset_write_counts(outcome)
        return _fail(store, handle, outcome, exc, secrets, elapsed_ms())

    _apply_counts(outcome, plan)
    _summary(outcome, **_plan_summary(plan, stored), children_removed=removed, change_log_id=change_id)

    # --- post-COMMIT: stock. El documento ya quedó confirmado pase lo que pase aquí. ---
    done_ids: list[int] = []
    outcome.status = RunStatus.SUCCESS.value
    if plan.stale:
        _summary(outcome, stock_refresh=STOCK_SKIPPED_STALE)
    elif not plan.stock_variant_ids:
        _summary(outcome, stock_refresh=STOCK_NOT_NEEDED)
    else:
        stock = _refresh_stock(
            store=store, company_id=company_id, office_id=plan.stock_office_id, variant_ids=plan.stock_variant_ids,
            client_factory=client_factory, clock=clock, monotonic=monotonic, getenv=getenv, trigger=trigger,
            host=host,
        )
        _summary(outcome, stock_refresh=stock["status"], stock_sync_run_ids=stock["run_ids"],
                 stock_requests=stock["requests"])
        if stock["status"] == STOCK_SUCCESS:
            done_ids = ([change_id] if change_id is not None and plan.affected_variant_ids else [])
            done_ids += plan.pending_change_ids
        else:
            outcome.status = RunStatus.PARTIAL.value
            outcome.error = sanitize_error(
                RuntimeError(f"documento confirmado; stock refresh {stock['status']}: {stock['error']}"), secrets
            )
            _summary(outcome, stock_error=outcome.error)

    _summary(outcome, stock_done_change_ids=done_ids)
    outcome.duration_ms = elapsed_ms()
    try:
        with store.transaction() as tx:
            tx.mark_stock_refresh_done(company_id, document_id, done_ids)
            tx.finish_success(handle, outcome)
    except Exception as exc:
        note = sanitize_error(exc, secrets)
        outcome.status = RunStatus.PARTIAL.value
        outcome.error = f"documento confirmado; cierre de corrida falló: {note}"
        _summary(outcome, stock_done_change_ids=[])
        try:
            store.finish_failed(handle, outcome)
        except Exception as record_exc:
            logger.error("[BSALE_RAW] no se pudo registrar el cierre: %s", sanitize_error(record_exc, secrets))

    logger.info(
        "[BSALE_RAW] company=%s resource=%s scope=%s mode=POINT status=%s document_type_id=%s office_id=%s "
        "details=%s change=%s affected=%s stock=%s requests=%s duration_ms=%s",
        company_id, resource, outcome.scope, outcome.status, bundle.document_type_id, bundle.office_id,
        len(bundle.details), plan.change_kind, len(plan.affected_variant_ids),
        outcome.document.get("stock_refresh"), outcome.requests, outcome.duration_ms,
    )
    return outcome


def _refresh_stock(
    *,
    store: RawStore,
    company_id: int,
    office_id: int | None,
    variant_ids: list[int],
    client_factory: ClientFactory,
    clock: Clock,
    monotonic: Callable[[], float],
    getenv: Callable[[str], str | None],
    trigger: str,
    host: str | None,
) -> dict[str, Any]:
    """``refresh_stock_variants`` (P0, mismo limitador) en lotes de ``MAX_POINT_VARIANTS``; nunca lanza."""
    statuses: list[str] = []
    run_ids: list[int] = []
    errors: list[str] = []
    requests = 0
    for start in range(0, len(variant_ids), MAX_POINT_VARIANTS):
        chunk = variant_ids[start: start + MAX_POINT_VARIANTS]
        try:
            out = refresh_stock_variants(
                store=store, company_id=company_id, variant_ids=chunk, office_id=office_id,
                client_factory=client_factory, clock=clock, monotonic=monotonic, getenv=getenv,
                trigger=trigger, host=host,
            )
        except Exception as exc:
            statuses.append(STOCK_FAILED)
            errors.append(f"{type(exc).__name__}: {exc}")
            continue
        statuses.append(out.status)
        requests += out.requests
        if out.sync_run_id is not None:
            run_ids.append(out.sync_run_id)
        if out.error:
            errors.append(out.error)
    if all(s == STOCK_SUCCESS for s in statuses):
        status = STOCK_SUCCESS
    elif all(s == STOCK_FAILED for s in statuses):
        status = STOCK_FAILED
    else:
        status = STOCK_PARTIAL
    return {"status": status, "run_ids": run_ids, "requests": requests, "error": "; ".join(errors)[:500] or None}
