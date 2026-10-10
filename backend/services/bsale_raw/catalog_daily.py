"""Job diario ``sync-catalog``: catálogo RAW de UNA empresa, sin recursos ajenos.

Orden fijo (orden de sincronización, no de FK: RAW no tiene FK entre tablas de datos):

1. ``taxes``          — ``run_entity_sync`` (FULL_RECONCILE)
2. ``product_types``  — ``run_entity_sync``
3. ``products``       — ``run_entity_sync``; trae ``product_type{id}`` y sólo ``product_taxes{href}``
4. ``variants``       — ``run_entity_sync``; trae ``product{id}``. Exige ``products`` SUCCESS
5. ``product_taxes``  — ``run_product_tax_sync`` (un GET por producto). Exige ``products`` y ``taxes`` SUCCESS

Una falla de ``product_taxes`` no afecta a ``variants`` (corre antes y no depende de ella). Cada recurso
conserva su propio lock, run, fusible y ``sync_state``; este módulo sólo orquesta y agrega un lock
``(company, catalog, daily)`` para impedir dos catálogos simultáneos de la misma empresa. No toca
stock, costos, precios, documentos, legacy ni ``sync-nightly``.
"""

from __future__ import annotations

import logging
import os
import socket
import time
from contextlib import nullcontext
from dataclasses import dataclass, field
from typing import Any, Callable, Protocol

from backend.services.bsale_raw.core.engine import read_token, redact, sanitize_error
from backend.services.bsale_raw.core.models import RunStatus, SyncMode
from backend.services.bsale_raw.core.store import EntityOutcome, LockBusyError, PgRawStore, SourceConfig

logger = logging.getLogger(__name__)

TRIGGER_CATALOG = "CATALOG_DAILY"
LOCK_RESOURCE = "catalog"
LOCK_SCOPE = "daily"
CATALOG_COMPANIES = frozenset({3})

ENTITY_RESOURCES = ("taxes", "product_types", "products", "variants")
PRODUCT_TAXES = "product_taxes"
CATALOG_ORDER = (*ENTITY_RESOURCES, PRODUCT_TAXES)
DEPENDENCIES: dict[str, tuple[str, ...]] = {
    "variants": ("products",),
    PRODUCT_TAXES: ("products", "taxes"),
}

SUCCESS = RunStatus.SUCCESS.value
PARTIAL = RunStatus.PARTIAL.value
FAILED = RunStatus.FAILED.value
SKIPPED = RunStatus.SKIPPED.value
SKIPPED_DEPENDENCY = "SKIPPED_DEPENDENCY"
SKIPPED_BY_FLAG = "SKIPPED_BY_FLAG"


@dataclass
class StepResult:
    resource: str
    status: str
    outcome: EntityOutcome | None = None
    error: str | None = None
    duration_ms: int = 0


@dataclass
class CatalogReport:
    company_id: int
    dry_run: bool = False
    status: str = FAILED
    error: str | None = None
    steps: list[StepResult] = field(default_factory=list)
    duration_ms: int = 0
    secrets: list[str] = field(default_factory=list, repr=False)

    def step(self, resource: str) -> StepResult | None:
        return next((s for s in self.steps if s.resource == resource), None)


class CatalogStore(Protocol):
    def resolve_source(self, company_id: int) -> SourceConfig: ...

    def advisory_lock(self, company_id: int, resource: str, scope: str) -> Any: ...


EntitySync = Callable[[int, str, bool], EntityOutcome]
TaxSync = Callable[[int, bool, "int | None"], EntityOutcome]


def default_entity_sync(company_id: int, resource: str, dry_run: bool) -> EntityOutcome:
    from backend.services.bsale_raw.core.engine import run_entity_sync

    store = PgRawStore(read_only=dry_run)
    try:
        return run_entity_sync(
            store=store, company_id=company_id, resource=resource, mode=SyncMode.FULL_RECONCILE,
            dry_run=dry_run, trigger=TRIGGER_CATALOG, host=socket.gethostname(),
        )
    finally:
        store.close()


def default_tax_sync(company_id: int, dry_run: bool, limit: int | None) -> EntityOutcome:
    from backend.services.bsale_raw.core.product_tax_engine import PgProductTaxStore, run_product_tax_sync

    store = PgProductTaxStore(read_only=dry_run)
    try:
        return run_product_tax_sync(
            store=store, company_id=company_id, dry_run=dry_run, limit=limit, trigger=TRIGGER_CATALOG,
            host=socket.gethostname(),
        )
    finally:
        store.close()


def _step(resource: str, call: Callable[[], EntityOutcome], secrets: list[str], monotonic: Callable[[], float]) -> StepResult:
    t0 = monotonic()
    try:
        outcome = call()
    except Exception as exc:  # p. ej. UnsupportedSyncError o conexión: aislado al recurso
        return StepResult(resource, FAILED, error=sanitize_error(exc, secrets), duration_ms=int((monotonic() - t0) * 1000))
    status = outcome.status if outcome.status in (SUCCESS, PARTIAL, SKIPPED) else FAILED
    error = redact(outcome.error, secrets) if outcome.error else None
    if status == SKIPPED:
        error = f"no ejecutado: {error or 'lock del recurso ocupado'}"
    return StepResult(resource, status, outcome=outcome, error=error, duration_ms=outcome.duration_ms)


def _overall(report: CatalogReport) -> str:
    ran = [s for s in report.steps if s.status != SKIPPED_BY_FLAG]
    if not ran:
        return FAILED
    if all(s.status == SUCCESS for s in ran):
        return SUCCESS
    if not any(s.status in (SUCCESS, PARTIAL) for s in ran):
        return FAILED
    return PARTIAL


def run_catalog_sync(
    *,
    store: CatalogStore,
    company_id: int,
    dry_run: bool = False,
    skip_product_taxes: bool = False,
    product_taxes_limit: int | None = None,
    entity_sync: EntitySync = default_entity_sync,
    tax_sync: TaxSync = default_tax_sync,
    getenv: Callable[[str], str | None] = os.getenv,
    monotonic: Callable[[], float] = time.perf_counter,
) -> CatalogReport:
    """Nunca lanza por fallas de API/BD: todo queda en el reporte."""
    t0 = monotonic()
    report = CatalogReport(company_id=company_id, dry_run=dry_run)

    def done() -> CatalogReport:
        report.duration_ms = int((monotonic() - t0) * 1000)
        return report

    if company_id not in CATALOG_COMPANIES:
        report.error = f"sync-catalog habilitado sólo para company_id {sorted(CATALOG_COMPANIES)}"
        return done()
    if product_taxes_limit is not None and not dry_run:
        report.error = "--product-taxes-limit sólo se acepta con --dry-run"
        return done()
    try:
        source = store.resolve_source(company_id)
        report.secrets.append(read_token(source, getenv))
    except Exception as exc:
        report.error = sanitize_error(exc, report.secrets)
        return done()

    lock = nullcontext() if dry_run else store.advisory_lock(company_id, LOCK_RESOURCE, LOCK_SCOPE)
    try:
        with lock:
            _run_steps(report, company_id, dry_run, skip_product_taxes, product_taxes_limit, entity_sync, tax_sync, monotonic)
    except LockBusyError as exc:
        report.status = SKIPPED
        report.error = f"otro sync-catalog de company_id={company_id} en curso: {sanitize_error(exc, report.secrets)}"
        logger.warning("[BSALE_RAW_CATALOG] %s", report.error)
        return done()
    except Exception as exc:
        report.status = FAILED
        report.error = sanitize_error(exc, report.secrets)
        return done()
    report.status = _overall(report)
    return done()


def _run_steps(
    report: CatalogReport,
    company_id: int,
    dry_run: bool,
    skip_product_taxes: bool,
    limit: int | None,
    entity_sync: EntitySync,
    tax_sync: TaxSync,
    monotonic: Callable[[], float],
) -> None:
    statuses: dict[str, str] = {}
    for resource in CATALOG_ORDER:
        blocked = [d for d in DEPENDENCIES.get(resource, ()) if statuses.get(d) != SUCCESS]
        if resource == PRODUCT_TAXES and skip_product_taxes:
            result = StepResult(resource, SKIPPED_BY_FLAG, error="omitido por --skip-product-taxes")
        elif blocked:
            reasons = ", ".join(f"{d} {statuses.get(d, 'no ejecutado')}" for d in blocked)
            result = StepResult(resource, SKIPPED_DEPENDENCY, error=f"requiere SUCCESS de: {reasons}")
        elif resource == PRODUCT_TAXES:
            result = _step(resource, lambda: tax_sync(company_id, dry_run, limit), report.secrets, monotonic)
        else:
            result = _step(
                resource, lambda r=resource: entity_sync(company_id, r, dry_run), report.secrets, monotonic
            )
        statuses[resource] = result.status
        report.steps.append(result)
        o = result.outcome
        logger.info(
            "[BSALE_RAW_CATALOG] company=%s resource=%s status=%s run_id=%s received=%s inserted=%s updated=%s "
            "missing=%s requests=%s duration_ms=%s",
            company_id, resource, result.status, o.sync_run_id if o else None, o.rows_received if o else 0,
            o.rows_inserted if o else 0, o.rows_updated if o else 0, o.rows_missing if o else 0,
            o.requests if o else 0, result.duration_ms,
        )


# --- salida ---------------------------------------------------------------------------------------

_TAX_FIELDS = (
    "products", "targets", "limit", "attempted", "fetched", "with_taxes", "without_taxes", "failed",
    "not_attempted", "unknown_tax_count", "failures_pending", "rps", "effective_rps", "aborted",
)


def _v(value: Any) -> str:
    return "" if value is None else str(value)


def format_catalog_report(report: CatalogReport) -> str:
    """key=value; nunca payload, token ni datos de clientes."""
    lines = ["CATALOG RAW SUMMARY", f"company={report.company_id}", f"status={report.status}"]
    if report.dry_run:
        lines.append("dry_run=true (sin escrituras; product_taxes usa los productos ya guardados en bsale_raw.products)")
    if report.error:
        lines.append(f"error={report.error}")
    total_requests = 0
    for s in report.steps:
        o = s.outcome
        parts = [f"resource={s.resource}", f"status={s.status}"]
        if o is not None:
            total_requests += o.requests
            parts += [
                f"run_id={_v(o.sync_run_id)}", f"api_count={_v(o.api_count)}", f"received={o.rows_received}",
                f"inserted={o.rows_inserted}", f"updated={o.rows_updated}", f"unchanged={o.rows_unchanged}",
                f"skipped_newer={o.rows_skipped_newer}", f"missing={o.rows_missing}", f"requests={o.requests}",
                f"http_429={o.http_429}", f"http_5xx={o.http_5xx}",
            ]
        parts.append(f"duration_ms={s.duration_ms}")
        lines.append("")
        lines.append(" ".join(parts))
        if o is not None and s.resource == PRODUCT_TAXES and o.point:
            lines.append("  " + " ".join(f"{k}={_v(o.point.get(k))}" for k in _TAX_FIELDS if k in o.point))
            for key in ("unknown_tax_products", "failed_sample"):
                sample = o.point.get(key)
                if sample:
                    lines.append(f"  {key}={sample}")
        if o is not None and o.fuse and o.fuse.get("tripped"):
            lines.append(f"  fuse={o.fuse.get('reason')}")
        if s.error:
            lines.append(f"  error={s.error}")
    lines.append("")
    lines.append(f"total_requests={total_requests}")
    lines.append(f"total_duration_ms={report.duration_ms}")
    return redact("\n".join(lines), report.secrets)
