"""
Orquestación de ``sync_bsale_catalog`` y de los scripts raíz (sync_catalog / sync_prices_costs / sync_stock).

- Advisory lock de sesión en una conexión dedicada en autocommit: impide ejecuciones paralelas
  sin mantener una transacción abierta durante las llamadas HTTP.
- Empresas cargadas en modo estricto (sin ``continue`` silencioso).
- SUCCESS sólo si todas las empresas esperadas pasaron todas las fases y las fases DB globales terminaron.
"""

from __future__ import annotations

import logging
import time
from contextlib import contextmanager
from typing import Any, Callable, Iterator

from backend.services.bsale.catalog_company_sync import sync_company_catalog
from backend.services.bsale.companies import BsaleCompany, CompanyConfigError, load_active_companies
from backend.services.bsale.prices_costs_sync import sync_company_prices_costs
from backend.services.bsale.stock_sync import sync_company_stock
from backend.services.bsale.sync_common import (
    ClientFactory,
    ConnectionFactory,
    default_client_factory,
    default_connection_factory,
)
from backend.services.bsale.sync_runs import (
    STATUS_FAILED,
    STATUS_SUCCESS,
    SyncRunRecorder,
    compute_run_status,
)

logger = logging.getLogger(__name__)

JOB_NAME = "sync_bsale_catalog"
ADVISORY_LOCK_BSALE_CATALOG = 5_927_184_030

EXIT_SUCCESS = 0
EXIT_FAILED = 1
EXIT_PARTIAL = 2
EXIT_LOCKED = 3

CompanyPhase = Callable[..., dict[str, Any]]

COMPANY_PHASES: dict[str, CompanyPhase] = {
    "catalog": sync_company_catalog,
    "prices_costs": sync_company_prices_costs,
    "stock": sync_company_stock,
}


class LockBusyError(RuntimeError):
    """Otra ejecución de sync_bsale_catalog tiene el advisory lock."""


@contextmanager
def bsale_catalog_lock(
    connection_factory: ConnectionFactory = default_connection_factory,
    key: int = ADVISORY_LOCK_BSALE_CATALOG,
) -> Iterator[None]:
    conn = connection_factory()
    try:
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("SELECT pg_try_advisory_lock(%s)", (key,))
        row = cur.fetchone()
        if not row or not row[0]:
            raise LockBusyError(
                f"{JOB_NAME} ya está en ejecución (advisory lock {key} ocupado); no se inicia otro"
            )
        try:
            yield
        finally:
            try:
                cur.execute("SELECT pg_advisory_unlock(%s)", (key,))
            except Exception:
                logger.exception("[BSALE_SYNC] no se pudo liberar advisory lock %s", key)
    finally:
        conn.close()


def load_companies_strict(connection_factory: ConnectionFactory) -> list[BsaleCompany]:
    conn = connection_factory()
    try:
        cur = conn.cursor()
        companies = load_active_companies(cur)
        cur.close()
        conn.rollback()
        return companies
    finally:
        conn.close()


def exit_code_for_status(status: str) -> int:
    if status == STATUS_SUCCESS:
        return EXIT_SUCCESS
    if status == STATUS_FAILED:
        return EXIT_FAILED
    return EXIT_PARTIAL


def _global_steps() -> list[tuple[str, Callable[[], dict[str, Any]]]]:
    from backend.services.bsale.catalog_sync_service import (
        backfill_units_per_box_from_sec,
        refresh_product_master_variants,
        refresh_products_master,
    )

    return [
        ("backfill_units_per_box_from_sec", backfill_units_per_box_from_sec),
        ("refresh_products_master", refresh_products_master),
        ("refresh_product_master_variants", refresh_product_master_variants),
    ]


def run_sync(
    *,
    job: str = JOB_NAME,
    phases: list[str] | None = None,
    include_global_steps: bool = True,
    connection_factory: ConnectionFactory = default_connection_factory,
    client_factory: ClientFactory = default_client_factory,
    company_phases: dict[str, CompanyPhase] | None = None,
    global_steps: list[tuple[str, Callable[[], dict[str, Any]]]] | None = None,
    recorder: SyncRunRecorder | None = None,
) -> dict[str, Any]:
    """Ejecuta el sync (requiere que el llamador ya tenga el lock). Nunca lanza: devuelve status."""
    t0 = time.perf_counter()
    registry = company_phases or COMPANY_PHASES
    phase_names = phases or list(registry)
    recorder = recorder or SyncRunRecorder(job, connection_factory=connection_factory)
    stats: dict[str, Any] = {"job": job, "phases": {}, "global_steps": {}, "errors": []}
    errors: list[str] = stats["errors"]

    try:
        companies = load_companies_strict(connection_factory)
    except CompanyConfigError as exc:
        errors.append(f"companies: {exc}")
        stats.update(status=STATUS_FAILED, companies_expected=[], companies_processed=[])
        recorder.finish(
            status=STATUS_FAILED,
            error=errors[0],
            companies_expected=[],
            companies_processed=[],
            stats=stats,
        )
        logger.error("[BSALE_SYNC] job=%s FAILED %s", job, errors[0])
        return stats

    expected = [c.company_id for c in companies]
    recorder.start(companies_expected=expected)
    ok_by_company = {cid: True for cid in expected}

    for phase in phase_names:
        fn = registry[phase]
        results = []
        for company in companies:
            res = fn(company, client_factory=client_factory, connection_factory=connection_factory)
            results.append(res)
            if not res.get("ok"):
                ok_by_company[company.company_id] = False
                errors.append(f"{phase} company_id={company.company_id}: {res.get('error')}")
        stats["phases"][phase] = results

    if include_global_steps:
        for name, step in global_steps if global_steps is not None else _global_steps():
            try:
                res = step()
            except Exception as exc:
                res = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
            stats["global_steps"][name] = res
            if not res.get("ok"):
                errors.append(f"{name}: {res.get('error')}")

    processed = [cid for cid, ok in ok_by_company.items() if ok]
    status = compute_run_status(
        expected_company_ids=expected, processed_company_ids=processed, errors=errors
    )
    stats.update(
        status=status,
        companies_expected=expected,
        companies_processed=processed,
        duration_ms=int((time.perf_counter() - t0) * 1000),
    )
    recorder.finish(
        status=status,
        error="; ".join(errors)[:8000] or None,
        companies_expected=expected,
        companies_processed=processed,
        stats=stats,
    )
    logger.info(
        "[BSALE_SYNC] job=%s status=%s companies_expected=%s companies_processed=%s errors=%s",
        job,
        status,
        expected,
        processed,
        len(errors),
    )
    return stats


def run_locked(
    *,
    connection_factory: ConnectionFactory = default_connection_factory,
    **kwargs: Any,
) -> tuple[int, dict[str, Any]]:
    """Toma el advisory lock y ejecuta ``run_sync``. Devuelve (exit_code, stats)."""
    try:
        with bsale_catalog_lock(connection_factory):
            stats = run_sync(connection_factory=connection_factory, **kwargs)
    except LockBusyError as exc:
        logger.error("[BSALE_SYNC] %s", exc)
        return EXIT_LOCKED, {"status": "locked", "errors": [str(exc)]}
    return exit_code_for_status(stats["status"]), stats


def run_single_phase_script(phase: str) -> int:
    """Entrada de los scripts raíz: una fase, con lock y registro propio."""
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    code, _ = run_locked(job=f"{JOB_NAME}:{phase}", phases=[phase], include_global_steps=False)
    return code
