"""Ciclo de stock ``bsale_raw``: SCANNER serial de todas las sucursales activas de UNA empresa.

Sólo orquesta ``run_stock_sync`` (modo SCANNER): no hay lógica Bsale propia, no hay FULL_RECONCILE
y nunca se borra. Sucursales activas = ``bsale_raw.offices`` con ``state = 0`` y ``missing_since IS
NULL`` (Bsale: 0 activa, 1 inactiva); nunca se inventan ids.

Concurrencia: lock de ciclo ``(company, stocks, cycle)`` en conexión dedicada (sin transacción
abierta durante HTTP). Si otro ciclo lo tiene, el ciclo termina SKIPPED sin escanear. Cada sucursal
sigue tomando su propio lock ``office:<id>`` dentro de ``run_stock_sync``; POINT no toma ninguno de
los dos.
"""

from __future__ import annotations

import logging
import socket
import time
from contextlib import nullcontext
from dataclasses import dataclass, field
from typing import Any, Callable, Protocol

from backend.services.bsale_raw.core.engine import sanitize_error
from backend.services.bsale_raw.core.models import RunStatus, SyncMode
from backend.services.bsale_raw.core.store import EntityOutcome, LockBusyError, PgRawStore

logger = logging.getLogger(__name__)

RESOURCE = "stocks"
CYCLE_SCOPE = "cycle"
TRIGGER_STOCK_CYCLE = "STOCK_CYCLE"
OFFICE_ACTIVE_STATE = 0

SUCCESS = "SUCCESS"
PARTIAL = "PARTIAL"
FAILED = "FAILED"
SKIPPED = "SKIPPED"

COUNTERS = (
    "rows_received", "rows_inserted", "rows_updated", "rows_unchanged", "rows_skipped_newer",
    "requests", "http_429", "http_5xx",
)


@dataclass(frozen=True)
class OfficeRow:
    office_id: int
    name: str | None
    state: int | None
    missing: bool


@dataclass
class OfficeResult:
    office_id: int
    status: str
    sync_run_id: int | None = None
    duration_ms: int = 0
    error: str | None = None
    counters: dict[str, int] = field(default_factory=dict)


@dataclass
class CycleReport:
    company_id: int
    dry_run: bool = False
    status: str = SUCCESS
    error: str | None = None
    warnings: list[str] = field(default_factory=list)
    offices_expected: list[int] = field(default_factory=list)
    results: list[OfficeResult] = field(default_factory=list)
    duration_ms: int = 0

    def _ids(self, status: str) -> list[int]:
        return [r.office_id for r in self.results if r.status == status]

    @property
    def offices_completed(self) -> list[int]:
        return self._ids(SUCCESS)

    @property
    def offices_failed(self) -> list[int]:
        return self._ids(FAILED)

    @property
    def offices_skipped(self) -> list[int]:
        return self._ids(SKIPPED)

    def total(self, counter: str) -> int:
        return sum(r.counters.get(counter, 0) for r in self.results)


class OfficeReader(Protocol):
    def list_offices(self, company_id: int) -> list[OfficeRow]: ...


class CycleLock(Protocol):
    def advisory_lock(self, company_id: int, resource: str, scope: str) -> Any: ...


ScanFn = Callable[[int, int, bool], EntityOutcome]

_OFFICES_SQL = (
    "SELECT bsale_id, name, state, missing_since IS NOT NULL "
    "FROM bsale_raw.offices WHERE company_id = %s ORDER BY bsale_id"
)


class PgStockCycleStore(PgRawStore):
    """Lectura de sucursales en sesión read-only + advisory lock de ciclo (conexión dedicada)."""

    def __init__(self, connection_factory=None) -> None:
        super().__init__(connection_factory, read_only=True)

    def list_offices(self, company_id: int) -> list[OfficeRow]:
        cur = self._work().cursor()
        try:
            cur.execute(_OFFICES_SQL, (company_id,))
            rows = cur.fetchall()
        finally:
            cur.close()
        return [
            OfficeRow(int(oid), name, None if state is None else int(state), bool(missing))
            for oid, name, state, missing in rows
        ]


def default_scan(company_id: int, office_id: int, dry_run: bool) -> EntityOutcome:
    from backend.services.bsale_raw.core.stock_engine import run_stock_sync

    store = PgRawStore(read_only=dry_run)
    try:
        return run_stock_sync(
            store=store, company_id=company_id, office_id=office_id, resource=RESOURCE,
            mode=SyncMode.SCANNER, dry_run=dry_run, trigger=TRIGGER_STOCK_CYCLE, host=socket.gethostname(),
        )
    finally:
        store.close()


def select_active_offices(offices: list[OfficeRow]) -> tuple[list[int], list[str]]:
    """(ids activos ordenados, advertencias). Estado desconocido = excluida y reportada."""
    active: list[int] = []
    warnings: list[str] = []
    for o in sorted(offices, key=lambda o: o.office_id):
        if o.missing:
            continue
        if o.state is None or o.state not in (0, 1):
            warnings.append(f"office_id={o.office_id} con state={o.state!r} desconocido: excluida")
            continue
        if o.state == OFFICE_ACTIVE_STATE:
            active.append(o.office_id)
    return active, warnings


def _office_result(office_id: int, outcome: EntityOutcome) -> OfficeResult:
    status = {
        RunStatus.SUCCESS.value: SUCCESS,
        RunStatus.SKIPPED.value: SKIPPED,
    }.get(outcome.status, FAILED)
    return OfficeResult(
        office_id=office_id,
        status=status,
        sync_run_id=outcome.sync_run_id,
        duration_ms=outcome.duration_ms,
        error=outcome.error,
        counters={c: int(getattr(outcome, c) or 0) for c in COUNTERS},
    )


def _cycle_status(report: CycleReport) -> str:
    done = len(report.offices_completed)
    if done == len(report.offices_expected):
        return SUCCESS
    if done == 0 and not report.offices_failed:
        return SKIPPED
    if done == 0:
        return FAILED
    return PARTIAL


def run_stock_cycle(
    *,
    company_id: int,
    dry_run: bool = False,
    reader: OfficeReader,
    lock: CycleLock,
    scan: ScanFn = default_scan,
    monotonic: Callable[[], float] = time.perf_counter,
) -> CycleReport:
    """Nunca lanza: cualquier falla queda en el reporte."""
    t0 = monotonic()
    report = CycleReport(company_id=company_id, dry_run=dry_run)

    def done() -> CycleReport:
        report.duration_ms = int((monotonic() - t0) * 1000)
        logger.info(
            "[BSALE_RAW_STOCK_CYCLE] company=%s status=%s expected=%s completed=%s failed=%s skipped=%s "
            "duration_ms=%s", company_id, report.status, report.offices_expected, report.offices_completed,
            report.offices_failed, report.offices_skipped, report.duration_ms,
        )
        return report

    # El dry-run no escribe ni toma locks (igual que run_stock_sync en dry-run).
    cycle_lock = nullcontext() if dry_run else lock.advisory_lock(company_id, RESOURCE, CYCLE_SCOPE)
    try:
        with cycle_lock:
            try:
                offices = reader.list_offices(company_id)
            except Exception as exc:
                report.status = FAILED
                report.error = f"no se pudo leer bsale_raw.offices: {sanitize_error(exc, [])}"
                return done()
            if not offices:
                report.status = FAILED
                report.error = f"bsale_raw.offices sin filas para company_id={company_id}; no se escanea nada"
                return done()
            report.offices_expected, report.warnings = select_active_offices(offices)
            if not report.offices_expected:
                report.status = FAILED
                report.error = f"company_id={company_id} sin sucursales activas (state=0, sin missing_since)"
                return done()

            for office_id in report.offices_expected:
                try:
                    result = _office_result(office_id, scan(company_id, office_id, dry_run))
                except Exception as exc:
                    result = OfficeResult(office_id=office_id, status=FAILED, error=sanitize_error(exc, []))
                report.results.append(result)
                logger.info(
                    "[BSALE_RAW_STOCK_CYCLE] company=%s office=%s status=%s run_id=%s duration_ms=%s",
                    company_id, office_id, result.status, result.sync_run_id, result.duration_ms,
                )
            report.status = _cycle_status(report)
    except LockBusyError:
        report.status = SKIPPED
        report.error = (
            f"ciclo de stock company_id={company_id} ya en ejecución (lock {RESOURCE}/{CYCLE_SCOPE} ocupado); "
            "no se inicia otro"
        )
    except Exception as exc:
        report.status = FAILED
        report.error = f"ciclo abortado: {sanitize_error(exc, [])}"
    return done()


def format_cycle(report: CycleReport) -> str:
    def ids(values: list[int]) -> str:
        return ",".join(map(str, values))

    lines = []
    if report.dry_run:
        lines.append("dry_run=true")
    lines += [
        f"company_id={report.company_id}",
        f"offices_expected={ids(report.offices_expected)}",
        f"offices_completed={ids(report.offices_completed)}",
        f"offices_failed={ids(report.offices_failed)}",
        f"offices_skipped={ids(report.offices_skipped)}",
        *(f"{c}={report.total(c)}" for c in COUNTERS),
        f"duration_ms={report.duration_ms}",
        f"status={report.status}",
    ]
    if report.error:
        lines.append(f"error={report.error}")
    lines += [f"warning={w}" for w in report.warnings]
    for r in report.results:
        line = (
            f"office={r.office_id} status={r.status} sync_run_id={'' if r.sync_run_id is None else r.sync_run_id} "
            f"received={r.counters.get('rows_received', 0)} inserted={r.counters.get('rows_inserted', 0)} "
            f"updated={r.counters.get('rows_updated', 0)} unchanged={r.counters.get('rows_unchanged', 0)} "
            f"skipped_newer={r.counters.get('rows_skipped_newer', 0)} requests={r.counters.get('requests', 0)} "
            f"duration_ms={r.duration_ms}"
        )
        lines.append(line + (f" error={r.error}" if r.error else ""))
    return "\n".join(lines)
