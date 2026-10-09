"""Ciclo ``scan-stocks``: SCANNER serial de sucursales activas. Sin red ni BD real.

Integración: ``run_stock_cycle`` + ``run_stock_sync`` real + ``FakeStore`` (locks, runs, tablas) +
``StockBsale`` (stack HTTP real sobre adapter falso).
"""

from __future__ import annotations

import io
import logging
from urllib.parse import parse_qs, urlsplit

import pytest
import requests

from backend.jobs.bsale_raw import cli
from backend.services.bsale_raw import stock_cycle
from backend.services.bsale_raw.core.models import SyncMode
from backend.services.bsale_raw.core.stock_engine import refresh_stock_point, run_stock_sync
from backend.services.bsale_raw.core.store import advisory_lock_keys
from backend.services.bsale_raw.stock_cycle import (
    FAILED,
    PARTIAL,
    SKIPPED,
    SUCCESS,
    OfficeRow,
    PgStockCycleStore,
    format_cycle,
    run_stock_cycle,
    select_active_offices,
)
from backend.tests.bsale_raw.test_bsale_raw_pipeline import (
    BASE,
    ENV,
    TOKEN,
    FakeConnection,
    TickClock,
    client_factory_for,
)
from backend.tests.bsale_raw.test_bsale_raw_stock import TABLE, StockBsale, old, setup, stock

C3_OFFICES = [
    OfficeRow(3, "Sucursal 3", 0, False),
    OfficeRow(1, "BODEGA CENTRAL", 0, False),
    OfficeRow(7, "Cerrada", 1, False),
    OfficeRow(2, "Sucursal 2", 0, False),
    OfficeRow(6, "Sucursal 6", 0, False),
    OfficeRow(4, "QUILLOTANA I", 0, False),
    OfficeRow(5, "Sucursal 5", 0, False),
]
VARIANTS = (10, 11, 12)
CYCLE_KEY = advisory_lock_keys(3, "stocks", "cycle")


class Reader:
    def __init__(self, offices=None, error: Exception | None = None):
        self.offices = list(C3_OFFICES if offices is None else offices)
        self.error = error
        self.calls = 0

    def list_offices(self, company_id):
        self.calls += 1
        if self.error:
            raise self.error
        return list(self.offices)


class World:
    def __init__(self, offices=None, *, reader_error=None, env=None):
        items = [stock(v, o, quantity=10.0 * o, reserved=float(o), available=9.0 * o)
                 for o in range(1, 8) for v in VARIANTS]
        self.store, self.adapter = setup(items)
        self.reader = Reader(offices, reader_error)
        self.clock = TickClock(BASE)
        self.env = dict(ENV) if env is None else env
        self.failing: set[int] = set()
        self.scans: list[tuple[int, int, bool]] = []
        self.during_scan = None

    def scan(self, company_id, office_id, dry_run):
        self.scans.append((company_id, office_id, dry_run))
        if self.during_scan is not None:
            self.during_scan(office_id)
        adapter = self.adapter
        if office_id in self.failing:
            adapter = StockBsale(script=[requests.ConnectionError(f"down {TOKEN}")] * 10, store=self.store)
        return run_stock_sync(
            store=self.store, company_id=company_id, office_id=office_id, mode=SyncMode.SCANNER,
            dry_run=dry_run, client_factory=client_factory_for(adapter), clock=self.clock,
            getenv=self.env.get, trigger=stock_cycle.TRIGGER_STOCK_CYCLE, host="test",
        )

    def run(self, *, dry_run=False):
        return run_stock_cycle(company_id=3, dry_run=dry_run, reader=self.reader, lock=self.store, scan=self.scan)

    def cli(self, *argv):
        out, err = io.StringIO(), io.StringIO()
        reports = []

        def runner(*, company_id, dry_run):
            assert company_id == 3
            reports.append(self.run(dry_run=dry_run))
            return reports[-1]

        code = cli.main(["scan-stocks", "--company", "3", *argv], stock_cycle_runner=runner, out=out, err=err)
        return code, out.getvalue(), (reports[0] if reports else None)

    def office_calls(self) -> list[int]:
        return [int(parse_qs(urlsplit(c.url).query)["officeid"][0]) for c in self.adapter.calls]


# --- descubrimiento / orden -------------------------------------------------------------------


def test_six_active_offices_scanned_serially_in_order_and_inactive_excluded():
    w = World()
    code, out, report = w.cli()
    assert code == cli.EXIT_SUCCESS and report.status == SUCCESS
    assert report.offices_expected == [1, 2, 3, 4, 5, 6]
    assert [o for _, o, _ in w.scans] == [1, 2, 3, 4, 5, 6]
    calls = w.office_calls()
    assert calls == sorted(calls) and 7 not in calls
    assert sorted(o for _, o in w.store.stock_rows(3)) == sorted([o for o in range(1, 7)] * len(VARIANTS))
    assert all(run["mode"] == "SCANNER" and run["trigger"] == "STOCK_CYCLE" for run in w.store.runs.values())
    assert len(w.store.runs) == 6
    assert "offices_expected=1,2,3,4,5,6" in out and "offices_completed=1,2,3,4,5,6" in out


def test_discovery_is_dynamic_not_hardcoded():
    w = World([OfficeRow(9, "Nueva", 0, False), OfficeRow(2, "Sucursal 2", 0, False)])
    w.adapter.items += [stock(10, 9)]
    report = w.run()
    assert report.offices_expected == [2, 9] and report.status == SUCCESS
    assert [o for _, o, _ in w.scans] == [2, 9]


def test_missing_and_unknown_state_offices_excluded_and_reported():
    offices = [OfficeRow(1, "a", 0, False), OfficeRow(2, "b", 0, True), OfficeRow(3, "c", None, False),
               OfficeRow(4, "d", 5, False)]
    active, warnings = select_active_offices(offices)
    assert active == [1]
    assert len(warnings) == 2 and "office_id=3" in warnings[0] and "office_id=4" in warnings[1]
    w = World(offices)
    code, out, report = w.cli()
    assert code == cli.EXIT_SUCCESS and report.offices_expected == [1]
    assert "warning=office_id=3 con state=None desconocido: excluida" in out


@pytest.mark.parametrize("offices,fragment", [
    ([], "sin filas"),
    ([OfficeRow(7, "Cerrada", 1, False), OfficeRow(1, "x", 0, True)], "sin sucursales activas"),
])
def test_zero_active_offices_fails_safely_without_scanning(offices, fragment):
    w = World(offices)
    code, out, report = w.cli()
    assert code == cli.EXIT_FAILED and report.status == FAILED
    assert fragment in report.error and w.scans == [] and w.adapter.calls == []
    assert not w.store.held


def test_offices_metadata_unreadable_fails_without_scanning():
    w = World(reader_error=RuntimeError("relation bsale_raw.offices does not exist"))
    code, out, report = w.cli()
    assert code == cli.EXIT_FAILED and "no se pudo leer bsale_raw.offices" in out
    assert w.scans == [] and not w.store.held


# --- errores ----------------------------------------------------------------------------------


def test_one_office_failure_is_isolated_partial_and_nonzero_exit():
    w = World()
    w.failing = {3}
    code, out, report = w.cli()
    assert code == cli.EXIT_PARTIAL and report.status == PARTIAL
    assert report.offices_failed == [3] and report.offices_completed == [1, 2, 4, 5, 6]
    assert [o for _, o, _ in w.scans] == [1, 2, 3, 4, 5, 6]
    assert "office=3 status=FAILED" in out and "offices_failed=3" in out


def test_all_offices_failing_is_failed():
    w = World()
    w.failing = {1, 2, 3, 4, 5, 6}
    code, _, report = w.cli()
    assert code == cli.EXIT_FAILED and report.status == FAILED


def test_exception_in_scan_is_isolated():
    w = World()
    real = w.scan

    def scan(company_id, office_id, dry_run):
        if office_id == 2:
            raise RuntimeError("conexión PG caída")
        return real(company_id, office_id, dry_run)

    report = run_stock_cycle(company_id=3, reader=w.reader, lock=w.store, scan=scan)
    assert report.status == PARTIAL and report.offices_failed == [2]
    assert "conexión PG caída" in report.results[1].error


def test_missing_token_fails_every_office_without_http():
    env = dict(ENV)
    del env["BSALE_TOKEN_SPA"]
    w = World(env=env)
    code, out, report = w.cli()
    assert code == cli.EXIT_FAILED and w.adapter.calls == []
    assert "BSALE_TOKEN_SPA" in out and TOKEN not in out


# --- concurrencia -----------------------------------------------------------------------------


def test_overlapping_cycle_is_skipped_without_scanning():
    w = World()
    w.store.held.add(CYCLE_KEY)
    code, out, report = w.cli()
    assert code == cli.EXIT_LOCKED and report.status == SKIPPED
    assert "ya en ejecución" in report.error and "status=SKIPPED" in out
    assert w.scans == [] and w.reader.calls == 0 and w.store.runs == {}


def test_cycle_lock_held_during_scans_and_released_after():
    w = World()
    seen = []
    w.during_scan = lambda office_id: seen.append(CYCLE_KEY in w.store.held)
    w.run()
    assert seen == [True] * 6 and CYCLE_KEY not in w.store.held


def test_cycle_lock_released_after_failure():
    w = World()
    w.failing = {1, 2, 3, 4, 5, 6}
    w.run()
    assert not w.store.held


def test_busy_office_lock_is_respected_and_reported():
    w = World()
    w.store.held.add(advisory_lock_keys(3, "stocks", "office:2"))
    code, out, report = w.cli()
    assert code == cli.EXIT_PARTIAL and report.offices_skipped == [2]
    assert 2 not in w.office_calls()
    assert "office=2 status=SKIPPED" in out and "lock ocupado" in out


def test_point_runs_while_cycle_is_scanning():
    w = World()
    points = []

    def during(office_id):
        if office_id == 3:
            points.append(refresh_stock_point(
                store=w.store, company_id=3, variant_id=10, office_id=1,
                client_factory=client_factory_for(w.adapter), clock=w.clock, getenv=ENV.get, host="test",
            ))

    w.during_scan = during
    report = w.run()
    assert report.status == SUCCESS and points[0].status == "SUCCESS"


# --- dry-run / semántica de stock -------------------------------------------------------------


def test_dry_run_propagated_writes_nothing_and_takes_no_lock():
    w = World()
    code, out, report = w.cli("--dry-run")
    assert code == cli.EXIT_SUCCESS and report.dry_run
    assert all(dry for _, _, dry in w.scans)
    assert w.store.stock_rows(3) == {} and w.store.runs == {}
    assert "lock" not in w.store.events
    assert out.startswith("dry_run=true")
    assert report.total("rows_inserted") == 6 * len(VARIANTS)


def test_never_deletes_nor_zeroes_absent_rows_and_keeps_exact_quantities():
    w = World()
    w.store.seed_stock(3, stock(99, 1, quantity=7.0, reserved=1.0, available=6.0), fetched_at=old())
    w.adapter.items.append(stock(50, 2, quantity=10.0, reserved=3.0, available=8.0))
    report = w.run()
    assert report.status == SUCCESS
    rows = w.store.stock_rows(3)
    assert rows[(99, 1)]["payload"]["quantity"] == 7.0
    assert (rows[(50, 2)]["quantity"], rows[(50, 2)]["quantity_reserved"], rows[(50, 2)]["quantity_available"]) == (10.0, 3.0, 8.0)
    assert all(run["mode"] != "FULL_RECONCILE" for run in w.store.runs.values())
    assert "delete_stale_stock" not in w.store.events


def test_newer_point_row_is_not_overwritten_by_cycle():
    w = World()
    future = BASE.replace(year=BASE.year + 1)
    w.store.seed_stock(3, stock(10, 1, quantity=1.0, reserved=0.0, available=1.0), fetched_at=future)
    report = w.run()
    assert report.total("rows_skipped_newer") == 1
    assert w.store.stock_rows(3, 1)[(10, 1)]["payload"]["quantity"] == 1.0


def test_rerun_is_idempotent_and_summary_consolidates_totals():
    w = World()
    first = w.run()
    second = w.run()
    n = 6 * len(VARIANTS)
    assert first.total("rows_inserted") == n
    assert (second.total("rows_inserted"), second.total("rows_updated"), second.total("rows_unchanged")) == (0, 0, n)
    assert second.total("requests") == sum(r.counters["requests"] for r in second.results) == 6
    text = format_cycle(second)
    for key in ("company_id=3", "offices_expected=", "offices_completed=", "offices_failed=", "offices_skipped=",
                f"rows_received={n}", "rows_inserted=0", "rows_updated=0", f"rows_unchanged={n}",
                "rows_skipped_newer=0", "requests=6", "http_429=0", "http_5xx=0", "duration_ms=", "status=SUCCESS"):
        assert key in text


def test_credentials_never_in_output_or_logs(caplog):
    w = World()
    w.failing = {2}
    with caplog.at_level(logging.DEBUG):
        code, out, report = w.cli()
    assert code == cli.EXIT_PARTIAL
    for value in ENV.values():
        assert value not in out and value not in caplog.text and value not in repr(report)


# --- CLI / SQL --------------------------------------------------------------------------------


@pytest.mark.parametrize("argv", [["scan-stocks"], ["scan-stocks", "--company", "0"], ["scan-stocks", "--company", "x"]])
def test_cli_usage_errors(argv):
    called = []
    code = cli.main(argv, stock_cycle_runner=lambda **kw: called.append(kw), out=io.StringIO(), err=io.StringIO())
    assert code == cli.EXIT_USAGE and called == []


@pytest.mark.parametrize("status,expected", [(SUCCESS, 0), (PARTIAL, 2), (FAILED, 1), (SKIPPED, 3)])
def test_cli_exit_codes(status, expected):
    report = stock_cycle.CycleReport(company_id=3, status=status)
    code = cli.main(["scan-stocks", "--company", "3"], stock_cycle_runner=lambda **kw: report,
                    out=io.StringIO(), err=io.StringIO())
    assert code == expected


def test_pg_store_reads_offices_read_only():
    conn = FakeConnection(next_results=[[(1, "BODEGA CENTRAL", 0, False), (7, "Cerrada", 1, False), (8, None, 0, True)]])
    store = PgStockCycleStore(lambda: conn)
    offices = store.list_offices(3)
    assert offices == [OfficeRow(1, "BODEGA CENTRAL", 0, False), OfficeRow(7, "Cerrada", 1, False),
                       OfficeRow(8, None, 0, True)]
    assert conn.session.get("readonly") is True
    sql, params = conn.executed[0]
    assert sql.startswith("SELECT bsale_id, name, state, missing_since IS NOT NULL FROM bsale_raw.offices")
    assert "ORDER BY bsale_id" in sql and params == (3,)


def test_cycle_module_has_no_destructive_sql_nor_reconcile():
    import re
    from pathlib import Path

    source = Path(stock_cycle.__file__).read_text(encoding="utf-8")
    assert not re.search(r"\b(INSERT\s+INTO|UPDATE\s+\w+(\.\w+)?\s+SET|DELETE\s+FROM|TRUNCATE)\b", source, re.I)
    assert "FULL_RECONCILE" not in source.replace("no hay FULL_RECONCILE", "")
    assert "mode=SyncMode.SCANNER" in source
