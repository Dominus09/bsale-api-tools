"""Orquestador nocturno ``sync-nightly``. Sin red y sin BD real.

Integración: ``run_nightly`` + ``run_entity_sync`` real + ``FakeStore`` en memoria + Bsale falso
(stack HTTP real sobre un adapter de ``requests``). La SQL de ``PgNightlyReader`` se valida con un
cursor falso.
"""

from __future__ import annotations

import copy
import io
import logging
import re
from pathlib import Path

import pytest
import requests

from backend.jobs.bsale_raw import cli
from backend.services.bsale_raw import nightly
from backend.services.bsale_raw.core.engine import UnsupportedSyncError, run_entity_sync
from backend.services.bsale_raw.core.models import RunStatus, SyncMode
from backend.services.bsale_raw.core.registry import REGISTRY
from backend.services.bsale_raw.core.store import EntityOutcome
from backend.services.bsale_raw.nightly import (
    FAILED,
    NEW_UNCLASSIFIED,
    NIGHTLY_RESOURCES,
    PARTIAL,
    ROLE_CODE_SII_DRIFT,
    ROLE_METADATA_MISSING,
    SKIPPED_DEPENDENCY,
    SUCCESS,
    UNCLASSIFIED,
    PgNightlyReader,
    SourceRow,
    classify_document_types,
    format_report,
    run_nightly,
)
from backend.tests.bsale_raw.test_bsale_raw_catalog_resources import product, variant
from backend.tests.bsale_raw.test_bsale_raw_config_resources import (
    document_type,
    price_list,
    product_type,
    tax,
)
from backend.tests.bsale_raw.test_bsale_raw_pipeline import (
    BASE,
    ENV,
    TOKEN,
    FakeBsale,
    FakeConnection,
    FakeStore,
    TickClock,
    client_factory_for,
    office,
)

COMPANIES = (1, 2, 3)
ALL_TOKENS = [v for v in ENV.values()]


def default_catalog() -> dict[str, list[dict]]:
    return {
        "taxes": [tax(1)],
        "document_types": [
            document_type(1, name="BOLETA ELECTRÓNICA T", codeSii="39"),
            document_type(6, name="FACTURA ELECTRÓNICA T", codeSii="33"),
        ],
        "product_types": [product_type(4)],
        "offices": [office(1)],
        "price_lists": [price_list(1)],
        "products": [product(1), product(2)],
        "variants": [variant(10, product_id=1), variant(11, product_id=2)],
    }


def default_roles() -> list[dict]:
    return [
        {"document_type_id": 1, "role": "BOLETA", "active": True, "expected_code_sii": 39},
        {"document_type_id": 6, "role": "FACTURA", "active": True, "expected_code_sii": 33},
    ]


class FakeNightlyReader:
    """Misma semántica que ``PgNightlyReader`` sobre el ``FakeStore`` (reloj de BD del store)."""

    def __init__(self, world: "World") -> None:
        self.w = world

    def list_sources(self):
        if self.w.sources_error:
            raise RuntimeError(f"connection refused password=pw-SECRET {TOKEN}")
        return [
            SourceRow(s.company_id, s.name, s.token_env, s.company_id not in self.w.inactive)
            for s in sorted(self.w.store.sources.values(), key=lambda s: s.company_id)
        ]

    def resolve_source(self, company_id):
        return self.w.store.resolve_source(company_id)

    def _new(self, resource, company_id, run_id):
        started = self.w.store.runs[run_id]["started_at"]
        table = self.w.store.tables[REGISTRY.get(resource).raw_table]
        return sorted(
            ((bid, r) for (cid, bid), r in table.items()
             if cid == company_id and r["sync_run_id"] == run_id and r["first_seen_at"] >= started),
            key=lambda x: x[0],
        )

    def new_rows(self, resource, company_id, run_id, columns):
        return [{"bsale_id": bid, **{c: r[c] for c in columns}} for bid, r in self._new(resource, company_id, run_id)]

    def count_new_rows(self, resource, company_id, run_id):
        return len(self._new(resource, company_id, run_id))

    def document_types(self, company_id):
        return [
            {"bsale_id": bid, "name": r["name"], "code_sii": r["code_sii"], "state": r["state"],
             "missing_since": r["missing_since"]}
            for bid, r in sorted(self.w.store.rows(company_id, "bsale_raw.document_types").items())
        ]

    def document_type_roles(self, company_id):
        self.w.role_reads.append(company_id)
        return copy.deepcopy(self.w.roles.get(company_id, []))


class World:
    def __init__(self, env: dict | None = None) -> None:
        self.store = FakeStore()
        self.clock = TickClock(BASE)
        self.catalog = {cid: default_catalog() for cid in COMPANIES}
        self.roles = {cid: default_roles() for cid in COMPANIES}
        self.env = dict(ENV) if env is None else env
        self.failing: set[tuple[int, str]] = set()
        self.inactive: set[int] = set()
        self.sources_error = False
        self.calls: list[tuple[int, str]] = []
        self.urls: list[str] = []
        self.role_reads: list[int] = []

    def factory(self, source, token, spec):
        cid = source.company_id
        self.calls.append((cid, spec.name))
        if (cid, spec.name) in self.failing:
            adapter = FakeBsale(script=[requests.ConnectionError("bsale down")] * 10, store=self.store)
        else:
            adapter = FakeBsale(self.catalog[cid][spec.name], store=self.store)
        real_send = adapter.send

        def send(request, **kw):
            self.urls.append(request.url)
            return real_send(request, **kw)

        adapter.send = send
        return client_factory_for(adapter)(source, token, spec)

    def sync(self, company_id, resource, dry_run):
        return run_entity_sync(
            store=self.store, company_id=company_id, resource=resource, mode=SyncMode.FULL_RECONCILE,
            dry_run=dry_run, client_factory=self.factory, clock=self.clock, getenv=self.env.get,
            trigger=nightly.TRIGGER_NIGHTLY, host="test",
        )

    def run(self, *, sync=None, **kw):
        return run_nightly(reader=FakeNightlyReader(self), sync=sync or self.sync, getenv=self.env.get, **kw)

    def cli(self, *argv, sync=None):
        out = io.StringIO()
        reports = []

        def runner(*, companies, dry_run):
            reports.append(self.run(sync=sync, companies=companies, dry_run=dry_run))
            return reports[-1]

        code = cli.main(["sync-nightly", *argv], nightly_runner=runner, out=out, err=io.StringIO())
        return code, out.getvalue(), (reports[0] if reports else None)


def statuses(report, company_id):
    company = next(c for c in report.companies if c.company_id == company_id)
    return {r.resource: r.status for r in company.resources}


def company(report, company_id):
    return next(c for c in report.companies if c.company_id == company_id)


# --- orden / alcance --------------------------------------------------------------------------


def test_runs_every_company_1_2_3_with_resources_in_fixed_order():
    w = World()
    code, out, report = w.cli()
    assert code == cli.EXIT_SUCCESS and report.status == SUCCESS
    assert w.calls == [(cid, res) for cid in COMPANIES for res in NIGHTLY_RESOURCES]
    assert NIGHTLY_RESOURCES == (
        "taxes", "document_types", "product_types", "offices", "price_lists", "products", "variants",
    )
    assert [c.company_id for c in report.companies] == [1, 2, 3]
    for run in w.store.runs.values():
        assert run["trigger"] == "NIGHTLY" and run["mode"] == "FULL_RECONCILE" and run["status"] == "SUCCESS"
    assert len(w.store.runs) == 21
    assert "NIGHTLY BSALE SUMMARY" in out and "TOTAL_DURATION:" in out


def test_summary_has_every_section_and_resource_record_fields():
    w = World()
    _, out, report = w.cli()
    for section in ("NIGHTLY BSALE SUMMARY", "NEW_DOCUMENT_TYPES:", "UNCLASSIFIED_DOCUMENT_TYPES:",
                    "NEW_PRICE_LISTS:", "NEW_PRODUCTS:", "NEW_VARIANTS:", "FAILED_RESOURCES:",
                    "ROLE_METADATA_DRIFT:", "TOTAL_DURATION:"):
        assert section in out
    r = company(report, 3).resources[0]
    assert r.company_id == 3 and r.resource == "taxes" and r.run_id is not None
    assert r.started_at is not None and r.finished_at is not None and r.started_at <= r.finished_at
    assert (r.api_count, r.fetched, r.inserted, r.updated, r.unchanged, r.missing, r.errors) == (1, 1, 1, 0, 0, 0, None)
    assert r.duration_ms >= 0


def test_only_active_sources_by_default_and_company_filter():
    w = World()
    w.inactive = {2}
    report = w.run()
    assert [c.company_id for c in report.companies] == [1, 3] and report.inactive_sources == [2]
    w2 = World()
    code, _, report2 = w2.cli("--company", "3", "--company", "1")
    assert code == cli.EXIT_SUCCESS
    assert [c.company_id for c in report2.companies] == [3, 1]
    assert {cid for cid, _ in w2.calls} == {1, 3}


# --- aislamiento por empresa / configuración --------------------------------------------------


def test_missing_token_fails_that_company_only_without_leaking():
    env = dict(ENV)
    del env["BSALE_TOKEN_Mini"]
    w = World(env=env)
    code, out, report = w.cli()
    assert code == cli.EXIT_PARTIAL and report.status == PARTIAL
    c1 = company(report, 1)
    assert c1.status == FAILED and c1.resources == [] and "BSALE_TOKEN_Mini" in c1.error
    assert not any(cid == 1 for cid, _ in w.calls)
    assert company(report, 2).status == SUCCESS and company(report, 3).status == SUCCESS
    for value in env.values():
        assert value not in out


def test_company_not_in_sources_fails_and_others_continue():
    w = World()
    report = w.run(companies=[9, 3])
    assert company(report, 9).status == FAILED and "bsale_raw.sources" in company(report, 9).error
    assert company(report, 3).status == SUCCESS and report.status == PARTIAL


def test_company_1_failing_does_not_block_2_and_3():
    w = World()
    w.failing = {(1, res) for res in NIGHTLY_RESOURCES}
    code, out, report = w.cli()
    assert code == cli.EXIT_PARTIAL
    assert company(report, 1).status == FAILED
    assert statuses(report, 1)["variants"] == SKIPPED_DEPENDENCY
    assert company(report, 2).status == SUCCESS and company(report, 3).status == SUCCESS
    assert "company_id=1 resource=products status=FAILED" in out


# --- dependencias -----------------------------------------------------------------------------


def test_products_failed_skips_variants_only_in_that_company():
    w = World()
    w.failing = {(2, "products")}
    code, out, report = w.cli()
    assert code == cli.EXIT_PARTIAL
    s2 = statuses(report, 2)
    assert s2["products"] == FAILED and s2["variants"] == SKIPPED_DEPENDENCY
    assert all(s2[r] == SUCCESS for r in NIGHTLY_RESOURCES if r not in ("products", "variants"))
    assert (2, "variants") not in w.calls
    assert statuses(report, 3)["variants"] == SUCCESS
    assert "resource=variants status=SKIPPED_DEPENDENCY error=products FAILED" in out


def test_other_failure_does_not_block_remaining_resources():
    w = World()
    w.failing = {(3, "taxes"), (3, "price_lists")}
    report = w.run()
    s3 = statuses(report, 3)
    assert s3["taxes"] == FAILED and s3["price_lists"] == FAILED
    assert all(s3[r] == SUCCESS for r in NIGHTLY_RESOURCES if r not in ("taxes", "price_lists"))
    assert company(report, 3).status == PARTIAL and report.status == PARTIAL


def test_lock_busy_counts_as_failed_and_skips_variants():
    w = World()

    def sync(cid, resource, dry_run):
        if (cid, resource) == (3, "products"):
            return EntityOutcome(company_id=cid, resource=resource, scope="", mode="FULL_RECONCILE",
                                 status=RunStatus.SKIPPED.value, error="LockBusyError: lock ocupado")
        return w.sync(cid, resource, dry_run)

    report = w.run(sync=sync)
    s3 = statuses(report, 3)
    assert s3["products"] == FAILED and s3["variants"] == SKIPPED_DEPENDENCY
    assert "lock ocupado" in company(report, 3).resources[5].errors


def test_exception_in_one_resource_is_isolated():
    w = World()

    def sync(cid, resource, dry_run):
        if resource == "offices":
            raise UnsupportedSyncError("offices no habilitado")
        return w.sync(cid, resource, dry_run)

    report = w.run(sync=sync)
    for cid in COMPANIES:
        assert statuses(report, cid)["offices"] == FAILED
        assert statuses(report, cid)["variants"] == SUCCESS
    assert report.status == PARTIAL


# --- tipos de documento / roles ---------------------------------------------------------------


def test_new_document_type_reported_with_exact_name_and_no_auto_role():
    w = World()
    assert w.run().status == SUCCESS
    roles_before = copy.deepcopy(w.roles)
    exact = "  Nota Interna Ñandú (no SII)  "
    w.catalog[3]["document_types"].append(document_type(70, name=exact, codeSii="0", state=0))
    code, out, report = w.cli()
    assert code == cli.EXIT_PARTIAL and report.status == PARTIAL
    [f] = report.new_document_types
    assert (f.kind, f.company_id, f.document_type_id, f.name, f.code_sii, f.state) == (NEW_UNCLASSIFIED, 3, 70, exact, "0", 0)
    assert '"  Nota Interna Ñandú (no SII)  "' in out
    assert "NEW_UNCLASSIFIED_DOCUMENT_TYPE company_id=3 document_type_id=70" in out
    assert w.roles == roles_before
    assert w.store.rows(3, "bsale_raw.document_types")[70]["name"] == exact


def test_old_unclassified_type_is_not_reported_as_new_again():
    w = World()
    w.catalog[3]["document_types"].append(document_type(70, name="Nota Interna", codeSii="0"))
    first = w.run()
    assert [f.document_type_id for f in first.new_document_types] == [70] and first.status == PARTIAL
    second = w.run()
    assert second.new_document_types == []
    assert [(f.kind, f.document_type_id) for f in second.unclassified_document_types] == [(UNCLASSIFIED, 70)]
    assert second.status == SUCCESS


def test_first_run_of_a_company_without_roles_reports_all_types_as_new():
    w = World()
    w.roles[1] = []
    report = w.run()
    assert sorted((f.company_id, f.document_type_id) for f in report.new_document_types) == [(1, 1), (1, 6)]
    assert report.status == PARTIAL


def test_type_with_only_inactive_role_is_unclassified():
    w = World()
    w.roles[3][0]["active"] = False
    report = w.run()
    [f] = report.new_document_types
    assert f.document_type_id == 1 and f.inactive_role == "BOLETA"


def test_role_metadata_missing_when_type_absent_in_bsale():
    w = World()
    w.roles[3].append({"document_type_id": 99, "role": "NOTA_CREDITO", "active": True, "expected_code_sii": 61})
    roles_before = copy.deepcopy(w.roles)
    code, out, report = w.cli()
    assert code == cli.EXIT_PARTIAL
    [d] = report.role_drift
    assert (d.kind, d.company_id, d.document_type_id, d.role, d.active) == (ROLE_METADATA_MISSING, 3, 99, "NOTA_CREDITO", True)
    assert "ROLE_METADATA_MISSING company_id=3 document_type_id=99 role=NOTA_CREDITO active=true" in out
    assert w.roles == roles_before


def test_role_code_sii_drift():
    w = World()
    w.catalog[3]["document_types"][1] = document_type(6, name="FACTURA ELECTRÓNICA T", codeSii="34")
    code, out, report = w.cli()
    assert code == cli.EXIT_PARTIAL
    [d] = report.role_drift
    assert (d.kind, d.document_type_id, d.expected_code_sii, d.bsale_code_sii) == (ROLE_CODE_SII_DRIFT, 6, 33, "34")
    assert "ROLE_CODE_SII_DRIFT company_id=3 document_type_id=6 role=FACTURA" in out


def test_role_validation_only_after_document_types_success():
    w = World()
    w.failing = {(3, "document_types")}
    report = w.run()
    assert 3 not in w.role_reads and sorted(w.role_reads) == [1, 2]
    assert report.status == PARTIAL


def test_classify_identity_by_id_not_name_and_code_sii_tolerates_whitespace():
    types = [
        {"bsale_id": 6, "name": "Otra cosa", "code_sii": " 33 ", "state": 0, "missing_since": None},
        {"bsale_id": 7, "name": "FACTURA ELECTRÓNICA T", "code_sii": "33", "state": 0, "missing_since": None},
        {"bsale_id": 8, "name": "x", "code_sii": "52", "state": 0, "missing_since": BASE},
    ]
    roles = [
        {"document_type_id": 6, "role": "FACTURA", "active": True, "expected_code_sii": 33},
        {"document_type_id": 26, "role": "COTIZACION", "active": True, "expected_code_sii": None},
        {"document_type_id": 8, "role": "GUIA_DESPACHO", "active": False, "expected_code_sii": 52},
    ]
    findings, drift = classify_document_types(3, types, roles, new_ids={7})
    assert [(f.kind, f.document_type_id) for f in findings] == [(NEW_UNCLASSIFIED, 7)]
    assert [(d.kind, d.document_type_id, d.active) for d in drift] == [
        (ROLE_METADATA_MISSING, 8, False), (ROLE_METADATA_MISSING, 26, True),
    ]


# --- listas / catálogo ------------------------------------------------------------------------


def test_new_price_list_reported_without_details():
    w = World()
    w.run()
    w.urls.clear()
    w.catalog[2]["price_lists"].append(price_list(20, name="Mayorista Norte ", state=1))
    code, out, report = w.cli()
    assert code == cli.EXIT_SUCCESS
    assert [(p.company_id, p.price_list_id, p.name, p.state) for p in report.new_price_lists] == [(2, 20, "Mayorista Norte ", 1)]
    assert 'NEW_PRICE_LIST company_id=2 price_list_id=20 name="Mayorista Norte " state=1' in out
    assert not any("details" in u for u in w.urls)


def test_new_products_and_variants_are_counts():
    w = World()
    first = w.run()
    assert first.new_products == {1: 2, 2: 2, 3: 2} and first.new_variants == {1: 2, 2: 2, 3: 2}
    w.catalog[3]["products"].append(product(3))
    w.catalog[3]["variants"] += [variant(12, product_id=3), variant(13, product_id=3)]
    w.catalog[3]["variants"][0] = variant(10, product_id=1, description="Variante 10 renombrada")
    code, out, report = w.cli()
    assert code == cli.EXIT_SUCCESS
    assert report.new_products == {1: 0, 2: 0, 3: 1} and report.new_variants == {1: 0, 2: 0, 3: 2}
    v3 = company(report, 3).resources[-1]
    assert (v3.fetched, v3.inserted, v3.updated, v3.missing) == (4, 2, 1, 0)
    assert "company_id=3 count=1" in out.split("NEW_PRODUCTS:")[1].split("NEW_VARIANTS:")[0]


# --- idempotencia / estados -------------------------------------------------------------------


def test_rerun_is_idempotent():
    w = World()
    assert w.run().status == SUCCESS
    tables_before = copy.deepcopy(w.store.tables)
    report = w.run()
    assert report.status == SUCCESS
    for c in report.companies:
        for r in c.resources:
            assert (r.inserted, r.updated, r.missing) == (0, 0, 0) and r.unchanged == r.fetched
    assert report.new_document_types == [] and report.new_price_lists == []
    assert report.new_products == {1: 0, 2: 0, 3: 0} and report.new_variants == {1: 0, 2: 0, 3: 0}
    for table, rows in w.store.tables.items():
        for key, row in rows.items():
            assert row["payload_hash"] == tables_before[table][key]["payload_hash"]
            assert row["first_seen_at"] == tables_before[table][key]["first_seen_at"]


def test_global_failed_when_no_company_could_be_processed():
    w = World()
    w.failing = {(cid, res) for cid in COMPANIES for res in NIGHTLY_RESOURCES}
    code, out, report = w.cli()
    assert code == cli.EXIT_FAILED and report.status == FAILED
    assert "status=FAILED" in out


def test_global_failed_when_sources_unreadable_without_leaking():
    w = World()
    w.sources_error = True
    code, out, report = w.cli()
    assert code == cli.EXIT_FAILED and report.status == FAILED and report.companies == []
    assert w.calls == []
    assert TOKEN not in out and "pw-SECRET" not in out
    assert "no se pudo leer bsale_raw.sources (RuntimeError)" in out


def test_global_failed_when_no_active_sources():
    w = World()
    w.inactive = set(COMPANIES)
    code, _, report = w.cli()
    assert code == cli.EXIT_FAILED and "empresas activas" in report.error


def test_dry_run_writes_nothing_and_skips_detection():
    w = World()
    code, out, report = w.cli("--dry-run")
    assert code == cli.EXIT_SUCCESS and report.dry_run
    assert all(not rows for rows in w.store.tables.values())
    assert w.role_reads == [] and report.new_products == {}
    assert "dry_run=true" in out


def test_tokens_never_in_output_or_logs(caplog):
    w = World()

    def sync(cid, resource, dry_run):
        if resource == "taxes":
            raise RuntimeError(f"Authorization: access_token {ENV['BSALE_TOKEN_Romero']} rechazado {TOKEN}")
        return w.sync(cid, resource, dry_run)

    with caplog.at_level(logging.DEBUG):
        code, out, report = w.cli(sync=sync)
    assert code == cli.EXIT_PARTIAL
    text = out + caplog.text + repr(report)
    for value in ALL_TOKENS:
        assert value not in text
    assert "***" in out


# --- CLI --------------------------------------------------------------------------------------


@pytest.mark.parametrize("argv", [["sync-nightly", "--company", "0"], ["sync-nightly", "--bogus"]])
def test_cli_usage_errors(argv):
    called = []
    code = cli.main(argv, nightly_runner=lambda **kw: called.append(kw), out=io.StringIO(), err=io.StringIO())
    assert code == cli.EXIT_USAGE and called == []


@pytest.mark.parametrize("status,expected", [(SUCCESS, 0), (PARTIAL, 2), (FAILED, 1)])
def test_cli_exit_codes(status, expected):
    report = nightly.NightlyReport(status=status)
    code = cli.main(["sync-nightly"], nightly_runner=lambda **kw: report, out=io.StringIO(), err=io.StringIO())
    assert code == expected


def test_existing_single_resource_sync_cli_unchanged():
    parser = cli.build_parser(REGISTRY.pipeline_names())
    args = parser.parse_args(["sync", "--company", "3", "--resource", "offices", "--mode", "full-reconcile"])
    assert args.command == "sync" and args.company == 3


# --- SQL del lector / alcance de escritura ----------------------------------------------------


def test_pg_reader_is_read_only_and_only_selects():
    conn = FakeConnection(next_results=[
        [(1, "Mini", "BSALE_TOKEN_Mini", True)],
        [(5, "Lista", 0)],
        [(7,)],
        [(1, "BOLETA", "39", 0, None)],
        [(1, "BOLETA", True, 39)],
    ])
    reader = PgNightlyReader(lambda: conn)
    import backend.services.bsale_raw.resources  # noqa: F401

    assert reader.list_sources() == [SourceRow(1, "Mini", "BSALE_TOKEN_Mini", True)]
    assert reader.new_rows("price_lists", 2, 77, ("name", "state")) == [{"bsale_id": 5, "name": "Lista", "state": 0}]
    assert reader.count_new_rows("variants", 3, 78) == 7
    assert reader.document_types(3)[0]["code_sii"] == "39"
    assert reader.document_type_roles(3)[0]["role"] == "BOLETA"
    assert conn.session.get("readonly") is True
    sqls = [s for s, _ in conn.executed]
    assert all(s.lstrip().upper().startswith("SELECT") for s in sqls)
    new_sql = sqls[1]
    assert "FROM bsale_raw.price_lists t JOIN bsale_raw.sync_runs r ON r.id = %s" in new_sql
    assert "t.sync_run_id = r.id AND t.first_seen_at >= r.started_at" in new_sql
    assert conn.executed[1][1] == (77, 2)
    assert "FROM distribuidora.document_type_roles" in sqls[4]
    with pytest.raises(ValueError):
        reader.new_rows("price_lists", 2, 77, ("payload",))


def test_nightly_module_has_no_writer_sql_and_no_bsale_schema():
    source = Path(nightly.__file__).read_text(encoding="utf-8")
    assert not re.search(r"\b(INSERT\s+INTO|UPDATE\s+\w+(\.\w+)?\s+SET|DELETE\s+FROM|TRUNCATE|ALTER\s+TABLE)\b", source, re.I)
    assert not re.search(r"(?<![\w.])bsale\.(?!io)", source)
    assert "distribuidora.document_type_roles" in source
    assert re.search(r"trigger=TRIGGER_NIGHTLY", source)
