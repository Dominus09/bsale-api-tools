"""Fase 4B: taxes, document_types, product_types, price_lists sobre el MISMO motor (sin red ni BD real)."""

from __future__ import annotations

import copy
import io
import logging
from datetime import timedelta
from decimal import Decimal
from urllib.parse import parse_qs, urlsplit

import pytest

from backend.jobs.bsale_raw import cli
from backend.services.bsale_raw.core.registry import REGISTRY, KeyKind
from backend.tests.bsale_raw._raw_sql_schema import parse
from backend.tests.bsale_raw.test_bsale_raw_pipeline import (
    BASE,
    NOT_YET_ENABLED,
    TOKEN,
    FakeBsale,
    FakeStore,
    TickClock,
    run,
)

API = "https://api.bsale.io/v1"
NEW_RESOURCES = ["taxes", "document_types", "product_types", "price_lists"]


def tax(i: int, **over) -> dict:
    item = {"href": f"{API}/taxes/{i}.json", "id": i, "name": f"IVA {i}", "percentage": "19.0",
            "forRetention": 0, "controlTax": 0, "code": "14", "overtax": 0, "state": 0, "ledgerAccount": ""}
    item.update(over)
    return item


def document_type(i: int, **over) -> dict:
    item = {"href": f"{API}/document_types/{i}.json", "id": i, "name": f"Tipo {i}", "initialNumber": 1,
            "codeSii": "33", "isElectronicDocument": 1, "breakdownTax": 1, "use": 1, "isSalesNote": 0,
            "isExempt": 0, "restrictsTax": 0, "useClient": 1, "thermalPrinter": 0, "state": 0, "copyNumber": 1,
            "isCreditNote": 0, "continuedHigh": 0, "ledgerAccount": "", "ipadPrint": 0, "ipadPrintHigh": 0,
            "restrictClientType": 0, "useMaxDays": 0, "maxDays": 0,
            "book_type": {"href": f"{API}/book_types/1.json", "id": "1"}}
    item.update(over)
    return item


def product_type(i: int, **over) -> dict:
    item = {"href": f"{API}/product_types/{i}.json", "id": i, "name": f"Categoría {i}", "isEditable": 1,
            "state": 0, "imagestionCategoryId": 0, "prestashopCategoryId": 0,
            "attributes": {"href": f"{API}/product_types/{i}/attributes.json"}}
    item.update(over)
    return item


def price_list(i: int, **over) -> dict:
    item = {"href": f"{API}/price_lists/{i}.json", "id": i, "name": f"Lista {i}", "description": "",
            "state": 0, "coin": {"href": f"{API}/coins/1.json", "id": "1"},
            "details": {"href": f"{API}/price_lists/{i}/details.json"}}
    item.update(over)
    return item


FACTORIES = {"taxes": tax, "document_types": document_type, "product_types": product_type, "price_lists": price_list}

EXPECTED_TYPED = {
    "taxes": lambda p: {"state": p["state"], "name": p["name"], "code": p["code"],
                        "percentage": Decimal(str(p["percentage"]))},
    "document_types": lambda p: {"state": p["state"], "name": p["name"], "code_sii": p["codeSii"],
                                 "is_electronic": p["isElectronicDocument"]},
    "product_types": lambda p: {"state": p["state"], "name": p["name"]},
    "price_lists": lambda p: {"state": p["state"], "name": p["name"], "coin_id": int(p["coin"]["id"])},
}


def setup(resource, items, **kw):
    store = FakeStore()
    adapter = FakeBsale(items, store=store, **kw)
    return store, adapter, REGISTRY.get(resource).raw_table


def go(store, adapter, resource, **kw):
    return run(store, adapter, resource=resource, **kw)


# --- registry / endpoint ----------------------------------------------------------------------


@pytest.mark.parametrize("resource", NEW_RESOURCES)
def test_resource_spec_declared_and_enabled(resource):
    spec = REGISTRY.get(resource)
    assert spec.pipeline_enabled and spec.key_kind is KeyKind.ENTITY and spec.full_reconcile
    assert spec.list_endpoint == f"/v1/{resource}.json"
    assert spec.raw_table == f"bsale_raw.{resource}"
    assert [c.column for c in spec.typed_columns] == list(EXPECTED_TYPED[resource](FACTORIES[resource](1)))


@pytest.mark.parametrize("resource", NEW_RESOURCES)
def test_typed_columns_exist_in_migration(resource):
    tables, _ = parse()
    columns = tables[resource].columns
    for col in REGISTRY.get(resource).typed_columns:
        assert col.column in columns, col.column


@pytest.mark.parametrize("resource", NEW_RESOURCES)
def test_single_sweep_without_state_or_expand_and_no_double_v1(resource):
    items = [FACTORIES[resource](i) for i in range(1, 61)]
    store, adapter, _ = setup(resource, items)
    out = go(store, adapter, resource)
    assert out.status == "SUCCESS", out.error
    assert {urlsplit(c.url).path for c in adapter.calls} == {f"/v1/{resource}.json"}
    for call in adapter.calls:
        assert set(parse_qs(urlsplit(call.url).query)) == {"limit", "offset"}
    assert out.pages == 2 and out.api_count == 60


# --- columnas / payload / state ---------------------------------------------------------------


@pytest.mark.parametrize("resource", NEW_RESOURCES)
def test_typed_columns_payload_and_state_preserved(resource):
    make = FACTORIES[resource]
    items = [make(1, state=0), make(2, state=1), make(3, state=0)]
    store, adapter, table = setup(resource, items)
    out = go(store, adapter, resource)
    assert out.status == "SUCCESS", out.error
    assert (out.api_count, out.rows_inserted, out.rows_missing) == (3, 3, 0)
    rows = store.rows(3, table)
    assert sorted(r["state"] for r in rows.values()) == [0, 0, 1]  # activos e inactivos conservados
    for item in items:
        row = rows[item["id"]]
        assert row["payload"] == item
        for column, value in EXPECTED_TYPED[resource](item).items():
            assert row[column] == value, column


@pytest.mark.parametrize("resource", NEW_RESOURCES)
def test_idempotency(resource):
    items = [FACTORIES[resource](i) for i in range(1, 5)]
    store, adapter, table = setup(resource, items)
    clock = TickClock(BASE)
    out1 = go(store, adapter, resource, clock=clock)
    assert (out1.rows_inserted, out1.rows_updated, out1.rows_unchanged) == (4, 0, 0)
    snap1 = copy.deepcopy(store.rows(3, table))
    out2 = go(store, adapter, resource, clock=clock)
    assert (out2.rows_inserted, out2.rows_updated, out2.rows_unchanged) == (0, 0, 4)
    assert out2.sync_run_id > out1.sync_run_id
    for bid, r2 in store.rows(3, table).items():
        r1 = snap1[bid]
        assert r2["first_seen_at"] == r1["first_seen_at"]
        assert r2["last_changed_at"] == r1["last_changed_at"]
        assert r2["last_seen_at"] > r1["last_seen_at"]
        assert r2["api_fetched_at"] > r1["api_fetched_at"]
        assert r2["sync_run_id"] == out2.sync_run_id
    state = store.sync_state[(3, resource, "global")]
    assert state["status"] == "SUCCESS" and state["last_sync_run_id"] == out2.sync_run_id


@pytest.mark.parametrize("resource", NEW_RESOURCES)
def test_missing_since_marked_and_cleared(resource):
    make = FACTORIES[resource]
    store, adapter, table = setup(resource, [make(i) for i in range(1, 10)])
    for i in range(1, 11):
        store.seed(3, make(i), fetched_at=BASE - timedelta(days=1), table=table)
    clock = TickClock(BASE)
    out = go(store, adapter, resource, clock=clock)
    assert out.status == "SUCCESS" and out.rows_missing == 1 and out.rows_deleted == 0
    assert store.rows(3, table)[10]["missing_since"] is not None and len(store.rows(3, table)) == 10

    adapter.items = [make(i) for i in range(1, 11)]
    out = go(store, adapter, resource, clock=clock)
    assert out.status == "SUCCESS" and store.rows(3, table)[10]["missing_since"] is None


@pytest.mark.parametrize("resource", NEW_RESOURCES)
def test_fuse_blocks_writes(resource):
    make = FACTORIES[resource]
    store, adapter, table = setup(resource, [make(i, name="cambio") for i in range(1, 8)])
    for i in range(1, 11):
        store.seed(3, make(i), fetched_at=BASE - timedelta(days=1), table=table)
    before = copy.deepcopy(store.rows(3, table))
    out = go(store, adapter, resource)
    assert out.status == "FAILED" and out.fuse["tripped"] and out.fuse["threshold_pct"] == 20.0
    assert store.rows(3, table) == before
    assert store.sync_state[(3, resource, "global")]["last_success_at"] is None


@pytest.mark.parametrize("resource", NEW_RESOURCES)
def test_stale_upsert_protection(resource):
    make = FACTORIES[resource]
    store, adapter, table = setup(resource, [make(1, name="snapshot viejo")])
    newer = BASE + timedelta(hours=3)
    store.seed(3, make(1, name="más nuevo"), fetched_at=newer, table=table)
    out = go(store, adapter, resource)
    assert out.status == "SUCCESS" and out.rows_skipped_newer == 1 and out.rows_updated == 0
    assert store.rows(3, table)[1]["payload"]["name"] == "más nuevo"


@pytest.mark.parametrize("resource", NEW_RESOURCES)
def test_company_isolation(resource):
    make = FACTORIES[resource]
    store, adapter, table = setup(resource, [make(1, name="C3")])
    for i in range(1, 6):
        store.seed(1, make(i, name="C1"), fetched_at=BASE - timedelta(days=1), table=table)
    before = copy.deepcopy(store.rows(1, table))
    out = go(store, adapter, resource, company_id=3)
    assert out.status == "SUCCESS" and out.rows_missing == 0
    assert store.rows(1, table) == before
    for other in store.tables:
        if other != table:
            assert store.tables[other] == {}, other


@pytest.mark.parametrize("resource", NEW_RESOURCES)
def test_token_never_logged(resource, caplog):
    caplog.set_level(logging.DEBUG)
    store, adapter, _ = setup(resource, [FACTORIES[resource](1)],
                              script=[(401, f'{{"error": "bad {TOKEN}"}}'.encode(), {})])
    out = go(store, adapter, resource)
    assert out.status == "FAILED"
    assert TOKEN not in out.error and TOKEN not in caplog.text
    assert TOKEN not in repr(store.runs) + repr(store.entity_runs) + repr(store.sync_state)
    assert TOKEN not in cli.format_outcome(out)


# --- reglas específicas -----------------------------------------------------------------------


def test_price_lists_never_downloads_details():
    store, adapter, table = setup("price_lists", [price_list(1, state=0), price_list(2, state=1)])
    out = go(store, adapter, "price_lists")
    assert out.status == "SUCCESS" and out.requests == 1
    assert all("details" not in c.url for c in adapter.calls)
    assert store.tables["bsale_raw.price_lists"] and "bsale_raw.variant_prices" not in store.tables
    assert store.rows(3, table)[1]["payload"]["details"] == {"href": f"{API}/price_lists/1/details.json"}


def test_document_type_33_preserved_without_oc33_logic():
    item = document_type(33, name="ORDEN DE COMPRA", codeSii="801", isElectronicDocument=1)
    store, adapter, table = setup("document_types", [item, document_type(1)])
    out = go(store, adapter, "document_types")
    assert out.status == "SUCCESS"
    row = store.rows(3, table)[33]
    assert row["payload"] == item and row["code_sii"] == "801" and row["name"] == "ORDEN DE COMPRA"
    assert {urlsplit(c.url).path for c in adapter.calls} == {"/v1/document_types.json"}
    assert not REGISTRY.get("documents").pipeline_enabled
    assert set(store.tables) == {REGISTRY.get(n).raw_table for n in REGISTRY.pipeline_names()}
    assert all(not rows for name, rows in store.tables.items() if name != table)


def test_document_type_empty_code_sii_kept_raw():
    store, adapter, table = setup("document_types", [document_type(5, codeSii="")])
    out = go(store, adapter, "document_types")
    assert out.status == "SUCCESS" and store.rows(3, table)[5]["code_sii"] == ""


@pytest.mark.parametrize("value, expected", [("19.0", Decimal("19.0")), (19, Decimal("19")),
                                             ("31.5", Decimal("31.5")), (None, None), ("", None)])
def test_tax_percentage_exact_decimal(value, expected):
    store, adapter, table = setup("taxes", [tax(1, percentage=value)])
    out = go(store, adapter, "taxes")
    assert out.status == "SUCCESS" and store.rows(3, table)[1]["percentage"] == expected


@pytest.mark.parametrize(
    "resource, bad",
    [("taxes", {"percentage": "diecinueve"}), ("price_lists", {"coin": "1"}), ("product_types", {"state": "x"})],
)
def test_invalid_typed_value_fails_without_writes(resource, bad):
    store, adapter, table = setup(resource, [FACTORIES[resource](1), FACTORIES[resource](2, **bad)])
    out = go(store, adapter, resource)
    assert out.status == "FAILED" and store.rows(3, table) == {}


def test_price_list_without_coin_is_null():
    item = price_list(1)
    del item["coin"]
    store, adapter, table = setup("price_lists", [item])
    out = go(store, adapter, "price_lists")
    assert out.status == "SUCCESS" and store.rows(3, table)[1]["coin_id"] is None


# --- CLI --------------------------------------------------------------------------------------


@pytest.mark.parametrize("resource", NEW_RESOURCES)
@pytest.mark.parametrize("dry_run", [True, False])
def test_cli_accepts_resource(resource, dry_run):
    seen = {}

    def runner(**kw):
        seen.update(kw)
        from backend.services.bsale_raw.core.store import EntityOutcome

        return EntityOutcome(company_id=3, resource=resource, scope="global", mode="FULL_RECONCILE",
                             status="SUCCESS", dry_run=dry_run)

    argv = ["sync", "--company", "3", "--resource", resource, "--mode", "full-reconcile"]
    buf = io.StringIO()
    assert cli.main(argv + (["--dry-run"] if dry_run else []), runner=runner, out=buf) == cli.EXIT_SUCCESS
    assert seen["resource"] == resource and seen["dry_run"] is dry_run
    assert f"resource={resource}" in buf.getvalue()


def test_cli_still_rejects_not_enabled():
    never = lambda **kw: pytest.fail("no debe ejecutarse")  # noqa: E731
    for name in NOT_YET_ENABLED:
        argv = ["sync", "--company", "3", "--resource", name, "--mode", "full-reconcile", "--dry-run"]
        assert cli.main(argv, runner=never, out=io.StringIO()) == cli.EXIT_USAGE
