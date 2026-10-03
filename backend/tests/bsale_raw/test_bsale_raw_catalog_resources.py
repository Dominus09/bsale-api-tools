"""Fase 4C: products y variants sobre el MISMO motor (sin red ni BD real)."""

from __future__ import annotations

import copy
import io
import logging
from datetime import timedelta
from urllib.parse import parse_qs, urlsplit

import pytest

from backend.jobs.bsale_raw import cli
from backend.services.bsale_raw.core.registry import REGISTRY, KeyKind
from backend.services.bsale_raw.core.snapshot import build_rows, fetch_snapshot
from backend.services.bsale_raw.core.store import EntityOutcome, PgRawTx
from backend.tests.bsale_raw._raw_sql_schema import parse
from backend.tests.bsale_raw.test_bsale_raw_pipeline import (
    BASE,
    NOT_YET_ENABLED,
    TOKEN,
    FakeBsale,
    FakeConnection,
    FakeStore,
    TickClock,
    client_factory_for,
    run,
)

API = "https://api.bsale.io/v1"
CATALOG = ["products", "variants"]


def product(i: int, **over) -> dict:
    item = {"href": f"{API}/products/{i}.json", "id": i, "name": f"Producto {i}", "description": "",
            "classification": 0, "ledgerAccount": "", "costCenter": "", "allowDecimal": 0, "stockControl": 1,
            "printDetailPack": 0, "state": 0, "prestashopProductId": 0, "presashopAttributeId": 0,
            "product_type": {"href": f"{API}/product_types/4.json", "id": "4"},
            "product_taxes": {"href": f"{API}/products/{i}/product_taxes.json"},
            "variants": {"href": f"{API}/products/{i}/variants.json"}}
    item.update(over)
    return item


def variant(i: int, *, product_id: int = 1, **over) -> dict:
    item = {"href": f"{API}/variants/{i}.json", "id": i, "description": f"Variante {i}", "unlimitedStock": 0,
            "allowNegativeStock": 0, "state": 0, "barCode": f"780{i:010d}", "code": f"SKU-{i}",
            "imagestionCenterCost": 0, "imagestionAccount": 0, "imagestionConceptCod": 0,
            "imagestionProyectCod": 0, "imagestionCategoryCod": 0, "imagestionProductId": 0,
            "serialNumber": 0, "prestashopCombinationId": 0, "prestashopValueId": 0,
            "product": {"href": f"{API}/products/{product_id}.json", "id": str(product_id)},
            "attribute_values": {"href": f"{API}/variants/{i}/attribute_values.json"},
            "costs": {"href": f"{API}/variants/{i}/costs.json"}}
    item.update(over)
    return item


FACTORIES = {"products": product, "variants": variant}

EXPECTED_TYPED = {
    "products": lambda p: {"state": p["state"], "name": p["name"],
                           "product_type_id": int(p["product_type"]["id"]),
                           "classification": p["classification"], "stock_control": p["stockControl"]},
    "variants": lambda v: {"state": v["state"], "product_id": int(v["product"]["id"]), "code": v["code"],
                           "bar_code": v["barCode"], "description": v["description"],
                           "unlimited_stock": v["unlimitedStock"]},
}


def setup(resource, items, **kw):
    store = FakeStore()
    adapter = FakeBsale(items, store=store, **kw)
    return store, adapter, REGISTRY.get(resource).raw_table


def go(store, adapter, resource, **kw):
    return run(store, adapter, resource=resource, **kw)


# --- registry / schema / endpoint -------------------------------------------------------------


@pytest.mark.parametrize("resource", CATALOG)
def test_resource_spec_declared_and_enabled(resource):
    spec = REGISTRY.get(resource)
    assert spec.pipeline_enabled and spec.key_kind is KeyKind.ENTITY and spec.full_reconcile
    assert spec.list_endpoint == f"/v1/{resource}.json"
    assert spec.raw_table == f"bsale_raw.{resource}"
    assert [c.column for c in spec.typed_columns] == list(EXPECTED_TYPED[resource](FACTORIES[resource](1)))


@pytest.mark.parametrize("resource", CATALOG)
def test_typed_columns_exist_in_migration(resource):
    tables, _ = parse()
    columns = tables[resource].columns
    for col in REGISTRY.get(resource).typed_columns:
        assert col.column in columns, col.column


def test_sku_and_barcode_never_unique_in_migration():
    tables, indexes = parse()
    for name in CATALOG:
        assert tables[name].pk == ("company_id", "bsale_id")
        assert all({"code", "bar_code", "name"}.isdisjoint(cols) for cols in tables[name].uniques.values())
    for ix in indexes.values():
        if ix.table in CATALOG and ix.unique:
            pytest.fail(f"índice único inesperado {ix.name}")


@pytest.mark.parametrize("resource", CATALOG)
def test_multi_page_single_sweep_without_state_or_expand_and_no_n_plus_one(resource):
    items = [FACTORIES[resource](i) for i in range(1, 121)]
    store, adapter, table = setup(resource, items)
    out = go(store, adapter, resource)
    assert out.status == "SUCCESS", out.error
    assert {urlsplit(c.url).path for c in adapter.calls} == {f"/v1/{resource}.json"}
    for call in adapter.calls:
        assert set(parse_qs(urlsplit(call.url).query)) == {"limit", "offset"}
    assert out.pages == 3 and out.requests == 3 and out.api_count == 120 and out.rows_received == 120
    assert len(store.rows(3, table)) == 120
    assert not any(seg in c.url for c in adapter.calls
                   for seg in ("product_taxes", "costs", "attribute_values", "/stocks", "price_lists"))
    assert not adapter.tx_violations


# --- columnas / payload / state ---------------------------------------------------------------


@pytest.mark.parametrize("resource", CATALOG)
def test_typed_columns_payload_and_active_inactive_preserved(resource):
    make = FACTORIES[resource]
    items = [make(1, state=0), make(2, state=1), make(3, state=0), make(4, state=1)]
    store, adapter, table = setup(resource, items)
    out = go(store, adapter, resource)
    assert out.status == "SUCCESS", out.error
    assert (out.api_count, out.rows_inserted, out.rows_missing) == (4, 4, 0)
    rows = store.rows(3, table)
    assert sorted(r["state"] for r in rows.values()) == [0, 0, 1, 1]
    for item in items:
        row = rows[item["id"]]
        assert row["payload"] == item
        for column, value in EXPECTED_TYPED[resource](item).items():
            assert row[column] == value, column


def test_variant_product_id_sku_and_barcode_mapping():
    store, adapter, table = setup("variants", [variant(31300, product_id=777, code="ABC-1", barCode="7801234")])
    out = go(store, adapter, "variants")
    assert out.status == "SUCCESS", out.error
    row = store.rows(3, table)[31300]
    assert (row["product_id"], row["code"], row["bar_code"]) == (777, "ABC-1", "7801234")


def test_product_without_type_is_null():
    item = product(1)
    del item["product_type"]
    store, adapter, table = setup("products", [item])
    out = go(store, adapter, "products")
    assert out.status == "SUCCESS" and store.rows(3, table)[1]["product_type_id"] is None


# --- SKU / barcode / relación producto: nunca se deduplica ni se repara ------------------------


def test_duplicate_barcode_allowed():
    items = [variant(1, barCode="7800000000001"), variant(2, barCode="7800000000001", product_id=2)]
    store, adapter, table = setup("variants", items)
    out = go(store, adapter, "variants")
    assert out.status == "SUCCESS" and out.rows_inserted == 2
    assert {r["bar_code"] for r in store.rows(3, table).values()} == {"7800000000001"}


def test_duplicate_sku_allowed():
    items = [variant(1, code="SKU-X"), variant(2, code="SKU-X"), variant(3, code="SKU-X", product_id=9)]
    store, adapter, table = setup("variants", items)
    out = go(store, adapter, "variants")
    assert out.status == "SUCCESS" and out.rows_inserted == 3
    assert [store.rows(3, table)[i]["code"] for i in (1, 2, 3)] == ["SKU-X"] * 3


def test_same_barcode_in_other_company_allowed_and_untouched():
    store, adapter, table = setup("variants", [variant(1, barCode="7800000000009")])
    store.seed(1, variant(1, barCode="7800000000009"), fetched_at=BASE - timedelta(days=1), table=table)
    before = copy.deepcopy(store.rows(1, table))
    out = go(store, adapter, "variants", company_id=3)
    assert out.status == "SUCCESS" and out.rows_inserted == 1
    assert store.rows(1, table) == before
    assert store.rows(3, table)[1]["bar_code"] == "7800000000009"


@pytest.mark.parametrize("value", ["", None])
def test_empty_or_null_sku_and_barcode_kept_raw(value):
    store, adapter, table = setup("variants", [variant(1, code=value, barCode=value)])
    out = go(store, adapter, "variants")
    assert out.status == "SUCCESS"
    row = store.rows(3, table)[1]
    assert row["code"] == value and row["bar_code"] == value


def test_variant_relation_stored_as_delivered():
    """Variante cuyo producto no está en RAW, o sin nodo product: se guarda tal cual, sin resolver."""
    orphan = variant(1, product_id=999999)
    no_product = variant(2)
    del no_product["product"]
    store, adapter, table = setup("variants", [orphan, no_product])
    out = go(store, adapter, "variants")
    assert out.status == "SUCCESS", out.error
    rows = store.rows(3, table)
    assert rows[1]["product_id"] == 999999 and rows[2]["product_id"] is None
    assert store.rows(3, "bsale_raw.products") == {}
    assert {urlsplit(c.url).path for c in adapter.calls} == {"/v1/variants.json"}


# --- garantías del motor ----------------------------------------------------------------------


@pytest.mark.parametrize("resource", CATALOG)
def test_idempotency(resource):
    items = [FACTORIES[resource](i) for i in range(1, 6)]
    store, adapter, table = setup(resource, items)
    clock = TickClock(BASE)
    out1 = go(store, adapter, resource, clock=clock)
    assert (out1.rows_inserted, out1.rows_updated, out1.rows_unchanged) == (5, 0, 0)
    snap1 = copy.deepcopy(store.rows(3, table))
    out2 = go(store, adapter, resource, clock=clock)
    assert (out2.rows_inserted, out2.rows_updated, out2.rows_unchanged) == (0, 0, 5)
    for bid, r2 in store.rows(3, table).items():
        assert r2["first_seen_at"] == snap1[bid]["first_seen_at"]
        assert r2["last_changed_at"] == snap1[bid]["last_changed_at"]
        assert r2["last_seen_at"] > snap1[bid]["last_seen_at"]


@pytest.mark.parametrize("resource", CATALOG)
def test_changed_payload_updates_row(resource):
    make = FACTORIES[resource]
    store, adapter, table = setup(resource, [make(1), make(2)])
    clock = TickClock(BASE)
    go(store, adapter, resource, clock=clock)
    first = copy.deepcopy(store.rows(3, table)[1])
    adapter.items = [make(1, state=1, description="cambiado"), make(2)]
    out = go(store, adapter, resource, clock=clock)
    assert (out.rows_updated, out.rows_unchanged) == (1, 1)
    row = store.rows(3, table)[1]
    assert row["state"] == 1 and row["payload"]["description"] == "cambiado"
    assert row["last_changed_at"] > first["last_changed_at"] and row["first_seen_at"] == first["first_seen_at"]


@pytest.mark.parametrize("resource", CATALOG)
def test_stale_upsert_protection(resource):
    make = FACTORIES[resource]
    store, adapter, table = setup(resource, [make(1, description="snapshot viejo")])
    store.seed(3, make(1, description="más nuevo"), fetched_at=BASE + timedelta(hours=3), table=table)
    out = go(store, adapter, resource)
    assert out.status == "SUCCESS" and out.rows_skipped_newer == 1 and out.rows_updated == 0
    assert store.rows(3, table)[1]["payload"]["description"] == "más nuevo"


@pytest.mark.parametrize("resource", CATALOG)
def test_missing_since_and_reappearance_without_delete(resource):
    make = FACTORIES[resource]
    store, adapter, table = setup(resource, [make(i) for i in range(1, 10)])
    for i in range(1, 11):
        store.seed(3, make(i), fetched_at=BASE - timedelta(days=1), table=table)
    clock = TickClock(BASE)
    out = go(store, adapter, resource, clock=clock)
    assert out.status == "SUCCESS" and out.rows_missing == 1 and out.rows_deleted == 0
    assert len(store.rows(3, table)) == 10 and store.rows(3, table)[10]["missing_since"] is not None

    adapter.items = [make(i) for i in range(1, 11)]
    out = go(store, adapter, resource, clock=clock)
    assert out.status == "SUCCESS" and store.rows(3, table)[10]["missing_since"] is None


@pytest.mark.parametrize("resource", CATALOG)
def test_fuse_over_20_pct_blocks_writes(resource):
    make = FACTORIES[resource]
    store, adapter, table = setup(resource, [make(i, description="cambio") for i in range(1, 8)])
    for i in range(1, 11):
        store.seed(3, make(i), fetched_at=BASE - timedelta(days=1), table=table)
    before = copy.deepcopy(store.rows(3, table))
    out = go(store, adapter, resource)
    assert out.status == "FAILED" and out.fuse["tripped"] and out.fuse["threshold_pct"] == 20.0
    assert store.rows(3, table) == before


@pytest.mark.parametrize("resource", CATALOG)
def test_company_isolation(resource):
    make = FACTORIES[resource]
    store, adapter, table = setup(resource, [make(1)])
    for i in range(1, 6):
        store.seed(1, make(i), fetched_at=BASE - timedelta(days=1), table=table)
    before = copy.deepcopy(store.rows(1, table))
    out = go(store, adapter, resource, company_id=3)
    assert out.status == "SUCCESS" and out.rows_missing == 0
    assert store.rows(1, table) == before
    assert all(rows == {} for name, rows in store.tables.items() if name != table)


# --- snapshots inválidos: falla antes de escribir --------------------------------------------


@pytest.mark.parametrize("resource", CATALOG)
def test_malformed_item_fails_without_writes(resource):
    make = FACTORIES[resource]
    store, adapter, table = setup(resource, [make(1), {**make(2), "id": "abc"}])
    out = go(store, adapter, resource)
    assert out.status == "FAILED" and store.rows(3, table) == {}


@pytest.mark.parametrize("resource", CATALOG)
def test_duplicate_bsale_id_fails_without_writes(resource):
    make = FACTORIES[resource]
    page1 = [make(i) for i in range(1, 51)]
    store, adapter, table = setup(resource, None, pages=[page1, [make(50), make(51)]], count=52)
    out = go(store, adapter, resource)
    assert out.status == "FAILED" and "duplicados" in out.error and store.rows(3, table) == {}


@pytest.mark.parametrize(
    "resource, bad",
    [
        ("products", {"state": "x"}),
        ("products", {"product_type": "4"}),
        ("products", {"classification": "uno"}),
        ("products", {"stockControl": [1]}),
        ("variants", {"product": "1"}),
        ("variants", {"product": {"id": "abc"}}),
        ("variants", {"unlimitedStock": "si"}),
        ("variants", {"state": 1.5}),
    ],
)
def test_invalid_typed_value_fails_without_writes(resource, bad):
    make = FACTORIES[resource]
    store, adapter, table = setup(resource, [make(1), make(2, **bad)])
    out = go(store, adapter, resource)
    assert out.status == "FAILED" and store.rows(3, table) == {}
    assert "lock_existing" not in store.events


# --- dry-run / token / CLI --------------------------------------------------------------------


@pytest.mark.parametrize("resource", CATALOG)
def test_dry_run_writes_nothing(resource):
    make = FACTORIES[resource]
    store, adapter, table = setup(resource, [make(i) for i in range(1, 8)])
    for i in (2, 3):
        store.seed(3, make(i), fetched_at=BASE - timedelta(days=1), table=table)
    before = copy.deepcopy((store.tables, store.runs, store.entity_runs, store.sync_state))
    out = go(store, adapter, resource, dry_run=True)
    assert out.dry_run and out.status == "SUCCESS" and out.sync_run_id is None
    assert (out.rows_inserted, out.rows_unchanged) == (5, 2)
    assert (store.tables, store.runs, store.entity_runs, store.sync_state) == before
    for forbidden in ("lock", "start_run", "tx_begin"):
        assert forbidden not in store.events


@pytest.mark.parametrize("resource", CATALOG)
def test_token_never_logged(resource, caplog):
    caplog.set_level(logging.DEBUG)
    store, adapter, _ = setup(resource, [FACTORIES[resource](1)],
                              script=[(401, f'{{"error": "bad {TOKEN}"}}'.encode(), {})])
    out = go(store, adapter, resource)
    assert out.status == "FAILED"
    assert TOKEN not in out.error and TOKEN not in caplog.text
    assert TOKEN not in repr(store.runs) + repr(store.entity_runs) + repr(store.sync_state)
    assert TOKEN not in cli.format_outcome(out)


@pytest.mark.parametrize("resource", CATALOG)
@pytest.mark.parametrize("dry_run", [True, False])
def test_cli_accepts_resource(resource, dry_run):
    seen = {}

    def runner(**kw):
        seen.update(kw)
        return EntityOutcome(company_id=3, resource=resource, scope="global", mode="FULL_RECONCILE",
                             status="SUCCESS", dry_run=dry_run)

    argv = ["sync", "--company", "3", "--resource", resource, "--mode", "full-reconcile"]
    buf = io.StringIO()
    assert cli.main(argv + (["--dry-run"] if dry_run else []), runner=runner, out=buf) == cli.EXIT_SUCCESS
    assert seen["resource"] == resource and seen["dry_run"] is dry_run


@pytest.mark.parametrize("name", ["variant_prices", "variant_costs", "documents", "clients"])
def test_cli_still_rejects_out_of_scope(name):
    assert name in NOT_YET_ENABLED
    never = lambda **kw: pytest.fail("no debe ejecutarse")  # noqa: E731
    for extra in ([], ["--dry-run"]):
        argv = ["sync", "--company", "3", "--resource", name, "--mode", "full-reconcile", *extra]
        assert cli.main(argv, runner=never, out=io.StringIO()) == cli.EXIT_USAGE


# --- PostgreSQL: operaciones por lote, nunca por fila ----------------------------------------


def test_pg_batches_statements_never_per_row():
    spec = REGISTRY.get("variants")
    adapter = FakeBsale([variant(i, barCode="780DUP", code="SKU-DUP") for i in range(1, 1201)])
    snap = fetch_snapshot(client_factory_for(adapter)(None, TOKEN, spec), spec.list_endpoint)
    rows = build_rows(spec, 3, snap)
    assert len(adapter.calls) == 24
    pages = [[(i,) for i in range(s, min(s + 500, 1201))] for s in (1, 501, 1001)]
    conn = FakeConnection(next_results=[[], *pages])
    tx = PgRawTx(conn.cursor())
    tx.lock_existing(spec, 3)
    applied = tx.upsert(spec, rows, sync_run_id=1, last_source="FULL_RECONCILE")
    tx.mark_missing(spec, 3, [5000, 5001], BASE)
    assert len(applied) == 1200
    statements = [s for s, _ in conn.executed]
    assert sum("FOR UPDATE" in s for s in statements) == 1
    assert sum(s.startswith("INSERT INTO bsale_raw.variants") for s in statements) == 3
    assert sum(s.startswith("UPDATE bsale_raw.variants") for s in statements) == 1
    assert len(statements) == 5
    assert not any("DELETE" in s.upper() for s in statements)
