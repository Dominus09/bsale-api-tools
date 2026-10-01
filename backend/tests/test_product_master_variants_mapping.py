"""product_master_variants: AUTO_EXACT / MISSING / AMBIGUOUS / MANUAL y aislamiento por empresa."""

from __future__ import annotations

from backend.services import order_weight_service as ows
from backend.services.bsale import product_master_mapping as m
from backend.services.bsale.catalog_sync_service import (
    _PM_UNITS_SYNCABLE_COUNT_SQL,
    _REFRESH_PRODUCTS_MASTER_SQL,
    _SYNC_PM_UNITS_SQL,
)
from backend.tests.bsale_sync_fakes import FakeConn

V = m.VariantRef


def _existing(pm, cid, status, variant_id=None, product_id=None, match_count=None, cands=(), barcode="780", mapping_source="BARCODE"):
    return m.ExistingMapping(pm, cid, status, variant_id, product_id, match_count, cands, barcode, mapping_source)


def _by_pair(decisions):
    return {(d.product_master_id, d.company_id): d for d in decisions}


def test_auto_exact_single_variant():
    decisions, _ = m.compute_mappings([(1, "780")], [V(3, 500, 10, " 780 ")], [])
    d = _by_pair(decisions)[(1, 3)]
    assert (d.mapping_status, d.variant_id, d.product_id, d.match_count) == ("AUTO_EXACT", 500, 10, 1)
    assert d.candidate_variant_ids == ()
    assert d.mapping_source == "BARCODE"


def test_missing_when_variant_disappears():
    existing = [_existing(1, 3, "AUTO_EXACT", 500, 10, 1)]
    decisions, _ = m.compute_mappings([(1, "780")], [], existing)
    d = _by_pair(decisions)[(1, 3)]
    assert (d.mapping_status, d.variant_id, d.product_id, d.match_count) == ("MISSING", None, None, 0)
    assert d.changed and d.previous_variant_id == 500


def test_missing_returns_to_auto_exact():
    existing = [_existing(1, 2, "MISSING", match_count=0)]
    decisions, _ = m.compute_mappings([(1, "780")], [V(2, 77, 9, "780")], existing)
    assert _by_pair(decisions)[(1, 2)].mapping_status == "AUTO_EXACT"


def test_ambiguous_never_picks_min_or_max():
    variants = [V(1, 30, 5, "780"), V(1, 10, 4, "780")]
    decisions, _ = m.compute_mappings([(1, "780")], variants, [])
    d = _by_pair(decisions)[(1, 1)]
    assert d.mapping_status == "AMBIGUOUS"
    assert d.variant_id is None and d.product_id is None
    assert d.candidate_variant_ids == (10, 30)
    assert d.match_count == 2


def test_manual_never_overwritten():
    existing = [_existing(1, 3, "MANUAL", 999, 88, 1)]
    decisions, skipped = m.compute_mappings([(1, "780")], [V(3, 500, 10, "780")], existing)
    assert skipped == 1
    assert (1, 3) not in _by_pair(decisions)


def test_variant_claimed_by_manual_elsewhere_becomes_ambiguous():
    existing = [_existing(2, 3, "MANUAL", 500, 10, 1, barcode="OTHER")]
    decisions, _ = m.compute_mappings([(1, "780"), (2, "OTHER")], [V(3, 500, 10, "780")], existing)
    d = _by_pair(decisions)[(1, 3)]
    assert d.mapping_status == "AMBIGUOUS" and d.variant_id is None


def test_same_variant_id_in_two_companies_never_crosses():
    variants = [V(1, 500, 10, "AAA"), V(3, 500, 77, "BBB")]
    decisions, _ = m.compute_mappings([(1, "AAA"), (2, "BBB")], variants, [])
    pairs = _by_pair(decisions)
    assert set(pairs) == {(1, 1), (2, 3)}
    assert pairs[(1, 1)].product_id == 10
    assert pairs[(2, 3)].product_id == 77


def test_pairs_come_from_existing_and_current_variants_not_companies_column():
    existing = [_existing(1, 2, "AUTO_EXACT", 40, 4, 1)]
    decisions, _ = m.compute_mappings([(1, "780")], [V(3, 500, 10, "780")], existing)
    assert set(_by_pair(decisions)) == {(1, 2), (1, 3)}
    assert _by_pair(decisions)[(1, 2)].mapping_status == "MISSING"


def test_unchanged_row_is_not_marked_changed():
    existing = [_existing(1, 3, "AUTO_EXACT", 500, 10, 1)]
    decisions, _ = m.compute_mappings([(1, "780")], [V(3, 500, 10, "780")], existing)
    assert _by_pair(decisions)[(1, 3)].changed is False


def _capture_persist(monkeypatch):
    captured: dict = {}
    monkeypatch.setattr(m, "execute_batch", lambda cur, sql, rows, page_size=None: captured.setdefault("release", (sql, rows)))
    monkeypatch.setattr(m, "execute_values", lambda cur, sql, rows, template=None, page_size=None: captured.setdefault("upsert", (sql, rows, template)))
    return captured


def test_persist_sql_protects_manual_and_releases_stale_claims(monkeypatch):
    captured = _capture_persist(monkeypatch)
    existing = [_existing(1, 3, "AUTO_EXACT", 500, 10, 1)]
    decisions, _ = m.compute_mappings([(1, "780")], [], existing)
    m.persist_mappings(object(), decisions)
    release_sql, release_rows = captured["release"]
    release_compact = " ".join(release_sql.split())
    assert release_rows == [(1, 3)]
    assert "mapping_status = 'MISSING'" in release_compact
    assert "product_id = NULL" in release_compact and "variant_id = NULL" in release_compact
    assert "IS DISTINCT FROM 'MANUAL'" in release_compact
    upsert_sql = " ".join(captured["upsert"][0].split())
    assert "ON CONFLICT (product_master_id, company_id)" in upsert_sql
    assert "WHERE pmv.mapping_status IS DISTINCT FROM 'MANUAL'" in upsert_sql


def test_upsert_always_includes_mapping_source_and_bigint_array(monkeypatch):
    captured = _capture_persist(monkeypatch)
    variants = [V(3, 500, 10, "780"), V(1, 30, 5, "BBB"), V(1, 31, 5, "BBB")]
    existing = [_existing(9, 2, "AUTO_EXACT", 40, 4, 1, barcode="CCC", mapping_source="LEGACY_COMPANIES")]
    decisions, _ = m.compute_mappings([(1, "780"), (2, "BBB"), (9, "CCC")], variants, existing)
    m.persist_mappings(object(), decisions)
    sql, rows, template = captured["upsert"]
    compact = " ".join(sql.split())
    assert "mapping_source" in compact.split("VALUES")[0]
    assert "mapping_source = EXCLUDED.mapping_source" in compact
    assert "%s::bigint[]" in template
    by_status = {r[3]: r for r in rows}
    for r in rows:
        assert r[4] in ("BARCODE", "LEGACY_COMPANIES")
        assert isinstance(r[8], list)
    assert by_status["AUTO_EXACT"][4] == "BARCODE"
    assert by_status["AMBIGUOUS"][4] == "BARCODE"
    assert by_status["MISSING"][4] == "LEGACY_COMPANIES"
    assert by_status["AUTO_EXACT"][5:7] == (10, 500)
    assert by_status["AUTO_EXACT"][8] == []
    assert by_status["AMBIGUOUS"][6] is None and by_status["AMBIGUOUS"][8] == [30, 31]
    assert by_status["MISSING"][6] is None and by_status["MISSING"][8] == []


def test_source_auto_exact_from_current_variant_is_barcode():
    existing = [_existing(1, 3, "MISSING", match_count=0, mapping_source="LEGACY_COMPANIES")]
    decisions, _ = m.compute_mappings([(1, "780")], [V(3, 500, 10, "780")], existing)
    d = _by_pair(decisions)[(1, 3)]
    assert (d.mapping_status, d.mapping_source, d.changed) == ("AUTO_EXACT", "BARCODE", True)


def test_source_ambiguous_from_current_variants_is_barcode():
    existing = [_existing(1, 3, "MISSING", match_count=0, mapping_source="LEGACY_COMPANIES")]
    variants = [V(3, 500, 10, "780"), V(3, 501, 11, "780")]
    decisions, _ = m.compute_mappings([(1, "780")], variants, existing)
    d = _by_pair(decisions)[(1, 3)]
    assert (d.mapping_status, d.mapping_source) == ("AMBIGUOUS", "BARCODE")


def test_source_legacy_missing_stays_legacy_and_unchanged():
    existing = [_existing(1, 2, "MISSING", match_count=0, mapping_source="LEGACY_COMPANIES")]
    decisions, _ = m.compute_mappings([(1, "780")], [], existing)
    d = _by_pair(decisions)[(1, 2)]
    assert (d.mapping_status, d.mapping_source) == ("MISSING", "LEGACY_COMPANIES")
    assert d.changed is False


def test_source_barcode_auto_exact_becoming_missing_keeps_barcode():
    existing = [_existing(1, 3, "AUTO_EXACT", 500, 10, 1, mapping_source="BARCODE")]
    decisions, _ = m.compute_mappings([(1, "780")], [], existing)
    d = _by_pair(decisions)[(1, 3)]
    assert (d.mapping_status, d.mapping_source, d.changed) == ("MISSING", "BARCODE", True)


def test_source_manual_row_untouched():
    existing = [_existing(1, 3, "MANUAL", 999, 88, 1, mapping_source="MANUAL")]
    decisions, skipped = m.compute_mappings([(1, "780")], [], existing)
    assert skipped == 1 and decisions == []


def test_single_variant_without_product_id_is_not_auto_exact():
    decisions, _ = m.compute_mappings([(1, "780")], [V(3, 500, None, "780")], [])
    d = _by_pair(decisions)[(1, 3)]
    assert d.mapping_status == "AMBIGUOUS"
    assert d.variant_id is None and d.candidate_variant_ids == (500,)


def test_decision_to_row_enforces_check_constraint():
    base = dict(product_master_id=1, company_id=3, barcode="780", mapping_source="BARCODE",
                match_count=1, candidate_variant_ids=(), changed=True, previous_variant_id=None)
    import pytest

    with pytest.raises(ValueError):
        m.decision_to_row(m.MappingDecision(mapping_status="AUTO_EXACT", product_id=None, variant_id=500, **base))
    with pytest.raises(ValueError):
        m.decision_to_row(m.MappingDecision(mapping_status="MISSING", product_id=None, variant_id=500, **base))
    with pytest.raises(ValueError):
        m.decision_to_row(m.MappingDecision(mapping_status="MANUAL", product_id=10, variant_id=500, **base))


def test_manual_never_written_even_via_pair_path(monkeypatch):
    def handler(sql, params):
        if "FROM bsale.variants" in sql:
            return [(3, 500, 10, "780")]
        if "FROM bsale.product_master_variants" in sql:
            return [(1, 3, "MANUAL", 999, 88, 1, [], "780", "MANUAL")]
        return None

    captured = _capture_persist(monkeypatch)
    conn = FakeConn(handler)
    out = m.upsert_mapping_for_pair(conn.cursor(), product_master_id=1, company_id=3, barcode="780")
    assert out["mapping_status"] == "MANUAL" and out["variant_id"] == 999
    assert captured == {}


def test_products_master_sql_has_no_global_variant_identity():
    for sql in (_SYNC_PM_UNITS_SQL, _PM_UNITS_SYNCABLE_COUNT_SQL, _REFRESH_PRODUCTS_MASTER_SQL):
        compact = " ".join(sql.split())
        assert "pm.variant_id = v.bsale_id" not in compact
        assert "variant_prices" not in compact
    refresh = " ".join(_REFRESH_PRODUCTS_MASTER_SQL.split())
    assert "array_agg(DISTINCT v.company_id" in refresh
    for col in ("sku", "product_type", "companies", "last_bsale_sync_at"):
        assert f"{col} = " in refresh
    on_conflict = refresh.split("ON CONFLICT")[1]
    for manual in ("supplier_id", "weight_box_kg", "height_cm", "logistics_completed", "sale_type", "quantity_step", "is_active"):
        assert manual not in on_conflict


def test_create_logistics_from_variant_preserves_legacy_identity(monkeypatch):
    def handler(sql, params):
        if sql.startswith("SELECT v.bar_code"):
            return [(" 780 ", "SKU1", 10, 500, "Pisco", "750cc", "Licores", 12)]
        if sql.startswith("INSERT INTO bsale.products_master"):
            return []
        if sql.startswith("SELECT id FROM bsale.products_master"):
            return [(55,)]
        if sql.startswith("SELECT id, barcode, variant_id"):
            return {
                "rows": [(55, "780", 9001, "Pisco canon", "750cc")],
                "description": [("id",), ("barcode",), ("variant_id",), ("product_name",), ("variant_name",)],
            }
        return None

    conn = FakeConn(handler)
    monkeypatch.setattr(ows, "get_connection", lambda: conn)
    calls = []

    def fake_mapping(cur, **kw):
        calls.append(kw)
        return {"mapping_status": "AUTO_EXACT", "variant_id": 500}

    monkeypatch.setattr(ows, "upsert_mapping_for_pair", fake_mapping)
    out = ows.create_logistics_from_variant(variant_id=500, company_id=1)

    assert out["variant_id"] == 9001
    assert out["created"] is False
    assert out["mapping_status"] == "AUTO_EXACT" and out["mapping_variant_id"] == 500
    assert calls == [{"product_master_id": 55, "company_id": 1, "barcode": "780"}]
    insert_sql = conn.sql_matching("INSERT INTO bsale.products_master")[0][0]
    assert "ON CONFLICT (barcode) DO NOTHING" in insert_sql
    for sql, _ in conn.sql_matching("UPDATE bsale.products_master"):
        assert "variant_id" not in sql and "product_id" not in sql
        assert "weight_box_kg" not in sql and "supplier_id" not in sql
    variant_lookup = conn.sql_matching("SELECT v.bar_code")[0]
    assert "v.company_id = %s AND v.bsale_id = %s" in variant_lookup[0]
    assert variant_lookup[1] == (1, 500)
    assert conn.commits == 1


def test_create_logistics_from_variant_without_barcode_fails(monkeypatch):
    conn = FakeConn(lambda sql, p: [("  ", None, 10, 500, "x", "y", None, None)] if sql.startswith("SELECT v.bar_code") else None)
    monkeypatch.setattr(ows, "get_connection", lambda: conn)
    try:
        ows.create_logistics_from_variant(variant_id=500, company_id=3)
    except ValueError as exc:
        assert "sin barcode" in str(exc)
    else:
        raise AssertionError("debía fallar")
    assert conn.commits == 0 and conn.rollbacks == 1
