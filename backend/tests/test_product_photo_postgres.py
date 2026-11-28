"""Real, disposable PostgreSQL QA. Never use production PG_* variables.

Run only with LQ_PRODUCT_MASTER_PG_QA=1 and the dedicated local service
lq_product_master_qa on port 55432 (GitHub Actions).
"""
from contextlib import closing
from pathlib import Path
import os

import psycopg2
import pytest
from fastapi import HTTPException

from backend.routers import product_photos as routes
from backend.routers import products_master as pm
from backend.services.catalog_photo_inventory import PhotoInventory
from backend.services import product_master_admin_service as admin
from backend.services import system_feature_controls as controls

pytestmark = pytest.mark.skipif(
    os.getenv("LQ_PRODUCT_MASTER_PG_QA") != "1",
    reason="Requires dedicated disposable PostgreSQL CI service",
)


@pytest.fixture(scope="module")
def database():
    def connect():
        return psycopg2.connect(
            host="127.0.0.1", port=55432, dbname="lq_product_master_qa",
            user="lq_qa", password="qa_only_not_production", connect_timeout=5,
        )

    with closing(connect()) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SELECT current_database(), current_user")
            assert cur.fetchone() == ("lq_product_master_qa", "lq_qa")
            cur.execute("SELECT to_regnamespace('bsale'), to_regnamespace('app')")
            assert cur.fetchone() == (None, None), "QA database must start empty"
            cur.execute("""
                CREATE SCHEMA bsale;
                CREATE TABLE bsale.products_master (
                    id integer PRIMARY KEY, barcode text NOT NULL UNIQUE,
                    variant_id bigint, product_name text, variant_name text, sku text,
                    is_active boolean NOT NULL DEFAULT true,
                    units_per_box integer, weight_box_kg numeric(12,4),
                    product_id bigint, product_type text, companies jsonb, supplier_id integer,
                    sale_type text, quantity_step integer, height_cm numeric, width_cm numeric, length_cm numeric,
                    logistics_completed boolean DEFAULT false, last_bsale_sync_at timestamptz, created_at timestamptz,
                    updated_at timestamptz NOT NULL DEFAULT now()
                );
                INSERT INTO bsale.products_master
                  (id, barcode, variant_id, product_name, variant_name, sku, units_per_box, weight_box_kg)
                VALUES (8,'00123',111,'Demo product','24 units','QA-00123',24,12.5),
                       (9,'00456',222,'Duplicate demo','6 units','QA-00456',6,3);
                UPDATE bsale.products_master SET product_type=' CERVEZAS ' WHERE id=8;
                UPDATE bsale.products_master SET product_type='BEBIDAS' WHERE id=9;
                CREATE TABLE bsale.companies (company_id bigint PRIMARY KEY, active boolean NOT NULL);
                INSERT INTO bsale.companies VALUES (1,true),(2,true),(3,true),(4,false);
                CREATE TABLE bsale.stocks (
                    company_id bigint, variant_id bigint, office_id bigint,
                    quantity_available numeric, quantity_reserved numeric,
                    updated_at timestamptz DEFAULT now(),
                    PRIMARY KEY (company_id, variant_id, office_id)
                );
                CREATE TABLE bsale.variants (
                    company_id integer, bsale_id bigint, bar_code text,
                    PRIMARY KEY (company_id, bsale_id)
                );
                INSERT INTO bsale.variants VALUES
                    (1,111,'00123'),(2,111,'00123'),(3,500,'00123'),
                    (3,501,'00456'),(3,502,'00456'),(4,111,'00123');
                INSERT INTO bsale.stocks (company_id,variant_id,office_id,quantity_available,quantity_reserved) VALUES
                    (1,111,1,100,20),(1,111,2,25,5),(2,111,1,40,10),(3,500,1,-5,0),
                    (3,501,1,0,0),(3,502,1,-2,0),(4,111,1,999,0),
                    (2,999,1,777,0);
                CREATE TABLE bsale.product_master_variants (
                    product_master_id integer, company_id integer, mapping_status text,
                    variant_id bigint, barcode text,
                    PRIMARY KEY (product_master_id, company_id)
                );
                INSERT INTO bsale.product_master_variants VALUES
                    (8,3,'AUTO_EXACT',500,'00123'),
                    (9,3,'AMBIGUOUS',NULL,'00456');
                CREATE TABLE bsale.catalog_fixture
                    (variant_id bigint, bar_code text, image_url text);
                INSERT INTO bsale.catalog_fixture VALUES
                    (500,'00123','https://cat.quillotana.cl/products/00123.webp'),
                    (501,'00456',NULL),(502,'00456',NULL);
                CREATE VIEW bsale.catalog_view AS SELECT * FROM bsale.catalog_fixture;
            """)
            sql = (Path(__file__).resolve().parents[1] / "sql" /
                   "070_product_logistics_and_photo_audit_PROPOSED.sql").read_text()
            cur.execute(sql)
            cur.execute(sql)
    return connect


@pytest.fixture
def services(database, monkeypatch):
    monkeypatch.setattr(admin, "get_connection", database)
    monkeypatch.setattr(controls, "get_connection", database)
    monkeypatch.setattr(routes, "get_connection", database)
    monkeypatch.setenv("PRODUCT_MASTER_PALLET_WRITES_ENABLED", "1")
    monkeypatch.setenv("CATALOG_SOURCE_PUBLIC", "legacy")
    return database


def test_migration_twice_preserves_existing_columns_and_checks_positive(services):
    with closing(services()) as conn, conn.cursor() as cur:
        cur.execute("SELECT barcode, units_per_box, weight_box_kg, boxes_per_pallet FROM bsale.products_master WHERE id=8")
        assert cur.fetchone() == ("00123", 24, 12.5, None)
        cur.execute("SELECT COUNT(*) FROM bsale.products_master")
        assert cur.fetchone()[0] == 2
        with pytest.raises(psycopg2.errors.CheckViolation):
            cur.execute("UPDATE bsale.products_master SET boxes_per_pallet=0 WHERE id=8")
        conn.rollback()
    assert admin.photo_audit_ready()
    data = admin.pallet_data([8])
    assert data["schema_ready"] and data["writes_enabled"]
    assert data["items"] == [{"id": 8, "boxes_per_pallet": None, "units_per_pallet": None, "load_weight_kg": None}]


def test_pallet_edit_audit_and_clear_preserve_units_and_weights(services):
    result = admin.set_pallet_boxes(8, 50, 42)
    assert result["units_per_pallet"] == 1200
    assert result["load_weight_kg"] == 625
    assert admin.pallet_stats()["with_boxes_per_pallet"] == 1
    admin.set_pallet_boxes(8, 50, 42)  # same value: no second audit
    admin.set_pallet_boxes(8, None, 42)
    assert admin.pallet_data([8])["items"][0]["boxes_per_pallet"] is None
    with closing(services()) as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT actor_user_id, before_boxes, after_boxes
            FROM app.product_master_pallet_audit WHERE products_master_id=8 ORDER BY id
        """)
        assert cur.fetchall() == [(42, None, 50), (42, 50, None)]
        cur.execute("SELECT units_per_box, weight_box_kg FROM bsale.products_master WHERE id=8")
        assert cur.fetchone() == (24, 12.5)


def test_duplicate_barcode_rejected_before_pallet_update(services):
    with pytest.raises(HTTPException) as exc:
        admin.set_pallet_boxes(9, 50, 42)
    assert exc.value.status_code == 409
    assert admin.pallet_data([9])["items"][0]["boxes_per_pallet"] is None
    assert routes._product(9)["blocked_reason"]


def test_canonical_variant_is_company_scoped_and_v2_blocks_publication(services, monkeypatch):
    product = routes._product(8)
    assert product["variant_id"] == 111  # legacy representative from company 1
    assert product["catalog_variant_ids"] == [500]
    assert product["blocked_reason"] is None
    monkeypatch.setenv("CATALOG_SOURCE_PUBLIC", "v2")
    assert "V2" in routes._product(8)["blocked_reason"]


def test_photo_audit_private_and_durable_without_github(services):
    product = routes._product(8)
    audit = admin.start_photo_audit(actor=42, product=product, expected_sha=None, prepared_sha256="f"*64)
    admin.finish_photo_audit(audit, {"new_sha": "a"*40, "commit_sha": "b"*40})
    failed = admin.start_photo_audit(actor=42, product=product, expected_sha="a"*40, prepared_sha256="e"*64)
    admin.finish_photo_audit(failed, None, 503)
    with closing(services()) as conn, conn.cursor() as cur:
        cur.execute("SELECT actor_user_id, catalog_variant_id, status, commit_sha FROM app.catalog_photo_audit WHERE audit_id=%s", (audit,))
        assert cur.fetchone() == (42, 500, "published", "b"*40)
        cur.execute("SELECT status, error_status FROM app.catalog_photo_audit WHERE audit_id=%s", (failed,))
        assert cur.fetchone() == ("needs_review", 503)


def test_type_and_photo_filter_count_and_pagination_on_postgres(services, monkeypatch):
    monkeypatch.setattr(pm, "get_connection", services)
    snap = PhotoInventory(frozenset({"00123"}), "a"*40, "2026-11-27T00:00:00+00:00")
    with_photo = pm._list_master_page(None, False, "CERVEZAS", None, False, 1, 0,
                                      inventory=snap, photo_status="with")
    assert with_photo["total"] == 1
    assert with_photo["items"][0]["barcode"] == "00123"
    assert with_photo["items"][0]["has_catalog_photo"] is True
    without_photo = pm._list_master_page(None, False, None, None, False, 1, 0,
                                         inventory=snap, photo_status="without")
    assert without_photo["total"] == 1
    assert without_photo["items"][0]["barcode"] == "00456"
    assert without_photo["items"][0]["has_catalog_photo"] is False
    wrong_type = pm._list_master_page(None, False, "BEBIDAS", None, False, 1, 0,
                                      inventory=snap, photo_status="with")
    assert wrong_type["total"] == 0 and not wrong_type["items"]
    second_page = pm._list_master_page(None, False, None, None, False, 1, 1,
                                       inventory=snap)
    assert second_page["total"] == 2 and len(second_page["items"]) == 1
    assert pm.list_product_master_types({"id": 42}) == {"items": ["BEBIDAS", "CERVEZAS"]}


def test_consolidated_stock_across_companies_offices_filters_before_pagination(services, monkeypatch):
    monkeypatch.setattr(pm, "get_connection", services)
    snap = PhotoInventory(frozenset({"00123"}), "a"*40, "2026-11-27T00:00:00+00:00")
    positive = pm._list_master_page(None, False, "CERVEZAS", None, False, 1, 0,
        inventory=snap, photo_status="with", include_stock=True, stock_status="with")
    assert positive["total"] == 1
    item = positive["items"][0]
    assert item["barcode"] == "00123"
    # Same variant ID in companies 1 & 2; multiple offices; negative included;
    # reserved not subtracted twice; inactive company 4 and orphan ignored.
    assert item["stock_consolidated"] == 160
    assert item["stock_companies"] == 3
    assert item["stock_shared_barcode"] is False
    assert positive["stock_inventory"]["companies_total"] == 3
    assert positive["stock_inventory"]["companies_with_data"] == 3
    negative = pm._list_master_page(None, False, None, None, False, 1, 0,
        inventory=snap, photo_status="without", include_stock=True, stock_status="without")
    assert negative["total"] == 1
    assert negative["items"][0]["stock_consolidated"] == -2
    assert negative["items"][0]["stock_shared_barcode"] is True
    no_match = pm._list_master_page(None, False, None, None, False, 1, 0,
        inventory=snap, photo_status="without", include_stock=True, stock_status="with")
    assert no_match["total"] == 0
    second = pm._list_master_page(None, False, None, None, False, 1, 1, include_stock=True)
    assert second["total"] == 2 and len(second["items"]) == 1


def test_consolidated_scope_requires_access_to_all_active_companies(services, monkeypatch):
    monkeypatch.setattr(pm, "get_connection", services)
    with pytest.raises(HTTPException) as exc:
        pm._list_master_page(None, False, None, None, False, 50, 0,
            include_stock=True, stock_company_ids=[3], stock_status="with")
    assert exc.value.status_code == 403
    allowed = pm._list_master_page(None, False, None, None, False, 50, 0,
        include_stock=True, stock_company_ids=[1,2,3], stock_status="with")
    assert allowed["total"] == 1


def test_missing_stock_distinguished_from_zero_and_negative(services, monkeypatch):
    monkeypatch.setattr(pm, "get_connection", services)
    with closing(services()) as conn, conn.cursor() as cur:
        cur.execute("""
            INSERT INTO bsale.products_master(id, barcode, product_name)
            VALUES (10,'00999','QA sin stock sincronizado');
            INSERT INTO bsale.variants VALUES (1,9999,'00999');
            INSERT INTO bsale.stocks(company_id,variant_id,office_id,quantity_available) VALUES (1,9999,1,NULL);
        """)
        conn.commit()
    try:
        missing = pm._list_master_page(None, False, None, None, False, 50, 0,
            include_stock=True, stock_status="unknown")
        assert [r["barcode"] for r in missing["items"]] == ["00999"]
        assert missing["items"][0]["stock_consolidated"] is None
        assert missing["items"][0]["stock_partial"] is True
        without = pm._list_master_page(None, False, None, None, False, 50, 0,
            include_stock=True, stock_status="without")
        assert [r["barcode"] for r in without["items"]] == ["00456"]
    finally:
        with closing(services()) as conn, conn.cursor() as cur:
            cur.execute("DELETE FROM bsale.stocks WHERE company_id=1 AND variant_id=9999; DELETE FROM bsale.variants WHERE company_id=1 AND bsale_id=9999; DELETE FROM bsale.products_master WHERE id=10")
            conn.commit()


def test_owner_controls_migration_and_pallet_integration(services, monkeypatch):
    key = "product_master_pallet_writes"
    photo_key = "catalog_photo_writes"
    ai_key = "catalog_photo_ai"
    monkeypatch.setenv("PRODUCT_MASTER_PALLET_WRITES_ENABLED", "1")
    monkeypatch.setenv("CATALOG_PHOTO_WRITES_ENABLED", "1")
    monkeypatch.setenv("CATALOG_PHOTO_GITHUB_TOKEN", "fake-private-token-do-not-return")
    monkeypatch.setenv("CATALOG_PHOTO_PREVIEW_SECRET", "private-test-key-" * 4)
    monkeypatch.setenv("CATALOG_PHOTO_AI_ENABLED", "1")
    monkeypatch.setattr(controls, "ai_model_ready", lambda: True)
    # Before migration, existing env behavior is unchanged and the panel is read-only.
    assert controls.enabled(key) is True
    assert controls.get_state()["schema_ready"] is False
    with pytest.raises(HTTPException) as exc:
        controls.set_override(key, override=False, expected_version=0, actor=42)
    assert exc.value.status_code == 503
    sql = (Path(__file__).resolve().parents[1] / "sql" /
           "071_system_feature_controls_PROPOSED.sql").read_text()
    with closing(services()) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(sql)
            cur.execute(sql)
    try:
        initial = controls.get_state()
        assert initial["schema_ready"] is True
        assert all(item["configured_enabled"] for item in initial["items"])
        assert all(item["source"] == "environment" for item in initial["items"])
        assert "fake-private-token" not in str(initial)
        assert "private-test-key" not in str(initial)

        disabled = controls.set_override(key, override=False, expected_version=0, actor=42)
        item = next(i for i in disabled["items"] if i["key"] == key)
        assert item["row_version"] == 1 and item["configured_enabled"] is False
        assert controls.enabled(key) is False
        data = admin.pallet_data([8, 9])
        assert data["writes_enabled"] is False
        assert sorted(row["id"] for row in data["items"]) == [8, 9]
        with pytest.raises(HTTPException) as exc:
            admin.set_pallet_boxes(8, 50, 42)
        assert exc.value.status_code == 503
        with pytest.raises(HTTPException) as exc:
            controls.set_override(key, override=True, expected_version=0, actor=42)
        assert exc.value.status_code == 409
        # A database override also enables a function with ENV absent/false.
        monkeypatch.delenv("PRODUCT_MASTER_PALLET_WRITES_ENABLED", raising=False)
        enabled = controls.set_override(key, override=True, expected_version=1, actor=42)
        assert next(i for i in enabled["items"] if i["key"] == key)["effective_enabled"]
        data = admin.pallet_data([8, 9])
        assert data["writes_enabled"] is True
        assert sorted(row["id"] for row in data["items"]) == [8, 9]
        # Both settings reads and subsequent pallet writes must retain real rows.
        # This regression must not be masked by checking the flag alone.
        assert all(row["boxes_per_pallet"] is None for row in data["items"])
        no_change = controls.set_override(key, override=True, expected_version=2, actor=42)
        assert len(no_change["history"]) == 2

        from backend.services import product_photo_service as photos
        assert photos.writes_enabled() and photos.ai_enabled()
        controls.set_override(photo_key, override=False, expected_version=0, actor=42)
        controls.set_override(ai_key, override=False, expected_version=0, actor=42)
        assert not photos.writes_enabled() and not photos.ai_enabled()
        # The migration remains idempotent even after configuration changes.
        with closing(services()) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(sql)
        assert controls.enabled(key) and not controls.enabled(photo_key)
        restored = controls.set_override(key, override=None, expected_version=2, actor=42)
        item = next(i for i in restored["items"] if i["key"] == key)
        assert item["source"] == "environment" and not item["configured_enabled"]
        assert not controls.enabled(key)
        assert len(restored["history"]) == 5

        # Enabling cannot bypass hard prerequisites. Rejected change rolls back.
        monkeypatch.delenv("CATALOG_PHOTO_GITHUB_TOKEN", raising=False)
        with pytest.raises(HTTPException) as exc:
            controls.set_override(photo_key, override=True, expected_version=1, actor=42)
        assert exc.value.status_code == 409
        monkeypatch.setattr(controls, "ai_model_ready", lambda: False)
        with pytest.raises(HTTPException) as exc:
            controls.set_override(ai_key, override=True, expected_version=1, actor=42)
        assert exc.value.status_code == 409
        assert len(controls.get_state()["history"]) == 5
        assert not photos.writes_enabled() and not photos.ai_enabled()
        with closing(services()) as conn, conn.cursor() as cur:
            cur.execute("""
                SELECT before_override, after_override, actor_user_id, row_version
                FROM app.system_feature_control_audit WHERE feature_key=%s ORDER BY id
            """, (key,))
            assert cur.fetchall() == [(None, False, 42, 1), (False, True, 42, 2), (True, None, 42, 3)]
    finally:
        # Dedicated test database only. Restore fixture for unrelated tests.
        with closing(services()) as conn, conn.cursor() as cur:
            cur.execute("DROP TABLE app.system_feature_control_audit; DROP TABLE app.system_feature_controls")
            conn.commit()


def test_feature_controls_do_not_fall_back_to_enabled_env_on_db_error(services, monkeypatch):
    monkeypatch.setenv("PRODUCT_MASTER_PALLET_WRITES_ENABLED", "1")
    def unavailable():
        raise psycopg2.OperationalError("fake database unavailable")
    monkeypatch.setattr(controls, "get_connection", unavailable)
    assert controls.enabled("product_master_pallet_writes") is False
    with pytest.raises(HTTPException) as exc:
        controls.get_state()
    assert exc.value.status_code == 503
