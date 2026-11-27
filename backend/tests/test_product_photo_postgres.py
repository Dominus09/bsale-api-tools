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
from backend.services import product_master_admin_service as admin

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
                    updated_at timestamptz NOT NULL DEFAULT now()
                );
                INSERT INTO bsale.products_master
                  (id, barcode, variant_id, product_name, variant_name, sku, units_per_box, weight_box_kg)
                VALUES (8,'00123',111,'Demo product','24 units','QA-00123',24,12.5),
                       (9,'00456',222,'Duplicate demo','6 units','QA-00456',6,3);
                CREATE TABLE bsale.variants (
                    company_id integer, bsale_id bigint, bar_code text,
                    PRIMARY KEY (company_id, bsale_id)
                );
                INSERT INTO bsale.variants VALUES
                    (1,111,'00123'),(3,500,'00123'),
                    (3,501,'00456'),(3,502,'00456');
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
    assert admin.pallet_data([8])["schema_ready"]


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
