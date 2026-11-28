"""Isolated PostgreSQL parity and benchmark of public catalog SQL.

No production credentials, no production schema changes. The original query is
kept here as the reference; every returned row, including duplicates and NULLs,
must match the optimized query for both public price lists and stock modes.
"""
from __future__ import annotations

import os
import time
from collections import Counter
from contextlib import closing

import psycopg2
import pytest

from backend.routers import catalog

pytestmark = pytest.mark.skipif(
    os.environ.get("LQ_CATALOG_PG_QA") != "1",
    reason="requires dedicated disposable PostgreSQL QA container",
)

LEGACY_QUERY = """
SELECT
    cv.variant_id AS id,
    TRIM(cv.product || ' ' || COALESCE(cv.variant, '')) AS name,
    cv.product_type AS type,
    cv.product,
    cv.variant,
    cv.bar_code AS barcode,
    COALESCE(cv.stock, 0) AS stock,
    cv.image_url AS image,
    CASE
        WHEN %s = 'factura' THEN cv.price_13
        WHEN %s = 'melinka' THEN cv.price_16
    END AS price,
    v.description AS variant_description,
    COALESCE(
        NULLIF(v.units_per_box, 0),
        NULLIF(pm.units_per_box, 0),
        (regexp_match(UPPER(COALESCE(v.description, '')),
                      'SEC[[:space:]]*([0-9]+)'))[1]::integer
    ) AS units_per_box,
    pm.sale_type AS pm_sale_type,
    pm.quantity_step AS pm_quantity_step
FROM bsale.catalog_view cv
LEFT JOIN bsale.variants v
    ON v.company_id = %s
   AND v.bsale_id = cv.variant_id
LEFT JOIN LATERAL (
    SELECT pm_inner.*
    FROM bsale.products_master pm_inner
    WHERE (
        NULLIF(BTRIM(cv.bar_code), '') IS NOT NULL
        AND BTRIM(pm_inner.barcode) = BTRIM(cv.bar_code)
    )
    OR pm_inner.variant_id = cv.variant_id
    ORDER BY
        CASE
            WHEN NULLIF(BTRIM(cv.bar_code), '') IS NOT NULL
             AND BTRIM(pm_inner.barcode) = BTRIM(cv.bar_code)
                THEN 0
            WHEN pm_inner.variant_id = cv.variant_id
                THEN 1
            ELSE 2
        END,
        pm_inner.updated_at DESC NULLS LAST,
        pm_inner.id DESC
    LIMIT 1
) pm ON TRUE
ORDER BY
    cv.product_type ASC,
    cv.product ASC,
    cv.variant ASC
"""


def _query(sql, in_stock):
    if in_stock:
        sql = sql.replace("\nORDER BY\n    cv.product_type", "\nWHERE COALESCE(cv.stock, 0) > 0\nORDER BY\n    cv.product_type", 1)
    return sql


def _connection():
    return psycopg2.connect(host="127.0.0.1", port=55432, dbname="lq_catalog_qa", user="lq_qa",
                            password="qa_only_not_production", connect_timeout=5)


@pytest.fixture(scope="module")
def populated():
    with closing(_connection()) as conn, conn.cursor() as cur:
        cur.execute("SELECT current_database(), current_user")
        assert cur.fetchone() == ("lq_catalog_qa", "lq_qa")
        cur.execute("SELECT to_regnamespace('bsale')")
        assert cur.fetchone()[0] is None
        cur.execute("""
            CREATE SCHEMA bsale;
            CREATE TABLE bsale.products_master (
                id bigint PRIMARY KEY, barcode text, variant_id bigint,
                units_per_box integer, sale_type text, quantity_step integer, updated_at timestamp
            );
            CREATE TABLE bsale.variants (
                company_id integer, bsale_id bigint, description text, units_per_box integer,
                PRIMARY KEY (company_id, bsale_id)
            );
            CREATE TABLE bsale.catalog_fixture (
                variant_id bigint, product text, variant text, product_type text,
                bar_code text, stock numeric, image_url text, price_13 numeric, price_16 numeric
            );
            CREATE VIEW bsale.catalog_view AS SELECT * FROM bsale.catalog_fixture;
            INSERT INTO bsale.products_master
            SELECT n, lpad(n::text, 13, '0'), n, 24, 'ENTERA', 24, '2026-01-01'
            FROM generate_series(1,7602) n;
            INSERT INTO bsale.variants
            SELECT 3, n, 'FORMATO (SEC 24)', CASE WHEN n % 3 = 0 THEN 6 ELSE NULL END
            FROM generate_series(1,7602) n;
            INSERT INTO bsale.catalog_fixture
            SELECT n, 'Producto ' || n, 'Variante', 'CERvezas', lpad(n::text,13,'0'),
                   n % 31 - 2, NULL, 1000 + n, 1200 + n
            FROM generate_series(1,7602) n;
            -- exact trimmed barcode wins over variant, regardless of newer variant timestamp.
            -- newer barcode winner with NULL commercial fields must NOT borrow variant fields.
            INSERT INTO bsale.products_master VALUES
              (9001,' 0000000000001 ',9900,12,'PARCIAL',6,'2026-02-01'),
              (9002,' 0000000000001',9901,NULL,NULL,NULL,'2026-03-01'),
              (9003,'other-barcode',1,99,'ENTERA',99,'2026-04-01'),
              (9004,'0000000000002 ',9902,8,'PARCIAL',4,'2026-03-01'),
              (9005,' 0000000000002',9903,10,'PARCIAL',5,'2026-03-01'),
              (9006,'0000000000002',9904,99,'ENTERA',99,NULL),
              (9007,'   ',9905,11,'ENTERA',11,'2026-03-01'),
              (9008,NULL,9905,7,'ENTERA',7,'2026-04-01'),
              (9009,'unused',NULL,100,'ENTERA',100,'2026-04-01')
            ON CONFLICT (id) DO NOTHING;
            INSERT INTO bsale.variants VALUES (1,1,'WRONG COMPANY (SEC 999)',999);
            INSERT INTO bsale.catalog_fixture VALUES
              (9905,'Fallback','NULL barcode','TEST',NULL,7,NULL,100,200),
              (9905,'Fallback','Blank barcode','TEST','   ',0,NULL,110,210),
              (9905,'Fallback','Unknown barcode','TEST','missing',-1,NULL,120,220),
              (9906,'No mapping',NULL,'TEST',NULL,NULL,NULL,NULL,NULL),
              (9907,'Barcode without variant','SKU','TEST',' 0000000000002 ',1,'/icon.svg',900,950),
              (1,'Producto 1','Variante','CERvezas','0000000000001',-1,NULL,1001,1201);
            ANALYZE bsale.products_master;
            ANALYZE bsale.catalog_fixture;
            ANALYZE bsale.variants;
        """)
        conn.commit()
    return _connection


@pytest.mark.parametrize("price_list,in_stock", [
    ("factura", False), ("factura", True), ("melinka", False), ("melinka", True),
])
def test_same_rows_and_order_as_legacy(populated, price_list, in_stock):
    with closing(populated()) as conn, conn.cursor() as cur:
        cur.execute("SET LOCAL statement_timeout = '90s'")
        params = (price_list, price_list, 3)
        times = []
        results = []
        for sql in (LEGACY_QUERY, catalog._CATALOG_QUERY):
            start = time.perf_counter()
            cur.execute(_query(sql, in_stock), params)
            rows = cur.fetchall()
            times.append((time.perf_counter() - start) * 1000)
            results.append(rows)
        assert Counter(results[0]) == Counter(results[1])
        # Order is by category, product, variant; exact tie order was undefined before.
        assert [(r[2], r[3], r[4]) for r in results[0]] == [(r[2], r[3], r[4]) for r in results[1]]
        print(f"synthetic rows={len(results[1])} list={price_list} stock={in_stock} "
              f"legacy_ms={times[0]:.1f} optimized_ms={times[1]:.1f}")


def test_public_route_stock_and_prices_against_fixture(populated, monkeypatch):
    monkeypatch.setattr(catalog, "get_connection", populated)
    monkeypatch.setenv("CATALOG_SOURCE_PUBLIC", "legacy")
    full = catalog.get_catalog(price_list="factura", in_stock=False)
    stocked = catalog.get_catalog(price_list="melinka", in_stock=True)
    assert len(full) == 7608
    assert len(stocked) == sum(r["stock"] > 0 for r in full)
    assert all(r["stock"] > 0 for r in stocked)
    assert next(r for r in stocked if r["id"] == 3)["price"] == 1203
    assert next(r for r in full if r["id"] == 3)["price"] == 1003
    assert next(r for r in full if r["id"] == 9905 and r["stock"] == 7)["units_per_box"] == 7
    assert next(r for r in stocked if r["id"] == 9907)["units_per_box"] == 10
