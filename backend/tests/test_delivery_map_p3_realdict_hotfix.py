"""Hotfix P3: get_delivery_map con RealDictCursor + tablas 055 vacías.

Reproduce KeyError: 0 de Coolify cuando to_regclass se lee con [0].
Requiere SIMPLE_PLANNING_TEST_DSN (Postgres aislado).
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from psycopg2.extras import RealDictCursor

from backend.repositories.distribuidora import client_location_repo as loc_repo
from backend.services.distribuidora import delivery_map_service as map_svc

DSN = (os.getenv("SIMPLE_PLANNING_TEST_DSN") or "").strip()
ROOT = Path(__file__).resolve().parents[2]
SQL_055 = ROOT / "backend" / "sql" / "distribuidora" / "055_client_location_photos.sql"

pytestmark = pytest.mark.skipif(
    not DSN,
    reason="Define SIMPLE_PLANNING_TEST_DSN a Postgres aislado (nunca analytics)",
)


def _assert_isolated(uri: str) -> None:
    low = uri.lower()
    for b in ("analytics", "quillotana", "prod", "coolify", "neon.tech", "supabase"):
        if b in low:
            raise RuntimeError(f"DSN sospechoso: {b}")


def _connect():
    import psycopg2

    _assert_isolated(DSN)
    return psycopg2.connect(DSN)


def _split_sql(raw: str) -> list[str]:
    parts: list[str] = []
    buf: list[str] = []
    for line in raw.splitlines():
        if line.strip() == "-- +go":
            chunk = "\n".join(buf).strip()
            if chunk:
                parts.append(chunk)
            buf = []
        else:
            buf.append(line)
    chunk = "\n".join(buf).strip()
    if chunk:
        parts.append(chunk)
    return parts


def _bootstrap(conn):
    cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS distribuidora")
    cur.execute("CREATE SCHEMA IF NOT EXISTS bsale")
    for t in (
        "client_location_audit",
        "client_verified_locations",
        "client_location_proposals",
        "client_facade_photos",
        "dispatch_plan_order_events",
        "dispatch_plan_orders",
        "dispatch_plan",
        "clients",
        "rutero",
    ):
        schema = "bsale" if t in ("clients", "rutero") else "distribuidora"
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{t} CASCADE")
    cur.execute(
        """
        CREATE TABLE distribuidora.dispatch_plan (
            id BIGSERIAL PRIMARY KEY,
            status TEXT NOT NULL DEFAULT 'confirmed',
            planning_code TEXT,
            planning_date DATE NOT NULL DEFAULT CURRENT_DATE,
            route_name TEXT NOT NULL DEFAULT 'Ruta',
            driver_count INT NOT NULL DEFAULT 1,
            assistant_count INT NOT NULL DEFAULT 0,
            driver_cost_clp INT NOT NULL DEFAULT 0,
            assistant_cost_clp INT NOT NULL DEFAULT 0,
            fuel_cost_clp INT NOT NULL DEFAULT 0,
            ferry_cost_clp INT NOT NULL DEFAULT 0,
            toll_cost_clp INT NOT NULL DEFAULT 0,
            extras_cost_clp INT NOT NULL DEFAULT 0,
            crew_cost_clp INT NOT NULL DEFAULT 0,
            total_route_cost_clp INT NOT NULL DEFAULT 0,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            planning_origin TEXT NOT NULL DEFAULT 'ors',
            ors_metrics_calculated BOOLEAN NOT NULL DEFAULT TRUE,
            needs_recalculation BOOLEAN NOT NULL DEFAULT FALSE,
            truck_name TEXT,
            driver_name TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE distribuidora.dispatch_plan_orders (
            id BIGSERIAL PRIMARY KEY,
            dispatch_plan_id BIGINT NOT NULL REFERENCES distribuidora.dispatch_plan(id),
            oc_document_id BIGINT NOT NULL,
            oc_number BIGINT,
            route_order INT,
            client_id BIGINT,
            client_name TEXT,
            fantasy_name TEXT,
            address TEXT,
            city TEXT,
            oc_total_amount NUMERIC,
            cantidad_unidades NUMERIC,
            cantidad_productos NUMERIC,
            document_type_to_generate TEXT,
            payment_method TEXT,
            seller_name TEXT,
            lat DOUBLE PRECISION,
            lng DOUBLE PRECISION
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE distribuidora.dispatch_plan_order_events (
            id BIGSERIAL PRIMARY KEY,
            dispatch_plan_id BIGINT NOT NULL REFERENCES distribuidora.dispatch_plan(id),
            action TEXT NOT NULL,
            user_name TEXT,
            reason TEXT,
            payload JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE bsale.clients (
            bsale_id BIGINT PRIMARY KEY,
            lat DOUBLE PRECISION,
            lon DOUBLE PRECISION
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE bsale.rutero (
            bsale_id BIGINT PRIMARY KEY,
            lat DOUBLE PRECISION,
            lon DOUBLE PRECISION,
            lat_operacional DOUBLE PRECISION,
            lon_operacional DOUBLE PRECISION
        )
        """
    )
    conn.commit()
    for stmt in _split_sql(SQL_055.read_text(encoding="utf-8")):
        cur.execute(stmt)
    conn.commit()
    cur.execute(
        """
        INSERT INTO distribuidora.dispatch_plan (id, planning_code)
        VALUES (82, 'PLAN-00082')
        """
    )
    cur.execute(
        """
        INSERT INTO distribuidora.dispatch_plan_orders (
            dispatch_plan_id, oc_document_id, oc_number, route_order,
            client_id, client_name, address, city, lat, lng, oc_total_amount
        ) VALUES
        (82, 1001, 70001, 1, 2244, 'Cliente A', 'Calle 1', 'Ancud', -41.8701, -73.8201, 1000),
        (82, 1002, 70002, 2, 2245, 'Cliente B', 'Calle 2', 'Castro', NULL, NULL, 2000)
        """
    )
    cur.execute(
        "INSERT INTO bsale.clients (bsale_id, lat, lon) VALUES (2245, -42.4800, -73.7600)"
    )
    conn.commit()
    cur.close()


@pytest.fixture()
def db(monkeypatch):
    conn = _connect()
    _bootstrap(conn)
    monkeypatch.setattr(map_svc, "get_connection", _connect)
    yield conn
    conn.close()


def test_realdict_to_regclass_no_keyerror_zero(db):
    """Reproduce el fallo Coolify: RealDictCursor + fetchone()[0] → KeyError: 0."""
    cur = db.cursor(cursor_factory=RealDictCursor)
    cur.execute(
        """
        SELECT to_regclass('distribuidora.client_verified_locations') IS NOT NULL
            AS table_exists
        """
    )
    row = cur.fetchone()
    assert row is not None
    with pytest.raises(KeyError):
        _ = row[0]
    assert row["table_exists"] is True
    cur.close()


def test_get_delivery_map_with_055_empty_no_keyerror(db):
    out = map_svc.get_delivery_map(82)
    assert "stops" in out
    assert len(out["stops"]) == 2
    by_key = {s["customer_key"]: s for s in out["stops"]}
    a = by_key["id:2244"]
    assert a["has_coordinates"] is True
    assert a["coordinates_source"] == "dispatch_plan_orders.lat/lng"
    assert abs(float(a["latitude"]) - (-41.8701)) < 1e-6
    b = by_key["id:2245"]
    assert b["has_coordinates"] is True
    assert b["coordinates_source"] == "bsale.clients.lat/lon"


def test_verified_location_priority(db):
    cur = db.cursor()
    loc_repo.upsert_verified_location(
        cur,
        client_id=2244,
        company_id=3,
        latitude=-41.1111,
        longitude=-73.2222,
        accuracy_m=8,
        source="driver_approved",
        approved_proposal_id=None,
        approved_by_user_id=99,
    )
    db.commit()
    cur.close()

    out = map_svc.get_delivery_map(82)
    stop = next(s for s in out["stops"] if s["customer_key"] == "id:2244")
    assert stop["coordinates_source"] == "client_verified_locations"
    assert abs(float(stop["latitude"]) - (-41.1111)) < 1e-6
    assert abs(float(stop["longitude"]) - (-73.2222)) < 1e-6


def test_tables_ready_realdict_and_tuple(db):
    cur_t = db.cursor()
    assert loc_repo.tables_ready(cur_t) is True
    cur_t.close()
    cur_d = db.cursor(cursor_factory=RealDictCursor)
    assert loc_repo.tables_ready(cur_d) is True
    cur_d.close()
