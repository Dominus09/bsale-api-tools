"""Ownership chofer en Postgres aislado (sin analytics)."""

from __future__ import annotations

import os

import pytest

DSN = (os.getenv("SIMPLE_PLANNING_TEST_DSN") or "").strip()

pytestmark = pytest.mark.skipif(
    not DSN,
    reason="Define SIMPLE_PLANNING_TEST_DSN a Postgres aislado (nunca analytics)",
)


def _connect():
    import psycopg2

    return psycopg2.connect(DSN)


def _schema(conn):
    cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS distribuidora")
    cur.execute("DROP TABLE IF EXISTS distribuidora.dispatch_plan_orders CASCADE")
    cur.execute("DROP TABLE IF EXISTS distribuidora.dispatch_plan CASCADE")
    cur.execute("DROP TABLE IF EXISTS distribuidora.trucks CASCADE")
    cur.execute(
        """
        CREATE TABLE distribuidora.trucks (
            id BIGSERIAL PRIMARY KEY, name TEXT, plate TEXT, max_weight_kg NUMERIC
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE distribuidora.dispatch_plan (
            id BIGSERIAL PRIMARY KEY,
            planning_code TEXT,
            planning_name TEXT,
            planning_date DATE,
            truck_id BIGINT REFERENCES distribuidora.trucks(id),
            truck_name TEXT,
            route_name TEXT,
            status TEXT NOT NULL,
            driver_user_id BIGINT,
            driver_name TEXT,
            confirmed_at TIMESTAMPTZ
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE distribuidora.dispatch_plan_orders (
            id BIGSERIAL PRIMARY KEY,
            dispatch_plan_id BIGINT REFERENCES distribuidora.dispatch_plan(id),
            oc_document_id BIGINT,
            client_id BIGINT
        )
        """
    )
    conn.commit()
    cur.close()


def test_list_and_summary_only_for_assigned_driver(monkeypatch):
    conn = _connect()
    try:
        _schema(conn)
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO distribuidora.trucks(name, plate) VALUES ('Hino 4', 'ABCD12') RETURNING id"
        )
        truck_id = int(cur.fetchone()[0])
        cur.execute(
            """
            INSERT INTO distribuidora.dispatch_plan(
              planning_code, planning_name, planning_date, truck_id, truck_name,
              route_name, status, driver_user_id, driver_name
            ) VALUES
              ('PLAN-A', 'Ruta A', CURRENT_DATE, %s, 'Hino 4', 'Sur',
               'picking_generated', 10, 'Chofer 10'),
              ('PLAN-B', 'Ruta B', CURRENT_DATE, %s, 'Hino 4', 'Norte',
               'picking_generated', 99, 'Otro')
            RETURNING id
            """,
            (truck_id, truck_id),
        )
        ids = [int(r[0]) for r in cur.fetchall()]
        plan_a, plan_b = ids[0], ids[1]
        cur.execute(
            """
            INSERT INTO distribuidora.dispatch_plan_orders(dispatch_plan_id, oc_document_id, client_id)
            VALUES (%s, 1, 100), (%s, 2, 100), (%s, 3, 101)
            """,
            (plan_a, plan_a, plan_a),
        )
        conn.commit()
        cur.close()
    finally:
        conn.close()

    monkeypatch.setenv("PG_HOST", "")  # unused; we patch get_connection
    import psycopg2
    from backend.services.distribuidora import dispatch_plan_driver_service as svc

    monkeypatch.setattr(svc, "get_connection", lambda: psycopg2.connect(DSN))

    mine = svc.list_plans_for_driver_user(10)
    assert len(mine) == 1
    assert mine[0]["planning_code"] == "PLAN-A"
    assert mine[0]["truck_plate"] == "ABCD12"
    assert mine[0]["order_count"] == 3
    assert mine[0]["client_count"] == 2

    assert svc.get_plan_summary_for_driver(10, plan_b) is None
    own = svc.get_plan_summary_for_driver(10, plan_a)
    assert own is not None
    assert own["planning_code"] == "PLAN-A"
    assert svc.driver_has_access_to_plan(10, plan_a) is True
    assert svc.driver_has_access_to_plan(10, plan_b) is False
