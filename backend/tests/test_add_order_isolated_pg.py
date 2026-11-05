"""Agregar OC: persistencia en Postgres aislado (nunca analytics).

Requiere SIMPLE_PLANNING_TEST_DSN. Valida el flujo ACTUAL (sin picking suplementario).
"""

from __future__ import annotations

import os

import pytest

from backend.services.distribuidora import dispatch_plan_load_batch_service as svc

DSN = (os.getenv("SIMPLE_PLANNING_TEST_DSN") or "").strip()

pytestmark = pytest.mark.skipif(
    not DSN,
    reason="Define SIMPLE_PLANNING_TEST_DSN a Postgres de prueba aislado (nunca analytics)",
)


def _connect():
    import psycopg2

    return psycopg2.connect(DSN)


def _bootstrap(conn):
    cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS distribuidora")
    for t in (
        "dispatch_plan_order_events",
        "dispatch_plan_order_load_assignments",
        "dispatch_plan_document_load_assignments",
        "dispatch_plan_orders",
        "dispatch_plan_load_batches",
        "dispatch_plan",
        "trucks",
    ):
        cur.execute(f"DROP TABLE IF EXISTS distribuidora.{t} CASCADE")
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
            status TEXT NOT NULL DEFAULT 'picking_generated',
            planning_code TEXT,
            truck_id BIGINT REFERENCES distribuidora.trucks(id),
            load_organization_status TEXT NOT NULL DEFAULT 'organized'
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE distribuidora.dispatch_plan_load_batches (
            id BIGSERIAL PRIMARY KEY,
            dispatch_plan_id BIGINT NOT NULL REFERENCES distribuidora.dispatch_plan(id) ON DELETE CASCADE,
            sort_order INT NOT NULL DEFAULT 0,
            name TEXT NOT NULL,
            description TEXT,
            presentation_role TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE distribuidora.dispatch_plan_orders (
            id BIGSERIAL PRIMARY KEY,
            dispatch_plan_id BIGINT NOT NULL REFERENCES distribuidora.dispatch_plan(id) ON DELETE CASCADE,
            oc_document_id BIGINT NOT NULL,
            oc_number BIGINT,
            route_order INT NOT NULL DEFAULT 0,
            client_id BIGINT,
            client_name TEXT,
            fantasy_name TEXT,
            address TEXT,
            city TEXT,
            seller_name TEXT,
            payment_method TEXT,
            document_type_to_generate TEXT,
            oc_total_amount NUMERIC,
            lat DOUBLE PRECISION,
            lng DOUBLE PRECISION,
            peso_total_kg NUMERIC,
            cantidad_productos NUMERIC,
            cantidad_unidades NUMERIC,
            cantidad_cajas NUMERIC,
            productos_sin_peso INT,
            cobertura_logistica NUMERIC,
            peso_calculated_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CONSTRAINT uq_dpo UNIQUE (dispatch_plan_id, oc_document_id)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE distribuidora.dispatch_plan_order_load_assignments (
            id BIGSERIAL PRIMARY KEY,
            dispatch_plan_id BIGINT NOT NULL,
            oc_document_id BIGINT NOT NULL,
            load_batch_id BIGINT NOT NULL,
            client_id BIGINT,
            assigned_by TEXT,
            assigned_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (dispatch_plan_id, oc_document_id)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE distribuidora.dispatch_plan_order_events (
            id BIGSERIAL PRIMARY KEY,
            dispatch_plan_id BIGINT NOT NULL,
            action TEXT NOT NULL,
            user_name TEXT,
            reason TEXT,
            oc_document_id BIGINT,
            oc_number BIGINT,
            picking_id BIGINT,
            picking_version INT,
            payload JSONB,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )
    cur.execute("INSERT INTO distribuidora.trucks (name) VALUES ('Hino test') RETURNING id")
    truck_id = cur.fetchone()[0]
    cur.execute(
        """
        INSERT INTO distribuidora.dispatch_plan (status, planning_code, truck_id, load_organization_status)
        VALUES ('picking_generated', 'PLAN-TEST-ADD', %s, 'organized')
        RETURNING id
        """,
        (truck_id,),
    )
    plan_id = cur.fetchone()[0]
    cur.execute(
        """
        INSERT INTO distribuidora.dispatch_plan_load_batches (dispatch_plan_id, sort_order, name, presentation_role)
        VALUES (%s, 1, 'grupo 1', 'priority'), (%s, 2, 'consolidado', 'consolidated')
        RETURNING id
        """,
        (plan_id, plan_id),
    )
    # RETURNING with multi-row: fetch both
    cur.execute(
        "SELECT id FROM distribuidora.dispatch_plan_load_batches WHERE dispatch_plan_id=%s ORDER BY sort_order",
        (plan_id,),
    )
    b1, b2 = [r[0] for r in cur.fetchall()]
    for oc, batch, route in ((101, b1, 1), (102, b2, 2)):
        cur.execute(
            """
            INSERT INTO distribuidora.dispatch_plan_orders
              (dispatch_plan_id, oc_document_id, oc_number, route_order, client_name)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (plan_id, oc, oc, route, f"Cliente {oc}"),
        )
        cur.execute(
            """
            INSERT INTO distribuidora.dispatch_plan_order_load_assignments
              (dispatch_plan_id, oc_document_id, load_batch_id)
            VALUES (%s, %s, %s)
            """,
            (plan_id, oc, batch),
        )
    conn.commit()
    cur.close()
    return plan_id, b1, b2


def test_add_order_preserves_batches_and_org_assignments(monkeypatch):
    """OC nueva: entra al plan; NO crea grupo suplementario; NO toca org original."""
    conn = _connect()
    try:
        plan_id, b1, b2 = _bootstrap(conn)
    finally:
        conn.close()

    monkeypatch.setattr(svc, "get_connection", _connect)
    # Sin picking persistido en este esquema mínimo → no regenera.
    monkeypatch.setattr(svc.batch_repo, "plan_has_current_picking", lambda *_a, **_k: False)
    monkeypatch.setattr(svc, "resolve_operational_statuses_batch", lambda *_a, **_k: {})
    monkeypatch.setattr(
        svc.plan_svc,
        "_enrich_orders_snapshot",
        lambda _cur, orders: [
            {
                **orders[0],
                "oc_number": 999,
                "client_name": "Nuevo",
                "oc_total_amount": 1000,
            }
        ],
    )
    monkeypatch.setattr(svc.plan_svc, "get_dispatch_plan", lambda *_a, **_k: {"id": plan_id})

    out = svc.add_order_to_plan(
        plan_id, oc_document_id=999, regenerate_picking=False, reason="prueba aislada"
    )
    assert out["added"] is True

    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT count(*) FROM distribuidora.dispatch_plan_orders WHERE dispatch_plan_id=%s",
            (plan_id,),
        )
        assert cur.fetchone()[0] == 3
        cur.execute(
            "SELECT count(*) FROM distribuidora.dispatch_plan_load_batches WHERE dispatch_plan_id=%s",
            (plan_id,),
        )
        assert cur.fetchone()[0] == 2  # sin grupo «OC adicionales»
        cur.execute(
            """
            SELECT oc_document_id, load_batch_id
            FROM distribuidora.dispatch_plan_order_load_assignments
            WHERE dispatch_plan_id=%s ORDER BY oc_document_id
            """,
            (plan_id,),
        )
        org = cur.fetchall()
        assert org == [(101, b1), (102, b2)]  # org intacta; 999 sin asignación
        cur.execute(
            """
            SELECT action, oc_document_id FROM distribuidora.dispatch_plan_order_events
            WHERE dispatch_plan_id=%s AND action='add_order'
            """,
            (plan_id,),
        )
        ev = cur.fetchone()
        assert ev and ev[1] == 999
        cur.close()
    finally:
        conn.close()

    with pytest.raises(ValueError, match="ya pertenece"):
        svc.add_order_to_plan(plan_id, oc_document_id=101, regenerate_picking=False)
