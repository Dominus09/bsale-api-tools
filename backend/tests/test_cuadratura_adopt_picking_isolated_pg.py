"""F3-D3 adopción de picking en cuadratura — Postgres aislado (nunca analytics).

Requiere SIMPLE_PLANNING_TEST_DSN.
"""

from __future__ import annotations

import json
import os

import pytest

from backend.services.distribuidora import dispatch_plan_cuadratura_service as svc

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
        "dispatch_plan_cuadratura_history",
        "dispatch_plan_cuadratura",
        "dispatch_plan_picking_products",
        "dispatch_plan_picking_clients",
        "dispatch_plan_pickings",
        "dispatch_plan_orders",
        "dispatch_plan",
    ):
        cur.execute(f"DROP TABLE IF EXISTS distribuidora.{t} CASCADE")

    cur.execute(
        """
        CREATE TABLE distribuidora.trucks (
            id BIGSERIAL PRIMARY KEY,
            name TEXT,
            plate TEXT,
            max_weight_kg NUMERIC
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
        CREATE TABLE distribuidora.dispatch_plan_orders (
            id BIGSERIAL PRIMARY KEY,
            dispatch_plan_id BIGINT NOT NULL REFERENCES distribuidora.dispatch_plan(id) ON DELETE CASCADE,
            oc_document_id BIGINT NOT NULL,
            oc_number BIGINT,
            route_order INT NOT NULL DEFAULT 0,
            client_name TEXT,
            oc_total_amount NUMERIC,
            UNIQUE (dispatch_plan_id, oc_document_id)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE distribuidora.dispatch_plan_pickings (
            id BIGSERIAL PRIMARY KEY,
            dispatch_plan_id BIGINT NOT NULL REFERENCES distribuidora.dispatch_plan(id) ON DELETE CASCADE,
            version INTEGER NOT NULL CHECK (version > 0),
            is_current BOOLEAN NOT NULL DEFAULT TRUE,
            include_probable BOOLEAN NOT NULL DEFAULT FALSE,
            generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            superseded_at TIMESTAMPTZ,
            header JSONB NOT NULL DEFAULT '{}'::jsonb,
            warnings JSONB NOT NULL DEFAULT '[]'::jsonb,
            stops_count INTEGER NOT NULL DEFAULT 0,
            product_lines_count INTEGER NOT NULL DEFAULT 0,
            document_total_clp NUMERIC(18, 2) NOT NULL DEFAULT 0,
            product_total_monto_clp NUMERIC(18, 2) NOT NULL DEFAULT 0,
            UNIQUE (dispatch_plan_id, version)
        )
        """
    )
    cur.execute(
        """
        CREATE UNIQUE INDEX uq_pickings_current
        ON distribuidora.dispatch_plan_pickings (dispatch_plan_id)
        WHERE is_current = TRUE
        """
    )
    cur.execute(
        """
        CREATE TABLE distribuidora.dispatch_plan_picking_clients (
            id BIGSERIAL PRIMARY KEY,
            picking_id BIGINT NOT NULL REFERENCES distribuidora.dispatch_plan_pickings(id) ON DELETE CASCADE,
            dispatch_plan_id BIGINT NOT NULL,
            route_order INTEGER NOT NULL DEFAULT 0,
            oc_document_id BIGINT,
            related_document_id BIGINT NOT NULL,
            client_id BIGINT,
            client_name TEXT,
            fantasy_name TEXT,
            document_number BIGINT,
            payment_method TEXT,
            document_total NUMERIC(18, 2),
            is_probable_included BOOLEAN NOT NULL DEFAULT FALSE
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE distribuidora.dispatch_plan_picking_products (
            id BIGSERIAL PRIMARY KEY,
            picking_id BIGINT NOT NULL REFERENCES distribuidora.dispatch_plan_pickings(id) ON DELETE CASCADE,
            dispatch_plan_id BIGINT NOT NULL,
            sort_order INTEGER NOT NULL DEFAULT 0,
            product_id BIGINT,
            variant_id BIGINT,
            tipo_producto TEXT,
            producto TEXT,
            variante TEXT,
            codigo_barras TEXT,
            unidades NUMERIC,
            cajas NUMERIC,
            total_monto NUMERIC
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE distribuidora.dispatch_plan_cuadratura (
            dispatch_plan_id BIGINT PRIMARY KEY REFERENCES distribuidora.dispatch_plan(id) ON DELETE CASCADE,
            transferencia_clp INT NOT NULL DEFAULT 0,
            efectivo_clp INT NOT NULL DEFAULT 0,
            cheque_clp INT NOT NULL DEFAULT 0,
            debito_clp INT NOT NULL DEFAULT 0,
            observacion TEXT,
            credit_notes JSONB NOT NULL DEFAULT '[]'::jsonb,
            not_loaded JSONB NOT NULL DEFAULT '[]'::jsonb,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            schema_version INT NOT NULL DEFAULT 1,
            status TEXT NOT NULL DEFAULT 'pending',
            documents JSONB NOT NULL DEFAULT '[]'::jsonb,
            credit_notes_v2 JSONB NOT NULL DEFAULT '[]'::jsonb,
            not_loaded_v2 JSONB NOT NULL DEFAULT '[]'::jsonb,
            cash_count JSONB NOT NULL DEFAULT '[]'::jsonb,
            picking_id BIGINT,
            picking_version INT,
            closed_at TIMESTAMPTZ,
            closed_by TEXT,
            resultado_cache JSONB
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE distribuidora.dispatch_plan_cuadratura_history (
            id BIGSERIAL PRIMARY KEY,
            dispatch_plan_id BIGINT NOT NULL,
            version INTEGER NOT NULL DEFAULT 1,
            schema_version INTEGER NOT NULL DEFAULT 2,
            status TEXT NOT NULL,
            snapshot JSONB NOT NULL,
            closed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            closed_by TEXT,
            observacion TEXT,
            diferencia_clp INTEGER,
            diferencia_status TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
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

    cur.execute(
        "INSERT INTO distribuidora.dispatch_plan (planning_code) VALUES ('PLAN-F3D3') RETURNING id"
    )
    plan_id = int(cur.fetchone()[0])

    # 3 docs originales en v1
    docs_v1 = [
        (901, 1, 11, "Cliente A", 10000, "efectivo"),
        (902, 2, 12, "Cliente B", 20000, "transferencia"),
        (903, 3, 13, "Cliente C", 15000, None),
    ]
    for oc, route, related, name, total, _pm in docs_v1:
        cur.execute(
            """
            INSERT INTO distribuidora.dispatch_plan_orders
              (dispatch_plan_id, oc_document_id, oc_number, route_order, client_name, oc_total_amount)
            VALUES (%s,%s,%s,%s,%s,%s)
            """,
            (plan_id, oc, oc, route, name, total),
        )

    cur.execute(
        """
        INSERT INTO distribuidora.dispatch_plan_pickings
          (dispatch_plan_id, version, is_current, stops_count, document_total_clp)
        VALUES (%s, 1, TRUE, 3, 45000)
        RETURNING id
        """,
        (plan_id,),
    )
    p1 = int(cur.fetchone()[0])
    for oc, route, related, name, total, pm in docs_v1:
        cur.execute(
            """
            INSERT INTO distribuidora.dispatch_plan_picking_clients
              (picking_id, dispatch_plan_id, route_order, oc_document_id, related_document_id,
               fantasy_name, document_number, payment_method, document_total)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (p1, plan_id, route, oc, related, name, related, pm, total),
        )

    conn.commit()
    cur.close()
    return plan_id, p1, docs_v1


def _insert_picking_v2(conn, plan_id: int, docs_v1, extra):
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE distribuidora.dispatch_plan_pickings
        SET is_current = FALSE, superseded_at = NOW()
        WHERE dispatch_plan_id = %s AND is_current = TRUE
        """,
        (plan_id,),
    )
    venta = sum(d[4] for d in docs_v1) + extra[4]
    cur.execute(
        """
        INSERT INTO distribuidora.dispatch_plan_pickings
          (dispatch_plan_id, version, is_current, stops_count, document_total_clp)
        VALUES (%s, 2, TRUE, %s, %s)
        RETURNING id
        """,
        (plan_id, len(docs_v1) + 1, venta),
    )
    p2 = int(cur.fetchone()[0])
    for oc, route, related, name, total, pm in list(docs_v1) + [extra]:
        cur.execute(
            """
            INSERT INTO distribuidora.dispatch_plan_picking_clients
              (picking_id, dispatch_plan_id, route_order, oc_document_id, related_document_id,
               fantasy_name, document_number, payment_method, document_total)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (p2, plan_id, route, oc, related, name, related, pm, total),
        )
        # OC extra
        if oc == extra[0]:
            cur.execute(
                """
                INSERT INTO distribuidora.dispatch_plan_orders
                  (dispatch_plan_id, oc_document_id, oc_number, route_order, client_name, oc_total_amount)
                VALUES (%s,%s,%s,%s,%s,%s)
                ON CONFLICT DO NOTHING
                """,
                (plan_id, oc, oc, route, name, total),
            )
    conn.commit()
    cur.close()
    return p2


def test_f3d3_adopt_picking_flow(monkeypatch):
    import backend.db as db
    from backend.services.distribuidora import dispatch_plan_service as plan_svc

    conn = _connect()
    try:
        plan_id, p1, docs_v1 = _bootstrap(conn)
    finally:
        conn.close()

    monkeypatch.setattr(db, "get_connection", _connect)
    monkeypatch.setattr(svc, "get_connection", _connect)
    monkeypatch.setattr(plan_svc, "get_connection", _connect)
    monkeypatch.setattr(svc, "_venta_facturada_clp", lambda *_a, **_k: 45000)

    # Inicia cuadratura adoptando v1 con medios/obs.
    saved = svc.save_dispatch_plan_cuadratura_v2(
        plan_id,
        {
            "schema_version": 2,
            "documents": [
                {
                    "related_document_id": 11,
                    "document_number": 11,
                    "oc_document_id": 901,
                    "client_name": "Cliente A",
                    "monto_clp": 10000,
                    "medio_pago": "efectivo",
                    "observacion": "contado caja",
                },
                {
                    "related_document_id": 12,
                    "document_number": 12,
                    "oc_document_id": 902,
                    "client_name": "Cliente B",
                    "monto_clp": 20000,
                    "medio_pago": "transferencia",
                    "observacion": "",
                },
                {
                    "related_document_id": 13,
                    "document_number": 13,
                    "oc_document_id": 903,
                    "client_name": "Cliente C",
                    "monto_clp": 15000,
                    "medio_pago": "pendiente",
                    "observacion": "",
                },
            ],
            "credit_notes_v2": [
                {
                    "documento": "12",
                    "nota_credito": "NC-1",
                    "monto": 1000,
                    "observacion": "parcial",
                }
            ],
            "cash_count": [{"denominacion_clp": 10000, "cantidad": 1, "subtotal_clp": 10000}],
            "observacion": "arqueo inicial",
        },
    )
    assert saved["adopted_picking_version"] == 1
    assert saved["picking_update"]["pending"] is False

    # Nueva versión con OC adicional (cliente nuevo).
    conn = _connect()
    try:
        _insert_picking_v2(
            conn,
            plan_id,
            docs_v1,
            (990, 4, 99, "Cliente Nuevo", 5000, None),
        )
    finally:
        conn.close()

    preview = svc.get_dispatch_plan_cuadratura(plan_id)
    # A: detecta drift; B: no adopta en GET; C: diff correcto
    assert preview["adopted_picking_version"] == 1
    assert preview["available_picking_version"] == 2
    assert preview["ventas"]["venta_picking_clp"] == 45000
    assert preview["picking_update"]["pending"] is True
    assert preview["picking_update"]["documents_added_count"] == 1
    assert preview["write_blocked_by_picking_update"] is True
    # Pagos/NC conservados en alcance adoptado
    medios = {d["related_document_id"]: d["medio_pago"] for d in preview["documents"]}
    assert medios[11] == "efectivo"
    assert medios[12] == "transferencia"
    assert preview["credit_notes_v2"][0]["nota_credito"] == "NC-1"
    assert preview["observacion"] == "arqueo inicial"

    # D: PUT bloqueado
    with pytest.raises(svc.CuadraturaConflictError) as ei:
        svc.save_dispatch_plan_cuadratura_v2(
            plan_id,
            {
                "schema_version": 2,
                "documents": preview["documents"],
                "expected_picking_version": 1,
            },
        )
    assert ei.value.code == "picking_update_pending"

    # E/F/G/H/I: adopción
    adopted = svc.adopt_cuadratura_picking(
        plan_id,
        user={"email": "ops@test.local"},
        expected_available_version=2,
    )
    assert adopted["adopt_status"] == "adopted"
    assert adopted["adopted_picking_version"] == 2
    assert adopted["picking_update"]["pending"] is False
    assert adopted["ventas"]["venta_picking_clp"] == 50000
    assert len(adopted["documents"]) == 4
    medios2 = {d["related_document_id"]: d for d in adopted["documents"]}
    assert medios2[11]["medio_pago"] == "efectivo"
    assert medios2[11]["observacion"] == "contado caja"
    assert medios2[12]["medio_pago"] == "transferencia"
    assert adopted["credit_notes_v2"][0]["monto"] == 1000
    assert any(d["related_document_id"] == 99 for d in adopted["documents"])
    # sin duplicados
    ids = [d["related_document_id"] for d in adopted["documents"]]
    assert len(ids) == len(set(ids))

    # K: reintento idempotente
    again = svc.adopt_cuadratura_picking(
        plan_id, user={"email": "ops@test.local"}, expected_available_version=2
    )
    assert again["adopt_status"] == "unchanged"

    # L: cierre y bloqueo
    closed = svc.close_dispatch_plan_cuadratura(
        plan_id, closed_by="ops@test.local", observacion="ok cierre"
    )
    assert closed["closed_at"] is not None
    with pytest.raises(svc.CuadraturaConflictError) as ei2:
        svc.adopt_cuadratura_picking(plan_id)
    assert ei2.value.code == "cuadratura_closed"
    with pytest.raises(svc.CuadraturaConflictError) as ei3:
        svc.save_dispatch_plan_cuadratura_v2(
            plan_id, {"schema_version": 2, "documents": closed["documents"]}
        )
    assert ei3.value.code == "cuadratura_closed"

    # Evento de auditoría
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT action, picking_version, payload
            FROM distribuidora.dispatch_plan_order_events
            WHERE dispatch_plan_id=%s AND action='adopt_cuadratura_picking'
            ORDER BY id
            """,
            (plan_id,),
        )
        rows = cur.fetchall()
        assert len(rows) >= 2
        payload = rows[0][2]
        if isinstance(payload, str):
            payload = json.loads(payload)
        assert payload.get("status") == "adopted"
        cur.close()
    finally:
        conn.close()
