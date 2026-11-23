"""Exclusión admin OC — PostgreSQL aislado (nunca prod / nunca OC 71041 real).

DSN: SIMPLE_PLANNING_TEST_DSN o archivo _tmp_isolated_pg_dsn.txt local.
"""

from __future__ import annotations

import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pytest

from backend.services.distribuidora import dispatch_plan_order_exclude_service as excl
from backend.services.distribuidora.dispatch_plan_service import (
    _invoicing_payload_from_rows,
)

ROOT = Path(__file__).resolve().parents[2]
SQL_062 = (
    ROOT
    / "backend"
    / "sql"
    / "distribuidora"
    / "062_dispatch_plan_order_admin_exclude_AWAITING_APPROVAL.sql"
)


def _dsn() -> str:
    env = (os.getenv("SIMPLE_PLANNING_TEST_DSN") or "").strip()
    if env:
        return env
    f = ROOT / "_tmp_isolated_pg_dsn.txt"
    if f.is_file():
        return f.read_text(encoding="utf-8").strip().splitlines()[0].strip()
    return ""


DSN = _dsn()

pytestmark = pytest.mark.skipif(
    not DSN,
    reason="Define SIMPLE_PLANNING_TEST_DSN (Postgres aislado) para esta suite",
)


def _connect():
    import psycopg2

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


def _setup(conn):
    cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS distribuidora")
    # Reset capability cache (062 se re-aplica abajo).
    import backend.repositories.distribuidora.dispatch_plan_repo as plan_repo_mod

    plan_repo_mod._EXCLUDE_COLS_READY = None

    for t in (
        "dispatch_plan_order_events",
        "dispatch_plan_order_load_assignments",
        "dispatch_plan_picking_clients",
        "dispatch_plan_pickings",
        "dispatch_plan_cuadratura",
        "delivery_document_results",
        "delivery_visits",
        "document_related",
        "document_details",
        "documents",
        "dispatch_plan_orders",
        "dispatch_plan",
        "trucks",
    ):
        cur.execute(f"DROP TABLE IF EXISTS distribuidora.{t} CASCADE")

    cur.execute(
        """
        CREATE TABLE distribuidora.trucks (
            id BIGSERIAL PRIMARY KEY,
            name TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE distribuidora.documents (
            document_id BIGINT PRIMARY KEY,
            number BIGINT,
            document_type_id INT,
            state INT DEFAULT 0,
            total_amount NUMERIC(18,2)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE distribuidora.document_details (
            detail_id BIGSERIAL PRIMARY KEY,
            document_id BIGINT NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE distribuidora.document_related (
            id SERIAL PRIMARY KEY,
            detail_id BIGINT NOT NULL,
            related_document_id BIGINT NOT NULL,
            related_document_type INT NOT NULL DEFAULT 6
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE distribuidora.dispatch_plan (
            id BIGSERIAL PRIMARY KEY,
            plan_session_id TEXT,
            planning_code TEXT,
            status TEXT NOT NULL DEFAULT 'closed',
            planning_date DATE DEFAULT CURRENT_DATE,
            truck_id BIGINT,
            needs_recalculation BOOLEAN NOT NULL DEFAULT FALSE,
            invalidated_at TIMESTAMPTZ,
            updated_at TIMESTAMPTZ DEFAULT NOW()
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE distribuidora.dispatch_plan_orders (
            id BIGSERIAL PRIMARY KEY,
            dispatch_plan_id BIGINT NOT NULL
                REFERENCES distribuidora.dispatch_plan(id) ON DELETE CASCADE,
            oc_document_id BIGINT NOT NULL,
            oc_number BIGINT,
            route_order INT DEFAULT 0,
            client_id BIGINT,
            client_name TEXT,
            oc_total_amount NUMERIC(18,2),
            created_at TIMESTAMPTZ DEFAULT NOW(),
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
            payload JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE distribuidora.dispatch_plan_pickings (
            id BIGSERIAL PRIMARY KEY,
            dispatch_plan_id BIGINT,
            version INT DEFAULT 1,
            is_current BOOLEAN DEFAULT TRUE
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE distribuidora.dispatch_plan_picking_clients (
            id BIGSERIAL PRIMARY KEY,
            picking_id BIGINT,
            dispatch_plan_id BIGINT,
            oc_document_id BIGINT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE distribuidora.dispatch_plan_order_load_assignments (
            id BIGSERIAL PRIMARY KEY,
            dispatch_plan_id BIGINT,
            oc_document_id BIGINT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE distribuidora.dispatch_plan_cuadratura (
            dispatch_plan_id BIGINT PRIMARY KEY,
            status TEXT,
            closed_at TIMESTAMPTZ
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE distribuidora.delivery_visits (
            id BIGSERIAL PRIMARY KEY,
            dispatch_plan_id BIGINT,
            result_status TEXT,
            started_at TIMESTAMPTZ,
            ended_at TIMESTAMPTZ,
            admin_classification TEXT DEFAULT 'operational',
            voided_at TIMESTAMPTZ,
            voided_by_user_id INT,
            void_reason TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE distribuidora.delivery_document_results (
            id BIGSERIAL PRIMARY KEY,
            visit_id BIGINT,
            oc_document_id BIGINT
        )
        """
    )

    for chunk in _split_sql(SQL_062.read_text(encoding="utf-8")):
        cur.execute(chunk)

    # Plan cerrado sintético: 7 facturadas + 1 pendiente (simula caso 71041)
    cur.execute(
        """
        INSERT INTO distribuidora.dispatch_plan (id, planning_code, status)
        VALUES (100, 'PLAN-TEST-EXCL', 'closed')
        """
    )
    for i in range(1, 8):
        cur.execute(
            """
            INSERT INTO distribuidora.dispatch_plan_orders (
                dispatch_plan_id, oc_document_id, oc_number, route_order,
                client_name, oc_total_amount
            ) VALUES (100, %s, %s, %s, %s, %s)
            """,
            (1000 + i, 70000 + i, i, f"Cliente {i}", 100000 + i),
        )
        cur.execute(
            "INSERT INTO distribuidora.documents(document_id, number, document_type_id, state, total_amount) "
            "VALUES (%s, %s, 6, 0, %s)",
            (2000 + i, 9000 + i, 50000 + i),
        )
    # OC pendiente sintética (NO es 71041 real)
    cur.execute(
        """
        INSERT INTO distribuidora.dispatch_plan_orders (
            dispatch_plan_id, oc_document_id, oc_number, route_order,
            client_name, oc_total_amount
        ) VALUES (100, 999001, 999001, 8, 'Cliente Pendiente', 223128)
        """
    )
    cur.execute(
        "INSERT INTO distribuidora.documents(document_id, number, document_type_id, state, total_amount) "
        "VALUES (999001, 999001, 8, 0, 223128)"
    )
    conn.commit()
    cur.close()


@pytest.fixture()
def isolated_conn(monkeypatch):
    conn = _connect()
    _setup(conn)

    def _get_connection():
        # Nueva conexión por operación (como prod); comparte misma BD aislada.
        return _connect()

    monkeypatch.setattr(excl, "get_connection", _get_connection)

    # Resolver operacional: pendiente sin factura
    class _StPending:
        billing_status = "pending"
        confirmed_invoice = None

    class _StInvoiced:
        billing_status = "invoiced"
        confirmed_invoice = {"document_id": 2001, "number": 9001, "total_amount": 50001}

    def _resolve(cur, oc_ids, **_k):
        out = {}
        for oid in oc_ids:
            if int(oid) == 999001:
                out[int(oid)] = _StPending()
            else:
                out[int(oid)] = _StInvoiced()
        return out

    monkeypatch.setattr(excl, "resolve_operational_statuses_batch", _resolve)
    monkeypatch.setattr(
        excl.plan_repo, "order_exclude_columns_ready", lambda _c: True
    )

    yield conn
    conn.close()


def _mgmt_user():
    return {"id": 7, "role": "gerencia", "email": "gerencia@test.local", "is_owner": True}


def test_exclude_eligible_pending_updates_counts(isolated_conn, monkeypatch):
    monkeypatch.setattr(
        excl, "has_management_access", lambda _u: True
    )
    # Asegurar list_plan_orders usa columnas reales
    from backend.repositories.distribuidora import dispatch_plan_repo as plan_repo

    monkeypatch.setattr(excl, "plan_repo", plan_repo)
    monkeypatch.setattr(plan_repo, "order_exclude_columns_ready", lambda _c: True)

    out = excl.exclude_order(
        user=_mgmt_user(),
        plan_id=100,
        oc_document_id=999001,
        reason="pedido cancelado por vendedor (lab)",
    )
    assert out["ok"] is True
    assert out["idempotent"] is False
    assert out["active_order_count"] == 7
    assert float(out["active_oc_amount"]) == sum(100000 + i for i in range(1, 8))
    assert out["needs_recalculation"] is True

    cur = isolated_conn.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM distribuidora.dispatch_plan_orders "
        "WHERE dispatch_plan_id=100 AND excluded_at IS NULL"
    )
    assert int(cur.fetchone()[0]) == 7
    cur.execute(
        "SELECT COUNT(*) FROM distribuidora.dispatch_plan_order_events "
        "WHERE action=%s AND oc_document_id=999001",
        (excl.ACTION_EXCLUDE_ADMIN,),
    )
    assert int(cur.fetchone()[0]) == 1
    cur.execute(
        "SELECT needs_recalculation FROM distribuidora.dispatch_plan WHERE id=100"
    )
    assert cur.fetchone()[0] is True
    # Bsale doc intacto
    cur.execute("SELECT state, total_amount FROM distribuidora.documents WHERE document_id=999001")
    st, amt = cur.fetchone()
    assert int(st) == 0
    assert float(amt) == 223128
    cur.close()


def test_exclude_invoiced_blocked(isolated_conn, monkeypatch):
    monkeypatch.setattr(excl, "has_management_access", lambda _u: True)
    from backend.repositories.distribuidora import dispatch_plan_repo as plan_repo

    monkeypatch.setattr(excl, "plan_repo", plan_repo)
    monkeypatch.setattr(plan_repo, "order_exclude_columns_ready", lambda _c: True)

    with pytest.raises(excl.OrderExcludeError) as ei:
        excl.exclude_order(
            user=_mgmt_user(),
            plan_id=100,
            oc_document_id=1001,
            reason="intento sobre facturada",
        )
    assert ei.value.http_status == 409
    assert ei.value.code == "has_confirmed_invoice"


def test_exclude_closed_plan_allowed_when_eligible(isolated_conn, monkeypatch):
    monkeypatch.setattr(excl, "has_management_access", lambda _u: True)
    from backend.repositories.distribuidora import dispatch_plan_repo as plan_repo

    monkeypatch.setattr(excl, "plan_repo", plan_repo)
    monkeypatch.setattr(plan_repo, "order_exclude_columns_ready", lambda _c: True)
    prev = excl.preview_exclude_order(
        user=_mgmt_user(), plan_id=100, oc_document_id=999001
    )
    assert prev["plan_status"] == "closed"
    assert prev["can_exclude"] is True


def test_exclude_blocked_by_picking(isolated_conn, monkeypatch):
    monkeypatch.setattr(excl, "has_management_access", lambda _u: True)
    from backend.repositories.distribuidora import dispatch_plan_repo as plan_repo

    monkeypatch.setattr(excl, "plan_repo", plan_repo)
    monkeypatch.setattr(plan_repo, "order_exclude_columns_ready", lambda _c: True)
    cur = isolated_conn.cursor()
    cur.execute(
        "INSERT INTO distribuidora.dispatch_plan_pickings(id, dispatch_plan_id, is_current) "
        "VALUES (1, 100, TRUE)"
    )
    cur.execute(
        "INSERT INTO distribuidora.dispatch_plan_picking_clients"
        "(picking_id, dispatch_plan_id, oc_document_id) VALUES (1, 100, 999001)"
    )
    isolated_conn.commit()
    cur.close()
    with pytest.raises(excl.OrderExcludeError) as ei:
        excl.exclude_order(
            user=_mgmt_user(),
            plan_id=100,
            oc_document_id=999001,
            reason="con picking activo",
        )
    assert ei.value.code == "in_current_picking"


def test_exclude_blocked_dispatched(isolated_conn, monkeypatch):
    monkeypatch.setattr(excl, "has_management_access", lambda _u: True)
    from backend.repositories.distribuidora import dispatch_plan_repo as plan_repo

    monkeypatch.setattr(excl, "plan_repo", plan_repo)
    monkeypatch.setattr(plan_repo, "order_exclude_columns_ready", lambda _c: True)
    cur = isolated_conn.cursor()
    cur.execute("UPDATE distribuidora.dispatch_plan SET status='dispatched' WHERE id=100")
    isolated_conn.commit()
    cur.close()
    with pytest.raises(excl.OrderExcludeError) as ei:
        excl.exclude_order(
            user=_mgmt_user(),
            plan_id=100,
            oc_document_id=999001,
            reason="despacho iniciado",
        )
    assert ei.value.code == "plan_status_blocks"


def test_exclude_blocked_cuadratura(isolated_conn, monkeypatch):
    monkeypatch.setattr(excl, "has_management_access", lambda _u: True)
    from backend.repositories.distribuidora import dispatch_plan_repo as plan_repo

    monkeypatch.setattr(excl, "plan_repo", plan_repo)
    monkeypatch.setattr(plan_repo, "order_exclude_columns_ready", lambda _c: True)
    cur = isolated_conn.cursor()
    cur.execute(
        "INSERT INTO distribuidora.dispatch_plan_cuadratura(dispatch_plan_id, status, closed_at) "
        "VALUES (100, 'closed', NOW())"
    )
    isolated_conn.commit()
    cur.close()
    with pytest.raises(excl.OrderExcludeError) as ei:
        excl.exclude_order(
            user=_mgmt_user(),
            plan_id=100,
            oc_document_id=999001,
            reason="cuadratura cerrada",
        )
    assert ei.value.code == "cuadratura_closed"


def test_exclude_idempotent_no_duplicate_events(isolated_conn, monkeypatch):
    monkeypatch.setattr(excl, "has_management_access", lambda _u: True)
    from backend.repositories.distribuidora import dispatch_plan_repo as plan_repo

    monkeypatch.setattr(excl, "plan_repo", plan_repo)
    monkeypatch.setattr(plan_repo, "order_exclude_columns_ready", lambda _c: True)
    excl.exclude_order(
        user=_mgmt_user(),
        plan_id=100,
        oc_document_id=999001,
        reason="primera exclusión lab",
    )
    out2 = excl.exclude_order(
        user=_mgmt_user(),
        plan_id=100,
        oc_document_id=999001,
        reason="segunda exclusión lab",
    )
    assert out2["idempotent"] is True
    cur = isolated_conn.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM distribuidora.dispatch_plan_order_events "
        "WHERE action=%s AND oc_document_id=999001",
        (excl.ACTION_EXCLUDE_ADMIN,),
    )
    assert int(cur.fetchone()[0]) == 1
    cur.close()


def test_exclude_concurrent_double_click(isolated_conn, monkeypatch):
    monkeypatch.setattr(excl, "has_management_access", lambda _u: True)
    from backend.repositories.distribuidora import dispatch_plan_repo as plan_repo

    monkeypatch.setattr(excl, "plan_repo", plan_repo)
    monkeypatch.setattr(plan_repo, "order_exclude_columns_ready", lambda _c: True)

    results: list[dict] = []
    errors: list[BaseException] = []
    barrier = threading.Barrier(2)

    def _run():
        try:
            barrier.wait(timeout=5)
            results.append(
                excl.exclude_order(
                    user=_mgmt_user(),
                    plan_id=100,
                    oc_document_id=999001,
                    reason="doble clic concurrente lab",
                )
            )
        except BaseException as e:  # noqa: BLE001
            errors.append(e)

    with ThreadPoolExecutor(max_workers=2) as pool:
        futs = [pool.submit(_run) for _ in range(2)]
        for f in as_completed(futs):
            f.result()

    assert not errors, errors
    assert len(results) == 2
    assert sum(1 for r in results if not r.get("idempotent")) == 1
    assert sum(1 for r in results if r.get("idempotent")) == 1
    cur = isolated_conn.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM distribuidora.dispatch_plan_order_events "
        "WHERE action=%s AND oc_document_id=999001",
        (excl.ACTION_EXCLUDE_ADMIN,),
    )
    assert int(cur.fetchone()[0]) == 1
    cur.close()


def test_forbidden_without_management(isolated_conn, monkeypatch):
    monkeypatch.setattr(excl, "has_management_access", lambda _u: False)
    with pytest.raises(excl.OrderExcludeError) as ei:
        excl.exclude_order(
            user={"id": 3, "role": "operativo", "email": "op@test"},
            plan_id=100,
            oc_document_id=999001,
            reason="sin permiso gerencia",
        )
    assert ei.value.http_status == 403


def test_supplemental_rejects_admin_excluded(isolated_conn, monkeypatch):
    """Sincronización / add suplementario no reincorpora OC excluida."""
    monkeypatch.setattr(excl, "has_management_access", lambda _u: True)
    from backend.repositories.distribuidora import dispatch_plan_repo as plan_repo

    monkeypatch.setattr(excl, "plan_repo", plan_repo)
    monkeypatch.setattr(plan_repo, "order_exclude_columns_ready", lambda _c: True)
    excl.exclude_order(
        user=_mgmt_user(),
        plan_id=100,
        oc_document_id=999001,
        reason="excluida antes de sync lab",
    )
    # Simula el chequeo de supplemental (novel = no activa, pero fila excluida).
    conn = _connect()
    cur = conn.cursor()
    assert plan_repo.order_exclude_columns_ready(cur) is True
    active_ids = {
        int(o["oc_document_id"])
        for o in plan_repo.list_plan_orders(cur, 100, include_excluded=False)
    }
    assert 999001 not in active_ids
    row = plan_repo.get_plan_order(cur, 100, 999001)
    assert row is not None and row.get("excluded_at") is not None
    cur.close()
    conn.close()


def test_fresh_invoice_blocks_even_if_ops_pending(isolated_conn, monkeypatch):
    """document_related fresco bloquea aunque el resolver diga pending."""
    monkeypatch.setattr(excl, "has_management_access", lambda _u: True)
    from backend.repositories.distribuidora import dispatch_plan_repo as plan_repo

    monkeypatch.setattr(excl, "plan_repo", plan_repo)
    monkeypatch.setattr(plan_repo, "order_exclude_columns_ready", lambda _c: True)

    cur = isolated_conn.cursor()
    cur.execute(
        "INSERT INTO distribuidora.documents(document_id, number, document_type_id, state, total_amount) "
        "VALUES (555001, 555001, 6, 0, 1000)"
    )
    cur.execute(
        "INSERT INTO distribuidora.document_details(detail_id, document_id) VALUES (1, 999001)"
    )
    cur.execute(
        "INSERT INTO distribuidora.document_related"
        "(detail_id, related_document_id, related_document_type) VALUES (1, 555001, 6)"
    )
    isolated_conn.commit()
    cur.close()

    with pytest.raises(excl.OrderExcludeError) as ei:
        excl.exclude_order(
            user=_mgmt_user(),
            plan_id=100,
            oc_document_id=999001,
            reason="factura fresca sync lab",
        )
    assert ei.value.code == "has_confirmed_invoice"


def test_venta_facturada_uses_doc_total_not_oc_and_dedups():
    class _St:
        billing_status = "invoiced"
        dispatch_status = "pending"
        planning_eligible = True
        dispatch_closed = False
        fulfillment_status = "pending"
        excluded_reason = None
        completion_source = "invoice"
        billing_label_es = "Facturada"
        fulfillment_label_es = "Pendiente"
        credit_notes = []
        evidence_source = "direct_related"
        confirmed_invoice = {
            "document_id": 55,
            "number": 9001,
            "total_amount": 150000,
        }

    rows = [
        {"oc_document_id": 1, "oc_number": 1, "status": "missing", "related_document_id": None},
        {
            "oc_document_id": 2,
            "oc_number": 2,
            "status": "confirmed",
            "related_document_id": 55,
            "document_total": 150000,
        },
    ]
    out = _invoicing_payload_from_rows(
        10, rows, operational_by_oc={1: _St(), 2: _St()}
    )
    assert float(out["items"][0]["document_total"]) == 150000.0
    assert float(out["items"][1]["document_total"]) == 150000.0
    # Dedup de venta facturada: dos OC → mismo related_document_id 55 → suma 1 vez
    # (lógica de dashboard: seen_confirmed_docs).
    seen: set[int] = set()
    confirmed_amount = 0.0
    for item in out["items"]:
        if item.get("status") != "confirmed":
            continue
        doc_id = int(item.get("related_document_id") or 0)
        if doc_id:
            if doc_id in seen:
                continue
            seen.add(doc_id)
        confirmed_amount += float(item.get("document_total") or 0)
    assert confirmed_amount == 150000.0
