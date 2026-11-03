"""Concurrencia real ORS vs simple_list sobre PostgreSQL aislado.

Requiere ``SIMPLE_PLANNING_TEST_DSN`` apuntando a una BD de prueba
(nunca analytics productiva). Sin DSN → skip (no afirmar concurrencia resuelta).
"""

from __future__ import annotations

import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest

DSN = (os.getenv("SIMPLE_PLANNING_TEST_DSN") or "").strip()

pytestmark = pytest.mark.skipif(
    not DSN,
    reason=(
        "Define SIMPLE_PLANNING_TEST_DSN a un Postgres de prueba aislado "
        "para validar locks concurrentes (no analytics)."
    ),
)


def _connect():
    import psycopg2

    return psycopg2.connect(DSN)


def _setup(conn):
    """Esquema mínimo alineado a ``dispatch_plan_oc_guard`` (no copia analytics)."""
    cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS distribuidora")
    # Recrear tablas sintéticas (BD aislada / efímera).
    cur.execute("DROP TABLE IF EXISTS distribuidora.dispatch_plan_order_events CASCADE")
    cur.execute("DROP TABLE IF EXISTS distribuidora.dispatch_plan_orders CASCADE")
    cur.execute("DROP TABLE IF EXISTS distribuidora.dispatch_plan CASCADE")
    cur.execute("DROP TABLE IF EXISTS distribuidora.documents CASCADE")
    cur.execute(
        """
        CREATE TABLE distribuidora.documents (
            document_id BIGINT PRIMARY KEY,
            source_document_id BIGINT,
            state INTEGER DEFAULT 0
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE distribuidora.dispatch_plan (
            id BIGSERIAL PRIMARY KEY,
            plan_session_id TEXT,
            status TEXT NOT NULL DEFAULT 'planned',
            planning_code TEXT,
            planning_date DATE,
            truck_id INTEGER
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE distribuidora.dispatch_plan_orders (
            id BIGSERIAL PRIMARY KEY,
            dispatch_plan_id BIGINT REFERENCES distribuidora.dispatch_plan(id),
            oc_document_id BIGINT NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE distribuidora.dispatch_plan_order_events (
            id BIGSERIAL PRIMARY KEY,
            dispatch_plan_id BIGINT,
            action TEXT,
            oc_document_id BIGINT,
            payload JSONB DEFAULT '{}'::jsonb
        )
        """
    )
    # Identidad Bsale: 9001 vigente + 9002 reemisión (mismo source).
    cur.execute(
        "INSERT INTO distribuidora.documents(document_id, source_document_id, state) "
        "VALUES (9001, 50001, 0), (9002, 50001, 0)"
    )
    conn.commit()
    cur.close()


def _try_confirm(origin: str) -> dict:
    from backend.services.distribuidora.dispatch_plan_oc_guard import (
        OcConfirmConflictError,
        acquire_oc_confirm_locks,
        assert_documents_active_for_confirm,
        assert_ocs_available_for_confirm,
    )

    conn = _connect()
    conn.autocommit = False
    result = {"origin": origin, "ok": False, "error": None}
    try:
        cur = conn.cursor()
        locked = acquire_oc_confirm_locks(cur, [9001])
        assert_documents_active_for_confirm(cur, locked)
        assert_ocs_available_for_confirm(cur, locked)
        cur.execute(
            """
            INSERT INTO distribuidora.dispatch_plan(plan_session_id, status, planning_code)
            VALUES (%s, 'planned', %s)
            RETURNING id
            """,
            (f"{origin}-sess", f"{origin}-PLAN"),
        )
        plan_id = int(cur.fetchone()[0])
        cur.execute(
            """
            INSERT INTO distribuidora.dispatch_plan_orders(dispatch_plan_id, oc_document_id)
            VALUES (%s, 9001)
            """,
            (plan_id,),
        )
        # Mantener la TX un instante para forzar contención del advisory lock.
        threading.Event().wait(0.15)
        conn.commit()
        result["ok"] = True
        result["plan_id"] = plan_id
    except OcConfirmConflictError as exc:
        conn.rollback()
        result["error"] = str(exc)
        result["code"] = getattr(exc, "code", "conflict")
    except Exception as exc:
        conn.rollback()
        result["error"] = str(exc)
    finally:
        conn.close()
    return result


def test_concurrent_ors_and_simple_only_one_wins():
    setup_conn = _connect()
    try:
        _setup(setup_conn)
    finally:
        setup_conn.close()

    barrier = threading.Barrier(2)

    def _run(origin: str):
        barrier.wait(timeout=5)
        return _try_confirm(origin)

    with ThreadPoolExecutor(max_workers=2) as pool:
        futs = [
            pool.submit(_run, "ors"),
            pool.submit(_run, "simple_list"),
        ]
        outcomes = [f.result(timeout=30) for f in as_completed(futs)]

    oks = [o for o in outcomes if o["ok"]]
    fails = [o for o in outcomes if not o["ok"]]
    assert len(oks) == 1, outcomes
    assert len(fails) == 1, outcomes
    assert fails[0].get("code") == "oc_plan_conflict" or (
        fails[0].get("error") and "ya está en plan" in str(fails[0].get("error"))
    ), fails

    verify = _connect()
    try:
        cur = verify.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM distribuidora.dispatch_plan_orders WHERE oc_document_id = 9001"
        )
        assert int(cur.fetchone()[0]) == 1
        # Rollback del perdedor: un solo plan persistido (sin filas parciales huérfanas).
        cur.execute("SELECT COUNT(*) FROM distribuidora.dispatch_plan")
        assert int(cur.fetchone()[0]) == 1
        # Reintento idempotente del ganador: mismo origen vuelve a chocar (OC ya planificada).
        winner_origin = oks[0]["origin"]
        retry = _try_confirm(f"{winner_origin}-retry")
        assert retry["ok"] is False
        assert retry.get("code") == "oc_plan_conflict" or "ya está en plan" in str(
            retry.get("error")
        )
        cur.execute("SELECT COUNT(*) FROM distribuidora.dispatch_plan")
        assert int(cur.fetchone()[0]) == 1
        # Reemisión Bsale (mismo source): bloquear identidad expandida 9002.
        reissue = _try_confirm_doc("reissue", 9002)
        assert reissue["ok"] is False, reissue
        cur.execute("SELECT COUNT(*) FROM distribuidora.dispatch_plan_orders")
        assert int(cur.fetchone()[0]) == 1
        # Sin locks permanentes: nueva TX puede tomar advisory lock de otra OC.
        cur.execute("SELECT pg_advisory_xact_lock(%s, %s)", (192_718_050, 9003 & 0x7FFFFFFF))
        verify.commit()
        cur.close()
    finally:
        verify.close()


def _try_confirm_doc(origin: str, document_id: int) -> dict:
    from backend.services.distribuidora.dispatch_plan_oc_guard import (
        OcConfirmConflictError,
        acquire_oc_confirm_locks,
        assert_documents_active_for_confirm,
        assert_ocs_available_for_confirm,
    )

    conn = _connect()
    conn.autocommit = False
    result = {"origin": origin, "ok": False, "error": None, "document_id": document_id}
    try:
        cur = conn.cursor()
        locked = acquire_oc_confirm_locks(cur, [document_id])
        assert_documents_active_for_confirm(cur, locked)
        assert_ocs_available_for_confirm(cur, locked)
        cur.execute(
            """
            INSERT INTO distribuidora.dispatch_plan(plan_session_id, status, planning_code)
            VALUES (%s, 'planned', %s)
            RETURNING id
            """,
            (f"{origin}-sess", f"{origin}-PLAN"),
        )
        plan_id = int(cur.fetchone()[0])
        cur.execute(
            """
            INSERT INTO distribuidora.dispatch_plan_orders(dispatch_plan_id, oc_document_id)
            VALUES (%s, %s)
            """,
            (plan_id, int(document_id)),
        )
        conn.commit()
        result["ok"] = True
        result["plan_id"] = plan_id
    except OcConfirmConflictError as exc:
        conn.rollback()
        result["error"] = str(exc)
        result["code"] = getattr(exc, "code", "conflict")
    except Exception as exc:
        conn.rollback()
        result["error"] = str(exc)
    finally:
        conn.close()
    return result
