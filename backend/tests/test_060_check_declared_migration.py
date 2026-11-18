"""Migración 060: check_declared en delivery_collections.method.

Contenido SQL + métodos del servicio siempre.
PG aislado con SIMPLE_PLANNING_TEST_DSN (nunca prod): aplica 056→060,
idempotencia, históricos con montos, declared_amount NULL.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SQL_056 = ROOT / "backend" / "sql" / "distribuidora" / "056_delivery_visits_evidence.sql"
SQL_057 = ROOT / "backend" / "sql" / "distribuidora" / "057_delivery_visit_closure_guards.sql"
SQL_058 = ROOT / "backend" / "sql" / "distribuidora" / "058_delivery_visit_admin_void.sql"
SQL_059 = (
    ROOT
    / "backend"
    / "sql"
    / "distribuidora"
    / "059_delivery_payment_declaration_AWAITING_APPROVAL.sql"
)
SQL_060 = (
    ROOT
    / "backend"
    / "sql"
    / "distribuidora"
    / "060_delivery_check_declared_AWAITING_APPROVAL.sql"
)
DSN = (os.getenv("SIMPLE_PLANNING_TEST_DSN") or "").strip()


def _strip_tx(sql: str) -> str:
    return "\n".join(
        ln
        for ln in sql.splitlines()
        if ln.strip().upper() not in ("BEGIN;", "COMMIT;", "BEGIN", "COMMIT")
    )


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


def test_060_sql_content_audit():
    text = SQL_060.read_text(encoding="utf-8")
    assert "AWAITING" in text.upper()
    assert "check_declared" in text
    assert "delivery_collections_method_check" in text
    assert "DROP CONSTRAINT IF EXISTS delivery_collections_method_check" in text
    assert "cash_and_transfer" in text
    for m in (
        "cash",
        "transfer_declared",
        "transfer_verified",
        "credit",
        "partial",
        "pending",
        "not_applicable",
        "cash_and_transfer",
        "check_declared",
    ):
        assert f"'{m}'" in text
    assert "Bsale" in text or "bsale" in text.lower() or "No toca Bsale" in text


def test_driver_payment_methods_include_check():
    from backend.services.distribuidora.delivery_visit_service import (
        COLLECTION_METHODS,
        DRIVER_PAYMENT_METHODS,
    )

    assert "check_declared" in DRIVER_PAYMENT_METHODS
    assert "check_declared" in COLLECTION_METHODS
    assert "credit" not in DRIVER_PAYMENT_METHODS


@pytest.mark.skipif(not DSN, reason="SIMPLE_PLANNING_TEST_DSN no definido")
def test_060_isolated_pg_apply_repeat_history_null_amount():
    import psycopg2
    from psycopg2.extras import RealDictCursor

    low = DSN.lower()
    for b in ("analytics", "quillotana", "prod", "coolify", "neon.tech", "supabase"):
        if b in low:
            raise RuntimeError(f"DSN sospechoso: {b}")

    try:
        conn = psycopg2.connect(DSN, cursor_factory=RealDictCursor)
    except Exception as exc:
        pytest.skip(f"Postgres aislado no disponible: {exc}")

    try:
        cur = conn.cursor()
        cur.execute("CREATE SCHEMA IF NOT EXISTS distribuidora")
        for t in (
            "delivery_report_snapshots",
            "delivery_conformity",
            "delivery_evidence",
            "delivery_collections",
            "delivery_line_results",
            "delivery_document_results",
            "delivery_visit_events",
            "delivery_visits",
            "dispatch_plan",
        ):
            cur.execute(f"DROP TABLE IF EXISTS distribuidora.{t} CASCADE")
        cur.execute(
            """
            CREATE TABLE distribuidora.dispatch_plan (
                id BIGSERIAL PRIMARY KEY,
                status TEXT NOT NULL DEFAULT 'confirmed',
                planning_code TEXT
            )
            """
        )
        conn.commit()

        for path in (SQL_056, SQL_057, SQL_058):
            for stmt in _split_sql(path.read_text(encoding="utf-8")):
                if any(
                    ln.strip() and not ln.strip().startswith("--")
                    for ln in stmt.splitlines()
                ):
                    cur.execute(stmt)
        conn.commit()

        sql059 = _strip_tx(SQL_059.read_text(encoding="utf-8"))
        sql060 = _strip_tx(SQL_060.read_text(encoding="utf-8"))
        cur.execute(sql059)
        conn.commit()

        # Histórico 059: cash_and_transfer + monto legado
        cur.execute(
            "INSERT INTO distribuidora.dispatch_plan (id, planning_code) VALUES (9060, 'PLAN-060')"
        )
        cur.execute(
            """
            INSERT INTO distribuidora.delivery_visits (
                id, company_id, dispatch_plan_id, customer_key, client_id,
                driver_user_id, status, version
            ) VALUES (90601, 3, 9060, 'id:9060', 9060, 1, 'completed', 2)
            """
        )
        cur.execute(
            """
            INSERT INTO distribuidora.delivery_document_results
                (id, visit_id, related_document_id, document_number, result_status, document_total)
            VALUES
                (906001, 90601, 1, 2001, 'full', 50000),
                (906002, 90601, 2, 2002, 'full', 30000)
            """
        )
        cur.execute(
            """
            INSERT INTO distribuidora.delivery_collections
                (visit_id, document_result_id, method, document_amount,
                 declared_amount, pending_balance, created_by_user_id)
            VALUES
                (90601, 906001, 'cash', 50000, 50000, 0, 1),
                (90601, 906002, 'cash_and_transfer', 30000, NULL, NULL, 1)
            """
        )
        conn.commit()

        # Sin 060, check_declared debe fallar
        with pytest.raises(Exception):
            cur.execute(
                """
                INSERT INTO distribuidora.delivery_collections
                    (visit_id, document_result_id, method, declared_amount, created_by_user_id)
                VALUES (90601, 906002, 'check_declared', NULL, 1)
                """
            )
        conn.rollback()

        # 060 dos veces (idempotente)
        for _ in range(2):
            cur.execute(sql060)
        conn.commit()

        # Históricos intactos
        cur.execute(
            """
            SELECT method, declared_amount, pending_balance
            FROM distribuidora.delivery_collections
            WHERE document_result_id = 906001
            """
        )
        hist = cur.fetchone()
        assert hist["method"] == "cash"
        assert float(hist["declared_amount"]) == 50000.0

        cur.execute(
            """
            SELECT method, declared_amount, pending_balance
            FROM distribuidora.delivery_collections
            WHERE document_result_id = 906002
            """
        )
        mix = cur.fetchone()
        assert mix["method"] == "cash_and_transfer"
        assert mix["declared_amount"] is None
        assert mix["pending_balance"] is None

        # Visita cerrada no se altera
        cur.execute(
            "SELECT status, version FROM distribuidora.delivery_visits WHERE id = 90601"
        )
        v = cur.fetchone()
        assert v["status"] == "completed"
        assert int(v["version"]) == 2

        # Nuevo cheque declarado con montos NULL
        cur.execute(
            "DELETE FROM distribuidora.delivery_collections WHERE document_result_id = 906002"
        )
        cur.execute(
            """
            INSERT INTO distribuidora.delivery_collections
                (visit_id, document_result_id, method, document_amount,
                 declared_amount, pending_balance, created_by_user_id)
            VALUES (90601, 906002, 'check_declared', NULL, NULL, NULL, 1)
            RETURNING method, declared_amount, pending_balance
            """
        )
        row = cur.fetchone()
        assert row["method"] == "check_declared"
        assert row["declared_amount"] is None
        assert row["pending_balance"] is None
        conn.commit()

        # Método inventado sigue rechazado
        with pytest.raises(Exception):
            cur.execute(
                """
                INSERT INTO distribuidora.delivery_collections
                    (visit_id, method, created_by_user_id)
                VALUES (90601, 'bank_verified_fake', 1)
                """
            )
        conn.rollback()

        # Rollback de transacción de prueba no destruye constraint
        cur.execute("BEGIN")
        cur.execute(
            """
            INSERT INTO distribuidora.delivery_collections
                (visit_id, method, declared_amount, created_by_user_id)
            VALUES (90601, 'pending', NULL, 1)
            """
        )
        cur.execute("ROLLBACK")
        cur.execute(
            """
            SELECT 1
            FROM pg_constraint
            WHERE conname = 'delivery_collections_method_check'
            """
        )
        assert cur.fetchone() is not None
        cur.close()
    finally:
        try:
            conn.close()
        except Exception:
            pass
