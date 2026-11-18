"""Migración 060: check_declared en delivery_collections.method.

Contenido SQL + métodos del servicio siempre.
PG aislado solo con SIMPLE_PLANNING_TEST_DSN (nunca prod).
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SQL_060 = (
    ROOT
    / "backend"
    / "sql"
    / "distribuidora"
    / "060_delivery_check_declared_AWAITING_APPROVAL.sql"
)
DSN = (os.getenv("SIMPLE_PLANNING_TEST_DSN") or "").strip()


def test_060_sql_content_audit():
    text = SQL_060.read_text(encoding="utf-8")
    assert "AWAITING" in text.upper()
    assert "check_declared" in text
    assert "delivery_collections_method_check" in text
    assert "DROP CONSTRAINT IF EXISTS delivery_collections_method_check" in text
    for m in (
        "cash",
        "transfer_declared",
        "cash_and_transfer",
        "check_declared",
        "pending",
        "not_applicable",
        "credit",
    ):
        assert f"'{m}'" in text


def test_driver_payment_methods_include_check():
    from backend.services.distribuidora.delivery_visit_service import (
        COLLECTION_METHODS,
        DRIVER_PAYMENT_METHODS,
    )

    assert "check_declared" in DRIVER_PAYMENT_METHODS
    assert "check_declared" in COLLECTION_METHODS
    assert "credit" not in DRIVER_PAYMENT_METHODS


@pytest.mark.skipif(not DSN, reason="SIMPLE_PLANNING_TEST_DSN no definido")
def test_060_check_constraint_on_isolated_pg():
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
        cur.execute("DROP TABLE IF EXISTS distribuidora.delivery_collections CASCADE")
        cur.execute(
            """
            CREATE TABLE distribuidora.delivery_collections (
                id BIGSERIAL PRIMARY KEY,
                visit_id BIGINT,
                document_result_id BIGINT,
                method TEXT NOT NULL,
                declared_amount NUMERIC,
                pending_balance NUMERIC,
                document_amount NUMERIC,
                note TEXT,
                created_by BIGINT
            )
            """
        )
        # Estado previo estilo 059
        cur.execute(
            """
            ALTER TABLE distribuidora.delivery_collections
            ADD CONSTRAINT delivery_collections_method_check
            CHECK (method IN (
                'cash','transfer_declared','transfer_verified','credit',
                'partial','pending','not_applicable','cash_and_transfer'
            ))
            """
        )
        conn.commit()

        # Sin 060, check_declared debe fallar
        with pytest.raises(Exception):
            cur.execute(
                """
                INSERT INTO distribuidora.delivery_collections
                    (visit_id, document_result_id, method, declared_amount)
                VALUES (1, 1, 'check_declared', NULL)
                """
            )
        conn.rollback()

        cur.execute(SQL_060.read_text(encoding="utf-8"))
        conn.commit()

        cur.execute(
            """
            INSERT INTO distribuidora.delivery_collections
                (visit_id, document_result_id, method, declared_amount)
            VALUES (1, 1, 'check_declared', NULL)
            RETURNING method, declared_amount
            """
        )
        row = cur.fetchone()
        assert row["method"] == "check_declared"
        assert row["declared_amount"] is None
        cur.close()
    finally:
        conn.close()
