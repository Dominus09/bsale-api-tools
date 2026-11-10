"""Migración 059: cash_and_transfer + declared_amount NULL (PG aislado).

SIMPLE_PLANNING_TEST_DSN — nunca analytics/prod.
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

DSN = (os.getenv("SIMPLE_PLANNING_TEST_DSN") or "").strip()


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


def test_059_sql_content_audit():
    text = SQL_059.read_text(encoding="utf-8")
    assert "AWAITING" in text.upper()
    assert "cash_and_transfer" in text
    assert "delivery_collections_method_check" in text
    assert "DROP CONSTRAINT IF EXISTS delivery_collections_method_check" in text
    for m in (
        "cash",
        "transfer_declared",
        "transfer_verified",
        "credit",
        "partial",
        "pending",
        "not_applicable",
    ):
        assert f"'{m}'" in text


@pytest.mark.skipif(not DSN, reason="SIMPLE_PLANNING_TEST_DSN no definido")
def test_059_applies_and_allows_null_amount_declarative():
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

        # 059 dos veces (idempotente). Sin BEGIN/COMMIT anidados.
        sql059 = "\n".join(
            ln
            for ln in SQL_059.read_text(encoding="utf-8").splitlines()
            if ln.strip().upper() not in ("BEGIN;", "COMMIT;", "BEGIN", "COMMIT")
        )
        for _ in range(2):
            cur.execute(sql059)
        conn.commit()

        cur.execute(
            "INSERT INTO distribuidora.dispatch_plan (id, planning_code) VALUES (9059, 'PLAN-059')"
        )
        cur.execute(
            """
            INSERT INTO distribuidora.delivery_visits (
                id, company_id, dispatch_plan_id, customer_key, client_id,
                driver_user_id, status, version
            ) VALUES (90591, 3, 9059, 'id:9059', 9059, 1, 'in_progress', 1)
            """
        )
        cur.execute(
            """
            INSERT INTO distribuidora.delivery_document_results
                (id, visit_id, related_document_id, document_number, result_status, document_total)
            VALUES
                (905901, 90591, 1, 1001, 'full', 50000),
                (905902, 90591, 2, 1002, 'full', 30000)
            """
        )
        conn.commit()

        # Histórico monetario
        cur.execute(
            """
            INSERT INTO distribuidora.delivery_collections
                (visit_id, document_result_id, method, document_amount,
                 declared_amount, pending_balance, created_by_user_id)
            VALUES (90591, 905901, 'cash', 50000, 50000, 0, 1)
            RETURNING declared_amount
            """
        )
        assert float(cur.fetchone()["declared_amount"]) == 50000.0

        # Declarativo sin monto (multi-doc)
        cur.execute(
            """
            INSERT INTO distribuidora.delivery_collections
                (visit_id, document_result_id, method, document_amount,
                 declared_amount, verified_amount, pending_balance, created_by_user_id)
            VALUES (90591, 905902, 'cash_and_transfer', 30000, NULL, NULL, NULL, 1)
            RETURNING method, declared_amount
            """
        )
        row = cur.fetchone()
        assert row["method"] == "cash_and_transfer"
        assert row["declared_amount"] is None
        conn.commit()

        # Agregación: NULL no cuenta como $0 recaudado
        cur.execute(
            """
            SELECT
              coalesce(sum(declared_amount) FILTER (WHERE declared_amount IS NOT NULL), 0) AS sum_amt,
              count(*) FILTER (WHERE declared_amount IS NULL) AS null_n
            FROM distribuidora.delivery_collections
            WHERE visit_id = 90591
            """
        )
        agg = cur.fetchone()
        assert float(agg["sum_amt"]) == 50000.0
        assert int(agg["null_n"]) == 1

        # not_applicable sin monto
        cur.execute(
            """
            INSERT INTO distribuidora.delivery_collections
                (visit_id, method, declared_amount, created_by_user_id)
            VALUES (90591, 'not_applicable', NULL, 1)
            """
        )
        conn.commit()

        # Método inventado rechazado
        with pytest.raises(Exception):
            cur.execute(
                """
                INSERT INTO distribuidora.delivery_collections
                    (visit_id, method, created_by_user_id)
                VALUES (90591, 'bank_verified_fake', 1)
                """
            )
        conn.rollback()

        # Reemplazo por documento (no duplicar): borrar y reinsertar
        cur.execute(
            "DELETE FROM distribuidora.delivery_collections WHERE document_result_id = 905902"
        )
        cur.execute(
            """
            INSERT INTO distribuidora.delivery_collections
                (visit_id, document_result_id, method, declared_amount, created_by_user_id)
            VALUES (90591, 905902, 'pending', NULL, 1)
            """
        )
        cur.execute(
            """
            SELECT count(*) AS n FROM distribuidora.delivery_collections
            WHERE document_result_id = 905902
            """
        )
        assert int(cur.fetchone()["n"]) == 1
        conn.commit()
    finally:
        try:
            conn.close()
        except Exception:
            pass
