"""Migración 058 + integridad void (unitario + PG aislado opcional).

PG aislado: SIMPLE_PLANNING_TEST_DSN (nunca analytics/prod).
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from backend.repositories.distribuidora import delivery_visit_repo as repo
from backend.repositories.distribuidora import sync_repo
from backend.services.distribuidora import delivery_visit_service as svc
from backend.services.distribuidora import dispatch_plan_pwa_driver_read_service as read_svc

ROOT = Path(__file__).resolve().parents[2]
SQL_056 = ROOT / "backend" / "sql" / "distribuidora" / "056_delivery_visits_evidence.sql"
SQL_057 = ROOT / "backend" / "sql" / "distribuidora" / "057_delivery_visit_closure_guards.sql"
SQL_058 = ROOT / "backend" / "sql" / "distribuidora" / "058_delivery_visit_admin_void.sql"

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


def test_058_registered_in_schema_files():
    assert "058_delivery_visit_admin_void.sql" in sync_repo.DISTRIBUIDORA_SCHEMA_FILES
    assert sync_repo.DISTRIBUIDORA_SCHEMA_FILES.index(
        "057_delivery_visit_closure_guards.sql"
    ) < sync_repo.DISTRIBUIDORA_SCHEMA_FILES.index("058_delivery_visit_admin_void.sql")


def test_058_sql_has_void_integrity_check():
    text = SQL_058.read_text(encoding="utf-8")
    assert "ck_delivery_visits_admin_void_integrity" in text
    assert "voided_by_user_id > 0" in text
    assert "length(btrim(void_reason)) > 0" in text
    assert "admin_classification <> 'voided_admin'" in text


def test_uq_active_index_definition_unchanged_in_056():
    text = SQL_056.read_text(encoding="utf-8")
    assert "uq_delivery_visit_active" in text
    assert "WHERE status = 'in_progress'" in text
    # 058 no debe debilitar el índice
    text058 = SQL_058.read_text(encoding="utf-8")
    assert "uq_delivery_visit_active" not in text058 or "DROP INDEX" not in text058


def test_incomplete_void_not_operational_exclusion():
    """voided_admin sin actor/motivo NO libera bloqueo (ni helper ni atención)."""
    incomplete = {
        "id": 1,
        "status": "partial",
        "ended_at": "2026-11-10T15:00:00+00:00",
        "admin_classification": "voided_admin",
        "voided_at": "2026-11-10T16:00:00+00:00",
        "voided_by_user_id": None,
        "void_reason": None,
    }
    assert not repo.is_admin_voided_row(incomplete)
    assert svc.is_valid_closed_visit(incomplete)

    rows = [("id:3575", 1, "partial", "2026-11-10", "voided_admin", "2026-11-10", None, None)]
    # Sin actor/motivo → sigue contando operativamente
    out = read_svc.resolve_visit_attention_rows(rows)
    assert out["id:3575"]["attention_status"] == "partial"


def test_pilot_and_duplicate_labels_informative_only():
    for cls in ("pilot_test", "duplicate_pending_review"):
        row = {
            "id": 1,
            "status": "partial",
            "ended_at": "2026-11-10T15:00:00+00:00",
            "admin_classification": cls,
            "voided_at": None,
            "voided_by_user_id": None,
            "void_reason": None,
        }
        assert not repo.is_admin_voided_row(row)
        assert svc.is_valid_closed_visit(row)


def test_complete_void_excludes_and_labels():
    row = {
        "id": 1,
        "status": "partial",
        "ended_at": "2026-11-10T15:00:00+00:00",
        "company_id": 3,
        "dispatch_plan_id": 82,
        "customer_key": "id:3575",
        "driver_user_id": 1,
        "admin_classification": "voided_admin",
        "voided_at": "2026-11-10T16:00:00+00:00",
        "voided_by_user_id": 9,
        "void_reason": "piloto",
        "void_ticket_ref": "T1",
        "version": 1,
    }
    assert repo.is_admin_voided_row(row)
    assert not svc.is_valid_closed_visit(row)
    pub = svc._visit_public(row)
    assert pub["history_label_es"] == "ANULADA"
    assert pub["admin_voided"] is True


def test_attention_complete_void_drops_operational_state():
    rows = [
        (
            "id:3575",
            1,
            "partial",
            "2026-11-10",
            "voided_admin",
            "2026-11-10",
            9,
            "piloto",
        ),
        (
            "id:3575",
            3,
            "cancelled",
            "2026-11-10",
            "voided_admin",
            "2026-11-10",
            9,
            "piloto",
        ),
    ]
    assert "id:3575" not in read_svc.resolve_visit_attention_rows(rows)


@pytest.mark.skipif(not DSN, reason="SIMPLE_PLANNING_TEST_DSN no definido")
def test_058_applies_over_057_and_rejects_incomplete_void_on_isolated_pg():
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
                # omit comment-only
                if any(
                    ln.strip() and not ln.strip().startswith("--")
                    for ln in stmt.splitlines()
                ):
                    cur.execute(stmt)
        conn.commit()

        assert repo.admin_void_columns_ready(cur)

        cur.execute(
            """
            INSERT INTO distribuidora.dispatch_plan (id, planning_code)
            VALUES (82, 'PLAN-00082')
            """
        )
        cur.execute(
            """
            INSERT INTO distribuidora.delivery_visits (
                id, company_id, dispatch_plan_id, customer_key, client_id,
                driver_user_id, status, started_at, ended_at, version
            ) VALUES
            (1, 3, 82, 'id:3575', 3575, 1, 'partial', NOW() - interval '2 hour', NOW() - interval '1 hour', 2),
            (3, 3, 82, 'id:3575', 3575, 1, 'in_progress', NOW() - interval '30 minute', NULL, 1)
            """
        )
        conn.commit()

        # pilot_test solo: permitido por CHECK, sigue bloqueando
        cur.execute(
            """
            UPDATE distribuidora.delivery_visits
            SET admin_classification = 'pilot_test'
            WHERE id = 1
            """
        )
        conn.commit()
        closed = repo.get_valid_closed_visit(cur, plan_id=82, customer_key="id:3575")
        assert closed is not None and int(closed["id"]) == 1

        # void incompleto rechazado por CHECK
        with pytest.raises(Exception):
            cur.execute(
                """
                UPDATE distribuidora.delivery_visits
                SET admin_classification = 'voided_admin',
                    voided_at = NOW()
                WHERE id = 1
                """
            )
            conn.commit()
        conn.rollback()

        # void completo #1 + cancel #3 en una TX
        cur.execute("BEGIN")
        cur.execute(
            """
            UPDATE distribuidora.delivery_visits
            SET admin_classification = 'voided_admin',
                voided_at = NOW(),
                voided_by_user_id = 99,
                void_reason = 'piloto aislado',
                void_ticket_ref = 'TEST'
            WHERE id = 1 AND status = 'partial'
            """
        )
        cur.execute(
            """
            UPDATE distribuidora.delivery_visits
            SET status = 'cancelled',
                admin_classification = 'voided_admin',
                voided_at = NOW(),
                voided_by_user_id = 99,
                void_reason = 'piloto aislado abierta',
                void_ticket_ref = 'TEST'
            WHERE id = 3 AND status = 'in_progress'
            """
        )
        cur.execute("COMMIT")

        assert repo.get_valid_closed_visit(cur, plan_id=82, customer_key="id:3575") is None
        assert repo.get_active_visit(cur, plan_id=82, customer_key="id:3575") is None

        # Nueva visita activa permitida por índice único
        cur.execute(
            """
            INSERT INTO distribuidora.delivery_visits (
                company_id, dispatch_plan_id, customer_key, client_id,
                driver_user_id, status, version
            ) VALUES (3, 82, 'id:3575', 3575, 1, 'in_progress', 1)
            RETURNING id
            """
        )
        new_id = int(cur.fetchone()["id"])
        conn.commit()
        assert new_id > 3

        # Doble in_progress rechazado
        with pytest.raises(Exception):
            cur.execute(
                """
                INSERT INTO distribuidora.delivery_visits (
                    company_id, dispatch_plan_id, customer_key, client_id,
                    driver_user_id, status, version
                ) VALUES (3, 82, 'id:3575', 3575, 1, 'in_progress', 1)
                """
            )
            conn.commit()
        conn.rollback()
    finally:
        conn.close()
