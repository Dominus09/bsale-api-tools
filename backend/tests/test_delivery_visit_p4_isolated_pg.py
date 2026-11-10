"""P4: visitas en Postgres aislado (nunca analytics).

Requiere SIMPLE_PLANNING_TEST_DSN.
"""

from __future__ import annotations

import os
from pathlib import Path
import pytest
from fastapi import HTTPException

from backend.repositories.distribuidora import delivery_visit_repo as visit_repo
from backend.services.distribuidora import delivery_visit_service as svc

DSN = (os.getenv("SIMPLE_PLANNING_TEST_DSN") or "").strip()
ROOT = Path(__file__).resolve().parents[2]
SQL_056 = ROOT / "backend" / "sql" / "distribuidora" / "056_delivery_visits_evidence.sql"
SQL_057 = ROOT / "backend" / "sql" / "distribuidora" / "057_delivery_visit_closure_guards.sql"

pytestmark = pytest.mark.skipif(
    not DSN,
    reason="Define SIMPLE_PLANNING_TEST_DSN a Postgres de prueba aislado (nunca analytics)",
)


def _assert_isolated(uri: str) -> None:
    low = uri.lower()
    for b in ("analytics", "quillotana", "prod", "coolify", "neon.tech", "supabase"):
        if b in low:
            raise RuntimeError(f"DSN sospechoso: {b}")


def _connect():
    import psycopg2
    from psycopg2.extras import RealDictCursor

    _assert_isolated(DSN)
    return psycopg2.connect(DSN, cursor_factory=RealDictCursor)


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
    for t in (
        "delivery_report_snapshots",
        "delivery_conformity",
        "delivery_evidence",
        "delivery_collections",
        "delivery_line_results",
        "delivery_document_results",
        "delivery_visit_events",
        "delivery_visits",
        "document_details",
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
    cur.execute(
        """
        CREATE TABLE distribuidora.document_details (
            detail_id BIGINT PRIMARY KEY,
            document_id BIGINT NOT NULL,
            line_number INT,
            variant_id BIGINT,
            variant_description TEXT,
            variant_code TEXT,
            quantity NUMERIC(18, 4)
        )
        """
    )
    conn.commit()
    for path in (SQL_056, SQL_057):
        raw = path.read_text(encoding="utf-8")
        for stmt in _split_sql(raw):
            cur.execute(stmt)
    conn.commit()
    cur.execute(
        "INSERT INTO distribuidora.dispatch_plan (id, planning_code) VALUES (10, 'PLAN-TEST-P4')"
    )
    # Documento 555 con dos líneas mismo variant (identidad distinta)
    cur.execute(
        """
        INSERT INTO distribuidora.document_details
            (detail_id, document_id, line_number, variant_id, variant_description, quantity)
        VALUES
            (88001, 555, 1, 1, 'Prod A', 4),
            (88002, 555, 2, 1, 'Prod A', 2)
        """
    )
    conn.commit()
    cur.close()


@pytest.fixture()
def isolated_db(monkeypatch):
    conn = _connect()
    _bootstrap(conn)
    conn.close()

    def fake_get_connection():
        return _connect()

    monkeypatch.setattr(svc, "get_connection", fake_get_connection)
    yield
    # cleanup soft
    c = _connect()
    cur = c.cursor()
    cur.execute("DROP TABLE IF EXISTS distribuidora.delivery_visits CASCADE")
    c.commit()
    cur.close()
    c.close()


def _user():
    return {"id": 42, "email": "chofer@test.local"}


def test_start_resume_finalize_idempotent(isolated_db, monkeypatch):
    monkeypatch.setattr(
        svc.read_svc, "enforce_driver_plan_read_access", lambda _u, _p: 42
    )
    monkeypatch.setattr(
        svc.loc_svc,
        "_resolve_client",
        lambda _p, _k: {"client_id": 9001, "customer_key": "id:9001", "client_name": "A"},
    )
    monkeypatch.setattr(svc.loc_svc, "_parse_client_id", lambda _k, _c: 9001)
    monkeypatch.setattr(
        svc.loc_svc, "_authorized_company_id_for_plan", lambda _u, _p: 3
    )

    first = svc.start_visit(
        user=_user(),
        plan_id=10,
        customer_key="id:9001",
        latitude=-42.47,
        longitude=-73.76,
        accuracy_m=12.5,
        gps_captured_at="2026-03-15T18:00:00Z",
        gps_source="device",
        idempotency_key="idem-start-1",
    )
    assert first["created"] is True
    assert first["visit"]["status"] == "in_progress"
    visit_id = first["visit"]["id"]
    version = first["visit"]["version"]

    second = svc.start_visit(
        user=_user(),
        plan_id=10,
        customer_key="id:9001",
        latitude=-42.47,
        longitude=-73.76,
        accuracy_m=20.0,
        gps_captured_at="2026-03-15T18:05:00Z",
        idempotency_key="idem-start-2",
    )
    assert second["created"] is False
    assert second["resumed"] is True
    assert second["visit"]["id"] == visit_id

    replay = svc.start_visit(
        user=_user(),
        plan_id=10,
        customer_key="id:9001",
        latitude=-42.47,
        longitude=-73.76,
        accuracy_m=20.0,
        idempotency_key="idem-start-1",
    )
    assert replay["idempotent_replay"] is True
    assert replay["visit"]["id"] == visit_id

    # Cierre "completed" ahora exige líneas + conformidad (probado en otro test).
    # Aquí validamos cancelación simple / no delivered path no aplica; usamos cancel.
    closed = svc.finalize_visit(
        user=_user(),
        plan_id=10,
        visit_id=visit_id,
        expected_version=version,
        status="cancelled",
        latitude=-42.47,
        longitude=-73.76,
        accuracy_m=15.0,
        gps_captured_at="2026-03-15T19:00:00Z",
    )
    assert closed["finalized"] is True
    assert closed["persisted"] is True
    assert closed["visit"]["status"] == "cancelled"

    again = svc.finalize_visit(
        user=_user(),
        plan_id=10,
        visit_id=visit_id,
        expected_version=version,
        status="cancelled",
    )
    assert again["idempotent_replay"] is True
    assert again["finalized"] is False


def test_start_without_gps_requires_exception(isolated_db, monkeypatch):
    monkeypatch.setattr(
        svc.read_svc, "enforce_driver_plan_read_access", lambda _u, _p: 42
    )
    monkeypatch.setattr(
        svc.loc_svc,
        "_resolve_client",
        lambda _p, _k: {"client_id": 9002, "customer_key": "id:9002"},
    )
    monkeypatch.setattr(svc.loc_svc, "_parse_client_id", lambda _k, _c: 9002)
    monkeypatch.setattr(
        svc.loc_svc, "_authorized_company_id_for_plan", lambda _u, _p: 3
    )

    with pytest.raises(HTTPException) as ei:
        svc.start_visit(
            user=_user(),
            plan_id=10,
            customer_key="id:9002",
        )
    assert ei.value.status_code == 400

    ok = svc.start_visit(
        user=_user(),
        plan_id=10,
        customer_key="id:9002",
        gps_exception_code="permission_denied",
        gps_exception_note="Usuario denegó geolocalización",
    )
    assert ok["created"] is True
    assert ok["visit"]["start_gps"]["lat"] is None
    assert ok["visit"]["gps_exception_code"] == "permission_denied"


def test_name_key_blocked(isolated_db, monkeypatch):
    monkeypatch.setattr(
        svc.read_svc, "enforce_driver_plan_read_access", lambda _u, _p: 42
    )
    monkeypatch.setattr(
        svc.loc_svc,
        "_resolve_client",
        lambda _p, _k: {"client_id": None, "customer_key": "name:sin-id"},
    )

    def _raise(_k, _c):
        raise HTTPException(status_code=422, detail="sin id")

    monkeypatch.setattr(svc.loc_svc, "_parse_client_id", _raise)
    with pytest.raises(HTTPException) as ei:
        svc.start_visit(
            user=_user(),
            plan_id=10,
            customer_key="name:sin-id",
            latitude=1.0,
            longitude=1.0,
        )
    assert ei.value.status_code == 422


def test_version_conflict(isolated_db, monkeypatch):
    monkeypatch.setattr(
        svc.read_svc, "enforce_driver_plan_read_access", lambda _u, _p: 42
    )
    monkeypatch.setattr(
        svc.loc_svc,
        "_resolve_client",
        lambda _p, _k: {"client_id": 9003, "customer_key": "id:9003"},
    )
    monkeypatch.setattr(svc.loc_svc, "_parse_client_id", lambda _k, _c: 9003)
    monkeypatch.setattr(
        svc.loc_svc, "_authorized_company_id_for_plan", lambda _u, _p: 3
    )

    started = svc.start_visit(
        user=_user(),
        plan_id=10,
        customer_key="id:9003",
        latitude=-41.0,
        longitude=-73.0,
        accuracy_m=8.0,
    )
    visit_id = started["visit"]["id"]

    with pytest.raises(HTTPException) as ei:
        svc.finalize_visit(
            user=_user(),
            plan_id=10,
            visit_id=visit_id,
            expected_version=99,
            status="cancelled",
        )
    assert ei.value.status_code == 409


def test_tables_ready_realdict(isolated_db):
    conn = _connect()
    cur = conn.cursor()
    assert visit_repo.tables_ready(cur) is True
    cur.close()
    conn.close()


def test_lines_collection_and_report(isolated_db, monkeypatch):
    monkeypatch.setattr(
        svc.read_svc, "enforce_driver_plan_read_access", lambda _u, _p: 42
    )
    monkeypatch.setattr(
        svc.loc_svc,
        "_resolve_client",
        lambda _p, _k: {"client_id": 9010, "customer_key": "id:9010"},
    )
    monkeypatch.setattr(svc.loc_svc, "_parse_client_id", lambda _k, _c: 9010)
    monkeypatch.setattr(
        svc.loc_svc, "_authorized_company_id_for_plan", lambda _u, _p: 3
    )

    started = svc.start_visit(
        user=_user(),
        plan_id=10,
        customer_key="id:9010",
        latitude=-42.0,
        longitude=-73.0,
        accuracy_m=10.0,
    )
    vid = started["visit"]["id"]
    ver = started["visit"]["version"]

    saved = svc.save_document_lines(
        user=_user(),
        plan_id=10,
        visit_id=vid,
        related_document_id=555,
        oc_document_id=100,
        document_number=9001,
        document_type="boleta",
        document_total=12000,
        result_status="partial",
        notes=None,
        lines=[
            {
                "source_detail_id": 88001,
                "line_number": 1,
                "variant_id": 1,
                "product_name": "Prod A",
                "planned_units": 4,
                "delivered_units": 3,
                "undelivered_reason": "physical_shortage",
            },
            {
                "source_detail_id": 88002,
                "line_number": 2,
                "variant_id": 1,
                "product_name": "Prod A",
                "planned_units": 2,
                "delivered_units": 2,
            },
        ],
    )
    assert len(saved["lines"]) == 2
    assert {int(x["source_detail_id"]) for x in saved["lines"]} == {88001, 88002}
    assert saved["persisted"] is True
    assert float(saved["lines"][0]["undelivered_units"]) == 3.0 or float(
        saved["lines"][0]["undelivered_units"]
    ) == 1.0

    col = svc.declare_collection(
        user=_user(),
        plan_id=10,
        visit_id=vid,
        document_result_id=int(saved["document_result"]["id"]),
        method="cash",
        document_amount=12000,
        declared_amount=5000,
        note="parcial",
    )
    assert col["persisted"] is True

    # Incidencia: evidencia excepción (sin R2) → pending_review
    exc = svc.register_evidence_exception(
        user=_user(),
        plan_id=10,
        visit_id=vid,
        evidence_kind="closed_store",
        exception_note="Camara no disponible en prueba aislada",
        latitude=-42.0,
        longitude=-73.0,
    )
    assert exc["persisted"] is True

    # No delivered: líneas con entregado>0 bloquearían; limpiamos vía status none
    # Re-guardar líneas en 0 entregado
    svc.save_document_lines(
        user=_user(),
        plan_id=10,
        visit_id=vid,
        related_document_id=555,
        oc_document_id=100,
        document_number=9001,
        document_type="boleta",
        document_total=12000,
        result_status="none",
        notes=None,
        lines=[
            {
                "source_detail_id": 88001,
                "variant_id": 1,
                "product_name": "Prod A",
                "planned_units": 4,
                "delivered_units": 0,
                "undelivered_reason": "physical_shortage",
            },
            {
                "source_detail_id": 88002,
                "variant_id": 1,
                "product_name": "Prod A",
                "planned_units": 2,
                "delivered_units": 0,
                "undelivered_reason": "physical_shortage",
            },
        ],
    )

    closed = svc.finalize_visit(
        user=_user(),
        plan_id=10,
        visit_id=vid,
        expected_version=ver,
        status="not_delivered",
        result_reason="closed_store",
        result_note="Local cerrado prueba",
        latitude=-42.0,
        longitude=-73.0,
    )
    assert closed["finalized"] is True
    assert closed["closure_review_status"] == "pending_review"

    report = svc.build_whatsapp_report(
        user=_user(),
        plan_id=10,
        visit_id=vid,
        client_name="Cliente Test",
        planning_code="PLAN-TEST-P4",
        driver_name="Chofer Test",
    )
    assert "Cliente Test" in report["report_text"]
    assert "LOCAL CERRADO" in report["report_text"] or "CERRADO" in report["header"]
    assert report["share"]["delivery_claimed"] is False
    assert report["share"]["auto_send"] is False
    assert "-42.0" not in report["report_text"]
    assert "group_invite_link" in report["omitted"]


def test_schema_integrity_restrict_and_composites(isolated_db):
    """056: RESTRICT al plan + FK compuestas + columnas 057."""
    conn = _connect()
    cur = conn.cursor()

    def _val(row, key=None):
        if row is None:
            return None
        if isinstance(row, dict) or hasattr(row, "keys"):
            if key is not None:
                return row[key]
            return next(iter(row.values()))
        return row[0] if key is None else row[key]

    tables = [
        "delivery_visits",
        "delivery_visit_events",
        "delivery_document_results",
        "delivery_line_results",
        "delivery_collections",
        "delivery_evidence",
        "delivery_conformity",
        "delivery_report_snapshots",
    ]
    for t in tables:
        cur.execute(
            "SELECT to_regclass(%s) IS NOT NULL AS ok",
            (f"distribuidora.{t}",),
        )
        assert _val(cur.fetchone(), "ok") is True, t

    for col in ("closure_review_status",):
        cur.execute(
            """
            SELECT 1 AS ok FROM information_schema.columns
            WHERE table_schema='distribuidora'
              AND table_name='delivery_visits'
              AND column_name=%s
            """,
            (col,),
        )
        assert cur.fetchone() is not None

    for col in (
        "summary_payload",
        "visit_version_at_sign",
        "invalidated_at",
        "invalidate_reason",
    ):
        cur.execute(
            """
            SELECT 1 AS ok FROM information_schema.columns
            WHERE table_schema='distribuidora'
              AND table_name='delivery_conformity'
              AND column_name=%s
            """,
            (col,),
        )
        assert cur.fetchone() is not None

    cur.execute(
        """
        SELECT 1 AS ok FROM information_schema.columns
        WHERE table_schema='distribuidora'
          AND table_name='delivery_line_results'
          AND column_name='source_detail_id'
        """
    )
    assert cur.fetchone() is not None

    cur.execute(
        """
        INSERT INTO distribuidora.delivery_visits (
            company_id, dispatch_plan_id, customer_key, client_id,
            driver_user_id, status
        ) VALUES (3, 10, 'id:77', 77, 42, 'in_progress')
        RETURNING id
        """
    )
    visit_id = int(_val(cur.fetchone(), "id"))
    conn.commit()

    # Borrar plan con visita debe fallar (RESTRICT)
    with pytest.raises(Exception):
        cur.execute("DELETE FROM distribuidora.dispatch_plan WHERE id = 10")
        conn.commit()
    conn.rollback()

    cur.execute(
        """
        INSERT INTO distribuidora.delivery_document_results (
            visit_id, related_document_id, result_status
        ) VALUES (%s, 1, 'full')
        RETURNING id
        """,
        (visit_id,),
    )
    doc_id = int(_val(cur.fetchone(), "id"))
    conn.commit()

    # Línea con visit_id ajeno debe fallar (FK compuesta)
    cur.execute(
        """
        INSERT INTO distribuidora.delivery_visits (
            company_id, dispatch_plan_id, customer_key, client_id,
            driver_user_id, status
        ) VALUES (3, 10, 'id:78', 78, 42, 'in_progress')
        RETURNING id
        """
    )
    other_visit = int(_val(cur.fetchone(), "id"))
    conn.commit()
    with pytest.raises(Exception):
        cur.execute(
            """
            INSERT INTO distribuidora.delivery_line_results (
                visit_id, document_result_id, source_detail_id,
                planned_units, delivered_units, undelivered_units
            ) VALUES (%s, %s, 1, 1, 1, 0)
            """,
            (other_visit, doc_id),
        )
        conn.commit()
    conn.rollback()

    # Evidencia con company_id distinto debe fallar
    with pytest.raises(Exception):
        cur.execute(
            """
            INSERT INTO distribuidora.delivery_evidence (
                visit_id, company_id, evidence_kind, object_key,
                captured_by_user_id, upload_confirmed
            ) VALUES (%s, 99, 'closed_store', 'k/test', 42, TRUE)
            """,
            (visit_id,),
        )
        conn.commit()
    conn.rollback()

    # Dos líneas mismo variant, distinto source_detail_id: OK
    cur.execute(
        """
        INSERT INTO distribuidora.delivery_line_results (
            visit_id, document_result_id, source_detail_id, variant_id,
            planned_units, delivered_units, undelivered_units
        ) VALUES
            (%s, %s, 1001, 55, 2, 2, 0),
            (%s, %s, 1002, 55, 3, 1, 2)
        """,
        (visit_id, doc_id, visit_id, doc_id),
    )
    conn.commit()

    # Mismo source_detail_id duplicado: NO
    with pytest.raises(Exception):
        cur.execute(
            """
            INSERT INTO distribuidora.delivery_line_results (
                visit_id, document_result_id, source_detail_id, variant_id,
                planned_units, delivered_units, undelivered_units
            ) VALUES (%s, %s, 1001, 55, 1, 1, 0)
            """,
            (visit_id, doc_id),
        )
        conn.commit()
    conn.rollback()

    cur.close()
    conn.close()
