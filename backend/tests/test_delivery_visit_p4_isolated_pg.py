"""P4: visitas en Postgres aislado (nunca analytics).

Requiere SIMPLE_PLANNING_TEST_DSN.
"""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import patch

import pytest
from fastapi import HTTPException

from backend.repositories.distribuidora import delivery_visit_repo as visit_repo
from backend.services.distribuidora import delivery_visit_service as svc

DSN = (os.getenv("SIMPLE_PLANNING_TEST_DSN") or "").strip()
ROOT = Path(__file__).resolve().parents[2]
SQL_056 = ROOT / "backend" / "sql" / "distribuidora" / "056_delivery_visits_evidence.sql"

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
    raw = SQL_056.read_text(encoding="utf-8")
    for stmt in _split_sql(raw):
        cur.execute(stmt)
    conn.commit()
    cur.execute(
        "INSERT INTO distribuidora.dispatch_plan (id, planning_code) VALUES (10, 'PLAN-TEST-P4')"
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

    closed = svc.finalize_visit(
        user=_user(),
        plan_id=10,
        visit_id=visit_id,
        expected_version=version,
        status="completed",
        latitude=-42.47,
        longitude=-73.76,
        accuracy_m=15.0,
        gps_captured_at="2026-03-15T19:00:00Z",
    )
    assert closed["finalized"] is True
    assert closed["persisted"] is True
    assert closed["visit"]["status"] == "completed"

    again = svc.finalize_visit(
        user=_user(),
        plan_id=10,
        visit_id=visit_id,
        expected_version=version,
        status="completed",
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
            status="partial",
        )
    assert ei.value.status_code == 409


def test_tables_ready_realdict(isolated_db):
    conn = _connect()
    cur = conn.cursor()
    assert visit_repo.tables_ready(cur) is True
    cur.close()
    conn.close()
