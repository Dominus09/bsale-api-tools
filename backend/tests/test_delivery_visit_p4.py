"""P4: reglas de visita sin DB (validaciones / status)."""

from __future__ import annotations

import pytest
from fastapi import HTTPException

from backend.services.distribuidora import delivery_visit_service as svc


def test_schema_status_shape(monkeypatch):
    class FakeCur:
        def execute(self, *_a, **_k):
            return None

        def fetchone(self):
            return {"tables_ready": False}

        def close(self):
            return None

    class FakeConn:
        def cursor(self):
            return FakeCur()

        def close(self):
            return None

    monkeypatch.setattr(svc, "get_connection", lambda: FakeConn())
    monkeypatch.setattr(
        "backend.repositories.distribuidora.delivery_visit_repo.tables_ready",
        lambda _cur: False,
    )
    st = svc.schema_status()
    assert st["migration"] == "056_delivery_visits_evidence.sql"
    assert st["mode"] == "online_only"
    assert st["tables_ready"] is False
    assert "offline" in st["note_es"].lower() or "online" in st["note_es"].lower()


def test_finalize_rejects_bad_status(monkeypatch):
    monkeypatch.setattr(
        svc.read_svc,
        "enforce_driver_plan_read_access",
        lambda _u, _p: 7,
    )
    with pytest.raises(HTTPException) as ei:
        svc.finalize_visit(
            user={"id": 7},
            plan_id=1,
            visit_id=1,
            expected_version=1,
            status="weird",
        )
    assert ei.value.status_code == 400


def test_finalize_not_delivered_requires_reason(monkeypatch):
    monkeypatch.setattr(
        svc.read_svc,
        "enforce_driver_plan_read_access",
        lambda _u, _p: 7,
    )
    with pytest.raises(HTTPException) as ei:
        svc.finalize_visit(
            user={"id": 7},
            plan_id=1,
            visit_id=1,
            expected_version=1,
            status="not_delivered",
            result_reason=None,
        )
    assert ei.value.status_code == 400


def test_finalize_other_requires_note(monkeypatch):
    monkeypatch.setattr(
        svc.read_svc,
        "enforce_driver_plan_read_access",
        lambda _u, _p: 7,
    )
    with pytest.raises(HTTPException) as ei:
        svc.finalize_visit(
            user={"id": 7},
            plan_id=1,
            visit_id=1,
            expected_version=1,
            status="not_delivered",
            result_reason="other",
            result_note="  ",
        )
    assert ei.value.status_code == 400


def test_local_santiago_formats():
    from datetime import datetime, timezone

    dt = datetime(2026, 3, 15, 15, 30, tzinfo=timezone.utc)
    local = svc._local_santiago(dt)
    assert local is not None
    assert "-03:00" in local or "-04:00" in local
