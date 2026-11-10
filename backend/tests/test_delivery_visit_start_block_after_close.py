"""Bloqueo de nueva visita / retomar duplicada tras cierre válido (plan+cliente)."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from backend.services.distribuidora import delivery_visit_service as svc


def _conn_mocks(gc):
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value = cur
    gc.return_value = conn
    return conn, cur


def test_start_visit_blocks_after_completed_close():
    user = {"id": 1, "role": "driver"}
    closed = {
        "id": 1,
        "status": "completed",
        "ended_at": datetime.now(timezone.utc),
        "driver_user_id": 1,
    }

    with (
        patch.object(svc.read_svc, "enforce_driver_plan_read_access", return_value=1),
        patch.object(svc.loc_svc, "_resolve_client", return_value={"client_id": 9}),
        patch.object(svc.loc_svc, "_parse_client_id", return_value=9),
        patch.object(svc.loc_svc, "_authorized_company_id_for_plan", return_value=3),
        patch.object(svc, "get_connection") as gc,
        patch.object(svc.visit_repo, "tables_ready", return_value=True),
        patch.object(svc.visit_repo, "find_by_idem", return_value=None),
        patch.object(svc.visit_repo, "lock_visits_for_client", return_value=[closed]),
        patch.object(svc.visit_repo, "get_valid_closed_visit", return_value=closed),
        patch.object(svc.visit_repo, "get_active_visit", return_value=None),
    ):
        _conn_mocks(gc)
        with pytest.raises(HTTPException) as ei:
            svc.start_visit(
                user=user,
                plan_id=82,
                customer_key="id:9",
                gps_exception_code="gps_unavailable",
                gps_exception_note="sin gps prueba",
                idempotency_key="k-block-1",
            )
        assert ei.value.status_code == 409
        detail = ei.value.detail
        assert isinstance(detail, dict)
        assert detail["code"] == svc.ERR_VISIT_ALREADY_CLOSED
        assert "finalizada" in str(detail["message"]).lower()


def test_start_visit_blocks_resume_when_prior_valid_close():
    """Romeo Alfa: #1 cerrada + #3 in_progress → no retomar #3."""
    user = {"id": 1, "role": "driver"}
    closed = {
        "id": 1,
        "status": "partial",
        "ended_at": datetime.now(timezone.utc),
        "driver_user_id": 1,
    }
    active = {
        "id": 3,
        "status": "in_progress",
        "driver_user_id": 1,
        "dispatch_plan_id": 82,
        "customer_key": "id:9",
        "version": 2,
    }

    with (
        patch.object(svc.read_svc, "enforce_driver_plan_read_access", return_value=1),
        patch.object(svc.loc_svc, "_resolve_client", return_value={"client_id": 9}),
        patch.object(svc.loc_svc, "_parse_client_id", return_value=9),
        patch.object(svc.loc_svc, "_authorized_company_id_for_plan", return_value=3),
        patch.object(svc, "get_connection") as gc,
        patch.object(svc.visit_repo, "tables_ready", return_value=True),
        patch.object(svc.visit_repo, "find_by_idem", return_value=None),
        patch.object(svc.visit_repo, "lock_visits_for_client", return_value=[closed, active]),
        patch.object(svc.visit_repo, "get_valid_closed_visit", return_value=closed),
        patch.object(svc.visit_repo, "get_active_visit", return_value=active),
    ):
        _conn_mocks(gc)
        with pytest.raises(HTTPException) as ei:
            svc.start_visit(
                user=user,
                plan_id=82,
                customer_key="id:9",
                gps_exception_code="gps_unavailable",
                gps_exception_note="sin gps prueba",
                idempotency_key="k-no-resume-dup",
            )
        assert ei.value.status_code == 409
        detail = ei.value.detail
        assert detail["code"] == svc.ERR_VISIT_SUPERSEDED_BY_CLOSED
        assert detail["closed_visit_id"] == 1
        assert detail["open_visit_id"] == 3


def test_start_visit_resumes_active_without_prior_close():
    user = {"id": 1, "role": "driver"}
    active = {
        "id": 3,
        "status": "in_progress",
        "driver_user_id": 1,
        "dispatch_plan_id": 82,
        "customer_key": "id:9",
        "version": 2,
    }

    with (
        patch.object(svc.read_svc, "enforce_driver_plan_read_access", return_value=1),
        patch.object(svc.loc_svc, "_resolve_client", return_value={"client_id": 9}),
        patch.object(svc.loc_svc, "_parse_client_id", return_value=9),
        patch.object(svc.loc_svc, "_authorized_company_id_for_plan", return_value=3),
        patch.object(svc, "get_connection") as gc,
        patch.object(svc.visit_repo, "tables_ready", return_value=True),
        patch.object(svc.visit_repo, "find_by_idem", return_value=None),
        patch.object(svc.visit_repo, "lock_visits_for_client", return_value=[active]),
        patch.object(svc.visit_repo, "get_valid_closed_visit", return_value=None),
        patch.object(svc.visit_repo, "get_active_visit", return_value=active),
        patch.object(svc.visit_repo, "insert_event"),
        patch.object(svc, "_visit_public", return_value={"id": 3, "status": "in_progress"}),
    ):
        _conn_mocks(gc)
        out = svc.start_visit(
            user=user,
            plan_id=82,
            customer_key="id:9",
            gps_exception_code="gps_unavailable",
            gps_exception_note="sin gps prueba",
            idempotency_key="k-resume-ok",
        )
        assert out["resumed"] is True
        assert out["created"] is False


def test_require_own_open_visit_blocks_superseded_duplicate():
    closed = {
        "id": 1,
        "status": "completed",
        "ended_at": datetime.now(timezone.utc),
    }
    open_row = {
        "id": 3,
        "status": "in_progress",
        "driver_user_id": 1,
        "dispatch_plan_id": 82,
        "customer_key": "id:9",
    }
    cur = MagicMock()
    with (
        patch.object(svc.read_svc, "enforce_driver_plan_read_access", return_value=1),
        patch.object(svc.visit_repo, "get_visit", return_value=open_row),
        patch.object(svc.visit_repo, "get_valid_closed_visit", return_value=closed),
    ):
        with pytest.raises(HTTPException) as ei:
            svc._require_own_open_visit(
                cur, user={"id": 1}, plan_id=82, visit_id=3
            )
        assert ei.value.status_code == 409
        assert ei.value.detail["code"] == svc.ERR_VISIT_SUPERSEDED_BY_CLOSED


def test_require_own_open_visit_allows_legitimate_active():
    open_row = {
        "id": 5,
        "status": "in_progress",
        "driver_user_id": 1,
        "dispatch_plan_id": 82,
        "customer_key": "id:9",
    }
    cur = MagicMock()
    with (
        patch.object(svc.read_svc, "enforce_driver_plan_read_access", return_value=1),
        patch.object(svc.visit_repo, "get_visit", return_value=open_row),
        patch.object(svc.visit_repo, "get_valid_closed_visit", return_value=None),
    ):
        out = svc._require_own_open_visit(
            cur, user={"id": 1}, plan_id=82, visit_id=5
        )
        assert int(out["id"]) == 5


def test_is_valid_closed_visit_requires_ended_at():
    assert svc.is_valid_closed_visit(
        {"status": "completed", "ended_at": datetime.now(timezone.utc)}
    )
    assert not svc.is_valid_closed_visit({"status": "completed", "ended_at": None})
    assert not svc.is_valid_closed_visit({"status": "in_progress", "ended_at": None})
    assert not svc.is_valid_closed_visit(
        {"status": "cancelled", "ended_at": datetime.now(timezone.utc)}
    )


def test_start_visit_calls_lock_before_insert_path():
    """Concurrencia: FOR UPDATE del plan+cliente antes de validar/insertar."""
    user = {"id": 1, "role": "driver"}
    lock = MagicMock(return_value=[])

    with (
        patch.object(svc.read_svc, "enforce_driver_plan_read_access", return_value=1),
        patch.object(svc.loc_svc, "_resolve_client", return_value={"client_id": 9}),
        patch.object(svc.loc_svc, "_parse_client_id", return_value=9),
        patch.object(svc.loc_svc, "_authorized_company_id_for_plan", return_value=3),
        patch.object(svc, "get_connection") as gc,
        patch.object(svc.visit_repo, "tables_ready", return_value=True),
        patch.object(svc.visit_repo, "find_by_idem", return_value=None),
        patch.object(svc.visit_repo, "lock_visits_for_client", lock),
        patch.object(svc.visit_repo, "get_valid_closed_visit", return_value=None),
        patch.object(svc.visit_repo, "get_active_visit", return_value=None),
        patch.object(
            svc.visit_repo,
            "insert_visit",
            return_value={
                "id": 10,
                "status": "in_progress",
                "driver_user_id": 1,
                "dispatch_plan_id": 82,
                "customer_key": "id:9",
                "company_id": 3,
                "version": 1,
                "started_at": datetime.now(timezone.utc),
                "ended_at": None,
            },
        ),
        patch.object(svc.visit_repo, "insert_event"),
        patch.object(
            svc, "_visit_public", return_value={"id": 10, "status": "in_progress"}
        ),
    ):
        _conn_mocks(gc)
        out = svc.start_visit(
            user=user,
            plan_id=82,
            customer_key="id:9",
            gps_exception_code="gps_unavailable",
            gps_exception_note="sin gps prueba",
            idempotency_key="k-new-1",
        )
        assert out["created"] is True or out.get("visit", {}).get("id") == 10
        lock.assert_called_once()


def test_concurrent_start_second_sees_valid_close_via_lock_order():
    """Simula 2ª solicitud tras 1ª que ya cerró (validación bajo el mismo lock)."""
    user = {"id": 1, "role": "driver"}
    closed = {
        "id": 1,
        "status": "completed",
        "ended_at": datetime.now(timezone.utc),
        "driver_user_id": 1,
    }

    with (
        patch.object(svc.read_svc, "enforce_driver_plan_read_access", return_value=1),
        patch.object(svc.loc_svc, "_resolve_client", return_value={"client_id": 9}),
        patch.object(svc.loc_svc, "_parse_client_id", return_value=9),
        patch.object(svc.loc_svc, "_authorized_company_id_for_plan", return_value=3),
        patch.object(svc, "get_connection") as gc,
        patch.object(svc.visit_repo, "tables_ready", return_value=True),
        patch.object(svc.visit_repo, "find_by_idem", return_value=None),
        patch.object(svc.visit_repo, "lock_visits_for_client", return_value=[closed]),
        patch.object(svc.visit_repo, "get_valid_closed_visit", return_value=closed),
        patch.object(svc.visit_repo, "get_active_visit", return_value=None),
        patch.object(svc.visit_repo, "insert_visit") as ins,
    ):
        _conn_mocks(gc)
        with pytest.raises(HTTPException) as ei:
            svc.start_visit(
                user=user,
                plan_id=82,
                customer_key="id:9",
                gps_exception_code="gps_unavailable",
                gps_exception_note="race",
                idempotency_key="k-race-2",
            )
        assert ei.value.status_code == 409
        ins.assert_not_called()
