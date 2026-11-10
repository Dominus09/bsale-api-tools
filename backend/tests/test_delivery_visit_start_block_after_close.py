"""Bloqueo de nueva visita normal tras cierre válido (plan+cliente)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from backend.services.distribuidora import delivery_visit_service as svc


def test_start_visit_blocks_after_completed_close():
    user = {"id": 1, "role": "driver"}
    closed = {"id": 1, "status": "completed", "driver_user_id": 1}

    with (
        patch.object(svc.read_svc, "enforce_driver_plan_read_access", return_value=1),
        patch.object(svc.loc_svc, "_resolve_client", return_value={"client_id": 9}),
        patch.object(svc.loc_svc, "_parse_client_id", return_value=9),
        patch.object(svc.loc_svc, "_authorized_company_id_for_plan", return_value=3),
        patch.object(svc, "get_connection") as gc,
        patch.object(svc.visit_repo, "tables_ready", return_value=True),
        patch.object(svc.visit_repo, "find_by_idem", return_value=None),
        patch.object(svc.visit_repo, "get_active_visit", return_value=None),
        patch.object(svc.visit_repo, "get_last_closed_visit", return_value=closed),
    ):
        conn = MagicMock()
        cur = MagicMock()
        conn.cursor.return_value = cur
        gc.return_value = conn
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
        assert "finalizada" in str(ei.value.detail).lower()


def test_start_visit_resumes_active_even_if_prior_closed():
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
        patch.object(svc.visit_repo, "get_active_visit", return_value=active),
        patch.object(svc.visit_repo, "insert_event"),
        patch.object(svc, "_visit_public", return_value={"id": 3, "status": "in_progress"}),
    ):
        conn = MagicMock()
        cur = MagicMock()
        conn.cursor.return_value = cur
        gc.return_value = conn
        out = svc.start_visit(
            user=user,
            plan_id=82,
            customer_key="id:9",
            gps_exception_code="gps_unavailable",
            gps_exception_note="sin gps prueba",
            idempotency_key="k-resume-1",
        )
        assert out["resumed"] is True
        assert out["created"] is False
