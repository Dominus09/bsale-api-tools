"""Anulación admin Romeo Alfa: #1+#3 prueba → cliente habilitado a nueva visita."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from backend.repositories.distribuidora import delivery_visit_repo as repo
from backend.services.distribuidora import delivery_visit_service as svc
from backend.services.distribuidora import dispatch_plan_pwa_driver_read_service as read_svc

ENDED = datetime(2026, 11, 10, 15, 0, tzinfo=timezone.utc)


def test_is_admin_voided_row():
    assert repo.is_admin_voided_row(
        {"admin_classification": "voided_admin", "voided_at": ENDED}
    )
    assert repo.is_admin_voided_row({"admin_classification": "pilot_test"})
    assert not repo.is_admin_voided_row(
        {"admin_classification": "operational", "voided_at": None}
    )


def test_is_valid_closed_excludes_voided_partial():
    voided = {
        "id": 1,
        "status": "partial",
        "ended_at": ENDED,
        "admin_classification": "voided_admin",
        "voided_at": ENDED,
    }
    assert not svc.is_valid_closed_visit(voided)
    assert svc.is_valid_closed_visit(
        {
            "id": 1,
            "status": "partial",
            "ended_at": ENDED,
            "admin_classification": "operational",
            "voided_at": None,
        }
    )


def test_attention_ignores_voided_visits_enables_pending():
    """Tras anular #1 y #3, mapa/listado → Pendiente (habilitado)."""
    rows = [
        ("id:3575", 1, "partial", ENDED, "voided_admin", ENDED),
        ("id:3575", 3, "cancelled", ENDED, "voided_admin", ENDED),
    ]
    out = read_svc.resolve_visit_attention_rows(rows)
    # Sin visitas operativas → clave ausente o pending si se agrupa vacío
    assert "id:3575" not in out


def test_attention_voided_close_does_not_mark_admin_review():
    rows = [
        ("id:3575", 1, "partial", ENDED, "voided_admin", ENDED),
        ("id:3575", 4, "in_progress", None, "operational", None),
    ]
    att = read_svc.resolve_visit_attention_rows(rows)["id:3575"]
    assert att["attention_status"] == "in_progress"
    assert att["admin_review_pending"] is False
    assert att["visit_id"] == 4


def test_start_visit_allowed_after_both_pilot_voids():
    """#1 voided + #3 cancelled/voided → puede insertar visita nueva."""
    user = {"id": 1, "role": "driver"}
    with (
        patch.object(svc.read_svc, "enforce_driver_plan_read_access", return_value=1),
        patch.object(svc.loc_svc, "_resolve_client", return_value={"client_id": 3575}),
        patch.object(svc.loc_svc, "_parse_client_id", return_value=3575),
        patch.object(svc.loc_svc, "_authorized_company_id_for_plan", return_value=3),
        patch.object(svc, "get_connection") as gc,
        patch.object(svc.visit_repo, "tables_ready", return_value=True),
        patch.object(svc.visit_repo, "find_by_idem", return_value=None),
        patch.object(svc.visit_repo, "lock_visits_for_client", return_value=[]),
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
                "customer_key": "id:3575",
                "company_id": 3,
                "version": 1,
                "started_at": ENDED,
                "ended_at": None,
                "admin_classification": "operational",
            },
        ),
        patch.object(svc.visit_repo, "insert_event"),
        patch.object(
            svc, "_visit_public", return_value={"id": 10, "status": "in_progress"}
        ),
    ):
        conn = MagicMock()
        cur = MagicMock()
        conn.cursor.return_value = cur
        gc.return_value = conn
        out = svc.start_visit(
            user=user,
            plan_id=82,
            customer_key="id:3575",
            gps_exception_code="gps_unavailable",
            gps_exception_note="entrega real",
            idempotency_key="romeo-real-1",
        )
        assert out["created"] is True
        assert out["visit"]["id"] == 10


def test_voided_visit_1_does_not_block_via_valid_closed_helper():
    cur = MagicMock()
    with (
        patch.object(repo, "admin_void_columns_ready", return_value=True),
    ):
        # Simula que el SQL con filtro void no encuentra fila
        cur.fetchone.return_value = None
        cur.description = [("id",)]
        out = repo.get_valid_closed_visit(
            cur, plan_id=82, customer_key="id:3575"
        )
        assert out is None
        sql = cur.execute.call_args[0][0]
        assert "admin_classification" in sql
        assert "voided_at" in sql


def test_other_clients_unaffected_by_attention_filter():
    rows = [
        ("id:3575", 1, "partial", ENDED, "voided_admin", ENDED),
        ("id:9999", 50, "completed", ENDED, "operational", None),
    ]
    out = read_svc.resolve_visit_attention_rows(rows)
    assert "id:3575" not in out
    assert out["id:9999"]["visit_id"] == 50
    assert out["id:9999"]["attention_status"] == "completed"


def test_require_open_does_not_treat_voided_prior_as_blocker():
    """Si #1 está voided, una activa legítima #4 sigue editable."""
    open_row = {
        "id": 4,
        "status": "in_progress",
        "driver_user_id": 1,
        "dispatch_plan_id": 82,
        "customer_key": "id:3575",
        "admin_classification": "operational",
        "voided_at": None,
    }
    cur = MagicMock()
    with (
        patch.object(svc.read_svc, "enforce_driver_plan_read_access", return_value=1),
        patch.object(svc.visit_repo, "get_visit", return_value=open_row),
        patch.object(svc.visit_repo, "get_valid_closed_visit", return_value=None),
    ):
        out = svc._require_own_open_visit(
            cur, user={"id": 1}, plan_id=82, visit_id=4
        )
        assert int(out["id"]) == 4
