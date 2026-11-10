"""Regresión Romeo Alfa (PLAN-00082): cierre válido #1 + abierta #3.

Fixtures aisladas (sin PG productivo). No muta datos reales.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from backend.services.distribuidora import delivery_visit_service as svc
from backend.services.distribuidora import dispatch_plan_pwa_driver_read_service as read_svc


ENDED = datetime(2026, 11, 10, 15, 0, tzinfo=timezone.utc)

# Escenario confirmado en pgAdmin (ids ilustrativos del piloto).
FIXTURE_CLOSED_V1 = {
    "id": 1,
    "status": "partial",
    "ended_at": ENDED,
    "driver_user_id": 1,
    "dispatch_plan_id": 82,
    "customer_key": "id:3575",
    "closure_review_status": "verified",
    "version": 3,
}
FIXTURE_OPEN_V3 = {
    "id": 3,
    "status": "in_progress",
    "ended_at": None,
    "driver_user_id": 1,
    "dispatch_plan_id": 82,
    "customer_key": "id:3575",
    "version": 2,
}


def test_attention_prefers_valid_close_over_open_duplicate():
    rows = [
        ("id:3575", 1, "partial", ENDED),
        ("id:3575", 3, "in_progress", None),
    ]
    out = read_svc.resolve_visit_attention_rows(rows)
    att = out["id:3575"]
    assert att["visit_id"] == 1
    assert att["visit_status"] == "partial"
    assert att["attention_status"] == "partial"
    assert att["admin_review_pending"] is True
    assert att["open_visit_id"] == 3
    assert att["operational_visit_id"] == 1
    assert "revisión admin" in att["attention_label_es"].lower()
    assert att["attention_status"] != "in_progress"


def test_attention_legitimate_active_without_prior_close():
    rows = [("id:9", 5, "in_progress", None)]
    att = read_svc.resolve_visit_attention_rows(rows)["id:9"]
    assert att["attention_status"] == "in_progress"
    assert att["admin_review_pending"] is False
    assert att["visit_id"] == 5


def test_attention_no_visits_pending():
    assert read_svc.resolve_visit_attention_rows([]) == {}


def test_attention_does_not_treat_cancelled_as_valid_close_blocker():
    rows = [
        ("id:9", 1, "cancelled", ENDED),
        ("id:9", 2, "in_progress", None),
    ]
    att = read_svc.resolve_visit_attention_rows(rows)["id:9"]
    assert att["attention_status"] == "in_progress"
    assert att["admin_review_pending"] is False


def test_start_blocks_resume_of_romeo_alfa_duplicate():
    user = {"id": 1, "role": "driver"}
    with (
        patch.object(svc.read_svc, "enforce_driver_plan_read_access", return_value=1),
        patch.object(svc.loc_svc, "_resolve_client", return_value={"client_id": 3575}),
        patch.object(svc.loc_svc, "_parse_client_id", return_value=3575),
        patch.object(svc.loc_svc, "_authorized_company_id_for_plan", return_value=3),
        patch.object(svc, "get_connection") as gc,
        patch.object(svc.visit_repo, "tables_ready", return_value=True),
        patch.object(svc.visit_repo, "find_by_idem", return_value=None),
        patch.object(
            svc.visit_repo,
            "lock_visits_for_client",
            return_value=[FIXTURE_CLOSED_V1, FIXTURE_OPEN_V3],
        ),
        patch.object(
            svc.visit_repo, "get_valid_closed_visit", return_value=FIXTURE_CLOSED_V1
        ),
        patch.object(svc.visit_repo, "get_active_visit", return_value=FIXTURE_OPEN_V3),
    ):
        conn = MagicMock()
        cur = MagicMock()
        conn.cursor.return_value = cur
        gc.return_value = conn
        with pytest.raises(HTTPException) as ei:
            svc.start_visit(
                user=user,
                plan_id=82,
                customer_key="id:3575",
                gps_exception_code="gps_unavailable",
                gps_exception_note="piloto",
                idempotency_key="romeo-resume",
            )
        assert ei.value.status_code == 409
        assert ei.value.detail["code"] == svc.ERR_VISIT_SUPERSEDED_BY_CLOSED


@pytest.mark.parametrize(
    "write_fn_name,kwargs",
    [
        ("_require_own_open_visit", {"plan_id": 82, "visit_id": 3}),
    ],
)
def test_direct_write_guard_blocks_visit_3(write_fn_name, kwargs):
    cur = MagicMock()
    with (
        patch.object(svc.read_svc, "enforce_driver_plan_read_access", return_value=1),
        patch.object(svc.visit_repo, "get_visit", return_value=FIXTURE_OPEN_V3),
        patch.object(
            svc.visit_repo, "get_valid_closed_visit", return_value=FIXTURE_CLOSED_V1
        ),
    ):
        with pytest.raises(HTTPException) as ei:
            getattr(svc, write_fn_name)(cur, user={"id": 1}, **kwargs)
        assert ei.value.status_code == 409
        assert ei.value.detail["code"] == svc.ERR_VISIT_SUPERSEDED_BY_CLOSED


def test_save_document_lines_blocked_on_duplicate_open():
    """Endpoint documentos no escribe en visita #3 si #1 está cerrada válida."""
    with (
        patch.object(svc, "get_connection") as gc,
        patch.object(svc.visit_repo, "tables_ready", return_value=True),
        patch.object(svc.visit_repo, "get_visit_for_update", return_value=FIXTURE_OPEN_V3),
        patch.object(
            svc,
            "_require_own_open_visit",
            side_effect=_http_superseded(),
        ),
    ):
        conn = MagicMock()
        cur = MagicMock()
        conn.cursor.return_value = cur
        gc.return_value = conn
        with pytest.raises(HTTPException) as ei:
            svc.save_document_lines(
                user={"id": 1},
                plan_id=82,
                visit_id=3,
                related_document_id=3975263,
                oc_document_id=None,
                document_number=51819,
                document_type="factura",
                document_total=155033,
                result_status="full",
                notes=None,
                lines=[
                    {
                        "source_detail_id": 1,
                        "planned_units": 60,
                        "delivered_units": 60,
                    }
                ],
                expected_version=2,
            )
        assert ei.value.status_code == 409
        assert ei.value.detail["code"] == svc.ERR_VISIT_SUPERSEDED_BY_CLOSED


def _http_superseded():
    def _raise(*_a, **_k):
        raise svc._http_409(
            svc.ERR_VISIT_SUPERSEDED_BY_CLOSED,
            "bloqueado",
            closed_visit_id=1,
            open_visit_id=3,
        )

    return _raise


def test_units_must_not_be_summed_across_visits_conceptually():
    """83 (#1) + 108 (#3) = 191 ≠ 108 facturadas: el estado operativo usa solo #1."""
    delivered_v1 = 35 + 48  # Royal Guard parcial + Heineken
    delivered_v3 = 60 + 48
    invoiced = 108
    assert delivered_v1 + delivered_v3 == 191
    assert delivered_v1 + delivered_v3 != invoiced
    # El marcador operativo apunta a la visita cerrada, no a la suma.
    att = read_svc.resolve_visit_attention_rows(
        [("id:3575", 1, "partial", ENDED), ("id:3575", 3, "in_progress", None)]
    )["id:3575"]
    assert att["operational_visit_id"] == 1
    assert att["open_visit_id"] == 3


def test_get_visit_detail_remains_readable_without_write_guard():
    src = __import__("inspect").getsource(svc.get_visit_detail)
    assert "_require_own_open_visit" not in src
    assert "commit(" not in src


def test_concurrent_lock_and_unique_active_documented():
    src = __import__("inspect").getsource(svc.start_visit)
    assert "lock_visits_for_client" in src
    assert "uq_delivery_visit_active" in src or "uq_delivery_visit" in src
