"""Confirmación simplificada: chofer obligatorio y rollback si falta elegibilidad."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from uuid import uuid4

import pytest

from backend.services.distribuidora import simple_planning_confirm_service as svc
from backend.services.distribuidora.dispatch_plan_driver_service import DriverAssignError


class _Conn:
    def __init__(self, sessions):
        self.sessions = sessions
        self.committed = False
        self.rolled_back = False
        self._cur = self

    def cursor(self):
        return self

    def execute(self, *_a, **_k):
        return None

    def fetchone(self):
        return None

    def close(self):
        return None

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True


def test_prepare_requires_driver_user_id(monkeypatch):
    monkeypatch.setattr(
        svc,
        "preview_truck_assignments",
        lambda **_k: {"trucks": [], "blocking_count": 0},
    )
    with pytest.raises(svc.SimplePlanningConfirmError) as ei:
        svc.prepare_confirm_session(
            user_id=1,
            company_id=3,
            office_id=1,
            planning_date=date.today(),
            truck_id=2,
            dispatch_name="X",
            driver_name=None,
            driver_user_id=None,
            assignments=[{"folio": 1, "truck_id": 2}],
            conn_factory=lambda: (_ for _ in ()).throw(RuntimeError("no db")),
        )
    assert ei.value.code == "driver_required"


def test_confirm_rejects_legacy_session_without_driver(monkeypatch):
    sid = str(uuid4())
    row = {
        "id": sid,
        "user_id": 7,
        "status": "open",
        "expires_at": datetime.now(timezone.utc) + timedelta(minutes=10),
        "company_id": 3,
        "office_id": 1,
        "truck_id": 2,
        "planning_date": date(2026, 12, 1),
        "dispatch_name": "Norte",
        "driver_name": None,
        "plan_session_id": f"simple-{sid}",
        "payload": {
            "orders": [
                {
                    "folio": 10,
                    "document_id": 10,
                    "identity_fingerprint": "abc",
                    "client": {"client_id": 1, "client_name": "C", "municipality": "X"},
                }
            ],
            "capacity_snapshot": {
                "capacity_known": True,
                "blocking": False,
                "known_weight_kg": 10,
                "truck_max_weight_kg": 1000,
                "incomplete_order_count": 0,
                "unevaluable_order_count": 0,
            },
            "truck": {"id": 2, "name": "Camion", "active": True},
            "weight_acknowledgement": {},
            # sin driver_user_id → sesión antigua
        },
    }
    conn = _Conn({sid: row})

    monkeypatch.setattr(svc.session_repo, "get_session_by_id_for_update", lambda *_a, **_k: row)
    monkeypatch.setattr(svc.session_repo, "mark_session_status", lambda *_a, **_k: None)
    monkeypatch.setattr(
        svc,
        "preview_truck_assignments",
        lambda **_k: {
            "trucks": [
                {
                    "truck_id": 2,
                    "truck": {"id": 2, "name": "Camion", "active": True},
                    "truck_missing": False,
                    "truck_inactive": False,
                    "orders": row["payload"]["orders"],
                    "capacity": row["payload"]["capacity_snapshot"],
                    "blocking": False,
                }
            ],
            "fingerprint_drifts": [],
            "blocking_count": 0,
            "active_plan_conflicts": [],
            "request_duplicate_folios": [],
        },
    )

    confirmed = []

    def _confirm(**kwargs):
        confirmed.append(kwargs)
        return {"plan": {"id": 999}}

    with pytest.raises(svc.SimplePlanningConfirmError) as ei:
        svc.confirm_simple_planning_session(
            session_id=sid,
            user_id=7,
            confirm_fn=_confirm,
            conn_factory=lambda: conn,
        )
    # Debe fallar por driver_required tras crear plan o antes; en cualquier caso rollback.
    assert ei.value.code == "driver_required"
    assert conn.rolled_back is True or not confirmed or conn.rolled_back
    # Si confirm llegó a ejecutarse, debió hacerse rollback de esa TX.
    if confirmed:
        assert conn.rolled_back is True


def test_confirm_rolls_back_when_driver_becomes_ineligible(monkeypatch):
    sid = str(uuid4())
    row = {
        "id": sid,
        "user_id": 7,
        "status": "open",
        "expires_at": datetime.now(timezone.utc) + timedelta(minutes=10),
        "company_id": 3,
        "office_id": 1,
        "truck_id": 2,
        "planning_date": date(2026, 12, 1),
        "dispatch_name": "Norte",
        "driver_name": "Chofer",
        "plan_session_id": f"simple-{sid}",
        "payload": {
            "orders": [
                {
                    "folio": 10,
                    "document_id": 10,
                    "identity_fingerprint": "abc",
                    "client": {"client_id": 1, "client_name": "C", "municipality": "X"},
                }
            ],
            "capacity_snapshot": {
                "capacity_known": True,
                "blocking": False,
                "known_weight_kg": 10,
                "truck_max_weight_kg": 1000,
                "incomplete_order_count": 0,
                "unevaluable_order_count": 0,
            },
            "truck": {"id": 2, "name": "Camion", "active": True},
            "weight_acknowledgement": {},
            "driver_user_id": 55,
            "driver_name": "Chofer",
        },
    }
    conn = _Conn({sid: row})
    monkeypatch.setattr(svc.session_repo, "get_session_by_id_for_update", lambda *_a, **_k: row)
    monkeypatch.setattr(svc.session_repo, "mark_session_status", lambda *_a, **_k: None)
    monkeypatch.setattr(
        svc,
        "preview_truck_assignments",
        lambda **_k: {
            "trucks": [
                {
                    "truck_id": 2,
                    "truck": {"id": 2, "name": "Camion", "active": True},
                    "truck_missing": False,
                    "truck_inactive": False,
                    "orders": row["payload"]["orders"],
                    "capacity": row["payload"]["capacity_snapshot"],
                    "blocking": False,
                }
            ],
            "fingerprint_drifts": [],
            "blocking_count": 0,
            "active_plan_conflicts": [],
            "request_duplicate_folios": [],
        },
    )

    def _confirm(**_k):
        return {"plan": {"id": 777}}

    def _boom(*_a, **_k):
        raise DriverAssignError("sin pwa", code="pwa_required", http_status=409)

    monkeypatch.setattr(
        "backend.services.distribuidora.dispatch_plan_driver_service.assert_user_is_eligible_driver",
        _boom,
    )

    with pytest.raises(svc.SimplePlanningConfirmError) as ei:
        svc.confirm_simple_planning_session(
            session_id=sid,
            user_id=7,
            confirm_fn=_confirm,
            conn_factory=lambda: conn,
        )
    assert ei.value.code == "pwa_required"
    assert conn.rolled_back is True
    assert conn.committed is False
