"""Asignación de chofer multiapp a dispatch_plan."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

import pytest

from backend.auth.multiapp import APP_ERP, APP_PWA, MultiAppScope
from backend.services.distribuidora import dispatch_plan_driver_service as svc


def _scope(*, apps=None, profiles=None, company_ids=None, offices=None):
    return MultiAppScope(
        tables_ready=True,
        apps_rows_present=True,
        apps=frozenset(apps or {APP_PWA}),
        profiles=frozenset(profiles or {"chofer"}),
        company_ids=company_ids,
        offices=frozenset(offices or ()),
    )


def test_can_assign_roles():
    assert svc.can_assign_dispatch_driver({"role": "gerencia"}) is True
    assert svc.can_assign_dispatch_driver({"role": "operativo"}) is True
    assert svc.can_assign_dispatch_driver({"role": "vendedor"}) is False


def test_assert_user_rejects_inactive(monkeypatch):
    cur = MagicMock()
    cur.fetchone.return_value = (9, "c@x.cl", "operativo", False, "Chofer")
    cur.description = [("id",), ("email",), ("role",), ("active",), ("nombre_usuario",)]
    with pytest.raises(svc.DriverAssignError) as ei:
        svc.assert_user_is_eligible_driver(cur, 9)
    assert ei.value.code == "user_inactive"


def test_assert_user_requires_pwa_and_chofer(monkeypatch):
    cur = MagicMock()
    cur.fetchone.return_value = (9, "c@x.cl", "operativo", True, "Chofer")
    cur.description = [("id",), ("email",), ("role",), ("active",), ("nombre_usuario",)]
    monkeypatch.setattr(
        svc,
        "load_multiapp_scope",
        lambda _uid: _scope(apps={APP_ERP}, profiles=set()),
    )
    with pytest.raises(svc.DriverAssignError) as ei:
        svc.assert_user_is_eligible_driver(cur, 9)
    assert ei.value.code == "pwa_required"

    monkeypatch.setattr(
        svc,
        "load_multiapp_scope",
        lambda _uid: _scope(apps={APP_PWA}, profiles={"vendedor"}),
    )
    with pytest.raises(svc.DriverAssignError) as ei2:
        svc.assert_user_is_eligible_driver(cur, 9)
    assert ei2.value.code == "chofer_profile_required"


def test_assert_user_company_office(monkeypatch):
    cur = MagicMock()
    cur.fetchone.return_value = (9, "c@x.cl", "operativo", True, "Chofer")
    cur.description = [("id",), ("email",), ("role",), ("active",), ("nombre_usuario",)]
    monkeypatch.setattr(
        svc,
        "load_multiapp_scope",
        lambda _uid: _scope(
            apps={APP_PWA},
            profiles={"chofer"},
            company_ids=(3,),
            offices={(3, 1)},
        ),
    )
    with pytest.raises(svc.DriverAssignError) as ei:
        svc.assert_user_is_eligible_driver(cur, 9, company_id=1, office_id=1)
    assert ei.value.code == "company_not_allowed"

    with pytest.raises(svc.DriverAssignError) as ei2:
        svc.assert_user_is_eligible_driver(cur, 9, company_id=3, office_id=2)
    assert ei2.value.code == "office_not_allowed"

    ok = svc.assert_user_is_eligible_driver(cur, 9, company_id=3, office_id=1)
    assert ok["display_name"] == "Chofer"


def test_assign_driver_happy_and_audit(monkeypatch):
    plan_row = (10, "planned", date(2026, 11, 10), None, None, 2)
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value = cur
    cur.fetchone.side_effect = [
        plan_row,  # FOR UPDATE plan
    ]
    cur.description = [
        ("id",),
        ("status",),
        ("planning_date",),
        ("driver_user_id",),
        ("driver_name",),
        ("truck_id",),
    ]

    monkeypatch.setattr(svc, "get_connection", lambda: conn)
    monkeypatch.setattr(
        svc,
        "assert_user_is_eligible_driver",
        lambda *_a, **_k: {
            "id": 55,
            "display_name": "Juan Chofer",
            "email": "j@x.cl",
            "nombre_usuario": "Juan Chofer",
        },
    )
    monkeypatch.setattr(svc, "find_same_day_driver_conflicts", lambda *_a, **_k: [])
    events = []

    def _ins(*_a, **kwargs):
        events.append(kwargs)
        return 1

    monkeypatch.setattr(svc.events_repo, "insert_order_event", _ins)

    out = svc.assign_driver_to_plan(
        plan_id=10,
        driver_user_id=55,
        reason="Asignación inicial ruta sur",
        actor={"role": "gerencia", "email": "boss@x.cl", "id": 1},
    )
    assert out["status"] == "assigned"
    assert out["driver_user_id"] == 55
    assert events and events[0]["action"] == svc.ACTION_ASSIGN_DRIVER
    assert events[0]["reason"] == "Asignación inicial ruta sur"
    conn.commit.assert_called()


def test_reassign_records_previous(monkeypatch):
    plan_row = (10, "picking_generated", date(2026, 11, 10), 40, "Viejo", 2)
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value = cur
    cur.fetchone.side_effect = [plan_row]
    cur.description = [
        ("id",),
        ("status",),
        ("planning_date",),
        ("driver_user_id",),
        ("driver_name",),
        ("truck_id",),
    ]
    monkeypatch.setattr(svc, "get_connection", lambda: conn)
    monkeypatch.setattr(
        svc,
        "assert_user_is_eligible_driver",
        lambda *_a, **_k: {"display_name": "Nuevo", "id": 77},
    )
    monkeypatch.setattr(
        svc,
        "find_same_day_driver_conflicts",
        lambda *_a, **_k: [{"id": 99, "status": "planned"}],
    )
    payload_holder = {}

    def _ins(*_a, **kwargs):
        payload_holder.update(kwargs.get("payload") or {})
        return 2

    monkeypatch.setattr(svc.events_repo, "insert_order_event", _ins)
    out = svc.assign_driver_to_plan(
        plan_id=10,
        driver_user_id=77,
        reason="Cambio de turno",
        actor={"role": "operativo", "email": "op@x.cl", "id": 2},
    )
    assert out["status"] == "reassigned"
    assert out["previous_driver_user_id"] == 40
    assert payload_holder["previous_driver_user_id"] == 40
    assert payload_holder["new_driver_user_id"] == 77
    assert len(out["same_day_conflicts"]) == 1


def test_assign_forbidden_without_permission():
    with pytest.raises(svc.DriverAssignError) as ei:
        svc.assign_driver_to_plan(
            plan_id=1,
            driver_user_id=2,
            reason="x" * 5,
            actor={"role": "vendedor", "email": "v@x.cl"},
        )
    assert ei.value.http_status == 403


def test_assign_requires_reason(monkeypatch):
    with pytest.raises(svc.DriverAssignError) as ei:
        svc.assign_driver_to_plan(
            plan_id=1,
            driver_user_id=2,
            reason="ab",
            actor={"role": "gerencia"},
        )
    assert ei.value.code == "reason_required"


def test_driver_access_after_reassign(monkeypatch):
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value = cur
    cur.fetchone.side_effect = [None, (1,)]
    monkeypatch.setattr(svc, "get_connection", lambda: conn)
    assert svc.driver_has_access_to_plan(40, 10) is False
    assert svc.driver_has_access_to_plan(77, 10) is True


def test_concurrent_reassign_uses_for_update(monkeypatch):
    """Ambas TX toman FOR UPDATE; la segunda ve el valor ya actualizado."""
    plan_first = (10, "planned", date(2026, 11, 10), None, None, 2)
    plan_second = (10, "planned", date(2026, 11, 10), 55, "Juan", 2)

    class Conn:
        def __init__(self, plan):
            self.plan = plan
            self.cur = MagicMock()
            self.cur.description = [
                ("id",),
                ("status",),
                ("planning_date",),
                ("driver_user_id",),
                ("driver_name",),
                ("truck_id",),
            ]
            self.cur.fetchone.return_value = plan
            self.commits = 0

        def cursor(self):
            return self.cur

        def commit(self):
            self.commits += 1

        def rollback(self):
            pass

        def close(self):
            pass

    c1 = Conn(plan_first)
    c2 = Conn(plan_second)
    seq = iter([c1, c2])
    monkeypatch.setattr(svc, "get_connection", lambda: next(seq))
    monkeypatch.setattr(
        svc,
        "assert_user_is_eligible_driver",
        lambda *_a, **_k: {"display_name": "X", "id": 1},
    )
    monkeypatch.setattr(svc, "find_same_day_driver_conflicts", lambda *_a, **_k: [])
    monkeypatch.setattr(svc.events_repo, "insert_order_event", lambda *_a, **_k: 1)

    r1 = svc.assign_driver_to_plan(
        plan_id=10,
        driver_user_id=55,
        reason="Primero",
        actor={"role": "gerencia", "email": "a@x.cl", "id": 1},
    )
    r2 = svc.assign_driver_to_plan(
        plan_id=10,
        driver_user_id=55,
        reason="Segundo idempotente",
        actor={"role": "gerencia", "email": "a@x.cl", "id": 1},
    )
    assert r1["status"] == "assigned"
    assert r2["status"] == "unchanged"
    assert "FOR UPDATE" in c1.cur.execute.call_args_list[0][0][0]
