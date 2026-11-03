"""Capability-only: lectura acotada a planes propios / asignación explícita."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from backend.auth.multiapp import APP_ERP, CAP_SIMPLE_PLANNING, MultiAppScope
from backend.auth.permissions import is_simple_planning_capability_only
from backend.services.distribuidora.dispatch_plan_access import (
    filter_plans_for_user,
    plan_ids_readable_by_capability_user,
    user_can_assign_driver_on_plan,
    user_can_read_dispatch_plan,
)


def _cap_user(uid: int = 42) -> dict:
    scope = MultiAppScope(
        tables_ready=True,
        apps_rows_present=True,
        apps=frozenset({APP_ERP}),
        erp_capabilities=frozenset({CAP_SIMPLE_PLANNING}),
    )
    return {
        "id": uid,
        "email": "excel@test.local",
        "role": "planificador_excel",
        "erp_capabilities": [CAP_SIMPLE_PLANNING],
        "_multiapp_scope": scope,
    }


def test_capability_only_flag():
    assert is_simple_planning_capability_only(_cap_user()) is True
    u = _cap_user()
    u["role"] = "operativo"
    assert is_simple_planning_capability_only(u) is False


def test_plan_ids_from_session_and_driver():
    cur = MagicMock()
    # sessions table ready, then confirmed_plan_id rows, then plan_session join, then driver col
    cur.fetchone.side_effect = [(1,), (1,)]  # tables/cols exist
    cur.fetchall.side_effect = [
        [(101,), (102,)],  # confirmed_plan_id
        [(103,)],  # via plan_session_id
        [(104,)],  # driver_user_id
    ]
    ids = plan_ids_readable_by_capability_user(cur, 42)
    assert ids == {101, 102, 103, 104}


def test_user_can_read_allows_owned_denies_other():
    user = _cap_user(7)

    with patch(
        "backend.services.distribuidora.dispatch_plan_access.get_connection"
    ) as gc:
        conn = MagicMock()
        cur = MagicMock()
        gc.return_value = conn
        conn.cursor.return_value = cur
        cur.fetchone.side_effect = [(1,), (1,)]
        cur.fetchall.side_effect = [[(55,)], [], []]
        assert user_can_read_dispatch_plan(user, 55) is True

    with patch(
        "backend.services.distribuidora.dispatch_plan_access.get_connection"
    ) as gc:
        conn = MagicMock()
        cur = MagicMock()
        gc.return_value = conn
        conn.cursor.return_value = cur
        cur.fetchone.side_effect = [(1,), (1,)]
        cur.fetchall.side_effect = [[(55,)], [], []]
        assert user_can_read_dispatch_plan(user, 999) is False


def test_filter_plans_for_capability_user():
    user = _cap_user(7)
    plans = [{"id": 1}, {"id": 2}, {"id": 3}]
    with patch(
        "backend.services.distribuidora.dispatch_plan_access.get_connection"
    ) as gc:
        conn = MagicMock()
        cur = MagicMock()
        gc.return_value = conn
        conn.cursor.return_value = cur
        cur.fetchone.side_effect = [(1,), (1,)]
        cur.fetchall.side_effect = [[(2,)], [], []]
        out = filter_plans_for_user(user, plans)
    assert [p["id"] for p in out] == [2]


def test_operativo_unfiltered():
    u = _cap_user()
    u["role"] = "operativo"
    plans = [{"id": 1}, {"id": 9}]
    assert filter_plans_for_user(u, plans) == plans
    assert user_can_read_dispatch_plan(u, 99999) is True


def test_assign_driver_capability_only_on_owned_plan():
    user = _cap_user(7)
    with patch(
        "backend.services.distribuidora.dispatch_plan_access.user_can_read_dispatch_plan",
        return_value=True,
    ):
        assert user_can_assign_driver_on_plan(user, 10) is True
    with patch(
        "backend.services.distribuidora.dispatch_plan_access.user_can_read_dispatch_plan",
        return_value=False,
    ):
        assert user_can_assign_driver_on_plan(user, 10) is False


def test_router_deny_helper_404_on_idor():
    from backend.routers import distribuidora_dispatch_plan as r

    user = _cap_user(7)
    with patch(
        "backend.routers.distribuidora_dispatch_plan.user_can_read_dispatch_plan",
        return_value=False,
    ):
        with pytest.raises(HTTPException) as ei:
            r._deny_plan_if_out_of_scope(user, 12345)
        assert ei.value.status_code == 404
