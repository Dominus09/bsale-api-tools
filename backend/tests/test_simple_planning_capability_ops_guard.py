"""Hardening: capability-only simple_planning no escala a ops ERP."""

from __future__ import annotations

import pytest
from fastapi import HTTPException

from backend.auth.multiapp import (
    APP_ERP,
    CAP_SIMPLE_PLANNING,
    MultiAppScope,
)
from backend.auth.permissions import (
    has_simple_planning_access,
    is_simple_planning_capability_only,
    require_erp_ops_staff,
)


def _cap_only_user() -> dict:
    scope = MultiAppScope(
        tables_ready=True,
        apps_rows_present=True,
        apps=frozenset({APP_ERP}),
        erp_capabilities=frozenset({CAP_SIMPLE_PLANNING}),
    )
    return {
        "id": 42,
        "email": "excel@test.local",
        "role": "planificador_excel",
        "erp_capabilities": [CAP_SIMPLE_PLANNING],
        "_multiapp_scope": scope,
    }


def test_capability_only_detected():
    u = _cap_only_user()
    assert has_simple_planning_access(u) is True
    assert is_simple_planning_capability_only(u) is True


def test_operativo_with_capability_is_not_capability_only():
    u = _cap_only_user()
    u["role"] = "operativo"
    assert has_simple_planning_access(u) is True
    assert is_simple_planning_capability_only(u) is False


def test_require_erp_ops_staff_denies_capability_only(monkeypatch):
    u = _cap_only_user()

    def _fake_staff(_auth=None):
        return u

    monkeypatch.setattr(
        "backend.utils.auth_staff.require_staff_user",
        _fake_staff,
    )
    with pytest.raises(HTTPException) as ei:
        require_erp_ops_staff(authorization="Bearer x")
    assert ei.value.status_code == 403
    assert "simple_planning" in str(ei.value.detail).lower()


def test_require_erp_ops_staff_allows_operativo(monkeypatch):
    u = _cap_only_user()
    u["role"] = "operativo"

    def _fake_staff(_auth=None):
        return u

    monkeypatch.setattr(
        "backend.utils.auth_staff.require_staff_user",
        _fake_staff,
    )
    out = require_erp_ops_staff(authorization="Bearer x")
    assert out["role"] == "operativo"
