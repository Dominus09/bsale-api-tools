"""Permisos capability simple_planning vs PWA / admin."""

from __future__ import annotations

from backend.auth.multiapp import (
    APP_ERP,
    APP_PWA,
    CAP_SIMPLE_PLANNING,
    MultiAppScope,
    has_app_access,
    has_erp_capability,
)
from backend.auth.permissions import has_management_access, has_simple_planning_access


def test_pwa_profile_alone_does_not_grant_erp_or_simple_planning():
    scope = MultiAppScope(
        tables_ready=True,
        apps_rows_present=True,
        apps=frozenset({APP_PWA}),
        profiles=frozenset({"chofer", "vendedor"}),
        erp_capabilities=frozenset(),
    )
    assert has_app_access(scope, APP_ERP) is False
    assert has_app_access(scope, APP_PWA) is True
    user = {
        "role": "vendedor",
        "profiles": ["chofer"],
        "erp_capabilities": [],
        "_multiapp_scope": scope,
    }
    assert has_simple_planning_access(user) is False
    assert has_management_access(user) is False


def test_capability_grants_simple_planning_not_management():
    scope = MultiAppScope(
        tables_ready=True,
        apps_rows_present=True,
        apps=frozenset({APP_ERP}),
        erp_capabilities=frozenset({CAP_SIMPLE_PLANNING}),
    )
    user = {
        "role": "operativo",  # even without role, capability alone:
        "id": 9,
        "erp_capabilities": [CAP_SIMPLE_PLANNING],
        "_multiapp_scope": scope,
    }
    # Force non-SIMPLE_PLANNING_ROLES
    user["role"] = "custom_role"
    assert has_erp_capability(scope, CAP_SIMPLE_PLANNING)
    assert has_simple_planning_access(user) is True
    assert has_management_access(user) is False


def test_capability_without_erp_app_still_needs_erp_for_endpoints():
    """La capability no concede ERP; require_erp_app sigue bloqueando."""
    scope = MultiAppScope(
        tables_ready=True,
        apps_rows_present=True,
        apps=frozenset({APP_PWA}),
        erp_capabilities=frozenset({CAP_SIMPLE_PLANNING}),
    )
    assert has_app_access(scope, APP_ERP) is False
    assert has_erp_capability(scope, CAP_SIMPLE_PLANNING) is True
