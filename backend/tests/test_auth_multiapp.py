"""Fundación multiapp: apps / perfiles / oficinas (sin BD real)."""

from __future__ import annotations

import pytest
from fastapi import HTTPException

from backend.auth.multiapp import (
    APP_ERP,
    APP_PWA,
    MultiAppScope,
    assert_company,
    assert_office,
    assert_profiles,
    has_app_access,
    has_profile,
    require_erp_app,
    require_pwa_app,
    require_pwa_profiles,
    scope_public_dict,
)
from backend.auth.permissions import build_auth_me_response
from backend.utils.auth_staff import require_staff_user


@pytest.fixture(autouse=True)
def _clean_flags(monkeypatch):
    monkeypatch.delenv("AUTH_REQUIRE_USER_APPS", raising=False)
    monkeypatch.delenv("AUTH_REQUIRE_USER_COMPANIES", raising=False)
    monkeypatch.delenv("AUTH_EMIT_COMPANY_IDS", raising=False)
    monkeypatch.delenv("AUTH_CHECK_ACTIVE_ON_REQUEST", raising=False)
    monkeypatch.delenv("AUTH_ENFORCE_TOKEN_VERSION", raising=False)


def _scope(**kwargs) -> MultiAppScope:
    return MultiAppScope(**kwargs)


def test_legacy_user_without_apps_rows_keeps_erp():
    scope = _scope(tables_ready=True, apps_rows_present=False, apps=frozenset())
    assert has_app_access(scope, APP_ERP) is True
    assert has_app_access(scope, APP_PWA) is False


def test_pre_migration_tables_missing_keeps_erp():
    scope = _scope(tables_ready=False, apps_rows_present=False)
    assert has_app_access(scope, APP_ERP) is True
    assert has_app_access(scope, APP_PWA) is False


def test_pwa_only_denied_erp():
    scope = _scope(
        tables_ready=True,
        apps_rows_present=True,
        apps=frozenset({APP_PWA}),
    )
    assert has_app_access(scope, APP_ERP) is False
    assert has_app_access(scope, APP_PWA) is True


def test_explicit_erp_and_pwa():
    scope = _scope(
        tables_ready=True,
        apps_rows_present=True,
        apps=frozenset({APP_ERP, APP_PWA}),
        profiles=frozenset({"chofer"}),
    )
    assert has_app_access(scope, APP_ERP)
    assert has_app_access(scope, APP_PWA)
    assert has_profile(scope, "chofer")
    assert not has_profile(scope, "jefe_sucursal")


def test_cutover_require_user_apps_denies_legacy(monkeypatch):
    monkeypatch.setenv("AUTH_REQUIRE_USER_APPS", "1")
    scope = _scope(tables_ready=True, apps_rows_present=False, apps=frozenset())
    assert has_app_access(scope, APP_ERP) is False


def test_chofer_not_supervisor_profile():
    scope = _scope(
        tables_ready=True,
        apps_rows_present=True,
        apps=frozenset({APP_PWA}),
        profiles=frozenset({"chofer"}),
    )
    with pytest.raises(HTTPException) as err:
        assert_profiles(scope, ["jefe_sucursal"])
    assert err.value.status_code == 403


def test_company_and_office_scope():
    scope = _scope(
        tables_ready=True,
        apps_rows_present=True,
        apps=frozenset({APP_PWA}),
        company_ids=(3,),
        offices=frozenset({(3, 1), (3, 4)}),
    )
    assert_company(scope, 3)
    with pytest.raises(HTTPException):
        assert_company(scope, 9)
    assert_office(scope, company_id=3, office_id=1)
    with pytest.raises(HTTPException):
        assert_office(scope, company_id=3, office_id=5)
    with pytest.raises(HTTPException):
        assert_office(scope, company_id=9, office_id=1)


def test_office_require_assignment_default_deny_when_empty():
    scope = _scope(
        tables_ready=True,
        apps_rows_present=True,
        apps=frozenset({APP_PWA}),
        company_ids=None,
        offices=frozenset(),
    )
    with pytest.raises(HTTPException):
        assert_office(scope, company_id=3, office_id=1, require_assignment=True)


def test_require_erp_app_allows_legacy(monkeypatch):
    monkeypatch.setattr(
        "backend.auth.multiapp.decode_staff_token",
        lambda _a: {"email": "erp@test.local", "role": "operativo", "uid": 10},
    )
    monkeypatch.setattr(
        "backend.auth.multiapp.enrich_staff_payload",
        lambda p: {**p, "id": 10, "uid": 10},
    )
    monkeypatch.setattr(
        "backend.auth.multiapp.load_multiapp_scope",
        lambda _uid: _scope(tables_ready=True, apps_rows_present=False),
    )
    user = require_erp_app("Bearer x")
    assert user["has_erp_access"] is True
    # Misma política vía require_staff_user (rutas ERP existentes).
    staff = require_staff_user("Bearer x")
    assert staff["has_erp_access"] is True


def test_require_erp_app_rejects_pwa_only(monkeypatch):
    monkeypatch.setattr(
        "backend.auth.multiapp.decode_staff_token",
        lambda _a: {"email": "pwa@test.local", "role": "operativo", "uid": 11},
    )
    monkeypatch.setattr(
        "backend.auth.multiapp.enrich_staff_payload",
        lambda p: {**p, "id": 11, "uid": 11},
    )
    monkeypatch.setattr(
        "backend.auth.multiapp.load_multiapp_scope",
        lambda _uid: _scope(
            tables_ready=True,
            apps_rows_present=True,
            apps=frozenset({APP_PWA}),
            profiles=frozenset({"chofer"}),
        ),
    )
    with pytest.raises(HTTPException) as err:
        require_erp_app("Bearer x")
    assert err.value.status_code == 403
    with pytest.raises(HTTPException):
        require_staff_user("Bearer x")


def test_require_pwa_app_default_deny_for_erp_legacy(monkeypatch):
    monkeypatch.setattr(
        "backend.auth.multiapp.decode_staff_token",
        lambda _a: {"email": "erp@test.local", "role": "gerencia", "uid": 12},
    )
    monkeypatch.setattr(
        "backend.auth.multiapp.enrich_staff_payload",
        lambda p: {**p, "id": 12, "uid": 12},
    )
    monkeypatch.setattr(
        "backend.auth.multiapp.load_multiapp_scope",
        lambda _uid: _scope(tables_ready=True, apps_rows_present=False),
    )
    with pytest.raises(HTTPException) as err:
        require_pwa_app("Bearer x")
    assert err.value.status_code == 403


def test_require_pwa_profiles_chofer(monkeypatch):
    monkeypatch.setattr(
        "backend.auth.multiapp.decode_staff_token",
        lambda _a: {"email": "ch@test.local", "role": "operativo", "uid": 13},
    )
    monkeypatch.setattr(
        "backend.auth.multiapp.enrich_staff_payload",
        lambda p: {**p, "id": 13, "uid": 13},
    )
    monkeypatch.setattr(
        "backend.auth.multiapp.load_multiapp_scope",
        lambda _uid: _scope(
            tables_ready=True,
            apps_rows_present=True,
            apps=frozenset({APP_PWA}),
            profiles=frozenset({"chofer"}),
        ),
    )
    dep = require_pwa_profiles("chofer", "bodega")
    user = dep("Bearer x")
    assert "chofer" in user["profiles"]
    dep_sup = require_pwa_profiles("jefe_sucursal")
    with pytest.raises(HTTPException):
        dep_sup("Bearer x")


def test_auth_me_includes_multiapp_fields(monkeypatch):
    monkeypatch.setattr(
        "backend.auth.multiapp.load_multiapp_scope",
        lambda _uid: _scope(
            tables_ready=True,
            apps_rows_present=True,
            apps=frozenset({APP_ERP}),
            profiles=frozenset(),
            company_ids=(3,),
            offices=frozenset({(3, 1)}),
        ),
    )
    monkeypatch.setattr(
        "backend.auth.permissions._fetch_staff_user_id",
        lambda _e: 99,
    )
    body = build_auth_me_response(
        {"email": "a@test.local", "role": "adm", "id": 99}
    )
    assert body["has_erp_access"] is True
    assert body["has_pwa_access"] is False
    assert body["apps"] == ["erp"]
    assert body["company_ids"] == [3]
    assert body["office_scopes"] == [{"company_id": 3, "office_id": 1}]


def test_deactivated_still_gated_by_enrich_when_flag_on(monkeypatch):
    """Revocación active: sigue opt-in; documentamos que PWA cutover debe activarla."""
    monkeypatch.setenv("AUTH_CHECK_ACTIVE_ON_REQUEST", "1")

    def fake_state(user_id, email):
        return {
            "id": 7,
            "active": False,
            "token_version": 1,
            "role": "operativo",
            "email": email,
            "has_token_version_column": True,
        }

    monkeypatch.setattr(
        "backend.auth.staff_session.fetch_staff_session_state", fake_state
    )
    from backend.auth.staff_session import enrich_staff_payload

    with pytest.raises(HTTPException) as err:
        enrich_staff_payload({"email": "x@test.local", "role": "operativo", "uid": 7})
    assert err.value.status_code == 401


def test_scope_public_dict_legacy(monkeypatch):
    monkeypatch.setattr(
        "backend.auth.multiapp.load_multiapp_scope",
        lambda _uid: _scope(tables_ready=True, apps_rows_present=False),
    )
    pub = scope_public_dict({"email": "a@test.local", "role": "gerencia", "id": 1})
    assert pub["has_erp_access"] is True
    assert pub["has_pwa_access"] is False
    assert "erp" in pub["apps"]
