"""Fase 1A.1: fail-closed multiapp + guards ERP en rutas de alto riesgo."""

from __future__ import annotations

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from backend.auth.jwt import create_staff_token
from backend.auth.multiapp import (
    APP_ERP,
    APP_PWA,
    MultiAppScope,
    has_app_access,
    require_erp_app,
)
from backend.routers import companies, dashboard, distribuidora_planificacion, uploads
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


def _auth(role: str = "operativo", email: str = "erp@test.local", user_id: int = 10) -> dict:
    token = create_staff_token(email=email, role=role, user_id=user_id, token_version=0)
    return {"Authorization": f"Bearer {token}"}


def _patch_session(monkeypatch, *, user_id: int, scope: MultiAppScope, role: str = "operativo"):
    monkeypatch.setattr(
        "backend.auth.multiapp.decode_staff_token",
        lambda _a: {"email": "u@test.local", "role": role, "uid": user_id},
    )
    monkeypatch.setattr(
        "backend.auth.multiapp.enrich_staff_payload",
        lambda p: {**p, "id": user_id, "uid": user_id, "email": p.get("email"), "role": role},
    )
    monkeypatch.setattr(
        "backend.auth.multiapp.load_multiapp_scope",
        lambda _uid: scope,
    )


class _FakeConn:
    def cursor(self):
        return self

    def execute(self, *_a, **_k):
        return None

    def fetchall(self):
        return [(3, "Cristal")]

    def close(self):
        return None


def _mount_hardening_app() -> FastAPI:
    app = FastAPI()
    app.include_router(companies.router)
    app.include_router(dashboard.router)
    app.include_router(distribuidora_planificacion.router)
    app.include_router(uploads.router)
    return app


def test_load_failed_never_grants_app_access():
    scope = _scope(load_failed=True, tables_ready=True, apps_rows_present=False)
    assert has_app_access(scope, APP_ERP) is False
    assert has_app_access(scope, APP_PWA) is False


def test_require_erp_app_503_when_scope_unavailable(monkeypatch):
    _patch_session(monkeypatch, user_id=42, scope=_scope(load_failed=True))
    with pytest.raises(HTTPException) as err:
        require_erp_app("Bearer x")
    assert err.value.status_code == 503


def test_pre_migration_and_empty_apps_still_erp():
    assert has_app_access(_scope(tables_ready=False), APP_ERP) is True
    assert (
        has_app_access(_scope(tables_ready=True, apps_rows_present=False), APP_ERP) is True
    )


def test_private_routes_401_without_auth(monkeypatch):
    """Sin Bearer: decode real → 401 (no mock de sesión)."""
    monkeypatch.setattr(
        "backend.routers.companies.get_connection",
        lambda: _FakeConn(),
    )
    client = TestClient(_mount_hardening_app())
    assert client.get("/companies").status_code == 401
    assert client.get("/dashboard/3").status_code == 401
    assert client.get("/distribuidora/planificacion/fuel-config").status_code == 401
    assert client.post("/upload/suppliers", files={}).status_code == 401


def test_erp_legacy_keeps_access_to_companies_and_planificacion(monkeypatch):
    _patch_session(
        monkeypatch,
        user_id=10,
        scope=_scope(tables_ready=True, apps_rows_present=False),
    )
    monkeypatch.setattr(
        "backend.routers.companies.get_connection",
        lambda: _FakeConn(),
    )
    monkeypatch.setattr(
        "backend.routers.distribuidora_planificacion.get_fuel_config",
        lambda: {"ok": True},
    )
    # fuel-config may call service differently — stub at module if needed
    client = TestClient(_mount_hardening_app())
    res = client.get("/companies", headers=_auth())
    assert res.status_code == 200
    assert res.json()[0]["company_id"] == 3

    fuel = client.get("/distribuidora/planificacion/fuel-config", headers=_auth())
    # 200 con stub, o 500 si el servicio real falta — nunca 401/403 para legacy ERP
    assert fuel.status_code not in (401, 403)


def test_pwa_only_gets_403_on_erp_routes(monkeypatch):
    _patch_session(
        monkeypatch,
        user_id=11,
        scope=_scope(
            tables_ready=True,
            apps_rows_present=True,
            apps=frozenset({APP_PWA}),
            profiles=frozenset({"chofer"}),
        ),
    )
    monkeypatch.setattr(
        "backend.routers.companies.get_connection",
        lambda: _FakeConn(),
    )
    monkeypatch.setattr("backend.routers.dashboard.noco_get", lambda *_a, **_k: [])
    client = TestClient(_mount_hardening_app())
    assert client.get("/companies", headers=_auth(user_id=11)).status_code == 403
    assert client.get("/dashboard/3", headers=_auth(user_id=11)).status_code == 403
    assert (
        client.get("/distribuidora/planificacion/fuel-config", headers=_auth(user_id=11)).status_code
        == 403
    )


def test_other_company_denied_when_scoped(monkeypatch):
    _patch_session(
        monkeypatch,
        user_id=10,
        scope=_scope(
            tables_ready=True,
            apps_rows_present=False,
            company_ids=(3,),
        ),
    )
    monkeypatch.setattr("backend.routers.dashboard.noco_get", lambda *_a, **_k: [])
    client = TestClient(_mount_hardening_app())
    assert client.get("/dashboard/3", headers=_auth()).status_code == 200
    assert client.get("/dashboard/9", headers=_auth()).status_code == 403


def test_require_staff_user_alias_still_erp(monkeypatch):
    _patch_session(
        monkeypatch,
        user_id=10,
        scope=_scope(tables_ready=True, apps_rows_present=False),
    )
    user = require_staff_user("Bearer x")
    assert user["has_erp_access"] is True


def test_missing_multiapp_config_does_not_grant_pwa():
    """Sin filas / sin tablas: PWA sigue deny (fail-safe para cuentas nuevas)."""
    assert has_app_access(_scope(tables_ready=False), APP_PWA) is False
    assert (
        has_app_access(_scope(tables_ready=True, apps_rows_present=False), APP_PWA) is False
    )
