"""HTTP real con TestClient: orden de rutas, permisos y validaciones (sin BD real)."""

from __future__ import annotations

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from backend.auth.jwt import create_staff_token
from backend.auth.multiapp import MultiAppScope
from backend.routers import admin_users


def _legacy_erp_scope() -> MultiAppScope:
    return MultiAppScope(tables_ready=True, apps_rows_present=False)


def _stub_erp_session(monkeypatch):
    """Evita BD: JWT → usuario + alcance ERP legacy."""

    def _enrich(p):
        return {
            **p,
            "id": p.get("uid") or 1,
            "uid": p.get("uid") or 1,
            "email": p.get("email"),
            "role": p.get("role"),
        }

    monkeypatch.setattr("backend.auth.multiapp.enrich_staff_payload", _enrich)
    monkeypatch.setattr(
        "backend.auth.multiapp.load_multiapp_scope",
        lambda _uid: _legacy_erp_scope(),
    )


@pytest.fixture()
def app_client(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "development")
    monkeypatch.delenv("AUTH_USER_ADMIN_EMAILS", raising=False)
    monkeypatch.delenv("AUTH_ASSIGN_ADM_EMAILS", raising=False)
    monkeypatch.delenv("AUTH_USER_ADMIN_DEV_FALLBACK", raising=False)
    _stub_erp_session(monkeypatch)
    app = FastAPI()
    app.include_router(admin_users.router)
    app.include_router(admin_users.public_router)
    return TestClient(app)


def _auth(role: str, email: str = "actor@test.local", user_id: int = 1) -> dict:
    token = create_staff_token(email=email, role=role, user_id=user_id, token_version=0)
    return {"Authorization": f"Bearer {token}"}


def test_static_meta_routes_before_dynamic_id(app_client, monkeypatch):
    monkeypatch.setattr(
        "backend.services.user_admin.service.roles_catalog",
        lambda: {"assignable_roles": ["operativo"], "module_matrix": [], "notes": []},
    )
    monkeypatch.setattr(
        "backend.services.user_admin.service.list_companies_catalog",
        lambda: [{"company_id": 1, "name": "A", "active": True}],
    )
    monkeypatch.setattr(
        "backend.services.user_admin.service.list_auth_events",
        lambda **kwargs: {"items": [], "total": 0, "meta": {"available": False}},
    )
    monkeypatch.setattr(
        "backend.services.user_admin.service.multiapp_status",
        lambda: {
            "tables": {
                "user_apps": True,
                "user_operational_profiles": True,
                "user_offices": True,
            },
            "ready": True,
            "apps": [{"code": "erp", "label": "ERP"}, {"code": "pwa", "label": "PWA"}],
            "profiles": [{"code": "chofer", "label": "Chofer"}],
        },
    )
    monkeypatch.setattr(
        "backend.services.user_admin.service.list_offices_catalog",
        lambda **kwargs: [
            {"company_id": 1, "office_id": 10, "name": "Central"},
        ],
    )

    r1 = app_client.get("/admin/users/meta/roles", headers=_auth("gerencia"))
    assert r1.status_code == 200
    assert "assignable_roles" in r1.json()

    r2 = app_client.get("/admin/users/meta/companies", headers=_auth("gerencia"))
    assert r2.status_code == 200
    assert r2.json()["items"][0]["company_id"] == 1

    r_apps = app_client.get("/admin/users/meta/apps", headers=_auth("gerencia"))
    assert r_apps.status_code == 200
    assert r_apps.json()["ready"] is True
    assert {a["code"] for a in r_apps.json()["items"]} == {"erp", "pwa"}

    r_prof = app_client.get("/admin/users/meta/profiles", headers=_auth("gerencia"))
    assert r_prof.status_code == 200
    assert r_prof.json()["items"][0]["code"] == "chofer"

    r_off = app_client.get(
        "/admin/users/meta/offices?company_id=1", headers=_auth("gerencia")
    )
    assert r_off.status_code == 200
    assert r_off.json()["items"][0]["office_id"] == 10

    r3 = app_client.get("/admin/users/auth-events", headers=_auth("adm"))
    assert r3.status_code == 200
    assert r3.json()["meta"]["available"] is False


def test_operativo_forbidden_list(app_client):
    res = app_client.get("/admin/users", headers=_auth("operativo", "op@test.local", 9))
    assert res.status_code == 403


def test_gerencia_can_list_but_not_mutate_without_allowlist(app_client, monkeypatch):
    monkeypatch.setattr(
        "backend.services.user_admin.service.list_users",
        lambda **kwargs: [],
    )
    ok = app_client.get("/admin/users", headers=_auth("gerencia", "g@test.local", 2))
    assert ok.status_code == 200

    denied = app_client.post(
        "/admin/users",
        headers=_auth("gerencia", "g@test.local", 2),
        json={
            "email": "nuevo@test.local",
            "role": "operativo",
            "invite": True,
            "company_ids": [],
        },
    )
    assert denied.status_code == 403


def test_production_adm_denied_without_allowlist(app_client, monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.delenv("AUTH_USER_ADMIN_EMAILS", raising=False)
    import backend.auth.user_admin_access as ua

    ua._warned_empty_allowlist = False
    res = app_client.post(
        "/admin/users",
        headers=_auth("adm", "adm@test.local", 4),
        json={
            "email": "x@test.local",
            "role": "operativo",
            "invite": True,
            "company_ids": [],
        },
    )
    assert res.status_code == 403


def test_allowlist_grants_gerencia_mutate(app_client, monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.setenv("AUTH_USER_ADMIN_EMAILS", "dueno@test.local")

    def fake_create(**kwargs):
        return {"id": 99, "email": kwargs["email"], "role": kwargs["role"], "active": True}

    monkeypatch.setattr("backend.services.user_admin.service.create_user", fake_create)
    res = app_client.post(
        "/admin/users",
        headers=_auth("gerencia", "dueno@test.local", 5),
        json={
            "email": "nuevo@test.local",
            "role": "operativo",
            "invite": True,
            "company_ids": [],
        },
    )
    assert res.status_code == 200
    assert res.json()["id"] == 99


def test_allowlist_blocks_adm_not_listed(app_client, monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.setenv("AUTH_USER_ADMIN_EMAILS", "dueno@test.local")
    res = app_client.post(
        "/admin/users",
        headers=_auth("adm", "otro-adm@test.local", 4),
        json={
            "email": "x@test.local",
            "role": "operativo",
            "invite": True,
            "company_ids": [],
        },
    )
    assert res.status_code == 403


def test_assign_adm_requires_grant_list(app_client, monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.setenv("AUTH_USER_ADMIN_EMAILS", "dueno@test.local")
    monkeypatch.delenv("AUTH_ASSIGN_ADM_EMAILS", raising=False)

    def refuse(**kwargs):
        from backend.auth.user_admin_access import can_assign_role

        if not can_assign_role(kwargs["actor"], kwargs["role"]):
            raise HTTPException(status_code=403, detail="No autorizado a asignar este rol")
        return {"id": 1}

    monkeypatch.setattr("backend.services.user_admin.service.create_user", refuse)
    res = app_client.post(
        "/admin/users",
        headers=_auth("gerencia", "dueno@test.local", 5),
        json={"email": "a@test.local", "role": "adm", "invite": True, "company_ids": []},
    )
    assert res.status_code == 403


def test_consume_rate_limit(app_client, monkeypatch):
    monkeypatch.setenv("AUTH_PASSWORD_RESET_RATE_LIMIT", "3")
    monkeypatch.setenv("AUTH_PASSWORD_RESET_RATE_WINDOW_SEC", "600")

    def fake_consume(**kwargs):
        return {"ok": True, "email": "u@test.local"}

    monkeypatch.setattr(
        "backend.services.user_admin.service.consume_password_token", fake_consume
    )
    from backend.auth import rate_limit as rl

    with rl._lock:
        rl._buckets.clear()

    body = {"token": "x", "new_password": "1234567890ab"}
    assert app_client.post("/auth/password-reset/consume", json=body).status_code == 200
    assert app_client.post("/auth/password-reset/consume", json=body).status_code == 200
    assert app_client.post("/auth/password-reset/consume", json=body).status_code == 200
    limited = app_client.post("/auth/password-reset/consume", json=body)
    assert limited.status_code == 429


def test_get_user_id_route_still_works(app_client, monkeypatch):
    monkeypatch.setattr(
        "backend.services.user_admin.service.get_user",
        lambda uid: {"id": uid, "email": "u@test.local", "role": "operativo", "active": True},
    )
    res = app_client.get("/admin/users/42", headers=_auth("adm"))
    assert res.status_code == 200
    assert res.json()["id"] == 42


def test_unauthenticated_401(app_client):
    assert app_client.get("/admin/users").status_code == 401


def test_create_accepts_multiapp_fields(app_client, monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.setenv("AUTH_USER_ADMIN_EMAILS", "dueno@test.local")

    captured = {}

    def fake_create(**kwargs):
        captured.update(kwargs)
        return {
            "id": 55,
            "email": kwargs["email"],
            "role": kwargs["role"],
            "apps": kwargs.get("apps") or [],
            "profiles": kwargs.get("profiles") or [],
            "has_erp_access": "erp" in (kwargs.get("apps") or []),
            "has_pwa_access": "pwa" in (kwargs.get("apps") or []),
        }

    monkeypatch.setattr("backend.services.user_admin.service.create_user", fake_create)
    res = app_client.post(
        "/admin/users",
        headers=_auth("gerencia", "dueno@test.local", 5),
        json={
            "email": "chofer@test.local",
            "role": "operativo",
            "invite": True,
            "company_ids": [3],
            "apps": ["pwa"],
            "profiles": ["chofer"],
            "office_scopes": [{"company_id": 3, "office_id": 1}],
        },
    )
    assert res.status_code == 200
    assert captured["apps"] == ["pwa"]
    assert captured["profiles"] == ["chofer"]
    assert captured["office_scopes"] == [{"company_id": 3, "office_id": 1}]
    assert res.json()["has_erp_access"] is False
