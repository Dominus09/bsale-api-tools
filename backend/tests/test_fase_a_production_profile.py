"""Fase A: perfil equivalente a producción + migraciones ausentes (sin BD real)."""

from __future__ import annotations

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from backend.auth import user_admin_access as ua
from backend.auth.jwt import create_staff_token
from backend.routers import admin_users
from backend.services.user_admin import service as svc


@pytest.fixture()
def prod_env(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.delenv("AUTH_USER_ADMIN_EMAILS", raising=False)
    monkeypatch.delenv("AUTH_ASSIGN_ADM_EMAILS", raising=False)
    monkeypatch.delenv("AUTH_USER_ADMIN_DEV_FALLBACK", raising=False)
    monkeypatch.delenv("JWT_EXPIRE_MINUTES", raising=False)
    monkeypatch.delenv("JWT_REQUIRE_EXP", raising=False)
    monkeypatch.delenv("AUTH_CHECK_ACTIVE_ON_REQUEST", raising=False)
    monkeypatch.delenv("AUTH_ENFORCE_TOKEN_VERSION", raising=False)
    monkeypatch.delenv("AUTH_AUDIT_EVENTS", raising=False)
    monkeypatch.delenv("AUTH_EMIT_COMPANY_IDS", raising=False)
    monkeypatch.delenv("AUTH_REQUIRE_USER_COMPANIES", raising=False)
    ua._warned_empty_allowlist = False


@pytest.fixture()
def client(prod_env, monkeypatch):
    app = FastAPI()
    app.include_router(admin_users.router)
    app.include_router(admin_users.public_router)
    monkeypatch.setattr(
        "backend.auth.user_admin_access.enrich_staff_payload",
        lambda p: {**p, "id": p.get("uid") or 1, "email": p.get("email")},
    )
    return TestClient(app)


def _auth(role: str, email: str = "adm@test.local") -> dict:
    return {
        "Authorization": f"Bearer {create_staff_token(email=email, role=role, user_id=1)}"
    }


def test_unknown_env_is_protected(monkeypatch):
    monkeypatch.delenv("ENVIRONMENT", raising=False)
    monkeypatch.delenv("ENV", raising=False)
    monkeypatch.setenv("AUTH_USER_ADMIN_DEV_FALLBACK", "1")
    monkeypatch.delenv("AUTH_USER_ADMIN_EMAILS", raising=False)
    ua._warned_empty_allowlist = False
    assert ua._is_protected_runtime() is True
    assert not ua.can_mutate_user_admin({"role": "adm", "email": "a@x.cl"})


def test_garbage_env_is_protected(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "coolify-unset")
    monkeypatch.setenv("AUTH_USER_ADMIN_DEV_FALLBACK", "1")
    monkeypatch.delenv("AUTH_USER_ADMIN_EMAILS", raising=False)
    ua._warned_empty_allowlist = False
    assert not ua.can_mutate_user_admin({"role": "adm", "email": "a@x.cl"})


def test_prod_mutations_blocked(client, prod_env):
    assert (
        client.post(
            "/admin/users",
            headers=_auth("adm"),
            json={
                "email": "n@test.local",
                "role": "operativo",
                "invite": True,
                "company_ids": [],
            },
        ).status_code
        == 403
    )
    assert (
        client.patch(
            "/admin/users/1",
            headers=_auth("adm"),
            json={"active": False},
        ).status_code
        == 403
    )
    assert (
        client.post(
            "/admin/users/1/reset-password",
            headers=_auth("adm"),
        ).status_code
        == 403
    )


def test_prod_view_allowed_for_gerencia(client, prod_env, monkeypatch):
    monkeypatch.setattr(
        "backend.services.user_admin.service.list_users", lambda **k: []
    )
    assert client.get("/admin/users", headers=_auth("gerencia", "g@test.local")).status_code == 200


def test_missing_password_tokens_table_controlled_409(monkeypatch, prod_env):
    monkeypatch.setenv("AUTH_USER_ADMIN_EMAILS", "a@x.cl")
    monkeypatch.setattr(svc, "_table_exists", lambda cur, s, t: False)

    class _Conn:
        def cursor(self):
            return self

        def execute(self, *a, **k):
            return None

        def fetchone(self):
            return ("u@test.local", True, "operativo")

        def commit(self):
            return None

        def rollback(self):
            return None

        def close(self):
            return None

    monkeypatch.setattr(svc, "get_connection", lambda: _Conn())
    with pytest.raises(HTTPException) as err:
        svc.issue_password_reset(
            actor={"id": 1, "role": "adm", "email": "a@x.cl"},
            user_id=99,
        )
    assert err.value.status_code == 409


def test_auth_events_without_table(monkeypatch, prod_env):
    monkeypatch.setattr(svc, "_table_exists", lambda cur, s, t: False)

    class _Conn:
        def cursor(self):
            return self

        def close(self):
            return None

    monkeypatch.setattr(svc, "get_connection", lambda: _Conn())
    out = svc.list_auth_events()
    assert out["meta"]["available"] is False
    assert out["items"] == []


def test_consume_without_table_503(monkeypatch, prod_env):
    monkeypatch.setattr(svc, "_table_exists", lambda cur, s, t: False)

    class _Conn:
        def cursor(self):
            return self

        def rollback(self):
            return None

        def close(self):
            return None

    monkeypatch.setattr(svc, "get_connection", lambda: _Conn())
    with pytest.raises(HTTPException) as err:
        svc.consume_password_token(raw_token="abc", new_password="1234567890ab")
    assert err.value.status_code == 503


def test_jwt_without_expire_still_works(prod_env):
    from backend.auth import jwt as staff_jwt

    token = staff_jwt.create_staff_token(email="a@test.local", role="operativo")
    payload = staff_jwt.decode_staff_token(f"Bearer {token}")
    assert payload["email"] == "a@test.local"
    assert "exp" not in payload
