"""Flags de /auth/me para el módulo Usuarios (sin BD)."""

from __future__ import annotations

from backend.auth.permissions import build_auth_me_response


def test_auth_me_includes_user_admin_flags(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.setenv("AUTH_USER_ADMIN_EMAILS", "dueno@x.cl")
    monkeypatch.setenv("AUTH_ASSIGN_ADM_EMAILS", "dueno@x.cl")
    monkeypatch.setattr(
        "backend.auth.permissions._fetch_staff_user_id", lambda email: 7
    )
    out = build_auth_me_response({"email": "dueno@x.cl", "role": "gerencia"})
    assert out["permissions"]["user_admin_view"] is True
    assert out["permissions"]["user_admin"] is True
    assert out["permissions"]["user_admin_assign_adm"] is True


def test_auth_me_adm_without_allowlist_cannot_mutate(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.delenv("AUTH_USER_ADMIN_EMAILS", raising=False)
    monkeypatch.delenv("AUTH_ASSIGN_ADM_EMAILS", raising=False)
    import backend.auth.user_admin_access as ua

    ua._warned_empty_allowlist = False
    monkeypatch.setattr(
        "backend.auth.permissions._fetch_staff_user_id", lambda email: 1
    )
    out = build_auth_me_response({"email": "adm@x.cl", "role": "adm"})
    assert out["permissions"]["user_admin_view"] is True
    assert out["permissions"]["user_admin"] is False
    assert out["permissions"]["user_admin_assign_adm"] is False


def test_auth_me_gerencia_empty_allowlist_view_only(monkeypatch):
    """Gerencia ve el módulo; sin allowlist no muta ni asigna adm."""
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.delenv("AUTH_USER_ADMIN_EMAILS", raising=False)
    monkeypatch.delenv("AUTH_ASSIGN_ADM_EMAILS", raising=False)
    import backend.auth.user_admin_access as ua

    ua._warned_empty_allowlist = False
    monkeypatch.setattr(
        "backend.auth.permissions._fetch_staff_user_id", lambda email: 9
    )
    out = build_auth_me_response({"email": "gerencia@x.cl", "role": "gerencia"})
    assert out["permissions"]["user_admin_view"] is True
    assert out["permissions"]["user_admin"] is False
    assert out["permissions"]["user_admin_assign_adm"] is False


def test_auth_me_operativo_no_user_admin_flags(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.setattr(
        "backend.auth.permissions._fetch_staff_user_id", lambda email: 3
    )
    out = build_auth_me_response({"email": "op@x.cl", "role": "operativo"})
    assert out["permissions"]["user_admin_view"] is False
    assert out["permissions"]["user_admin"] is False
    assert out["permissions"]["user_admin_assign_adm"] is False
