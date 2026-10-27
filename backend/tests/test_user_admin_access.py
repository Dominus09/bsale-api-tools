"""Permisos del módulo Usuarios y accesos (sin BD)."""

from __future__ import annotations

from backend.auth import user_admin_access as ua


def test_view_allows_gerencia_and_adm():
    assert ua.can_view_user_admin({"role": "gerencia"})
    assert ua.can_view_user_admin({"role": "adm"})
    assert not ua.can_view_user_admin({"role": "operativo"})


def test_production_empty_allowlist_denies_all_mutations(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.delenv("AUTH_USER_ADMIN_EMAILS", raising=False)
    ua._warned_empty_allowlist = False
    assert not ua.can_mutate_user_admin({"role": "adm", "email": "a@x.cl"})
    assert not ua.can_mutate_user_admin({"role": "gerencia", "email": "g@x.cl"})


def test_staging_empty_allowlist_denies(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "staging")
    monkeypatch.delenv("AUTH_USER_ADMIN_EMAILS", raising=False)
    ua._warned_empty_allowlist = False
    assert not ua.can_mutate_user_admin({"role": "adm", "email": "a@x.cl"})


def test_dev_without_fallback_denies(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "development")
    monkeypatch.delenv("AUTH_USER_ADMIN_EMAILS", raising=False)
    monkeypatch.delenv("AUTH_USER_ADMIN_DEV_FALLBACK", raising=False)
    assert not ua.can_mutate_user_admin({"role": "adm", "email": "a@x.cl"})


def test_dev_fallback_opt_in(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "development")
    monkeypatch.delenv("AUTH_USER_ADMIN_EMAILS", raising=False)
    monkeypatch.setenv("AUTH_USER_ADMIN_DEV_FALLBACK", "1")
    assert ua.can_mutate_user_admin({"role": "adm", "email": "a@x.cl"})
    assert not ua.can_mutate_user_admin({"role": "gerencia", "email": "g@x.cl"})


def test_unset_environment_denies_even_with_dev_fallback_flag(monkeypatch):
    monkeypatch.delenv("ENVIRONMENT", raising=False)
    monkeypatch.delenv("ENV", raising=False)
    monkeypatch.setenv("AUTH_USER_ADMIN_DEV_FALLBACK", "1")
    monkeypatch.delenv("AUTH_USER_ADMIN_EMAILS", raising=False)
    ua._warned_empty_allowlist = False
    assert not ua.can_mutate_user_admin({"role": "adm", "email": "a@x.cl"})


def test_allowlist_grants_gerencia_in_production(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.setenv("AUTH_USER_ADMIN_EMAILS", "dueno@x.cl")
    assert ua.can_mutate_user_admin({"role": "gerencia", "email": "dueno@x.cl"})
    assert not ua.can_mutate_user_admin({"role": "adm", "email": "otro@x.cl"})


def test_assign_adm_requires_explicit_grant(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.setenv("AUTH_USER_ADMIN_EMAILS", "dueno@x.cl")
    monkeypatch.delenv("AUTH_ASSIGN_ADM_EMAILS", raising=False)
    monkeypatch.delenv("AUTH_OWNER_USER_IDS", raising=False)
    actor = {"id": 3, "role": "gerencia", "email": "dueno@x.cl"}
    assert ua.can_assign_role(actor, "operativo")
    assert not ua.can_assign_role(actor, "adm")
    assert not ua.can_assign_role(actor, "gerencia")
    monkeypatch.setenv("AUTH_ASSIGN_ADM_EMAILS", "dueno@x.cl")
    assert ua.can_assign_adm_role(actor)


def test_cannot_assign_superadmin(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.setenv("AUTH_USER_ADMIN_EMAILS", "a@x.cl")
    monkeypatch.setenv("AUTH_ASSIGN_ADM_EMAILS", "a@x.cl")
    monkeypatch.delenv("AUTH_OWNER_USER_IDS", raising=False)
    actor = {"id": 2, "role": "adm", "email": "a@x.cl"}
    assert not ua.can_assign_role(actor, "superadmin")
    assert not ua.can_assign_adm_role(actor)
