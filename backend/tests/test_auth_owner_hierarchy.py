"""Jerarquía OWNER / GERENCIA / ADM / OPERATIVO (sin BD)."""

from __future__ import annotations

import pytest
from fastapi import HTTPException

from backend.auth import user_admin_access as ua
from backend.auth.hierarchy import is_owner
from backend.auth.permissions import build_auth_me_response, has_admin_access


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.delenv("AUTH_OWNER_USER_IDS", raising=False)
    monkeypatch.delenv("AUTH_USER_ADMIN_EMAILS", raising=False)
    monkeypatch.delenv("AUTH_ASSIGN_ADM_EMAILS", raising=False)
    monkeypatch.delenv("AUTH_USER_ADMIN_DEV_FALLBACK", raising=False)
    ua._warned_empty_allowlist = False


def test_owner_unset_nobody_is_owner(monkeypatch):
    monkeypatch.delenv("AUTH_OWNER_USER_IDS", raising=False)
    assert not is_owner({"id": 1, "role": "gerencia", "email": "c@x.cl"})
    assert not ua.can_mutate_user_admin({"id": 1, "role": "gerencia", "email": "c@x.cl"})


def test_owner_configured_grants_mutate_and_assign_adm(monkeypatch):
    monkeypatch.setenv("AUTH_OWNER_USER_IDS", "42")
    actor = {"id": 42, "role": "gerencia", "email": "carlos@x.cl"}
    assert is_owner(actor)
    assert ua.can_mutate_user_admin(actor)
    assert ua.can_assign_adm_role(actor)
    assert ua.can_assign_role(actor, "gerencia")
    assert ua.can_assign_role(actor, "adm")
    assert ua.can_assign_role(actor, "operativo")
    assert not ua.can_assign_role(actor, "owner")


def test_auth_me_exposes_is_owner(monkeypatch):
    monkeypatch.setenv("AUTH_OWNER_USER_IDS", "7")
    monkeypatch.setattr(
        "backend.auth.permissions._fetch_staff_user_id", lambda email: 7
    )
    out = build_auth_me_response({"email": "c@x.cl", "role": "gerencia", "id": 7})
    assert out["is_owner"] is True
    assert out["permissions"]["user_admin"] is True
    assert out["permissions"]["user_admin_assign_adm"] is True
    assert has_admin_access({"id": 7, "role": "gerencia"})


def test_adm_cannot_create_gerencia(monkeypatch):
    monkeypatch.setenv("AUTH_USER_ADMIN_EMAILS", "adm@x.cl")
    monkeypatch.setenv("AUTH_ASSIGN_ADM_EMAILS", "adm@x.cl")
    actor = {"id": 2, "role": "adm", "email": "adm@x.cl"}
    assert ua.can_mutate_user_admin(actor)
    assert not ua.can_assign_role(actor, "gerencia")
    assert not ua.can_assign_adm_role(actor)


def test_adm_cannot_manage_other_adm(monkeypatch):
    monkeypatch.setenv("AUTH_USER_ADMIN_EMAILS", "adm@x.cl")
    actor = {"id": 2, "role": "adm", "email": "adm@x.cl"}
    assert ua.can_manage_subject(actor, target_id=9, target_role="operativo")
    assert not ua.can_manage_subject(actor, target_id=8, target_role="adm")
    assert not ua.can_manage_subject(actor, target_id=3, target_role="gerencia")


def test_adm_cannot_self_promote(monkeypatch):
    monkeypatch.setenv("AUTH_USER_ADMIN_EMAILS", "adm@x.cl")
    monkeypatch.setenv("AUTH_ASSIGN_ADM_EMAILS", "g@x.cl")
    actor = {"id": 2, "role": "adm", "email": "adm@x.cl"}
    with pytest.raises(HTTPException) as ei:
        ua.assert_panel_action_allowed(
            actor,
            target_id=2,
            current_role="adm",
            new_role="gerencia",
        )
    assert ei.value.status_code == 403


def test_gerencia_cannot_modify_owner(monkeypatch):
    monkeypatch.setenv("AUTH_OWNER_USER_IDS", "1")
    monkeypatch.setenv("AUTH_USER_ADMIN_EMAILS", "g@x.cl")
    actor = {"id": 5, "role": "gerencia", "email": "g@x.cl"}
    assert not ua.can_manage_subject(actor, target_id=1, target_role="gerencia")
    with pytest.raises(HTTPException) as ei:
        ua.assert_panel_action_allowed(
            actor,
            target_id=1,
            current_role="gerencia",
            new_role="operativo",
        )
    assert ei.value.status_code == 403


def test_gerencia_cannot_create_gerencia(monkeypatch):
    monkeypatch.setenv("AUTH_USER_ADMIN_EMAILS", "g@x.cl")
    actor = {"id": 5, "role": "gerencia", "email": "g@x.cl"}
    assert not ua.can_assign_role(actor, "gerencia")
    assert ua.can_assign_role(actor, "operativo")


def test_gerencia_with_assign_adm_can_create_adm(monkeypatch):
    monkeypatch.setenv("AUTH_USER_ADMIN_EMAILS", "g@x.cl")
    monkeypatch.setenv("AUTH_ASSIGN_ADM_EMAILS", "g@x.cl")
    actor = {"id": 5, "role": "gerencia", "email": "g@x.cl"}
    assert ua.can_assign_role(actor, "adm")


def test_cannot_reset_owner_password(monkeypatch):
    monkeypatch.setenv("AUTH_OWNER_USER_IDS", "1")
    monkeypatch.setenv("AUTH_USER_ADMIN_EMAILS", "g@x.cl")
    actor = {"id": 5, "role": "gerencia", "email": "g@x.cl"}
    with pytest.raises(HTTPException) as ei:
        ua.assert_panel_action_allowed(
            actor,
            target_id=1,
            current_role="gerencia",
            password_reset=True,
        )
    assert ei.value.status_code == 403


def test_owner_can_manage_gerencia_and_companies(monkeypatch):
    monkeypatch.setenv("AUTH_OWNER_USER_IDS", "1")
    actor = {"id": 1, "role": "gerencia", "email": "c@x.cl"}
    assert ua.can_manage_subject(actor, target_id=9, target_role="gerencia")
    ua.assert_panel_action_allowed(
        actor,
        target_id=9,
        current_role="gerencia",
        changing_companies=True,
    )


def test_frontend_claim_is_owner_ignored_without_id(monkeypatch):
    monkeypatch.setenv("AUTH_OWNER_USER_IDS", "1")
    # Payload malicioso sin id real no obtiene OWNER.
    fake = {"role": "gerencia", "email": "x@x.cl", "is_owner": True}
    assert not is_owner(fake)
