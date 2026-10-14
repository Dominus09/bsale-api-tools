"""Vencimiento de JWT preparado como opt-in: sin variables, el comportamiento actual no cambia."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import jwt as pyjwt
import pytest
from fastapi import HTTPException

from backend.auth import jwt as staff_jwt


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    monkeypatch.delenv("JWT_EXPIRE_MINUTES", raising=False)
    monkeypatch.delenv("JWT_REQUIRE_EXP", raising=False)


def _bearer(token: str) -> str:
    return f"Bearer {token}"


def test_default_tokens_keep_current_behavior_without_exp():
    token = staff_jwt.create_staff_token(email="a@test.local", role="gerencia")
    payload = staff_jwt.decode_staff_token(_bearer(token))
    assert "exp" not in payload and payload["role"] == "gerencia"


def test_expire_minutes_adds_exp_and_iat(monkeypatch):
    monkeypatch.setenv("JWT_EXPIRE_MINUTES", "60")
    token = staff_jwt.create_staff_token(email="a@test.local", role="gerencia")
    payload = staff_jwt.decode_staff_token(_bearer(token))
    assert 3500 <= payload["exp"] - payload["iat"] <= 3600


@pytest.mark.parametrize("value", ["0", "-5", "abc", ""])
def test_invalid_or_disabled_lifetime_is_ignored(monkeypatch, value):
    monkeypatch.setenv("JWT_EXPIRE_MINUTES", value)
    assert staff_jwt.token_lifetime_minutes() is None


def test_expired_token_is_rejected_as_expired_session():
    past = datetime.now(timezone.utc) - timedelta(minutes=5)
    token = pyjwt.encode({"email": "a@test.local", "role": "gerencia", "exp": past}, staff_jwt.SECRET, algorithm="HS256")
    with pytest.raises(HTTPException) as err:
        staff_jwt.decode_staff_token(_bearer(token))
    assert err.value.status_code == 401 and err.value.detail == "Sesión expirada"


def test_require_exp_rejects_legacy_tokens_only_when_enabled(monkeypatch):
    legacy = staff_jwt.create_staff_token(email="a@test.local", role="gerencia")
    assert staff_jwt.decode_staff_token(_bearer(legacy))["email"] == "a@test.local"
    monkeypatch.setenv("JWT_REQUIRE_EXP", "true")
    with pytest.raises(HTTPException) as err:
        staff_jwt.decode_staff_token(_bearer(legacy))
    assert err.value.status_code == 401
    monkeypatch.setenv("JWT_EXPIRE_MINUTES", "30")
    fresh = staff_jwt.create_staff_token(email="a@test.local", role="gerencia")
    assert staff_jwt.decode_staff_token(_bearer(fresh))["email"] == "a@test.local"


def test_staff_email_helper_returns_none_for_expired_token():
    past = datetime.now(timezone.utc) - timedelta(minutes=1)
    token = pyjwt.encode({"email": "a@test.local", "exp": past}, staff_jwt.SECRET, algorithm="HS256")
    assert staff_jwt.staff_email_from_authorization(_bearer(token)) is None
