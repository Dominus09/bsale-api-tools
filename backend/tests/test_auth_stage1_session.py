"""JWT claims etapa 1 + validación opt-in de sesión (sin BD real)."""

from __future__ import annotations

import pytest
from fastapi import HTTPException

from backend.auth import jwt as staff_jwt
from backend.auth.staff_session import enrich_staff_payload


@pytest.fixture(autouse=True)
def _clean_session_flags(monkeypatch):
    monkeypatch.delenv("AUTH_CHECK_ACTIVE_ON_REQUEST", raising=False)
    monkeypatch.delenv("AUTH_ENFORCE_TOKEN_VERSION", raising=False)
    monkeypatch.delenv("JWT_EXPIRE_MINUTES", raising=False)
    monkeypatch.delenv("JWT_REQUIRE_EXP", raising=False)


def _bearer(token: str) -> str:
    return f"Bearer {token}"


def test_create_token_includes_uid_and_tv():
    token = staff_jwt.create_staff_token(
        email="a@test.local",
        role="gerencia",
        user_id=42,
        token_version=3,
    )
    payload = staff_jwt.decode_staff_token(_bearer(token))
    assert payload["uid"] == 42
    assert payload["id"] == 42
    assert payload["tv"] == 3


def test_create_token_with_company_ids():
    token = staff_jwt.create_staff_token(
        email="a@test.local",
        role="adm",
        user_id=1,
        company_ids=[1, 3],
    )
    payload = staff_jwt.decode_staff_token(_bearer(token))
    assert payload["company_ids"] == [1, 3]


def test_enrich_maps_uid_without_db_when_flags_off():
    out = enrich_staff_payload({"email": "a@test.local", "role": "adm", "uid": 7})
    assert out["id"] == 7 and out["uid"] == 7


def test_enrich_enforce_tv_rejects_missing_claim(monkeypatch):
    monkeypatch.setenv("AUTH_ENFORCE_TOKEN_VERSION", "1")

    def fake_state(user_id, email):
        return {
            "id": 7,
            "active": True,
            "token_version": 2,
            "role": "adm",
            "email": email,
            "has_token_version_column": True,
        }

    monkeypatch.setattr(
        "backend.auth.staff_session.fetch_staff_session_state", fake_state
    )
    with pytest.raises(HTTPException) as err:
        enrich_staff_payload({"email": "a@test.local", "role": "adm", "uid": 7})
    assert err.value.status_code == 401
    assert "revocada" in str(err.value.detail).lower() or "desactualizada" in str(
        err.value.detail
    ).lower()


def test_enrich_enforce_tv_accepts_matching_claim(monkeypatch):
    monkeypatch.setenv("AUTH_ENFORCE_TOKEN_VERSION", "1")

    def fake_state(user_id, email):
        return {
            "id": 7,
            "active": True,
            "token_version": 2,
            "role": "adm",
            "email": email,
            "has_token_version_column": True,
        }

    monkeypatch.setattr(
        "backend.auth.staff_session.fetch_staff_session_state", fake_state
    )
    out = enrich_staff_payload(
        {"email": "a@test.local", "role": "adm", "uid": 7, "tv": 2}
    )
    assert out["id"] == 7


def test_enrich_active_check_rejects_inactive(monkeypatch):
    monkeypatch.setenv("AUTH_CHECK_ACTIVE_ON_REQUEST", "1")

    def fake_state(user_id, email):
        return {
            "id": 7,
            "active": False,
            "token_version": 0,
            "role": "adm",
            "email": email,
            "has_token_version_column": False,
        }

    monkeypatch.setattr(
        "backend.auth.staff_session.fetch_staff_session_state", fake_state
    )
    with pytest.raises(HTTPException) as err:
        enrich_staff_payload({"email": "a@test.local", "role": "adm", "uid": 7})
    assert err.value.status_code == 401
    assert "desactivada" in str(err.value.detail).lower()


def test_legacy_token_without_uid_still_decodes():
    token = staff_jwt.create_staff_token(email="old@test.local", role="operativo")
    payload = staff_jwt.decode_staff_token(_bearer(token))
    assert "uid" not in payload
    assert payload["email"] == "old@test.local"
