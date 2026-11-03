"""Seguridad: driver-access no permite sondear choferes ajenos."""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.auth.jwt import create_staff_token
from backend.auth.multiapp import MultiAppScope
from backend.routers import distribuidora_dispatch_plan


@pytest.fixture()
def client(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "development")
    monkeypatch.delenv("AUTH_OWNER_USER_IDS", raising=False)

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
        lambda _uid: MultiAppScope(tables_ready=True, apps_rows_present=False),
    )
    monkeypatch.setattr(
        "backend.services.distribuidora.dispatch_plan_driver_service.driver_has_access_to_plan",
        lambda *_a, **_k: True,
    )
    app = FastAPI()
    app.include_router(distribuidora_dispatch_plan.router)
    return TestClient(app)


def _auth(role: str, user_id: int) -> dict:
    token = create_staff_token(
        email=f"u{user_id}@test.local",
        role=role,
        user_id=user_id,
        token_version=0,
    )
    return {"Authorization": f"Bearer {token}"}


def test_non_assigner_cannot_probe_other_driver(client):
    res = client.get(
        "/distribuidora/dispatch-plans/10/driver-access/99",
        headers=_auth("vendedor", 7),
    )
    assert res.status_code == 403


def test_non_assigner_can_check_self(client):
    res = client.get(
        "/distribuidora/dispatch-plans/10/driver-access/7",
        headers=_auth("vendedor", 7),
    )
    assert res.status_code == 200
    assert res.json()["has_access"] is True


def test_assigner_can_probe_any(client):
    res = client.get(
        "/distribuidora/dispatch-plans/10/driver-access/99",
        headers=_auth("gerencia", 1),
    )
    assert res.status_code == 200
