"""HTTP: asignación de chofer a despacho."""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.auth.jwt import create_staff_token
from backend.auth.multiapp import MultiAppScope
from backend.routers import distribuidora_dispatch_plan
from backend.services.distribuidora.dispatch_plan_driver_service import DriverAssignError


def _stub(monkeypatch):
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


@pytest.fixture()
def client(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "development")
    monkeypatch.delenv("AUTH_OWNER_USER_IDS", raising=False)
    _stub(monkeypatch)
    app = FastAPI()
    app.include_router(distribuidora_dispatch_plan.router)
    return TestClient(app)


def _auth(role: str = "gerencia", user_id: int = 10) -> dict:
    token = create_staff_token(
        email="boss@test.local",
        role=role,
        user_id=user_id,
        token_version=0,
    )
    return {"Authorization": f"Bearer {token}"}


def test_assign_driver_ok(client, monkeypatch):
    monkeypatch.setattr(
        "backend.services.distribuidora.dispatch_plan_driver_service.assign_driver_to_plan",
        lambda **_k: {
            "status": "assigned",
            "plan_id": 5,
            "driver_user_id": 9,
            "same_day_conflicts": [],
        },
    )
    res = client.post(
        "/distribuidora/dispatch-plans/5/assign-driver",
        headers=_auth("gerencia"),
        json={"driver_user_id": 9, "reason": "Asignación ruta"},
    )
    assert res.status_code == 200
    assert res.json()["status"] == "assigned"


def test_assign_maps_business_error(client, monkeypatch):
    def _boom(**_k):
        raise DriverAssignError("sin perfil", code="chofer_profile_required", http_status=409)

    monkeypatch.setattr(
        "backend.services.distribuidora.dispatch_plan_driver_service.assign_driver_to_plan",
        _boom,
    )
    res = client.post(
        "/distribuidora/dispatch-plans/5/assign-driver",
        headers=_auth(),
        json={"driver_user_id": 9, "reason": "Asignación ruta"},
    )
    assert res.status_code == 409


def test_available_drivers_forbidden_for_random_role(client):
    res = client.get(
        "/distribuidora/dispatch-plans/available-drivers",
        headers=_auth("vendedor"),
    )
    assert res.status_code == 403
