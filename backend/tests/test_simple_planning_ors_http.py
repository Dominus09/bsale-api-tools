"""HTTP: calculate-route capability-only vs scope."""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.auth.jwt import create_staff_token
from backend.auth.multiapp import MultiAppScope
from backend.routers import distribuidora_simple_planning


def _stub_erp(monkeypatch, *, erp_capabilities=None):
    caps = frozenset(erp_capabilities or ())

    def _enrich(p):
        return {
            **p,
            "id": p.get("uid") or 1,
            "uid": p.get("uid") or 1,
            "email": p.get("email"),
            "role": p.get("role"),
            "erp_capabilities": sorted(caps),
        }

    monkeypatch.setattr("backend.auth.multiapp.enrich_staff_payload", _enrich)
    monkeypatch.setattr(
        "backend.auth.multiapp.load_multiapp_scope",
        lambda _uid: MultiAppScope(
            tables_ready=True,
            apps_rows_present=False,
            erp_capabilities=caps,
        ),
    )


@pytest.fixture()
def client(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "development")
    monkeypatch.delenv("AUTH_OWNER_USER_IDS", raising=False)
    _stub_erp(monkeypatch, erp_capabilities={"simple_planning"})
    monkeypatch.setattr(
        "backend.routers.distribuidora_simple_planning._company_office_exist_in_catalog",
        lambda *_a, **_k: None,
    )
    app = FastAPI()
    app.include_router(distribuidora_simple_planning.router)
    return TestClient(app)


def _auth(role: str = "vendedor", user_id: int = 10) -> dict:
    token = create_staff_token(
        email="planner@test.local",
        role=role,
        user_id=user_id,
        token_version=0,
    )
    return {"Authorization": f"Bearer {token}"}


def test_calculate_route_capability_ok(client, monkeypatch):
    monkeypatch.setattr(
        "backend.routers.distribuidora_simple_planning.calculate_simple_planning_route",
        lambda **_k: {
            "ok": True,
            "ors_metrics_calculated": True,
            "truck_id": 2,
            "distance_km": 10.0,
            "duration_min": 20.0,
            "ors_snapshot": {"route_fingerprint": "x", "truck_id": 2},
        },
    )
    res = client.post(
        "/distribuidora/simple-planning/calculate-route",
        headers=_auth("vendedor"),
        json={
            "company_id": 3,
            "office_id": 1,
            "truck_id": 2,
            "document_ids": [10, 20],
            "camion": "Hyundai",
        },
    )
    assert res.status_code == 200
    assert res.json()["ors_metrics_calculated"] is True


def test_calculate_route_vendedor_without_capability_denied(client, monkeypatch):
    _stub_erp(monkeypatch, erp_capabilities=())
    res = client.post(
        "/distribuidora/simple-planning/calculate-route",
        headers=_auth("vendedor"),
        json={
            "company_id": 3,
            "office_id": 1,
            "truck_id": 2,
            "document_ids": [10],
        },
    )
    assert res.status_code == 403
