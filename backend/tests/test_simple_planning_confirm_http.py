"""HTTP prepare-confirm / confirm planificación simplificada."""

from __future__ import annotations

from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.auth.jwt import create_staff_token
from backend.auth.multiapp import MultiAppScope
from backend.routers import distribuidora_simple_planning
from backend.services.distribuidora.simple_planning_confirm_service import (
    SimplePlanningConfirmError,
)


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
    _stub_erp(monkeypatch)
    monkeypatch.setattr(
        "backend.routers.distribuidora_simple_planning._company_office_exist_in_catalog",
        lambda *_a, **_k: None,
    )
    app = FastAPI()
    app.include_router(distribuidora_simple_planning.router)
    return TestClient(app)


def _auth(role: str = "operativo", user_id: int = 10) -> dict:
    token = create_staff_token(
        email="planner@test.local",
        role=role,
        user_id=user_id,
        token_version=0,
    )
    return {"Authorization": f"Bearer {token}"}


def test_prepare_confirm_ok(client, monkeypatch):
    monkeypatch.setattr(
        "backend.routers.distribuidora_simple_planning.prepare_confirm_session",
        lambda **_k: {
            "status": "created",
            "session": {"session_id": str(uuid4()), "truck_id": 2},
        },
    )
    res = client.post(
        "/distribuidora/simple-planning/prepare-confirm",
        headers=_auth("operativo"),
        json={
            "company_id": 3,
            "office_id": 1,
            "truck_id": 2,
            "planning_date": "2026-11-10",
            "assignments": [{"folio": 1, "truck_id": 2}],
        },
    )
    assert res.status_code == 200
    assert res.json()["status"] == "created"


def test_confirm_ok(client, monkeypatch):
    sid = str(uuid4())
    monkeypatch.setattr(
        "backend.routers.distribuidora_simple_planning.confirm_simple_planning_session",
        lambda **_k: {
            "status": "confirmed",
            "wrote": True,
            "planning_origin": "simple_list",
            "ors_metrics_calculated": False,
            "plan_id": 9,
        },
    )
    res = client.post(
        "/distribuidora/simple-planning/confirm",
        headers=_auth("gerencia"),
        json={"session_id": sid},
    )
    assert res.status_code == 200
    assert res.json()["ors_metrics_calculated"] is False


def test_confirm_maps_conflict(client, monkeypatch):
    def _boom(**_k):
        raise SimplePlanningConfirmError("dup", code="oc_conflict", http_status=409)

    monkeypatch.setattr(
        "backend.routers.distribuidora_simple_planning.confirm_simple_planning_session",
        _boom,
    )
    res = client.post(
        "/distribuidora/simple-planning/confirm",
        headers=_auth(),
        json={"session_id": str(uuid4())},
    )
    assert res.status_code == 409


def test_capability_user_allowed(client, monkeypatch):
    _stub_erp(monkeypatch, erp_capabilities={"simple_planning"})
    monkeypatch.setattr(
        "backend.routers.distribuidora_simple_planning.prepare_confirm_session",
        lambda **_k: {"status": "created", "session": {}},
    )
    res = client.post(
        "/distribuidora/simple-planning/prepare-confirm",
        headers=_auth("vendedor"),
        json={
            "company_id": 3,
            "office_id": 1,
            "truck_id": 1,
            "assignments": [{"folio": 1, "truck_id": 1}],
        },
    )
    assert res.status_code == 200


def test_vendedor_without_capability_denied(client):
    res = client.post(
        "/distribuidora/simple-planning/prepare-confirm",
        headers=_auth("vendedor"),
        json={
            "company_id": 3,
            "office_id": 1,
            "truck_id": 1,
            "assignments": [{"folio": 1, "truck_id": 1}],
        },
    )
    assert res.status_code == 403


def test_other_company_forbidden(client, monkeypatch):
    monkeypatch.setattr(
        "backend.auth.multiapp.load_multiapp_scope",
        lambda _uid: MultiAppScope(
            tables_ready=True,
            apps_rows_present=False,
            company_ids=(3,),
            offices=frozenset({(3, 1)}),
        ),
    )
    res = client.post(
        "/distribuidora/simple-planning/prepare-confirm",
        headers=_auth("gerencia", user_id=42),
        json={
            "company_id": 1,
            "office_id": 1,
            "truck_id": 1,
            "assignments": [{"folio": 1, "truck_id": 1}],
        },
    )
    assert res.status_code == 403
