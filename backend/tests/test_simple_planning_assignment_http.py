"""HTTP: assign-preview planificación simplificada."""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.auth.jwt import create_staff_token
from backend.auth.multiapp import MultiAppScope
from backend.routers import distribuidora_simple_planning


def _legacy_erp_scope() -> MultiAppScope:
    return MultiAppScope(tables_ready=True, apps_rows_present=False)


def _stub_erp(monkeypatch):
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
        lambda _uid: _legacy_erp_scope(),
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


def test_assign_preview_ok_readonly(client, monkeypatch):
    monkeypatch.setattr(
        "backend.routers.distribuidora_simple_planning.preview_truck_assignments",
        lambda **_k: {
            "status": "completed",
            "wrote": False,
            "mode": "simple_planning_assign_preview_readonly",
            "planning_origin": "simple_list",
            "trucks": [],
            "blocking_count": 0,
        },
    )
    res = client.post(
        "/distribuidora/simple-planning/assign-preview",
        headers=_auth("operativo"),
        json={
            "company_id": 3,
            "office_id": 1,
            "planning_date": "2026-11-10",
            "dispatch_name": "Sur",
            "assignments": [{"folio": 100, "truck_id": 2, "driver_name": "Juan"}],
        },
    )
    assert res.status_code == 200
    body = res.json()
    assert body["wrote"] is False
    assert body["planning_origin"] == "simple_list"


def test_assign_preview_rejects_write_flag(client, monkeypatch):
    monkeypatch.setattr(
        "backend.routers.distribuidora_simple_planning.preview_truck_assignments",
        lambda **_k: {"wrote": True},
    )
    res = client.post(
        "/distribuidora/simple-planning/assign-preview",
        headers=_auth(),
        json={
            "company_id": 3,
            "office_id": 1,
            "assignments": [{"folio": 1, "truck_id": 1}],
        },
    )
    assert res.status_code == 500


def test_assign_other_company_forbidden(client, monkeypatch):
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
        "/distribuidora/simple-planning/assign-preview",
        headers=_auth("gerencia", user_id=42),
        json={
            "company_id": 1,
            "office_id": 1,
            "assignments": [{"folio": 1, "truck_id": 1}],
        },
    )
    assert res.status_code == 403


def test_assign_unauthenticated(client):
    res = client.post(
        "/distribuidora/simple-planning/assign-preview",
        json={
            "company_id": 3,
            "office_id": 1,
            "assignments": [{"folio": 1, "truck_id": 1}],
        },
    )
    assert res.status_code in (401, 403)


def test_vendedor_denied_without_planner_profile(client):
    res = client.post(
        "/distribuidora/simple-planning/assign-preview",
        headers=_auth("vendedor"),
        json={
            "company_id": 3,
            "office_id": 1,
            "assignments": [{"folio": 1, "truck_id": 1}],
        },
    )
    assert res.status_code == 403


def test_operativo_allowed_for_preview(client, monkeypatch):
    monkeypatch.setattr(
        "backend.routers.distribuidora_simple_planning.preview_ocs_from_folios",
        lambda **_k: {"wrote": False, "items": [], "blocking_count": 0},
    )
    res = client.post(
        "/distribuidora/simple-planning/preview",
        headers=_auth("operativo"),
        json={"company_id": 3, "office_id": 1, "folios": ["1"]},
    )
    assert res.status_code == 200
