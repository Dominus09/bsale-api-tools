"""HTTP: organización anticipada de carga — permisos, scopes y 409."""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.auth.jwt import create_staff_token
from backend.auth.multiapp import (
    APP_ERP,
    CAP_SIMPLE_PLANNING,
    MultiAppScope,
)
from backend.routers import distribuidora_dispatch_plan
from backend.services.distribuidora.dispatch_plan_load_organization_service import (
    LoadOrganizationConflictError,
)


def _stub(monkeypatch):
    def _enrich(p):
        role = p.get("role")
        base = {
            **p,
            "id": p.get("uid") or 1,
            "uid": p.get("uid") or 1,
            "email": p.get("email"),
            "role": role,
        }
        if role == "planificador_excel":
            scope = MultiAppScope(
                tables_ready=True,
                apps_rows_present=True,
                apps=frozenset({APP_ERP}),
                erp_capabilities=frozenset({CAP_SIMPLE_PLANNING}),
            )
            base["erp_capabilities"] = [CAP_SIMPLE_PLANNING]
            base["_multiapp_scope"] = scope
        return base

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


def _auth(role: str = "operativo", user_id: int = 10) -> dict:
    token = create_staff_token(
        email=f"{role}@test.local",
        role=role,
        user_id=user_id,
        token_version=0,
    )
    return {"Authorization": f"Bearer {token}"}


def test_get_load_organization_authorized(client, monkeypatch):
    monkeypatch.setattr(
        "backend.routers.distribuidora_dispatch_plan.user_can_read_dispatch_plan",
        lambda *_a, **_k: True,
    )
    monkeypatch.setattr(
        "backend.routers.distribuidora_dispatch_plan.load_org_svc.get_load_organization",
        lambda plan_id: {
            "dispatch_plan_id": plan_id,
            "schema_ready": True,
            "load_organization_status": "legacy",
            "load_organization_version": 0,
            "batches": [],
            "clients": [],
            "group_summaries": [],
            "assignments": [],
            "completeness": {
                "total_orders": 0,
                "assigned_orders": 0,
                "unassigned_orders": 0,
                "is_complete": False,
                "missing": [],
            },
        },
    )
    res = client.get(
        "/distribuidora/dispatch-plans/42/load-organization",
        headers=_auth("operativo"),
    )
    assert res.status_code == 200
    assert res.json()["dispatch_plan_id"] == 42


def test_get_load_organization_out_of_scope_404(client, monkeypatch):
    monkeypatch.setattr(
        "backend.routers.distribuidora_dispatch_plan.user_can_read_dispatch_plan",
        lambda *_a, **_k: False,
    )
    res = client.get(
        "/distribuidora/dispatch-plans/99/load-organization",
        headers=_auth("operativo"),
    )
    assert res.status_code == 404


def test_put_load_organization_forbidden_capability_only(client):
    res = client.put(
        "/distribuidora/dispatch-plans/42/load-organization",
        headers=_auth("planificador_excel"),
        json={"assignments": [], "organization_version": 0, "mark_organized": False},
    )
    assert res.status_code == 403


def test_put_load_organization_out_of_scope_404(client, monkeypatch):
    monkeypatch.setattr(
        "backend.routers.distribuidora_dispatch_plan.user_can_read_dispatch_plan",
        lambda *_a, **_k: False,
    )
    res = client.put(
        "/distribuidora/dispatch-plans/42/load-organization",
        headers=_auth("operativo"),
        json={"assignments": [], "organization_version": 0, "mark_organized": False},
    )
    assert res.status_code == 404


def test_put_load_organization_version_conflict_409(client, monkeypatch):
    monkeypatch.setattr(
        "backend.routers.distribuidora_dispatch_plan.user_can_read_dispatch_plan",
        lambda *_a, **_k: True,
    )

    def _boom(*_a, **_k):
        raise LoadOrganizationConflictError("versión", code="version_conflict")

    monkeypatch.setattr(
        "backend.routers.distribuidora_dispatch_plan.load_org_svc.save_load_organization",
        _boom,
    )
    res = client.put(
        "/distribuidora/dispatch-plans/42/load-organization",
        headers=_auth("operativo"),
        json={
            "assignments": [{"oc_document_id": 1, "load_batch_id": 2}],
            "organization_version": 0,
            "mark_organized": False,
        },
    )
    assert res.status_code == 409


def test_put_load_organization_foreign_oc_400(client, monkeypatch):
    monkeypatch.setattr(
        "backend.routers.distribuidora_dispatch_plan.user_can_read_dispatch_plan",
        lambda *_a, **_k: True,
    )

    def _boom(*_a, **_k):
        raise ValueError("OC document_id=999 no pertenece a este despacho (42).")

    monkeypatch.setattr(
        "backend.routers.distribuidora_dispatch_plan.load_org_svc.save_load_organization",
        _boom,
    )
    res = client.put(
        "/distribuidora/dispatch-plans/42/load-organization",
        headers=_auth("operativo"),
        json={
            "assignments": [{"oc_document_id": 999, "load_batch_id": 1}],
            "organization_version": 0,
        },
    )
    assert res.status_code == 400
    assert "no pertenece" in res.json()["detail"]


def test_put_load_organization_foreign_batch_400(client, monkeypatch):
    monkeypatch.setattr(
        "backend.routers.distribuidora_dispatch_plan.user_can_read_dispatch_plan",
        lambda *_a, **_k: True,
    )

    def _boom(*_a, **_k):
        raise ValueError("Grupo load_batch_id=77 no pertenece a este despacho (42).")

    monkeypatch.setattr(
        "backend.routers.distribuidora_dispatch_plan.load_org_svc.save_load_organization",
        _boom,
    )
    res = client.put(
        "/distribuidora/dispatch-plans/42/load-organization",
        headers=_auth("operativo"),
        json={
            "assignments": [{"oc_document_id": 1, "load_batch_id": 77}],
            "organization_version": 0,
        },
    )
    assert res.status_code == 400
    assert "Grupo" in res.json()["detail"]


def test_post_load_batch_out_of_scope_404(client, monkeypatch):
    monkeypatch.setattr(
        "backend.routers.distribuidora_dispatch_plan.user_can_read_dispatch_plan",
        lambda *_a, **_k: False,
    )
    res = client.post(
        "/distribuidora/dispatch-plans/42/load-batches",
        headers=_auth("operativo"),
        json={"name": "G1", "description": "role=priority"},
    )
    assert res.status_code == 404


def test_post_load_batch_forbidden_capability_only(client):
    res = client.post(
        "/distribuidora/dispatch-plans/42/load-batches",
        headers=_auth("planificador_excel"),
        json={"name": "G1"},
    )
    assert res.status_code == 403


def test_delete_load_batch_out_of_scope_404(client, monkeypatch):
    monkeypatch.setattr(
        "backend.routers.distribuidora_dispatch_plan.user_can_read_dispatch_plan",
        lambda *_a, **_k: False,
    )
    res = client.delete(
        "/distribuidora/dispatch-plans/42/load-batches/7",
        headers=_auth("operativo"),
    )
    assert res.status_code == 404


def test_unauthenticated_rejected(client):
    res = client.get("/distribuidora/dispatch-plans/42/load-organization")
    assert res.status_code in (401, 403)
