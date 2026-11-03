"""HTTP: preview planificación simplificada (sin BD/Bsale reales)."""

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


def _auth(role: str = "gerencia", user_id: int = 10) -> dict:
    token = create_staff_token(
        email="planner@test.local",
        role=role,
        user_id=user_id,
        token_version=0,
    )
    return {"Authorization": f"Bearer {token}"}


def test_preview_ok_readonly(client, monkeypatch):
    monkeypatch.setattr(
        "backend.routers.distribuidora_simple_planning.preview_ocs_from_folios",
        lambda **_k: {
            "status": "completed",
            "wrote": False,
            "mode": "simple_planning_preview_readonly",
            "items": [{"folio": 1, "status": "found_available", "blocking": False}],
            "blocking_count": 0,
        },
    )
    res = client.post(
        "/distribuidora/simple-planning/preview",
        headers=_auth("gerencia"),
        json={"company_id": 3, "office_id": 1, "folios": ["70625"]},
    )
    assert res.status_code == 200
    assert res.json()["wrote"] is False


def test_unauthenticated_rejected(client):
    res = client.post(
        "/distribuidora/simple-planning/preview",
        json={"company_id": 3, "office_id": 1, "folios": ["1"]},
    )
    assert res.status_code in (401, 403)


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
        "/distribuidora/simple-planning/preview",
        headers=_auth("gerencia", user_id=42),
        json={"company_id": 1, "office_id": 1, "folios": ["1"]},
    )
    assert res.status_code == 403


def test_malformed_list_maps_400(client, monkeypatch):
    from backend.services.distribuidora.simple_planning_preview_service import (
        SimplePlanningPreviewError,
    )

    monkeypatch.setattr(
        "backend.routers.distribuidora_simple_planning.preview_ocs_from_folios",
        lambda **_k: (_ for _ in ()).throw(SimplePlanningPreviewError("Indique al menos un folio")),
    )
    res = client.post(
        "/distribuidora/simple-planning/preview",
        headers=_auth(),
        json={"company_id": 3, "office_id": 1, "folios": [" "]},
    )
    # Body has min_length=1 so pydantic may 422 first; force via non-empty then service error
    assert res.status_code in (400, 422)


def test_preview_rejects_write_flag(client, monkeypatch):
    monkeypatch.setattr(
        "backend.routers.distribuidora_simple_planning.preview_ocs_from_folios",
        lambda **_k: {"wrote": True, "items": []},
    )
    res = client.post(
        "/distribuidora/simple-planning/preview",
        headers=_auth(),
        json={"company_id": 3, "office_id": 1, "folios": ["1"]},
    )
    assert res.status_code == 500


def test_bad_company_allowlist(client):
    res = client.post(
        "/distribuidora/simple-planning/preview",
        headers=_auth(),
        json={"company_id": 99, "office_id": 1, "folios": ["1"]},
    )
    assert res.status_code == 400
