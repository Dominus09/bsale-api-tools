"""HTTP: permisos y validaciones del auditor OC (sin BD/Bsale reales)."""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.auth.jwt import create_staff_token
from backend.auth.multiapp import MultiAppScope
from backend.routers import distribuidora_oc_audit


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
    app = FastAPI()
    app.include_router(distribuidora_oc_audit.router)
    return TestClient(app)


def _auth(role: str, email: str = "actor@test.local", user_id: int = 10) -> dict:
    token = create_staff_token(email=email, role=role, user_id=user_id, token_version=0)
    return {"Authorization": f"Bearer {token}"}


def test_operativo_forbidden_on_meta(client):
    res = client.get(
        "/distribuidora/oc-audit/meta",
        params={"company_id": 3, "office_id": 1},
        headers=_auth("operativo"),
    )
    assert res.status_code == 403


def test_gerencia_meta_ok(client, monkeypatch):
    monkeypatch.setattr(
        "backend.routers.distribuidora_oc_audit.get_local_oc_folio_bounds",
        lambda **_k: {
            "min_folio": 100,
            "max_folio": 200,
            "positive_count": 50,
            "company_id": 3,
            "office_id": 1,
            "document_type_id": 33,
        },
    )
    monkeypatch.setattr(
        "backend.routers.distribuidora_oc_audit.suggest_recent_folio_range",
        lambda **_k: {"from_folio": 101, "to_folio": 200, "window": 100},
    )
    res = client.get(
        "/distribuidora/oc-audit/meta",
        params={"company_id": 3, "office_id": 1},
        headers=_auth("gerencia"),
    )
    assert res.status_code == 200
    body = res.json()
    assert body["writes_enabled"] is True
    assert body["recovery_enabled"] is True
    assert body["recovery_requires_simulation"] is True
    assert body["recovery_bulk_enabled"] is False
    assert body["compare_requires_manual_action"] is True
    assert body["document_type_id"] == 33


def test_local_audit_rejects_bad_company(client):
    res = client.post(
        "/distribuidora/oc-audit/local",
        headers=_auth("adm"),
        json={"company_id": 99, "office_id": 1, "from_folio": 1, "to_folio": 10},
    )
    assert res.status_code == 400


def test_local_audit_ok_no_bsale(client, monkeypatch):
    monkeypatch.setattr(
        "backend.routers.distribuidora_oc_audit.audit_oc_correlatives",
        lambda *a, **k: {
            "status": "completed",
            "wrote": False,
            "investigate": False,
            "findings": [{"folio": 1, "status": "ok", "wrote": False}],
            "counts": {"ok": 1},
            "company_id": 3,
            "office_id": 1,
        },
    )
    res = client.post(
        "/distribuidora/oc-audit/local",
        headers=_auth("gerencia"),
        json={"company_id": 3, "office_id": 1, "from_folio": 1, "to_folio": 10},
    )
    assert res.status_code == 200
    assert res.json()["wrote"] is False
    assert res.json()["investigate"] is False


def test_investigate_requires_token(client, monkeypatch):
    monkeypatch.setattr(
        "backend.routers.distribuidora_oc_audit._bsale_token",
        lambda: "",
    )
    res = client.post(
        "/distribuidora/oc-audit/investigate",
        headers=_auth("gerencia"),
        json={"company_id": 3, "office_id": 1, "folios": [70511]},
    )
    assert res.status_code == 503


def test_investigate_maps_bsale_failure_to_502(client, monkeypatch):
    monkeypatch.setattr(
        "backend.routers.distribuidora_oc_audit._bsale_token",
        lambda: "token",
    )
    monkeypatch.setattr(
        "backend.routers.distribuidora_oc_audit.BsaleClient",
        lambda *_a, **_k: object(),
    )

    def _boom(*_a, **_k):
        raise RuntimeError("Bsale HTTP 502")

    monkeypatch.setattr(
        "backend.routers.distribuidora_oc_audit.investigate_selected_folios",
        _boom,
    )
    res = client.post(
        "/distribuidora/oc-audit/investigate",
        headers=_auth("adm"),
        json={"company_id": 3, "office_id": 1, "folios": [70001]},
    )
    assert res.status_code == 502
    assert "Bsale" in res.json()["detail"]


def test_unauthenticated_rejected(client):
    res = client.get("/distribuidora/oc-audit/meta")
    assert res.status_code in (401, 403)


def test_operativo_forbidden_on_compare_and_recover(client, monkeypatch):
    monkeypatch.setattr(
        "backend.routers.distribuidora_oc_audit._bsale_token",
        lambda: "token",
    )
    headers = _auth("operativo")
    assert (
        client.post(
            "/distribuidora/oc-audit/compare",
            headers=headers,
            json={"company_id": 3, "office_id": 1},
        ).status_code
        == 403
    )
    assert (
        client.post(
            "/distribuidora/oc-audit/recover/simulate",
            headers=headers,
            json={"company_id": 3, "office_id": 1, "folio": 1},
        ).status_code
        == 403
    )
    assert (
        client.post(
            "/distribuidora/oc-audit/recover/confirm",
            headers=headers,
            json={
                "company_id": 3,
                "office_id": 1,
                "folio": 1,
                "simulation_token": "x" * 20,
            },
        ).status_code
        == 403
    )


def test_compare_ok_readonly(client, monkeypatch):
    monkeypatch.setattr(
        "backend.routers.distribuidora_oc_audit._bsale_token",
        lambda: "token",
    )
    monkeypatch.setattr(
        "backend.routers.distribuidora_oc_audit.BsaleClient",
        lambda *_a, **_k: object(),
    )
    monkeypatch.setattr(
        "backend.routers.distribuidora_oc_audit.compare_bsale_pg_folio_tip",
        lambda *_a, **_k: {
            "status": "completed",
            "wrote": False,
            "verification_status": "equal",
            "pg_max_folio": 10,
            "bsale_max_folio": 10,
            "pending_investigate_count": 0,
            "pending_investigate_folios": [],
        },
    )
    res = client.post(
        "/distribuidora/oc-audit/compare",
        headers=_auth("gerencia"),
        json={"company_id": 3, "office_id": 1},
    )
    assert res.status_code == 200
    assert res.json()["wrote"] is False


def test_recover_simulate_forbidden_without_auth(client):
    res = client.post(
        "/distribuidora/oc-audit/recover/simulate",
        json={"company_id": 3, "office_id": 1, "folio": 1},
    )
    assert res.status_code in (401, 403)


def test_recover_confirm_maps_conflict(client, monkeypatch):
    from backend.services.distribuidora.oc_audit_recovery_service import (
        RecoveryConflictError,
    )

    monkeypatch.setattr(
        "backend.routers.distribuidora_oc_audit._bsale_token",
        lambda: "token",
    )
    monkeypatch.setattr(
        "backend.routers.distribuidora_oc_audit.BsaleClient",
        lambda *_a, **_k: object(),
    )

    def _boom(*_a, **_k):
        raise RecoveryConflictError("documento cambió")

    monkeypatch.setattr(
        "backend.routers.distribuidora_oc_audit.confirm_oc_recovery",
        _boom,
    )
    res = client.post(
        "/distribuidora/oc-audit/recover/confirm",
        headers=_auth("adm"),
        json={
            "company_id": 3,
            "office_id": 1,
            "folio": 99,
            "simulation_token": "a" * 40,
        },
    )
    assert res.status_code == 409


def test_recover_simulate_maps_429(client, monkeypatch):
    from backend.services.distribuidora.oc_audit_recovery_service import (
        RecoveryTransientError,
    )

    monkeypatch.setattr(
        "backend.routers.distribuidora_oc_audit._bsale_token",
        lambda: "token",
    )
    monkeypatch.setattr(
        "backend.routers.distribuidora_oc_audit.BsaleClient",
        lambda *_a, **_k: object(),
    )
    monkeypatch.setattr(
        "backend.routers.distribuidora_oc_audit.simulate_oc_recovery",
        lambda *_a, **_k: (_ for _ in ()).throw(
            RecoveryTransientError("Bsale HTTP 429: rate limit")
        ),
    )
    res = client.post(
        "/distribuidora/oc-audit/recover/simulate",
        headers=_auth("gerencia"),
        json={"company_id": 3, "office_id": 1, "folio": 12},
    )
    assert res.status_code == 429
