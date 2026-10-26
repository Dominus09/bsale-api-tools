"""Márgenes V2 — autenticación, permiso ``margins`` y validación de parámetros en la API real."""

from __future__ import annotations

import pytest
from fastapi import FastAPI

from backend.routers import analytics_v2 as router_mod
from backend.services.analytics_v2.access import AnalyticsV2AccessError
from backend.services.analytics_v2.margins_service import MarginsValidationError
from backend.tests.test_analytics_v2_access import AsgiClient, _auth

MARGIN_ENDPOINTS = [
    "/analytics-v2/margins/summary?company_id=3",
    "/analytics-v2/margins/filters?company_id=3",
    "/analytics-v2/margins/scope?company_id=3",
    "/analytics-v2/margins/rows?company_id=3",
    "/analytics-v2/margins/products/10?company_id=3",
    "/analytics-v2/margins/policy-review?company_id=3",
]


@pytest.fixture()
def client(monkeypatch):
    calls: list[tuple[dict, int]] = []

    def fake_run(user, company_id, fn, **_kwargs):
        calls.append((user, company_id))
        if company_id == 1:
            raise AnalyticsV2AccessError(409, "La empresa aún no tiene BSALE RAW completo")
        if company_id == 4:
            raise MarginsValidationError("cost_basis inválido: x")
        if company_id == 5:
            raise RuntimeError("password=secreto host=db")
        return {"ok": True, "company_id": company_id}

    def forbidden_cost_run(*_args, **_kwargs):
        raise AssertionError("Márgenes no debe usar el runner de costos")

    monkeypatch.setattr(router_mod, "run_margins", fake_run)
    monkeypatch.setattr(router_mod, "run_cost_control", forbidden_cost_run)
    app = FastAPI()
    app.include_router(router_mod.router)
    c = AsgiClient(app)
    c.calls = calls  # type: ignore[attr-defined]
    return c


@pytest.mark.parametrize("url", MARGIN_ENDPOINTS)
def test_margins_require_authentication(client, url):
    assert client.get(url).status_code == 401
    assert client.get(url, headers={"Authorization": "Bearer demo-token-12345"}).status_code == 401
    assert client.calls == []


@pytest.mark.parametrize("url", MARGIN_ENDPOINTS)
def test_margins_deny_operational_role(client, url):
    assert client.get(url, headers=_auth("operativo")).status_code == 403
    assert client.calls == []


@pytest.mark.parametrize("role", ["gerencia", "adm", "finanzas"])
def test_margins_allow_margin_permission_roles(client, role):
    res = client.get("/analytics-v2/margins/rows?company_id=3", headers=_auth(role))
    assert res.status_code == 200 and res.json() == {"ok": True, "company_id": 3}


def test_finanzas_has_margins_but_not_costs(client, monkeypatch):
    monkeypatch.setattr(router_mod, "run_cost_control", lambda *a, **k: {"ok": True})
    headers = _auth("finanzas")
    assert client.get("/analytics-v2/margins/summary?company_id=3", headers=headers).status_code == 200
    assert client.get("/analytics-v2/costs/summary?company_id=3", headers=headers).status_code == 403


def test_margins_company_and_param_validation(client):
    h = _auth("gerencia")
    assert client.get("/analytics-v2/margins/rows", headers=h).status_code == 422
    assert client.get("/analytics-v2/margins/rows?company_id=0", headers=h).status_code == 422
    assert client.get("/analytics-v2/margins/rows?company_id=3&page_size=201", headers=h).status_code == 422
    assert client.get("/analytics-v2/margins/rows?company_id=3&price_list_id=0", headers=h).status_code == 422
    assert client.get("/analytics-v2/margins/rows?company_id=1", headers=h).status_code == 409
    assert client.get("/analytics-v2/margins/rows?company_id=4", headers=h).status_code == 422


def test_margins_internal_errors_do_not_leak(client):
    res = client.get("/analytics-v2/margins/rows?company_id=5", headers=_auth("gerencia"))
    assert res.status_code == 500
    assert "secreto" not in res.text and "password" not in res.text


def test_margin_endpoints_are_read_only(client):
    h = _auth("gerencia")
    for method in ("post", "patch", "put"):
        res = getattr(client, method)("/analytics-v2/margins/rows?company_id=3", headers=h)
        assert res.status_code == 405
    assert client.calls == []
