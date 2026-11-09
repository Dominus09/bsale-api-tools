"""Validación de origin_lat/origin_lng en mapa PWA P2."""

from __future__ import annotations

import math

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from backend.auth.jwt import create_staff_token
from backend.auth.multiapp import APP_PWA, MultiAppScope
from backend.routers import distribuidora_pwa_driver
from backend.services.distribuidora import dispatch_plan_pwa_driver_read_service as read_svc


def test_validate_origin_both_omitted():
    assert read_svc.validate_origin_coords(None, None) == (None, None)


def test_validate_origin_only_lat():
    with pytest.raises(HTTPException) as ei:
        read_svc.validate_origin_coords(-41.5, None)
    assert ei.value.status_code == 400
    assert "juntos" in ei.value.detail


def test_validate_origin_only_lng():
    with pytest.raises(HTTPException) as ei:
        read_svc.validate_origin_coords(None, -73.8)
    assert ei.value.status_code == 400


def test_validate_origin_valid_and_limits():
    assert read_svc.validate_origin_coords(-41.5, -73.8) == (-41.5, -73.8)
    assert read_svc.validate_origin_coords(-90.0, -180.0) == (-90.0, -180.0)
    assert read_svc.validate_origin_coords(90.0, 180.0) == (90.0, 180.0)
    assert read_svc.validate_origin_coords(0.0, 0.0) == (0.0, 0.0)


def test_validate_origin_out_of_range():
    with pytest.raises(HTTPException) as ei:
        read_svc.validate_origin_coords(-90.1, 0.0)
    assert ei.value.status_code == 400
    assert "origin_lat" in ei.value.detail

    with pytest.raises(HTTPException) as ei2:
        read_svc.validate_origin_coords(0.0, 180.1)
    assert ei2.value.status_code == 400
    assert "origin_lng" in ei2.value.detail


def test_validate_origin_nan_inf():
    with pytest.raises(HTTPException) as ei:
        read_svc.validate_origin_coords(float("nan"), 0.0)
    assert ei.value.status_code == 400
    assert "finitos" in ei.value.detail

    with pytest.raises(HTTPException) as ei2:
        read_svc.validate_origin_coords(0.0, float("inf"))
    assert ei2.value.status_code == 400

    with pytest.raises(HTTPException) as ei3:
        read_svc.validate_origin_coords(float("-inf"), 1.0)
    assert ei3.value.status_code == 400
    assert math.isnan(float("nan"))


def _token(uid: int = 10) -> dict:
    tok = create_staff_token(
        email=f"u{uid}@test.local",
        role="operativo",
        user_id=uid,
        token_version=0,
    )
    return {"Authorization": f"Bearer {tok}"}


@pytest.fixture()
def mapa_client(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "development")
    monkeypatch.delenv("AUTH_OWNER_USER_IDS", raising=False)
    monkeypatch.delenv("AUTH_REQUIRE_USER_APPS", raising=False)

    scopes = {
        10: MultiAppScope(
            tables_ready=True,
            apps_rows_present=True,
            apps=frozenset({APP_PWA}),
            profiles=frozenset({"chofer"}),
        )
    }

    def _enrich(p):
        uid = int(p.get("uid") or p.get("id") or 0)
        return {**p, "id": uid, "uid": uid, "email": p.get("email"), "role": "operativo"}

    monkeypatch.setattr("backend.auth.multiapp.enrich_staff_payload", _enrich)
    monkeypatch.setattr(
        "backend.auth.multiapp.load_multiapp_scope",
        lambda uid: scopes.get(
            int(uid),
            MultiAppScope(tables_ready=True, apps_rows_present=True, apps=frozenset()),
        ),
    )
    monkeypatch.setattr(
        "backend.services.distribuidora.dispatch_plan_driver_service.driver_has_access_to_plan",
        lambda driver_id, plan_id: int(driver_id) == 10 and int(plan_id) == 79,
    )
    monkeypatch.setattr(
        "backend.services.distribuidora.dispatch_plan_driver_service.resolve_plan_company_office",
        lambda _cur, plan_id: (None, None),
    )

    class _FakeConn:
        def cursor(self):
            return self

        def close(self):
            return None

    monkeypatch.setattr(
        "backend.services.distribuidora.dispatch_plan_pwa_driver_read_service.get_connection",
        lambda: _FakeConn(),
    )
    monkeypatch.setattr(
        "backend.services.distribuidora.dispatch_plan_pwa_driver_read_service.get_map_for_driver",
        lambda plan_id, origin_lat=None, origin_lng=None: {
            "dispatch_plan_id": plan_id,
            "markers": [],
            "count": 0,
            "origin": {"lat": origin_lat, "lng": origin_lng},
            "summary": {"clients": 0, "with_coordinates": 0, "without_coordinates": 0},
            "empty_state": {"code": "no_georeferenced_clients", "message_es": "Sin GPS"},
            "read_only": True,
        },
    )

    app = FastAPI()
    app.include_router(distribuidora_pwa_driver.router)
    return TestClient(app)


def test_http_origin_omitted_ok(mapa_client):
    res = mapa_client.get("/distribuidora/pwa/driver/despachos/79/mapa", headers=_token())
    assert res.status_code == 200
    assert res.json()["origin"]["lat"] is None


def test_http_origin_valid(mapa_client):
    res = mapa_client.get(
        "/distribuidora/pwa/driver/despachos/79/mapa",
        headers=_token(),
        params={"origin_lat": -41.5, "origin_lng": -73.8},
    )
    assert res.status_code == 200
    assert res.json()["origin"]["lat"] == -41.5


def test_http_origin_only_one(mapa_client):
    res = mapa_client.get(
        "/distribuidora/pwa/driver/despachos/79/mapa",
        headers=_token(),
        params={"origin_lat": -41.5},
    )
    assert res.status_code == 400


def test_http_origin_out_of_range(mapa_client):
    res = mapa_client.get(
        "/distribuidora/pwa/driver/despachos/79/mapa",
        headers=_token(),
        params={"origin_lat": 91, "origin_lng": 0},
    )
    assert res.status_code == 400
    assert "origin_lat" in res.json()["detail"]


def test_http_origin_nan(mapa_client):
    res = mapa_client.get(
        "/distribuidora/pwa/driver/despachos/79/mapa",
        headers=_token(),
        params={"origin_lat": "nan", "origin_lng": "0"},
    )
    assert res.status_code == 400
