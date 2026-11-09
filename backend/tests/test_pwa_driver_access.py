"""P1 PWA chofer: ownership, /auth/me PWA, POST delivery-map con scope."""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.auth.jwt import create_staff_token
from backend.auth.multiapp import APP_ERP, APP_PWA, MultiAppScope
from backend.routers import auth as auth_router_mod
from backend.routers import distribuidora_dispatch_plan
from backend.routers import distribuidora_pwa_driver


def _token(user_id: int, *, email: str | None = None) -> dict:
    tok = create_staff_token(
        email=email or f"u{user_id}@test.local",
        role="operativo",
        user_id=user_id,
        token_version=0,
    )
    return {"Authorization": f"Bearer {tok}"}


@pytest.fixture()
def app_client(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "development")
    monkeypatch.delenv("AUTH_OWNER_USER_IDS", raising=False)
    monkeypatch.delenv("AUTH_REQUIRE_USER_APPS", raising=False)

    scopes: dict[int, MultiAppScope] = {}

    def _enrich(p):
        uid = int(p.get("uid") or p.get("id") or 0)
        return {
            **p,
            "id": uid,
            "uid": uid,
            "email": p.get("email"),
            "role": p.get("role") or "operativo",
        }

    def _load(uid: int) -> MultiAppScope:
        return scopes.get(
            int(uid),
            MultiAppScope(tables_ready=True, apps_rows_present=True, apps=frozenset()),
        )

    monkeypatch.setattr("backend.auth.multiapp.enrich_staff_payload", _enrich)
    monkeypatch.setattr("backend.auth.multiapp.load_multiapp_scope", _load)

    plans_by_driver: dict[int, list[dict]] = {}
    plan_owner: dict[int, int] = {}

    def _list(driver_id: int, *, limit: int = 50):
        return list(plans_by_driver.get(int(driver_id), []))[:limit]

    def _summary(driver_id: int, plan_id: int):
        if plan_owner.get(int(plan_id)) != int(driver_id):
            return None
        for row in plans_by_driver.get(int(driver_id), []):
            if int(row["id"]) == int(plan_id):
                return row
        return None

    monkeypatch.setattr(
        "backend.services.distribuidora.dispatch_plan_driver_service.list_plans_for_driver_user",
        _list,
    )
    monkeypatch.setattr(
        "backend.services.distribuidora.dispatch_plan_driver_service.get_plan_summary_for_driver",
        _summary,
    )

    # capability-only deny for POST scope tests
    readable: set[int] = set()

    def _can_read(user, plan_id: int) -> bool:
        from backend.auth.permissions import is_simple_planning_capability_only

        if not is_simple_planning_capability_only(user):
            return True
        return int(plan_id) in readable

    monkeypatch.setattr(
        "backend.routers.distribuidora_dispatch_plan.user_can_read_dispatch_plan",
        _can_read,
    )
    monkeypatch.setattr(
        "backend.services.distribuidora.delivery_map_service.set_delivery_stop_status",
        lambda *a, **k: {"ok": True},
    )

    app = FastAPI()
    app.include_router(auth_router_mod.auth_router)
    app.include_router(distribuidora_pwa_driver.router)
    app.include_router(distribuidora_dispatch_plan.router)
    client = TestClient(app)
    return client, scopes, plans_by_driver, plan_owner, readable


def test_pwa_chofer_lists_only_assigned(app_client):
    client, scopes, plans, owners, _readable = app_client
    scopes[10] = MultiAppScope(
        tables_ready=True,
        apps_rows_present=True,
        apps=frozenset({APP_PWA}),
        profiles=frozenset({"chofer"}),
    )
    plans[10] = [
        {
            "id": 79,
            "planning_code": "PLAN-00079",
            "planning_date": "2026-11-05",
            "route_name": "Melinka",
            "truck_name": "Hino 4",
            "truck_plate": "ABCD12",
            "status": "picking_generated",
            "driver_user_id": 10,
            "client_count": 25,
            "order_count": 25,
        }
    ]
    owners[79] = 10
    res = client.get("/distribuidora/pwa/driver/despachos", headers=_token(10))
    assert res.status_code == 200
    body = res.json()
    assert body["count"] == 1
    assert body["items"][0]["planning_code"] == "PLAN-00079"


def test_pwa_chofer_empty_list(app_client):
    client, scopes, _plans, _owners, _readable = app_client
    scopes[11] = MultiAppScope(
        tables_ready=True,
        apps_rows_present=True,
        apps=frozenset({APP_PWA}),
        profiles=frozenset({"chofer"}),
    )
    res = client.get("/distribuidora/pwa/driver/despachos", headers=_token(11))
    assert res.status_code == 200
    assert res.json()["count"] == 0
    assert res.json()["items"] == []


def test_no_pwa_app_denied(app_client):
    client, scopes, _plans, _owners, _readable = app_client
    scopes[12] = MultiAppScope(
        tables_ready=True,
        apps_rows_present=True,
        apps=frozenset({APP_ERP}),
        profiles=frozenset(),
    )
    res = client.get("/distribuidora/pwa/driver/despachos", headers=_token(12))
    assert res.status_code == 403


def test_pwa_without_chofer_profile_denied(app_client):
    client, scopes, _plans, _owners, _readable = app_client
    scopes[13] = MultiAppScope(
        tables_ready=True,
        apps_rows_present=True,
        apps=frozenset({APP_PWA}),
        profiles=frozenset({"bodega"}),
    )
    res = client.get("/distribuidora/pwa/driver/despachos", headers=_token(13))
    assert res.status_code == 403


def test_foreign_plan_is_404(app_client):
    client, scopes, plans, owners, _readable = app_client
    scopes[10] = MultiAppScope(
        tables_ready=True,
        apps_rows_present=True,
        apps=frozenset({APP_PWA}),
        profiles=frozenset({"chofer"}),
    )
    plans[10] = [{"id": 79, "planning_code": "PLAN-00079", "driver_user_id": 10}]
    owners[79] = 10
    owners[80] = 99
    res = client.get("/distribuidora/pwa/driver/despachos/80", headers=_token(10))
    assert res.status_code == 404


def test_own_plan_detail_ok(app_client):
    client, scopes, plans, owners, _readable = app_client
    scopes[10] = MultiAppScope(
        tables_ready=True,
        apps_rows_present=True,
        apps=frozenset({APP_PWA}),
        profiles=frozenset({"chofer"}),
    )
    plans[10] = [
        {
            "id": 79,
            "planning_code": "PLAN-00079",
            "status": "picking_generated",
            "driver_user_id": 10,
            "driver_name": "Chofer Test",
        }
    ]
    owners[79] = 10
    res = client.get("/distribuidora/pwa/driver/despachos/79", headers=_token(10))
    assert res.status_code == 200
    assert res.json()["plan"]["planning_code"] == "PLAN-00079"
    assert res.json()["read_only"] is True


def test_auth_me_allows_pwa_only(app_client):
    client, scopes, _plans, _owners, _readable = app_client
    scopes[20] = MultiAppScope(
        tables_ready=True,
        apps_rows_present=True,
        apps=frozenset({APP_PWA}),
        profiles=frozenset({"chofer"}),
    )
    res = client.get("/auth/me", headers=_token(20))
    assert res.status_code == 200
    body = res.json()
    assert body["has_pwa_access"] is True
    assert body["has_erp_access"] is False
    assert "chofer" in body["profiles"]


def test_auth_me_still_allows_erp(app_client):
    client, scopes, _plans, _owners, _readable = app_client
    scopes[1] = MultiAppScope(
        tables_ready=True,
        apps_rows_present=False,
        apps=frozenset(),
    )
    res = client.get("/auth/me", headers=_token(1, email="adm@test.local"))
    assert res.status_code == 200
    assert res.json()["has_erp_access"] is True


def test_delivery_map_post_denied_for_capability_without_link(app_client, monkeypatch):
    client, scopes, _plans, _owners, readable = app_client
    scopes[7] = MultiAppScope(tables_ready=True, apps_rows_present=False)
    monkeypatch.setattr(
        "backend.auth.permissions.is_simple_planning_capability_only",
        lambda _u: True,
    )
    readable.clear()  # no vínculo
    res = client.post(
        "/distribuidora/dispatch-plans/79/delivery-map/stops/c1/status",
        headers=_token(7),
        json={"status": "delivered"},
    )
    assert res.status_code == 404


def test_delivery_map_post_ok_when_in_scope(app_client, monkeypatch):
    client, scopes, _plans, _owners, readable = app_client
    scopes[7] = MultiAppScope(tables_ready=True, apps_rows_present=False)
    monkeypatch.setattr(
        "backend.auth.permissions.is_simple_planning_capability_only",
        lambda _u: True,
    )
    readable.add(79)
    res = client.post(
        "/distribuidora/dispatch-plans/79/delivery-map/stops/c1/status",
        headers=_token(7),
        json={"status": "delivered"},
    )
    assert res.status_code == 200
    assert res.json()["ok"] is True


def test_invalid_jwt_rejected(app_client):
    client, _scopes, _plans, _owners, _readable = app_client
    res = client.get(
        "/distribuidora/pwa/driver/despachos",
        headers={"Authorization": "Bearer not-a-jwt"},
    )
    assert res.status_code in (401, 403)
