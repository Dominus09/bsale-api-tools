"""P2 PWA chofer: endpoints clientes/mapa/ficha + ownership + oficina."""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.auth.jwt import create_staff_token
from backend.auth.multiapp import APP_PWA, MultiAppScope
from backend.routers import distribuidora_pwa_driver


def _token(user_id: int) -> dict:
    tok = create_staff_token(
        email=f"u{user_id}@test.local",
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
    plan_company_office: dict[int, tuple[int | None, int | None]] = {}
    owned: set[tuple[int, int]] = set()

    def _enrich(p):
        uid = int(p.get("uid") or p.get("id") or 0)
        return {**p, "id": uid, "uid": uid, "email": p.get("email"), "role": "operativo"}

    def _load(uid: int) -> MultiAppScope:
        return scopes.get(
            int(uid),
            MultiAppScope(tables_ready=True, apps_rows_present=True, apps=frozenset()),
        )

    monkeypatch.setattr("backend.auth.multiapp.enrich_staff_payload", _enrich)
    monkeypatch.setattr("backend.auth.multiapp.load_multiapp_scope", _load)

    monkeypatch.setattr(
        "backend.services.distribuidora.dispatch_plan_driver_service.driver_has_access_to_plan",
        lambda driver_id, plan_id: (int(driver_id), int(plan_id)) in owned,
    )
    monkeypatch.setattr(
        "backend.services.distribuidora.dispatch_plan_driver_service.get_plan_summary_for_driver",
        lambda driver_id, plan_id: (
            {
                "id": int(plan_id),
                "planning_code": f"PLAN-{int(plan_id):05d}",
                "status": "picking_generated",
                "driver_user_id": int(driver_id),
                "client_count": 20,
                "order_count": 27,
            }
            if (int(driver_id), int(plan_id)) in owned
            else None
        ),
    )
    monkeypatch.setattr(
        "backend.services.distribuidora.dispatch_plan_driver_service.list_plans_for_driver_user",
        lambda driver_id, limit=50: [
            {
                "id": pid,
                "planning_code": f"PLAN-{pid:05d}",
                "status": "picking_generated",
                "driver_user_id": int(driver_id),
            }
            for d, pid in owned
            if d == int(driver_id)
        ][:limit],
    )

    def _resolve(_cur, plan_id: int):
        return plan_company_office.get(int(plan_id), (None, None))

    monkeypatch.setattr(
        "backend.services.distribuidora.dispatch_plan_driver_service.resolve_plan_company_office",
        _resolve,
    )

    # Evitar abrir conexión real en resolve (llamado con cursor; monkeypatch encima).
    class _FakeConn:
        def cursor(self):
            return self

        def close(self):
            return None

    monkeypatch.setattr(
        "backend.services.distribuidora.dispatch_plan_pwa_driver_read_service.get_connection",
        lambda: _FakeConn(),
    )

    clients_payload = {
        "dispatch_plan_id": 79,
        "items": [
            {
                "customer_key": "id:2087",
                "client_id": 2087,
                "client_name": "Galpo  de lata",
                "city": "Ancud",
                "address": "Los jazmines 514",
                "document_count": 1,
                "units_total": 12,
                "load_groups": [{"load_batch_id": 1, "name": "Grupo adicional"}],
                "has_coordinates": False,
                "route_order": 25,
            }
        ],
        "count": 1,
        "summary": {"clients": 1, "documents": 1, "with_coordinates": 0, "without_coordinates": 1},
        "picking_ready": True,
        "read_only": True,
    }
    ficha_payload = {
        "dispatch_plan_id": 79,
        "customer_key": "id:2087",
        "client": {
            "customer_key": "id:2087",
            "client_id": 2087,
            "client_name": "Galpo  de lata",
            "has_coordinates": False,
            "facade_photo_placeholder": True,
            "load_groups": [{"load_batch_id": 1, "name": "Grupo adicional"}],
        },
        "documents": [
            {
                "document_number": 2792373,
                "document_type": "Boleta",
                "oc_number": 70566,
                "load_group": {"name": "Grupo adicional"},
                "products": [{"description": "Producto X", "unidades": 2}],
                "product_count": 1,
            }
        ],
        "document_count": 1,
        "units_total": 12,
        "read_only": True,
    }
    map_payload = {
        "dispatch_plan_id": 79,
        "markers": [],
        "count": 0,
        "summary": {"clients": 1, "with_coordinates": 0, "without_coordinates": 1},
        "empty_state": {
            "code": "no_georeferenced_clients",
            "message_es": "Sin GPS",
        },
        "read_only": True,
    }

    monkeypatch.setattr(
        "backend.services.distribuidora.dispatch_plan_pwa_driver_read_service.list_clients_for_driver",
        lambda plan_id, q=None: clients_payload,
    )
    monkeypatch.setattr(
        "backend.services.distribuidora.dispatch_plan_pwa_driver_read_service.get_map_for_driver",
        lambda plan_id, origin_lat=None, origin_lng=None: map_payload,
    )

    from fastapi import HTTPException

    def _ficha(plan_id, customer_key):
        if customer_key != "id:2087":
            raise HTTPException(status_code=404, detail="Cliente no pertenece a este despacho")
        return ficha_payload

    monkeypatch.setattr(
        "backend.services.distribuidora.dispatch_plan_pwa_driver_read_service.get_client_ficha_for_driver",
        _ficha,
    )

    app = FastAPI()
    app.include_router(distribuidora_pwa_driver.router)
    client = TestClient(app)
    return client, scopes, owned, plan_company_office


def _chofer(scopes, uid=10, *, company_ids=None, offices=None):
    scopes[uid] = MultiAppScope(
        tables_ready=True,
        apps_rows_present=True,
        apps=frozenset({APP_PWA}),
        profiles=frozenset({"chofer"}),
        company_ids=company_ids,
        offices=frozenset(offices or []),
    )


def test_p2_clientes_ok(app_client):
    client, scopes, owned, plan_co = app_client
    _chofer(scopes, 10, company_ids=(3,), offices={(3, 1)})
    owned.add((10, 79))
    plan_co[79] = (3, 1)
    res = client.get("/distribuidora/pwa/driver/despachos/79/clientes", headers=_token(10))
    assert res.status_code == 200
    body = res.json()
    assert body["count"] == 1
    assert body["items"][0]["client_name"] == "Galpo  de lata"
    assert body["items"][0]["load_groups"][0]["name"] == "Grupo adicional"


def test_p2_ficha_and_foreign_denied(app_client):
    client, scopes, owned, plan_co = app_client
    _chofer(scopes, 10, company_ids=(3,), offices={(3, 1)})
    owned.add((10, 79))
    plan_co[79] = (3, 1)

    ok = client.get(
        "/distribuidora/pwa/driver/despachos/79/clientes/id:2087",
        headers=_token(10),
    )
    assert ok.status_code == 200
    assert ok.json()["documents"][0]["document_number"] == 2792373

    foreign = client.get(
        "/distribuidora/pwa/driver/despachos/79/clientes",
        headers=_token(99),
    )
    # sin perfil pwa/chofer
    assert foreign.status_code == 403

    _chofer(scopes, 99, company_ids=(3,), offices={(3, 1)})
    other = client.get(
        "/distribuidora/pwa/driver/despachos/79/clientes",
        headers=_token(99),
    )
    assert other.status_code == 404


def test_p2_office_revoked_blocks(app_client):
    client, scopes, owned, plan_co = app_client
    _chofer(scopes, 10, company_ids=(3,), offices={(3, 9)})  # otra oficina
    owned.add((10, 79))
    plan_co[79] = (3, 1)
    res = client.get("/distribuidora/pwa/driver/despachos/79/mapa", headers=_token(10))
    assert res.status_code == 403
    assert "Oficina" in res.json()["detail"]


def test_p2_detail_nav_enabled(app_client):
    client, scopes, owned, plan_co = app_client
    _chofer(scopes, 10)
    owned.add((10, 79))
    plan_co[79] = (None, None)
    res = client.get("/distribuidora/pwa/driver/despachos/79", headers=_token(10))
    assert res.status_code == 200
    nav = res.json()["nav"]
    assert nav["clientes"] is True
    assert nav["mapa"] is True


def test_p2_mapa_empty_ok(app_client):
    client, scopes, owned, plan_co = app_client
    _chofer(scopes, 10)
    owned.add((10, 79))
    plan_co[79] = (None, None)
    res = client.get("/distribuidora/pwa/driver/despachos/79/mapa", headers=_token(10))
    assert res.status_code == 200
    assert res.json()["count"] == 0
    assert res.json()["empty_state"]["code"] == "no_georeferenced_clients"
