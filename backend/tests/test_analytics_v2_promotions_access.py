"""Promociones V2 — API real: autenticación, permisos por acción, cuerpo estricto y errores."""

from __future__ import annotations

import asyncio
import json
from typing import Any
from urllib.parse import urlsplit

import pytest
from fastapi import FastAPI

from backend.routers import analytics_v2 as router_mod
from backend.services.analytics_v2.access import AnalyticsV2AccessError, promotion_capabilities
from backend.services.analytics_v2.promotion_campaigns import CampaignConflict, CampaignNotFound
from backend.services.analytics_v2.promotions_service import PromotionsNotReady
from backend.tests.test_analytics_v2_access import AsgiResponse, _auth


class JsonAsgiClient:
    def __init__(self, app: FastAPI) -> None:
        self.app = app

    def request(self, method: str, url: str, headers: dict[str, str] | None = None, body: Any = None) -> AsgiResponse:
        parts = urlsplit(url)
        raw = json.dumps(body if body is not None else {}).encode()
        hdrs = {"content-type": "application/json", **{k.lower(): v for k, v in (headers or {}).items()}}
        scope = {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": method.upper(),
            "scheme": "http",
            "path": parts.path,
            "raw_path": parts.path.encode(),
            "query_string": parts.query.encode(),
            "root_path": "",
            "headers": [(k.encode(), v.encode()) for k, v in hdrs.items()],
            "client": ("test", 1),
            "server": ("test", 80),
        }
        messages: list[dict[str, Any]] = []
        sent = False

        async def receive() -> dict[str, Any]:
            nonlocal sent
            if not sent:
                sent = True
                return {"type": "http.request", "body": raw, "more_body": False}
            return {"type": "http.disconnect"}

        async def send(message: dict[str, Any]) -> None:
            messages.append(message)

        asyncio.run(self.app(scope, receive, send))
        status = next(m["status"] for m in messages if m["type"] == "http.response.start")
        data = b"".join(m.get("body", b"") for m in messages if m["type"] == "http.response.body")
        return AsgiResponse(status, data)


BASE = "/analytics-v2/promotions"
READS = [
    f"{BASE}/summary?company_id=3",
    f"{BASE}/filters?company_id=3",
    f"{BASE}/campaigns?company_id=3",
    f"{BASE}/campaigns/5?company_id=3",
    f"{BASE}/items?company_id=3&section=remates",
    f"{BASE}/history?company_id=3",
    f"{BASE}/legacy?company_id=3",
]
CREATE = {"kind": "oferta", "name": "Ofertas", "starts_on": "2026-10-13", "ends_on": "2026-10-31"}
ITEM = {"version": 1, "variant_id": 10, "price_list_id": 13, "promo_gross_price": "1290"}
WRITES = [
    ("post", f"{BASE}/campaigns?company_id=3", CREATE, "create"),
    ("patch", f"{BASE}/campaigns/5?company_id=3", {"version": 1, "notes": "x"}, "edit"),
    ("post", f"{BASE}/campaigns/5/items?company_id=3", ITEM, "edit"),
    ("patch", f"{BASE}/campaigns/5/items/7?company_id=3", {"version": 1, "promo_gross_price": "1200"}, "edit"),
    ("post", f"{BASE}/campaigns/5/items/7/remove?company_id=3", {"version": 1, "reason": "x"}, "edit"),
    ("post", f"{BASE}/campaigns/5/submit?company_id=3", {"version": 1}, "edit"),
    ("post", f"{BASE}/campaigns/5/approve?company_id=3", {"version": 1}, "approve"),
    ("post", f"{BASE}/campaigns/5/reject?company_id=3", {"version": 1, "reason": "x"}, "approve"),
    ("post", f"{BASE}/campaigns/5/cancel?company_id=3", {"version": 1, "reason": "x"}, "cancel"),
    ("post", f"{BASE}/legacy/1/import?company_id=3", None, "create"),
]


@pytest.fixture()
def client(monkeypatch):
    calls: list[tuple[str, dict, int]] = []

    def fake(kind):
        def run(user, company_id, fn):
            calls.append((kind, user, company_id))
            if company_id == 1:
                raise AnalyticsV2AccessError(409, "La empresa aún no tiene BSALE RAW completo")
            if company_id == 4:
                raise PromotionsNotReady("sin migrar")
            if company_id == 5:
                raise RuntimeError("password=secreto host=db")
            if company_id == 6:
                raise CampaignConflict("La campaña fue modificada por otro usuario")
            if company_id == 7:
                raise CampaignNotFound("Campaña inexistente en la empresa")
            return {"ok": True, "kind": kind}

        return run

    monkeypatch.setattr(router_mod, "run_promotions_read", fake("read"))
    monkeypatch.setattr(router_mod, "run_promotions_write", fake("write"))
    app = FastAPI()
    app.include_router(router_mod.router)
    c = JsonAsgiClient(app)
    c.calls = calls  # type: ignore[attr-defined]
    return c


@pytest.mark.parametrize("url", READS + [f"{BASE}/products/search?company_id=3&q=ace"])
def test_reads_require_authentication(client, url):
    assert client.request("GET", url).status_code == 401
    assert client.request("GET", url, {"Authorization": "Bearer demo-token-12345"}).status_code == 401
    assert client.calls == []


@pytest.mark.parametrize("method,url,body,_perm", WRITES)
def test_writes_require_authentication(client, method, url, body, _perm):
    assert client.request(method, url, body=body).status_code == 401
    assert client.calls == []


@pytest.mark.parametrize("url", READS)
def test_view_permission(client, url):
    assert client.request("GET", url, _auth("operativo")).status_code == 403
    for role in ("gerencia", "adm", "finanzas", "comercial"):
        res = client.request("GET", url, _auth(role))
        assert res.status_code == 200 and res.json() == {"ok": True, "kind": "read"}


def test_permission_matrix_is_separated_by_action():
    assert promotion_capabilities({"role": "finanzas"}) == {"view": True, "create": False, "edit": False, "approve": False, "cancel": False}
    assert promotion_capabilities({"role": "comercial"}) == {"view": True, "create": True, "edit": True, "approve": False, "cancel": False}
    assert promotion_capabilities({"role": "gerencia"}) == dict.fromkeys(("view", "create", "edit", "approve", "cancel"), True)
    assert not any(promotion_capabilities({"role": "operativo"}).values())
    assert not any(promotion_capabilities(None).values())


@pytest.mark.parametrize("method,url,body,perm", WRITES)
def test_write_permissions_by_action(client, method, url, body, perm):
    for role in ("operativo", "finanzas", "comercial", "gerencia", "adm"):
        allowed = promotion_capabilities({"role": role})[perm]
        res = client.request(method, url, _auth(role), body=body)
        assert res.status_code == (200 if allowed else 403), (role, perm, res.text)
    assert all(kind == "write" for kind, *_ in client.calls)


def test_product_search_requires_edit_permission(client):
    url = f"{BASE}/products/search?company_id=3&q=aceite"
    assert client.request("GET", url, _auth("finanzas")).status_code == 403
    assert client.request("GET", url, _auth("comercial")).status_code == 200
    assert client.request("GET", f"{BASE}/products/search?company_id=3&q=a", _auth("gerencia")).status_code == 422


@pytest.mark.parametrize(
    "body",
    [
        {**ITEM, "normal_gross_price": "5000"},
        {**ITEM, "reference_stock": 999},
        {**ITEM, "company_id": 1},
        {k: v for k, v in ITEM.items() if k != "version"},
        {**ITEM, "variant_id": 0},
    ],
    ids=["precio_normal_navegador", "stock_navegador", "empresa_en_body", "sin_version", "variante_invalida"],
)
def test_item_body_rejects_browser_supplied_facts(client, body):
    res = client.request("POST", f"{BASE}/campaigns/5/items?company_id=3", _auth("gerencia"), body=body)
    assert res.status_code == 422
    assert client.calls == []


def test_create_body_is_strict(client):
    h = _auth("gerencia")
    assert client.request("POST", f"{BASE}/campaigns?company_id=3", h, body={**CREATE, "approval_status": "aprobada"}).status_code == 422
    assert client.request("POST", f"{BASE}/campaigns?company_id=3", h, body={**CREATE, "starts_on": "13/10/2026"}).status_code == 422
    assert client.request("POST", f"{BASE}/campaigns", h, body=CREATE).status_code == 422


def test_company_comes_from_query_and_is_verified_by_backend(client):
    h = _auth("gerencia")
    assert client.request("GET", f"{BASE}/summary?company_id=1", h).status_code == 409
    assert client.request("POST", f"{BASE}/campaigns?company_id=1", h, body=CREATE).status_code == 409
    assert [c[2] for c in client.calls] == [1, 1]


def test_error_mapping_and_no_leaks(client):
    h = _auth("gerencia")
    assert client.request("POST", f"{BASE}/campaigns?company_id=4", h, body=CREATE).status_code == 503
    assert client.request("POST", f"{BASE}/campaigns/5/approve?company_id=6", h, body={"version": 1}).status_code == 409
    assert client.request("GET", f"{BASE}/campaigns/5?company_id=7", h).status_code == 404
    res = client.request("POST", f"{BASE}/campaigns?company_id=5", h, body=CREATE)
    assert res.status_code == 500 and "secreto" not in res.text and "password" not in res.text


def test_path_ids_reach_the_service_unchanged(monkeypatch):
    seen: list[tuple[str, tuple]] = []

    class Stub:
        def __getattr__(self, name):
            def call(*args, **_kwargs):
                seen.append((name, args[:2]))
                return {"ok": True}

            return call

    monkeypatch.setattr(router_mod, "run_promotions_write", lambda user, company_id, fn: fn(Stub()))
    app = FastAPI()
    app.include_router(router_mod.router)
    c = JsonAsgiClient(app)
    h = _auth("gerencia")
    assert c.request("PATCH", f"{BASE}/campaigns/5/items/7?company_id=3", h, body={"version": 1, "notes": "x"}).status_code == 200
    assert c.request("POST", f"{BASE}/campaigns/5/items/7/remove?company_id=3", h, body={"version": 1, "reason": "x"}).status_code == 200
    assert c.request("POST", f"{BASE}/legacy/9/import?company_id=3", h).status_code == 200
    assert c.request("POST", f"{BASE}/campaigns/5/approve?company_id=3", h, body={"version": 2}).status_code == 200
    assert seen == [("update_item", (5, 7)), ("remove_item", (5, 7)), ("import_legacy", (9,)), ("transition", (5, "approve"))]


def test_no_delete_endpoints(client):
    h = _auth("gerencia")
    assert client.request("DELETE", f"{BASE}/campaigns/5?company_id=3", h).status_code == 405
    assert client.request("DELETE", f"{BASE}/campaigns/5/items/7?company_id=3", h).status_code == 405
    assert client.calls == []
