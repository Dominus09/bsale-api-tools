"""HTTP: revisiones de costos — auth, body estricto, sin costos ni assignee del cliente."""

from __future__ import annotations

import asyncio
import json
from typing import Any
from urllib.parse import urlsplit

import pytest
from fastapi import FastAPI, HTTPException

from backend.routers import analytics_v2 as router_mod
from backend.services.analytics_v2.access import AnalyticsV2AccessError
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


@pytest.fixture()
def client(monkeypatch):
    calls: list[tuple[str, Any]] = []

    class Stub:
        def list(self, **kwargs):
            calls.append(("list", kwargs))
            return {
                "items": [],
                "total": 0,
                "page": 1,
                "page_size": 50,
                "sort": kwargs.get("sort"),
                "company": {"id": 3, "name": "La Quillotana"},
                "meta": {},
            }

        def get(self, review_id):
            calls.append(("get", review_id))
            return {"id": review_id, "events": [], "is_human_review": True, "company_name": "La Quillotana"}

        def create_or_get(self, **kwargs):
            calls.append(("create", kwargs))
            assert "assignee_user_id" not in kwargs
            assert "current_cost_ref" not in kwargs
            assert "product_name" not in kwargs
            return {
                "item": {
                    "id": 1,
                    "status": "pendiente",
                    "assignee_user_id": None,
                    "company_name": "La Quillotana",
                },
                "created": True,
            }

        def start(self, review_id):
            calls.append(("start", review_id))
            return {"id": review_id, "status": "en_revision", "assignee_user_id": 7}

        def update_observations(self, review_id, observations):
            calls.append(("obs", review_id, observations))
            return {"id": review_id, "observations": observations}

        def update_priority(self, review_id, priority):
            calls.append(("priority", review_id, priority))
            return {"id": review_id, "priority": priority}

        def resolve(self, review_id, *, conclusion):
            calls.append(("resolve", review_id, conclusion))
            return {"id": review_id, "status": "resuelto", "conclusion": conclusion}

        def discard(self, review_id, *, conclusion):
            calls.append(("discard", review_id, conclusion))
            return {"id": review_id, "status": "descartado", "conclusion": conclusion}

    def fake_run(user, company_id, fn):
        calls.append(("run", user.get("role"), company_id))
        if company_id == 1:
            raise AnalyticsV2AccessError(409, "La empresa aún no tiene BSALE RAW completo")
        return fn(Stub())

    monkeypatch.setattr(router_mod, "run_cost_reviews", fake_run)
    app = FastAPI()
    app.include_router(router_mod.router)
    return JsonAsgiClient(app), calls


def test_requires_auth(client):
    c, calls = client
    assert c.request("GET", "/analytics-v2/costs/reviews?company_id=3").status_code == 401
    assert calls == []


def test_list_with_sort(client):
    c, calls = client
    res = c.request("GET", "/analytics-v2/costs/reviews?company_id=3&sort=priority_desc", _auth("gerencia"))
    assert res.status_code == 200
    body = json.loads(res.body)
    assert body["company"]["name"] == "La Quillotana"
    assert any(x[0] == "list" and x[1].get("sort") == "priority_desc" for x in calls)


def test_create_rejects_client_cost_fields(client):
    c, _ = client
    res = c.request(
        "POST",
        "/analytics-v2/costs/reviews?company_id=3",
        _auth("gerencia"),
        body={
            "variant_id": 10,
            "anomaly_code": "alza_anormal",
            "current_cost_ref": 1,
            "product_name": "hack",
        },
    )
    assert res.status_code == 422


def test_create_rejects_assignee(client):
    c, _ = client
    res = c.request(
        "POST",
        "/analytics-v2/costs/reviews?company_id=3",
        _auth("gerencia"),
        body={"variant_id": 10, "anomaly_code": "alza_anormal", "assignee_user_id": 999},
    )
    assert res.status_code == 422


def test_create_and_start(client):
    c, calls = client
    res = c.request(
        "POST",
        "/analytics-v2/costs/reviews?company_id=3",
        _auth("adm"),
        body={"variant_id": 10, "anomaly_code": "alza_anormal", "source": "dashboard_alert"},
    )
    assert res.status_code == 200
    assert json.loads(res.body)["item"]["assignee_user_id"] is None
    res2 = c.request("POST", "/analytics-v2/costs/reviews/1/start?company_id=3", _auth("adm"), body={})
    assert res2.status_code == 200
    assert any(x[0] == "start" for x in calls)


def test_priority_patch(client):
    c, calls = client
    res = c.request(
        "PATCH",
        "/analytics-v2/costs/reviews/1/priority?company_id=3",
        _auth("gerencia"),
        body={"priority": "alta"},
    )
    assert res.status_code == 200
    assert any(x[0] == "priority" for x in calls)


def test_resolve_requires_conclusion(client):
    c, _ = client
    res = c.request(
        "POST",
        "/analytics-v2/costs/reviews/1/resolve?company_id=3",
        _auth("gerencia"),
        body={"conclusion": "abc"},
    )
    assert res.status_code == 422


def test_resolve_ok(client):
    c, calls = client
    res = c.request(
        "POST",
        "/analytics-v2/costs/reviews/1/resolve?company_id=3",
        _auth("gerencia"),
        body={"conclusion": "Validado con proveedor"},
    )
    assert res.status_code == 200
    assert any(x[0] == "resolve" for x in calls)


def test_raw_not_ready(client):
    c, _ = client
    res = c.request("GET", "/analytics-v2/costs/reviews?company_id=1", _auth("gerencia"))
    assert res.status_code == 409


def test_no_delete_routes_registered():
    delete_reviews = []
    for route in router_mod.router.routes:
        methods = getattr(route, "methods", None) or set()
        path = getattr(route, "path", "") or ""
        if "DELETE" in methods and "/costs/reviews" in path:
            delete_reviews.append(path)
    assert delete_reviews == []


def test_guard_propagates_http_from_service(monkeypatch):
    def boom(user, company_id, fn):
        class S:
            def start(self, review_id):
                raise HTTPException(status_code=409, detail="Otro usuario inició")

        return fn(S())

    monkeypatch.setattr(router_mod, "run_cost_reviews", boom)
    app = FastAPI()
    app.include_router(router_mod.router)
    c = JsonAsgiClient(app)
    res = c.request("POST", "/analytics-v2/costs/reviews/1/start?company_id=3", _auth("gerencia"), body={})
    assert res.status_code == 409
