"""AnalÃ­tica V2 â€” autenticaciÃ³n, permisos y empresa verificada en backend; promociones protegidas."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import pytest
from fastapi import FastAPI

from backend.auth.jwt import create_staff_token
from backend.routers import analytics_v2 as router_mod
from backend.routers import promotions as promotions_mod
from backend.services.analytics_v2.access import (
    COMPANY_SQL,
    AnalyticsV2AccessError,
    resolve_company_scope,
)

REPO_ROOT = Path(__file__).resolve().parents[2]


class AsgiResponse:
    def __init__(self, status_code: int, body: bytes) -> None:
        self.status_code = status_code
        self.text = body.decode("utf-8", errors="replace")

    def json(self) -> Any:
        return json.loads(self.text)


class AsgiClient:
    """Cliente ASGI mÃ­nimo (sin httpx): ejecuta la app real con sus dependencias."""

    def __init__(self, app: FastAPI) -> None:
        self.app = app

    def request(self, method: str, url: str, headers: dict[str, str] | None = None) -> AsgiResponse:
        parts = urlsplit(url)
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
            "headers": [(k.lower().encode(), v.encode()) for k, v in (headers or {}).items()],
            "client": ("test", 1),
            "server": ("test", 80),
        }
        messages: list[dict[str, Any]] = []
        sent = False

        async def receive() -> dict[str, Any]:
            nonlocal sent
            if not sent:
                sent = True
                return {"type": "http.request", "body": b"{}", "more_body": False}
            return {"type": "http.disconnect"}

        async def send(message: dict[str, Any]) -> None:
            messages.append(message)

        asyncio.run(self.app(scope, receive, send))
        status = next(m["status"] for m in messages if m["type"] == "http.response.start")
        body = b"".join(m.get("body", b"") for m in messages if m["type"] == "http.response.body")
        return AsgiResponse(status, body)

    def get(self, url: str, headers: dict[str, str] | None = None) -> AsgiResponse:
        return self.request("GET", url, headers)

    def post(self, url: str, headers: dict[str, str] | None = None) -> AsgiResponse:
        return self.request("POST", url, headers)

    def patch(self, url: str, headers: dict[str, str] | None = None) -> AsgiResponse:
        return self.request("PATCH", url, headers)

    def put(self, url: str, headers: dict[str, str] | None = None) -> AsgiResponse:
        return self.request("PUT", url, headers)


def _auth(role: str, **extra: Any) -> dict[str, str]:
    token = create_staff_token(email=f"{role}@test.local", role=role, extra=extra or None)
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture()
def client(monkeypatch):
    calls: list[tuple[dict, int]] = []

    def fake_run(user, company_id, fn):
        calls.append((user, company_id))
        if company_id == 1:
            raise AnalyticsV2AccessError(409, "La empresa aÃºn no tiene BSALE RAW completo")
        return {"ok": True, "company_id": company_id}

    monkeypatch.setattr(router_mod, "run_cost_control", fake_run)
    app = FastAPI()
    app.include_router(router_mod.router)
    c = AsgiClient(app)
    c.calls = calls  # type: ignore[attr-defined]
    return c


ENDPOINTS = [
    "/analytics-v2/freshness?company_id=3",
    "/analytics-v2/costs/summary?company_id=3",
    "/analytics-v2/costs/filters?company_id=3",
    "/analytics-v2/costs/products?company_id=3",
    "/analytics-v2/costs/products/10?company_id=3",
    "/analytics-v2/costs/products/10/history?company_id=3",
    "/analytics-v2/costs/alerts?company_id=3",
    "/analytics-v2/costs/receptions/5?company_id=3",
]


@pytest.mark.parametrize("url", ENDPOINTS)
def test_v2_requires_authentication(client, url):
    assert client.get(url).status_code == 401
    assert client.get(url, headers={"Authorization": "Bearer demo-token-12345"}).status_code == 401
    assert client.calls == []


@pytest.mark.parametrize("url", ENDPOINTS)
def test_v2_denies_operational_role(client, url):
    assert client.get(url, headers=_auth("operativo")).status_code == 403
    assert client.calls == []


@pytest.mark.parametrize("role", ["gerencia", "adm"])
def test_v2_allows_management_roles(client, role):
    res = client.get("/analytics-v2/costs/summary?company_id=3", headers=_auth(role))
    assert res.status_code == 200
    assert res.json() == {"ok": True, "company_id": 3}


def test_company_without_raw_returns_409_not_mixed_data(client):
    res = client.get("/analytics-v2/costs/summary?company_id=1", headers=_auth("gerencia"))
    assert res.status_code == 409


def test_company_id_is_required_and_page_size_is_bounded(client):
    h = _auth("gerencia")
    assert client.get("/analytics-v2/costs/summary", headers=h).status_code == 422
    assert client.get("/analytics-v2/costs/products?company_id=3&page_size=201", headers=h).status_code == 422
    assert client.get("/analytics-v2/costs/products?company_id=3&page=0", headers=h).status_code == 422
    assert client.get("/analytics-v2/costs/products?company_id=3&page_size=200", headers=h).status_code == 200


def test_internal_errors_do_not_leak_details(monkeypatch):
    def boom(user, company_id, fn):
        raise RuntimeError("password=secreto host=db")

    monkeypatch.setattr(router_mod, "run_cost_control", boom)
    app = FastAPI()
    app.include_router(router_mod.router)
    res = AsgiClient(app).get("/analytics-v2/costs/summary?company_id=3", headers=_auth("adm"))
    assert res.status_code == 500
    assert "secreto" not in res.text and "password" not in res.text


# ---------------------------------------------------------------- empresa en backend


def _executor(rows):
    seen: list[tuple[str, tuple]] = []

    def execute(sql, params):
        seen.append((sql, params))
        return rows

    execute.seen = seen  # type: ignore[attr-defined]
    return execute


def test_company_scope_ok_and_never_reads_bsale_token():
    ex = _executor([{"company_id": 3, "name": "La Quillotana SpA", "active": True}])
    scope = resolve_company_scope({"role": "gerencia"}, 3, ex)
    assert (scope.company_id, scope.company_name) == (3, "La Quillotana SpA")
    assert "bsale_token" not in COMPANY_SQL
    assert ex.seen == [(COMPANY_SQL, (3,))]


@pytest.mark.parametrize(
    ("user", "company_id", "rows", "status"),
    [
        ({"role": "gerencia", "company_ids": [1, 2]}, 3, [{"company_id": 3, "active": True}], 403),
        ({"role": "gerencia", "company_ids": "3"}, 3, [{"company_id": 3, "active": True}], 403),
        ({"role": "gerencia"}, 99, [], 404),
        ({"role": "gerencia"}, 3, [{"company_id": 3, "active": False}], 403),
        ({"role": "gerencia"}, 2, [{"company_id": 2, "active": True}], 409),
    ],
)
def test_company_scope_denials(user, company_id, rows, status):
    with pytest.raises(AnalyticsV2AccessError) as exc:
        resolve_company_scope(user, company_id, _executor(rows))
    assert exc.value.status_code == status


def test_token_company_claim_allows_listed_company():
    ex = _executor([{"company_id": 3, "name": "LQ", "active": True}])
    assert resolve_company_scope({"company_ids": [3]}, 3, ex).company_id == 3


# ---------------------------------------------------------------- promociones


def test_promotions_router_requires_staff_token_on_every_route():
    app = FastAPI()
    app.include_router(promotions_mod.router)
    c = AsgiClient(app)
    checks = [
        ("get", "/promotions"),
        ("get", "/promotions/grid"),
        ("get", "/promotions/active-snapshot?company_id=3&barcode=1"),
        ("get", "/promotions/1"),
        ("post", "/promotions"),
        ("patch", "/promotions/snapshots/1/sale-price"),
        ("put", "/promotions/1/toggle"),
    ]
    for method, url in checks:
        # Header ausente: la dependencia compartida require_staff_user responde 422; token inválido: 401.
        assert getattr(c, method)(url).status_code in (401, 422), url
        bad = {"Authorization": "Bearer demo-token-12345"}
        assert getattr(c, method)(url, headers=bad).status_code == 401, url
    paths = {getattr(r, "path", None) for r in promotions_mod.router.routes}
    assert {"/promotions", "/promotions/grid", "/promotions/{promotion_id}"} <= paths


# ---------------------------------------------------------------- frontend fail-closed


def test_frontend_login_has_no_demo_fallback():
    api = (REPO_ROOT / "frontend" / "lib" / "api.ts").read_text(encoding="utf-8")
    header = (REPO_ROOT / "frontend" / "components" / "layout" / "header.tsx").read_text(encoding="utf-8")
    assert "DEMO_PASSWORD" not in api
    assert 'role: "admin"' not in api
    assert "falling back to demo mode" not in api
    assert "mockMarginSummary" not in api and "mockCompanies" not in api
    assert "setDemoMode" not in api
    assert "getIsDemoMode" not in header
    login_src = api[api.index("export async function login(") : api.index("export async function getAuthMe(")]
    assert "throw new Error(NETWORK_ERROR_MESSAGE)" in login_src
    assert "LEGACY_DEMO_TOKEN" not in login_src
