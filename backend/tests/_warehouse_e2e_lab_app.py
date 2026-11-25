"""App FastAPI mínima para lab E2E warehouse (PG aislado). No usa prod."""

from __future__ import annotations

import os
from typing import Any

import psycopg2
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from backend.auth.permissions import require_management_access
from backend.routers import warehouse as wh
from backend.services.warehouse.service import WarehouseLayoutService


def _assert_isolated(dsn: str) -> None:
    low = dsn.lower()
    for ban in ("analytics", "quillotana", "prod", "production", "coolify", "neon.tech", "supabase", "amazonaws.com"):
        if ban in low:
            raise RuntimeError(f"DSN no aislado: {ban}")


def build_lab_app(dsn: str) -> FastAPI:
    _assert_isolated(dsn)

    def _factory():
        return psycopg2.connect(dsn)

    app = FastAPI(title="warehouse-e2e-lab")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.include_router(wh.router)

    def _mgmt_user() -> dict[str, Any]:
        return {
            "id": 9001,
            "email": "warehouse-lab@test.local",
            "role": "gerencia",
            "management_access": True,
            "is_owner": False,
        }

    app.dependency_overrides[require_management_access] = _mgmt_user
    # El router llama _svc() → default_service(); monkeypatch vía atributo
    svc = WarehouseLayoutService(_factory)
    app.dependency_overrides  # keep
    wh._svc = lambda: svc  # type: ignore[assignment]

    @app.get("/auth/me")
    def auth_me():
        u = _mgmt_user()
        return {
            **u,
            "name": "Warehouse Lab",
            "erp_capabilities": [],
            "permissions": {"margins": True},
            "admin_access": False,
            "apps": ["erp"],
        }

    @app.get("/health")
    def health():
        return {"ok": True, "lab": "warehouse-e2e", "dsn_isolated": True}

    return app
