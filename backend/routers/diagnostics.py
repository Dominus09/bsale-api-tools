"""API interna de diagnóstico (protegida: rol admin + flags de entorno)."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, Request

from backend.db import get_connection
from backend.diagnostics import store
from backend.diagnostics.security import diagnostics_feature_enabled, require_diagnostics_admin

router = APIRouter(prefix="/diagnostics", tags=["Diagnostics"])


def _environment_label() -> str:
    return (os.getenv("ENVIRONMENT") or os.getenv("ENV") or "development").strip()


def _version(request: Request) -> str:
    return (os.getenv("APP_VERSION") or getattr(request.app, "version", None) or "1.0.0").strip()


def _database_status() -> tuple[str, str | None]:
    try:
        conn = get_connection()
        try:
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.close()
        finally:
            conn.close()
        return "connected", None
    except Exception as e:
        return "disconnected", str(e)[:200]


@router.get("/health")
def diagnostics_health(
    request: Request,
    _user: dict = Depends(require_diagnostics_admin),
) -> dict[str, Any]:
    db_state, db_err = _database_status()
    overall = "ok" if db_state == "connected" else "degraded"
    return {
        "status": overall,
        "backend": "online",
        "database": db_state,
        "databaseError": db_err,
        "environment": _environment_label(),
        "uptime": store.process_uptime_seconds(),
        "serverTime": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "version": _version(request),
        "recentRequestCount": store.recent_request_count(),
        "recentErrorCount": store.recent_error_count(),
        "avgResponseTimeMs": store.global_avg_duration_ms(),
        "diagnosticsApiEnabled": diagnostics_feature_enabled(),
    }


@router.get("/requests")
def diagnostics_requests(
    limit: int = 200,
    _user: dict = Depends(require_diagnostics_admin),
) -> dict[str, Any]:
    lim = max(1, min(500, limit))
    return {"items": store.list_requests(lim)}


@router.get("/logs")
def diagnostics_logs(
    limit: int = 200,
    _user: dict = Depends(require_diagnostics_admin),
) -> dict[str, Any]:
    lim = max(1, min(500, limit))
    return {"items": store.list_logs(lim)}


@router.get("/errors")
def diagnostics_errors(
    limit: int = 100,
    _user: dict = Depends(require_diagnostics_admin),
) -> dict[str, Any]:
    lim = max(1, min(300, limit))
    return {"items": store.list_errors(lim)}


def _registered_routes(request: Request) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for route in request.app.routes:
        methods = getattr(route, "methods", None)
        path = getattr(route, "path", None)
        name = getattr(route, "name", "") or ""
        if methods and path:
            for m in sorted(methods):
                if m in ("HEAD",):
                    continue
                out.append(
                    {
                        "method": m,
                        "path": path,
                        "name": name,
                        "description": "",
                    }
                )
    out.sort(key=lambda r: (r["path"], r["method"]))
    return out


@router.get("/endpoints")
def diagnostics_endpoints(
    request: Request,
    _user: dict = Depends(require_diagnostics_admin),
) -> dict[str, Any]:
    return {
        "registered": _registered_routes(request),
        "observed": store.aggregate_endpoint_stats(),
    }
