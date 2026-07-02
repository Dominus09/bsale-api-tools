"""Middleware ASGI: registra cada petición HTTP en el store de diagnóstico (sin secretos)."""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from backend.auth.jwt import staff_email_from_authorization
from backend.diagnostics import store
from backend.diagnostics.sanitize import sanitize_free_text, sanitize_query_string
from backend.diagnostics.security import diagnostics_feature_enabled

logger = logging.getLogger("diagnostics.http")


def _client_ip(request: Request) -> str | None:
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip() or None
    if request.client:
        return request.client.host
    return None


def _user_from_auth_header(request: Request) -> str | None:
    return staff_email_from_authorization(request.headers.get("authorization"))


def _safe_path(request: Request) -> str:
    path = request.url.path
    q = request.url.query
    if not q:
        return path
    return f"{path}?{sanitize_query_string(q)}"


class DiagnosticsRequestLogMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next) -> Response:
        if not diagnostics_feature_enabled():
            return await call_next(request)

        path = request.url.path
        if request.method == "OPTIONS":
            return await call_next(request)
        if path == "/favicon.ico":
            return await call_next(request)
        if path in ("/docs", "/redoc", "/openapi.json"):
            return await call_next(request)

        t0 = time.perf_counter()
        ts = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        safe_path = _safe_path(request)
        user = _user_from_auth_header(request)
        ip = _client_ip(request)
        ua = request.headers.get("user-agent")
        origin = request.headers.get("origin") or request.headers.get("referer")

        err_msg: str | None = None
        status = 500
        try:
            response = await call_next(request)
            status = response.status_code
            return response
        except Exception as e:
            err_msg = sanitize_free_text(str(e), max_len=500)
            logger.exception("diagnostics: request failed %s %s", request.method, safe_path)
            raise
        finally:
            ms = (time.perf_counter() - t0) * 1000.0
            try:
                store.append_request(
                    store.RequestRecord(
                        ts_iso=ts,
                        method=request.method,
                        path=safe_path,
                        status_code=status,
                        duration_ms=ms,
                        client_ip=ip,
                        user=user,
                        user_agent=sanitize_free_text(ua, max_len=400) if ua else None,
                        origin=sanitize_free_text(origin, max_len=400) if origin else None,
                        error=err_msg,
                    )
                )
            except Exception:
                logger.exception("diagnostics: failed to append request record")
