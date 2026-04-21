"""Log de duración para rutas ``/distribuidora`` (diagnóstico 502 / timeouts)."""

from __future__ import annotations

import logging
import time

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

logger = logging.getLogger("distribuidora.http")


class DistribuidoraRequestLogMiddleware(BaseHTTPMiddleware):
    """Registra inicio, fin y duración de cada request bajo ``/distribuidora``."""

    async def dispatch(self, request: Request, call_next) -> Response:
        path = request.url.path
        if not path.startswith("/distribuidora"):
            return await call_next(request)

        t0 = time.perf_counter()
        logger.info("start %s %s", request.method, path)
        try:
            response = await call_next(request)
            ms = (time.perf_counter() - t0) * 1000
            logger.info(
                "end %s %s status=%s duration_ms=%.0f",
                request.method,
                path,
                response.status_code,
                ms,
            )
            return response
        except Exception:
            ms = (time.perf_counter() - t0) * 1000
            logger.exception("fail %s %s duration_ms=%.0f", request.method, path, ms)
            raise
