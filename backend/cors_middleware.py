"""
CORS en ASGI puro: responde OPTIONS (preflight) y añade Access-Control-Allow-Origin
en respuestas reales. Evita depender solo de Starlette CORSMiddleware si el proxy o
una versión antigua deja la respuesta sin cabeceras CORS.
"""

from __future__ import annotations

import re
from collections.abc import Sequence

from starlette.types import ASGIApp, Message, Receive, Scope, Send

_ALL_METHODS = b"DELETE, GET, HEAD, OPTIONS, PATCH, POST, PUT"


def _header(scope_headers: list[tuple[bytes, bytes]], name: bytes) -> str | None:
    n = name.lower()
    for k, v in scope_headers:
        if k.lower() == n:
            return v.decode("latin1")
    return None


def _origin_ok(origin: str | None, allow_origins: Sequence[str], rx: re.Pattern[str] | None) -> bool:
    if not origin:
        return False
    if origin in allow_origins:
        return True
    if rx is not None and rx.fullmatch(origin):
        return True
    return False


def _has_acao(headers: list[tuple[bytes, bytes]]) -> bool:
    return any(k.lower() == b"access-control-allow-origin" for k, _ in headers)


class QuillotanaCorsMiddleware:
    """Outer ASGI: OPTIONS preflight + ACAO en http.response.start si falta."""

    def __init__(
        self,
        app: ASGIApp,
        allow_origins: Sequence[str],
        allow_origin_regex: str | None = None,
    ) -> None:
        self.app = app
        self.allow_origins = list(allow_origins)
        self._rx = re.compile(allow_origin_regex) if allow_origin_regex else None

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        headers = scope["headers"]
        method = scope["method"]
        origin = _header(headers, b"origin")

        # Preflight
        if method == "OPTIONS" and _header(headers, b"access-control-request-method"):
            if not _origin_ok(origin, self.allow_origins, self._rx) or not origin:
                await send(
                    {
                        "type": "http.response.start",
                        "status": 400,
                        "headers": [
                            (b"content-type", b"text/plain; charset=utf-8"),
                            (b"content-length", b"24"),
                        ],
                    }
                )
                await send({"type": "http.response.body", "body": b"CORS origin not allowed"})
                return

            acrh = _header(headers, b"access-control-request-headers")
            out: list[tuple[bytes, bytes]] = [
                (b"access-control-allow-origin", origin.encode("utf-8")),
                (b"access-control-allow-methods", _ALL_METHODS),
                (b"access-control-max-age", b"86400"),
                (b"vary", b"Origin"),
            ]
            if acrh:
                out.append((b"access-control-allow-headers", acrh.encode("latin1")))
            else:
                out.append((b"access-control-allow-headers", b"authorization, content-type"))

            await send({"type": "http.response.start", "status": 200, "headers": out})
            await send({"type": "http.response.body", "body": b"OK"})
            return

        if not origin or not _origin_ok(origin, self.allow_origins, self._rx):
            await self.app(scope, receive, send)
            return

        async def send_with_cors(message: Message) -> None:
            if message["type"] == "http.response.start":
                hdrs = list(message.get("headers") or [])
                if not _has_acao(hdrs):
                    hdrs.append((b"access-control-allow-origin", origin.encode("utf-8")))
                    vary_ok = any(k.lower() == b"vary" for k, _ in hdrs)
                    if not vary_ok:
                        hdrs.append((b"vary", b"Origin"))
                    message = {**message, "headers": hdrs}
            await send(message)

        await self.app(scope, receive, send_with_cors)
