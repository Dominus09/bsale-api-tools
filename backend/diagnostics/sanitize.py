"""Sanitización de query strings y textos para evitar filtrar secretos."""

from __future__ import annotations

import re
from urllib.parse import parse_qsl, urlencode

_SENSITIVE_SUBSTRINGS = (
    "password",
    "passwd",
    "token",
    "authorization",
    "cookie",
    "secret",
    "apikey",
    "api_key",
    "refreshtoken",
    "refresh_token",
    "accesstoken",
    "access_token",
    "clave",
    "contraseña",
    "contrase",
)


def _key_is_sensitive(key: str) -> bool:
    lk = key.lower().strip()
    return any(s in lk for s in _SENSITIVE_SUBSTRINGS)


def sanitize_query_string(query: str | None, *, placeholder: str = "[redacted]") -> str:
    if not query:
        return ""
    pairs: list[tuple[str, str]] = []
    for k, v in parse_qsl(query, keep_blank_values=True):
        if _key_is_sensitive(k):
            pairs.append((k, placeholder))
        else:
            pairs.append((k, v))
    return urlencode(pairs)


_REDACT_TOKEN = re.compile(
    r"(?i)(bearer\s+)([a-z0-9\-._~+/]+=*)(?=\s|$)",
    re.IGNORECASE,
)


def sanitize_free_text(text: str | None, *, max_len: int = 2000) -> str:
    if not text:
        return ""
    t = str(text)
    t = _REDACT_TOKEN.sub(r"\1[redacted]", t)
    if len(t) > max_len:
        return t[:max_len] + "…"
    return t
