"""Rate limiting simple en memoria (proceso). Suficiente para staging/API única.

No sustituye un WAF/redis en multi-réplica; documentado como limitación.
"""

from __future__ import annotations

import os
import threading
import time
from collections import defaultdict, deque

from fastapi import HTTPException

_lock = threading.Lock()
_buckets: dict[str, deque[float]] = defaultdict(deque)


def _limit_from_env(name: str, default: int) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return max(1, int(raw))
    except ValueError:
        return default


def check_rate_limit(
    *,
    key: str,
    limit: int | None = None,
    window_seconds: int | None = None,
) -> None:
    lim = limit if limit is not None else _limit_from_env("AUTH_PASSWORD_RESET_RATE_LIMIT", 20)
    win = (
        window_seconds
        if window_seconds is not None
        else _limit_from_env("AUTH_PASSWORD_RESET_RATE_WINDOW_SEC", 300)
    )
    now = time.monotonic()
    with _lock:
        q = _buckets[key]
        while q and now - q[0] > win:
            q.popleft()
        if len(q) >= lim:
            raise HTTPException(
                status_code=429,
                detail="Demasiados intentos. Espere unos minutos e intente de nuevo",
            )
        q.append(now)
