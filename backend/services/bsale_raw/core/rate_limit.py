"""Rate limiter por empresa/token para la capa ``bsale_raw``.

Límite oficial documentado (FAQ Bsale, código 429): 3.000 requests cada 300 segundos.
El alcance exacto (por token, por instancia o por IP) es ``NEEDS_LIVE_VERIFICATION``;
por eso cada empresa tiene su propio limitador y el presupuesto por defecto queda bajo
el límite oficial para dejar margen a los syncs legacy que comparten token.
"""

from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass
from typing import Callable

import requests

BSALE_DOCUMENTED_MAX_REQUESTS = 3000
BSALE_DOCUMENTED_WINDOW_SECONDS = 300.0

DEFAULT_BUDGET_FRACTION = 0.5
DEFAULT_BURST = 10


@dataclass(frozen=True)
class RateLimitConfig:
    requests_per_second: float
    burst: int

    @classmethod
    def from_env(cls, company_id: int) -> "RateLimitConfig":
        """``BSALE_RAW_RPS_<company_id>`` > ``BSALE_RAW_RPS`` > 50 % del límite oficial."""
        documented_rps = BSALE_DOCUMENTED_MAX_REQUESTS / BSALE_DOCUMENTED_WINDOW_SECONDS
        raw = os.getenv(f"BSALE_RAW_RPS_{company_id}") or os.getenv("BSALE_RAW_RPS")
        rps = float(raw) if raw else documented_rps * DEFAULT_BUDGET_FRACTION
        if rps <= 0 or rps > documented_rps:
            raise ValueError(
                f"RPS inválido para company_id={company_id}: {rps} (máximo documentado {documented_rps})"
            )
        burst = int(os.getenv("BSALE_RAW_BURST") or DEFAULT_BURST)
        if burst < 1:
            raise ValueError("BSALE_RAW_BURST debe ser >= 1")
        return cls(requests_per_second=rps, burst=burst)


class TokenBucket:
    """Token bucket thread-safe. ``acquire`` bloquea hasta disponer de un token."""

    def __init__(
        self,
        config: RateLimitConfig,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.config = config
        self._clock = clock
        self._sleep = sleep
        self._tokens = float(config.burst)
        self._updated = clock()
        self._lock = threading.Lock()

    def _refill(self) -> None:
        now = self._clock()
        elapsed = max(0.0, now - self._updated)
        self._tokens = min(
            float(self.config.burst), self._tokens + elapsed * self.config.requests_per_second
        )
        self._updated = now

    def acquire(self) -> float:
        """Consume un token; devuelve los segundos esperados."""
        waited = 0.0
        while True:
            with self._lock:
                self._refill()
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return waited
                wait = (1.0 - self._tokens) / self.config.requests_per_second
            self._sleep(wait)
            waited += wait


class CompanyRateLimiters:
    """Un ``TokenBucket`` por ``company_id``; las empresas no comparten presupuesto."""

    def __init__(self, factory: Callable[[int], TokenBucket] | None = None) -> None:
        self._factory = factory or (lambda cid: TokenBucket(RateLimitConfig.from_env(cid)))
        self._buckets: dict[int, TokenBucket] = {}
        self._lock = threading.Lock()

    def for_company(self, company_id: int) -> TokenBucket:
        with self._lock:
            bucket = self._buckets.get(company_id)
            if bucket is None:
                bucket = self._factory(company_id)
                self._buckets[company_id] = bucket
            return bucket


class RateLimitedSession(requests.Session):
    """``requests.Session`` que consume un token antes de cada request, reintentos incluidos."""

    def __init__(self, bucket: TokenBucket) -> None:
        super().__init__()
        self._bucket = bucket
        self.last_rate_limit_headers: dict[str, str] = {}

    def request(self, method, url, *args, **kwargs):  # type: ignore[override]
        self._bucket.acquire()
        response = super().request(method, url, *args, **kwargs)
        self.last_rate_limit_headers = {
            k: v for k, v in response.headers.items() if k.lower().startswith(("x-ratelimit", "ratelimit", "retry-after"))
        }
        return response
