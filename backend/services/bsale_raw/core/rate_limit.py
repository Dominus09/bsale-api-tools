"""Rate limiter por empresa/token con prioridad central para la capa ``bsale_raw``.

Límite oficial documentado (FAQ Bsale, código 429): 3.000 requests cada 300 segundos. En la
verificación en vivo (fase 2) no se observaron headers de cuota, y el alcance del límite (token,
instancia o IP) no está demostrado: por eso cada empresa tiene un único limitador local
conservador, compartido por TODOS los consumidores de esa empresa, y se respeta 429/Retry-After.

Prioridades (menor número = mayor prioridad): P0 webhook/targeted, P1 OC33, P2 stock,
P3 prices, P4 catalog, P5 costs, P6 clients/config.
"""

from __future__ import annotations

import heapq
import itertools
import os
import threading
import time
from dataclasses import dataclass
from enum import IntEnum
from typing import Callable

import requests

BSALE_DOCUMENTED_MAX_REQUESTS = 3000
BSALE_DOCUMENTED_WINDOW_SECONDS = 300.0

DEFAULT_BUDGET_FRACTION = 0.5
DEFAULT_BURST = 10


class RequestPriority(IntEnum):
    P0_TARGETED = 0
    P1_OC33 = 1
    P2_STOCK = 2
    P3_PRICES = 3
    P4_CATALOG = 4
    P5_COSTS = 5
    P6_CLIENTS_CONFIG = 6


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
    """Token bucket thread-safe."""

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

    def try_acquire(self) -> bool:
        with self._lock:
            self._refill()
            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return True
            return False

    def seconds_until_token(self) -> float:
        with self._lock:
            self._refill()
            if self._tokens >= 1.0:
                return 0.0
            return (1.0 - self._tokens) / self.config.requests_per_second

    def acquire(self) -> float:
        """Consume un token; devuelve los segundos esperados."""
        waited = 0.0
        while not self.try_acquire():
            wait = self.seconds_until_token()
            self._sleep(wait)
            waited += wait
        return waited


class PriorityRateLimiter:
    """Limitador único de una empresa: cada token se entrega al waiter de mayor prioridad (FIFO dentro de la misma)."""

    def __init__(self, bucket: TokenBucket) -> None:
        self.bucket = bucket
        self._cond = threading.Condition()
        self._waiting: list[tuple[int, int]] = []
        self._seq = itertools.count()

    def waiting_count(self) -> int:
        with self._cond:
            return len(self._waiting)

    def poke(self) -> None:
        """Despierta a los waiters (p. ej. tras avanzar un reloj de prueba)."""
        with self._cond:
            self._cond.notify_all()

    def acquire(self, priority: RequestPriority) -> None:
        ticket = (int(priority), next(self._seq))
        with self._cond:
            heapq.heappush(self._waiting, ticket)
            self._cond.notify_all()
            try:
                while True:
                    if self._waiting[0] == ticket and self.bucket.try_acquire():
                        return
                    if self._waiting[0] == ticket:
                        self._cond.wait(timeout=max(self.bucket.seconds_until_token(), 0.001))
                    else:
                        self._cond.wait()
            finally:
                self._waiting.remove(ticket)
                heapq.heapify(self._waiting)
                self._cond.notify_all()


class CompanyRateLimiters:
    """Un ``PriorityRateLimiter`` por ``company_id``; las empresas no comparten presupuesto."""

    def __init__(self, factory: Callable[[int], PriorityRateLimiter] | None = None) -> None:
        self._factory = factory or (
            lambda cid: PriorityRateLimiter(TokenBucket(RateLimitConfig.from_env(cid)))
        )
        self._limiters: dict[int, PriorityRateLimiter] = {}
        self._lock = threading.Lock()

    def for_company(self, company_id: int) -> PriorityRateLimiter:
        with self._lock:
            limiter = self._limiters.get(company_id)
            if limiter is None:
                limiter = self._factory(company_id)
                self._limiters[company_id] = limiter
            return limiter


class RateLimitedSession(requests.Session):
    """``requests.Session`` que pide turno al limitador de la empresa antes de cada request, reintentos incluidos."""

    def __init__(self, limiter: PriorityRateLimiter, priority: RequestPriority) -> None:
        super().__init__()
        self._limiter = limiter
        self.priority = priority
        self.last_rate_limit_headers: dict[str, str] = {}

    def request(self, method, url, *args, **kwargs):  # type: ignore[override]
        self._limiter.acquire(self.priority)
        response = super().request(method, url, *args, **kwargs)
        self.last_rate_limit_headers = {
            k: v for k, v in response.headers.items() if k.lower().startswith(("x-ratelimit", "ratelimit", "retry-after"))
        }
        return response
