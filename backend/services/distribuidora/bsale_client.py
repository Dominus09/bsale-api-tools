"""Cliente HTTP mínimo para API Bsale v1."""

from __future__ import annotations

import logging
import random
import time
from typing import Any

import requests

from backend.services.distribuidora.bsale_params import (
    BSALE_QUERY_OFFICE_ID,
    log_office_filter_debug_response,
)

logger = logging.getLogger(__name__)

BASE_BSALE = "https://api.bsale.io/v1"
MAX_TRANSIENT = 40
DEFAULT_429_BACKOFF_SCHEDULE = (5.0, 10.0, 20.0, 40.0, 60.0)
DEFAULT_429_BACKOFF_MAX = 60.0


def parse_retry_after_seconds(response: requests.Response) -> float | None:
    """
    Lee Retry-After (header) o retry_after (body JSON).
    Retorna segundos o None si no hay valor usable.
    """
    header = response.headers.get("Retry-After") or response.headers.get("retry-after")
    if header is not None:
        raw = str(header).strip()
        if raw.isdigit():
            return float(int(raw))
        try:
            return float(raw)
        except ValueError:
            pass

    try:
        body = response.json()
    except Exception:
        return None
    if not isinstance(body, dict):
        return None
    for key in ("retry_after", "retryAfter", "Retry-After"):
        if key in body and body[key] is not None:
            try:
                return float(body[key])
            except (TypeError, ValueError):
                continue
    return None


def compute_429_wait_seconds(
    response: requests.Response,
    *,
    retry_index: int,
    backoff_schedule: tuple[float, ...] = DEFAULT_429_BACKOFF_SCHEDULE,
    backoff_max: float = DEFAULT_429_BACKOFF_MAX,
    jitter_max: float = 0.35,
) -> float:
    """
    Si hay Retry-After → usarlo exacto (+ jitter pequeño).
    Si no → exponential schedule 5/10/20/40/60 capped.
    ``retry_index`` es 0-based (primer 429 → 0).
    """
    from_header = parse_retry_after_seconds(response)
    if from_header is not None and from_header >= 0:
        base = float(from_header)
    else:
        idx = min(max(retry_index, 0), len(backoff_schedule) - 1)
        base = float(backoff_schedule[idx])
        base = min(base, backoff_max)
    jitter = random.uniform(0.0, max(0.0, jitter_max))
    return base + jitter


class BsaleClient:
    def __init__(
        self,
        token: str,
        *,
        min_interval_sec: float | None = None,
        min_interval_jitter_sec: float = 0.0,
        max_429_retries: int | None = None,
        rate_stats: dict[str, Any] | None = None,
    ) -> None:
        """
        ``min_interval_sec``: throttle global entre requests (None = sin throttle extra).
        ``max_429_retries``: tope de reintentos tras 429 (None = reintentar indefinido, legado).
        ``rate_stats``: contadores mutables (requests_total, rate_limit_events, ...).
        """
        self._token = token
        self.session = requests.Session()
        self._min_interval_sec = min_interval_sec
        self._min_interval_jitter_sec = max(0.0, float(min_interval_jitter_sec))
        self._max_429_retries = max_429_retries
        self._rate_stats = rate_stats
        self._last_request_monotonic: float | None = None

    @property
    def access_token(self) -> str:
        return self._token

    def _bump_stat(self, key: str, delta: float | int = 1) -> None:
        if self._rate_stats is None:
            return
        prev = self._rate_stats.get(key) or 0
        self._rate_stats[key] = prev + delta

    def _throttle_before_request(self) -> None:
        if self._min_interval_sec is None or self._min_interval_sec <= 0:
            return
        interval = float(self._min_interval_sec)
        if self._min_interval_jitter_sec > 0:
            interval += random.uniform(0.0, self._min_interval_jitter_sec)
        now = time.monotonic()
        if self._last_request_monotonic is not None:
            elapsed = now - self._last_request_monotonic
            if elapsed < interval:
                wait = interval - elapsed
                time.sleep(wait)
                self._bump_stat("wait_seconds_total", float(wait))
                self._bump_stat("throttle_wait_seconds", float(wait))

    def get(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        *,
        timeout: int = 45,
    ) -> dict[str, Any]:
        """``path`` puede ser ruta relativa (``/documents.json``) o URL absoluta."""
        url = path if path.startswith("http") else f"{BASE_BSALE}{path}"
        params = params or {}
        transient = 0
        rate_limit_retries = 0
        while True:
            self._throttle_before_request()
            try:
                r = self.session.get(
                    url,
                    headers={"access_token": self._token},
                    params=params,
                    timeout=timeout,
                )
            except requests.RequestException as e:
                transient += 1
                if transient >= MAX_TRANSIENT:
                    raise RuntimeError(f"Bsale red: {e}") from e
                logger.warning("Bsale red (%s/%s): %s", transient, MAX_TRANSIENT, e)
                time.sleep(3)
                continue
            finally:
                self._last_request_monotonic = time.monotonic()
                self._bump_stat("requests_total", 1)

            if r.status_code == 401:
                raise RuntimeError(
                    "Bsale 401 Unauthorized — revisar BSALE_TOKEN o BSALE_TOKEN_SPA"
                )

            if r.status_code == 429:
                self._bump_stat("rate_limit_events", 1)
                if (
                    self._max_429_retries is not None
                    and rate_limit_retries >= self._max_429_retries
                ):
                    raise RuntimeError(
                        f"Bsale HTTP 429: rate limit agotado tras "
                        f"{self._max_429_retries} retries: {(r.text or '')[:500]}"
                    )
                wait = compute_429_wait_seconds(r, retry_index=rate_limit_retries)
                rate_limit_retries += 1
                self._bump_stat("retry_count", 1)
                self._bump_stat("wait_seconds_total", float(wait))
                logger.warning(
                    "Bsale 429 — retry %s/%s esperando %.2f s",
                    rate_limit_retries,
                    self._max_429_retries
                    if self._max_429_retries is not None
                    else "∞",
                    wait,
                )
                time.sleep(wait)
                continue

            if r.status_code in (500, 502, 503, 504):
                transient += 1
                if transient >= MAX_TRANSIENT:
                    raise RuntimeError(f"Bsale HTTP {r.status_code} persistente")
                logger.warning("Bsale HTTP %s — reintento 3s", r.status_code)
                time.sleep(3)
                continue

            if not (200 <= r.status_code < 300):
                raise RuntimeError(f"Bsale HTTP {r.status_code}: {(r.text or '')[:500]}")

            if BSALE_QUERY_OFFICE_ID in params:
                log_office_filter_debug_response(
                    method="GET",
                    path=path,
                    params=params,
                    response_url=getattr(r.request, "url", None),
                    context="BsaleClient.get",
                )

            transient = 0
            return r.json()
