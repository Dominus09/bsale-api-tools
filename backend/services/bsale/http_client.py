"""Cliente HTTP Bsale compartido: timeouts, retry acotado sólo para errores transitorios y paginación."""

from __future__ import annotations

import email.utils
import logging
import random
import time
from datetime import datetime, timezone
from typing import Any, Callable
from urllib.parse import urlsplit

import requests

logger = logging.getLogger(__name__)

BASE_URL = "https://api.bsale.io/v1"
DEFAULT_LIMIT = 50
DEFAULT_CONNECT_TIMEOUT = 10.0
DEFAULT_READ_TIMEOUT = 60.0
DEFAULT_MAX_ATTEMPTS = 5
DEFAULT_BACKOFF_BASE = 1.0
DEFAULT_BACKOFF_MAX = 60.0
DEFAULT_RETRY_AFTER_MAX = 300.0
DEFAULT_MAX_PAGES = 5000

TRANSIENT_STATUS = frozenset({408, 425, 429, 500, 502, 503, 504})

_BODY_SNIPPET_CHARS = 200


class BsaleHttpError(RuntimeError):
    """Error HTTP Bsale. El mensaje nunca incluye el access_token."""

    def __init__(
        self,
        message: str,
        *,
        endpoint: str,
        status: int | None = None,
        attempt: int | None = None,
    ) -> None:
        self.endpoint = endpoint
        self.status = status
        self.attempt = attempt
        super().__init__(
            f"{message} (endpoint={endpoint} status={status} attempt={attempt})"
        )


class BsaleRetryExhaustedError(BsaleHttpError):
    """Se agotaron los intentos ante errores transitorios."""


class BsaleResponseError(BsaleHttpError):
    """Respuesta 2xx con cuerpo inválido (no JSON / no objeto / sin items)."""


class BsalePaginationError(BsaleHttpError):
    """Paginación inconsistente (offset repetido, página repetida o demasiadas páginas)."""


def endpoint_label(url: str) -> str:
    """Ruta del endpoint sin query string (la query nunca se loguea)."""
    parts = urlsplit(url)
    return parts.path or url


def _parse_retry_after(value: str | None) -> float | None:
    if not value:
        return None
    value = value.strip()
    try:
        return max(0.0, float(value))
    except ValueError:
        pass
    try:
        dt = email.utils.parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return max(0.0, (dt - datetime.now(timezone.utc)).total_seconds())


class BsaleHttpClient:
    """
    GET JSON contra la API Bsale con ``requests.Session``.

    - Retry sólo para 408/425/429/500/502/503/504, ``Timeout`` y ``ConnectionError``.
    - Máximo ``max_attempts`` intentos; luego ``BsaleRetryExhaustedError``.
    - 4xx no transitorios (400/401/403/404...) fallan de inmediato.
    - Espera: ``Retry-After`` si existe; si no, backoff exponencial con jitter.
    """

    def __init__(
        self,
        token: str,
        *,
        base_url: str = BASE_URL,
        session: requests.Session | None = None,
        connect_timeout: float = DEFAULT_CONNECT_TIMEOUT,
        read_timeout: float = DEFAULT_READ_TIMEOUT,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        backoff_base: float = DEFAULT_BACKOFF_BASE,
        backoff_max: float = DEFAULT_BACKOFF_MAX,
        retry_after_max: float = DEFAULT_RETRY_AFTER_MAX,
        sleep: Callable[[float], None] = time.sleep,
        rng: Callable[[], float] = random.random,
    ) -> None:
        if not token:
            raise ValueError("BsaleHttpClient requiere token")
        if max_attempts < 1:
            raise ValueError("max_attempts debe ser >= 1")
        self._token = token
        self.base_url = base_url.rstrip("/")
        self._allowed_host = urlsplit(self.base_url).netloc
        self.session = session or requests.Session()
        self.timeout = (connect_timeout, read_timeout)
        self.max_attempts = max_attempts
        self.backoff_base = backoff_base
        self.backoff_max = backoff_max
        self.retry_after_max = retry_after_max
        self._sleep = sleep
        self._rng = rng

    def __repr__(self) -> str:
        return f"BsaleHttpClient(base_url={self.base_url!r})"

    def _url(self, path_or_url: str) -> str:
        if path_or_url.startswith(("http://", "https://")):
            host = urlsplit(path_or_url).netloc
            if host != self._allowed_host:
                raise BsaleHttpError(
                    "Host no permitido para enviar credenciales Bsale",
                    endpoint=endpoint_label(path_or_url),
                )
            return path_or_url
        return f"{self.base_url}/{path_or_url.lstrip('/')}"

    def _backoff_delay(self, attempt: int) -> float:
        ceiling = min(self.backoff_max, self.backoff_base * (2 ** (attempt - 1)))
        return ceiling / 2 + self._rng() * ceiling / 2

    def _retry_delay(self, response: requests.Response | None, attempt: int) -> float:
        if response is not None:
            wait = _parse_retry_after(response.headers.get("Retry-After"))
            if wait is None and response.status_code == 429:
                try:
                    body = response.json()
                    if isinstance(body, dict) and body.get("retry_after") is not None:
                        wait = max(0.0, float(body["retry_after"]))
                except (ValueError, TypeError):
                    wait = None
            if wait is not None:
                return min(wait, self.retry_after_max)
        return self._backoff_delay(attempt)

    def get_json(self, path_or_url: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        url = self._url(path_or_url)
        endpoint = endpoint_label(url)
        headers = {"access_token": self._token, "Accept": "application/json"}
        last_status: int | None = None
        last_reason = ""

        for attempt in range(1, self.max_attempts + 1):
            response: requests.Response | None = None
            try:
                response = self.session.get(
                    url, headers=headers, params=params, timeout=self.timeout
                )
            except (requests.Timeout, requests.ConnectionError) as exc:
                last_status = None
                last_reason = type(exc).__name__
            else:
                status = response.status_code
                if 200 <= status < 300:
                    return self._decode(response, endpoint=endpoint, attempt=attempt)
                if status not in TRANSIENT_STATUS:
                    snippet = (response.text or "")[:_BODY_SNIPPET_CHARS].strip()
                    raise BsaleHttpError(
                        f"HTTP {status} no reintentable: {snippet}",
                        endpoint=endpoint,
                        status=status,
                        attempt=attempt,
                    )
                last_status = status
                last_reason = f"HTTP {status}"

            if attempt >= self.max_attempts:
                break
            delay = self._retry_delay(response, attempt)
            logger.warning(
                "[BSALE_HTTP] retry endpoint=%s status=%s reason=%s attempt=%s/%s wait=%.2fs",
                endpoint,
                last_status,
                last_reason,
                attempt,
                self.max_attempts,
                delay,
            )
            self._sleep(delay)

        raise BsaleRetryExhaustedError(
            f"Reintentos agotados ({last_reason})",
            endpoint=endpoint,
            status=last_status,
            attempt=self.max_attempts,
        )

    @staticmethod
    def _decode(response: requests.Response, *, endpoint: str, attempt: int) -> dict[str, Any]:
        try:
            data = response.json()
        except ValueError as exc:
            raise BsaleResponseError(
                "Respuesta no es JSON válido",
                endpoint=endpoint,
                status=response.status_code,
                attempt=attempt,
            ) from exc
        if not isinstance(data, dict):
            raise BsaleResponseError(
                f"JSON no es objeto ({type(data).__name__})",
                endpoint=endpoint,
                status=response.status_code,
                attempt=attempt,
            )
        return data

    def fetch_all_items(
        self,
        path_or_url: str,
        params: dict[str, Any] | None = None,
        *,
        limit: int = DEFAULT_LIMIT,
        max_pages: int = DEFAULT_MAX_PAGES,
    ) -> list[dict[str, Any]]:
        """
        Descarga todas las páginas con limit/offset.

        Termina con página vacía, página menor a ``limit`` u ``offset >= count``.
        Falla ante offset repetido, página idéntica a la anterior o ``max_pages`` excedido.
        """
        base_params = dict(params or {})
        url = self._url(path_or_url)
        endpoint = endpoint_label(url)
        items: list[dict[str, Any]] = []
        seen_offsets: set[int] = set()
        prev_signature: tuple[Any, ...] | None = None
        offset = 0

        for _ in range(max_pages):
            if offset in seen_offsets:
                raise BsalePaginationError(f"Offset repetido {offset}", endpoint=endpoint)
            seen_offsets.add(offset)

            data = self.get_json(url, {**base_params, "limit": limit, "offset": offset})
            page = data.get("items")
            if page is None:
                raise BsaleResponseError("Respuesta paginada sin 'items'", endpoint=endpoint)
            if not isinstance(page, list):
                raise BsaleResponseError("'items' no es lista", endpoint=endpoint)
            if not page:
                return items

            signature = tuple(
                it.get("id") if isinstance(it, dict) else repr(it) for it in page
            )
            if prev_signature is not None and signature == prev_signature:
                raise BsalePaginationError(
                    f"Página repetida en offset {offset}", endpoint=endpoint
                )
            prev_signature = signature

            for it in page:
                if not isinstance(it, dict):
                    raise BsaleResponseError("Item no es objeto", endpoint=endpoint)
            items.extend(page)

            if len(page) < limit:
                return items
            offset += limit
            count = data.get("count")
            if isinstance(count, int) and offset >= count:
                return items

        raise BsalePaginationError(f"Se excedió max_pages={max_pages}", endpoint=endpoint)
