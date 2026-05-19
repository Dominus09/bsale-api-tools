"""Cliente HTTP mínimo para API Bsale v1."""

from __future__ import annotations

import logging
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


class BsaleClient:
    def __init__(self, token: str) -> None:
        self._token = token
        self.session = requests.Session()

    @property
    def access_token(self) -> str:
        return self._token

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
        while True:
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

            if r.status_code == 401:
                raise RuntimeError(
                    "Bsale 401 Unauthorized — revisar BSALE_TOKEN o BSALE_TOKEN_SPA"
                )

            if r.status_code == 429:
                try:
                    wait = int(r.json().get("retry_after", 60))
                except Exception:
                    wait = 60
                logger.warning("Bsale 429 — esperando %s s", wait)
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
