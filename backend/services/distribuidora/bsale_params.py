"""
Parámetros query Bsale (nombres oficiales en minúsculas según documentación Chile).

https://apichile.bsalelab.com/lista-de-endpoints/documentos
"""

from __future__ import annotations

import logging
from typing import Any
from urllib.parse import urlencode

logger = logging.getLogger(__name__)

# Clave oficial GET listado documentos / related (no usar officeId camelCase).
BSALE_QUERY_OFFICE_ID = "officeid"


def merge_bsale_office_query(
    params: dict[str, Any] | None,
    office_id: int,
    *,
    context: str = "",
) -> dict[str, Any]:
    """
    Copia ``params`` e inyecta ``officeid`` (sobrescribe camelCase legacy si existía).

    Elimina ``officeId`` si el caller lo pasó por error.
    """
    out = dict(params or {})
    out.pop("officeId", None)
    out[BSALE_QUERY_OFFICE_ID] = office_id
    if logger.isEnabledFor(logging.DEBUG) or _office_filter_debug_enabled():
        _log_office_filter_debug_params(out, office_id, context=context)
    return out


def _office_filter_debug_enabled() -> bool:
    import os

    return os.getenv("OFFICE_FILTER_DEBUG", "").strip().lower() in ("1", "true", "yes")


def _log_office_filter_debug_params(
    params: dict[str, Any],
    office_id: int,
    *,
    context: str = "",
) -> None:
    ctx = f" {context}" if context else ""
    logger.info(
        "[OFFICE_FILTER_DEBUG]%s officeid=%s params=%s",
        ctx,
        office_id,
        params,
    )


def log_office_filter_debug_response(
    *,
    method: str,
    path: str,
    params: dict[str, Any],
    response_url: str | None,
    context: str = "",
) -> None:
    """Log URL final tras la petición (requests expande query string)."""
    if not (_office_filter_debug_enabled() or logger.isEnabledFor(logging.DEBUG)):
        return
    ctx = f" {context}" if context else ""
    oid = params.get(BSALE_QUERY_OFFICE_ID)
    logger.info(
        "[OFFICE_FILTER_DEBUG]%s %s %s officeid=%s url=%s",
        ctx,
        method,
        path,
        oid,
        response_url or _preview_url(path, params),
    )


def _preview_url(path: str, params: dict[str, Any]) -> str:
    from backend.services.distribuidora.bsale_client import BASE_BSALE

    base = path if path.startswith("http") else f"{BASE_BSALE}{path}"
    qs = urlencode(params, doseq=True)
    return f"{base}?{qs}" if qs else base
