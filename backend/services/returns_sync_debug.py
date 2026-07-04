"""
Diagnóstico de sync returns — logs detallados sin alterar parámetros de negocio.

Referencias implementadas en el repo:
- returnDate en respuesta: Integer Unix GMT
  https://apichile.bsalelab.com/lista-de-endpoints/documentos/devoluciones
- returndate (filtro GET): «Permite filtrar por fecha de devolución» (sin ejemplo de rango)
- emissiondaterange en documentos: [unix_from,unix_to] (sync_service._utc_day_timestamp_bounds)
- users/returns: startdate/enddate como Integer (timestamps)
"""

from __future__ import annotations

import logging
import time
from datetime import date, datetime, time as dt_time, timezone
from typing import Any
from urllib.parse import urlencode

import requests

from backend.services.distribuidora.bsale_client import BASE_BSALE, BsaleClient
from backend.utils.bsale_field_parse import parse_optional_int

logger = logging.getLogger(__name__)

# Documentación Bsale Chile — campo returnDate y filtros de fecha en devoluciones.
_BSALE_RETURNS_DOC = (
    "https://apichile.bsalelab.com/lista-de-endpoints/documentos/devoluciones"
)
_BSALE_DOCS_DATE_DOC = "https://apichile.bsalelab.com/lista-de-endpoints/documentos"


def bootstrap_date_formats(d0: date, d1: date) -> dict[str, Any]:
    """
    Conversión explícita del rango bootstrap a formatos relevantes para Bsale.

    No modifica qué se envía al API; documenta Unix UTC usado por el sync actual.
    """
    start_dt = datetime.combine(d0, dt_time.min, tzinfo=timezone.utc)
    end_dt = datetime.combine(d1, dt_time(23, 59, 59), tzinfo=timezone.utc)
    from_ts = int(start_dt.timestamp())
    to_ts = int(end_dt.timestamp())
    returndate_param = f"[{from_ts},{to_ts}]"

    return {
        "date_from_iso": d0.isoformat(),
        "date_to_iso": d1.isoformat(),
        "date_from_utc": start_dt.isoformat(),
        "date_to_utc": end_dt.isoformat(),
        "date_from_unix": from_ts,
        "date_to_unix": to_ts,
        "returndate_param_used_by_sync": returndate_param,
        "api_returnDate_field_type": "Integer (Unix GMT timestamp)",
        "api_returndate_filter_doc": (
            "Filtro «returndate» sin ejemplo de formato en doc devoluciones; "
            "sync usa rango [unix,unix] al estilo emissiondaterange en documentos"
        ),
        "api_yyyy_mm_dd_accepted": (
            "No documentado para GET /returns.json; "
            "users/:id/returns.json usa startdate/enddate Integer"
        ),
        "doc_urls": {
            "returns": _BSALE_RETURNS_DOC,
            "documents_dates": _BSALE_DOCS_DATE_DOC,
        },
    }


def log_bootstrap_date_conversion(d0: date, d1: date, *, company_id: int, office_id: int) -> None:
    info = bootstrap_date_formats(d0, d1)
    logger.info(
        "[RETURNS_SYNC_DEBUG] Bootstrap fechas | company=%s office=%s | "
        "ISO %s → %s | Unix UTC %s → %s | returndate=%s",
        company_id,
        office_id,
        info["date_from_iso"],
        info["date_to_iso"],
        info["date_from_unix"],
        info["date_to_unix"],
        info["returndate_param_used_by_sync"],
    )
    logger.info(
        "[RETURNS_SYNC_DEBUG] API Bsale returnDate=%s | returndate filter: %s | "
        "YYYY-MM-DD en returns.json: %s | doc=%s",
        info["api_returnDate_field_type"],
        info["api_returndate_filter_doc"],
        info["api_yyyy_mm_dd_accepted"],
        info["doc_urls"]["returns"],
    )


def _mask_token(token: str) -> str:
    t = (token or "").strip()
    if not t:
        return "[vacío]"
    if len(t) <= 4:
        return "***"
    return f"***{t[-4:]}"


def _build_full_url(path: str, params: dict[str, Any]) -> str:
    base = path if path.startswith("http") else f"{BASE_BSALE}{path}"
    qs = urlencode(params, doseq=True)
    return f"{base}?{qs}" if qs else base


def _safe_headers(token: str) -> dict[str, str]:
    return {
        "access_token": _mask_token(token),
        "Content-Type": "(requests default)",
    }


def _item_ids(items: list[dict[str, Any]]) -> tuple[int | None, int | None]:
    if not items:
        return None, None
    first = parse_optional_int(items[0].get("id"))
    last = parse_optional_int(items[-1].get("id"))
    return first, last


def log_returns_request_pre(
    *,
    company_id: int,
    office_id: int,
    path: str,
    params: dict[str, Any],
    token: str,
    date_from_ts: int,
    date_to_ts: int,
) -> None:
    full_url = _build_full_url(path, params)
    from_iso = datetime.fromtimestamp(date_from_ts, tz=timezone.utc).isoformat()
    to_iso = datetime.fromtimestamp(date_to_ts, tz=timezone.utc).isoformat()

    logger.info("[RETURNS_SYNC_DEBUG] ── Request GET returns ──")
    logger.info("[RETURNS_SYNC_DEBUG] company=%s office=%s", company_id, office_id)
    logger.info("[RETURNS_SYNC_DEBUG] date_from_unix=%s date_to_unix=%s", date_from_ts, date_to_ts)
    logger.info(
        "[RETURNS_SYNC_DEBUG] date_from_utc=%s date_to_utc=%s",
        from_iso,
        to_iso,
    )
    logger.info(
        "[RETURNS_SYNC_DEBUG] limit=%s offset=%s",
        params.get("limit"),
        params.get("offset"),
    )
    logger.info("[RETURNS_SYNC_DEBUG] query_params=%s", params)
    logger.info("[RETURNS_SYNC_DEBUG] headers=%s", _safe_headers(token))
    logger.info("[RETURNS_SYNC_DEBUG] url=%s", full_url)


def log_returns_response_post(
    *,
    http_status: int,
    payload: dict[str, Any],
    items: list[dict[str, Any]],
    response_url: str | None = None,
) -> None:
    count = payload.get("count")
    limit = payload.get("limit")
    offset = payload.get("offset")
    first_id, last_id = _item_ids(items)

    logger.info("[RETURNS_SYNC_DEBUG] ── Response GET returns ──")
    if response_url:
        logger.info("[RETURNS_SYNC_DEBUG] response_url=%s", response_url)
    logger.info("[RETURNS_SYNC_DEBUG] HTTP_status=%s", http_status)
    logger.info(
        "[RETURNS_SYNC_DEBUG] count=%s limit=%s offset=%s items_received=%s",
        count,
        limit,
        offset,
        len(items),
    )
    logger.info(
        "[RETURNS_SYNC_DEBUG] first_id=%s last_id=%s",
        first_id if first_id is not None else "—",
        last_id if last_id is not None else "—",
    )

    if count == 0:
        logger.warning(
            "[RETURNS_SYNC_DEBUG] No se encontraron devoluciones para los parámetros enviados"
        )


def fetch_returns_page_json(
    client: BsaleClient,
    path: str,
    params: dict[str, Any],
    *,
    company_id: int,
    office_id: int,
    date_from_ts: int,
    date_to_ts: int,
) -> dict[str, Any]:
    """
    GET /returns.json con los mismos parámetros que el sync, más logs de diagnóstico.
  """
    log_returns_request_pre(
        company_id=company_id,
        office_id=office_id,
        path=path,
        params=params,
        token=client.access_token,
        date_from_ts=date_from_ts,
        date_to_ts=date_to_ts,
    )

    url = path if path.startswith("http") else f"{BASE_BSALE}{path}"
    transient = 0
    max_transient = 40

    while True:
        try:
            r = client.session.get(
                url,
                headers={"access_token": client.access_token},
                params=params,
                timeout=45,
            )
        except requests.RequestException as exc:
            transient += 1
            if transient >= max_transient:
                raise RuntimeError(f"Bsale red: {exc}") from exc
            logger.warning(
                "[RETURNS_SYNC_DEBUG] Bsale red (%s/%s): %s",
                transient,
                max_transient,
                exc,
            )
            time.sleep(3)
            continue

        items = []
        payload: dict[str, Any] = {}
        if r.status_code == 200 and r.content:
            try:
                payload = r.json()
                items = payload.get("items") or []
            except Exception:
                payload = {}

        log_returns_response_post(
            http_status=r.status_code,
            payload=payload,
            items=items,
            response_url=getattr(r.request, "url", None),
        )

        if r.status_code == 401:
            raise RuntimeError(
                "Bsale 401 Unauthorized — revisar BSALE_TOKEN o BSALE_TOKEN_SPA"
            )

        if r.status_code == 429:
            try:
                wait = int(r.json().get("retry_after", 60))
            except Exception:
                wait = 60
            logger.warning("[RETURNS_SYNC_DEBUG] Bsale 429 — esperando %s s", wait)
            time.sleep(wait)
            continue

        if r.status_code in (500, 502, 503, 504):
            transient += 1
            if transient >= max_transient:
                raise RuntimeError(f"Bsale HTTP {r.status_code} persistente")
            logger.warning("[RETURNS_SYNC_DEBUG] Bsale HTTP %s — reintento 3s", r.status_code)
            time.sleep(3)
            continue

        if not (200 <= r.status_code < 300):
            raise RuntimeError(f"Bsale HTTP {r.status_code}: {(r.text or '')[:500]}")

        return payload
