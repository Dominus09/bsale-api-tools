#!/usr/bin/env python3
"""
Extracción FULL RAW Bsale → Excel (análisis de estructura real).

- NO PostgreSQL, NO FastAPI, NO inserts.
- Listado por ``/documents.json`` (officeid + rango emisión), luego por cada id:
  ``/documents/{id}.json``, ``details``, ``references``, ``payments``,
  ``taxes`` o ``document_taxes``, ``sellers`` (incl. ``href``), ``attributes``.
- Sin PDF binario: solo URLs que vengan en JSON.
- Hojas ``raw_*`` con ``pandas.json_normalize``; resúmenes y tipos detectados.
- Diagnóstico HTTP: logs ``[FETCH START]/[FETCH OK]/[FETCH ERROR]``, timeout
  ``(connect=10s, read=30s)``, hoja ``request_stats``, anti-loop en paginación.

Salida:
  ``exports/bsale_full_debug.xlsx`` (o variante con timestamp si hay bloqueo
  por Excel/OneDrive; ver ``safe_excel_filename``).
"""

from __future__ import annotations

import hashlib
import json
import sys
import time
import warnings
from collections import defaultdict
from collections.abc import Iterator
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from backend.services.distribuidora.bsale_params import merge_bsale_office_query
from backend.utils.bsale_token_env import require_bsale_token

# ---------------------------------------------------------------------------
# Configuración (editar aquí)
# ---------------------------------------------------------------------------

OFFICE_ID = 1

DATE_FROM = "2026-04-01"
DATE_TO = "2026-04-02"

BASE_BSALE = "https://api.bsale.io/v1"
LIMIT_BSALE = 50
DETAILS_LIMIT = 50
# ``requests``: (connect_timeout_sec, read_timeout_sec)
TIMEOUT_CONNECT_SEC = 10
TIMEOUT_READ_SEC = 30
SLOW_REQUEST_THRESHOLD_SEC = 10.0
SLEEP_BETWEEN_CALLS_SEC = 0.25
RETRY_ATTEMPTS = 3
RETRY_SLEEP_SEC = 2.0

EXPORT_REL_PATH = "exports/bsale_full_debug.xlsx"

# Bsale La Quillotana / catálogo estándar: 9 = NOTA DE CRÉDITO (resta en totales).
_NOTA_CREDITO_TYPE_ID = 9


def _payload_fingerprint(obj: Any) -> str:
    try:
        raw = json.dumps(obj, sort_keys=True, ensure_ascii=False, default=str).encode("utf-8")
        return hashlib.sha256(raw).hexdigest()[:40]
    except Exception:
        return repr(obj)[:200]


class RequestDiag:
    """Métricas por HTTP GET (incluye reintentos) para hoja ``request_stats``."""

    def __init__(self) -> None:
        self.slow_requests: int = 0
        self._http_calls: list[tuple[str, float, bool]] = []

    def log_http(self, endpoint: str, duration_sec: float, is_error: bool) -> None:
        self._http_calls.append((endpoint, duration_sec, is_error))
        if duration_sec > SLOW_REQUEST_THRESHOLD_SEC:
            self.slow_requests += 1

    def to_request_stats_rows(self) -> list[dict[str, Any]]:
        dd: dict[str, dict[str, Any]] = defaultdict(
            lambda: {
                "cantidad_llamadas": 0,
                "total_sec": 0.0,
                "max_sec": 0.0,
                "errores": 0,
            }
        )
        for ep, dur, err in self._http_calls:
            a = dd[ep]
            a["cantidad_llamadas"] += 1
            a["total_sec"] += dur
            a["max_sec"] = max(a["max_sec"], dur)
            if err:
                a["errores"] += 1
        rows: list[dict[str, Any]] = []
        for ep in sorted(dd.keys()):
            a = dd[ep]
            n = int(a["cantidad_llamadas"])
            rows.append(
                {
                    "endpoint": ep,
                    "cantidad_llamadas": n,
                    "promedio_duracion_sec": round(a["total_sec"] / n, 4) if n else 0.0,
                    "max_duracion_sec": round(a["max_sec"], 4),
                    "errores": int(a["errores"]),
                }
            )
        return rows


def _unique_timestamped_excel_path(base: Path) -> Path:
    """``{stem}_YYYYMMDD_HHMMSS.xlsx`` en el mismo directorio que ``base``."""
    stem = base.stem
    suffix = base.suffix if base.suffix else ".xlsx"
    parent = base.parent
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    alt = parent / f"{stem}_{ts}{suffix}"
    n = 0
    while alt.exists() and n < 1000:
        n += 1
        ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        alt = parent / f"{stem}_{ts}{suffix}"
    return alt


def safe_excel_filename(target: Path) -> Path:
    """
    Evita ``PermissionError`` típico con Excel/OneDrive: si ``target`` existe,
    intenta borrarlo; si no se puede, devuelve
    ``{stem}_{YYYYMMDD_HHMMSS}.xlsx`` en el mismo directorio (único si hiciera
    falta).
    """
    target = target.expanduser().resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    if not target.exists():
        return target
    try:
        target.unlink()
        return target
    except OSError:
        return _unique_timestamped_excel_path(target)


def _die(msg: str, code: int = 1) -> None:
    print(f"[export_bsale_documents_test] ERROR: {msg}", file=sys.stderr)
    raise SystemExit(code)


def _token() -> str:
    return require_bsale_token(label="export_bsale_documents_test")


def _parse_date(s: str, label: str) -> date:
    try:
        return datetime.strptime(s.strip(), "%Y-%m-%d").date()
    except ValueError as e:
        raise SystemExit(
            f"[export_bsale_documents_test] {label} inválida (YYYY-MM-DD): {s!r}"
        ) from e


def _utc_day_epoch_bounds(d: date) -> tuple[int, int]:
    start = datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=timezone.utc)
    end_ts = int((start + timedelta(days=1)).timestamp()) - 1
    return int(start.timestamp()), end_ts


def _safe_int(v: Any) -> int | None:
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _safe_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _iter_dates(d0: date, d1: date) -> Iterator[date]:
    d = d0
    while d <= d1:
        yield d
        d += timedelta(days=1)


def _document_type_blob(doc: dict[str, Any]) -> dict[str, Any]:
    dt = doc.get("document_type") or doc.get("documentType")
    return dt if isinstance(dt, dict) else {}


def _document_type_id(doc: dict[str, Any]) -> int | None:
    return _safe_int(_document_type_blob(doc).get("id"))


def _is_nota_credito(doc: dict[str, Any]) -> bool:
    """Nota de crédito = ``document_type_id`` 9 (definición explícita Bsale)."""
    tid = _document_type_id(doc)
    return tid == _NOTA_CREDITO_TYPE_ID


def _adjusted_financial_field(doc: dict[str, Any], field: str) -> float | None:
    """
    Resúmenes financieros: para tipo 9 (NC), ``totalAmount``, ``netAmount`` y
    ``taxAmount`` se multiplican por -1 para que resten del total final.
    Resto de tipos: valor crudo.
    """
    v = _safe_float(doc.get(field))
    if v is None:
        return None
    if _is_nota_credito(doc):
        return v * -1.0
    return v


def _get_json(
    session: requests.Session,
    token: str,
    path: str,
    params: dict[str, Any] | None = None,
    *,
    allow_404: bool = False,
    document_id: int | str | None = None,
    endpoint: str = "unknown",
    diag: RequestDiag | None = None,
) -> dict[str, Any] | None:
    url = path if path.startswith("http") else f"{BASE_BSALE}{path}"
    params = params or {}
    timeout = (TIMEOUT_CONNECT_SEC, TIMEOUT_READ_SEC)
    doc_label = document_id if document_id is not None else "list"

    last_err: str | None = None
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        print(
            f"[FETCH START]\ndocument_id={doc_label}\nendpoint={endpoint}",
            flush=True,
        )
        t0 = time.perf_counter()
        r: requests.Response | None = None
        try:
            r = session.get(
                url,
                headers={"access_token": token},
                params=params,
                timeout=timeout,
            )
        except requests.RequestException as e:
            elapsed = time.perf_counter() - t0
            last_err = f"red: {type(e).__name__}: {e}"
            if diag is not None:
                diag.log_http(endpoint, elapsed, is_error=True)
            print(
                f"[FETCH ERROR]\ndocument_id={doc_label}\nendpoint={endpoint}\nerror={last_err}",
                flush=True,
            )
            if attempt >= RETRY_ATTEMPTS:
                raise RuntimeError(last_err) from e
            time.sleep(RETRY_SLEEP_SEC)
            continue

        elapsed = time.perf_counter() - t0

        if r.status_code == 401:
            if diag is not None:
                diag.log_http(endpoint, elapsed, is_error=True)
            print(
                f"[FETCH ERROR]\ndocument_id={doc_label}\nendpoint={endpoint}\nerror=http_401",
                flush=True,
            )
            _die("Bsale 401 Unauthorized: token inválido/expirado.")

        if r.status_code == 404 and allow_404:
            if diag is not None:
                diag.log_http(endpoint, elapsed, is_error=False)
            print(
                f"[FETCH OK]\ndocument_id={doc_label}\nendpoint={endpoint}\n"
                f"duration_seconds={elapsed:.3f}\nstatus=http_404_allowed",
                flush=True,
            )
            return None

        if r.status_code == 429:
            if diag is not None:
                diag.log_http(endpoint, elapsed, is_error=True)
            print(
                f"[FETCH ERROR]\ndocument_id={doc_label}\nendpoint={endpoint}\nerror=http_429",
                flush=True,
            )
            wait = 60
            try:
                wait = int(r.json().get("retry_after", 60))
            except Exception:
                pass
            time.sleep(max(1, min(300, wait)))
            if attempt >= RETRY_ATTEMPTS:
                last_err = f"rate_limit 429 tras espera {wait}s"
                break
            continue

        if r.status_code in (500, 502, 503, 504):
            last_err = f"http_{r.status_code}"
            if diag is not None:
                diag.log_http(endpoint, elapsed, is_error=True)
            print(
                f"[FETCH ERROR]\ndocument_id={doc_label}\nendpoint={endpoint}\nerror={last_err}",
                flush=True,
            )
            if attempt >= RETRY_ATTEMPTS:
                break
            time.sleep(RETRY_SLEEP_SEC)
            continue

        if r.status_code == 404:
            last_err = "http_404"
            if diag is not None:
                diag.log_http(endpoint, elapsed, is_error=True)
            print(
                f"[FETCH ERROR]\ndocument_id={doc_label}\nendpoint={endpoint}\nerror={last_err}",
                flush=True,
            )
            if attempt >= RETRY_ATTEMPTS:
                break
            time.sleep(RETRY_SLEEP_SEC)
            continue

        if not (200 <= r.status_code < 300):
            last_err = f"http_{r.status_code}: {(r.text or '')[:300]}"
            if diag is not None:
                diag.log_http(endpoint, elapsed, is_error=True)
            print(
                f"[FETCH ERROR]\ndocument_id={doc_label}\nendpoint={endpoint}\nerror={last_err}",
                flush=True,
            )
            if attempt >= RETRY_ATTEMPTS:
                break
            time.sleep(RETRY_SLEEP_SEC)
            continue

        try:
            out = r.json()
        except ValueError:
            last_err = "respuesta no JSON"
            if diag is not None:
                diag.log_http(endpoint, elapsed, is_error=True)
            print(
                f"[FETCH ERROR]\ndocument_id={doc_label}\nendpoint={endpoint}\nerror={last_err}",
                flush=True,
            )
            if attempt >= RETRY_ATTEMPTS:
                break
            time.sleep(RETRY_SLEEP_SEC)
            continue

        if diag is not None:
            diag.log_http(endpoint, elapsed, is_error=False)
        print(
            f"[FETCH OK]\ndocument_id={doc_label}\nendpoint={endpoint}\n"
            f"duration_seconds={elapsed:.3f}",
            flush=True,
        )
        return out

    raise RuntimeError(last_err or "error desconocido")


def _document_root(payload: dict[str, Any]) -> dict[str, Any]:
    d = payload.get("document")
    if isinstance(d, dict) and d.get("id") is not None:
        return d
    return payload


def _tag_rows(rows: list[dict[str, Any]], doc_id: int) -> None:
    for r in rows:
        r["_documentId"] = doc_id


def _items_from_container(resp: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(resp, dict):
        return []
    for key in (
        "items",
        "payments",
        "references",
        "attributes",
        "sellers",
        "taxes",
        "documentTaxes",
    ):
        v = resp.get(key)
        if isinstance(v, list):
            return [x for x in v if isinstance(x, dict)]
    return []


def _maybe_fetch_href(
    session: requests.Session,
    token: str,
    blob: Any,
    *,
    document_id: int | str | None,
    diag: RequestDiag | None,
) -> dict[str, Any] | None:
    if not isinstance(blob, dict):
        return None
    href = blob.get("href") or blob.get("url")
    if not isinstance(href, str) or not href.strip():
        return None
    return _get_json(
        session,
        token,
        href.strip(),
        None,
        allow_404=True,
        document_id=document_id,
        endpoint="sellers_href",
        diag=diag,
    )


def _collect_paginated_details(
    session: requests.Session,
    token: str,
    doc_id: int,
    out: list[dict[str, Any]],
    errors: list[str],
    diag: RequestDiag | None,
) -> int:
    offset = 0
    n = 0
    prev_fp: str | None = None
    prev_ids_key: tuple[Any, ...] | None = None
    while True:
        try:
            det = _get_json(
                session,
                token,
                f"/documents/{doc_id}/details.json",
                {"limit": DETAILS_LIMIT, "offset": offset},
                allow_404=True,
                document_id=doc_id,
                endpoint="details",
                diag=diag,
            )
        except Exception as e:
            errors.append(f"details doc={doc_id} off={offset}: {e}")
            break
        time.sleep(SLEEP_BETWEEN_CALLS_SEC)
        if det is None:
            break
        items = det.get("items") or []
        if not items:
            break

        fp = _payload_fingerprint(det)
        if fp == prev_fp and prev_fp is not None:
            msg = (
                f"details doc={doc_id}: payload repetido en paginación (offset={offset}), "
                "posible loop; se aborta."
            )
            errors.append(msg)
            warnings.warn(msg, RuntimeWarning, stacklevel=2)
            break
        prev_fp = fp

        ids_key = tuple(_safe_int(x.get("id")) for x in items if isinstance(x, dict))
        if ids_key and ids_key == prev_ids_key:
            msg = (
                f"details doc={doc_id}: mismos ids de ítems que página anterior "
                f"(offset={offset}); se aborta."
            )
            errors.append(msg)
            warnings.warn(msg, RuntimeWarning, stacklevel=2)
            break
        prev_ids_key = ids_key

        prev_offset = offset
        for it in items:
            if isinstance(it, dict):
                out.append({**it, "_documentId": doc_id})
                n += 1
        if len(items) < DETAILS_LIMIT:
            break
        next_offset = offset + len(items)
        if next_offset <= prev_offset:
            msg = f"details doc={doc_id}: offset no avanza ({prev_offset} -> {next_offset}); se aborta."
            errors.append(msg)
            warnings.warn(msg, RuntimeWarning, stacklevel=2)
            break
        offset = next_offset
    return n


def _collect_endpoint_items(
    session: requests.Session,
    token: str,
    doc_id: int,
    path: str,
    endpoint_label: str,
    out: list[dict[str, Any]],
    errors: list[str],
    diag: RequestDiag | None,
    *,
    allow_404: bool = True,
    resolve_href: bool = False,
) -> int:
    try:
        data = _get_json(
            session,
            token,
            path,
            None,
            allow_404=allow_404,
            document_id=doc_id,
            endpoint=endpoint_label,
            diag=diag,
        )
    except Exception as e:
        errors.append(f"{path} doc={doc_id}: {e}")
        return 0
    time.sleep(SLEEP_BETWEEN_CALLS_SEC)
    if data is None:
        return 0
    if resolve_href:
        fetched = _maybe_fetch_href(session, token, data, document_id=doc_id, diag=diag)
        if fetched is not None:
            data = fetched
        elif isinstance(data, dict) and isinstance(data.get("sellers"), dict):
            fetched2 = _maybe_fetch_href(
                session, token, data.get("sellers"), document_id=doc_id, diag=diag
            )
            if fetched2 is not None:
                data = fetched2
    items = _items_from_container(data)
    if not items and isinstance(data, dict) and data.get("id") is not None:
        # respuesta de objeto único
        items = [data]
    _tag_rows(items, doc_id)
    out.extend(items)
    return len(items)


def main() -> None:
    token = _token()
    d0 = _parse_date(DATE_FROM, "DATE_FROM")
    d1 = _parse_date(DATE_TO, "DATE_TO")
    if d1 < d0:
        _die("DATE_TO debe ser >= DATE_FROM")

    session = requests.Session()
    errors: list[str] = []
    diag = RequestDiag()

    documents: list[dict[str, Any]] = []
    details: list[dict[str, Any]] = []
    references: list[dict[str, Any]] = []
    payments: list[dict[str, Any]] = []
    document_taxes: list[dict[str, Any]] = []
    sellers: list[dict[str, Any]] = []
    attributes: list[dict[str, Any]] = []

    # --- Listado documentos (paginado por día) ---
    for day in _iter_dates(d0, d1):
        start_ts, end_ts = _utc_day_epoch_bounds(day)
        offset = 0
        prev_list_fp: str | None = None
        prev_list_ids: tuple[Any, ...] | None = None
        while True:
            params = merge_bsale_office_query(
                {
                    "limit": LIMIT_BSALE,
                    "offset": offset,
                    "emissiondaterange": f"[{start_ts},{end_ts}]",
                },
                OFFICE_ID,
            )
            try:
                data = _get_json(
                    session,
                    token,
                    "/documents.json",
                    params,
                    document_id="list",
                    endpoint="documents_list",
                    diag=diag,
                )
            except Exception as e:
                errors.append(f"documents.json day={day} offset={offset}: {e}")
                print(f"[export_bsale_documents_test] errores: {e}")
                break

            print(
                f"[export_bsale_documents_test] página descargada: day={day} offset={offset}"
            )
            time.sleep(SLEEP_BETWEEN_CALLS_SEC)

            items = data.get("items") or []
            if not items:
                break

            fp = _payload_fingerprint(data)
            if fp == prev_list_fp and prev_list_fp is not None:
                msg = (
                    f"documents.json day={day}: respuesta repetida (offset={offset}), "
                    "posible loop; se aborta el día."
                )
                errors.append(msg)
                warnings.warn(msg, RuntimeWarning, stacklevel=2)
                break
            prev_list_fp = fp

            ids_key = tuple(_safe_int(x.get("id")) for x in items if isinstance(x, dict))
            if ids_key and ids_key == prev_list_ids:
                msg = (
                    f"documents.json day={day}: mismos ids que página anterior "
                    f"(offset={offset}); se aborta el día."
                )
                errors.append(msg)
                warnings.warn(msg, RuntimeWarning, stacklevel=2)
                break
            prev_list_ids = ids_key

            prev_offset = offset
            for doc in items:
                if not isinstance(doc, dict):
                    continue
                oid = _safe_int((doc.get("office") or {}).get("id"))
                if oid != OFFICE_ID:
                    continue
                documents.append(doc)

            next_offset = offset + len(items)
            if next_offset <= prev_offset:
                msg = (
                    f"documents.json day={day}: offset no avanza "
                    f"({prev_offset} -> {next_offset}); se aborta."
                )
                errors.append(msg)
                warnings.warn(msg, RuntimeWarning, stacklevel=2)
                break
            offset = next_offset

    # --- Por documento: GET completo + subrecursos ---
    canonical_docs: list[dict[str, Any]] = []
    for doc in documents:
        doc_id = _safe_int(doc.get("id"))
        if doc_id is None:
            continue

        full_doc: dict[str, Any] = doc
        try:
            one = _get_json(
                session,
                token,
                f"/documents/{doc_id}.json",
                None,
                allow_404=True,
                document_id=doc_id,
                endpoint="document",
                diag=diag,
            )
            time.sleep(SLEEP_BETWEEN_CALLS_SEC)
            if isinstance(one, dict):
                full_doc = _document_root(one)
        except Exception as e:
            errors.append(f"/documents/{doc_id}.json: {e}")

        canonical_docs.append(full_doc)

        _collect_paginated_details(session, token, doc_id, details, errors, diag)
        _collect_endpoint_items(
            session,
            token,
            doc_id,
            f"/documents/{doc_id}/references.json",
            "references",
            references,
            errors,
            diag,
        )
        _collect_endpoint_items(
            session,
            token,
            doc_id,
            f"/documents/{doc_id}/payments.json",
            "payments",
            payments,
            errors,
            diag,
        )
        # Impuestos a nivel documento: probar rutas habituales en Bsale
        n_tax = _collect_endpoint_items(
            session,
            token,
            doc_id,
            f"/documents/{doc_id}/taxes.json",
            "taxes",
            document_taxes,
            errors,
            diag,
        )
        if n_tax == 0:
            _collect_endpoint_items(
                session,
                token,
                doc_id,
                f"/documents/{doc_id}/document_taxes.json",
                "document_taxes",
                document_taxes,
                errors,
                diag,
            )

        _collect_endpoint_items(
            session,
            token,
            doc_id,
            f"/documents/{doc_id}/sellers.json",
            "sellers",
            sellers,
            errors,
            diag,
            resolve_href=True,
        )
        _collect_endpoint_items(
            session,
            token,
            doc_id,
            f"/documents/{doc_id}/attributes.json",
            "attributes",
            attributes,
            errors,
            diag,
        )

    # --- Raw flatten (máxima visibilidad) ---
    df_raw_docs = (
        pd.json_normalize(canonical_docs, sep=".") if canonical_docs else pd.DataFrame()
    )
    df_raw_details = pd.json_normalize(details, sep=".") if details else pd.DataFrame()
    df_raw_refs = (
        pd.json_normalize(references, sep=".") if references else pd.DataFrame()
    )
    df_raw_payments = (
        pd.json_normalize(payments, sep=".") if payments else pd.DataFrame()
    )
    df_raw_taxes = (
        pd.json_normalize(document_taxes, sep=".") if document_taxes else pd.DataFrame()
    )
    df_raw_sellers = (
        pd.json_normalize(sellers, sep=".") if sellers else pd.DataFrame()
    )
    df_raw_attrs = (
        pd.json_normalize(attributes, sep=".") if attributes else pd.DataFrame()
    )

    # --- resumen ---
    docs_sin_detalle = 0
    with_detail: set[int] = set()
    for r in details:
        did = _safe_int(r.get("_documentId"))
        if did is not None:
            with_detail.add(did)
    for d in canonical_docs:
        did = _safe_int(d.get("id"))
        if did is None:
            continue
        if did not in with_detail:
            docs_sin_detalle += 1

    ventas_positivas = sum(1 for d in canonical_docs if not _is_nota_credito(d))
    notas_credito = sum(1 for d in canonical_docs if _is_nota_credito(d))
    total_neto_ajustado = 0.0
    for d in canonical_docs:
        na = _adjusted_financial_field(d, "netAmount")
        if na is not None:
            total_neto_ajustado += na

    df_resumen = pd.DataFrame(
        [
            {"metrica": "rango_fecha_desde", "valor": d0.isoformat()},
            {"metrica": "rango_fecha_hasta", "valor": d1.isoformat()},
            {"metrica": "office_id", "valor": OFFICE_ID},
            {"metrica": "cantidad_documentos", "valor": len(canonical_docs)},
            {"metrica": "cantidad_detalles", "valor": len(details)},
            {"metrica": "cantidad_pagos", "valor": len(payments)},
            {"metrica": "cantidad_referencias", "valor": len(references)},
            {"metrica": "cantidad_sellers", "valor": len(sellers)},
            {"metrica": "cantidad_taxes", "valor": len(document_taxes)},
            {"metrica": "cantidad_attributes", "valor": len(attributes)},
            {"metrica": "documentos_sin_detalle", "valor": docs_sin_detalle},
            {"metrica": "errores_capturados", "valor": len(errors)},
            {"metrica": "slow_requests", "valor": diag.slow_requests},
            {
                "metrica": "ventas_positivas",
                "valor": ventas_positivas,
            },
            {"metrica": "notas_de_credito", "valor": notas_credito},
            {
                "metrica": "total_neto_ajustado",
                "valor": round(total_neto_ajustado, 2),
            },
        ]
    )

    # --- document_types_detected ---
    by_type: dict[int | None, dict[str, Any]] = {}
    for d in canonical_docs:
        tid = _document_type_id(d)
        folio = d.get("number")
        if tid not in by_type:
            by_type[tid] = {
                "document_type_id": tid,
                "cantidad": 0,
                "suma_totalAmount": 0.0,
                "suma_ajustada": 0.0,
                "ejemplo_de_folio": folio,
            }
        b = by_type[tid]
        b["cantidad"] += 1
        ta = _safe_float(d.get("totalAmount"))
        if ta is not None:
            b["suma_totalAmount"] += ta
        adj = _adjusted_financial_field(d, "totalAmount")
        if adj is not None:
            b["suma_ajustada"] += adj
        if folio is not None and b.get("ejemplo_de_folio") is None:
            b["ejemplo_de_folio"] = folio

    type_rows = sorted(
        by_type.values(),
        key=lambda x: (x["document_type_id"] is None, x["document_type_id"] or -1),
    )
    for row in type_rows:
        tid = row.get("document_type_id")
        row["clasificacion_bsale"] = (
            "NOTA_CREDITO" if tid == _NOTA_CREDITO_TYPE_ID else ""
        )
    _dt_cols = [
        "document_type_id",
        "clasificacion_bsale",
        "cantidad",
        "suma_totalAmount",
        "suma_ajustada",
        "ejemplo_de_folio",
    ]
    df_types = (
        pd.DataFrame(type_rows, columns=_dt_cols)
        if not type_rows
        else pd.DataFrame(type_rows)[_dt_cols]
    )

    # --- resumen financiero por tipo ---
    fin_rows: list[dict[str, Any]] = []
    total_bruto = 0.0
    total_neto = 0.0
    total_tax = 0.0

    def _fin_row_for_type(tid: int | None, b: dict[str, Any]) -> dict[str, Any]:
        nonlocal total_bruto, total_neto, total_tax
        ta_raw = float(b["suma_totalAmount"])
        ta_adj = float(b["suma_ajustada"])
        net_sum_raw = 0.0
        net_sum_adj = 0.0
        tax_sum_raw = 0.0
        tax_sum_adj = 0.0
        for d in canonical_docs:
            if _document_type_id(d) != tid:
                continue
            nr = _safe_float(d.get("netAmount"))
            if nr is not None:
                net_sum_raw += nr
            na = _adjusted_financial_field(d, "netAmount")
            if na is not None:
                net_sum_adj += na
            tr = _safe_float(d.get("taxAmount"))
            if tr is not None:
                tax_sum_raw += tr
            tadj = _adjusted_financial_field(d, "taxAmount")
            if tadj is not None:
                tax_sum_adj += tadj
        total_bruto += ta_adj
        total_neto += net_sum_adj
        total_tax += tax_sum_adj
        return {
            "document_type_id": tid,
            "cantidad_documentos": int(b["cantidad"]),
            "suma_totalAmount": round(ta_raw, 2),
            "suma_totalAmount_ajustada_NC_resta": round(ta_adj, 2),
            "suma_netAmount": round(net_sum_raw, 2),
            "suma_netAmount_ajustada_NC_resta": round(net_sum_adj, 2),
            "suma_taxAmount": round(tax_sum_raw, 2),
            "suma_taxAmount_ajustada_NC_resta": round(tax_sum_adj, 2),
        }

    numeric_ids = sorted(
        [k for k in by_type if k is not None],
        key=lambda x: int(x),  # type: ignore[arg-type, return-value]
    )
    for tid in numeric_ids:
        fin_rows.append(_fin_row_for_type(tid, by_type[tid]))
    if None in by_type:
        fin_rows.append(_fin_row_for_type(None, by_type[None]))

    fin_rows.append(
        {
            "document_type_id": "__TOTAL__",
            "cantidad_documentos": len(canonical_docs),
            "suma_totalAmount": None,
            "suma_totalAmount_ajustada_NC_resta": round(total_bruto, 2),
            "suma_netAmount": None,
            "suma_netAmount_ajustada_NC_resta": round(total_neto, 2),
            "suma_taxAmount": None,
            "suma_taxAmount_ajustada_NC_resta": round(total_tax, 2),
        }
    )
    df_fin = pd.DataFrame(fin_rows)

    stats_rows = diag.to_request_stats_rows()
    df_req_stats = pd.DataFrame(stats_rows)
    if df_req_stats.empty:
        df_req_stats = pd.DataFrame(
            columns=[
                "endpoint",
                "cantidad_llamadas",
                "promedio_duracion_sec",
                "max_duracion_sec",
                "errores",
            ]
        )

    # --- Export ---
    repo_root = Path(__file__).resolve().parents[2]
    base_export = repo_root / EXPORT_REL_PATH

    def _write_excel_workbook(writer: pd.ExcelWriter) -> None:
        df_raw_docs.to_excel(writer, sheet_name="raw_documents", index=False)
        df_raw_details.to_excel(writer, sheet_name="raw_details", index=False)
        df_raw_refs.to_excel(writer, sheet_name="raw_references", index=False)
        df_raw_payments.to_excel(writer, sheet_name="raw_payments", index=False)
        df_raw_taxes.to_excel(writer, sheet_name="raw_document_taxes", index=False)
        df_raw_sellers.to_excel(writer, sheet_name="raw_sellers", index=False)
        df_raw_attrs.to_excel(writer, sheet_name="raw_attributes", index=False)
        df_resumen.to_excel(writer, sheet_name="resumen", index=False)
        df_fin.to_excel(writer, sheet_name="resumen_financiero", index=False)
        df_types.to_excel(writer, sheet_name="document_types_detected", index=False)
        df_req_stats.to_excel(writer, sheet_name="request_stats", index=False)

    out_path = safe_excel_filename(base_export)
    try:
        with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
            _write_excel_workbook(writer)
    except PermissionError:
        out_path = _unique_timestamped_excel_path(base_export.resolve())
        print(
            f"[export_bsale_documents_test] aviso: PermissionError al escribir; "
            f"usando ruta alternativa.",
            flush=True,
        )
        with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
            _write_excel_workbook(writer)

    filas_rel = (
        len(details)
        + len(references)
        + len(payments)
        + len(document_taxes)
        + len(sellers)
        + len(attributes)
    )
    print(f"[export_bsale_documents_test] cantidad documentos: {len(canonical_docs)}")
    print(f"[export_bsale_documents_test] cantidad entidades relacionadas (filas): {filas_rel}")
    print(f"[export_bsale_documents_test] errores: {len(errors)}")
    print(
        f"[export_bsale_documents_test] slow_requests (>{SLOW_REQUEST_THRESHOLD_SEC:g}s): "
        f"{diag.slow_requests}"
    )
    print(
        f"[export_bsale_documents_test] ruta final generada: {out_path}",
        flush=True,
    )


if __name__ == "__main__":
    main()
