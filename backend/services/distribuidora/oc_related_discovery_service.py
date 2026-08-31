"""
Motor canónico OC → boleta/factura vía Bsale ``relateddetailid``.

Usado por ``live_sync_related`` (incremental) y ``catchup_oc_invoice_relations`` (rango histórico).
Solo tipos confirmados 1/6; sin probable matching.
"""

from __future__ import annotations

import logging
import os
import time
from datetime import date, datetime, timezone
from typing import Any

from psycopg2.extensions import connection as PgConnection

from backend.services.distribuidora.bsale_client import BsaleClient
from backend.services.distribuidora.bsale_params import merge_bsale_office_query
from backend.utils.distribuidora_oc_sql import OC_PURCHASE_NOT_INVOICED_BY_RELATED_SQL

logger = logging.getLogger(__name__)

COMPANY_ID = 3
OFFICE_ID = 1
DOC_TYPE_OC = 33
RELATED_DETAIL_PAGE_LIMIT = 50

CONFIRMED_INVOICE_TYPES = frozenset({1, 6})
CATCHUP_INVOICE_TYPES = CONFIRMED_INVOICE_TYPES

DEFAULT_RELATED_SYNC_LOOKBACK_DAYS = 14
DEFAULT_RELATED_SYNC_MAX_RUNTIME_SEC = 240
DEFAULT_MIN_INTERVAL_SEC = 0.75
DEFAULT_INTERVAL_JITTER_SEC = 0.25
DEFAULT_MAX_429_RETRIES = 5
DEFAULT_PENDING_ROTATION_SLOT_SEC = 300

# Compatibilidad con nombres históricos del catchup.
CATCHUP_DEFAULT_MIN_INTERVAL_SEC = DEFAULT_MIN_INTERVAL_SEC
CATCHUP_DEFAULT_INTERVAL_JITTER_SEC = DEFAULT_INTERVAL_JITTER_SEC
CATCHUP_MAX_429_RETRIES = DEFAULT_MAX_429_RETRIES

DISCOVERY_MODE_FULL = "full"
DISCOVERY_MODE_FAST_CONFIRM = "fast_confirm"

STOP_REASON_COMPLETED = "completed"
STOP_REASON_RUNTIME_BUDGET = "runtime_budget_exhausted"
STOP_REASON_SKIPPED_ALREADY_RUNNING = "SKIPPED_ALREADY_RUNNING"


def resolve_related_sync_max_runtime_sec(live_mode: bool) -> int | None:
    """Presupuesto de ejecución solo para live incremental (segundos)."""
    if not live_mode:
        return None
    raw = os.getenv("RELATED_SYNC_MAX_RUNTIME_SEC", "").strip()
    if raw:
        try:
            return max(30, int(raw))
        except ValueError:
            pass
    return DEFAULT_RELATED_SYNC_MAX_RUNTIME_SEC


class OcRelatedApiError(Exception):
    """Error Bsale (incl. 429 agotado) — NO clasificar como sin relación."""

    def __init__(self, message: str, *, rate_limited: bool = False) -> None:
        super().__init__(message)
        self.rate_limited = rate_limited


CatchupApiError = OcRelatedApiError


def _sync_helpers():
    from backend.services.distribuidora import sync_related_service as srs

    return srs


def _utc_day_emission_bounds(d: date) -> tuple[datetime, datetime]:
    from datetime import timedelta

    start = datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=timezone.utc)
    end_excl = start + timedelta(days=1)
    return start, end_excl


def _bsale_token() -> str:
    return (os.getenv("BSALE_TOKEN") or "").strip() or (os.getenv("BSALE_TOKEN_SPA") or "").strip()


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def resolve_related_sync_lookback_days(lookback_days: int | None = None) -> int:
    """
    Días de emisión hacia atrás para candidatas OC en sync incremental.

    Prioridad: argumento explícito → ``RELATED_SYNC_LOOKBACK_DAYS`` (14) →
    ``LIVE_SYNC_RELATED_WINDOW_DAYS`` (legacy) → ``DISTRIBUIDORA_RELATED_LOOKBACK_DAYS`` (10).
    """
    if lookback_days is not None:
        return max(1, int(lookback_days))
    for env_name, default in (
        ("RELATED_SYNC_LOOKBACK_DAYS", DEFAULT_RELATED_SYNC_LOOKBACK_DAYS),
        ("LIVE_SYNC_RELATED_WINDOW_DAYS", DEFAULT_RELATED_SYNC_LOOKBACK_DAYS),
        ("DISTRIBUIDORA_RELATED_LOOKBACK_DAYS", 10),
    ):
        raw = os.getenv(env_name, "").strip()
        if raw:
            try:
                return max(1, int(raw))
            except ValueError:
                pass
    return DEFAULT_RELATED_SYNC_LOOKBACK_DAYS


def compute_pending_rotation_offset(
    total_pending: int,
    pending_limit: int,
    *,
    slot_seconds: int | None = None,
    now_ts: float | None = None,
) -> int:
    """
    Offset rotativo para el bucket de OCs pendientes (evita starvation).

    Rota cada ``slot_seconds`` (default 300 s ≈ ciclo cron 5 min).
    """
    if total_pending <= pending_limit or pending_limit <= 0:
        return 0
    slot = slot_seconds if slot_seconds is not None else _env_int(
        "RELATED_SYNC_PENDING_ROTATION_SEC",
        DEFAULT_PENDING_ROTATION_SLOT_SEC,
    )
    pages = (total_pending + pending_limit - 1) // pending_limit
    ts = now_ts if now_ts is not None else time.time()
    page = int(ts // max(1, slot)) % pages
    return page * pending_limit


def create_bsale_client_for_related_discovery(
    token: str | None = None,
    *,
    rate_stats: dict[str, Any] | None = None,
) -> BsaleClient:
    """Cliente Bsale con throttle, Retry-After y backoff del catchup probado."""
    tok = (token or _bsale_token()).strip()
    return BsaleClient(
        tok,
        min_interval_sec=float(
            os.getenv("RELATED_SYNC_MIN_INTERVAL_SEC", str(DEFAULT_MIN_INTERVAL_SEC))
        ),
        min_interval_jitter_sec=float(
            os.getenv(
                "RELATED_SYNC_INTERVAL_JITTER_SEC",
                str(DEFAULT_INTERVAL_JITTER_SEC),
            )
        ),
        max_429_retries=_env_int("RELATED_SYNC_MAX_429_RETRIES", DEFAULT_MAX_429_RETRIES),
        rate_stats=rate_stats,
    )


def _generation_date_iso(blob: dict[str, Any]) -> str | None:
    raw = blob.get("generationDate") or blob.get("generation_date")
    if raw is None:
        return None
    try:
        ts = int(raw)
    except (TypeError, ValueError):
        return None
    if ts <= 0:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def _related_number(blob: dict[str, Any]) -> int | None:
    n = blob.get("number")
    if n is None and isinstance(blob.get("document"), dict):
        n = blob["document"].get("number")
    try:
        return int(n) if n is not None else None
    except (TypeError, ValueError):
        return None


def _is_rate_limit_message(msg: str) -> bool:
    m = msg.lower()
    return "429" in m or "rate limit" in m


def _oc_number_and_state(cur, document_id: int) -> tuple[int | None, int]:
    cur.execute(
        "SELECT number, COALESCE(state, 0) FROM distribuidora.documents WHERE document_id = %s",
        (document_id,),
    )
    row = cur.fetchone()
    if not row:
        return None, 0
    number = int(row[0]) if row[0] is not None else None
    return number, int(row[1] or 0)


def fetch_oc_document_ids_for_range(
    cur,
    *,
    start_date: date,
    end_date: date,
    company_id: int = COMPANY_ID,
    office_id: int = OFFICE_ID,
) -> list[int]:
    """OC tipo 33 emitidas en [start_date, end_date] inclusive (días UTC)."""
    if start_date > end_date:
        start_date, end_date = end_date, start_date
    day_start, _ = _utc_day_emission_bounds(start_date)
    _, day_end_excl = _utc_day_emission_bounds(end_date)
    cur.execute(
        """
        SELECT d.document_id
        FROM distribuidora.documents d
        WHERE d.document_type_id = %s
          AND d.company_id = %s
          AND d.office_id = %s
          AND d.emission_date IS NOT NULL
          AND d.emission_date >= %s
          AND d.emission_date < %s
        ORDER BY d.emission_date ASC, d.document_id ASC
        """,
        (DOC_TYPE_OC, company_id, office_id, day_start, day_end_excl),
    )
    return [int(r[0]) for r in cur.fetchall()]


def count_pending_ocs_in_lookback(
    cur,
    *,
    lookback_days: int,
    company_id: int = COMPANY_ID,
    office_id: int = OFFICE_ID,
) -> int:
    lb = max(1, lookback_days)
    cur.execute(
        f"""
        SELECT COUNT(*)::bigint
        FROM distribuidora.documents d
        WHERE d.document_type_id = %s
          AND d.company_id = %s
          AND d.office_id = %s
          AND d.emission_date >= (NOW() AT TIME ZONE 'UTC' - (%s * interval '1 day'))
          AND {OC_PURCHASE_NOT_INVOICED_BY_RELATED_SQL}
        """,
        (DOC_TYPE_OC, company_id, office_id, lb),
    )
    row = cur.fetchone()
    return int(row[0] or 0) if row else 0


def fetch_pending_oc_ids_for_incremental(
    cur,
    *,
    lookback_days: int,
    pending_limit: int,
    pending_offset: int = 0,
    company_id: int = COMPANY_ID,
    office_id: int = OFFICE_ID,
) -> list[int]:
    """OC pendientes (sin arista 1/6) dentro del lookback, con offset rotativo."""
    lb = max(1, lookback_days)
    offset = max(0, int(pending_offset))
    cur.execute(
        f"""
        SELECT d.document_id
        FROM distribuidora.documents d
        WHERE d.document_type_id = %s
          AND d.company_id = %s
          AND d.office_id = %s
          AND d.emission_date >= (NOW() AT TIME ZONE 'UTC' - (%s * interval '1 day'))
          AND {OC_PURCHASE_NOT_INVOICED_BY_RELATED_SQL}
        ORDER BY d.emission_date ASC NULLS LAST, d.document_id ASC
        OFFSET %s
        LIMIT %s
        """,
        (DOC_TYPE_OC, company_id, office_id, lb, offset, max(1, pending_limit)),
    )
    return [int(r[0]) for r in cur.fetchall()]


def fetch_recent_oc_ids_for_refresh(
    cur,
    *,
    lookback_days: int,
    refresh_limit: int,
    company_id: int = COMPANY_ID,
    office_id: int = OFFICE_ID,
) -> list[int]:
    lb = max(1, lookback_days)
    cur.execute(
        """
        SELECT d.document_id
        FROM distribuidora.documents d
        WHERE d.document_type_id = %s
          AND d.company_id = %s
          AND d.office_id = %s
          AND d.emission_date >= (NOW() AT TIME ZONE 'UTC' - (%s * interval '1 day'))
        ORDER BY d.document_id DESC
        LIMIT %s
        """,
        (DOC_TYPE_OC, company_id, office_id, lb, max(1, refresh_limit)),
    )
    return [int(r[0]) for r in cur.fetchall()]


def merge_oc_candidate_ids(
    pending_ids: list[int],
    refresh_ids: list[int],
) -> list[int]:
    seen: set[int] = set()
    merged: list[int] = []
    for doc_id in pending_ids:
        if doc_id not in seen:
            seen.add(doc_id)
            merged.append(doc_id)
    for doc_id in refresh_ids:
        if doc_id not in seen:
            seen.add(doc_id)
            merged.append(doc_id)
    return merged


def emission_date_bounds_for_document_ids(cur, document_ids: list[int]) -> tuple[str | None, str | None]:
    if not document_ids:
        return None, None
    cur.execute(
        """
        SELECT MIN(emission_date), MAX(emission_date)
        FROM distribuidora.documents
        WHERE document_id = ANY(%s::bigint[])
        """,
        (document_ids,),
    )
    row = cur.fetchone()
    if not row:
        return None, None

    def _iso(v: Any) -> str | None:
        if v is None:
            return None
        if isinstance(v, datetime):
            dt = v if v.tzinfo else v.replace(tzinfo=timezone.utc)
            return dt.isoformat()
        return str(v)

    return _iso(row[0]), _iso(row[1])


def load_existing_invoice_relations_for_oc(
    cur,
    oc_document_id: int,
) -> tuple[set[tuple[int, int]], set[tuple[int, int]]]:
    """
    Returns:
        existing_pairs: (detail_id, related_document_id)
        confirmed_doc_keys: (related_document_id, related_type) unique at OC level
    """
    cur.execute(
        """
        SELECT dr.detail_id, dr.related_document_id, dr.related_document_type
        FROM distribuidora.document_details dd
        INNER JOIN distribuidora.document_related dr ON dr.detail_id = dd.detail_id
        WHERE dd.document_id = %s
          AND dr.related_document_type IN (1, 6)
        """,
        (oc_document_id,),
    )
    pairs: set[tuple[int, int]] = set()
    doc_keys: set[tuple[int, int]] = set()
    for detail_id, related_id, related_type in cur.fetchall():
        pairs.add((int(detail_id), int(related_id)))
        doc_keys.add((int(related_id), int(related_type)))
    return pairs, doc_keys


def fetch_relateddetailid_items(
    client: BsaleClient,
    detail_id: int,
    *,
    office_id: int,
    throttle: float,
    log_ctx: str,
) -> tuple[list[dict[str, Any]], int]:
    """Paginación ``relateddetailid`` secuencial; propaga OcRelatedApiError en fallo HTTP."""
    merged: list[dict[str, Any]] = []
    api_calls = 0
    offset = 0
    while True:
        try:
            data = client.get(
                "/documents.json",
                merge_bsale_office_query(
                    {
                        "relateddetailid": detail_id,
                        "limit": RELATED_DETAIL_PAGE_LIMIT,
                        "offset": offset,
                    },
                    office_id,
                    context="relateddetailid",
                ),
            )
        except RuntimeError as exc:
            msg = str(exc)
            raise OcRelatedApiError(msg, rate_limited=_is_rate_limit_message(msg)) from exc
        except Exception as exc:
            raise OcRelatedApiError(
                str(exc), rate_limited=_is_rate_limit_message(str(exc))
            ) from exc

        api_calls += 1
        items = data.get("items") or []
        if not items:
            break
        for it in items:
            if isinstance(it, dict):
                merged.append(it)
        if len(items) < RELATED_DETAIL_PAGE_LIMIT:
            break
        offset += len(items)
        if throttle > 0:
            time.sleep(throttle)
    return merged, api_calls


def discover_confirmed_related_documents_for_oc(
    client: BsaleClient,
    cur,
    oc_document_id: int,
    *,
    office_id: int = OFFICE_ID,
    throttle: float = 0.0,
    stats: dict[str, Any] | None = None,
    discovery_mode: str = DISCOVERY_MODE_FULL,
) -> dict[str, Any]:
    """Alias canónico de ``discover_invoice_edges_for_oc``."""
    return discover_invoice_edges_for_oc(
        client,
        cur,
        oc_document_id,
        office_id=office_id,
        throttle=throttle,
        stats=stats,
        discovery_mode=discovery_mode,
    )


def discover_invoice_edges_for_oc(
    client: BsaleClient,
    cur,
    oc_document_id: int,
    *,
    office_id: int = OFFICE_ID,
    throttle: float = 0.0,
    stats: dict[str, Any] | None = None,
    discovery_mode: str = DISCOVERY_MODE_FULL,
) -> dict[str, Any]:
    """
    Descubre aristas detail→(1|6) desde Bsale sin persistir.

    ``discovery_mode``:
      - ``full``: recorre todos los detalles (catchup / mantenimiento).
      - ``fast_confirm``: corta al primer tipo 1/6 confirmado (live incremental).
    """
    fast = discovery_mode == DISCOVERY_MODE_FAST_CONFIRM
    srs = _sync_helpers()
    oc_number, oc_state = _oc_number_and_state(cur, oc_document_id)
    log_ctx = srs._related_log_ctx(oc_number, oc_document_id)
    source_id, _folio = srs._bsale_source_id_from_pg(cur, oc_document_id)
    cancelled = int(oc_state or 0) != 0

    existing_pairs, confirmed_doc_keys = load_existing_invoice_relations_for_oc(
        cur, oc_document_id
    )
    confirmed_before = bool(confirmed_doc_keys)

    if cancelled:
        return {
            "oc_document_id": oc_document_id,
            "oc_number": oc_number,
            "oc_state": int(oc_state or 0),
            "detail_ids_consulted": [],
            "confirmed_before": confirmed_before,
            "would_confirm": False,
            "status": "cancelled",
            "api_error": None,
            "rate_limited": False,
            "edges": [],
            "unique_related_documents": [],
            "api_calls": 0,
            "existing_pairs_count": len(existing_pairs),
            "discovery_mode": discovery_mode,
            "details_total": 0,
            "details_queried": 0,
            "details_skipped_after_confirmation": 0,
            "early_exit": False,
            "fast_confirmed": False,
        }

    if fast and confirmed_before:
        return {
            "oc_document_id": oc_document_id,
            "oc_number": oc_number,
            "oc_state": int(oc_state or 0),
            "detail_ids_consulted": [],
            "confirmed_before": True,
            "would_confirm": False,
            "status": "existing",
            "api_error": None,
            "rate_limited": False,
            "edges": [],
            "unique_related_documents": [],
            "api_calls": 0,
            "existing_pairs_count": len(existing_pairs),
            "discovery_mode": discovery_mode,
            "details_total": 0,
            "details_queried": 0,
            "details_skipped_after_confirmation": 0,
            "early_exit": True,
            "fast_confirmed": True,
            "skipped_local_already_confirmed": True,
        }

    try:
        detail_ids, calls_details = srs._fetch_detail_ids_from_bsale_details(
            client,
            oc_document_id,
            throttle=throttle,
            log_ctx=log_ctx,
            bsale_source_document_id=source_id,
        )
    except RuntimeError as exc:
        msg = str(exc)
        raise OcRelatedApiError(msg, rate_limited=_is_rate_limit_message(msg)) from exc

    if not detail_ids:
        detail_ids = srs._detail_ids_for_document(cur, oc_document_id)

    details_total = len(detail_ids)
    detail_ids_queried: list[int] = []
    edges: list[dict[str, Any]] = []
    unique_docs: dict[tuple[int, int], dict[str, Any]] = {}
    api_calls = calls_details
    api_error: OcRelatedApiError | None = None
    max_type33 = srs._related_max_type33_depth()
    early_exit = False
    fast_confirmed = False
    details_skipped_after_confirmation = 0
    found_confirmed_invoice = confirmed_before

    for idx, detail_id in enumerate(detail_ids):
        if fast and found_confirmed_invoice:
            details_skipped_after_confirmation = details_total - len(detail_ids_queried)
            early_exit = True
            break

        try:
            items, c_rel = fetch_relateddetailid_items(
                client,
                detail_id,
                office_id=office_id,
                throttle=throttle,
                log_ctx=log_ctx,
            )
        except OcRelatedApiError as exc:
            api_error = exc
            break

        detail_ids_queried.append(int(detail_id))
        api_calls += c_rel
        try:
            triples, tc = srs._documents_json_items_to_triples(
                client,
                cur,
                detail_id,
                items,
                oc_document_id=oc_document_id,
                throttle=throttle,
                max_type33_depth=max_type33,
                stats=stats,
                log_ctx=log_ctx,
            )
        except RuntimeError as exc:
            msg = str(exc)
            api_error = OcRelatedApiError(msg, rate_limited=_is_rate_limit_message(msg))
            break
        api_calls += tc

        found_invoice_this_detail = False
        for did, rid, tid in triples:
            if int(tid) not in CONFIRMED_INVOICE_TYPES:
                continue
            found_invoice_this_detail = True
            found_confirmed_invoice = True
            pair = (int(did), int(rid))
            already = pair in existing_pairs
            classification = "existing" if already else (
                "would_insert_receipt" if int(tid) == 1 else "would_insert_invoice"
            )
            blob = next(
                (it for it in items if srs._parse_related_document_blob(it)[0] == rid),
                {},
            )
            edge = {
                "detail_id": int(did),
                "related_document_id": int(rid),
                "related_document_type": int(tid),
                "related_number": _related_number(blob if isinstance(blob, dict) else {}),
                "generation_date": _generation_date_iso(blob if isinstance(blob, dict) else {}),
                "classification": classification,
                "already_in_document_related": already,
                "relation_source": "bsale_relateddetailid",
            }
            edges.append(edge)
            key = (int(rid), int(tid))
            if key not in unique_docs:
                unique_docs[key] = {
                    "related_document_id": int(rid),
                    "related_document_type": int(tid),
                    "related_number": edge["related_number"],
                    "generation_date": edge["generation_date"],
                    "relation_source": "bsale_relateddetailid",
                }

        if fast and found_invoice_this_detail:
            fast_confirmed = True
            early_exit = True
            details_skipped_after_confirmation = details_total - len(detail_ids_queried)
            break

    would_confirm = any(
        e["classification"] in ("would_insert_invoice", "would_insert_receipt")
        for e in edges
    )
    if api_error is not None:
        oc_status = "rate_limited" if api_error.rate_limited else "api_error"
    elif not edges:
        oc_status = "no_relation_found"
    elif would_confirm:
        oc_status = "would_insert"
    elif confirmed_before or edges:
        oc_status = "existing"
    else:
        oc_status = "no_relation_found"

    hypothetical_related_calls = details_total
    api_calls_saved = max(0, hypothetical_related_calls - len(detail_ids_queried))

    return {
        "oc_document_id": oc_document_id,
        "oc_number": oc_number,
        "oc_state": int(oc_state or 0),
        "detail_ids_consulted": detail_ids_queried,
        "confirmed_before": confirmed_before,
        "would_confirm": would_confirm and not api_error,
        "status": oc_status,
        "api_error": str(api_error) if api_error else None,
        "rate_limited": bool(api_error and api_error.rate_limited),
        "edges": edges,
        "unique_related_documents": list(unique_docs.values()),
        "api_calls": api_calls,
        "discovery_mode": discovery_mode,
        "details_total": details_total,
        "details_queried": len(detail_ids_queried),
        "details_skipped_after_confirmation": details_skipped_after_confirmation,
        "early_exit": early_exit,
        "fast_confirmed": fast_confirmed,
        "api_calls_saved_by_early_exit": api_calls_saved if fast else 0,
    }


def edges_to_insert_triples(oc_res: dict[str, Any]) -> list[tuple[int, int, int]]:
    """Aristas nuevas tipo 1/6 listas para ``_insert_related_triples``."""
    triples: list[tuple[int, int, int]] = []
    for edge in oc_res.get("edges") or []:
        if edge.get("classification") not in ("would_insert_invoice", "would_insert_receipt"):
            continue
        triples.append(
            (
                int(edge["detail_id"]),
                int(edge["related_document_id"]),
                int(edge["related_document_type"]),
            )
        )
    return triples


def apply_discovered_invoice_edges(
    conn: PgConnection,
    cur,
    oc_res: dict[str, Any],
    *,
    stats: dict[str, Any] | None = None,
) -> int:
    """Persiste aristas nuevas 1/6 descubiertas (idempotente)."""
    if not oc_res.get("would_confirm"):
        return 0
    triples = edges_to_insert_triples(oc_res)
    if not triples:
        return 0
    srs = _sync_helpers()
    oc_number = oc_res.get("oc_number")
    oc_id = int(oc_res.get("oc_document_id") or 0)
    log_ctx = srs._related_log_ctx(oc_number, oc_id)
    return srs._insert_related_triples(conn, cur, triples, stats=stats, log_ctx=log_ctx)


def classify_oc_discovery_result(oc_res: dict[str, Any]) -> str:
    """Clasificación simple para métricas del sync incremental."""
    status = str(oc_res.get("status") or "")
    if oc_res.get("rate_limited") or status == "rate_limited":
        return "rate_limited"
    if status == "api_error":
        return "api_error"
    if status == "cancelled":
        return "cancelled"
    if oc_res.get("would_confirm"):
        return "discovered"
    if status == "existing" or oc_res.get("confirmed_before"):
        return "existing"
    return "no_relation"
