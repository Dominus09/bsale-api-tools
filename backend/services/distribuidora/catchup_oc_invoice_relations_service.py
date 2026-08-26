"""
Catchup histórico OC → boleta/factura vía Bsale ``relateddetailid``.

Dry-run por defecto: consulta Bsale, clasifica aristas, NO escribe.
Apply explícito: ``ON CONFLICT DO NOTHING`` (misma semántica que sync_related).

Reutiliza helpers de ``sync_related_service``; no usa probable para crear relaciones.
Rate-limit respetuoso: throttle global, Retry-After, backoff + jitter, max 5 retries.
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timezone
from typing import Any

from backend.db import get_connection
from backend.services.distribuidora.bsale_client import BsaleClient
from backend.services.distribuidora.bsale_params import merge_bsale_office_query
from backend.services.distribuidora.sync_related_service import (
    DOC_TYPE_OC,
    RELATED_DETAIL_PAGE_LIMIT,
    _bsale_source_id_from_pg,
    _detail_ids_for_document,
    _documents_json_items_to_triples,
    _fetch_detail_ids_from_bsale_details,
    _insert_related_triples,
    _oc_number_for_document,
    _parse_related_document_blob,
    _related_log_ctx,
    _related_max_type33_depth,
    _utc_day_emission_bounds,
)

logger = logging.getLogger(__name__)

CATCHUP_INVOICE_TYPES = frozenset({1, 6})  # boleta, factura — NO NC (9)
PLAN_OC_CANARIES = frozenset(
    {68933, 68920, 68927, 68565, 68572, 68631, 68632, 68714, 68538, 68728}
)

# Throttle conservador entre requests Bsale (750–1000 ms + jitter interno).
CATCHUP_DEFAULT_MIN_INTERVAL_SEC = 0.75
CATCHUP_DEFAULT_INTERVAL_JITTER_SEC = 0.25  # → hasta ~1000 ms
CATCHUP_MAX_429_RETRIES = 5


class CatchupApiError(Exception):
    """Error Bsale (incl. 429 agotado) — NO clasificar como sin relación."""

    def __init__(self, message: str, *, rate_limited: bool = False) -> None:
        super().__init__(message)
        self.rate_limited = rate_limited


def _bsale_token() -> str:
    return (os.getenv("BSALE_TOKEN") or "").strip() or (os.getenv("BSALE_TOKEN_SPA") or "").strip()


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


def fetch_oc_document_ids_for_range(
    cur,
    *,
    start_date: date,
    end_date: date,
    company_id: int,
    office_id: int,
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
    """Paginación ``relateddetailid`` secuencial; propaga CatchupApiError en fallo HTTP."""
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
                    context="catchup_relateddetailid",
                ),
            )
        except RuntimeError as exc:
            msg = str(exc)
            raise CatchupApiError(msg, rate_limited=_is_rate_limit_message(msg)) from exc
        except Exception as exc:
            raise CatchupApiError(str(exc), rate_limited=_is_rate_limit_message(str(exc))) from exc

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
        # Espaciado lo maneja BsaleClient (throttle global). No paralelizar.
        if throttle > 0:
            time.sleep(throttle)
    return merged, api_calls


def discover_invoice_edges_for_oc(
    client: BsaleClient,
    cur,
    oc_document_id: int,
    *,
    office_id: int,
    throttle: float,
    stats: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Descubre aristas detail→(1|6) desde Bsale sin persistir.

    Retorna estructura con detail_ids, edges (detail-level), unique_docs (OC-level).
    """
    oc_number = _oc_number_for_document(cur, oc_document_id)
    log_ctx = _related_log_ctx(oc_number, oc_document_id)
    source_id, _folio = _bsale_source_id_from_pg(cur, oc_document_id)

    try:
        detail_ids, calls_details = _fetch_detail_ids_from_bsale_details(
            client,
            oc_document_id,
            throttle=throttle,
            log_ctx=log_ctx,
            bsale_source_document_id=source_id,
        )
    except RuntimeError as exc:
        msg = str(exc)
        raise CatchupApiError(msg, rate_limited=_is_rate_limit_message(msg)) from exc

    if not detail_ids:
        detail_ids = _detail_ids_for_document(cur, oc_document_id)

    existing_pairs, confirmed_doc_keys = load_existing_invoice_relations_for_oc(
        cur, oc_document_id
    )
    confirmed_before = bool(confirmed_doc_keys)

    edges: list[dict[str, Any]] = []
    unique_docs: dict[tuple[int, int], dict[str, Any]] = {}
    api_calls = calls_details
    api_error: CatchupApiError | None = None
    max_type33 = _related_max_type33_depth()

    for detail_id in detail_ids:
        try:
            items, c_rel = fetch_relateddetailid_items(
                client,
                detail_id,
                office_id=office_id,
                throttle=throttle,
                log_ctx=log_ctx,
            )
        except CatchupApiError as exc:
            api_error = exc
            break

        api_calls += c_rel
        try:
            triples, tc = _documents_json_items_to_triples(
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
            api_error = CatchupApiError(msg, rate_limited=_is_rate_limit_message(msg))
            break
        api_calls += tc

        for did, rid, tid in triples:
            if int(tid) not in CATCHUP_INVOICE_TYPES:
                continue
            pair = (int(did), int(rid))
            already = pair in existing_pairs
            classification = "existing" if already else (
                "would_insert_receipt" if int(tid) == 1 else "would_insert_invoice"
            )
            blob = next(
                (
                    it
                    for it in items
                    if _parse_related_document_blob(it)[0] == rid
                ),
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

    return {
        "oc_document_id": oc_document_id,
        "oc_number": oc_number,
        "detail_ids_consulted": [int(x) for x in detail_ids],
        "confirmed_before": confirmed_before,
        "would_confirm": would_confirm and not api_error,
        "status": oc_status,
        "api_error": str(api_error) if api_error else None,
        "rate_limited": bool(api_error and api_error.rate_limited),
        "edges": edges,
        "unique_related_documents": list(unique_docs.values()),
        "api_calls": api_calls,
    }


@dataclass
class CatchupOcInvoiceReport:
    dry_run: bool = True
    company_id: int = 3
    office_id: int = 1
    start_date: str = ""
    end_date: str = ""
    oc_scanned: int = 0
    details_scanned: int = 0
    relations_existing: int = 0
    relations_discovered: int = 0
    invoice_links_would_insert: int = 0
    receipt_links_would_insert: int = 0
    invoice_links_inserted: int = 0
    receipt_links_inserted: int = 0
    ocs_with_new_confirmed_relation: int = 0
    ocs_without_relation: int = 0
    api_errors: int = 0
    rate_limited: int = 0
    # Rate-limit / progreso
    requests_total: int = 0
    rate_limit_events: int = 0
    retry_count: int = 0
    wait_seconds_total: float = 0.0
    ocs_completed: int = 0
    ocs_rate_limited: int = 0
    ocs_with_relation: int = 0
    samples: dict[str, Any] = field(default_factory=dict)
    plan_oc_results: list[dict[str, Any]] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def _progress_line(
    *,
    index: int,
    total: int,
    oc_res: dict[str, Any],
    rate_stats: dict[str, Any],
) -> str:
    return (
        f"[catchup_oc_invoice_relations] OC {index}/{total} "
        f"oc_number={oc_res.get('oc_number')} status={oc_res.get('status')} "
        f"requests={int(rate_stats.get('requests_total') or 0)} "
        f"429={int(rate_stats.get('rate_limit_events') or 0)} "
        f"retries={int(rate_stats.get('retry_count') or 0)} "
        f"wait_seconds={float(rate_stats.get('wait_seconds_total') or 0):.1f}"
    )


def run_catchup_oc_invoice_relations(
    *,
    start_date: date,
    end_date: date,
    company_id: int = 3,
    office_id: int = 1,
    dry_run: bool = True,
    throttle: float | None = None,
    plan_canaries: frozenset[int] | None = None,
) -> CatchupOcInvoiceReport:
    plan_canaries = plan_canaries or PLAN_OC_CANARIES
    report = CatchupOcInvoiceReport(
        dry_run=dry_run,
        company_id=company_id,
        office_id=office_id,
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
    )

    token = _bsale_token()
    if not token:
        report.errors.append("sin token Bsale (BSALE_TOKEN / BSALE_TOKEN_SPA)")
        return report

    # Throttle de helpers internos: el espaciado principal lo hace BsaleClient.
    # Si el caller pasa --throttle-sec, se suma como sleep extra en paginación.
    helper_throttle = float(throttle) if throttle is not None else 0.0

    rate_stats: dict[str, Any] = {
        "requests_total": 0,
        "rate_limit_events": 0,
        "retry_count": 0,
        "wait_seconds_total": 0.0,
        "throttle_wait_seconds": 0.0,
    }

    conn = get_connection()
    sample_new: list[dict[str, Any]] = []
    sample_no_rel: list[dict[str, Any]] = []

    try:
        cur = conn.cursor()
        oc_ids = fetch_oc_document_ids_for_range(
            cur,
            start_date=start_date,
            end_date=end_date,
            company_id=company_id,
            office_id=office_id,
        )
        report.oc_scanned = len(oc_ids)
        total = len(oc_ids)

        client = BsaleClient(
            token,
            min_interval_sec=CATCHUP_DEFAULT_MIN_INTERVAL_SEC,
            min_interval_jitter_sec=CATCHUP_DEFAULT_INTERVAL_JITTER_SEC,
            max_429_retries=CATCHUP_MAX_429_RETRIES,
            rate_stats=rate_stats,
        )
        stats: dict[str, Any] = {}

        for idx, oc_id in enumerate(oc_ids, start=1):
            try:
                oc_res = discover_invoice_edges_for_oc(
                    client,
                    cur,
                    oc_id,
                    office_id=office_id,
                    throttle=helper_throttle,
                    stats=stats,
                )
            except CatchupApiError as exc:
                oc_res = {
                    "oc_document_id": oc_id,
                    "oc_number": _oc_number_for_document(cur, oc_id),
                    "status": "rate_limited" if exc.rate_limited else "api_error",
                    "confirmed_before": False,
                    "would_confirm": False,
                    "edges": [],
                    "unique_related_documents": [],
                    "detail_ids_consulted": [],
                    "api_error": str(exc),
                    "rate_limited": bool(exc.rate_limited),
                }
                if not exc.rate_limited:
                    report.errors.append(f"oc_id={oc_id}: {exc}")
            except Exception as exc:
                report.api_errors += 1
                report.errors.append(f"oc_id={oc_id}: {exc}")
                oc_res = {
                    "oc_document_id": oc_id,
                    "oc_number": _oc_number_for_document(cur, oc_id),
                    "status": "api_error",
                    "confirmed_before": False,
                    "would_confirm": False,
                    "edges": [],
                    "unique_related_documents": [],
                    "detail_ids_consulted": [],
                    "api_error": str(exc),
                    "rate_limited": _is_rate_limit_message(str(exc)),
                }
                if oc_res["rate_limited"]:
                    oc_res["status"] = "rate_limited"

            report.ocs_completed += 1
            report.details_scanned += len(oc_res.get("detail_ids_consulted") or [])

            print(
                _progress_line(
                    index=idx,
                    total=total,
                    oc_res=oc_res,
                    rate_stats=rate_stats,
                ),
                flush=True,
            )

            if oc_res.get("rate_limited") or oc_res.get("status") == "rate_limited":
                report.rate_limited += 1
                report.ocs_rate_limited += 1
            elif oc_res.get("status") == "api_error":
                report.api_errors += 1

            if oc_res.get("status") == "no_relation_found":
                report.ocs_without_relation += 1

            if oc_res.get("status") in ("existing", "would_insert"):
                report.ocs_with_relation += 1

            seen_existing: set[tuple[int, int]] = set()
            seen_would_inv: set[tuple[int, int]] = set()
            seen_would_rec: set[tuple[int, int]] = set()
            for edge in oc_res.get("edges") or []:
                cls = edge.get("classification")
                rid = int(edge["related_document_id"])
                tid = int(edge["related_document_type"])
                doc_key = (rid, tid)
                if cls == "existing":
                    if doc_key not in seen_existing:
                        seen_existing.add(doc_key)
                        report.relations_existing += 1
                elif cls == "would_insert_invoice":
                    if doc_key not in seen_would_inv:
                        seen_would_inv.add(doc_key)
                        report.relations_discovered += 1
                        report.invoice_links_would_insert += 1
                elif cls == "would_insert_receipt":
                    if doc_key not in seen_would_rec:
                        seen_would_rec.add(doc_key)
                        report.relations_discovered += 1
                        report.receipt_links_would_insert += 1

            if oc_res.get("would_confirm"):
                report.ocs_with_new_confirmed_relation += 1
                if len(sample_new) < 8:
                    sample_new.append(_oc_summary(oc_res))
            elif oc_res.get("status") == "no_relation_found" and len(sample_no_rel) < 8:
                sample_no_rel.append(_oc_summary(oc_res))

            oc_number = oc_res.get("oc_number")
            if oc_number is not None and int(oc_number) in plan_canaries:
                report.plan_oc_results.append(_plan_oc_entry(oc_res))

            if not dry_run and oc_res.get("would_confirm"):
                triples = [
                    (
                        int(e["detail_id"]),
                        int(e["related_document_id"]),
                        int(e["related_document_type"]),
                    )
                    for e in oc_res.get("edges") or []
                    if e.get("classification")
                    in ("would_insert_invoice", "would_insert_receipt")
                ]
                if triples:
                    inv_triples = [t for t in triples if t[2] == 6]
                    rec_triples = [t for t in triples if t[2] == 1]
                    log = _related_log_ctx(oc_number, oc_id)
                    if inv_triples:
                        report.invoice_links_inserted += _insert_related_triples(
                            conn, cur, inv_triples, stats=stats, log_ctx=log
                        )
                    if rec_triples:
                        report.receipt_links_inserted += _insert_related_triples(
                            conn, cur, rec_triples, stats=stats, log_ctx=log
                        )

        cur.close()
        if dry_run:
            conn.rollback()
    finally:
        conn.close()

    report.requests_total = int(rate_stats.get("requests_total") or 0)
    report.rate_limit_events = int(rate_stats.get("rate_limit_events") or 0)
    report.retry_count = int(rate_stats.get("retry_count") or 0)
    report.wait_seconds_total = float(rate_stats.get("wait_seconds_total") or 0.0)

    report.samples = {
        "would_insert_examples": sample_new,
        "no_relation_examples": sample_no_rel,
    }
    return report


def _oc_summary(oc_res: dict[str, Any]) -> dict[str, Any]:
    uniq = oc_res.get("unique_related_documents") or []
    first = uniq[0] if uniq else {}
    return {
        "oc_document_id": oc_res.get("oc_document_id"),
        "oc_number": oc_res.get("oc_number"),
        "status": oc_res.get("status"),
        "confirmed_before": oc_res.get("confirmed_before"),
        "would_confirm": oc_res.get("would_confirm"),
        "related_number": first.get("related_number"),
        "related_type": first.get("related_document_type"),
        "generation_date": first.get("generation_date"),
        "unique_docs_count": len(uniq),
        "edges_count": len(oc_res.get("edges") or []),
    }


def _plan_oc_entry(oc_res: dict[str, Any]) -> dict[str, Any]:
    uniq = oc_res.get("unique_related_documents") or []
    best = uniq[0] if uniq else {}
    return {
        "oc_number": oc_res.get("oc_number"),
        "oc_document_id": oc_res.get("oc_document_id"),
        "confirmed_before": bool(oc_res.get("confirmed_before")),
        "would_confirm": bool(oc_res.get("would_confirm")),
        "related_number": best.get("related_number"),
        "related_type": best.get("related_document_type"),
        "generation_date": best.get("generation_date"),
        "relation_source": "bsale_relateddetailid",
        "status": oc_res.get("status"),
        "api_error": oc_res.get("api_error"),
    }
