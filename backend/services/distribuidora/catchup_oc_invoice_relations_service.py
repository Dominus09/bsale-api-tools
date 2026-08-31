"""
Catchup histórico OC → boleta/factura (orquestador).

La lógica de descubrimiento vive en ``oc_related_discovery_service`` (motor canónico).
Este módulo solo selecciona candidatos (rango o folio), dry-run/apply y reporta.
"""

from __future__ import annotations

import logging
import os
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timezone
from typing import Any

from backend.db import get_connection
from backend.services.distribuidora.oc_related_discovery_service import (
    CATCHUP_DEFAULT_INTERVAL_JITTER_SEC,
    CATCHUP_DEFAULT_MIN_INTERVAL_SEC,
    CATCHUP_INVOICE_TYPES,
    CATCHUP_MAX_429_RETRIES,
    DOC_TYPE_OC,
    CatchupApiError,
    OcRelatedApiError,
    apply_discovered_invoice_edges,
    create_bsale_client_for_related_discovery,
    discover_confirmed_related_documents_for_oc,
    discover_invoice_edges_for_oc,
    edges_to_insert_triples,
    fetch_oc_document_ids_for_range,
    fetch_relateddetailid_items,
    load_existing_invoice_relations_for_oc,
)
from backend.services.distribuidora.sync_related_service import (
    _insert_related_triples,
    _oc_number_for_document,
    _related_log_ctx,
)

logger = logging.getLogger(__name__)

PLAN_OC_CANARIES = frozenset(
    {68933, 68920, 68927, 68565, 68572, 68631, 68632, 68714, 68538, 68728}
)


def _bsale_token() -> str:
    return (os.getenv("BSALE_TOKEN") or "").strip() or (os.getenv("BSALE_TOKEN_SPA") or "").strip()


def _is_rate_limit_message(msg: str) -> bool:
    m = msg.lower()
    return "429" in m or "rate limit" in m


def _generation_date_iso(blob: dict[str, Any]) -> str | None:
    from backend.services.distribuidora.oc_related_discovery_service import _generation_date_iso as _g

    return _g(blob)


@dataclass
class CatchupOcInvoiceReport:
    dry_run: bool = True
    company_id: int = 3
    office_id: int = 1
    mode: str = "by_range"
    start_date: str = ""
    end_date: str = ""
    oc_number: int | None = None
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
    ocs_cancelled: int = 0
    api_errors: int = 0
    rate_limited: int = 0
    requests_total: int = 0
    rate_limit_events: int = 0
    retry_count: int = 0
    wait_seconds_total: float = 0.0
    ocs_completed: int = 0
    ocs_rate_limited: int = 0
    ocs_with_relation: int = 0
    no_relation_folios: list[int] = field(default_factory=list)
    cancelled_folios: list[int] = field(default_factory=list)
    samples: dict[str, Any] = field(default_factory=dict)
    plan_oc_results: list[dict[str, Any]] = field(default_factory=list)
    canary: dict[str, Any] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def _dt_iso(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return dt.isoformat()
    return str(value)


def find_oc_by_number(
    cur,
    *,
    oc_number: int,
    company_id: int,
    office_id: int,
) -> dict[str, Any] | None:
    """
    Localiza OC por folio exacto (company/office/tipo 33).

    No filtra por ``emission_date``.
    """
    cur.execute(
        """
        SELECT document_id, number, emission_date, generation_date, COALESCE(state, 0)
        FROM distribuidora.documents
        WHERE company_id = %s
          AND office_id = %s
          AND document_type_id = %s
          AND number = %s
        ORDER BY document_id DESC
        LIMIT 1
        """,
        (company_id, office_id, DOC_TYPE_OC, int(oc_number)),
    )
    row = cur.fetchone()
    if not row:
        return None
    return {
        "oc_document_id": int(row[0]),
        "oc_number": int(row[1]) if row[1] is not None else int(oc_number),
        "emission_date": _dt_iso(row[2]),
        "generation_date": _dt_iso(row[3]),
        "state": int(row[4] or 0),
    }


def _canary_result(
    *,
    oc_number: int,
    oc_meta: dict[str, Any] | None,
    oc_res: dict[str, Any] | None = None,
    status: str | None = None,
) -> dict[str, Any]:
    """Resumen canario para stdout / report.canary."""
    if status == "local_oc_not_found" or oc_meta is None:
        return {
            "oc_number": int(oc_number),
            "oc_document_id": None,
            "emission_date": None,
            "generation_date": None,
            "state": None,
            "details_scanned": 0,
            "confirmed_before": False,
            "status": "local_oc_not_found",
            "would_confirm": False,
            "related_number": None,
            "related_type": None,
            "unique_docs_count": 0,
            "edges_count": 0,
        }

    res = oc_res or {}
    uniq = res.get("unique_related_documents") or []
    first = uniq[0] if uniq else {}
    out: dict[str, Any] = {
        "oc_number": int(oc_meta.get("oc_number") or oc_number),
        "oc_document_id": oc_meta.get("oc_document_id"),
        "emission_date": oc_meta.get("emission_date"),
        # generation_date de la OC (PG); el del relacionado va en related_generation_date.
        "generation_date": oc_meta.get("generation_date"),
        "state": oc_meta.get("state"),
        "details_scanned": len(res.get("detail_ids_consulted") or []),
        "confirmed_before": bool(res.get("confirmed_before")),
        "status": status or res.get("status"),
        "would_confirm": bool(res.get("would_confirm")),
        "unique_docs_count": len(uniq),
        "edges_count": len(res.get("edges") or []),
        "related_number": first.get("related_number"),
        "related_type": first.get("related_document_type"),
        "related_generation_date": first.get("generation_date"),
    }
    return out


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


def _process_oc_ids(
    *,
    conn,
    cur,
    client,
    oc_ids: list[int],
    report: CatchupOcInvoiceReport,
    office_id: int,
    helper_throttle: float,
    dry_run: bool,
    rate_stats: dict[str, Any],
    plan_canaries: frozenset[int],
) -> None:
    sample_new: list[dict[str, Any]] = []
    sample_no_rel: list[dict[str, Any]] = []
    stats: dict[str, Any] = {}
    total = len(oc_ids)

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
        except (CatchupApiError, OcRelatedApiError) as exc:
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
        elif oc_res.get("status") == "cancelled":
            report.ocs_cancelled += 1
            folio = oc_res.get("oc_number")
            if folio is not None:
                report.cancelled_folios.append(int(folio))

        if oc_res.get("status") == "no_relation_found":
            report.ocs_without_relation += 1
            folio = oc_res.get("oc_number")
            if folio is not None:
                report.no_relation_folios.append(int(folio))

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
            triples = edges_to_insert_triples(oc_res)
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

        report.samples["_last_oc_res"] = oc_res

    report.samples = {
        "would_insert_examples": sample_new,
        "no_relation_examples": sample_no_rel,
        "_last_oc_res": report.samples.get("_last_oc_res"),
    }


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
        mode="by_range",
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
    )

    token = _bsale_token()
    if not token:
        report.errors.append("sin token Bsale (BSALE_TOKEN / BSALE_TOKEN_SPA)")
        return report

    helper_throttle = float(throttle) if throttle is not None else 0.0

    rate_stats: dict[str, Any] = {
        "requests_total": 0,
        "rate_limit_events": 0,
        "retry_count": 0,
        "wait_seconds_total": 0.0,
        "throttle_wait_seconds": 0.0,
    }

    conn = get_connection()
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

        client = create_bsale_client_for_related_discovery(token, rate_stats=rate_stats)
        _process_oc_ids(
            conn=conn,
            cur=cur,
            client=client,
            oc_ids=oc_ids,
            report=report,
            office_id=office_id,
            helper_throttle=helper_throttle,
            dry_run=dry_run,
            rate_stats=rate_stats,
            plan_canaries=plan_canaries,
        )
        report.samples.pop("_last_oc_res", None)

        cur.close()
        if dry_run:
            conn.rollback()
    finally:
        conn.close()

    report.requests_total = int(rate_stats.get("requests_total") or 0)
    report.rate_limit_events = int(rate_stats.get("rate_limit_events") or 0)
    report.retry_count = int(rate_stats.get("retry_count") or 0)
    report.wait_seconds_total = float(rate_stats.get("wait_seconds_total") or 0.0)
    return report


def run_catchup_oc_invoice_relations_by_oc_number(
    *,
    oc_number: int,
    company_id: int = 3,
    office_id: int = 1,
    dry_run: bool = True,
    throttle: float | None = None,
) -> CatchupOcInvoiceReport:
    """
    Modo canario: una sola OC por folio exacto (sin filtro emission_date).

    Usa el mismo motor ``discover_invoice_edges_for_oc``.
    """
    report = CatchupOcInvoiceReport(
        dry_run=dry_run,
        company_id=company_id,
        office_id=office_id,
        mode="by_oc_number",
        oc_number=int(oc_number),
    )

    token = _bsale_token()
    if not token:
        report.errors.append("sin token Bsale (BSALE_TOKEN / BSALE_TOKEN_SPA)")
        report.canary = _canary_result(
            oc_number=oc_number, oc_meta=None, status="local_oc_not_found"
        )
        return report

    helper_throttle = float(throttle) if throttle is not None else 0.0
    rate_stats: dict[str, Any] = {
        "requests_total": 0,
        "rate_limit_events": 0,
        "retry_count": 0,
        "wait_seconds_total": 0.0,
        "throttle_wait_seconds": 0.0,
    }

    conn = get_connection()
    try:
        cur = conn.cursor()
        oc_meta = find_oc_by_number(
            cur,
            oc_number=int(oc_number),
            company_id=company_id,
            office_id=office_id,
        )
        if oc_meta is None:
            report.canary = _canary_result(
                oc_number=oc_number, oc_meta=None, status="local_oc_not_found"
            )
            report.oc_scanned = 0
            print(
                f"[catchup_oc_invoice_relations] canary oc_number={oc_number} "
                f"status=local_oc_not_found company={company_id} office={office_id}",
                flush=True,
            )
            cur.close()
            if dry_run:
                conn.rollback()
            return report

        oc_id = int(oc_meta["oc_document_id"])
        report.oc_scanned = 1
        client = create_bsale_client_for_related_discovery(token, rate_stats=rate_stats)
        _process_oc_ids(
            conn=conn,
            cur=cur,
            client=client,
            oc_ids=[oc_id],
            report=report,
            office_id=office_id,
            helper_throttle=helper_throttle,
            dry_run=dry_run,
            rate_stats=rate_stats,
            plan_canaries=frozenset(),
        )
        last = report.samples.pop("_last_oc_res", None)
        report.canary = _canary_result(
            oc_number=oc_number, oc_meta=oc_meta, oc_res=last
        )
        print(
            f"[catchup_oc_invoice_relations] canary "
            f"oc_number={report.canary.get('oc_number')} "
            f"oc_document_id={report.canary.get('oc_document_id')} "
            f"emission_date={report.canary.get('emission_date')} "
            f"status={report.canary.get('status')} "
            f"would_confirm={report.canary.get('would_confirm')} "
            f"related_number={report.canary.get('related_number')} "
            f"related_type={report.canary.get('related_type')}",
            flush=True,
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
