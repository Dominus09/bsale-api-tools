"""Auditoría read-only de correlativos de OC (tipo 33).

Detecta saltos numéricos por ``(company_id, office_id, document_type_id=33)``
y, opcionalmente, investiga cada faltante en Bsale con los filtros ya usados por
``oc_source_resolver.search_documents_by_folio`` (``number``, ``documenttypeid``,
``officeid`` + paginación).

No escribe en PostgreSQL ni dispara recuperación. No toca ventas ni predespacho.

Clasificación de ``number=0``: no forma parte de la secuencia de correlativos;
las filas locales con ``number <= 0`` se listan en ``zero_number_rows``.
Secuencias compartidas: siempre se particiona por empresa + oficina + tipo 33.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any, Callable

from backend.db import get_connection
from backend.services.distribuidora.bsale_client import BsaleClient
from backend.services.distribuidora.oc_source_resolver import (
    COMPANY_ID,
    OC_DOCUMENT_TYPE_ID,
    OFFICE_ID,
    is_active_oc_candidate,
    is_cancelled_bsale_document,
    search_documents_by_folio,
    select_active_oc_source,
    summarize_bsale_document,
)

logger = logging.getLogger(__name__)

STATUS_OK = "ok"
STATUS_GAP_PENDING = "gap_pending"
STATUS_MISSING_IN_PG = "missing_in_pg"
STATUS_VERSION_DIFF = "version_diff"
STATUS_CANCELLED = "cancelled"
STATUS_NOT_FOUND_IN_BSALE = "not_found_in_bsale"
STATUS_VERIFICATION_ERROR = "verification_error"

DEFAULT_MAX_RANGE_SPAN = 500
DEFAULT_MAX_GAPS = 100
DEFAULT_MAX_INVESTIGATE = 50


class CorrelativeRangeError(ValueError):
    """Rango inválido o demasiado amplio para auditoría segura."""


def _int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def validate_correlative_range(
    *,
    from_folio: int,
    to_folio: int,
    max_range_span: int = DEFAULT_MAX_RANGE_SPAN,
) -> tuple[int, int]:
    start = int(from_folio)
    end = int(to_folio)
    if start <= 0 or end <= 0:
        raise CorrelativeRangeError("from_folio y to_folio deben ser > 0")
    if end < start:
        raise CorrelativeRangeError("to_folio debe ser >= from_folio")
    span = end - start + 1
    limit = max(1, int(max_range_span))
    if span > limit:
        raise CorrelativeRangeError(
            f"Rango demasiado amplio ({span} números); máximo permitido={limit}"
        )
    return start, end


def find_numeric_gaps(
    present_folios: set[int] | list[int],
    *,
    from_folio: int,
    to_folio: int,
    max_gaps: int = DEFAULT_MAX_GAPS,
    max_range_span: int = DEFAULT_MAX_RANGE_SPAN,
) -> dict[str, Any]:
    """Calcula saltos enteros positivos en ``[from_folio, to_folio]``."""
    start, end = validate_correlative_range(
        from_folio=from_folio,
        to_folio=to_folio,
        max_range_span=max_range_span,
    )
    present = {int(n) for n in present_folios if int(n) > 0}
    gaps: list[int] = []
    truncated = False
    cap = max(0, int(max_gaps))
    for folio in range(start, end + 1):
        if folio in present:
            continue
        if len(gaps) >= cap:
            truncated = True
            break
        gaps.append(folio)
    return {
        "gaps": gaps,
        "gap_count": len(gaps),
        "truncated": truncated,
        "present_count": sum(1 for n in range(start, end + 1) if n in present),
        "from_folio": start,
        "to_folio": end,
        "range_span": end - start + 1,
    }


def load_local_oc_folios(
    *,
    company_id: int = COMPANY_ID,
    office_id: int = OFFICE_ID,
    from_folio: int,
    to_folio: int,
    document_type_id: int = OC_DOCUMENT_TYPE_ID,
    max_range_span: int = DEFAULT_MAX_RANGE_SPAN,
) -> dict[str, Any]:
    """Carga filas locales del rango. Solo lectura."""
    start, end = validate_correlative_range(
        from_folio=from_folio,
        to_folio=to_folio,
        max_range_span=max_range_span,
    )
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                d.number,
                d.document_id,
                d.state,
                NULLIF(to_jsonb(d)->>'source_document_id', '')::bigint
                    AS source_document_id,
                d.total_amount,
                d.emission_date
            FROM distribuidora.documents d
            WHERE d.company_id = %s
              AND d.office_id = %s
              AND d.document_type_id = %s
              AND d.number BETWEEN %s AND %s
            ORDER BY d.number, d.document_id
            """,
            (
                int(company_id),
                int(office_id),
                int(document_type_id),
                start,
                end,
            ),
        )
        rows = cur.fetchall()
        cur.close()
    finally:
        conn.close()

    by_folio: dict[int, dict[str, Any]] = {}
    zero_number_rows: list[dict[str, Any]] = []
    for number, document_id, state, source_document_id, total_amount, emission_date in rows:
        folio = _int(number)
        row = {
            "folio": folio,
            "document_id": int(document_id),
            "state": _int(state),
            "source_document_id": _int(source_document_id),
            "total_amount": total_amount,
            "emission_date": emission_date.isoformat() if emission_date else None,
            "company_id": int(company_id),
            "office_id": int(office_id),
            "document_type_id": int(document_type_id),
        }
        if folio is None or folio <= 0:
            zero_number_rows.append(row)
            continue
        prev = by_folio.get(folio)
        if prev is None or (prev.get("state") != 0 and row.get("state") == 0):
            by_folio[folio] = row
    return {
        "by_folio": by_folio,
        "zero_number_rows": zero_number_rows,
        "row_count": len(rows),
    }


def _belongs_to_requested_sequence(
    document: dict[str, Any],
    *,
    folio: int,
    company_id: int,
    office_id: int,
    document_type_id: int,
) -> bool:
    summary = summarize_bsale_document(document, expected_company_id=company_id)
    if summary.get("company_id") != int(company_id):
        return False
    if summary.get("office_id") != int(office_id):
        return False
    if summary.get("document_type_id") != int(document_type_id):
        return False
    number = summary.get("number")
    # Activo del folio o cancelado (number=0 / state≠0) del mismo scope.
    if number == int(folio):
        return True
    if is_cancelled_bsale_document(document):
        return True
    return False


def _filter_matching_bsale_items(
    items: list[dict[str, Any]],
    *,
    folio: int,
    company_id: int,
    office_id: int,
    document_type_id: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    matched: list[dict[str, Any]] = []
    mismatched: list[dict[str, Any]] = []
    for raw in items:
        if not isinstance(raw, dict):
            continue
        if _belongs_to_requested_sequence(
            raw,
            folio=folio,
            company_id=company_id,
            office_id=office_id,
            document_type_id=document_type_id,
        ):
            matched.append(raw)
        else:
            ok, reasons = is_active_oc_candidate(
                raw,
                folio=folio,
                company_id=company_id,
                office_id=office_id,
                document_type_id=document_type_id,
            )
            summary = summarize_bsale_document(raw, expected_company_id=company_id)
            mismatched.append({**summary, "eligible": ok, "discard_reasons": reasons})
    return matched, mismatched


def investigate_folio_in_bsale(
    client: BsaleClient,
    *,
    folio: int,
    company_id: int = COMPANY_ID,
    office_id: int = OFFICE_ID,
    document_type_id: int = OC_DOCUMENT_TYPE_ID,
    local_row: dict[str, Any] | None = None,
    search_fn: Callable[..., list[dict[str, Any]]] | None = None,
) -> dict[str, Any]:
    """Consulta Bsale para un folio. Nunca escribe."""
    search = search_fn or search_documents_by_folio
    query = {
        "number": int(folio),
        "documenttypeid": int(document_type_id),
        "officeid": int(office_id),
    }
    try:
        raw_items = search(
            client,
            folio=int(folio),
            office_id=int(office_id),
            document_type_id=int(document_type_id),
        )
    except Exception as exc:
        return {
            "folio": int(folio),
            "status": STATUS_VERIFICATION_ERROR,
            "error": str(exc),
            "error_kind": type(exc).__name__,
            "bsale_query": query,
            "local_document_id": (local_row or {}).get("document_id"),
            "wrote": False,
        }

    if not isinstance(raw_items, list):
        return {
            "folio": int(folio),
            "status": STATUS_VERIFICATION_ERROR,
            "error": "bsale_response_not_a_list",
            "bsale_query": query,
            "wrote": False,
        }

    matched, mismatched = _filter_matching_bsale_items(
        raw_items,
        folio=int(folio),
        company_id=int(company_id),
        office_id=int(office_id),
        document_type_id=int(document_type_id),
    )
    if mismatched and not matched:
        return {
            "folio": int(folio),
            "status": STATUS_VERIFICATION_ERROR,
            "error": "bsale_returned_unrelated_documents",
            "unrelated_count": len(mismatched),
            "unrelated_sample": mismatched[:3],
            "bsale_query": query,
            "wrote": False,
        }

    active, evaluated = select_active_oc_source(
        matched,
        folio=int(folio),
        company_id=int(company_id),
        office_id=int(office_id),
        document_type_id=int(document_type_id),
    )

    if active is not None:
        active_summary = summarize_bsale_document(
            active, expected_company_id=company_id
        )
        active_id = int(active_summary["id"])
        if local_row is None:
            return {
                "folio": int(folio),
                "status": STATUS_MISSING_IN_PG,
                "bsale_source_document_id": active_id,
                "bsale_state": active_summary.get("state"),
                "bsale_total_amount": active_summary.get("totalAmount"),
                "candidates_evaluated": len(evaluated),
                "unrelated_count": len(mismatched),
                "wrote": False,
            }
        local_source = local_row.get("source_document_id")
        local_id = local_row.get("document_id")
        if local_source is not None and int(local_source) != active_id:
            return {
                "folio": int(folio),
                "status": STATUS_VERSION_DIFF,
                "local_document_id": local_id,
                "postgresql_source_document_id": int(local_source),
                "bsale_source_document_id": active_id,
                "wrote": False,
            }
        if local_source is None and local_id is not None and int(local_id) != active_id:
            return {
                "folio": int(folio),
                "status": STATUS_VERSION_DIFF,
                "local_document_id": local_id,
                "postgresql_source_document_id": None,
                "bsale_source_document_id": active_id,
                "wrote": False,
            }
        return {
            "folio": int(folio),
            "status": STATUS_OK,
            "local_document_id": local_id,
            "bsale_source_document_id": active_id,
            "wrote": False,
        }

    cancelled = [item for item in matched if is_cancelled_bsale_document(item)]
    if cancelled:
        top = max(
            cancelled,
            key=lambda d: (
                _int(d.get("generationDate")) or -1,
                _int(d.get("id")) or -1,
            ),
        )
        summary = summarize_bsale_document(top, expected_company_id=company_id)
        return {
            "folio": int(folio),
            "status": STATUS_CANCELLED,
            "bsale_source_document_id": summary.get("id"),
            "bsale_state": summary.get("state"),
            "bsale_number": summary.get("number"),
            "note": "active_version_absent_cancelled_evidence",
            "wrote": False,
        }

    if not matched:
        return {
            "folio": int(folio),
            "status": STATUS_NOT_FOUND_IN_BSALE,
            "note": "empty_bsale_result_after_valid_query",
            "bsale_query": query,
            "wrote": False,
        }

    return {
        "folio": int(folio),
        "status": STATUS_VERIFICATION_ERROR,
        "error": "no_active_or_cancelled_match",
        "candidates_evaluated": len(evaluated),
        "unrelated_count": len(mismatched),
        "wrote": False,
    }


def audit_oc_correlatives(
    client: BsaleClient | None = None,
    *,
    company_id: int = COMPANY_ID,
    office_id: int = OFFICE_ID,
    from_folio: int,
    to_folio: int,
    document_type_id: int = OC_DOCUMENT_TYPE_ID,
    investigate: bool = True,
    max_range_span: int = DEFAULT_MAX_RANGE_SPAN,
    max_gaps: int = DEFAULT_MAX_GAPS,
    max_investigate: int = DEFAULT_MAX_INVESTIGATE,
    local_loader: Callable[..., dict[str, Any]] | None = None,
    search_fn: Callable[..., list[dict[str, Any]]] | None = None,
) -> dict[str, Any]:
    """
    Auditoría read-only de correlativos OC.

    ``investigate=False`` solo reporta ``ok`` / ``gap_pending``.
    Con ``investigate=True`` consulta Bsale hasta ``max_investigate`` gaps
    (y version_diff de existentes si queda cupo).
    """
    started = time.perf_counter()
    started_at = datetime.now(timezone.utc)
    start, end = validate_correlative_range(
        from_folio=from_folio,
        to_folio=to_folio,
        max_range_span=max_range_span,
    )
    if int(document_type_id) != int(OC_DOCUMENT_TYPE_ID):
        raise CorrelativeRangeError(
            f"Solo se auditan OC tipo {OC_DOCUMENT_TYPE_ID}; recibido={document_type_id}"
        )

    loader = local_loader or load_local_oc_folios
    local = loader(
        company_id=int(company_id),
        office_id=int(office_id),
        from_folio=start,
        to_folio=end,
        document_type_id=int(document_type_id),
        max_range_span=max_range_span,
    )
    by_folio: dict[int, dict[str, Any]] = local["by_folio"]
    gap_info = find_numeric_gaps(
        set(by_folio.keys()),
        from_folio=start,
        to_folio=end,
        max_gaps=max_gaps,
        max_range_span=max_range_span,
    )

    findings: list[dict[str, Any]] = []
    for folio, row in sorted(by_folio.items()):
        findings.append(
            {
                "folio": folio,
                "status": STATUS_OK,
                "local_document_id": row.get("document_id"),
                "postgresql_source_document_id": row.get("source_document_id"),
                "local_state": row.get("state"),
                "wrote": False,
            }
        )

    gaps: list[int] = list(gap_info["gaps"])
    for folio in gaps:
        findings.append({"folio": folio, "status": STATUS_GAP_PENDING, "wrote": False})

    investigated = 0
    investigation_truncated = False
    if investigate:
        if client is None:
            raise ValueError("client Bsale requerido cuando investigate=True")
        investigate_limit = max(0, int(max_investigate))
        targets = gaps[:investigate_limit]
        if len(gaps) > investigate_limit:
            investigation_truncated = True
        for folio in targets:
            result = investigate_folio_in_bsale(
                client,
                folio=folio,
                company_id=int(company_id),
                office_id=int(office_id),
                document_type_id=int(document_type_id),
                local_row=None,
                search_fn=search_fn,
            )
            investigated += 1
            for idx, item in enumerate(findings):
                if item.get("folio") == folio and item.get("status") == STATUS_GAP_PENDING:
                    findings[idx] = result
                    break

        remaining = max(0, investigate_limit - investigated)
        if remaining and by_folio:
            for folio, row in sorted(by_folio.items()):
                if remaining <= 0:
                    investigation_truncated = True
                    break
                result = investigate_folio_in_bsale(
                    client,
                    folio=folio,
                    company_id=int(company_id),
                    office_id=int(office_id),
                    document_type_id=int(document_type_id),
                    local_row=row,
                    search_fn=search_fn,
                )
                investigated += 1
                remaining -= 1
                if result.get("status") in (
                    STATUS_OK,
                    STATUS_VERSION_DIFF,
                    STATUS_CANCELLED,
                    STATUS_VERIFICATION_ERROR,
                    STATUS_MISSING_IN_PG,
                    STATUS_NOT_FOUND_IN_BSALE,
                ):
                    for idx, item in enumerate(findings):
                        if item.get("folio") == folio and item.get("status") == STATUS_OK:
                            findings[idx] = {**item, **result}
                            break

    counts: dict[str, int] = {}
    for item in findings:
        status = str(item.get("status") or "unknown")
        counts[status] = counts.get(status, 0) + 1

    if any(bool(item.get("wrote")) for item in findings):
        raise RuntimeError("auditoría correlativos no debe marcar wrote=True")

    summary = {
        "status": "completed",
        "mode": "correlative_audit_readonly",
        "wrote": False,
        "investigate": bool(investigate),
        "company_id": int(company_id),
        "office_id": int(office_id),
        "document_type_id": int(document_type_id),
        "from_folio": start,
        "to_folio": end,
        "range_span": end - start + 1,
        "max_range_span": int(max_range_span),
        "max_gaps": int(max_gaps),
        "max_investigate": int(max_investigate),
        "local_folios": len(by_folio),
        "zero_number_rows": local.get("zero_number_rows") or [],
        "gaps_detected": gap_info["gap_count"],
        "gaps_truncated": bool(gap_info["truncated"]),
        "investigated": investigated,
        "investigation_truncated": investigation_truncated,
        "counts": counts,
        "findings": findings,
        "bsale_filters_documented": {
            "path": "/documents.json",
            "params": ["number", "documenttypeid", "officeid", "limit", "offset"],
            "source": "oc_source_resolver.search_documents_by_folio",
        },
        "notes": {
            "number_zero": (
                "Filas con number<=0 no entran al correlativo; se reportan en "
                "zero_number_rows. En Bsale number=0 suele indicar anulación/reemisión."
            ),
            "shared_sequences": (
                "La auditoría nunca mezcla company_id/office_id distintos; "
                "cada corrida audita una sola secuencia documental tipo 33."
            ),
            "document_id_vs_source": (
                "local document_id puede diferir de source_document_id tras reemisión; "
                "version_diff compara el source vigente de Bsale."
            ),
        },
        "started_at": started_at.isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "duration_seconds": round(time.perf_counter() - started, 3),
    }
    logger.info(
        "oc_correlative_audit company_id=%s office_id=%s range=[%s,%s] "
        "local=%s gaps=%s investigated=%s wrote=False",
        company_id,
        office_id,
        start,
        end,
        len(by_folio),
        gap_info["gap_count"],
        investigated,
    )
    return summary
