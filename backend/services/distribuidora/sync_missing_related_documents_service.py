"""
Recuperación puntual de headers faltantes referenciados en ``document_related``.

Fuente de verdad Bsale: ``GET /v1/documents/{related_document_id}.json`` (el id en
``document_related`` es el id Bsale / PK local).

Dry-run por defecto: consulta Bsale, NO escribe PG.
Apply explícito: upsert header + hijos vía ``sync_service``; materializa NC si aplica.

Rate-limit: misma estrategia que ``catchup_oc_invoice_relations`` (throttle + Retry-After).
"""

from __future__ import annotations

import copy
import logging
import os
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any

from backend.db import get_connection
from backend.repositories.distribuidora.document_related_repo import (
    fetch_credit_note_links_for_invoice,
    fetch_orphan_related_document_candidates,
    document_header_exists,
)
from backend.repositories.distribuidora.documents_repo import (
    document_dict_from_bsale,
    upsert_documents,
)
from backend.services.distribuidora.bsale_client import BsaleClient
from backend.services.distribuidora.catchup_oc_invoice_relations_service import (
    CATCHUP_DEFAULT_INTERVAL_JITTER_SEC,
    CATCHUP_DEFAULT_MIN_INTERVAL_SEC,
    CATCHUP_MAX_429_RETRIES,
    CatchupApiError,
    fetch_relateddetailid_items,
)
from backend.services.distribuidora.document_relation_sync_service import (
    DOC_TYPE_NC,
    materialize_cn_related_rows,
)
from backend.services.distribuidora.sync_related_service import (
    _detail_ids_for_document,
    _insert_related_triples,
)
from backend.services.distribuidora.sync_service import _refresh_document_children
from backend.utils.db_tx import release_transaction, safe_rollback

logger = logging.getLogger(__name__)

INVOICE_TYPES = frozenset({1, 6})

# Canario documentado (OC 68677) — referencia para tests/diagnóstico; no hardcode en lógica.
CANARY_OC_68677 = {
    "oc_document_id": 3852324,
    "oc_number": 68677,
    "origin_detail_id": 9019600,
    "related_document_id": 3853417,
    "related_document_type": 6,
    "expected_bsale_number": 50367,
    "expected_nc_number": 18408,
}


class MissingRelatedApiError(Exception):
    """Error Bsale al recuperar un related huérfano."""

    def __init__(self, message: str, *, rate_limited: bool = False, not_found: bool = False) -> None:
        super().__init__(message)
        self.rate_limited = rate_limited
        self.not_found = not_found


def _bsale_token() -> str:
    return (os.getenv("BSALE_TOKEN") or "").strip() or (os.getenv("BSALE_TOKEN_SPA") or "").strip()


def _is_rate_limit_message(msg: str) -> bool:
    m = msg.lower()
    return "429" in m or "rate limit" in m


def _is_not_found_message(msg: str) -> bool:
    return "404" in msg


def _generation_date_iso(blob: dict[str, Any]) -> str | None:
    raw = blob.get("generationDate")
    if raw is None:
        return None
    try:
        ts = int(raw)
    except (TypeError, ValueError):
        return None
    if ts <= 0:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def bsale_document_type_id(blob: dict[str, Any]) -> int | None:
    doc_type = blob.get("document_type") or blob.get("documentType") or {}
    if isinstance(doc_type, dict) and doc_type.get("id") is not None:
        try:
            return int(doc_type["id"])
        except (TypeError, ValueError):
            return None
    raw = blob.get("documentTypeId")
    if raw is not None:
        try:
            return int(raw)
        except (TypeError, ValueError):
            return None
    return None


def fetch_bsale_document_by_id(client: BsaleClient, related_document_id: int) -> dict[str, Any]:
    """``GET /documents/{related_document_id}.json`` — copia independiente."""
    path = f"/documents/{related_document_id}.json"
    try:
        raw = client.get(path)
    except RuntimeError as exc:
        msg = str(exc)
        if _is_not_found_message(msg):
            raise MissingRelatedApiError(msg, not_found=True) from exc
        raise MissingRelatedApiError(
            msg,
            rate_limited=_is_rate_limit_message(msg),
        ) from exc
    except Exception as exc:
        msg = str(exc)
        raise MissingRelatedApiError(
            msg,
            rate_limited=_is_rate_limit_message(msg),
        ) from exc

    if not isinstance(raw, dict):
        raise MissingRelatedApiError(
            f"Respuesta Bsale inválida (tipo={type(raw).__name__}) para {path}",
        )
    bsale_id = raw.get("id")
    if bsale_id is None:
        raise MissingRelatedApiError(f"Bsale documento sin id en {path}")
    try:
        if int(bsale_id) != int(related_document_id):
            raise MissingRelatedApiError(
                f"Ambigüedad: pedido id={related_document_id} devolvió id={bsale_id}",
            )
    except (TypeError, ValueError) as exc:
        raise MissingRelatedApiError(f"id Bsale no numérico: {bsale_id!r}") from exc
    return copy.deepcopy(raw)


def validate_bsale_against_candidate(
    *,
    related_document_id: int,
    expected_type: int,
    blob: dict[str, Any],
    company_id: int,
    office_id: int,
    sync_stats: dict[str, Any] | None = None,
) -> tuple[bool, str | None]:
    """
    Valida id/tipo/office/company. Retorna (ok, reason_if_not).
    """
    bsale_type = bsale_document_type_id(blob)
    if bsale_type is None:
        return False, "missing_document_type"
    if int(bsale_type) != int(expected_type):
        return False, f"type_mismatch expected={expected_type} bsale={bsale_type}"
    row = document_dict_from_bsale(
        blob,
        company_id=company_id,
        default_office_id=office_id,
        sync_stats=sync_stats,
    )
    if row is None:
        return False, "skipped_company_or_office"
    if int(row["document_id"]) != int(related_document_id):
        return False, "document_id_mismatch"
    return True, None


def discover_nc_bsale_ids_for_invoice(
    client: BsaleClient,
    cur,
    invoice_document_id: int,
    *,
    office_id: int,
    throttle: float,
) -> list[dict[str, Any]]:
    """
    Tras factura/boleta en PG, busca NC vía ``relateddetailid`` en líneas de la factura.
    Fuente: id Bsale real devuelto por API (tipo 9); sin fuzzy matching.
    """
    detail_ids = _detail_ids_for_document(cur, invoice_document_id)
    found: dict[int, dict[str, Any]] = {}
    for detail_id in detail_ids:
        try:
            items, _calls = fetch_relateddetailid_items(
                client,
                detail_id,
                office_id=office_id,
                throttle=throttle,
                log_ctx=f"[missing_related nc detail={detail_id}]",
            )
        except CatchupApiError:
            continue
        for it in items:
            if not isinstance(it, dict):
                continue
            doc_type = it.get("documentTypeId")
            if doc_type is None and isinstance(it.get("document_type"), dict):
                doc_type = it["document_type"].get("id")
            try:
                tid = int(doc_type) if doc_type is not None else None
            except (TypeError, ValueError):
                tid = None
            if tid != DOC_TYPE_NC:
                continue
            nc_id = it.get("id")
            if nc_id is None:
                continue
            try:
                nc_id_int = int(nc_id)
            except (TypeError, ValueError):
                continue
            if nc_id_int not in found:
                found[nc_id_int] = {
                    "nc_bsale_document_id": nc_id_int,
                    "nc_number": it.get("number"),
                    "via_invoice_detail_id": detail_id,
                    "invoice_document_id": invoice_document_id,
                }
    return list(found.values())


def _persist_document_from_bsale(
    client: BsaleClient,
    cur,
    conn,
    blob: dict[str, Any],
    *,
    company_id: int,
    office_id: int,
    stats: dict[str, Any],
) -> int:
    """Upsert header + refresh hijos (details, references, …)."""
    row = document_dict_from_bsale(
        blob,
        company_id=company_id,
        default_office_id=office_id,
        sync_stats=stats,
    )
    if row is None:
        raise ValueError("document_dict_from_bsale returned None after validation")
    row["_bsale_document"] = blob
    local_document_id = int(row["document_id"])
    folio = row.get("number")
    try:
        folio_int = int(folio) if folio is not None else None
    except (TypeError, ValueError):
        folio_int = None

    job = f"sync_missing_related:{local_document_id}"
    try:
        upsert_documents(cur, [row], stats)
        conn.commit()
        _refresh_document_children(
            client,
            cur,
            conn,
            local_document_id,
            row.get("document_type_id"),
            stats,
            raw_document=blob,
            folio=folio_int,
        )
        release_transaction(conn, job=job)
    except Exception:
        safe_rollback(conn, job=job)
        raise
    return local_document_id


def _materialize_cn_for_invoice(
    cur,
    *,
    company_id: int,
    office_id: int,
    invoice_document_id: int,
    dry_run: bool,
    stats: dict[str, Any],
) -> tuple[int, int]:
    """Materializa aristas invoice_detail → NC si ambos headers existen en PG."""
    links = fetch_credit_note_links_for_invoice(
        cur,
        company_id=company_id,
        office_id=office_id,
        invoice_document_id=invoice_document_id,
    )
    triples = materialize_cn_related_rows(links)
    inserted = 0
    if dry_run or not triples:
        return len(triples), inserted
    inserted = _insert_related_triples(
        cur.connection,
        cur,
        triples,
        stats=stats,
        log_ctx=f"[missing_related cn inv={invoice_document_id}]",
    )
    cur.connection.commit()
    return len(triples), inserted


@dataclass
class SyncMissingRelatedReport:
    dry_run: bool = True
    company_id: int = 3
    office_id: int = 1
    candidates: int = 0
    found_in_bsale: int = 0
    would_insert: int = 0
    already_present: int = 0
    not_found: int = 0
    api_errors: int = 0
    rate_limited: int = 0
    headers_inserted: int = 0
    cn_links_would_materialize: int = 0
    cn_links_inserted: int = 0
    derived_nc_candidates: list[dict[str, Any]] = field(default_factory=list)
    samples: list[dict[str, Any]] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    rate_stats: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def run_sync_missing_related_documents(
    *,
    company_id: int = 3,
    office_id: int = 1,
    dry_run: bool = True,
    limit: int = 500,
    offset: int = 0,
    related_document_ids: list[int] | None = None,
    throttle: float | None = None,
) -> SyncMissingRelatedReport:
    report = SyncMissingRelatedReport(
        dry_run=dry_run,
        company_id=company_id,
        office_id=office_id,
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
    report.rate_stats = rate_stats

    conn = get_connection()
    stats: dict[str, Any] = {}

    try:
        cur = conn.cursor()
        rows = fetch_orphan_related_document_candidates(
            cur,
            company_id=company_id,
            office_id=office_id,
            limit=limit,
            offset=offset,
            related_document_ids=related_document_ids,
        )
        report.candidates = len(rows)

        client = BsaleClient(
            token,
            min_interval_sec=CATCHUP_DEFAULT_MIN_INTERVAL_SEC,
            min_interval_jitter_sec=CATCHUP_DEFAULT_INTERVAL_JITTER_SEC,
            max_429_retries=CATCHUP_MAX_429_RETRIES,
            rate_stats=rate_stats,
        )

        synced_invoice_ids: list[int] = []

        for cand in rows:
            related_id = int(cand["related_document_id"])
            expected_type = int(cand["related_document_type"])
            sample: dict[str, Any] = {
                "oc_numbers": cand.get("oc_numbers") or [],
                "oc_document_ids": cand.get("oc_document_ids") or [],
                "origin_detail_ids": cand.get("origin_detail_ids") or [],
                "related_document_id": related_id,
                "expected_related_document_type": expected_type,
                "reference_count": int(cand.get("reference_count") or 0),
                "earliest_oc_emission": cand.get("earliest_oc_emission"),
                "would_insert": False,
                "already_present": False,
                "not_found": False,
                "api_error": None,
                "rate_limited": False,
                "bsale_number": None,
                "bsale_document_type": None,
                "generationDate": None,
                "state": None,
                "validation_error": None,
                "cn_links_would_materialize": 0,
                "derived_nc_candidates": [],
            }

            if document_header_exists(cur, related_id):
                report.already_present += 1
                sample["already_present"] = True
                report.samples.append(sample)
                continue

            try:
                blob = fetch_bsale_document_by_id(client, related_id)
            except MissingRelatedApiError as exc:
                if exc.not_found:
                    report.not_found += 1
                    sample["not_found"] = True
                elif exc.rate_limited:
                    report.rate_limited += 1
                    sample["rate_limited"] = True
                    sample["api_error"] = str(exc)
                    report.api_errors += 1
                else:
                    report.api_errors += 1
                    sample["api_error"] = str(exc)
                report.samples.append(sample)
                continue
            except Exception as exc:
                report.api_errors += 1
                sample["api_error"] = str(exc)
                report.samples.append(sample)
                continue

            report.found_in_bsale += 1
            sample["bsale_number"] = blob.get("number")
            sample["bsale_document_type"] = bsale_document_type_id(blob)
            sample["generationDate"] = _generation_date_iso(blob)
            sample["state"] = blob.get("state")

            ok, reason = validate_bsale_against_candidate(
                related_document_id=related_id,
                expected_type=expected_type,
                blob=blob,
                company_id=company_id,
                office_id=office_id,
                sync_stats=stats,
            )
            if not ok:
                sample["validation_error"] = reason
                report.samples.append(sample)
                continue

            sample["would_insert"] = True
            report.would_insert += 1

            if dry_run:
                if int(expected_type) in INVOICE_TYPES:
                    derived = discover_nc_bsale_ids_for_invoice(
                        client,
                        cur,
                        related_id,
                        office_id=office_id,
                        throttle=helper_throttle,
                    )
                    sample["derived_nc_candidates"] = derived
                    report.derived_nc_candidates.extend(derived)
                    would_cn, _ins = _materialize_cn_for_invoice(
                        cur,
                        company_id=company_id,
                        office_id=office_id,
                        invoice_document_id=related_id,
                        dry_run=True,
                        stats=stats,
                    )
                    sample["cn_links_would_materialize"] = would_cn
                    report.cn_links_would_materialize += would_cn
                report.samples.append(sample)
                continue

            # APPLY: upsert header + hijos
            try:
                _persist_document_from_bsale(
                    client,
                    cur,
                    conn,
                    blob,
                    company_id=company_id,
                    office_id=office_id,
                    stats=stats,
                )
                report.headers_inserted += 1
                if int(expected_type) in INVOICE_TYPES:
                    synced_invoice_ids.append(related_id)
            except Exception as exc:
                report.errors.append(f"insert {related_id}: {exc}")
                sample["api_error"] = str(exc)
                report.samples.append(sample)
                continue

            if int(expected_type) in INVOICE_TYPES:
                would_cn, inserted_cn = _materialize_cn_for_invoice(
                    cur,
                    company_id=company_id,
                    office_id=office_id,
                    invoice_document_id=related_id,
                    dry_run=False,
                    stats=stats,
                )
                sample["cn_links_would_materialize"] = would_cn
                report.cn_links_would_materialize += would_cn
                report.cn_links_inserted += inserted_cn

                derived = discover_nc_bsale_ids_for_invoice(
                    client,
                    cur,
                    related_id,
                    office_id=office_id,
                    throttle=helper_throttle,
                )
                sample["derived_nc_candidates"] = derived
                for nc_cand in derived:
                    nc_id = int(nc_cand["nc_bsale_document_id"])
                    if document_header_exists(cur, nc_id):
                        continue
                    try:
                        nc_blob = fetch_bsale_document_by_id(client, nc_id)
                    except MissingRelatedApiError:
                        continue
                    nc_ok, _ = validate_bsale_against_candidate(
                        related_document_id=nc_id,
                        expected_type=DOC_TYPE_NC,
                        blob=nc_blob,
                        company_id=company_id,
                        office_id=office_id,
                        sync_stats=stats,
                    )
                    if not nc_ok:
                        continue
                    try:
                        _persist_document_from_bsale(
                            client,
                            cur,
                            conn,
                            nc_blob,
                            company_id=company_id,
                            office_id=office_id,
                            stats=stats,
                        )
                        report.headers_inserted += 1
                        _materialize_cn_for_invoice(
                            cur,
                            company_id=company_id,
                            office_id=office_id,
                            invoice_document_id=related_id,
                            dry_run=False,
                            stats=stats,
                        )
                    except Exception as exc:
                        report.errors.append(f"insert nc {nc_id}: {exc}")

            report.samples.append(sample)

    finally:
        try:
            conn.close()
        except Exception:
            pass

    return report
