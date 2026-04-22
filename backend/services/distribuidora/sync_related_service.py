"""
Sync incremental de relaciones OC → otros documentos vía
``GET /v1/documents/{document_id}/references.json`` (no ``relateddetailid`` ni refs embebidas).

Escribe ``distribuidora.document_related`` con deduplicación.

Incluye ``sync_related_documents_range`` para rellenar histórico por rango de emisión (día a día).
"""

from __future__ import annotations

import logging
import os
import random
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, TypeVar

import psycopg2.errors
from psycopg2.extensions import connection as PgConnection

from backend.db import get_connection
from backend.repositories.distribuidora.sync_repo import (
    ensure_distribuidora_schema,
    insert_sync_status_row,
)
from backend.services.distribuidora.bsale_client import BsaleClient

logger = logging.getLogger(__name__)

# Lock exclusivo global para **todo** trabajo sobre ``document_related`` (incremental + rango).
# Misma sesión: try_advisory_lock al abrir conexión, unlock en ``finally`` antes de ``close``.
ADVISORY_LOCK_RELATED = 5_927_184_005

DEADLOCK_MAX_ATTEMPTS = 5
DEADLOCK_SLEEP_SEC = 2.5

T = TypeVar("T")
COMPANY_ID = 3
OFFICE_ID = 1
DOC_TYPE_OC = 33


def _utc_day_emission_bounds(d: date) -> tuple[datetime, datetime]:
    """Inicio UTC del día y fin exclusivo (``[start, end)``) para filtrar ``emission_date``."""
    start = datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=timezone.utc)
    end_excl = start + timedelta(days=1)
    return start, end_excl


def _safe_int(v: Any) -> int | None:
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _related_item_office_id(it: dict[str, Any]) -> int | None:
    off = it.get("office") or it.get("Office") or {}
    if not isinstance(off, dict):
        return None
    return _safe_int(off.get("id"))


def _bsale_token() -> str:
    return (os.getenv("BSALE_TOKEN") or "").strip() or (os.getenv("BSALE_TOKEN_SPA") or "").strip()


def _with_deadlock_retry(conn: PgConnection, label: str, fn: Callable[[], T]) -> T:
    """Ejecuta ``fn`` (debe hacer ``commit`` o solo lecturas acotadas) y reintenta ante deadlock."""
    for attempt in range(1, DEADLOCK_MAX_ATTEMPTS + 1):
        try:
            return fn()
        except psycopg2.errors.DeadlockDetected:
            logger.warning(
                "DeadlockDetected (%s) intento %s/%s — rollback y reintento",
                label,
                attempt,
                DEADLOCK_MAX_ATTEMPTS,
            )
            try:
                conn.rollback()
            except Exception:
                logger.exception("rollback tras deadlock (%s)", label)
            if attempt >= DEADLOCK_MAX_ATTEMPTS:
                raise
            time.sleep(DEADLOCK_SLEEP_SEC + random.uniform(0, 0.5))
    raise RuntimeError("unreachable")


def _fetch_oc_document_ids(
    cur,
    *,
    lookback_days: int,
    limit_documents: int,
) -> list[int]:
    cur.execute(
        """
        SELECT DISTINCT d.document_id
        FROM distribuidora.documents d
        WHERE d.document_type_id = %s
          AND d.company_id = %s
          AND d.office_id = %s
          AND d.emission_date >= (NOW() AT TIME ZONE 'UTC' - (%s * interval '1 day'))
        ORDER BY d.document_id DESC
        LIMIT %s
        """,
        (DOC_TYPE_OC, COMPANY_ID, OFFICE_ID, max(1, lookback_days), limit_documents),
    )
    return [int(r[0]) for r in cur.fetchall()]


def _fetch_oc_document_ids_for_emission_day(cur, day: date) -> list[int]:
    """``document_id`` distintos de OC (tipo 33) cuya emisión cae ese día calendario (UTC)."""
    day_start, day_end_excl = _utc_day_emission_bounds(day)
    cur.execute(
        """
        SELECT DISTINCT d.document_id
        FROM distribuidora.documents d
        WHERE d.document_type_id = %s
          AND d.company_id = %s
          AND d.office_id = %s
          AND d.emission_date IS NOT NULL
          AND d.emission_date >= %s
          AND d.emission_date < %s
        ORDER BY d.document_id
        """,
        (DOC_TYPE_OC, COMPANY_ID, OFFICE_ID, day_start, day_end_excl),
    )
    return [int(r[0]) for r in cur.fetchall()]


def _detail_ids_for_document(cur, document_id: int) -> list[int]:
    cur.execute(
        """
        SELECT detail_id
        FROM distribuidora.document_details
        WHERE document_id = %s
        ORDER BY detail_id
        """,
        (document_id,),
    )
    return [int(r[0]) for r in cur.fetchall()]


def _related_document_from_reference_item(it: dict[str, Any]) -> dict[str, Any] | None:
    """El documento relacionado suele ir anidado en ``item['document']`` (no usar ``item['id']`` como doc)."""
    doc = it.get("document")
    if isinstance(doc, dict) and _safe_int(doc.get("id")) is not None:
        return doc
    if _safe_int(it.get("id")) is not None and (
        it.get("documentType") is not None or it.get("document_type") is not None
    ):
        return it
    return None


def _document_type_id_from_doc(doc: dict[str, Any]) -> int | None:
    dt = doc.get("documentType") or doc.get("document_type")
    if isinstance(dt, dict):
        tid = _safe_int(dt.get("id"))
        if tid is not None:
            return tid
    if isinstance(dt, int):
        return dt
    return _safe_int(doc.get("document_type_id") or doc.get("documentTypeId"))


def _detail_id_from_reference_item(it: dict[str, Any]) -> int | None:
    det = it.get("detail") or it.get("Detail")
    if isinstance(det, dict):
        did = _safe_int(det.get("id"))
        if did is not None:
            return did
    return _safe_int(
        it.get("detailId")
        or it.get("detail_id")
        or it.get("relatedDetailId")
        or it.get("related_detail_id")
    )


def _reference_items_to_related_triples(
    source_document_id: int,
    items: list[Any],
    valid_detail_ids: set[int],
    *,
    fallback_single_detail_id: int | None,
    stats: dict[str, Any] | None,
) -> list[tuple[int, int, int]]:
    """``(detail_id, related_document_id, related_document_type_id)`` listos para insertar."""
    out: list[tuple[int, int, int]] = []
    if not valid_detail_ids:
        logger.warning(
            "references sin líneas locales: source_document_id=%s (document_details vacío)",
            source_document_id,
        )
        return out

    for it in items:
        if not isinstance(it, dict):
            continue
        doc = _related_document_from_reference_item(it)
        if not doc:
            continue
        rid = _safe_int(doc.get("id"))
        if rid is None:
            continue
        roff = _related_item_office_id(doc)
        if roff is None or roff != OFFICE_ID:
            if stats is not None:
                stats["related_skipped_other_office"] = (
                    int(stats.get("related_skipped_other_office") or 0) + 1
                )
            logger.info(
                "Relación omitida por office distinta: related_document_id=%s office=%s "
                "(esperado office_id=%s) source_document_id=%s",
                rid,
                roff,
                OFFICE_ID,
                source_document_id,
            )
            continue
        tid = _document_type_id_from_doc(doc)
        if tid is None:
            continue

        detail_id = _detail_id_from_reference_item(it)
        if detail_id is None and fallback_single_detail_id is not None:
            detail_id = fallback_single_detail_id
        if detail_id is None:
            logger.warning(
                "references item sin detail_id (OC multilínea?): source_document_id=%s "
                "related_document_id=%s keys=%s",
                source_document_id,
                rid,
                sorted(it.keys()),
            )
            continue
        if detail_id not in valid_detail_ids:
            logger.warning(
                "references detail_id no pertenece al documento: source_document_id=%s "
                "detail_id=%s related_document_id=%s",
                source_document_id,
                detail_id,
                rid,
            )
            continue
        out.append((detail_id, rid, tid))
    return out


def _insert_related_triples(
    conn: PgConnection,
    cur,
    triples: list[tuple[int, int, int]],
    *,
    stats: dict[str, Any] | None = None,
) -> int:
    """Inserta relaciones con ``ON CONFLICT DO NOTHING``; un commit por fila + reintento deadlock."""
    n = 0
    for detail_id, rid, tid in triples:

        def _insert_one(
            _did: int = detail_id,
            _rid: int = rid,
            _tid: int = tid,
        ) -> int:
            cur.execute(
                """
                INSERT INTO distribuidora.document_related (
                    detail_id, related_document_id, related_document_type
                )
                VALUES (%s, %s, %s)
                ON CONFLICT (detail_id, related_document_id) DO NOTHING
                """,
                (_did, _rid, _tid),
            )
            rc = int(cur.rowcount or 0)
            conn.commit()
            return rc

        inserted = _with_deadlock_retry(
            conn,
            f"document_related detail_id={detail_id} related_document_id={rid}",
            _insert_one,
        )
        n += inserted
    return n


def _fetch_and_persist_related_for_document(
    client: BsaleClient,
    conn: PgConnection,
    cur,
    document_id: int,
    *,
    throttle: float,
    stats: dict[str, Any] | None = None,
) -> tuple[int, int, int]:
    """
    ``GET /documents/{document_id}/references.json``; persiste en ``document_related``.

    Retorna ``(n_items_api, filas_insertadas, llamadas_http)``.
    """
    try:
        data = client.get(f"/documents/{document_id}/references.json")
    except Exception as e:
        logger.warning("references.json document_id=%s: %s", document_id, e)
        return 0, 0, 0

    items = data.get("items")
    if items is None:
        items = data.get("references") or []
    if not isinstance(items, list):
        items = []

    n_items = len(items)
    if stats is not None:
        stats["references_items_total"] = int(stats.get("references_items_total") or 0) + n_items

    detail_list = _detail_ids_for_document(cur, document_id)
    valid = set(detail_list)
    fallback = detail_list[0] if len(detail_list) == 1 else None

    triples = _reference_items_to_related_triples(
        document_id,
        items,
        valid,
        fallback_single_detail_id=fallback,
        stats=stats,
    )
    inserted = _insert_related_triples(conn, cur, triples, stats=stats)

    logger.info(
        "document_id=%s references.json items=%s parseadas=%s insertadas=%s",
        document_id,
        n_items,
        len(triples),
        inserted,
    )

    if throttle > 0:
        time.sleep(throttle)
    return n_items, inserted, 1


def sync_distribuidora_related_documents(
    *,
    strict_token: bool = False,
    lookback_days: int | None = None,
    limit_details: int | None = None,
    limit_documents: int | None = None,
) -> dict[str, Any]:
    """
    Para cada ``document_id`` de OC reciente, ``GET …/documents/{id}/references.json`` y persiste relaciones.

    Env:
      DISTRIBUIDORA_RELATED_LOOKBACK_DAYS (default 7)
      DISTRIBUIDORA_RELATED_DETAIL_LIMIT (default 250) — límite de **documentos** OC a procesar
    """
    token = _bsale_token()
    if not token:
        if strict_token:
            raise ValueError("Ningún token Bsale: defina BSALE_TOKEN o BSALE_TOKEN_SPA.")
        return {"skipped": True, "skip_reason": "sin token", "inserted": 0}

    lb = lookback_days if lookback_days is not None else int(os.getenv("DISTRIBUIDORA_RELATED_LOOKBACK_DAYS", "7"))
    lim_src = limit_documents if limit_documents is not None else limit_details
    lim = lim_src if lim_src is not None else int(os.getenv("DISTRIBUIDORA_RELATED_DETAIL_LIMIT", "250"))

    t0 = time.perf_counter()
    stats: dict[str, Any] = {
        "documents_considered": 0,
        "details_considered": 0,
        "references_items_total": 0,
        "rows_inserted": 0,
        "api_calls": 0,
        "related_skipped_other_office": 0,
        "duration_seconds": 0.0,
        "skipped": False,
        "omitido_concurrencia": False,
        "errors": None,
    }

    conn = get_connection()
    got_lock = False
    try:
        cur = conn.cursor()
        cur.execute("SELECT pg_try_advisory_lock(%s)", (ADVISORY_LOCK_RELATED,))
        got_lock = bool(cur.fetchone()[0])
        if not got_lock:
            stats["omitido_concurrencia"] = True
            cur.close()
            stats["duration_seconds"] = round(time.perf_counter() - t0, 3)
            return stats

        ensure_distribuidora_schema(cur)
        conn.commit()

        logger.info(
            "sync related distribuidora: office_id=%s; GET references.json por documento OC "
            "(company_id=%s office_id=%s)",
            OFFICE_ID,
            COMPANY_ID,
            OFFICE_ID,
        )

        document_ids = _fetch_oc_document_ids(cur, lookback_days=lb, limit_documents=lim)
        stats["documents_considered"] = len(document_ids)
        stats["details_considered"] = stats["documents_considered"]

        client = BsaleClient(token)
        throttle = float(os.getenv("DISTRIBUIDORA_RELATED_API_DELAY_SEC", "0.12"))

        for doc_id in document_ids:
            try:
                n_items, ins, calls = _fetch_and_persist_related_for_document(
                    client, conn, cur, doc_id, throttle=throttle, stats=stats
                )
            except Exception as e:
                logger.warning("references.json document_id=%s: %s", doc_id, e)
                continue
            stats["api_calls"] += calls
            stats["rows_inserted"] += ins
            logger.debug(
                "related doc_id=%s items=%s inserted=%s",
                doc_id,
                n_items,
                ins,
            )

        def _finalize_incremental() -> None:
            insert_sync_status_row(
                cur,
                sync_type="related",
                records_processed=int(stats["rows_inserted"]),
                status="success",
            )
            conn.commit()

        _with_deadlock_retry(conn, "related incremental insert_sync_status", _finalize_incremental)
        cur.close()
        logger.info(
            "sync related OK: documents=%s references_items=%s inserted=%s "
            "omitidas otra office=%s api=%s s=%.2f",
            stats["documents_considered"],
            stats.get("references_items_total", 0),
            stats["rows_inserted"],
            stats.get("related_skipped_other_office", 0),
            stats["api_calls"],
            time.perf_counter() - t0,
        )
    except Exception as e:
        logger.exception("sync related: %s", e)
        stats["errors"] = str(e)
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        try:
            if got_lock:
                c2 = conn.cursor()
                c2.execute("SELECT pg_advisory_unlock(%s)", (ADVISORY_LOCK_RELATED,))
                c2.close()
        except Exception:
            logger.exception("advisory unlock related")
        try:
            conn.close()
        except Exception:
            pass

    stats["duration_seconds"] = round(time.perf_counter() - t0, 3)
    return stats


def sync_related_documents_range(
    *,
    start_date: date,
    end_date: date,
    strict_token: bool = True,
) -> dict[str, Any]:
    """
    Rellena ``distribuidora.document_related`` para OC (tipo 33) con emisión entre ``start_date`` y ``end_date``
    (inclusive, días calendario UTC), recorriendo **día a día**.

    Por cada ``document_id`` de esas OC: GET ``/documents/{id}/references.json``.
    """
    if start_date > end_date:
        start_date, end_date = end_date, start_date

    token = _bsale_token()
    if not token:
        if strict_token:
            raise ValueError("Ningún token Bsale: defina BSALE_TOKEN o BSALE_TOKEN_SPA.")
        return {"skipped": True, "skip_reason": "sin token"}

    t0 = time.perf_counter()
    stats: dict[str, Any] = {
        "mode": "related_range",
        "days_processed": 0,
        "documents_processed": 0,
        "details_processed": 0,
        "rows_inserted": 0,
        "api_calls": 0,
        "relations_found": 0,
        "references_items_total": 0,
        "related_skipped_other_office": 0,
        "duration_seconds": 0.0,
        "omitido_concurrencia": False,
        "errors": None,
    }

    conn = get_connection()
    got_lock = False
    try:
        cur = conn.cursor()
        cur.execute("SELECT pg_try_advisory_lock(%s)", (ADVISORY_LOCK_RELATED,))
        got_lock = bool(cur.fetchone()[0])
        if not got_lock:
            stats["omitido_concurrencia"] = True
            cur.close()
            stats["duration_seconds"] = round(time.perf_counter() - t0, 3)
            return stats

        ensure_distribuidora_schema(cur)
        conn.commit()

        logger.info(
            "sync related rango: solo office_id=%s (Bsale officeId=%s); OC company_id=%s office_id=%s",
            OFFICE_ID,
            OFFICE_ID,
            COMPANY_ID,
            OFFICE_ID,
        )

        client = BsaleClient(token)
        throttle = float(os.getenv("DISTRIBUIDORA_RELATED_API_DELAY_SEC", "0.12"))

        current = start_date
        while current <= end_date:
            logger.info("Procesando related día: %s", current.isoformat())
            document_ids = _fetch_oc_document_ids_for_emission_day(cur, current)
            for doc_id in document_ids:
                logger.info("Documento OC procesado (references.json): %s", doc_id)
                items_total, ins, calls = _fetch_and_persist_related_for_document(
                    client, conn, cur, doc_id, throttle=throttle, stats=stats
                )
                stats["api_calls"] += calls
                stats["documents_processed"] += 1
                stats["details_processed"] = stats["documents_processed"]
                stats["rows_inserted"] += ins
                stats["relations_found"] += items_total
                logger.info(
                    "document_id=%s references items=%s insertadas=%s",
                    doc_id,
                    items_total,
                    ins,
                )

            stats["days_processed"] += 1
            current += timedelta(days=1)

        def _finalize_range() -> None:
            insert_sync_status_row(
                cur,
                sync_type="related",
                records_processed=int(stats["rows_inserted"]),
                status="success",
            )
            conn.commit()

        _with_deadlock_retry(conn, "related range insert_sync_status", _finalize_range)
        cur.close()
        logger.info(
            "sync related range OK: days=%s documents=%s references_items=%s inserted=%s "
            "omitidas otra office=%s s=%.2f",
            stats["days_processed"],
            stats["documents_processed"],
            stats.get("references_items_total", 0),
            stats["rows_inserted"],
            stats.get("related_skipped_other_office", 0),
            time.perf_counter() - t0,
        )
    except Exception as e:
        logger.exception("sync related range: %s", e)
        stats["errors"] = str(e)
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        try:
            if got_lock:
                c2 = conn.cursor()
                c2.execute("SELECT pg_advisory_unlock(%s)", (ADVISORY_LOCK_RELATED,))
                c2.close()
        except Exception:
            logger.exception("advisory unlock related range")
        try:
            conn.close()
        except Exception:
            pass

    stats["duration_seconds"] = round(time.perf_counter() - t0, 3)
    return stats


def _parse_iso_to_date(s: str) -> date:
    t = s.strip()
    if len(t) == 10 and t[4] == "-" and t[7] == "-":
        return datetime.strptime(t, "%Y-%m-%d").date()
    dt = datetime.fromisoformat(t.replace("Z", "+00:00"))
    return dt.astimezone(timezone.utc).date()


def run_resync_related_range_background(start_date_iso: str, end_date_iso: str) -> None:
    try:
        sd = _parse_iso_to_date(start_date_iso)
        ed = _parse_iso_to_date(end_date_iso)
        sync_related_documents_range(start_date=sd, end_date=ed, strict_token=True)
    except Exception:
        logger.exception("resync related range background")


def run_sync_distribuidora_related_background() -> None:
    try:
        sync_distribuidora_related_documents(strict_token=True)
    except Exception:
        logger.exception("related sync background")
