"""Reconciliación de OCs por folio y source Bsale vigente."""

from __future__ import annotations

import logging
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator

from backend.db import get_connection
from backend.repositories.distribuidora.details_repo import replace_document_details
from backend.repositories.distribuidora.documents_repo import (
    document_dict_from_bsale,
    upsert_documents,
)
from backend.services.distribuidora.bsale_client import BsaleClient
from backend.services.distribuidora.bsale_params import merge_bsale_office_query
from backend.services.distribuidora.oc_source_resolver import (
    COMPANY_ID,
    OC_DOCUMENT_TYPE_ID,
    OFFICE_ID,
    PAGE_LIMIT,
    compute_oc_source_hash,
    discover_oc_sources,
    fetch_all_document_details,
    select_active_oc_source,
    source_updated_at,
    summarize_bsale_document,
)
from backend.services.order_weight_service import (
    calculate_order_weight,
    recalculate_order_weight_in_transaction,
)

logger = logging.getLogger(__name__)
ADVISORY_LOCK_OC_RECONCILIATION = 5_927_184_013
# Debe competir con los tres writers existentes de encabezado/detalles.
ADVISORY_LOCK_OC_WRITERS = (
    5_927_184_003,  # sync_service.ADVISORY_LOCK_KEY
    5_927_184_010,  # live_sync_service.ADVISORY_LOCK_DOCUMENTS_LIVE
    5_927_184_011,  # live_sync_service.ADVISORY_LOCK_DETAILS_LIVE
)

SOURCE_METADATA_COLUMNS = frozenset(
    {"source_document_id", "source_hash", "source_updated_at", "last_synced_at"}
)


def _num(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _equal(a: Any, b: Any, *, tolerance: float = 0.01) -> bool:
    na, nb = _num(a), _num(b)
    if na is not None and nb is not None:
        return abs(na - nb) < tolerance
    return a == b


def _line_from_pg(row: tuple[Any, ...]) -> dict[str, Any]:
    return {
        "detail_id": int(row[0]),
        "variant_id": int(row[1]) if row[1] is not None else None,
        "quantity": _num(row[2]),
        "total_amount": _num(row[3]),
    }


def _line_from_bsale(item: dict[str, Any]) -> dict[str, Any]:
    variant = item.get("variant") if isinstance(item.get("variant"), dict) else {}
    return {
        "detail_id": int(item["id"]) if item.get("id") is not None else None,
        "variant_id": (
            int(variant["id"]) if variant.get("id") is not None else None
        ),
        "quantity": _num(item.get("quantity")),
        "total_amount": _num(item.get("totalAmount")),
    }


def compare_oc_state(
    *,
    pg_document: dict[str, Any] | None,
    pg_details: list[dict[str, Any]],
    bsale_document: dict[str, Any],
    bsale_details: list[dict[str, Any]],
) -> dict[str, Any]:
    """Diff operacional por encabezado y líneas."""
    bsale_summary = summarize_bsale_document(bsale_document)
    header_fields = (
        ("number", pg_document.get("number") if pg_document else None, bsale_summary["number"]),
        (
            "total_amount",
            pg_document.get("total_amount") if pg_document else None,
            bsale_document.get("totalAmount"),
        ),
        (
            "net_amount",
            pg_document.get("net_amount") if pg_document else None,
            bsale_document.get("netAmount"),
        ),
        (
            "tax_amount",
            pg_document.get("tax_amount") if pg_document else None,
            bsale_document.get("taxAmount"),
        ),
        ("state", pg_document.get("state") if pg_document else None, bsale_summary["state"]),
        (
            "commercial_state",
            pg_document.get("commercial_state") if pg_document else None,
            bsale_summary["commercialState"],
        ),
    )
    header_diff = [
        {
            "field": field,
            "postgresql": pg_value,
            "bsale": bsale_value,
            "matches": _equal(pg_value, bsale_value),
        }
        for field, pg_value, bsale_value in header_fields
    ]

    pg_by_id = {
        int(line["detail_id"]): line
        for line in pg_details
        if line.get("detail_id") is not None
    }
    bsale_lines = [_line_from_bsale(item) for item in bsale_details]
    bsale_by_id = {
        int(line["detail_id"]): line
        for line in bsale_lines
        if line.get("detail_id") is not None
    }
    only_pg = sorted(set(pg_by_id) - set(bsale_by_id))
    only_bsale = sorted(set(bsale_by_id) - set(pg_by_id))
    line_diff: list[dict[str, Any]] = []
    for detail_id in sorted(set(pg_by_id) & set(bsale_by_id)):
        p = pg_by_id[detail_id]
        b = bsale_by_id[detail_id]
        for field in ("variant_id", "quantity", "total_amount"):
            if not _equal(p.get(field), b.get(field), tolerance=0.0001):
                line_diff.append(
                    {
                        "detail_id": detail_id,
                        "field": field,
                        "postgresql": p.get(field),
                        "bsale": b.get(field),
                        "matches": False,
                    }
                )

    pg_qty = sum(_num(line.get("quantity")) or 0 for line in pg_details)
    bsale_qty = sum(_num(line.get("quantity")) or 0 for line in bsale_lines)
    pg_total = sum(_num(line.get("total_amount")) or 0 for line in pg_details)
    bsale_total = sum(_num(line.get("total_amount")) or 0 for line in bsale_lines)
    matches = (
        pg_document is not None
        and all(item["matches"] for item in header_diff)
        and not only_pg
        and not only_bsale
        and not line_diff
        and len(pg_details) == len(bsale_lines)
        and _equal(pg_qty, bsale_qty)
        and _equal(pg_total, bsale_total)
    )
    return {
        "matches": matches,
        "header": header_diff,
        "lines": line_diff,
        "only_in_postgresql_detail_ids": only_pg,
        "only_in_bsale_detail_ids": only_bsale,
        "postgresql_quantity": pg_qty,
        "bsale_quantity": bsale_qty,
        "postgresql_line_total": pg_total,
        "bsale_line_total": bsale_total,
        "postgresql_lines": pg_details,
        "bsale_lines": bsale_lines,
    }


def _load_local_oc(
    *,
    folio: int | None,
    local_document_id: int | None,
) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        if local_document_id is not None:
            cur.execute(
                """
                SELECT document_id, number, company_id, office_id, document_type_id,
                       total_amount, net_amount, tax_amount, state, commercial_state,
                       generation_date, raw_data,
                       NULLIF(to_jsonb(d)->>'source_document_id', '')::bigint
                           AS source_document_id,
                       to_jsonb(d)->>'source_hash' AS source_hash,
                       NULLIF(to_jsonb(d)->>'source_updated_at', '')::timestamptz
                           AS source_updated_at,
                       NULLIF(to_jsonb(d)->>'last_synced_at', '')::timestamptz
                           AS last_synced_at
                FROM distribuidora.documents d
                WHERE document_id = %s
                LIMIT 1
                """,
                (int(local_document_id),),
            )
        else:
            cur.execute(
                """
                SELECT document_id, number, company_id, office_id, document_type_id,
                       total_amount, net_amount, tax_amount, state, commercial_state,
                       generation_date, raw_data,
                       NULLIF(to_jsonb(d)->>'source_document_id', '')::bigint
                           AS source_document_id,
                       to_jsonb(d)->>'source_hash' AS source_hash,
                       NULLIF(to_jsonb(d)->>'source_updated_at', '')::timestamptz
                           AS source_updated_at,
                       NULLIF(to_jsonb(d)->>'last_synced_at', '')::timestamptz
                           AS last_synced_at
                FROM distribuidora.documents d
                WHERE company_id = %s AND office_id = %s
                  AND document_type_id = %s AND number = %s
                LIMIT 1
                """,
                (COMPANY_ID, OFFICE_ID, OC_DOCUMENT_TYPE_ID, int(folio)),
            )
        row = cur.fetchone()
        if not row:
            cur.close()
            return None, []
        columns = [desc[0] for desc in cur.description]
        document = dict(zip(columns, row))
        cur.execute(
            """
            SELECT detail_id, variant_id, quantity, total_amount
            FROM distribuidora.document_details
            WHERE document_id = %s
            ORDER BY line_number NULLS LAST, detail_id
            """,
            (int(document["document_id"]),),
        )
        details = [_line_from_pg(item) for item in cur.fetchall()]
        cur.close()
        return document, details
    finally:
        conn.close()


def _known_source_ids(document: dict[str, Any] | None) -> list[int]:
    if not document:
        return []
    values: list[Any] = [
        document.get("document_id"),
        document.get("source_document_id"),
    ]
    raw = document.get("raw_data")
    if isinstance(raw, dict):
        values.append(raw.get("id"))
    output: list[int] = []
    for value in values:
        try:
            source_id = int(value)
        except (TypeError, ValueError):
            continue
        if source_id > 0 and source_id not in output:
            output.append(source_id)
    return output


def _projected_weight(
    *,
    local_document_id: int | None,
    pg_details: list[dict[str, Any]],
    bsale_details: list[dict[str, Any]],
) -> dict[str, Any]:
    before: dict[str, Any] = {}
    if local_document_id is not None:
        try:
            before = calculate_order_weight(
                int(local_document_id),
                company_id=COMPANY_ID,
                office_id=OFFICE_ID,
                persist_cache=False,
            )
        except Exception:
            logger.exception("No se pudo calcular peso read-only de OC %s", local_document_id)
    unit_by_variant: dict[int, float] = {}
    for line in before.get("lines") or []:
        variant_id = line.get("variant_id")
        unit = _num(line.get("peso_unitario_kg"))
        if variant_id is not None and unit is not None:
            unit_by_variant[int(variant_id)] = unit
    projected = 0.0
    unresolved = 0
    for item in bsale_details:
        line = _line_from_bsale(item)
        variant_id = line.get("variant_id")
        unit = unit_by_variant.get(int(variant_id)) if variant_id is not None else None
        if unit is None:
            unresolved += 1
            continue
        projected += (_num(line.get("quantity")) or 0) * unit
    return {
        "before_kg": _num(before.get("peso_total_kg")),
        "after_projected_kg": round(projected, 3),
        "projected_unresolved_lines": unresolved,
    }


def _assert_source_schema(cur) -> None:
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'distribuidora'
          AND table_name = 'documents'
          AND column_name = ANY(%s::text[])
        """,
        (list(SOURCE_METADATA_COLUMNS),),
    )
    found = {row[0] for row in cur.fetchall()}
    missing = SOURCE_METADATA_COLUMNS - found
    if missing:
        raise RuntimeError(
            "Migración 044 pendiente; faltan columnas: " + ", ".join(sorted(missing))
        )


@contextmanager
def _oc_writer_locks() -> Iterator[None]:
    """Adquiere en una sesión los locks de todos los writers de OCs."""
    conn = get_connection()
    acquired: list[int] = []
    try:
        cur = conn.cursor()
        for lock_key in ADVISORY_LOCK_OC_WRITERS:
            cur.execute("SELECT pg_try_advisory_lock(%s)", (lock_key,))
            if not bool(cur.fetchone()[0]):
                raise RuntimeError(
                    "Otro sync de documentos/detalles está ejecutándose; reintente"
                )
            acquired.append(lock_key)
        conn.commit()
        cur.close()
        yield
    finally:
        for lock_key in reversed(acquired):
            try:
                cur = conn.cursor()
                cur.execute("SELECT pg_advisory_unlock(%s)", (lock_key,))
                cur.close()
            except Exception:
                logger.exception("No se pudo liberar advisory lock %s", lock_key)
        conn.close()


def _reconcile_one_oc(
    client: BsaleClient,
    *,
    folio: int | None = None,
    local_document_id: int | None = None,
    dry_run: bool = True,
    active_document: dict[str, Any] | None = None,
    user_email: str | None = None,
) -> dict[str, Any]:
    """Descubre source vigente, compara y opcionalmente persiste una OC."""
    if folio is None and local_document_id is None:
        raise ValueError("Indique folio o local_document_id")
    pg_document, pg_details = _load_local_oc(
        folio=folio,
        local_document_id=local_document_id,
    )
    if local_document_id is not None:
        if pg_document is None:
            raise ValueError(
                f"local_document_id={local_document_id} no existe en PostgreSQL"
            )
        if int(pg_document["document_id"]) != int(local_document_id):
            raise ValueError(
                "La PK local resuelta no coincide con document_id solicitado"
            )
    resolved_folio = int(folio or (pg_document or {}).get("number") or 0)
    if resolved_folio <= 0:
        raise ValueError("No se pudo resolver el folio local")

    if active_document is None:
        discovery = discover_oc_sources(
            client,
            folio=resolved_folio,
            known_source_ids=_known_source_ids(pg_document),
            company_id=COMPANY_ID,
            office_id=OFFICE_ID,
            document_type_id=OC_DOCUMENT_TYPE_ID,
        )
        selected = discovery.get("active_document")
    else:
        selected = active_document
        _, evaluated = select_active_oc_source(
            [active_document],
            folio=resolved_folio,
            company_id=COMPANY_ID,
            office_id=OFFICE_ID,
            document_type_id=OC_DOCUMENT_TYPE_ID,
        )
        discovery = {
            "folio": resolved_folio,
            "source": "reconciliation_window",
            "documents": evaluated,
            "active_document": active_document,
            "active_source_document_id": summarize_bsale_document(active_document).get("id"),
        }
    if not isinstance(selected, dict):
        return {
            "status": "source_not_found",
            "dry_run": dry_run,
            "folio": resolved_folio,
            "source_discovery": discovery,
            "wrote": False,
        }

    selected_summary = summarize_bsale_document(selected)
    source_id = int(selected_summary["id"])
    # Defensa final, aun si el caller entregó ``active_document``.
    reselected, evaluated = select_active_oc_source(
        [selected],
        folio=resolved_folio,
        company_id=COMPANY_ID,
        office_id=OFFICE_ID,
        document_type_id=OC_DOCUMENT_TYPE_ID,
    )
    if reselected is None:
        raise ValueError(f"Source Bsale no elegible: {evaluated}")

    known_ids_for_log = _known_source_ids(pg_document)
    logger.info(
        "reconcile_oc_selected folio=%s local_document_id=%s "
        "previous_source_document_id=%s current_bsale_source_document_id=%s",
        resolved_folio,
        (pg_document or {}).get("document_id"),
        (pg_document or {}).get("source_document_id")
        or (known_ids_for_log[-1] if known_ids_for_log else None),
        source_id,
    )
    details = fetch_all_document_details(client, source_id)
    digest = compute_oc_source_hash(selected, details)
    diff = compare_oc_state(
        pg_document=pg_document,
        pg_details=pg_details,
        bsale_document=selected,
        bsale_details=details,
    )
    stored_hash = (pg_document or {}).get("source_hash")
    hash_matches = stored_hash == digest and stored_hash is not None
    local_id = int((pg_document or {}).get("document_id") or 0) or None
    weight = _projected_weight(
        local_document_id=local_id,
        pg_details=pg_details,
        bsale_details=details,
    )
    raw_pg = (pg_document or {}).get("raw_data")
    raw_pg_id = raw_pg.get("id") if isinstance(raw_pg, dict) else None
    previous_source_raw = (
        (pg_document or {}).get("source_document_id")
        or raw_pg_id
        or local_id
    )
    try:
        previous_source_id = (
            int(previous_source_raw) if previous_source_raw is not None else None
        )
    except (TypeError, ValueError):
        previous_source_id = None
    discovery_report = {
        key: value for key, value in discovery.items() if key != "active_document"
    }
    discovery_report["active_document"] = selected_summary
    pg_document_report = None
    if pg_document is not None:
        pg_document_report = {
            key: value for key, value in pg_document.items() if key != "raw_data"
        }
        pg_document_report["raw_source_document_id"] = raw_pg_id
    report: dict[str, Any] = {
        "status": "dry_run",
        "dry_run": dry_run,
        "wrote": False,
        "folio": resolved_folio,
        "local_document_id": local_id,
        "previous_source_document_id": previous_source_id,
        "current_bsale_source_document_id": source_id,
        "source_changed": previous_source_id is not None
        and source_id != previous_source_id,
        "source_hash": digest,
        "stored_source_hash": stored_hash,
        "source_hash_matches": hash_matches,
        "source_updated_at": (
            source_updated_at(selected).isoformat()
            if source_updated_at(selected)
            else None
        ),
        "postgresql_document": pg_document_report,
        "bsale_document": selected_summary,
        "postgresql_details": pg_details,
        "bsale_details": [_line_from_bsale(item) for item in details],
        "diff": diff,
        "weight": weight,
        "source_discovery": discovery_report,
    }
    if dry_run:
        report["status"] = "dry_run_in_sync" if diff["matches"] else "dry_run_needs_sync"
        return report
    if hash_matches and diff["matches"]:
        report["status"] = "already_in_sync"
        return report

    conn = get_connection()
    try:
        cur = conn.cursor()
        try:
            _assert_source_schema(cur)
            row = document_dict_from_bsale(
                selected,
                company_id=COMPANY_ID,
                default_office_id=OFFICE_ID,
            )
            if row is None:
                raise RuntimeError("El source activo no pudo mapearse a documents")
            upsert_documents(cur, [row])
            persisted_local_id = int(row["document_id"])
            if local_id is not None and persisted_local_id != local_id:
                raise RuntimeError(
                    f"PK local cambió inesperadamente: {local_id} -> {persisted_local_id}"
                )
            local_id = persisted_local_id
            details_written = replace_document_details(
                cur,
                local_id,
                details,
                invalidate_cache=False,
            )
            cur.execute(
                """
                UPDATE distribuidora.documents
                SET source_document_id = %s,
                    source_hash = %s,
                    source_updated_at = %s,
                    last_synced_at = NOW(),
                    updated_at = NOW()
                WHERE document_id = %s
                """,
                (source_id, digest, source_updated_at(selected), local_id),
            )
            weight_result = recalculate_order_weight_in_transaction(
                cur,
                document_id=local_id,
                company_id=COMPANY_ID,
                office_id=OFFICE_ID,
                user_email=user_email,
                persist=True,
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
    finally:
        conn.close()

    report.update(
        {
            "status": "synced",
            "wrote": True,
            "local_document_id": local_id,
            "details_replaced": details_written,
            "peso_despues_kg": weight_result.get("peso_total_kg"),
            "cobertura_despues": weight_result.get("porcentaje_cobertura"),
        }
    )
    logger.info(
        "reconcile_oc_done folio=%s local_document_id=%s "
        "bsale_source_document_id=%s details_replaced=%s peso_total_kg=%s",
        resolved_folio,
        local_id,
        source_id,
        details_written,
        weight_result.get("peso_total_kg"),
    )
    return report


def reconcile_one_oc(
    client: BsaleClient,
    *,
    folio: int | None = None,
    local_document_id: int | None = None,
    dry_run: bool = True,
    active_document: dict[str, Any] | None = None,
    user_email: str | None = None,
) -> dict[str, Any]:
    """Ejecuta read-only sin lock; toda escritura compite con syncs existentes."""
    kwargs = {
        "folio": folio,
        "local_document_id": local_document_id,
        "dry_run": dry_run,
        "active_document": active_document,
        "user_email": user_email,
    }
    if dry_run:
        return _reconcile_one_oc(client, **kwargs)
    with _oc_writer_locks():
        return _reconcile_one_oc(client, **kwargs)


def _fetch_recent_oc_documents(
    client: BsaleClient,
    *,
    window_days: int,
) -> list[dict[str, Any]]:
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=max(30, int(window_days)))
    offset = 0
    output: list[dict[str, Any]] = []
    while True:
        params = merge_bsale_office_query(
            {
                "documenttypeid": OC_DOCUMENT_TYPE_ID,
                "emissiondaterange": f"[{int(start.timestamp())},{int(now.timestamp())}]",
                "limit": PAGE_LIMIT,
                "offset": offset,
            },
            OFFICE_ID,
            context="reconcile_recent_ocs",
        )
        payload = client.get("/documents.json", params)
        items = payload.get("items") if isinstance(payload, dict) else None
        if not isinstance(items, list):
            break
        output.extend(item for item in items if isinstance(item, dict))
        if len(items) < PAGE_LIMIT:
            break
        offset += len(items)
    return output


def reconcile_recent_ocs(
    client: BsaleClient,
    *,
    window_days: int = 30,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Ventana móvil de emisión, agrupada por folio y con hash para skip writes."""
    lock_conn = get_connection()
    got_lock = False
    try:
        lock_cur = lock_conn.cursor()
        lock_cur.execute(
            "SELECT pg_try_advisory_lock(%s)",
            (ADVISORY_LOCK_OC_RECONCILIATION,),
        )
        got_lock = bool(lock_cur.fetchone()[0])
        lock_conn.commit()
        lock_cur.close()
        if not got_lock:
            return {
                "window_days": max(30, int(window_days)),
                "omitido_concurrencia": True,
                "results": [],
            }

        items = _fetch_recent_oc_documents(client, window_days=max(30, window_days))
        by_folio: dict[int, list[dict[str, Any]]] = {}
        for item in items:
            number = summarize_bsale_document(item).get("number")
            if number is None or int(number) <= 0:
                continue
            by_folio.setdefault(int(number), []).append(item)

        results: list[dict[str, Any]] = []
        for folio, candidates in sorted(by_folio.items()):
            active, _ = select_active_oc_source(candidates, folio=folio)
            if active is None:
                continue
            try:
                result = reconcile_one_oc(
                    client,
                    folio=folio,
                    dry_run=dry_run,
                    active_document=active,
                )
            except Exception as exc:
                logger.exception("Reconcile OC folio=%s falló", folio)
                results.append(
                    {
                        "folio": folio,
                        "status": "error",
                        "error": str(exc),
                        "wrote": False,
                    }
                )
            else:
                results.append(result)
        return {
            "window_days": max(30, int(window_days)),
            "api_documents": len(items),
            "folios": len(by_folio),
            "synced": sum(1 for item in results if item.get("wrote")),
            "unchanged": sum(
                1 for item in results if item.get("status") == "already_in_sync"
            ),
            "errors": sum(1 for item in results if item.get("status") == "error"),
            "omitido_concurrencia": False,
            "results": results,
        }
    finally:
        if got_lock:
            try:
                unlock_cur = lock_conn.cursor()
                unlock_cur.execute(
                    "SELECT pg_advisory_unlock(%s)",
                    (ADVISORY_LOCK_OC_RECONCILIATION,),
                )
                unlock_cur.close()
            except Exception:
                logger.exception("No se pudo liberar lock de reconciliación OC")
        lock_conn.close()
