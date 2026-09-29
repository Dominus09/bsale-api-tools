"""
Canario por folio OC y reconciliación liviana de OCs recientes (Bsale → PostgreSQL).

* Canario (``run_oc_folio_canary``): read-only. Busca el folio en Bsale (sucursal pedida y
  cualquier sucursal), lo busca en PostgreSQL en **todas** las company/office y entrega la
  causa concreta por la que el sync lo omitiría.
* Reconciliación (``reconcile_recent_oc_documents``): lista OCs (tipo 33) de Bsale por
  ``generationdaterange`` de los últimos N días, compara folios contra PostgreSQL y repara
  solo los faltantes vía ``reconcile_one_oc`` (``document_dict_from_bsale`` →
  ``upsert_documents`` → details/attributes/references/related/peso).
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Iterable

from backend.repositories.distribuidora.documents_repo import document_dict_from_bsale
from backend.services.distribuidora.bsale_client import BsaleClient
from backend.services.distribuidora.bsale_params import merge_bsale_office_query
from backend.services.distribuidora.oc_source_resolver import (
    OC_DOCUMENT_TYPE_ID,
    PAGE_LIMIT,
    fetch_all_document_details,
    select_active_oc_source,
    summarize_bsale_document,
)

logger = logging.getLogger(__name__)

DEFAULT_RECENT_DAYS = 3
DEFAULT_MAX_PAGES = 20
DEFAULT_MAX_REPAIRS = 25
ORDERS_EMISSION_WINDOW_DAYS_DEFAULT = 45
ORDERS_GENERATION_WINDOW_DAYS_DEFAULT = 14

LocalFolioLoader = Callable[[Iterable[int]], set[int]]
RepairFn = Callable[[int, dict[str, Any]], dict[str, Any]]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(ts: int | None) -> str | None:
    if ts is None:
        return None
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat()
    except (TypeError, ValueError, OSError, OverflowError):
        return None


def _paged_documents(
    client: BsaleClient,
    params: dict[str, Any],
    *,
    max_pages: int,
) -> tuple[list[dict[str, Any]], int, bool]:
    """Pagina ``/documents.json`` por offset; dedup por id. Retorna (items, páginas, truncado)."""
    by_id: dict[int, dict[str, Any]] = {}
    anonymous: list[dict[str, Any]] = []
    offset = 0
    pages = 0
    truncated = False
    while True:
        if pages >= max_pages:
            truncated = True
            break
        payload = client.get("/documents.json", {**params, "limit": PAGE_LIMIT, "offset": offset})
        pages += 1
        items = payload.get("items") if isinstance(payload, dict) else None
        if not isinstance(items, list) or not items:
            break
        for item in items:
            if not isinstance(item, dict):
                continue
            raw_id = item.get("id")
            try:
                by_id[int(raw_id)] = item
            except (TypeError, ValueError):
                anonymous.append(item)
        if len(items) < PAGE_LIMIT:
            break
        offset += len(items)
    return list(by_id.values()) + anonymous, pages, truncated


# ---------------------------------------------------------------------------
# Canario por folio
# ---------------------------------------------------------------------------


def search_oc_folio_all_offices(client: BsaleClient, *, folio: int) -> list[dict[str, Any]]:
    """``number`` + ``documenttypeid=33`` sin ``officeid``: detecta el folio en otra sucursal."""
    items, _, _ = _paged_documents(
        client,
        {"number": int(folio), "documenttypeid": OC_DOCUMENT_TYPE_ID},
        max_pages=5,
    )
    return items


def search_oc_folio_in_office(
    client: BsaleClient, *, folio: int, office_id: int
) -> list[dict[str, Any]]:
    items, _, _ = _paged_documents(
        client,
        merge_bsale_office_query(
            {"number": int(folio), "documenttypeid": OC_DOCUMENT_TYPE_ID},
            int(office_id),
            context="oc_folio_canary",
        ),
        max_pages=5,
    )
    return items


def _client_name(client: BsaleClient, document: dict[str, Any]) -> str | None:
    raw = document.get("client")
    if not isinstance(raw, dict):
        return None
    for key in ("company", "name"):
        if raw.get(key):
            return str(raw[key])
    cid = raw.get("id")
    if cid is None:
        return None
    try:
        payload = client.get(f"/clients/{int(cid)}.json")
    except Exception as exc:
        return f"<error cliente {cid}: {type(exc).__name__}>"
    if not isinstance(payload, dict):
        return None
    name = payload.get("company") or " ".join(
        str(p) for p in (payload.get("firstName"), payload.get("lastName")) if p
    )
    return name or None


def load_local_oc_matches(
    cur,
    *,
    folio: int,
    bsale_ids: Iterable[int],
) -> list[dict[str, Any]]:
    """
    Read-only: filas locales con el folio (tipo 33, **cualquier** company/office) o cuyo
    ``document_id`` / ``source_document_id`` coincide con algún id Bsale del folio.
    """
    ids = sorted({int(i) for i in bsale_ids if i is not None})
    cols = (
        "document_id, company_id, office_id, document_type_id, number, state, "
        "NULLIF(to_jsonb(d)->>'source_document_id', '')::bigint AS source_document_id, "
        "NULLIF(d.raw_data->>'id', '')::bigint AS raw_bsale_id"
    )
    rows: dict[int, dict[str, Any]] = {}

    def _collect(match: str) -> None:
        for r in cur.fetchall() or []:
            did = int(r[0])
            entry = rows.setdefault(
                did,
                {
                    "document_id": did,
                    "company_id": r[1],
                    "office_id": r[2],
                    "document_type_id": r[3],
                    "number": r[4],
                    "state": r[5],
                    "source_document_id": r[6],
                    "raw_bsale_id": r[7],
                    "matched_by": [],
                },
            )
            if match not in entry["matched_by"]:
                entry["matched_by"].append(match)

    cur.execute(
        f"SELECT {cols} FROM distribuidora.documents d "
        "WHERE d.document_type_id = %s AND d.number = %s",
        (OC_DOCUMENT_TYPE_ID, int(folio)),
    )
    _collect("folio")
    if ids:
        cur.execute(
            f"SELECT {cols} FROM distribuidora.documents d WHERE d.document_id = ANY(%s)",
            (ids,),
        )
        _collect("document_id=bsale_id")
        cur.execute(
            """
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'distribuidora' AND table_name = 'documents'
              AND column_name = 'source_document_id'
            """
        )
        if cur.fetchone():
            cur.execute(
                f"SELECT {cols} FROM distribuidora.documents d "
                "WHERE d.source_document_id = ANY(%s)",
                (ids,),
            )
            _collect("source_document_id=bsale_id")
    return sorted(rows.values(), key=lambda r: r["document_id"])


def compute_live_emission_window(
    *,
    now: datetime,
    last_watermark: datetime | None,
    window_hours: float = 2.0,
    overlap_seconds: int = 900,
) -> tuple[int, int]:
    """Replica ``live_sync_service._compute_window`` (epoch) para diagnóstico."""
    window_from = now - timedelta(hours=window_hours)
    if last_watermark is not None:
        wm = last_watermark if last_watermark.tzinfo else last_watermark.replace(tzinfo=timezone.utc)
        wm_from = wm - timedelta(seconds=overlap_seconds)
        if wm_from < window_from:
            window_from = wm_from
    return int(window_from.timestamp()), int(now.timestamp())


def emission_visible_in_window(emission_ts: int | None, window: tuple[int, int]) -> bool:
    if emission_ts is None:
        return False
    return window[0] <= int(emission_ts) <= window[1]


def diagnose_oc_skip(
    *,
    folio: int,
    company_id: int,
    office_id: int,
    office_hits: list[dict[str, Any]],
    any_office_hits: list[dict[str, Any]],
    local_rows: list[dict[str, Any]],
    now: datetime,
    orders_emission_days: int = ORDERS_EMISSION_WINDOW_DAYS_DEFAULT,
    orders_generation_days: int = ORDERS_GENERATION_WINDOW_DAYS_DEFAULT,
) -> dict[str, Any]:
    """Función pura: explica por qué el sync omitiría (u omitió) el folio."""
    reasons: list[str] = []
    detail: dict[str, Any] = {}

    active, evaluated = select_active_oc_source(
        office_hits,
        folio=folio,
        company_id=company_id,
        office_id=office_id,
    )
    detail["bsale_evaluated_in_office"] = evaluated

    local_same_key = [
        r
        for r in local_rows
        if "folio" in r["matched_by"]
        and int(r["company_id"] or 0) == company_id
        and int(r["office_id"] or 0) == office_id
    ]
    local_other_scope = [
        r
        for r in local_rows
        if "folio" in r["matched_by"] and r not in local_same_key
    ]

    if local_same_key:
        reasons.append("present_locally")
    if local_other_scope:
        reasons.append("present_locally_other_company_or_office")

    if not office_hits:
        other = [
            summarize_bsale_document(d, expected_company_id=company_id) for d in any_office_hits
        ]
        other = [o for o in other if o.get("number") == int(folio)]
        detail["bsale_other_office_hits"] = other
        if other:
            reasons.append("bsale_folio_only_in_other_office")
        else:
            reasons.append("not_found_in_bsale")
        return {"primary_cause": reasons[0], "reasons": reasons, **detail}

    if active is None:
        discards = sorted({r for e in evaluated for r in e.get("discard_reasons") or []})
        detail["bsale_discard_reasons"] = discards
        reasons.append("bsale_source_not_eligible:" + ",".join(discards))

    chosen = active or office_hits[0]
    summary = summarize_bsale_document(chosen, expected_company_id=company_id)
    mapped = document_dict_from_bsale(
        chosen, company_id=company_id, default_office_id=office_id, sync_stats={}
    )
    if mapped is None:
        reasons.append("document_dict_from_bsale_rejected")

    live_window = compute_live_emission_window(now=now, last_watermark=now)
    emission_ts = summary.get("emissionDate")
    generation_ts = summary.get("generationDate")
    detail["emission_is_utc_midnight"] = emission_ts is not None and int(emission_ts) % 86400 == 0
    detail["live_emission_window_now"] = [_iso(live_window[0]), _iso(live_window[1])]
    detail["emission_visible_to_live_window_now"] = emission_visible_in_window(
        emission_ts, live_window
    )
    if not detail["emission_visible_to_live_window_now"]:
        reasons.append("live_sync_emission_window_blind")

    now_ts = int(now.timestamp())
    em_from = now_ts - orders_emission_days * 86400
    gen_from = now_ts - orders_generation_days * 86400
    in_orders = (emission_ts is not None and int(emission_ts) >= em_from) or (
        generation_ts is not None and int(generation_ts) >= gen_from
    )
    detail["inside_orders_sync_windows"] = in_orders
    if not in_orders:
        reasons.append("outside_orders_sync_windows")

    source_id = summary.get("id")
    pk_collisions = [
        r
        for r in local_rows
        if "document_id=bsale_id" in r["matched_by"]
        and (
            r["document_type_id"] != OC_DOCUMENT_TYPE_ID
            or r["number"] != int(folio)
            or int(r["office_id"] or 0) != office_id
            or int(r["company_id"] or 0) != company_id
        )
    ]
    if pk_collisions:
        detail["pk_collisions"] = pk_collisions
        reasons.append("local_pk_collision_document_id")

    if not local_same_key and not pk_collisions and mapped is not None and active is not None:
        if in_orders:
            reasons.append("eligible_but_never_persisted_check_upsert_failures_or_orders_job")

    priority = (
        "present_locally",
        "local_pk_collision_document_id",
        "document_dict_from_bsale_rejected",
        "outside_orders_sync_windows",
        "eligible_but_never_persisted_check_upsert_failures_or_orders_job",
    )
    primary = next((p for p in priority if p in reasons), None)
    if primary is None:
        primary = next((r for r in reasons if r.startswith("bsale_source_not_eligible")), None)
    if primary is None:
        primary = reasons[0] if reasons else "unknown"
    detail["bsale_source_document_id"] = source_id
    return {"primary_cause": primary, "reasons": reasons, **detail}


def run_oc_folio_canary(
    client: BsaleClient,
    *,
    folio: int,
    company_id: int,
    office_id: int,
    local_loader: Callable[[int, list[int]], list[dict[str, Any]]] | None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Canario read-only. ``local_loader(folio, bsale_ids)`` → filas locales (o None sin DB)."""
    now = now or _utc_now()
    office_hits = search_oc_folio_in_office(client, folio=folio, office_id=office_id)
    any_office_hits = search_oc_folio_all_offices(client, folio=folio)

    active, _ = select_active_oc_source(
        office_hits, folio=folio, company_id=company_id, office_id=office_id
    )
    chosen = active
    if chosen is None:
        pool = office_hits or [
            d
            for d in any_office_hits
            if summarize_bsale_document(d, expected_company_id=company_id).get("number")
            == int(folio)
        ]
        chosen = pool[0] if pool else None

    report: dict[str, Any] = {
        "folio": int(folio),
        "company_id": int(company_id),
        "office_id": int(office_id),
        "found_in_bsale": chosen is not None,
        "bsale_hits_in_office": len(office_hits),
        "bsale_hits_any_office": len(any_office_hits),
    }
    bsale_ids: list[int] = []
    for d in office_hits + any_office_hits:
        try:
            bsale_ids.append(int(d.get("id")))
        except (TypeError, ValueError):
            continue

    if chosen is not None:
        s = summarize_bsale_document(chosen, expected_company_id=company_id)
        details_count: int | str
        try:
            details_count = len(fetch_all_document_details(client, int(s["id"])))
        except Exception as exc:
            details_count = f"error:{type(exc).__name__}"
        report.update(
            {
                "bsale_document_id": s["id"],
                "number": s["number"],
                "document_type_id": s["document_type_id"],
                "bsale_office_id": s["office_id"],
                "emission_date": _iso(s["emissionDate"]),
                "emission_date_epoch": s["emissionDate"],
                "generation_date": _iso(s["generationDate"]),
                "state": s["state"],
                "commercial_state": s["commercialState"],
                "total": s["totalAmount"],
                "client": _client_name(client, chosen),
                "details_count": details_count,
                "active_source_selected": active is not None,
            }
        )

    if local_loader is None:
        report["local_lookup"] = "skipped_no_db"
        local_rows: list[dict[str, Any]] = []
    else:
        local_rows = local_loader(int(folio), bsale_ids)
        report["local_matches"] = local_rows

    same = [
        r
        for r in local_rows
        if "folio" in r["matched_by"]
        and int(r["company_id"] or 0) == company_id
        and int(r["office_id"] or 0) == office_id
    ]
    report["found_locally"] = bool(same)
    if same:
        r0 = same[0]
        report.update(
            {
                "local_document_id": r0["document_id"],
                "local_source_document_id": r0["source_document_id"],
                "local_company_id": r0["company_id"],
                "local_office_id": r0["office_id"],
            }
        )
    else:
        report.update(
            {
                "local_document_id": None,
                "local_source_document_id": None,
                "local_company_id": None,
                "local_office_id": None,
            }
        )
    report["found_locally_other_scope"] = [
        r for r in local_rows if "folio" in r["matched_by"] and r not in same
    ]
    report["diagnosis"] = diagnose_oc_skip(
        folio=folio,
        company_id=company_id,
        office_id=office_id,
        office_hits=office_hits,
        any_office_hits=any_office_hits,
        local_rows=local_rows,
        now=now,
    )
    return report


# ---------------------------------------------------------------------------
# Reconciliación liviana de recientes (B)
# ---------------------------------------------------------------------------


def fetch_recent_bsale_ocs(
    client: BsaleClient,
    *,
    office_id: int,
    days: int,
    max_pages: int,
    now: datetime | None = None,
) -> tuple[list[dict[str, Any]], int, bool]:
    """OCs tipo 33 con ``generationDate`` en los últimos ``days`` días (hora real de creación)."""
    now = now or _utc_now()
    start = now - timedelta(days=max(1, int(days)))
    params = merge_bsale_office_query(
        {
            "documenttypeid": OC_DOCUMENT_TYPE_ID,
            "generationdaterange": f"[{int(start.timestamp())},{int(now.timestamp())}]",
        },
        int(office_id),
        context="reconcile_recent_oc_documents",
    )
    return _paged_documents(client, params, max_pages=max(1, int(max_pages)))


def reconcile_recent_oc_documents(
    client: BsaleClient,
    *,
    company_id: int,
    office_id: int,
    days: int = DEFAULT_RECENT_DAYS,
    max_pages: int = DEFAULT_MAX_PAGES,
    max_repairs: int = DEFAULT_MAX_REPAIRS,
    apply: bool = False,
    load_local_folios: LocalFolioLoader,
    repair_one: RepairFn | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """
    Detecta folios OC activos en Bsale (sucursal ``office_id``) que no existen en PostgreSQL
    para ``(company_id, office_id, 33, folio)`` y, con ``apply``, los repara uno a uno.

    Idempotente: un folio ya persistido deja de ser faltante y no se vuelve a tocar.
    """
    t0 = time.perf_counter()
    items, pages, truncated = fetch_recent_bsale_ocs(
        client, office_id=office_id, days=days, max_pages=max_pages, now=now
    )
    by_folio: dict[int, list[dict[str, Any]]] = {}
    ignored_other_scope = 0
    for item in items:
        s = summarize_bsale_document(item, expected_company_id=company_id)
        if s.get("document_type_id") != OC_DOCUMENT_TYPE_ID:
            ignored_other_scope += 1
            continue
        if s.get("office_id") != int(office_id) or s.get("company_id") != int(company_id):
            ignored_other_scope += 1
            continue
        number = s.get("number")
        if number is None or int(number) <= 0:
            continue
        by_folio.setdefault(int(number), []).append(item)

    active_by_folio: dict[int, dict[str, Any]] = {}
    for folio, candidates in by_folio.items():
        active, _ = select_active_oc_source(
            candidates, folio=folio, company_id=company_id, office_id=office_id
        )
        if active is not None:
            active_by_folio[folio] = active

    present = load_local_folios(sorted(active_by_folio)) if active_by_folio else set()
    missing = sorted(f for f in active_by_folio if f not in present)

    results: list[dict[str, Any]] = []
    repaired = 0
    errors = 0
    for folio in missing:
        active = active_by_folio[folio]
        s = summarize_bsale_document(active, expected_company_id=company_id)
        entry: dict[str, Any] = {
            "folio": folio,
            "bsale_document_id": s.get("id"),
            "generation_date": _iso(s.get("generationDate")),
            "emission_date": _iso(s.get("emissionDate")),
            "total": s.get("totalAmount"),
        }
        if not apply or repair_one is None:
            entry["status"] = "missing_dry_run"
            results.append(entry)
            continue
        if repaired + errors >= max_repairs:
            entry["status"] = "deferred_budget"
            results.append(entry)
            continue
        try:
            out = repair_one(folio, active)
            entry["status"] = out.get("status")
            entry["local_document_id"] = out.get("local_document_id")
            entry["details_replaced"] = out.get("details_replaced")
            if out.get("wrote"):
                repaired += 1
        except Exception as exc:
            errors += 1
            entry["status"] = "error"
            entry["error"] = str(exc)[:500]
            logger.exception("reconcile_recent_oc_failed folio=%s", folio)
        results.append(entry)

    return {
        "mode": "apply" if apply else "dry_run",
        "company_id": int(company_id),
        "office_id": int(office_id),
        "days": int(days),
        "api_pages": pages,
        "api_truncated_by_budget": truncated,
        "bsale_items": len(items),
        "bsale_ignored_other_scope": ignored_other_scope,
        "bsale_active_folios": len(active_by_folio),
        "local_present": len(present),
        "missing": len(missing),
        "missing_folios": missing,
        "repaired": repaired,
        "errors": errors,
        "results": results,
        "duration_seconds": round(time.perf_counter() - t0, 3),
    }


def load_local_oc_folios_db(
    cur, *, company_id: int, office_id: int, folios: Iterable[int]
) -> set[int]:
    nums = sorted({int(f) for f in folios})
    if not nums:
        return set()
    cur.execute(
        """
        SELECT number FROM distribuidora.documents
        WHERE company_id = %s AND office_id = %s AND document_type_id = %s
          AND number = ANY(%s)
        """,
        (int(company_id), int(office_id), OC_DOCUMENT_TYPE_ID, nums),
    )
    return {int(r[0]) for r in cur.fetchall() or []}
