#!/usr/bin/env python3
"""
Ingeniería inversa controlada: comparar estructura REAL de documentos Bsale
por ``document_type_id`` (muestras pequeñas, sin PostgreSQL ni listados masivos).

Salidas:
  - ``exports/debug_document_types/*.json`` — RAW por documento y endpoint.
  - ``exports/document_types_analysis.xlsx`` — claves, ``references_analysis`` y
    relación operacional OC→ventas vía ``relateddetailid`` (hojas
    ``related_documents``, ``relationship_analysis``).
"""

from __future__ import annotations

import json
import sys
import time
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

from backend.utils.bsale_token_env import require_bsale_token

# ---------------------------------------------------------------------------
# Configuración (editar aquí)
# ---------------------------------------------------------------------------

OFFICE_ID = 1

DATE_FROM = "2026-04-01"
DATE_TO = "2026-04-10"

TARGET_TYPES = [1, 6, 9, 33]
DOCUMENTS_PER_TYPE = 3

BASE_BSALE = "https://api.bsale.io/v1"
LIST_LIMIT = 50
MAX_LIST_PAGES_PER_DAY = 60  # tope por día (evita bucles enormes)
DETAILS_LIMIT = 50
MAX_DETAIL_PAGES = 25
RELATED_DETAIL_LIST_LIMIT = 50
MAX_RELATED_DETAIL_PAGES = 20  # paginación por ``relateddetailid``
TIMEOUT = (10, 30)
SLEEP_SEC = 0.2

TYPE_SLUG: dict[int, str] = {
    1: "boleta",
    6: "factura",
    9: "nc",
    33: "oc",
}

EXPORT_JSON_DIR = "exports/debug_document_types"
EXPORT_XLSX = "exports/document_types_analysis.xlsx"


def _die(msg: str, code: int = 1) -> None:
    print(f"[debug_document_types] ERROR: {msg}", file=sys.stderr)
    raise SystemExit(code)


def _token() -> str:
    return require_bsale_token(label="debug_document_types")


def _parse_date(s: str, label: str) -> date:
    try:
        return datetime.strptime(s.strip(), "%Y-%m-%d").date()
    except ValueError as e:
        raise SystemExit(f"[debug_document_types] {label} inválida (YYYY-MM-DD): {s!r}") from e


def _utc_day_epoch_bounds(d: date) -> tuple[int, int]:
    start = datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=timezone.utc)
    end_ts = int((start + timedelta(days=1)).timestamp()) - 1
    return int(start.timestamp()), end_ts


def _iter_dates(d0: date, d1: date) -> Iterator[date]:
    d = d0
    while d <= d1:
        yield d
        d += timedelta(days=1)


def _safe_int(v: Any) -> int | None:
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _document_type_id(doc: dict[str, Any]) -> int | None:
    dt = doc.get("document_type") or doc.get("documentType")
    if not isinstance(dt, dict):
        return None
    return _safe_int(dt.get("id"))


def _document_root(payload: dict[str, Any]) -> dict[str, Any]:
    d = payload.get("document")
    if isinstance(d, dict) and d.get("id") is not None:
        return d
    return payload


def _get_json(
    session: requests.Session,
    token: str,
    path: str,
    params: dict[str, Any] | None = None,
    *,
    allow_404: bool = False,
) -> tuple[int, Any]:
    url = path if path.startswith("http") else f"{BASE_BSALE}{path}"
    r = session.get(
        url,
        headers={"access_token": token},
        params=params or {},
        timeout=TIMEOUT,
    )
    if r.status_code == 401:
        _die("401 Unauthorized: token inválido o expirado.")
    if r.status_code == 404 and allow_404:
        return 404, None
    if not (200 <= r.status_code < 300):
        return r.status_code, {
            "_http_error": True,
            "status": r.status_code,
            "body_preview": (r.text or "")[:400],
        }
    try:
        return r.status_code, r.json()
    except ValueError:
        return r.status_code, {"_parse_error": True, "preview": (r.text or "")[:400]}


def _save_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )


def _items_from_container(body: Any) -> list[dict[str, Any]]:
    if not isinstance(body, dict):
        return []
    for key in (
        "items",
        "payments",
        "references",
        "taxes",
        "documentTaxes",
    ):
        v = body.get(key)
        if isinstance(v, list):
            return [x for x in v if isinstance(x, dict)]
    return []


def _discover_documents(
    session: requests.Session,
    token: str,
    d0: date,
    d1: date,
) -> dict[int, list[dict[str, Any]]]:
    """Por cada tipo en ``TARGET_TYPES``, hasta ``DOCUMENTS_PER_TYPE`` ítems del listado."""
    found: dict[int, list[dict[str, Any]]] = {t: [] for t in TARGET_TYPES}

    def filled() -> bool:
        return all(len(found[t]) >= DOCUMENTS_PER_TYPE for t in TARGET_TYPES)

    for day in _iter_dates(d0, d1):
        if filled():
            break
        start_ts, end_ts = _utc_day_epoch_bounds(day)
        offset = 0
        for _page in range(MAX_LIST_PAGES_PER_DAY):
            if filled():
                break
            st, data = _get_json(
                session,
                token,
                "/documents.json",
                {
                    "limit": LIST_LIMIT,
                    "offset": offset,
                    "emissiondaterange": f"[{start_ts},{end_ts}]",
                    "officeId": OFFICE_ID,
                },
            )
            time.sleep(SLEEP_SEC)
            if st != 200 or not isinstance(data, dict):
                print(
                    f"[debug_document_types] listado day={day} offset={offset} "
                    f"http={st} (se sigue al siguiente día)",
                    flush=True,
                )
                break
            items = data.get("items") or []
            if not items:
                break
            for doc in items:
                if not isinstance(doc, dict):
                    continue
                oid = _safe_int((doc.get("office") or {}).get("id"))
                if oid != OFFICE_ID:
                    continue
                tid = _document_type_id(doc)
                if tid in TARGET_TYPES and len(found[tid]) < DOCUMENTS_PER_TYPE:
                    found[tid].append(doc)
                    print(
                        f"[debug_document_types] candidato type={tid} id={doc.get('id')} "
                        f"number={doc.get('number')}",
                        flush=True,
                    )
                    if filled():
                        break
            offset += len(items)
    return found


def _extract_detail_line_ids(details_bundle: dict[str, Any]) -> list[int]:
    """Ids de línea de detalle Bsale (``detail.id`` en cada ítem)."""
    out: list[int] = []
    for pg in details_bundle.get("pages", []):
        b = pg.get("body")
        if not isinstance(b, dict):
            continue
        for it in b.get("items") or []:
            if isinstance(it, dict):
                did = _safe_int(it.get("id"))
                if did is not None:
                    out.append(did)
    return out


def _fetch_documents_by_related_detail_id(
    session: requests.Session,
    token: str,
    detail_id: int,
) -> list[dict[str, Any]]:
    """
    Relación operacional OC → ventas: ``GET /documents.json?relateddetailid=…``.
    """
    merged: list[dict[str, Any]] = []
    offset = 0
    for _ in range(MAX_RELATED_DETAIL_PAGES):
        st, data = _get_json(
            session,
            token,
            "/documents.json",
            {
                "relateddetailid": detail_id,
                "limit": RELATED_DETAIL_LIST_LIMIT,
                "offset": offset,
                "officeId": OFFICE_ID,
            },
        )
        time.sleep(SLEEP_SEC)
        if st != 200 or not isinstance(data, dict):
            break
        items = data.get("items") or []
        for it in items:
            if isinstance(it, dict):
                merged.append(it)
        if len(items) < RELATED_DETAIL_LIST_LIMIT:
            break
        offset += len(items)
    return merged


def _client_summary(client: Any) -> str:
    if not isinstance(client, dict):
        return "" if client is None else str(client)[:400]
    parts: list[str] = []
    for k in ("company", "name", "firstName", "lastName", "code", "activity"):
        v = client.get(k)
        if v is not None and str(v).strip():
            parts.append(str(v).strip())
    if parts:
        return " | ".join(parts)[:500]
    return json.dumps(client, ensure_ascii=False, default=str)[:400]


def _fetch_details_bundle(
    session: requests.Session,
    token: str,
    doc_id: int,
) -> dict[str, Any]:
    pages: list[dict[str, Any]] = []
    off = 0
    for _ in range(MAX_DETAIL_PAGES):
        st, body = _get_json(
            session,
            token,
            f"/documents/{doc_id}/details.json",
            {"limit": DETAILS_LIMIT, "offset": off},
            allow_404=True,
        )
        time.sleep(SLEEP_SEC)
        pages.append({"offset": off, "http_status": st, "body": body})
        if st == 404 or body is None or not isinstance(body, dict):
            break
        items = body.get("items") or []
        if not items:
            break
        off += len(items)
        if len(items) < DETAILS_LIMIT:
            break
    return {"document_id": doc_id, "pages": pages}


def _analyze_embedded_references(doc_root: dict[str, Any]) -> tuple[bool, list[str]]:
    keys: list[str] = []
    for k in (
        "references",
        "reference",
        "relatedDocuments",
        "related_documents",
        "referencedDocuments",
        "referenced_documents",
    ):
        if k in doc_root:
            keys.append(k)
    return (len(keys) > 0, keys)


def _analyze_references_endpoint(
    st: int,
    body: Any,
) -> tuple[int, str, str, list[str]]:
    """
    Devuelve ``(count, structure, notes, top_level_keys_of_response)``.
    ``count`` = len(items) si aplica.
    """
    keys: list[str] = []
    if st == 404:
        return 0, "endpoint_404", "Sin recurso /references.json", keys
    if not isinstance(body, dict):
        return 0, f"non_dict_body:{type(body).__name__}", "", keys
    keys = sorted(body.keys())
    items = body.get("items")
    if isinstance(items, list):
        n = len(items)
        if n == 0:
            return 0, "endpoint_items_empty", "items: lista vacía", keys
        return n, "endpoint_items_list", f"{n} ítems", keys
    return 0, "endpoint_no_items_key", f"keys={keys}", keys


def _collect_keys_from_dicts(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """key -> {occurrences, nulls}"""
    acc: dict[str, dict[str, int]] = defaultdict(lambda: {"occ": 0, "null": 0})
    for row in rows:
        for k, v in row.items():
            acc[k]["occ"] += 1
            if v is None:
                acc[k]["null"] += 1
    return {k: dict(v) for k, v in acc.items()}


def _flatten_key_rows(
    document_type_id: int,
    document_id: int,
    key_stats: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for key, st in sorted(key_stats.items()):
        out.append(
            {
                "document_type_id": document_type_id,
                "document_id": document_id,
                "key": key,
                "occurrences": st.get("occ", 0),
                "null_count": st.get("null", 0),
            }
        )
    return out


def main() -> None:
    token = _token()
    d0 = _parse_date(DATE_FROM, "DATE_FROM")
    d1 = _parse_date(DATE_TO, "DATE_TO")
    if d1 < d0:
        _die("DATE_TO debe ser >= DATE_FROM")

    repo_root = Path(__file__).resolve().parents[2]
    json_dir = repo_root / EXPORT_JSON_DIR
    xlsx_path = repo_root / EXPORT_XLSX

    session = requests.Session()

    print("[debug_document_types] fase 1: descubrir documentos por tipo …", flush=True)
    found = _discover_documents(session, token, d0, d1)
    for tid in TARGET_TYPES:
        n = len(found.get(tid, []))
        print(
            f"[debug_document_types] tipo {tid} ({TYPE_SLUG.get(tid, '?')}): "
            f"{n}/{DOCUMENTS_PER_TYPE} encontrados",
            flush=True,
        )

    summary_rows: list[dict[str, Any]] = []
    detail_key_rows: list[dict[str, Any]] = []
    payment_key_rows: list[dict[str, Any]] = []
    tax_key_rows: list[dict[str, Any]] = []
    reference_item_key_rows: list[dict[str, Any]] = []
    ref_analysis_rows: list[dict[str, Any]] = []
    related_documents_rows: list[dict[str, Any]] = []
    oc_link_metrics: list[dict[str, Any]] = []

    roots_by_type: dict[int, list[dict[str, Any]]] = defaultdict(list)

    for tid in TARGET_TYPES:
        slug = TYPE_SLUG.get(tid, f"type_{tid}")
        for stub in found.get(tid, []):
            doc_id = _safe_int(stub.get("id"))
            if doc_id is None:
                continue
            t0 = time.perf_counter()
            print(
                f"\n[debug_document_types] --- tipo={tid} slug={slug} document_id={doc_id} ---",
                flush=True,
            )

            st_doc, raw_doc = _get_json(
                session, token, f"/documents/{doc_id}.json", None, allow_404=False
            )
            time.sleep(SLEEP_SEC)
            if st_doc != 200 or not isinstance(raw_doc, dict):
                print(f"[debug_document_types] fallo document: http={st_doc}", flush=True)
                continue

            root = _document_root(raw_doc)
            roots_by_type[tid].append(root)

            prefix = f"{slug}_{doc_id}"
            _save_json(json_dir / f"{prefix}_document.json", raw_doc)

            details = _fetch_details_bundle(session, token, doc_id)
            _save_json(json_dir / f"{prefix}_details.json", details)

            st_pay, pay = _get_json(
                session,
                token,
                f"/documents/{doc_id}/payments.json",
                None,
                allow_404=True,
            )
            time.sleep(SLEEP_SEC)
            _save_json(json_dir / f"{prefix}_payments.json", {"http_status": st_pay, "body": pay})

            st_tax, tax = _get_json(
                session,
                token,
                f"/documents/{doc_id}/taxes.json",
                None,
                allow_404=True,
            )
            time.sleep(SLEEP_SEC)
            _save_json(json_dir / f"{prefix}_taxes.json", {"http_status": st_tax, "body": tax})

            st_ref, ref = _get_json(
                session,
                token,
                f"/documents/{doc_id}/references.json",
                None,
                allow_404=True,
            )
            time.sleep(SLEEP_SEC)
            _save_json(json_dir / f"{prefix}_references.json", {"http_status": st_ref, "body": ref})

            elapsed = round(time.perf_counter() - t0, 3)
            emb, emb_keys = _analyze_embedded_references(root)
            emb_vtype = ""
            emb_preview = ""
            if emb_keys:
                v0 = root.get(emb_keys[0])
                emb_vtype = type(v0).__name__
                emb_preview = json.dumps(v0, ensure_ascii=False, default=str)[:400]
            ref_count, ref_struct, ref_note, ref_tl_keys = _analyze_references_endpoint(st_ref, ref)

            endpoints = "document,details,payments,taxes,references"
            print(
                f"[debug_document_types] endpoints descargados: {endpoints} "
                f"(t_total={elapsed}s)",
                flush=True,
            )
            print(
                f"[debug_document_types] referencias: embedded={emb} keys={emb_keys} "
                f"endpoint_count={ref_count} structure={ref_struct}",
                flush=True,
            )

            summary_rows.append(
                {
                    "document_type_id": tid,
                    "type_slug": slug,
                    "document_id": doc_id,
                    "number": root.get("number"),
                    "elapsed_sec": elapsed,
                    "endpoints_downloaded": endpoints,
                    "has_embedded_reference_keys": emb,
                    "embedded_reference_keys": ",".join(emb_keys) if emb_keys else "",
                    "references_endpoint_http": st_ref,
                    "references_endpoint_count": ref_count,
                    "references_structure": ref_struct,
                    "references_notes": ref_note,
                }
            )

            has_endpoint_refs = st_ref == 200 and ref_count > 0
            ref_analysis_rows.append(
                {
                    "document_type_id": tid,
                    "type_slug": slug,
                    "document_id": doc_id,
                    "has_references_embedded": emb,
                    "has_references_endpoint_items": has_endpoint_refs,
                    "has_any_references_signal": emb or has_endpoint_refs,
                    "embedded_first_key_value_type": emb_vtype,
                    "embedded_first_key_preview": emb_preview,
                    "embedded_keys": ",".join(emb_keys) if emb_keys else "",
                    "references_endpoint_http": st_ref,
                    "references_count": ref_count,
                    "structure_detected": ref_struct,
                    "reference_response_top_keys": ",".join(ref_tl_keys),
                    "keys_in_first_reference_item": (
                        ",".join(sorted(ref.get("items", [{}])[0].keys()))
                        if isinstance(ref, dict)
                        and isinstance(ref.get("items"), list)
                        and ref["items"]
                        and isinstance(ref["items"][0], dict)
                        else ""
                    ),
                    "notes": ref_note,
                }
            )

            # Detail líneas
            dlines: list[dict[str, Any]] = []
            for pg in details.get("pages", []):
                b = pg.get("body")
                if isinstance(b, dict):
                    for it in b.get("items") or []:
                        if isinstance(it, dict):
                            dlines.append(it)
            dk = _collect_keys_from_dicts(dlines)
            detail_key_rows.extend(_flatten_key_rows(tid, doc_id, dk))

            pay_items = _items_from_container(pay) if isinstance(pay, dict) else []
            pk = _collect_keys_from_dicts(pay_items)
            payment_key_rows.extend(_flatten_key_rows(tid, doc_id, pk))

            tax_items = _items_from_container(tax) if isinstance(tax, dict) else []
            txk = _collect_keys_from_dicts(tax_items)
            tax_key_rows.extend(_flatten_key_rows(tid, doc_id, txk))

            ref_items = _items_from_container(ref) if isinstance(ref, dict) else []
            rk = _collect_keys_from_dicts(ref_items)
            reference_item_key_rows.extend(_flatten_key_rows(tid, doc_id, rk))

            # --- OC (33): relación operacional vía ``relateddetailid`` (no ``references``) ---
            if tid == 33:
                oc_number = root.get("number")
                detail_ids_list = _extract_detail_line_ids(details)
                per_detail_matches: dict[int, int] = {}
                related_doc_ids_union: set[int] = set()
                related_sales_bo_fa_ids: set[int] = set()

                for did in detail_ids_list:
                    raw_items = _fetch_documents_by_related_detail_id(
                        session, token, did
                    )
                    filtered = [
                        x
                        for x in raw_items
                        if isinstance(x, dict) and _safe_int(x.get("id")) != doc_id
                    ]
                    n_m = len(filtered)
                    per_detail_matches[did] = n_m
                    tipos_iter: set[int] = set()
                    for it in filtered:
                        rid = _safe_int(it.get("id"))
                        rdt = _document_type_id(it)
                        if rid is not None:
                            related_doc_ids_union.add(rid)
                        if rdt is not None:
                            tipos_iter.add(rdt)
                            if rdt in (1, 6) and rid is not None:
                                related_sales_bo_fa_ids.add(rid)
                        related_documents_rows.append(
                            {
                                "oc_document_id": doc_id,
                                "oc_number": oc_number,
                                "detail_id": did,
                                "related_matches_for_detail": n_m,
                                "related_document_id": rid,
                                "related_document_type_id": rdt,
                                "related_document_number": it.get("number"),
                                "related_totalAmount": it.get("totalAmount"),
                                "related_client": _client_summary(
                                    it.get("client") or it.get("cliente")
                                ),
                                "related_emissionDate": it.get("emissionDate")
                                    or it.get("emission_date"),
                            }
                        )
                    if not filtered:
                        related_documents_rows.append(
                            {
                                "oc_document_id": doc_id,
                                "oc_number": oc_number,
                                "detail_id": did,
                                "related_matches_for_detail": 0,
                                "related_document_id": None,
                                "related_document_type_id": None,
                                "related_document_number": None,
                                "related_totalAmount": None,
                                "related_client": "",
                                "related_emissionDate": None,
                            }
                        )
                    print(
                        f"[debug_document_types] relateddetailid detail_id={did} "
                        f"matches={n_m} tipos={sorted(tipos_iter)}",
                        flush=True,
                    )

                n_details_with_related = sum(
                    1 for _d, c in per_detail_matches.items() if c > 0
                )
                max_m = max(per_detail_matches.values()) if per_detail_matches else 0
                oc_link_metrics.append(
                    {
                        "oc_document_id": doc_id,
                        "oc_number": oc_number,
                        "n_detail_lines": len(detail_ids_list),
                        "n_details_with_at_least_one_related": n_details_with_related,
                        "max_matches_single_detail": max_m,
                        "total_related_match_rows": sum(per_detail_matches.values()),
                        "unique_related_document_ids_count": len(related_doc_ids_union),
                        "unique_boleta_factura_related_ids_count": len(
                            related_sales_bo_fa_ids
                        ),
                        "oc_sin_relacion": len(related_doc_ids_union) == 0,
                        "oc_multiples_ventas_boleta_factura": len(related_sales_bo_fa_ids)
                        > 1,
                    }
                )

    # --- Agregados por tipo: claves top-level y null rate ---
    def _aggregate_doc_top_keys(
        tid: int, roots: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        if not roots:
            return []
        n = len(roots)
        all_keys: set[str] = set()
        for r in roots:
            all_keys |= set(r.keys())
        rows_out: list[dict[str, Any]] = []
        for key in sorted(all_keys):
            present = sum(1 for r in roots if key in r)
            nulls = sum(1 for r in roots if r.get(key) is None)
            rows_out.append(
                {
                    "document_type_id": tid,
                    "key": key,
                    "samples_n": n,
                    "docs_with_key": present,
                    "docs_where_value_null": nulls,
                    "null_rate_in_samples": round(nulls / n, 4) if n else None,
                }
            )
        return rows_out

    agg_top_by_type: list[dict[str, Any]] = []
    for tid in TARGET_TYPES:
        agg_top_by_type.extend(_aggregate_doc_top_keys(tid, roots_by_type[tid]))

    # Comparación estructural: claves predominantes solo en NC / solo en OC
    def _key_union(tids: list[int]) -> set[str]:
        u: set[str] = set()
        for t in tids:
            for r in roots_by_type[t]:
                u |= set(r.keys())
        return u

    u_all = _key_union(TARGET_TYPES)
    u_bo_fa = _key_union([1]) | _key_union([6])
    u_nc = _key_union([9])
    u_oc = _key_union([33])
    nc_only = sorted(u_nc - u_bo_fa)
    oc_only = sorted(u_oc - u_bo_fa - u_nc)

    structural_rows = [
        {
            "analysis": "keys_in_nc_samples_not_in_boleta_factura_union",
            "document_type_ids": "9 vs (1,6)",
            "keys_csv": ",".join(nc_only)[:32000],
        },
        {
            "analysis": "keys_in_oc_samples_not_in_boleta_factura_nc_union",
            "document_type_ids": "33 vs (1,6,9)",
            "keys_csv": ",".join(oc_only)[:32000],
        },
        {
            "analysis": "union_all_top_level_keys_observed",
            "document_type_ids": "all_target_types",
            "keys_csv": ",".join(sorted(u_all))[:32000],
        },
    ]

    # --- relationship_analysis (``relateddetailid``, no ``references``) ---
    related_unique_ids: set[int] = set()
    related_type_ids_global: set[int] = set()
    for r in related_documents_rows:
        rid = _safe_int(r.get("related_document_id"))
        if rid is not None:
            related_unique_ids.add(rid)
        rt = _safe_int(r.get("related_document_type_id"))
        if rt is not None:
            related_type_ids_global.add(rt)

    oc_sin_rel = [m["oc_document_id"] for m in oc_link_metrics if m.get("oc_sin_relacion")]
    oc_mult_bf = [
        f"{m['oc_document_id']} (bf_únicos={m.get('unique_boleta_factura_related_ids_count', 0)})"
        for m in oc_link_metrics
        if m.get("oc_multiples_ventas_boleta_factura")
    ]
    relationship_summary = [
        {
            "analysis_item": "OC_sin_relacion_document_ids",
            "value": ",".join(str(x) for x in oc_sin_rel) if oc_sin_rel else "(ninguna en muestra)",
        },
        {
            "analysis_item": "OC_con_multiples_ventas_boleta_factura",
            "value": "; ".join(oc_mult_bf) if oc_mult_bf else "(ninguna en muestra)",
        },
        {
            "analysis_item": "documentos_relacionados_unicos_detectados",
            "value": str(len(related_unique_ids)),
        },
        {
            "analysis_item": "tipos_relacionados_encontrados_ids",
            "value": ",".join(str(x) for x in sorted(related_type_ids_global))
            if related_type_ids_global
            else "(ninguno)",
        },
        {
            "analysis_item": "total_filas_related_documents_sheet",
            "value": str(len(related_documents_rows)),
        },
        {
            "analysis_item": "metodo",
            "value": "GET /documents.json?relateddetailid=<detail_id> desde líneas GET .../details.json (OC tipo 33).",
        },
    ]

    df_summary = pd.DataFrame(summary_rows)
    df_top_agg = pd.DataFrame(agg_top_by_type)
    df_detail = pd.DataFrame(detail_key_rows)
    df_pay = pd.DataFrame(payment_key_rows)
    df_tax = pd.DataFrame(tax_key_rows)
    df_ref_keys = pd.DataFrame(reference_item_key_rows)
    df_ref_analysis = pd.DataFrame(ref_analysis_rows)
    df_structural = pd.DataFrame(structural_rows)

    _rel_cols = [
        "oc_document_id",
        "oc_number",
        "detail_id",
        "related_matches_for_detail",
        "related_document_id",
        "related_document_type_id",
        "related_document_number",
        "related_totalAmount",
        "related_client",
        "related_emissionDate",
    ]
    df_related = (
        pd.DataFrame(related_documents_rows)
        if related_documents_rows
        else pd.DataFrame(columns=_rel_cols)
    )

    df_ra_summary = pd.DataFrame(relationship_summary)
    _ocm_cols = [
        "oc_document_id",
        "oc_number",
        "n_detail_lines",
        "n_details_with_at_least_one_related",
        "max_matches_single_detail",
        "total_related_match_rows",
        "unique_related_document_ids_count",
        "unique_boleta_factura_related_ids_count",
        "oc_sin_relacion",
        "oc_multiples_ventas_boleta_factura",
    ]
    df_ra_oc = (
        pd.DataFrame(oc_link_metrics)
        if oc_link_metrics
        else pd.DataFrame(columns=_ocm_cols)
    )

    xlsx_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        df_summary.to_excel(writer, sheet_name="summary", index=False)
        if not df_structural.empty:
            start = len(df_summary) + 3
            df_structural.to_excel(
                writer, sheet_name="summary", index=False, startrow=start
            )
        df_top_agg.to_excel(writer, sheet_name="top_level_keys", index=False)
        df_detail.to_excel(writer, sheet_name="detail_keys", index=False)
        df_pay.to_excel(writer, sheet_name="payment_keys", index=False)
        df_tax.to_excel(writer, sheet_name="tax_keys", index=False)
        df_ref_keys.to_excel(writer, sheet_name="reference_keys", index=False)
        df_ref_analysis.to_excel(writer, sheet_name="references_analysis", index=False)
        df_related.to_excel(writer, sheet_name="related_documents", index=False)
        df_ra_summary.to_excel(writer, sheet_name="relationship_analysis", index=False)
        start_ra = len(df_ra_summary) + 2
        df_ra_oc.to_excel(
            writer,
            sheet_name="relationship_analysis",
            index=False,
            startrow=start_ra,
        )

    print(f"[debug_document_types] JSON: {json_dir}", flush=True)
    print(f"[debug_document_types] Excel: {xlsx_path}", flush=True)


if __name__ == "__main__":
    main()