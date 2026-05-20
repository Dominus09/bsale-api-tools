"""
Investigación inversa: desde boleta/factura hacia OC (solo lectura).

Flujo: localizar documento por folio en ``documents.json`` (``officeid=1``),
``details`` + ``relateddetailid`` + ``references``, comparar productos con OC objetivo.

Uso (raíz del repo, ``PG_*`` + ``BSALE_TOKEN``)::

    python -m backend.debug.debug_reverse_document_resolution
    python -m backend.debug.debug_reverse_document_resolution 2616098
    python -m backend.debug.debug_reverse_document_resolution 2616098 --oc-number 66697

Salida:
  ``exports/debug_reverse_resolution_{folio}.json``
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections import Counter
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from backend.db import get_connection
from backend.services.distribuidora.bsale_client import BsaleClient
from backend.services.distribuidora.bsale_params import merge_bsale_office_query
from backend.services.distribuidora.sync_related_service import _safe_int
from backend.utils.bsale_token_env import load_dotenv_if_available, read_bsale_token_from_env

COMPANY_ID = 3
OFFICE_ID = 1
DOC_TYPE_OC = 33
DOC_TYPE_BOLETA = 1
DEFAULT_BOLETA_NUMBER = 2616098
DEFAULT_OC_NUMBER = 66697
LIST_LIMIT = 50
DETAILS_LIMIT = 50
API_THROTTLE_SEC = 0.12


def _throttle() -> None:
    if API_THROTTLE_SEC > 0:
        time.sleep(API_THROTTLE_SEC)


def _document_root(payload: dict[str, Any]) -> dict[str, Any]:
    d = payload.get("document")
    if isinstance(d, dict) and d.get("id") is not None:
        return d
    return payload if isinstance(payload, dict) else {}


def _type_id(blob: dict[str, Any]) -> int | None:
    dt = blob.get("documentType") or blob.get("document_type")
    if isinstance(dt, dict):
        return _safe_int(dt.get("id"))
    return None


def _type_name(blob: dict[str, Any]) -> str | None:
    dt = blob.get("documentType") or blob.get("document_type")
    if isinstance(dt, dict):
        n = dt.get("name")
        return str(n) if n is not None else None
    return None


def _client_summary(blob: dict[str, Any]) -> dict[str, Any]:
    c = blob.get("client") or blob.get("cliente")
    if not isinstance(c, dict):
        return {"id": None, "name": None}
    name = c.get("company") or c.get("firstName") or c.get("name")
    if name and c.get("lastName"):
        name = f"{name} {c.get('lastName')}".strip()
    return {"id": _safe_int(c.get("id")), "name": name, "code": c.get("code")}


def _detail_line(item: dict[str, Any]) -> dict[str, Any]:
    variant = item.get("variant")
    variant_id = None
    if isinstance(variant, dict):
        variant_id = _safe_int(variant.get("id"))
    elif item.get("variant_id") is not None:
        variant_id = _safe_int(item.get("variant_id"))
    qty = item.get("quantity")
    try:
        qty_f = float(qty) if qty is not None else None
    except (TypeError, ValueError):
        qty_f = None
    return {
        "detail_id": _safe_int(item.get("id")),
        "quantity": qty,
        "quantity_float": qty_f,
        "variant_id": variant_id,
        "netUnitValue": item.get("netUnitValue") or item.get("net_unit_value"),
    }


def _reference_line(item: dict[str, Any]) -> dict[str, Any]:
    ref_doc = item.get("referenceDocument") or item.get("reference_document") or {}
    ref_type = item.get("referenceDocumentType") or item.get("reference_document_type") or {}
    if not isinstance(ref_doc, dict):
        ref_doc = {}
    if not isinstance(ref_type, dict):
        ref_type = {}
    return {
        "referenced_document_id": _safe_int(ref_doc.get("id") or item.get("referenceDocumentId")),
        "number": item.get("number") or ref_doc.get("number"),
        "reference_number_str": item.get("number"),
        "type": {
            "id": _safe_int(ref_type.get("id") or ref_doc.get("documentTypeId")),
            "name": ref_type.get("name") if isinstance(ref_type, dict) else None,
        },
        "relationType": item.get("relationType") or item.get("relation_type"),
        "totals": {
            "netAmount": item.get("netAmount") or item.get("net_amount"),
            "taxAmount": item.get("taxAmount") or item.get("tax_amount"),
            "totalAmount": item.get("totalAmount") or item.get("total_amount"),
        },
        "raw": item,
    }


def _related_doc_line(item: dict[str, Any]) -> dict[str, Any]:
    dt = item.get("documentType") or item.get("document_type")
    return {
        "id": _safe_int(item.get("id")),
        "number": item.get("number"),
        "document_type": {
            "id": _safe_int(dt.get("id")) if isinstance(dt, dict) else None,
            "name": dt.get("name") if isinstance(dt, dict) else None,
        },
        "totalAmount": item.get("totalAmount") or item.get("total_amount"),
        "emissionDate": item.get("emissionDate") or item.get("emission_date"),
        "raw": item,
    }


def _to_decimal(v: Any) -> Decimal | None:
    if v is None:
        return None
    try:
        return Decimal(str(v))
    except Exception:
        return None


def _emission_utc_date(blob: dict[str, Any]) -> date | None:
    raw = blob.get("emissionDate") or blob.get("emission_date")
    if raw is None:
        return None
    try:
        ts = int(raw)
        return datetime.fromtimestamp(ts, tz=timezone.utc).date()
    except (TypeError, ValueError, OSError):
        return None


def _resolve_db_document(
    cur,
    *,
    number: int,
    document_type_id: int | None = None,
) -> dict[str, Any] | None:
    if document_type_id is not None:
        cur.execute(
            """
            SELECT document_id, number, document_type_id, emission_date, total_amount, client_id, state
            FROM distribuidora.documents
            WHERE company_id = %s AND office_id = %s
              AND document_type_id = %s AND number = %s
            ORDER BY emission_date DESC NULLS LAST, document_id DESC
            LIMIT 1
            """,
            (COMPANY_ID, OFFICE_ID, document_type_id, number),
        )
    else:
        cur.execute(
            """
            SELECT document_id, number, document_type_id, emission_date, total_amount, client_id, state
            FROM distribuidora.documents
            WHERE company_id = %s AND office_id = %s AND number = %s
            ORDER BY emission_date DESC NULLS LAST, document_id DESC
            LIMIT 1
            """,
            (COMPANY_ID, OFFICE_ID, number),
        )
    row = cur.fetchone()
    if not row:
        return None
    return {
        "document_id": int(row[0]),
        "number": int(row[1]) if row[1] is not None else row[1],
        "document_type_id": int(row[2]) if row[2] is not None else None,
        "emission_date": row[3],
        "total_amount": row[4],
        "client_id": row[5],
        "state": row[6],
    }


def _search_documents_by_number(
    client: BsaleClient,
    number: int,
    *,
    documenttypeid: int | None = None,
) -> dict[str, Any]:
    """``GET /documents.json?number=&officeid=1`` (paginado)."""
    pages: list[dict[str, Any]] = []
    offset = 0
    while True:
        params: dict[str, Any] = {"number": number, "limit": LIST_LIMIT, "offset": offset}
        if documenttypeid is not None:
            params["documenttypeid"] = documenttypeid
        raw = client.get(
            "/documents.json",
            merge_bsale_office_query(params, OFFICE_ID, context="reverse_search_number"),
        )
        _throttle()
        pages.append({"offset": offset, "params": params, "raw": raw})
        items = raw.get("items") if isinstance(raw, dict) else None
        if not isinstance(items, list) or len(items) < LIST_LIMIT:
            break
        offset += len(items)
    return {"number": number, "documenttypeid_filter": documenttypeid, "pages": pages}


def _search_by_reference_number(
    client: BsaleClient,
    reference_number: int,
) -> dict[str, Any]:
    """Documentos que referencian un folio (p. ej. OC)."""
    raw = client.get(
        "/documents.json",
        merge_bsale_office_query(
            {"referencenumber": reference_number, "limit": LIST_LIMIT, "offset": 0},
            OFFICE_ID,
            context="reverse_referencenumber",
        ),
    )
    _throttle()
    return {"referencenumber": reference_number, "raw": raw}


def _collect_items_from_list_search(search_result: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for page in search_result.get("pages") or []:
        raw = page.get("raw")
        if not isinstance(raw, dict):
            continue
        items = raw.get("items")
        if isinstance(items, list):
            for it in items:
                if isinstance(it, dict):
                    out.append(it)
    return out


def _pick_boleta_candidate(
    items: list[dict[str, Any]],
    target_number: int,
) -> dict[str, Any] | None:
    """Elige el ítem cuyo folio coincide; prioriza tipo 1 (boleta)."""
    matches = []
    for it in items:
        num = _safe_int(it.get("number"))
        if num != target_number:
            continue
        matches.append(it)
    if not matches:
        return None
    for it in matches:
        if _type_id(it) == DOC_TYPE_BOLETA:
            return it
    return matches[0]


def _fetch_details_all(client: BsaleClient, document_id: int) -> dict[str, Any]:
    pages: list[dict[str, Any]] = []
    offset = 0
    while True:
        raw = client.get(
            f"/documents/{document_id}/details.json",
            {"limit": DETAILS_LIMIT, "offset": offset},
        )
        _throttle()
        pages.append({"offset": offset, "raw": raw})
        items = raw.get("items") if isinstance(raw, dict) else None
        if not isinstance(items, list) or len(items) < DETAILS_LIMIT:
            break
        offset += len(items)
    return {"document_id": document_id, "pages": pages}


def _details_items(details_resp: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for page in details_resp.get("pages") or []:
        raw = page.get("raw")
        if isinstance(raw, dict) and isinstance(raw.get("items"), list):
            for it in raw["items"]:
                if isinstance(it, dict):
                    out.append(it)
    return out


def _fetch_relateddetail_all(client: BsaleClient, detail_id: int) -> dict[str, Any]:
    pages: list[dict[str, Any]] = []
    offset = 0
    while True:
        raw = client.get(
            "/documents.json",
            merge_bsale_office_query(
                {"relateddetailid": detail_id, "limit": LIST_LIMIT, "offset": offset},
                OFFICE_ID,
                context=f"reverse_related detail={detail_id}",
            ),
        )
        _throttle()
        pages.append({"offset": offset, "raw": raw})
        items = raw.get("items") if isinstance(raw, dict) else None
        if not isinstance(items, list) or len(items) < LIST_LIMIT:
            break
        offset += len(items)
    return {"detail_id": detail_id, "pages": pages}


def _fetch_references(client: BsaleClient, document_id: int) -> dict[str, Any]:
    raw = client.get(f"/documents/{document_id}/references.json", None)
    _throttle()
    return {"document_id": document_id, "raw": raw}


def _reference_items(refs_wrap: dict[str, Any]) -> list[dict[str, Any]]:
    raw = refs_wrap.get("raw")
    if isinstance(raw, dict) and isinstance(raw.get("items"), list):
        return [x for x in raw["items"] if isinstance(x, dict)]
    return []


def _product_signature(lines: list[dict[str, Any]]) -> Counter[tuple[int | None, float | None]]:
    sig: Counter[tuple[int | None, float | None]] = Counter()
    for ln in lines:
        sig[(ln.get("variant_id"), ln.get("quantity_float"))] += 1
    return sig


def _compare_products(
    boleta_lines: list[dict[str, Any]],
    oc_lines: list[dict[str, Any]],
) -> dict[str, Any]:
    b_sig = _product_signature(boleta_lines)
    o_sig = _product_signature(oc_lines)
    if not b_sig:
        return {
            "product_match_percentage": 0.0,
            "boleta_lines": 0,
            "oc_lines": len(oc_lines),
            "matched_lines": 0,
            "boleta_variants": [],
            "oc_variants": [x.get("variant_id") for x in oc_lines],
            "only_in_boleta": list(b_sig.keys()),
            "only_in_oc": list(o_sig.keys()),
        }
    matched = 0
    for key, b_count in b_sig.items():
        o_count = o_sig.get(key, 0)
        matched += min(b_count, o_count)
    total_b = sum(b_sig.values())
    pct = round(100.0 * matched / total_b, 2) if total_b else 0.0
    only_b = [k for k in b_sig if k not in o_sig]
    only_o = [k for k in o_sig if k not in b_sig]
    return {
        "product_match_percentage": pct,
        "boleta_lines": total_b,
        "oc_lines": sum(o_sig.values()),
        "matched_lines": matched,
        "boleta_variants": sorted({k[0] for k in b_sig}),
        "oc_variants": sorted({k[0] for k in o_sig}),
        "only_in_boleta": only_b,
        "only_in_oc": only_o,
    }


def _ref_points_to_oc(
    ref_items: list[dict[str, Any]],
    *,
    oc_number: int,
    oc_document_id: int | None,
) -> dict[str, Any]:
    hits: list[dict[str, Any]] = []
    other_oc_33: list[dict[str, Any]] = []
    for item in ref_items:
        line = _reference_line(item)
        ref_num = _safe_int(line.get("number") or line.get("reference_number_str"))
        ref_id = line.get("referenced_document_id")
        tid = (line.get("type") or {}).get("id")
        if ref_num == oc_number or (oc_document_id and ref_id == oc_document_id):
            hits.append(line)
        elif tid == DOC_TYPE_OC or (ref_num is not None and tid == DOC_TYPE_OC):
            other_oc_33.append(line)
    return {
        "reference_to_target_oc": hits,
        "reference_to_other_oc_33": other_oc_33,
        "has_target_oc_reference": len(hits) > 0,
        "has_any_oc_33_reference": len(other_oc_33) > 0,
    }


def _related_points_to_oc(
    related_items: list[dict[str, Any]],
    *,
    oc_number: int,
    oc_document_id: int | None,
) -> list[dict[str, Any]]:
    hits: list[dict[str, Any]] = []
    for it in related_items:
        line = _related_doc_line(it)
        num = _safe_int(line.get("number"))
        did = line.get("id")
        tid = (line.get("document_type") or {}).get("id")
        if num == oc_number or (oc_document_id and did == oc_document_id):
            hits.append(line)
        elif tid == DOC_TYPE_OC:
            hits.append({**line, "note": "other_oc_33"})
    return hits


def _classify_final(
    *,
    linked_via: list[str],
    product_match_pct: float,
    same_client: bool,
    same_amount: bool,
    same_day: bool,
    ref_analysis: dict[str, Any],
    related_hits: list[dict[str, Any]],
) -> str:
    if "relateddetailid" in linked_via and ref_analysis.get("has_target_oc_reference"):
        return "A) linkage operational found"
    if "relateddetailid" in linked_via and related_hits:
        return "A) linkage operational found"
    if "references" in linked_via and ref_analysis.get("has_target_oc_reference"):
        return "B) references linkage only"
    if product_match_pct >= 80 and (same_client or same_amount or same_day):
        return "C) product coincidence only"
    if not linked_via and product_match_pct < 20:
        return "D) completely isolated documents"
    if product_match_pct >= 50 and not linked_via:
        return "C) product coincidence only"
    if ref_analysis.get("has_target_oc_reference") and "references" not in linked_via:
        return "E) probable manual/fuera flujo operacional"
    if not linked_via:
        return "D) completely isolated documents"
    return "E) probable manual/fuera flujo operacional"


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Resolución inversa boleta → OC (solo lectura).")
    p.add_argument(
        "boleta_number",
        nargs="?",
        type=int,
        default=DEFAULT_BOLETA_NUMBER,
        help=f"Folio boleta (default {DEFAULT_BOLETA_NUMBER})",
    )
    p.add_argument(
        "--oc-number",
        type=int,
        default=DEFAULT_OC_NUMBER,
        help=f"OC objetivo para comparar (default {DEFAULT_OC_NUMBER})",
    )
    p.add_argument("--document-id", type=int, default=None, help="document_id Bsale si ya se conoce")
    return p


def main() -> int:
    load_dotenv_if_available()
    args = _build_arg_parser().parse_args()
    boleta_number = args.boleta_number
    oc_number = args.oc_number

    token = read_bsale_token_from_env()
    if not token:
        print("Defina BSALE_TOKEN o BSALE_TOKEN_SPA.", file=sys.stderr)
        return 2

    client = BsaleClient(token)

    conn = get_connection()
    cur = conn.cursor()
    try:
        oc_db = _resolve_db_document(cur, number=oc_number, document_type_id=DOC_TYPE_OC)
        boleta_db = _resolve_db_document(cur, number=boleta_number, document_type_id=DOC_TYPE_BOLETA)
        if boleta_db is None:
            boleta_db = _resolve_db_document(cur, number=boleta_number, document_type_id=None)
    finally:
        cur.close()
        conn.close()

    out_path = _REPO / "exports" / f"debug_reverse_resolution_{boleta_number}.json"

    payload: dict[str, Any] = {
        "meta": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "boleta_number": boleta_number,
            "target_oc_number": oc_number,
            "read_only": True,
        },
        "boleta_search_by_number": None,
        "boleta_search_by_number_type1": None,
        "referencenumber_search_for_oc": None,
        "boleta_from_db": boleta_db,
        "oc_from_db": oc_db,
        "boleta_document_api": None,
        "boleta_details_response": None,
        "boleta_relateddetailid_responses": [],
        "boleta_references_response": None,
        "oc_details_response": None,
        "reference_detection": None,
        "related_detection": None,
        "product_comparison": None,
        "summary": None,
    }

    print("=" * 60)
    print(f"REVERSE DOCUMENT RESOLUTION — boleta {boleta_number} → OC {oc_number}")
    print("=" * 60)

    # --- 1) Buscar boleta en documents.json ---
    print("\n--- 1) GET /documents.json?number=…&officeid=1 ---")
    search_all = _search_documents_by_number(client, boleta_number)
    payload["boleta_search_by_number"] = search_all
    items_all = _collect_items_from_list_search(search_all)
    print(f"  ítems listados (sin filtro tipo): {len(items_all)}")

    search_t1 = _search_documents_by_number(client, boleta_number, documenttypeid=DOC_TYPE_BOLETA)
    payload["boleta_search_by_number_type1"] = search_t1
    items_t1 = _collect_items_from_list_search(search_t1)
    print(f"  ítems listados (documenttypeid=1): {len(items_t1)}")

    candidate = _pick_boleta_candidate(items_t1 or items_all, boleta_number)
    if candidate is None and args.document_id is not None:
        doc_id = args.document_id
        print(f"  sin match en listado; usando --document-id={doc_id}")
    elif candidate is not None:
        doc_id = int(candidate["id"])
        print(f"  candidato API: document_id={doc_id} type={_type_id(candidate)} number={candidate.get('number')}")
    elif boleta_db:
        doc_id = int(boleta_db["document_id"])
        print(f"  sin match API; usando BD document_id={doc_id}")
    else:
        print(f"  ERROR: no se encontró boleta {boleta_number} en API ni BD.", file=sys.stderr)
        return 2

    # --- Extra: documentos que referencian OC por referencenumber ---
    print(f"\n--- Extra) GET /documents.json?referencenumber={oc_number}&officeid=1 ---")
    refnum_search = _search_by_reference_number(client, oc_number)
    payload["referencenumber_search_for_oc"] = refnum_search
    refnum_items = []
    raw_rn = refnum_search.get("raw")
    if isinstance(raw_rn, dict) and isinstance(raw_rn.get("items"), list):
        refnum_items = [x for x in raw_rn["items"] if isinstance(x, dict)]
    print(f"  documentos con referencenumber={oc_number}: {len(refnum_items)}")
    boleta_in_refnum = any(_safe_int(x.get("number")) == boleta_number for x in refnum_items)
    print(f"  boleta {boleta_number} presente en ese listado: {boleta_in_refnum}")

    # --- 2) Documento completo ---
    doc_api = client.get(f"/documents/{doc_id}.json", None)
    _throttle()
    payload["boleta_document_api"] = {"raw": doc_api}
    root = _document_root(doc_api)
    client_info = _client_summary(root)

    print("\n--- 2) Boleta /documents/{id}.json ---")
    print(f"  document_id:     {doc_id}")
    print(f"  document_type:   {_type_id(root)} ({_type_name(root)})")
    print(f"  totalAmount:     {root.get('totalAmount')}")
    print(f"  emissionDate:    {root.get('emissionDate')}")
    print(f"  client:          {client_info}")
    print(f"  state:           {root.get('state')}")

    # --- 3) Details boleta ---
    print("\n--- 3) GET /documents/{id}/details.json (boleta) ---")
    boleta_details = _fetch_details_all(client, doc_id)
    payload["boleta_details_response"] = boleta_details
    boleta_detail_items = _details_items(boleta_details)
    boleta_lines = [_detail_line(it) for it in boleta_detail_items]
    for ln in boleta_lines:
        print(
            f"  detail_id={ln['detail_id']} quantity={ln['quantity']} "
            f"variant_id={ln['variant_id']} netUnitValue={ln['netUnitValue']}",
        )

    # --- 4) relateddetailid por detail ---
    print("\n--- 4) relateddetailid por línea boleta ---")
    all_related_hits: list[dict[str, Any]] = []
    oc_document_id = int(oc_db["document_id"]) if oc_db else None
    for ln in boleta_lines:
        did = ln.get("detail_id")
        if did is None:
            continue
        rel_wrap = _fetch_relateddetail_all(client, int(did))
        payload["boleta_relateddetailid_responses"].append(rel_wrap)
        rel_items: list[dict[str, Any]] = []
        for page in rel_wrap.get("pages") or []:
            raw = page.get("raw")
            if isinstance(raw, dict) and isinstance(raw.get("items"), list):
                rel_items.extend(x for x in raw["items"] if isinstance(x, dict))
        if not rel_items:
            print(f"  detail {did} → 0 relacionados")
        for it in rel_items:
            line = _related_doc_line(it)
            print(
                f"  detail {did} → id={line['id']} number={line['number']} "
                f"type={line['document_type']['id']} ({line['document_type']['name']})",
            )
        hits = _related_points_to_oc(
            rel_items,
            oc_number=oc_number,
            oc_document_id=oc_document_id,
        )
        all_related_hits.extend(hits)

    payload["related_detection"] = {
        "hits_to_target_oc": all_related_hits,
        "any_related_items": sum(
            len((p.get("raw") or {}).get("items") or [])
            for r in payload["boleta_relateddetailid_responses"]
            for p in r.get("pages") or []
            if isinstance(p.get("raw"), dict)
        ),
    }

    # --- 5) references boleta ---
    print("\n--- 5) GET /documents/{id}/references.json (boleta) ---")
    refs_wrap = _fetch_references(client, doc_id)
    payload["boleta_references_response"] = refs_wrap
    ref_items = _reference_items(refs_wrap)
    ref_lines = [_reference_line(x) for x in ref_items]
    if not ref_lines:
        print("  (vacío)")
    for rl in ref_lines:
        print(f"  RAW resumen: {json.dumps(rl, ensure_ascii=False, default=str)}")

    ref_analysis = _ref_points_to_oc(
        ref_items,
        oc_number=oc_number,
        oc_document_id=oc_document_id,
    )
    payload["reference_detection"] = ref_analysis

    print("\n--- 6) Detección enlace hacia OC ---")
    print(f"  A) referencia directa OC {oc_number}: {ref_analysis['has_target_oc_reference']}")
    if ref_analysis["reference_to_target_oc"]:
        for h in ref_analysis["reference_to_target_oc"]:
            print(f"     → {h}")
    print(f"  B) referencia otra OC tipo 33: {ref_analysis['has_any_oc_33_reference']}")
    for h in ref_analysis["reference_to_other_oc_33"]:
        print(f"     → {h}")
    print(f"  relateddetailid → OC objetivo: {len(all_related_hits)} hit(s)")
    for h in all_related_hits:
        print(f"     → {h}")

    # --- 7) Comparar productos con OC ---
    print(f"\n--- 7) Comparación productos boleta vs OC {oc_number} ---")
    oc_lines: list[dict[str, Any]] = []
    oc_root: dict[str, Any] = {}
    if oc_db:
        oc_id = int(oc_db["document_id"])
        oc_details = _fetch_details_all(client, oc_id)
        payload["oc_details_response"] = oc_details
        oc_lines = [_detail_line(it) for it in _details_items(oc_details)]
        oc_api = client.get(f"/documents/{oc_id}.json", None)
        _throttle()
        oc_root = _document_root(oc_api)
        print(f"  OC document_id={oc_id} líneas={len(oc_lines)}")
    else:
        print("  OC no encontrada en BD local; comparación productos limitada.")

    product_cmp = _compare_products(boleta_lines, oc_lines)
    payload["product_comparison"] = product_cmp
    print(f"  product_match_percentage: {product_cmp['product_match_percentage']}%")
    print(f"  only_in_boleta: {product_cmp.get('only_in_boleta')}")
    print(f"  only_in_oc: {product_cmp.get('only_in_oc')}")

    oc_client = _client_summary(oc_root) if oc_root else {"id": oc_db.get("client_id") if oc_db else None}
    same_client = (
        client_info.get("id") is not None
        and oc_client.get("id") is not None
        and client_info.get("id") == oc_client.get("id")
    )
    b_total = _to_decimal(root.get("totalAmount"))
    o_total = _to_decimal(oc_root.get("totalAmount") if oc_root else (oc_db or {}).get("total_amount"))
    same_amount = (
        b_total is not None and o_total is not None and abs(b_total - o_total) < Decimal("0.01")
    )
    b_day = _emission_utc_date(root)
    o_day = _emission_utc_date(oc_root) if oc_root else None
    if o_day is None and oc_db and oc_db.get("emission_date"):
        try:
            o_day = oc_db["emission_date"].astimezone(timezone.utc).date()
        except Exception:
            o_day = None
    same_day = b_day is not None and o_day is not None and b_day == o_day

    linked_via: list[str] = []
    if all_related_hits:
        linked_via.append("relateddetailid")
    if ref_analysis.get("has_target_oc_reference"):
        linked_via.append("references")
    if boleta_in_refnum and "references" not in linked_via:
        linked_via.append("referencenumber_listing")
    if not linked_via:
        linked_via.append("none")

    likely_same = (
        product_cmp["product_match_percentage"] >= 70
        and (same_client or same_amount or same_day)
    ) or bool(all_related_hits) or ref_analysis.get("has_target_oc_reference")

    classification = _classify_final(
        linked_via=linked_via,
        product_match_pct=float(product_cmp["product_match_percentage"]),
        same_client=same_client,
        same_amount=same_amount,
        same_day=same_day,
        ref_analysis=ref_analysis,
        related_hits=all_related_hits,
    )

    summary = {
        "boleta_document_id": doc_id,
        "boleta_number": boleta_number,
        "linked_oc_found": bool(all_related_hits or ref_analysis.get("has_target_oc_reference")),
        "linked_oc_number": oc_number if (
            all_related_hits or ref_analysis.get("has_target_oc_reference")
        ) else None,
        "linked_via": linked_via,
        "product_match_percentage": product_cmp["product_match_percentage"],
        "same_client": same_client,
        "same_amount": same_amount,
        "same_day": same_day,
        "likely_same_operation": likely_same,
        "boleta_in_referencenumber_search_for_oc": boleta_in_refnum,
        "final_classification": classification,
        "reference_detection": ref_analysis,
        "related_hits_count": len(all_related_hits),
    }
    payload["summary"] = summary

    print("\n" + "=" * 50)
    print("REVERSE DOCUMENT ANALYSIS SUMMARY")
    print("=" * 50)
    print(f"boleta_document_id:       {summary['boleta_document_id']}")
    print(f"linked_oc_found:          {summary['linked_oc_found']}")
    print(f"linked_oc_number:         {summary['linked_oc_number']}")
    print(f"linked_via:               {summary['linked_via']}")
    print(f"product_match_percentage: {summary['product_match_percentage']}")
    print(f"same_client:              {summary['same_client']}")
    print(f"same_amount:              {summary['same_amount']}")
    print(f"same_day:                 {summary['same_day']}")
    print(f"likely_same_operation:    {summary['likely_same_operation']}")
    print(f"\nFINAL CLASSIFICATION:\n{summary['final_classification']}")
    print("=" * 50)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    print(f"\nRAW JSON: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
