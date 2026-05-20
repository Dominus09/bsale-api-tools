"""
Investigación profunda OC → factura: relateddetailid + references (solo lectura).

Resuelve la OC en ``distribuidora.documents``, consulta Bsale sin filtrar tipos,
expande OC hijas (tipo 33) hasta profundidad 5 y guarda JSON RAW completo.

Uso (raíz del repo, ``PG_*`` + ``BSALE_TOKEN``)::

    python -m backend.debug.debug_full_oc_resolution
    python -m backend.debug.debug_full_oc_resolution 66697
    python -m backend.debug.debug_full_oc_resolution --document-id 12345

Salida:
  ``exports/debug_oc_{número}_full_resolution.json``
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
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
TERMINAL_TYPES = frozenset({1, 6, 9})
DEFAULT_OC_NUMBER = 66697
MAX_DEPTH = 5
DETAILS_PAGE_LIMIT = 50
RELATED_PAGE_LIMIT = 50
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
        return {"id": None, "name": None, "raw": c}
    name = c.get("company") or c.get("firstName") or c.get("name")
    if name and c.get("lastName"):
        name = f"{name} {c.get('lastName')}".strip()
    return {
        "id": _safe_int(c.get("id")),
        "name": name,
        "code": c.get("code"),
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
    }


def _detail_line(item: dict[str, Any]) -> dict[str, Any]:
    variant = item.get("variant")
    variant_id = None
    if isinstance(variant, dict):
        variant_id = _safe_int(variant.get("id"))
    elif item.get("variant_id") is not None:
        variant_id = _safe_int(item.get("variant_id"))
    return {
        "detail_id": _safe_int(item.get("id")),
        "quantity": item.get("quantity"),
        "netUnitValue": item.get("netUnitValue") or item.get("net_unit_value"),
        "variant_id": variant_id,
        "raw_keys": sorted(item.keys()),
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
        "type": {
            "id": _safe_int(ref_type.get("id") or ref_doc.get("documentTypeId")),
            "name": ref_type.get("name"),
        },
        "relationType": item.get("relationType") or item.get("relation_type"),
        "totals": {
            "netAmount": item.get("netAmount") or item.get("net_amount"),
            "taxAmount": item.get("taxAmount") or item.get("tax_amount"),
            "totalAmount": item.get("totalAmount") or item.get("total_amount"),
        },
        "raw": item,
    }


def _resolve_from_db(cur, *, oc_number: int | None, document_id: int | None) -> dict[str, Any] | None:
    if document_id is not None:
        cur.execute(
            """
            SELECT document_id, number, document_type_id, emission_date, total_amount,
                   client_id, state, commercial_state
            FROM distribuidora.documents
            WHERE company_id = %s AND office_id = %s AND document_id = %s
            LIMIT 1
            """,
            (COMPANY_ID, OFFICE_ID, document_id),
        )
    else:
        cur.execute(
            """
            SELECT document_id, number, document_type_id, emission_date, total_amount,
                   client_id, state, commercial_state
            FROM distribuidora.documents
            WHERE company_id = %s AND office_id = %s
              AND document_type_id = %s AND number = %s
            LIMIT 1
            """,
            (COMPANY_ID, OFFICE_ID, DOC_TYPE_OC, oc_number),
        )
    row = cur.fetchone()
    if not row:
        return None
    cur.execute(
        """
        SELECT COUNT(*)::int
        FROM distribuidora.document_references
        WHERE source_document_id = %s
        """,
        (int(row[0]),),
    )
    ref_count_row = cur.fetchone()
    refs_count = int(ref_count_row[0]) if ref_count_row else 0
    return {
        "document_id": int(row[0]),
        "number": int(row[1]) if row[1] is not None else row[1],
        "document_type_id": int(row[2]) if row[2] is not None else None,
        "emission_date": row[3],
        "total_amount": row[4],
        "client_id": row[5],
        "state": row[6],
        "commercial_state": row[7],
        "references_count_db": refs_count,
    }


def _fetch_document_json(client: BsaleClient, document_id: int) -> dict[str, Any]:
    raw = client.get(f"/documents/{document_id}.json", None)
    _throttle()
    return {"request_path": f"/documents/{document_id}.json", "raw": raw}


def _fetch_details_all(client: BsaleClient, document_id: int) -> dict[str, Any]:
    pages: list[dict[str, Any]] = []
    offset = 0
    while True:
        raw = client.get(
            f"/documents/{document_id}/details.json",
            {"limit": DETAILS_PAGE_LIMIT, "offset": offset},
        )
        _throttle()
        pages.append({"offset": offset, "raw": raw})
        items = raw.get("items") if isinstance(raw, dict) else None
        if not isinstance(items, list) or len(items) < DETAILS_PAGE_LIMIT:
            break
        offset += len(items)
    return {
        "document_id": document_id,
        "pages": pages,
    }


def _fetch_relateddetail_all(
    client: BsaleClient,
    detail_id: int,
) -> dict[str, Any]:
    pages: list[dict[str, Any]] = []
    offset = 0
    while True:
        raw = client.get(
            "/documents.json",
            merge_bsale_office_query(
                {
                    "relateddetailid": detail_id,
                    "limit": RELATED_PAGE_LIMIT,
                    "offset": offset,
                },
                OFFICE_ID,
                context=f"debug_full_oc_resolution detail={detail_id}",
            ),
        )
        _throttle()
        pages.append({"offset": offset, "raw": raw})
        items = raw.get("items") if isinstance(raw, dict) else None
        if not isinstance(items, list) or len(items) < RELATED_PAGE_LIMIT:
            break
        offset += len(items)
    return {
        "detail_id": detail_id,
        "query": {
            "relateddetailid": detail_id,
            "officeid": OFFICE_ID,
        },
        "pages": pages,
    }


def _fetch_references_all(client: BsaleClient, document_id: int) -> dict[str, Any]:
    raw = client.get(f"/documents/{document_id}/references.json", None)
    _throttle()
    return {
        "document_id": document_id,
        "request_path": f"/documents/{document_id}/references.json",
        "raw": raw,
    }


def _items_from_details_response(details_resp: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for page in details_resp.get("pages") or []:
        raw = page.get("raw") if isinstance(page, dict) else None
        if not isinstance(raw, dict):
            continue
        items = raw.get("items")
        if isinstance(items, list):
            for it in items:
                if isinstance(it, dict):
                    out.append(it)
    return out


def _items_from_related_response(rel_resp: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for page in rel_resp.get("pages") or []:
        raw = page.get("raw") if isinstance(page, dict) else None
        if not isinstance(raw, dict):
            continue
        items = raw.get("items")
        if isinstance(items, list):
            for it in items:
                if isinstance(it, dict):
                    out.append(it)
    return out


def _walk_oc(
    client: BsaleClient,
    document_id: int,
    *,
    depth: int,
    root_document_id: int,
    payload: dict[str, Any],
    visited_oc: set[int],
    stats: dict[str, Any],
) -> list[dict[str, Any]]:
    """Recorre details + relateddetailid; devuelve nodos del árbol de recursión."""
    nodes: list[dict[str, Any]] = []
    if document_id in visited_oc:
        loop = {
            "document_id": document_id,
            "depth": depth,
            "status": "LOOP_DETECTED",
            "root_document_id": root_document_id,
        }
        stats["loops_detected"].append(loop)
        nodes.append(loop)
        return nodes

    visited_oc.add(document_id)
    node_entry: dict[str, Any] = {
        "document_id": document_id,
        "depth": depth,
        "status": "OK",
        "details": [],
    }
    nodes.append(node_entry)

    details_key = str(document_id)
    if details_key not in payload["details_response"]:
        payload["details_response"][details_key] = _fetch_details_all(client, document_id)
    details_resp = payload["details_response"][details_key]
    detail_items = _items_from_details_response(details_resp)
    stats["details_count"] += len(detail_items)

    for dit in detail_items:
        detail_id = _safe_int(dit.get("id"))
        if detail_id is None:
            continue
        dline = _detail_line(dit)
        detail_branch: dict[str, Any] = {
            "detail": dline,
            "relateddetailid": None,
            "related_documents_printed": [],
            "child_oc_branches": [],
        }
        node_entry["details"].append(detail_branch)

        print(f"  detail_id={dline['detail_id']} quantity={dline['quantity']} "
              f"netUnitValue={dline['netUnitValue']} variant_id={dline['variant_id']}")

        rel_resp = _fetch_relateddetail_all(client, detail_id)
        payload["relateddetailid_responses"].append(
            {
                "parent_document_id": document_id,
                "depth": depth,
                "root_document_id": root_document_id,
                **rel_resp,
            },
        )
        detail_branch["relateddetailid"] = rel_resp
        related_items = _items_from_related_response(rel_resp)

        if not related_items:
            print(f"    relateddetailid={detail_id} → 0 documentos")
        for rit in related_items:
            line = _related_doc_line(rit)
            detail_branch["related_documents_printed"].append(line)
            stats["related_relations_found"] += 1
            print(
                f"    related → id={line['id']} number={line['number']} "
                f"type={line['document_type']['id']} ({line['document_type']['name']}) "
                f"totalAmount={line['totalAmount']} emissionDate={line['emissionDate']}",
            )
            tid = line["document_type"]["id"]
            rid = line["id"]
            if tid in TERMINAL_TYPES and rid is not None:
                stats["terminal_docs_found"].append(
                    {
                        "document_id": rid,
                        "number": line["number"],
                        "document_type_id": tid,
                        "via_detail_id": detail_id,
                        "from_document_id": document_id,
                        "depth": depth,
                        "via": "relateddetailid_direct" if depth == 0 else "relateddetailid_nested",
                    },
                )
                if depth == 0:
                    stats["related_direct"] = True
                else:
                    stats["related_via_oc33"] = True
            elif tid == DOC_TYPE_OC and rid is not None:
                if depth >= MAX_DEPTH:
                    detail_branch["child_oc_branches"].append(
                        {"document_id": rid, "status": "MAX_DEPTH_REACHED"},
                    )
                    continue
                child_nodes = _walk_oc(
                    client,
                    int(rid),
                    depth=depth + 1,
                    root_document_id=root_document_id,
                    payload=payload,
                    visited_oc=visited_oc,
                    stats=stats,
                )
                detail_branch["child_oc_branches"].extend(child_nodes)
                stats["related_via_oc33"] = True

    stats["max_depth"] = max(stats["max_depth"], depth)
    return nodes


def _classify(stats: dict[str, Any], references_items: list[dict[str, Any]]) -> str:
    has_related = stats["related_relations_found"] > 0
    has_refs = len(references_items) > 0
    has_terminal = len(stats["terminal_docs_found"]) > 0
    state = stats.get("root_state")
    state_invoiced_hint = state is not None and int(state) != 0

    if has_related and stats.get("related_via_oc33"):
        return "B) relateddetailid via OC33"
    if has_related and has_terminal:
        return "A) relateddetailid direct"
    if has_related:
        return "A) relateddetailid direct"
    if has_refs and not has_related:
        if state_invoiced_hint or stats.get("references_count_db", 0) > 0:
            return "E) inconsistent Bsale linkage"
        return "C) references only"
    if state_invoiced_hint and not has_related:
        return "E) inconsistent Bsale linkage"
    return "D) no relation found"


def _print_references_lines(references_items: list[dict[str, Any]]) -> None:
    print("\n--- GET /documents/{id}/references.json (RAW resumen) ---")
    if not references_items:
        print("  (sin ítems en references.json)")
    for ref in references_items:
        line = _reference_line(ref)
        print(
            f"  ref_doc_id={line['referenced_document_id']} number={line['number']} "
            f"type={line['type']} relationType={line['relationType']} totals={line['totals']}",
        )


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Resolución completa OC: details + relateddetailid + references (solo lectura).",
    )
    p.add_argument(
        "oc_number",
        nargs="?",
        type=int,
        default=DEFAULT_OC_NUMBER,
        help=f"Número OC (default {DEFAULT_OC_NUMBER})",
    )
    p.add_argument("--document-id", type=int, default=None, help="Bsale document_id (alternativa)")
    return p


def main() -> int:
    load_dotenv_if_available()
    args = _build_arg_parser().parse_args()

    token = read_bsale_token_from_env()
    if not token:
        print("Defina BSALE_TOKEN o BSALE_TOKEN_SPA.", file=sys.stderr)
        return 2

    conn = get_connection()
    cur = conn.cursor()
    try:
        db_row = _resolve_from_db(
            cur,
            oc_number=args.oc_number if args.document_id is None else None,
            document_id=args.document_id,
        )
    finally:
        cur.close()
        conn.close()

    client = BsaleClient(token)

    if db_row is None:
        if args.document_id is None:
            print(
                f"OC {args.oc_number} no encontrada en distribuidora.documents "
                f"(company={COMPANY_ID} office={OFFICE_ID} type={DOC_TYPE_OC}).",
                file=sys.stderr,
            )
            return 2
        root_document_id = args.document_id
        root_number = args.oc_number
        db_row = {"document_id": root_document_id, "number": root_number, "source": "cli_only"}
    else:
        root_document_id = int(db_row["document_id"])
        root_number = db_row.get("number") or args.oc_number

    oc_label = root_number if root_number is not None else args.oc_number
    out_path = _REPO / "exports" / f"debug_oc_{oc_label}_full_resolution.json"

    print("=" * 60)
    print(f"FULL OC RESOLUTION — OC {oc_label} (document_id={root_document_id})")
    print("=" * 60)

    print("\n--- PostgreSQL distribuidora.documents ---")
    if db_row:
        print(f"  document_id:        {db_row.get('document_id')}")
        print(f"  number:             {db_row.get('number')}")
        print(f"  totalAmount (DB):   {db_row.get('total_amount')}")
        print(f"  emissionDate (DB):  {db_row.get('emission_date')}")
        print(f"  client_id (DB):     {db_row.get('client_id')}")
        print(f"  state (DB):         {db_row.get('state')}")
        print(f"  references (DB):  {db_row.get('references_count_db')}")

    doc_api = _fetch_document_json(client, root_document_id)
    root_blob = _document_root(doc_api.get("raw") or {})
    client_info = _client_summary(root_blob)

    print("\n--- API /documents/{id}.json ---")
    print(f"  number:             {root_blob.get('number')}")
    print(f"  totalAmount:        {root_blob.get('totalAmount')}")
    print(f"  emissionDate:       {root_blob.get('emissionDate')}")
    print(f"  client:             {client_info}")
    print(f"  state:              {root_blob.get('state')}")

    payload: dict[str, Any] = {
        "meta": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "oc_number": oc_label,
            "company_id": COMPANY_ID,
            "office_id": OFFICE_ID,
            "read_only": True,
        },
        "root_from_db": db_row,
        "root_document_api": doc_api,
        "details_response": {},
        "relateddetailid_responses": [],
        "references_response": None,
        "recursion_tree": [],
    }

    stats: dict[str, Any] = {
        "details_count": 0,
        "related_relations_found": 0,
        "references_found": 0,
        "terminal_docs_found": [],
        "max_depth": 0,
        "loops_detected": [],
        "related_direct": False,
        "related_via_oc33": False,
        "root_state": db_row.get("state") if db_row else root_blob.get("state"),
        "references_count_db": (db_row or {}).get("references_count_db", 0),
    }

    print("\n--- GET /documents/{id}/details.json + relateddetailid por línea ---")
    visited: set[int] = set()
    tree = _walk_oc(
        client,
        root_document_id,
        depth=0,
        root_document_id=root_document_id,
        payload=payload,
        visited_oc=visited,
        stats=stats,
    )
    payload["recursion_tree"] = tree

    refs_wrap = _fetch_references_all(client, root_document_id)
    payload["references_response"] = refs_wrap
    refs_raw = refs_wrap.get("raw") if isinstance(refs_wrap, dict) else {}
    ref_items: list[dict[str, Any]] = []
    if isinstance(refs_raw, dict):
        items = refs_raw.get("items")
        if isinstance(items, list):
            ref_items = [x for x in items if isinstance(x, dict)]
    stats["references_found"] = len(ref_items)
    _print_references_lines(ref_items)

    classification = _classify(stats, ref_items)

    summary = {
        "root_oc": oc_label,
        "root_document_id": root_document_id,
        "details_count": stats["details_count"],
        "related_relations_found": stats["related_relations_found"],
        "references_found": stats["references_found"],
        "terminal_docs_found": len(stats["terminal_docs_found"]),
        "terminal_docs": stats["terminal_docs_found"],
        "max_depth": stats["max_depth"],
        "loops_detected": len(stats["loops_detected"]),
        "loops": stats["loops_detected"],
        "final_classification": classification,
    }
    payload["summary"] = summary

    print("\n" + "=" * 50)
    print("FULL OC RESOLUTION SUMMARY")
    print("=" * 50)
    print(f"root_oc:                  {summary['root_oc']}")
    print(f"root_document_id:         {summary['root_document_id']}")
    print(f"details_count:            {summary['details_count']}")
    print(f"related_relations_found:  {summary['related_relations_found']}")
    print(f"references_found:         {summary['references_found']}")
    print(f"terminal_docs_found:      {summary['terminal_docs_found']}")
    print(f"max_depth:                {summary['max_depth']}")
    print(f"loops_detected:           {summary['loops_detected']}")
    print(f"\nFINAL CLASSIFICATION:\n{summary['final_classification']}")
    print("=" * 50)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    print(f"\nRAW JSON guardado: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
