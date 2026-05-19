"""
FASE 7.8 — Investigación: grafo ``relateddetailid`` desde una OC (tipo 33).

Solo lectura (API Bsale + SELECT PostgreSQL). No escribe ``document_related`` ni modifica datos.

Uso:
  python -m backend.debug.debug_related_graph_oc 66615
  python -m backend.debug.debug_related_graph_oc --document-id 3707537
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from backend.db import get_connection
from backend.services.distribuidora.bsale_client import BsaleClient
from backend.services.distribuidora.bsale_params import merge_bsale_office_query
from backend.services.distribuidora.sync_related_service import (
    _parse_related_document_blob,
    _safe_int,
)
from backend.utils.bsale_token_env import load_dotenv_if_available, read_bsale_token_from_env

COMPANY_ID = 3
OFFICE_ID = 1
DOC_TYPE_OC = 33
TERMINAL_TYPES = frozenset({1, 6, 9})
DETAILS_LIMIT = 50
RELATED_LIMIT = 50
MAX_DEPTH = 5
API_THROTTLE_SEC = 0.1


def _client_summary(blob: dict[str, Any]) -> dict[str, Any]:
    c = blob.get("client") or blob.get("cliente")
    if not isinstance(c, dict):
        return {"id": None, "name": None}
    return {
        "id": _safe_int(c.get("id")),
        "name": c.get("company") or c.get("firstName") or c.get("name"),
    }


def _document_root(payload: dict[str, Any]) -> dict[str, Any]:
    d = payload.get("document")
    if isinstance(d, dict) and d.get("id") is not None:
        return d
    return payload if isinstance(payload, dict) else {}


def _node_from_blob(document_id: int, blob: dict[str, Any]) -> dict[str, Any]:
    dt = blob.get("documentType") or blob.get("document_type")
    type_id = _safe_int(dt.get("id")) if isinstance(dt, dict) else None
    return {
        "document_id": document_id,
        "number": blob.get("number"),
        "document_type_id": type_id,
        "emissionDate": blob.get("emissionDate") or blob.get("emission_date"),
        "totalAmount": blob.get("totalAmount") or blob.get("total_amount"),
        "client": _client_summary(blob),
        "state": blob.get("state"),
    }


def _fetch_details_ids(client: BsaleClient, document_id: int) -> tuple[list[int], int]:
    ids: list[int] = []
    offset = 0
    calls = 0
    while True:
        data = client.get(
            f"/documents/{document_id}/details.json",
            {"limit": DETAILS_LIMIT, "offset": offset},
        )
        calls += 1
        items = data.get("items") or []
        if not isinstance(items, list):
            break
        for it in items:
            if isinstance(it, dict) and it.get("id") is not None:
                ids.append(int(it["id"]))
        if len(items) < DETAILS_LIMIT:
            break
        offset += len(items)
        if API_THROTTLE_SEC:
            time.sleep(API_THROTTLE_SEC)
    return ids, calls


def _fetch_related_items(client: BsaleClient, detail_id: int) -> tuple[list[dict[str, Any]], int]:
    merged: list[dict[str, Any]] = []
    offset = 0
    calls = 0
    while True:
        data = client.get(
            "/documents.json",
            merge_bsale_office_query(
                {
                    "relateddetailid": detail_id,
                    "limit": RELATED_LIMIT,
                    "offset": offset,
                },
                OFFICE_ID,
            ),
        )
        calls += 1
        items = data.get("items") or []
        if not isinstance(items, list):
            break
        for it in items:
            if isinstance(it, dict):
                merged.append(it)
        if len(items) < RELATED_LIMIT:
            break
        offset += len(items)
        if API_THROTTLE_SEC:
            time.sleep(API_THROTTLE_SEC)
    return merged, calls


def _fetch_document_json(client: BsaleClient, document_id: int) -> tuple[dict[str, Any] | None, int]:
    try:
        raw = client.get(f"/documents/{document_id}.json", None)
        return _document_root(raw), 1
    except Exception:
        return None, 1


def _resolve_root_from_db(cur, *, oc_number: int | None, document_id: int | None) -> dict[str, Any] | None:
    if document_id is not None:
        cur.execute(
            """
            SELECT document_id, number, document_type_id, emission_date, total_amount, client_id
            FROM distribuidora.documents
            WHERE company_id = %s AND office_id = %s AND document_id = %s
            LIMIT 1
            """,
            (COMPANY_ID, OFFICE_ID, document_id),
        )
    else:
        cur.execute(
            """
            SELECT document_id, number, document_type_id, emission_date, total_amount, client_id
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
    return {
        "document_id": int(row[0]),
        "number": int(row[1]) if row[1] is not None else row[1],
        "document_type_id": int(row[2]) if row[2] is not None else None,
        "emission_date": row[3],
        "total_amount": row[4],
        "client_id": row[5],
    }


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Grafo relateddetailid desde una OC (tipo 33). Solo lectura.",
    )
    p.add_argument(
        "oc_number",
        nargs="?",
        type=int,
        help="Número de OC (folio) en distribuidora.documents",
    )
    p.add_argument(
        "--document-id",
        type=int,
        default=None,
        metavar="ID",
        help="Bsale document_id de la OC (alternativa al número)",
    )
    return p


def run_graph(
    client: BsaleClient,
    root_document_id: int,
    root_number: int | None,
    *,
    root_source: str,
) -> dict[str, Any]:
    nodes: dict[str, dict[str, Any]] = {}
    edges: list[dict[str, Any]] = []
    terminal_documents: list[dict[str, Any]] = []
    loops_detected: list[dict[str, Any]] = []
    unresolved_branches: list[dict[str, Any]] = []
    visited_oc: set[int] = set()
    api_calls = 0

    def ensure_node(doc_id: int, blob_hint: dict[str, Any] | None) -> dict[str, Any]:
        nonlocal api_calls
        key = str(doc_id)
        if key in nodes:
            return nodes[key]
        blob = blob_hint
        if blob is None or not blob.get("number"):
            b, c = _fetch_document_json(client, doc_id)
            api_calls += c
            blob = b or {}
        node = _node_from_blob(doc_id, blob)
        nodes[key] = node
        return node

    def expand_oc(doc_id: int, depth: int, parent_detail_id: int | None, parent_doc_id: int | None) -> None:
        nonlocal api_calls
        if doc_id in visited_oc:
            loops_detected.append(
                {
                    "document_id": doc_id,
                    "from_document_id": parent_doc_id,
                    "via_detail_id": parent_detail_id,
                    "depth": depth,
                    "status": "LOOP_DETECTED",
                },
            )
            return
        visited_oc.add(doc_id)

        root_blob, c0 = _fetch_document_json(client, doc_id)
        api_calls += c0
        ensure_node(doc_id, root_blob or {})

        detail_ids, c1 = _fetch_details_ids(client, doc_id)
        api_calls += c1

        if not detail_ids:
            unresolved_branches.append(
                {
                    "document_id": doc_id,
                    "depth": depth,
                    "reason": "no_details",
                    "parent_document_id": parent_doc_id,
                    "via_detail_id": parent_detail_id,
                },
            )

        for detail_id in detail_ids:
            items, c2 = _fetch_related_items(client, detail_id)
            api_calls += c2
            if not items:
                unresolved_branches.append(
                    {
                        "document_id": doc_id,
                        "depth": depth,
                        "reason": "no_related_for_detail",
                        "detail_id": detail_id,
                    },
                )
            for item in items:
                rid, rtype, blob, err = _parse_related_document_blob(item)
                if err or rid is None:
                    unresolved_branches.append(
                        {
                            "document_id": doc_id,
                            "depth": depth,
                            "reason": "parse_error",
                            "detail_id": detail_id,
                            "message": err or "sin related_id",
                        },
                    )
                    continue

                to_node = ensure_node(rid, blob)
                edge = {
                    "from_document_id": doc_id,
                    "from_detail_id": detail_id,
                    "to_document_id": rid,
                    "to_document_type_id": rtype,
                    "depth": depth,
                    "status": "OK",
                }
                edges.append(edge)

                if rtype in TERMINAL_TYPES:
                    edge["branch_status"] = "TERMINAL_SALE"
                    terminal_documents.append(
                        {
                            "document_id": rid,
                            "number": to_node.get("number"),
                            "document_type_id": rtype,
                            "reached_via_detail_id": detail_id,
                            "from_oc_document_id": doc_id,
                            "depth": depth,
                        },
                    )
                    continue

                if rtype == DOC_TYPE_OC:
                    if depth >= MAX_DEPTH:
                        edge["branch_status"] = "MAX_DEPTH"
                        unresolved_branches.append(
                            {
                                "document_id": rid,
                                "depth": depth + 1,
                                "reason": "max_depth",
                                "from_document_id": doc_id,
                                "via_detail_id": detail_id,
                            },
                        )
                        continue
                    edge["branch_status"] = "CONTINUE_OC_33"
                    expand_oc(rid, depth + 1, detail_id, doc_id)
                    continue

                edge["branch_status"] = "NON_TERMINAL_TYPE"
                unresolved_branches.append(
                    {
                        "document_id": rid,
                        "document_type_id": rtype,
                        "depth": depth,
                        "reason": "unsupported_related_type",
                        "from_document_id": doc_id,
                        "via_detail_id": detail_id,
                    },
                )

    expand_oc(root_document_id, 0, None, None)

    oc_nodes_33 = [n for n in nodes.values() if n.get("document_type_id") == DOC_TYPE_OC]
    term_types = {int(t["document_type_id"]) for t in terminal_documents if t.get("document_type_id") is not None}

    if terminal_documents:
        conclusion = (
            "A) La OC llega a factura/boleta/NC siguiendo cadena type 33 "
            f"(terminales type {sorted(term_types)})."
        )
    elif edges:
        conclusion = (
            "B) La OC solo llega a type 33 u otros tipos no finales; "
            "no hay documento tributario final (1/6/9) vía relateddetailid en este grafo."
        )
    else:
        conclusion = (
            "C) No hay relaciones suficientes por relateddetailid "
            "(sin aristas desde details.json de la raíz)."
        )

    return {
        "root_oc": {
            "number": root_number,
            "document_id": root_document_id,
            "source": root_source,
        },
        "max_depth": MAX_DEPTH,
        "office_id_filter": OFFICE_ID,
        "nodes": nodes,
        "edges": edges,
        "terminal_documents": terminal_documents,
        "loops_detected": loops_detected,
        "unresolved_branches": unresolved_branches,
        "api_calls": api_calls,
        "summary": {
            "total_nodes": len(nodes),
            "total_edges": len(edges),
            "oc_nodes_type_33": len(oc_nodes_33),
            "terminal_sales_docs_type_1_6_9": len(terminal_documents),
            "unresolved_branches": len(unresolved_branches),
            "loops_detected": len(loops_detected),
            "conclusion": conclusion,
        },
    }


def _edges_by_from(edges: list[dict[str, Any]]) -> dict[int, list[dict[str, Any]]]:
    m: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for e in edges:
        m[int(e["from_document_id"])].append(e)
    return m


def _print_tree(
    nodes: dict[str, dict[str, Any]],
    edges_by_from: dict[int, list[dict[str, Any]]],
    doc_id: int,
    depth: int,
    visited_print: set[int],
) -> None:
    ind = "  " * depth
    if doc_id in visited_print:
        print(f"{ind}… (ya mostrado doc_id={doc_id})")
        return
    visited_print.add(doc_id)

    n = nodes.get(str(doc_id), {})
    num = n.get("number", "?")
    tid = n.get("document_type_id", "?")
    em = n.get("emissionDate", "")
    ta = n.get("totalAmount", "")
    cl = n.get("client") or {}
    cl_s = f"client={cl.get('id')} {cl.get('name') or ''}".strip()
    label = "OC" if tid == DOC_TYPE_OC else ("Factura" if tid == 6 else ("Boleta" if tid == 1 else ("NC" if tid == 9 else f"type {tid}")))

    if depth == 0:
        print(f"{ind}{label} {num} / doc_id {doc_id} / type {tid}")
    else:
        print(f"{ind}→ {label} {num} / doc_id {doc_id} / type {tid}")
    print(f"{ind}   emissionDate={em} totalAmount={ta} {cl_s}")

    for e in edges_by_from.get(doc_id, []):
        did = int(e["from_detail_id"])
        print(f"{ind}  detail {did} (profundidad arista depth={e['depth']}) status={e.get('branch_status', e.get('status'))}")
        to_id = int(e["to_document_id"])
        tt = e.get("to_document_type_id")
        if tt in TERMINAL_TYPES:
            tn = nodes.get(str(to_id), {})
            print(
                f"{ind}    → TERMINAL {tn.get('number')} / doc_id {to_id} / type {tt} "
                f"/ estado rama={e.get('branch_status')}",
            )
        elif tt == DOC_TYPE_OC:
            _print_tree(nodes, edges_by_from, to_id, depth + 2, visited_print)
        else:
            tn = nodes.get(str(to_id), {})
            print(
                f"{ind}    → doc {tn.get('number')} / doc_id {to_id} / type {tt} "
                f"/ estado rama={e.get('branch_status')}",
            )


def _print_summary(result: dict[str, Any]) -> None:
    s = result["summary"]
    print()
    print("========== RESUMEN ==========")
    print(f"total_nodes:              {s['total_nodes']}")
    print(f"total_edges:              {s['total_edges']}")
    print(f"oc_nodes_type_33:         {s['oc_nodes_type_33']}")
    print(f"terminal_sales (1/6/9):   {s['terminal_sales_docs_type_1_6_9']}")
    print(f"unresolved_branches:    {s['unresolved_branches']}")
    print(f"loops_detected:         {s['loops_detected']}")
    print(f"api_calls:                {result.get('api_calls', 0)}")
    print()
    print("conclusion:")
    print(s["conclusion"])
    print("=============================")


def main() -> int:
    load_dotenv_if_available()
    args = _build_arg_parser().parse_args()
    if args.document_id is None and args.oc_number is None:
        print("Indique número de OC o --document-id", file=sys.stderr)
        return 2

    token = read_bsale_token_from_env()
    if not token:
        print("Defina BSALE_TOKEN o BSALE_TOKEN_SPA (.env o entorno).", file=sys.stderr)
        return 2

    conn = get_connection()
    cur = conn.cursor()
    try:
        row = _resolve_root_from_db(
            cur,
            oc_number=args.oc_number,
            document_id=args.document_id,
        )
        root_source = "db"
        root_document_id: int
        root_number: int | None

        if row:
            if row.get("document_type_id") != DOC_TYPE_OC:
                print(
                    f"document_type_id={row.get('document_type_id')} no es OC (33).",
                    file=sys.stderr,
                )
                return 2
            root_document_id = int(row["document_id"])
            root_number = int(row["number"]) if row["number"] is not None else None
        else:
            if args.document_id is None:
                print(
                    "OC no encontrada en distribuidora.documents (company/office/type/number).",
                    file=sys.stderr,
                )
                return 2
            client0 = BsaleClient(token)
            blob, _ = _fetch_document_json(client0, args.document_id)
            if not blob:
                print("No se pudo leer /documents/{id}.json desde Bsale.", file=sys.stderr)
                return 2
            dt = blob.get("documentType") or blob.get("document_type")
            tid = _safe_int(dt.get("id")) if isinstance(dt, dict) else None
            if tid != DOC_TYPE_OC:
                print(f"API document_type_id={tid} no es OC (33).", file=sys.stderr)
                return 2
            root_document_id = args.document_id
            root_number = _safe_int(blob.get("number"))
            root_source = "api_only"

        out_num = root_number if root_number is not None else root_document_id
        out_path = _REPO / "exports" / f"debug_related_graph_oc_{out_num}.json"

        client = BsaleClient(token)
        result = run_graph(client, root_document_id, root_number, root_source=root_source)

        print("=== Grafo relateddetailid (solo lectura) ===\n")
        edges_by_from = _edges_by_from(result["edges"])
        _print_tree(
            result["nodes"],
            edges_by_from,
            root_document_id,
            0,
            set(),
        )
        _print_summary(result)

        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(
            json.dumps(result, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
        print(f"\nJSON escrito: {out_path}")
        return 0
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
