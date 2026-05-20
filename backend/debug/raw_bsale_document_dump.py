"""
Volcado RAW total Bsale (sin interpretar, filtrar ni resumir respuestas API).

Uso (``BSALE_TOKEN`` + raíz del repo)::

    python -m backend.debug.raw_bsale_document_dump
    python -m backend.debug.raw_bsale_document_dump --document-id 3755778 --document-number 66697 --out exports/raw_dump_oc_66697.json
    python -m backend.debug.raw_bsale_document_dump --document-id 3756913 --document-number 2616098 --out exports/raw_dump_boleta_2616098.json

Sin argumentos: vuelca el par OC 66697 + boleta 2616098 (ids por defecto) y compara.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from backend.services.distribuidora.bsale_client import BsaleClient
from backend.services.distribuidora.bsale_params import merge_bsale_office_query
from backend.utils.bsale_token_env import load_dotenv_if_available, read_bsale_token_from_env

OFFICE_ID = 1
PAGE_LIMIT = 50
API_THROTTLE_SEC = 0.12

DEFAULT_OC = {"document_id": 3755778, "document_number": 66697, "out": "exports/raw_dump_oc_66697.json"}
DEFAULT_BOLETA = {
    "document_id": 3756913,
    "document_number": 2616098,
    "out": "exports/raw_dump_boleta_2616098.json",
}

SUSPICIOUS_KEY_RE = re.compile(
    r"(tracking|trackingnumber|workflow|parent|parentid|origin|relation|linked|"
    r"metadata|custom|extra|attributes|link|source|child|correlation|hash|guid|uuid)",
    re.IGNORECASE,
)


def _throttle() -> None:
    if API_THROTTLE_SEC > 0:
        time.sleep(API_THROTTLE_SEC)


def _walk_paths(obj: Any, prefix: str = "") -> Iterator[str]:
    if isinstance(obj, dict):
        for key, val in obj.items():
            path = f"{prefix}.{key}" if prefix else str(key)
            yield path
            yield from _walk_paths(val, path)
    elif isinstance(obj, list):
        for idx, val in enumerate(obj):
            path = f"{prefix}[{idx}]"
            yield from _walk_paths(val, path)


def _leaf_paths_values(obj: Any, prefix: str = "") -> dict[str, Any]:
    """Rutas hoja → valor escalar/lista (solo para comparación; no altera RAW)."""
    out: dict[str, Any] = {}
    if isinstance(obj, dict):
        for key, val in obj.items():
            path = f"{prefix}.{key}" if prefix else str(key)
            if isinstance(val, (dict, list)):
                out.update(_leaf_paths_values(val, path))
            else:
                out[path] = val
    elif isinstance(obj, list):
        for idx, val in enumerate(obj):
            path = f"{prefix}[{idx}]"
            if isinstance(val, (dict, list)):
                out.update(_leaf_paths_values(val, path))
            else:
                out[path] = val
    return out


def _collect_key_scan(obj: Any) -> dict[str, Any]:
    all_paths = sorted(set(_walk_paths(obj)))
    top_level = sorted(obj.keys()) if isinstance(obj, dict) else []
    nested_keys: dict[str, list[str]] = {}
    if isinstance(obj, dict):
        for key, val in obj.items():
            if isinstance(val, dict):
                nested_keys[key] = sorted(val.keys())
            elif isinstance(val, list) and val and isinstance(val[0], dict):
                nested_keys[key] = sorted({k for it in val[:20] if isinstance(it, dict) for k in it})
    suspicious: list[dict[str, Any]] = []
    for path in all_paths:
        segment = path.split(".")[-1].split("[")[0]
        if SUSPICIOUS_KEY_RE.search(segment):
            suspicious.append({"path": path, "segment": segment})
    return {
        "keys_detected": top_level,
        "all_paths_count": len(all_paths),
        "all_paths_sample": all_paths[:500],
        "nested_keys": nested_keys,
        "suspicious_fields": suspicious,
    }


def _value_hash(v: Any) -> str | None:
    if v is None or isinstance(v, (dict, list)):
        return None
    try:
        raw = json.dumps(v, sort_keys=True, ensure_ascii=False, default=str)
    except TypeError:
        raw = str(v)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def _paginate_get(
    client: BsaleClient,
    path: str,
    params: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Páginas RAW; cada elemento es el JSON completo de Bsale por request."""
    pages: list[dict[str, Any]] = []
    offset = 0
    base_params = dict(params or {})
    while True:
        p = {**base_params, "limit": PAGE_LIMIT, "offset": offset}
        raw = client.get(path, p)
        _throttle()
        pages.append(
            {
                "request": {
                    "method": "GET",
                    "path": path,
                    "params": p,
                },
                "response": raw,
            },
        )
        items = raw.get("items") if isinstance(raw, dict) else None
        if not isinstance(items, list) or len(items) < PAGE_LIMIT:
            break
        offset += len(items)
    return pages


def _resolve_id_by_number(client: BsaleClient, number: int) -> list[dict[str, Any]]:
    """Búsqueda auxiliar RAW (no sustituye el dump principal)."""
    raw = client.get(
        "/documents.json",
        merge_bsale_office_query({"number": number, "limit": PAGE_LIMIT, "offset": 0}, OFFICE_ID),
    )
    _throttle()
    return [
        {
            "request": {
                "method": "GET",
                "path": "/documents.json",
                "params": {"number": number, "officeid": OFFICE_ID, "limit": PAGE_LIMIT, "offset": 0},
            },
            "response": raw,
        },
    ]


def dump_document(
    client: BsaleClient,
    *,
    document_id: int,
    document_number: int | None,
    label: str,
) -> dict[str, Any]:
    """Recolecta todas las respuestas Bsale sin transformar ``response``."""
    payload: dict[str, Any] = {
        "meta": {
            "label": label,
            "document_id": document_id,
            "document_number": document_number,
            "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
            "read_only": True,
        },
        "auxiliary_number_search": None,
        "document_json": None,
        "details_json_pages": [],
        "references_json": None,
        "relateddetailid_responses": [],
    }

    if document_number is not None:
        payload["auxiliary_number_search"] = _resolve_id_by_number(client, document_number)

    doc_req = {
        "method": "GET",
        "path": f"/documents/{document_id}.json",
        "params": None,
    }
    doc_raw = client.get(f"/documents/{document_id}.json", None)
    _throttle()
    payload["document_json"] = {"request": doc_req, "response": doc_raw}

    payload["details_json_pages"] = _paginate_get(
        client,
        f"/documents/{document_id}/details.json",
        None,
    )

    ref_req = {
        "method": "GET",
        "path": f"/documents/{document_id}/references.json",
        "params": None,
    }
    ref_raw = client.get(f"/documents/{document_id}/references.json", None)
    _throttle()
    payload["references_json"] = {"request": ref_req, "response": ref_raw}

    detail_ids: list[int] = []
    for page in payload["details_json_pages"]:
        resp = page.get("response")
        if not isinstance(resp, dict):
            continue
        items = resp.get("items")
        if not isinstance(items, list):
            continue
        for it in items:
            if isinstance(it, dict) and it.get("id") is not None:
                try:
                    detail_ids.append(int(it["id"]))
                except (TypeError, ValueError):
                    pass

    for detail_id in detail_ids:
        pages = _paginate_get(
            client,
            "/documents.json",
            merge_bsale_office_query({"relateddetailid": detail_id}, OFFICE_ID),
        )
        payload["relateddetailid_responses"].append(
            {
                "detail_id": detail_id,
                "pages": pages,
            },
        )

    # Escaneo de claves (derivado; las secciones ``response`` quedan intactas).
    scan_roots: list[Any] = [doc_raw, ref_raw]
    for page in payload["details_json_pages"]:
        scan_roots.append(page.get("response"))
    for rel in payload["relateddetailid_responses"]:
        for page in rel.get("pages") or []:
            scan_roots.append(page.get("response"))

    merged_scan: dict[str, Any] = {
        "keys_detected": [],
        "all_paths_count": 0,
        "all_paths_sample": [],
        "nested_keys": {},
        "suspicious_fields": [],
    }
    all_paths: set[str] = set()
    suspicious: list[dict[str, Any]] = []
    keys_detected: set[str] = set()
    nested: dict[str, list[str]] = {}
    for root in scan_roots:
        if root is None:
            continue
        part = _collect_key_scan(root)
        keys_detected.update(part.get("keys_detected") or [])
        all_paths.update(_walk_paths(root))
        suspicious.extend(part.get("suspicious_fields") or [])
        for k, v in (part.get("nested_keys") or {}).items():
            nested.setdefault(k, [])
            nested[k] = sorted(set(nested[k]) | set(v))

    merged_scan["keys_detected"] = sorted(keys_detected)
    merged_scan["all_paths_count"] = len(all_paths)
    merged_scan["sorted_all_paths"] = sorted(all_paths)
    merged_scan["all_paths_sample"] = merged_scan["sorted_all_paths"][:500]
    merged_scan["nested_keys"] = nested
    merged_scan["suspicious_fields"] = suspicious
    payload["key_scan"] = merged_scan
    payload["leaf_values"] = _leaf_paths_values(doc_raw)

    return payload


def compare_dumps(a: dict[str, Any], b: dict[str, Any]) -> dict[str, Any]:
    """Comparación estructural (listados); no modifica RAW."""
    leaves_a = a.get("leaf_values") or {}
    leaves_b = b.get("leaf_values") or {}
    paths_a = set(leaves_a)
    paths_b = set(leaves_b)
    common_paths = sorted(paths_a & paths_b)
    common_same_value: list[dict[str, Any]] = []
    common_diff_value: list[dict[str, Any]] = []
    for path in common_paths:
        va, vb = leaves_a[path], leaves_b[path]
        entry = {"path": path, "value_a": va, "value_b": vb}
        if va == vb:
            common_same_value.append(entry)
        else:
            common_diff_value.append(entry)

    hashes_a = {p: _value_hash(v) for p, v in leaves_a.items()}
    hashes_b = {p: _value_hash(v) for p, v in leaves_b.items()}
    common_hashes: list[dict[str, Any]] = []
    for path in common_paths:
        ha, hb = hashes_a.get(path), hashes_b.get(path)
        if ha and hb and ha == hb:
            common_hashes.append({"path": path, "hash": ha, "value_a": leaves_a[path], "value_b": leaves_b[path]})

    scan_a = a.get("key_scan") or {}
    scan_b = b.get("key_scan") or {}
    keys_a = set(scan_a.get("keys_detected") or [])
    keys_b = set(scan_b.get("keys_detected") or [])

    def _extract_subtree(doc_raw: Any, *segments: str) -> Any:
        cur = doc_raw
        for seg in segments:
            if not isinstance(cur, dict):
                return None
            cur = cur.get(seg)
        return cur

    doc_a = (a.get("document_json") or {}).get("response")
    doc_b = (b.get("document_json") or {}).get("response")

    entity_compare: dict[str, Any] = {}
    for label, seg in (
        ("client", "client"),
        ("user", "user"),
        ("office", "office"),
        ("documentType", "documentType"),
        ("seller", "seller"),
    ):
        va = _extract_subtree(doc_a, seg)
        vb = _extract_subtree(doc_b, seg)
        entity_compare[label] = {"a": va, "b": vb, "equal": va == vb}

    tracking_paths = sorted(
        p
        for p in (scan_a.get("sorted_all_paths") or []) + (scan_b.get("sorted_all_paths") or [])
        if SUSPICIOUS_KEY_RE.search(p.split(".")[-1])
    )

    return {
        "document_a": a.get("meta"),
        "document_b": b.get("meta"),
        "common_top_level_keys": sorted(keys_a & keys_b),
        "only_in_a_top_level_keys": sorted(keys_a - keys_b),
        "only_in_b_top_level_keys": sorted(keys_b - keys_a),
        "common_leaf_paths_count": len(common_paths),
        "common_leaf_same_value": common_same_value,
        "common_leaf_diff_value": common_diff_value,
        "common_value_hashes": common_hashes,
        "entity_subtrees": entity_compare,
        "suspicious_paths_union": tracking_paths,
        "suspicious_a": scan_a.get("suspicious_fields") or [],
        "suspicious_b": scan_b.get("suspicious_fields") or [],
    }


def _print_dump_summary(label: str, dump: dict[str, Any]) -> None:
    scan = dump.get("key_scan") or {}
    print(f"\n--- {label} ---")
    print(f"  document_id:        {dump['meta']['document_id']}")
    print(f"  document_number:    {dump['meta'].get('document_number')}")
    print(f"  keys_detected:      {scan.get('keys_detected')}")
    print(f"  all_paths_count:    {scan.get('all_paths_count')}")
    print(f"  nested_keys:        {json.dumps(scan.get('nested_keys'), ensure_ascii=False)[:1200]}")
    susp = scan.get("suspicious_fields") or []
    print(f"  suspicious_fields:  {len(susp)} hit(s)")
    for hit in susp[:40]:
        print(f"    - {hit.get('path')}")
    if len(susp) > 40:
        print(f"    ... +{len(susp) - 40} más (ver JSON)")


def _print_comparison(cmp: dict[str, Any]) -> None:
    print("\n" + "=" * 50)
    print("RAW DOCUMENT ANALYSIS — COMPARISON")
    print("=" * 50)
    print(f"common_top_level_keys:     {cmp.get('common_top_level_keys')}")
    print(f"only_in_a:                 {cmp.get('only_in_a_top_level_keys')}")
    print(f"only_in_b:                 {cmp.get('only_in_b_top_level_keys')}")
    print(f"common_leaf_paths_count:   {cmp.get('common_leaf_paths_count')}")
    print(f"common_leaf_same_value:    {len(cmp.get('common_leaf_same_value') or [])} path(s)")
    for row in (cmp.get("common_leaf_same_value") or [])[:25]:
        print(f"  = {row['path']}: {row['value_a']!r}")
    if len(cmp.get("common_leaf_same_value") or []) > 25:
        print("  ...")
    print(f"common_value_hashes:       {len(cmp.get('common_value_hashes') or [])} path(s)")
    for row in (cmp.get("common_value_hashes") or [])[:15]:
        print(f"  # {row['path']} hash={row['hash']}")
    print("entity_subtrees:")
    for k, v in (cmp.get("entity_subtrees") or {}).items():
        print(f"  {k}: equal={v.get('equal')}")
    print(f"suspicious_paths_union:    {len(cmp.get('suspicious_paths_union') or [])} path(s)")
    for p in (cmp.get("suspicious_paths_union") or [])[:30]:
        print(f"  ? {p}")


def _write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Volcado RAW total documento Bsale (read-only).")
    p.add_argument("--document-id", type=int, action="append", help="Bsale document_id (repetible)")
    p.add_argument("--document-number", type=int, action="append", help="Folio (repetible, alinea con --document-id)")
    p.add_argument("--out", type=str, action="append", help="Ruta export JSON (una por documento)")
    p.add_argument(
        "--label",
        type=str,
        action="append",
        help="Etiqueta por documento (default: doc_0, doc_1, …)",
    )
    p.add_argument(
        "--no-default-pair",
        action="store_true",
        help="No volcar el par OC 66697 + boleta 2616098 por defecto",
    )
    return p


def main() -> int:
    load_dotenv_if_available()
    args = _build_parser().parse_args()

    token = read_bsale_token_from_env()
    if not token:
        print("Defina BSALE_TOKEN o BSALE_TOKEN_SPA.", file=sys.stderr)
        return 2

    client = BsaleClient(token)

    jobs: list[dict[str, Any]] = []
    if args.document_id:
        ids = args.document_id
        nums = args.document_number or []
        outs = args.out or []
        labels = args.label or []
        for i, doc_id in enumerate(ids):
            num = nums[i] if i < len(nums) else None
            out = outs[i] if i < len(outs) else f"exports/raw_dump_doc_{doc_id}.json"
            label = labels[i] if i < len(labels) else f"doc_{i}"
            jobs.append({"document_id": doc_id, "document_number": num, "out": out, "label": label})
    elif not args.no_default_pair:
        jobs = [
            {**DEFAULT_OC, "label": "oc_66697"},
            {**DEFAULT_BOLETA, "label": "boleta_2616098"},
        ]
    else:
        print("Indique --document-id o omita --no-default-pair.", file=sys.stderr)
        return 2

    print("=" * 60)
    print("RAW BSALE DOCUMENT DUMP (sin transformar respuestas API)")
    print("=" * 60)

    dumps: list[dict[str, Any]] = []
    for job in jobs:
        print(f"\n[dump] {job['label']} document_id={job['document_id']} …")
        dump = dump_document(
            client,
            document_id=int(job["document_id"]),
            document_number=job.get("document_number"),
            label=str(job["label"]),
        )
        out_path = _REPO / str(job["out"])
        _write_json(out_path, dump)
        print(f"  written: {out_path}")
        _print_dump_summary(job["label"], dump)
        dumps.append(dump)

    print("\n" + "=" * 50)
    print("RAW DOCUMENT ANALYSIS")
    print("=" * 50)
    for dump in dumps:
        scan = dump.get("key_scan") or {}
        print(f"\n[{dump['meta']['label']}]")
        print(f"  keys_detected:      {scan.get('keys_detected')}")
        print(f"  nested_keys:        {list((scan.get('nested_keys') or {}).keys())}")
        print(f"  suspicious_fields:  {len(scan.get('suspicious_fields') or [])}")

    if len(dumps) == 2:
        cmp = compare_dumps(dumps[0], dumps[1])
        _print_comparison(cmp)
        cmp_path = _REPO / "exports/raw_dump_comparison_oc66697_boleta2616098.json"
        _write_json(cmp_path, cmp)
        print(f"\ncomparison JSON: {cmp_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
