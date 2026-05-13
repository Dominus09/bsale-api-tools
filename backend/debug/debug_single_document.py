#!/usr/bin/env python3
"""
Inspección forense de UN documento Bsale: referencias, claves y vínculos.

- Sin pandas, sin Excel, sin listados masivos.
- Salida JSON bajo ``exports/`` y reporte ``REFERENCES ANALYSIS`` en consola.
"""

from __future__ import annotations

import json
import re
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import requests

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from backend.utils.bsale_token_env import require_bsale_token

# ---------------------------------------------------------------------------
# Configuración (editar aquí)
# ---------------------------------------------------------------------------

DOCUMENT_ID = 0  # id numérico Bsale del documento a inspeccionar

BASE_BSALE = "https://api.bsale.io/v1"
TIMEOUT = (10, 30)
DETAILS_LIMIT = 50
MAX_DETAIL_PAGES = 100  # tope defensivo por documento (evita bucles)

# Subcadenas para agrupar claves “relacionadas” con referencias / trazabilidad
_KEY_NEEDLES = (
    "reference",
    "references",
    "detail",
    "related",
    "origin",
    "parent",
    "applied",
)

# Patrones de nombre de campo para el barrido automático (ids, folios, tipos)
_ID_FOLIO_KEY_PATTERNS = (
    re.compile(r"documentid", re.I),
    re.compile(r"documenttype", re.I),
    re.compile(r"document_type", re.I),
    re.compile(r"referencedocument", re.I),
    re.compile(r"reference", re.I),
    re.compile(r"folio", re.I),
    re.compile(r"^number$", re.I),
    re.compile(r"related", re.I),
    re.compile(r"origin", re.I),
    re.compile(r"parent", re.I),
    re.compile(r"applied", re.I),
    re.compile(r"href", re.I),
)


def _die(msg: str, code: int = 1) -> None:
    print(f"[debug_single_document] ERROR: {msg}", file=sys.stderr)
    raise SystemExit(code)


def _token() -> str:
    return require_bsale_token(label="debug_single_document")


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
    """Devuelve ``(status_code, body_json | None)``."""
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
        text = (r.text or "")[:500]
        return r.status_code, {
            "_http_error": True,
            "status": r.status_code,
            "body_preview": text,
        }
    try:
        return r.status_code, r.json()
    except ValueError:
        return r.status_code, {
            "_parse_error": True,
            "raw_preview": (r.text or "")[:500],
        }


def _save_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )


def _sorted_pretty(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True, default=str)


def _iter_key_paths(obj: Any, base: str = "$") -> Iterator[tuple[str, str, Any]]:
    if isinstance(obj, dict):
        for k, v in obj.items():
            p = f"{base}/{k}" if base != "$" else f"{base}/{k}"
            yield p, k, v
            yield from _iter_key_paths(v, p)
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            p = f"{base}[{i}]"
            yield p, str(i), v
            yield from _iter_key_paths(v, p)


def _key_needle_hits(key: str) -> list[str]:
    kl = key.lower()
    return [n for n in _KEY_NEEDLES if n in kl]


def _id_folio_key_match(key: str) -> bool:
    return any(p.search(key) for p in _ID_FOLIO_KEY_PATTERNS)


def _preview(val: Any, max_len: int = 220) -> str:
    s = json.dumps(val, ensure_ascii=False, default=str)
    if len(s) > max_len:
        return s[: max_len - 3] + "..."
    return s


def _collect_detail_pages(
    session: requests.Session,
    token: str,
    doc_id: int,
) -> dict[str, Any]:
    pages: list[Any] = []
    offset = 0
    for _ in range(MAX_DETAIL_PAGES):
        status, body = _get_json(
            session,
            token,
            f"/documents/{doc_id}/details.json",
            {"limit": DETAILS_LIMIT, "offset": offset},
            allow_404=True,
        )
        pages.append({"offset": offset, "http_status": status, "body": body})
        if status == 404 or body is None:
            break
        if not isinstance(body, dict):
            break
        items = body.get("items") or []
        if not items:
            break
        offset += len(items)
        if len(items) < DETAILS_LIMIT:
            break
    return {"document_id": doc_id, "pages_fetched": len(pages), "pages": pages}


def _analyze_tree(
    label: str,
    obj: Any,
) -> tuple[list[str], list[dict[str, Any]], list[dict[str, Any]]]:
    """top-level keys (solo dict raíz), hits por needles, hits por patrones id/folio."""
    top_keys: list[str] = []
    if isinstance(obj, dict) and label in ("raw_response", "document_root"):
        top_keys = sorted(obj.keys())

    hits_needle: list[dict[str, Any]] = []
    hits_id: list[dict[str, Any]] = []

    for path, key, val in _iter_key_paths(obj):
        nh = _key_needle_hits(key)
        if nh:
            hits_needle.append(
                {
                    "source": label,
                    "path": path,
                    "key": key,
                    "needles": nh,
                    "value_type": type(val).__name__,
                    "example": _preview(val, 180),
                }
            )
        if _id_folio_key_match(key):
            hits_id.append(
                {
                    "source": label,
                    "path": path,
                    "key": key,
                    "value_type": type(val).__name__,
                    "example": _preview(val, 180),
                }
            )

    return top_keys, hits_needle, hits_id


def _print_section(title: str) -> None:
    print(f"\n{'=' * 72}\n{title}\n{'=' * 72}", flush=True)


def _print_hits(title: str, rows: list[dict[str, Any]], limit: int = 80) -> None:
    print(f"\n-- {title} (mostrando hasta {limit} filas) --", flush=True)
    for i, row in enumerate(rows[:limit]):
        print(f"  [{i + 1}] {json.dumps(row, ensure_ascii=False)}", flush=True)
    if len(rows) > limit:
        print(f"  ... ({len(rows) - limit} filas más)", flush=True)


def main() -> None:
    token = _token()
    doc_id = int(DOCUMENT_ID)
    if doc_id <= 0:
        _die("DOCUMENT_ID debe ser un entero > 0.")

    repo_root = Path(__file__).resolve().parents[2]
    out_dir = repo_root / "exports"

    session = requests.Session()

    st_doc, raw_doc = _get_json(
        session, token, f"/documents/{doc_id}.json", None, allow_404=False
    )
    if st_doc != 200 or not isinstance(raw_doc, dict):
        _die(f"No se pudo obtener documento {doc_id}: http_status={st_doc}")

    path_full = out_dir / "debug_document_full.json"
    _save_json(path_full, raw_doc)
    root = _document_root(raw_doc)

    st_ref, ref_body = _get_json(
        session,
        token,
        f"/documents/{doc_id}/references.json",
        None,
        allow_404=True,
    )

    details_bundle = _collect_detail_pages(session, token, doc_id)
    _save_json(out_dir / "debug_details.json", details_bundle)

    st_pay, pay_body = _get_json(
        session,
        token,
        f"/documents/{doc_id}/payments.json",
        None,
        allow_404=True,
    )
    _save_json(out_dir / "debug_payments.json", {"http_status": st_pay, "body": pay_body})

    st_tax, tax_body = _get_json(
        session,
        token,
        f"/documents/{doc_id}/taxes.json",
        None,
        allow_404=True,
    )
    _save_json(out_dir / "debug_taxes.json", {"http_status": st_tax, "body": tax_body})

    # --- Consola: JSON ordenado completo (envelope API + documento lógico) ---
    _print_section("JSON COMPLETO — respuesta API cruda /documents/{id}.json (sort_keys=True)")
    print(_sorted_pretty(raw_doc), flush=True)
    _print_section("JSON COMPLETO — documento lógico (document o raíz, sort_keys=True)")
    print(_sorted_pretty(root), flush=True)

    top_raw, needle_raw, id_raw = _analyze_tree("raw_response", raw_doc)
    top_root, needle_root, id_root = _analyze_tree("document_root", root)
    ref_scan: Any = {"http_status": st_ref, "body": ref_body}
    _, needle_ref, id_ref = _analyze_tree("references_endpoint", ref_scan)
    details_scan: dict[str, Any] = {"pages": details_bundle.get("pages", [])}
    _, needle_det, id_det = _analyze_tree("details_pages", details_scan)
    pay_scan = pay_body if isinstance(pay_body, dict) else {}
    _, needle_pay, id_pay = _analyze_tree("payments_endpoint", pay_scan)
    tax_scan = tax_body if isinstance(tax_body, dict) else {}
    _, needle_tax, id_tax = _analyze_tree("taxes_endpoint", tax_scan)

    embedded_refs = None
    if isinstance(root, dict):
        for k in ("references", "reference", "relatedDocuments", "related_documents"):
            if k in root:
                embedded_refs = k
                break

    _print_section("REFERENCES ANALYSIS")

    print("\n1) Archivos generados", flush=True)
    for p in (
        path_full,
        out_dir / "debug_details.json",
        out_dir / "debug_payments.json",
        out_dir / "debug_taxes.json",
    ):
        print(f"   - {p}", flush=True)

    print("\n2) Endpoint GET /documents/{id}/references.json", flush=True)
    print(f"   http_status: {st_ref}", flush=True)
    print(f"   body_type: {type(ref_body).__name__}", flush=True)
    if isinstance(ref_body, dict):
        print(f"   top-level keys: {sorted(ref_body.keys())}", flush=True)
        items = ref_body.get("items")
        if isinstance(items, list):
            print(f"   items: lista de longitud {len(items)}", flush=True)
            if items and isinstance(items[0], dict):
                print(f"   primer ítem keys: {sorted(items[0].keys())}", flush=True)
    print("   cuerpo (ordenado, completo):", flush=True)
    print(_sorted_pretty(ref_body), flush=True)

    print("\n3) Documento principal — top-level keys", flush=True)
    print(f"   raw_response: {top_raw}", flush=True)
    print(f"   document_root: {top_root}", flush=True)

    print("\n4) ¿Referencias embebidas en document_root?", flush=True)
    if embedded_refs:
        print(
            f"   Sí: primera clave encontrada en raíz lógica: {embedded_refs!r}",
            flush=True,
        )
        v = root.get(embedded_refs) if isinstance(root, dict) else None
        print(f"   tipo_valor: {type(v).__name__}", flush=True)
        print(f"   vista: {_preview(v, 400)}", flush=True)
    else:
        print(
            "   No se encontraron en la raíz lógica claves exactas "
            "references | reference | relatedDocuments | related_documents.",
            flush=True,
        )

    print("\n5) Claves cuyo nombre contiene (reference|detail|related|origin|parent|applied)", flush=True)
    merged_needle = (
        needle_raw + needle_root + needle_ref + needle_det + needle_pay + needle_tax
    )
    by_source: dict[str, list[dict[str, Any]]] = {}
    for row in merged_needle:
        by_source.setdefault(row["source"], []).append(row)
    for src in sorted(by_source.keys()):
        print(f"\n   [{src}] {len(by_source[src])} coincidencias", flush=True)
        _print_hits(f"needle hits — {src}", by_source[src], limit=40)

    print(
        "\n6) Campos automáticos (documentId, documentType, folio, number, href, …)",
        flush=True,
    )
    merged_id = id_raw + id_root + id_ref + id_det + id_pay + id_tax
    by_src2: dict[str, list[dict[str, Any]]] = {}
    for row in merged_id:
        by_src2.setdefault(row["source"], []).append(row)
    for src in sorted(by_src2.keys()):
        print(f"\n   [{src}] {len(by_src2[src])} coincidencias", flush=True)
        _print_hits(f"id/folio/href hits — {src}", by_src2[src], limit=40)

    print(
        "\n7) Resumen counts (todas las apariciones en el árbol, por fuente)",
        flush=True,
    )
    print(
        json.dumps(
            {
                "needle_hits_total": len(merged_needle),
                "id_folio_hits_total": len(merged_id),
                "by_source_needle": {k: len(v) for k, v in sorted(by_source.items())},
                "by_source_id": {k: len(v) for k, v in sorted(by_src2.items())},
            },
            ensure_ascii=False,
            indent=2,
        ),
        flush=True,
    )

    _print_section("FIN — revisar también los JSON en exports/")
    print(f"document_id={doc_id}", flush=True)


if __name__ == "__main__":
    main()
