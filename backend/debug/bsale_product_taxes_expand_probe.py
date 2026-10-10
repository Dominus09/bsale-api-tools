"""Prueba READ-ONLY: ¿``products.json`` entrega ``product_taxes`` completo con ``expand``? (company 3).

Sólo GET, a lo sumo ~1 request/segundo, tope de requests, sin reintentos y corte ante 429 (reutiliza
``Probe`` de ``bsale_raw_live_probe``). Nunca imprime el token; la salida tiene sólo ids, conteos,
claves y estados HTTP (sin nombres ni payloads).

Compara, para una muestra de productos, la relación obtenida con ``expand`` contra la consulta
individual ``/v1/products/{id}/product_taxes.json`` (referencia) en dos páginas del listado, más
productos explícitos (p. ej. uno sin impuestos según el legacy):

    python -m backend.debug.bsale_product_taxes_expand_probe --out /tmp/product_taxes_expand.json [--product 123 ...]

Veredicto: ``EXPAND_COMPLETE`` sólo si en TODOS los productos comparados el nodo expandido trae ítems
con los mismos ``tax.id`` (mismo orden) y ``count`` que la referencia, incluido al menos un producto
sin impuestos expandido como lista vacía explícita, y en las dos páginas.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any

from backend.debug.bsale_raw_live_probe import API, CompanyAborted, Probe

COMPANY_ID = 3
TOKEN_ENV = "BSALE_TOKEN_SPA"
PAGE_SIZE = 5
MAX_EXPLICIT = 5


def _tax_ids(items: Any) -> list[Any] | None:
    if not isinstance(items, list):
        return None
    out = []
    for it in items:
        tax = it.get("tax") if isinstance(it, dict) else None
        out.append(tax.get("id") if isinstance(tax, dict) else None)
    return out


def _node(node: Any) -> dict[str, Any]:
    """Forma del nodo ``product_taxes`` (sin valores sensibles)."""
    if not isinstance(node, dict):
        return {"type": type(node).__name__}
    return {
        "keys": sorted(node),
        "count": node.get("count"),
        "items": len(node["items"]) if isinstance(node.get("items"), list) else None,
        "tax_ids": _tax_ids(node.get("items")),
        "has_next": bool(node.get("next")),
    }


def _reference(p: Probe, product_id: int) -> dict[str, Any]:
    st, body = p.get(API, f"/v1/products/{product_id}/product_taxes.json", {"limit": 50})
    if not isinstance(body, dict):
        return {"status": st}
    return {"status": st, "count": body.get("count"), "tax_ids": _tax_ids(body.get("items")),
            "item_keys": sorted(body["items"][0]) if body.get("items") else []}


def _compare(expanded: dict[str, Any], ref: dict[str, Any]) -> str:
    if ref.get("status") != 200:
        return "REFERENCE_FAILED"
    if expanded.get("items") is None:
        return "NOT_EXPANDED"
    if expanded.get("has_next") or (expanded.get("count") is not None and expanded["count"] != expanded["items"]):
        return "TRUNCATED"
    if expanded.get("tax_ids") == ref.get("tax_ids") and (expanded.get("count") in (None, ref.get("count"))):
        return "MATCH"
    return "MISMATCH"


def probe(token: str, explicit: list[int]) -> dict[str, Any]:
    p = Probe(COMPANY_ID, token)
    out: dict[str, Any] = {"company_id": COMPANY_ID, "pages": [], "explicit": {}}
    try:
        st, base = p.get(API, "/v1/products.json", {"limit": PAGE_SIZE})
        base_items = (base or {}).get("items") or [] if isinstance(base, dict) else []
        out["baseline"] = {"status": st, "count": (base or {}).get("count") if isinstance(base, dict) else None,
                           "product_taxes_node": _node(base_items[0].get("product_taxes")) if base_items else None}
        for offset in (0, PAGE_SIZE):
            params = {"limit": PAGE_SIZE, "offset": offset, "expand": "[product_taxes]"}
            st, body = p.get(API, "/v1/products.json", params)
            items = (body or {}).get("items") or [] if isinstance(body, dict) else []
            page: dict[str, Any] = {
                "offset": offset, "status": st,
                "count": (body or {}).get("count") if isinstance(body, dict) else None,
                "items": len(items), "products": {},
            }
            for it in items:
                pid = it.get("id")
                expanded = _node(it.get("product_taxes"))
                ref = _reference(p, int(pid))
                page["products"][str(pid)] = {"expanded": expanded, "reference": ref, "verdict": _compare(expanded, ref)}
            out["pages"].append(page)
        for pid in explicit[:MAX_EXPLICIT]:
            st, body = p.get(API, f"/v1/products/{pid}.json", {"expand": "[product_taxes]"})
            expanded = _node((body or {}).get("product_taxes") if isinstance(body, dict) else None)
            ref = _reference(p, pid)
            out["explicit"][str(pid)] = {"status": st, "expanded": expanded, "reference": ref,
                                         "verdict": _compare(expanded, ref)}
    except CompanyAborted as exc:
        out["aborted"] = str(exc)
    out["requests"] = p.requests
    out["request_log"] = p.log
    out["verdict"] = verdict(out)
    return out


def verdict(out: dict[str, Any]) -> str:
    if out.get("aborted"):
        return "INCONCLUSIVE (abortado)"
    pages = out.get("pages") or []
    if len(pages) < 2 or any(pg.get("status") != 200 for pg in pages):
        return "EXPAND_REJECTED_OR_FAILED"
    if out.get("baseline", {}).get("count") is not None and any(pg.get("count") != out["baseline"]["count"] for pg in pages):
        return "INCONCLUSIVE (count distinto con expand)"
    results = [v for pg in pages for v in pg["products"].values()] + list(out.get("explicit", {}).values())
    verdicts = {r["verdict"] for r in results}
    if not results or "REFERENCE_FAILED" in verdicts:
        return "INCONCLUSIVE (referencia individual fallida)"
    if verdicts == {"NOT_EXPANDED"}:
        return "EXPAND_IGNORED"
    if verdicts != {"MATCH"}:
        return f"EXPAND_UNRELIABLE {sorted(verdicts)}"
    if not any(r["reference"].get("count") == 0 for r in results):
        return "INCONCLUSIVE (sin producto sin impuestos en la muestra: usar --product)"
    return "EXPAND_COMPLETE"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", required=True, help="ruta del JSON de resultados (fuera del repo)")
    parser.add_argument("--product", type=int, action="append", default=[], help="product_id adicional (repetible)")
    args = parser.parse_args()
    try:
        from dotenv import load_dotenv

        load_dotenv()
    except ImportError:
        pass
    token = (os.getenv(TOKEN_ENV) or "").strip()
    if not token:
        print(f"SKIP: {TOKEN_ENV} ausente")
        return 1
    result = probe(token, [p for p in args.product if p > 0])
    with open(args.out, "w", encoding="utf-8") as fh:
        json.dump({"generated_at": datetime.now(timezone.utc).isoformat(), **result}, fh, ensure_ascii=False, indent=2)
    print(f"requests={result['requests']} aborted={result.get('aborted')} verdict={result['verdict']}")
    print(f"resultados: {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
