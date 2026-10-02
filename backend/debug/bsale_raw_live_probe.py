"""Verificación en vivo READ-ONLY para ``bsale_raw`` (fase 2).

Sólo GET. Hosts permitidos: ``api.bsale.io`` y ``credential.bsale.io``. Máximo 1 request/segundo
(las empresas se recorren en secuencia), tope de requests por empresa, sin reintentos y abortando
la empresa ante un 429. Nunca imprime tokens: la ruta de ``credential.bsale.io`` se sanitiza y los
errores de red se registran sólo por tipo. La salida omite datos personales (sólo ids y metadatos).

Uso (tokens en el entorno, nunca en argumentos):

    python -m backend.debug.bsale_raw_live_probe --out %TEMP%\\bsale_raw_probe.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlencode, urlsplit

import requests

COMPANIES = (
    (1, "Minimarkets La Quillotana", "BSALE_TOKEN_Mini"),
    (2, "Carlos Romero", "BSALE_TOKEN_Romero"),
    (3, "La Quillotana SPA", "BSALE_TOKEN_SPA"),
)
KNOWN_VARIANTS = {1: [10203], 3: [31300, 31301]}

API = "https://api.bsale.io"
CREDENTIAL = "https://credential.bsale.io"
ALLOWED_HOSTS = {"api.bsale.io", "credential.bsale.io"}
MIN_INTERVAL_S = 1.05
MAX_REQUESTS_PER_COMPANY = 60
TIMEOUT = (10, 30)
HEADER_HINTS = ("rate", "limit", "retry", "remaining", "reset", "quota", "x-request", "server", "via", "x-cache", "cf-")


class CompanyAborted(RuntimeError):
    pass


class Probe:
    def __init__(self, company_id: int, token: str) -> None:
        self.company_id = company_id
        self._token = token
        self.session = requests.Session()
        self.requests = 0
        self.log: list[dict[str, Any]] = []
        self.header_names: set[str] = set()
        self.header_values: dict[str, set[str]] = {}
        self._last = 0.0

    def _throttle(self) -> None:
        wait = MIN_INTERVAL_S - (time.monotonic() - self._last)
        if wait > 0:
            time.sleep(wait)
        self._last = time.monotonic()

    def get(self, base: str, path: str, params: dict[str, Any] | None = None, *, label: str | None = None) -> tuple[int | None, Any]:
        if self.requests >= MAX_REQUESTS_PER_COMPANY:
            raise CompanyAborted("tope de requests por empresa alcanzado")
        url = base + path
        if params:
            url += "?" + urlencode(params, safe="[],")
        if urlsplit(url).netloc not in ALLOWED_HOSTS or urlsplit(url).scheme != "https":
            raise CompanyAborted("host no permitido")
        shown = label or (path + ("?" + urlencode(params, safe="[],") if params else ""))
        self._throttle()
        self.requests += 1
        entry: dict[str, Any] = {"endpoint": shown}
        try:
            resp = self.session.get(url, headers={"access_token": self._token, "Accept": "application/json"}, timeout=TIMEOUT)
        except requests.RequestException as exc:
            entry.update(status=None, error=type(exc).__name__)
            self.log.append(entry)
            return None, None
        entry["status"] = resp.status_code
        entry["elapsed_ms"] = int(resp.elapsed.total_seconds() * 1000)
        for name, value in resp.headers.items():
            low = name.lower()
            if low == "set-cookie":
                continue
            self.header_names.add(low)
            if any(h in low for h in HEADER_HINTS):
                self.header_values.setdefault(low, set()).add(value[:80])
        body: Any = None
        try:
            body = resp.json()
        except ValueError:
            entry["body"] = "NO_JSON"
        if isinstance(body, dict):
            for key in ("count", "limit", "offset"):
                if key in body:
                    entry[key] = body[key]
            if isinstance(body.get("items"), list):
                entry["items"] = len(body["items"])
            entry["has_next"] = bool(body.get("next"))
            if "error" in body:
                entry["error"] = str(body["error"])[:120]
        self.log.append(entry)
        if resp.status_code == 429:
            entry["retry_after"] = resp.headers.get("Retry-After")
            raise CompanyAborted("429 recibido; se detiene la empresa")
        return resp.status_code, body


def _rel_id(node: Any) -> Any:
    return node.get("id") if isinstance(node, dict) else None


def _first(body: Any) -> dict[str, Any] | None:
    if isinstance(body, dict) and isinstance(body.get("items"), list) and body["items"]:
        return body["items"][0]
    return None


def _keys(obj: Any) -> list[str]:
    return sorted(obj.keys()) if isinstance(obj, dict) else []


def probe_company(company_id: int, name: str, token: str) -> dict[str, Any]:
    p = Probe(company_id, token)
    out: dict[str, Any] = {"company_id": company_id, "local_name": name, "tests": {}}
    t = out["tests"]
    try:
        # P1 instancia
        st, body = p.get(CREDENTIAL, f"/v1/instances/basic/{token}.json", label="/v1/instances/basic/<TOKEN>.json")
        if isinstance(body, dict):
            t["instance"] = {
                "status": st,
                "cpnId": body.get("id"),
                "name": body.get("name"),
                "state": body.get("state"),
                "country": body.get("country"),
                "code_present": bool(body.get("code")),
                "keys": _keys(body),
            }
        else:
            t["instance"] = {"status": st}

        # P3 / P4 state products y variants
        for resource in ("products", "variants"):
            res: dict[str, Any] = {}
            for variant_label, params in (("state0", {"state": 0, "limit": 1}), ("state1", {"state": 1, "limit": 1}), ("nostate", {"limit": 1})):
                st, body = p.get(API, f"/v1/{resource}.json", params)
                item = _first(body)
                res[variant_label] = {
                    "status": st,
                    "count": body.get("count") if isinstance(body, dict) else None,
                    "items": len(body.get("items") or []) if isinstance(body, dict) else None,
                    "item_state": item.get("state") if item else None,
                    "has_next": bool(body.get("next")) if isinstance(body, dict) else None,
                    "has_prev": "previous" in body or "prev" in body if isinstance(body, dict) else None,
                    "top_keys": _keys(body),
                }
            t[f"state_{resource}"] = res

        known: dict[str, Any] = {}
        known_product_ids: dict[int, Any] = {}
        for vid in KNOWN_VARIANTS.get(company_id, []):
            st, body = p.get(API, f"/v1/variants/{vid}.json")
            if isinstance(body, dict) and st == 200:
                known[str(vid)] = {"status": st, "state": body.get("state"), "product_id": _rel_id(body.get("product")), "has_code": bool(body.get("code")), "has_barcode": bool(body.get("barCode"))}
                known_product_ids[vid] = _rel_id(body.get("product"))
            else:
                known[str(vid)] = {"status": st, "error": body.get("error") if isinstance(body, dict) else None}
        t["known_variants"] = known

        # P5 stock
        st, body = p.get(API, "/v1/stocks.json", {"limit": 1})
        stock_item = _first(body)
        stock_info: dict[str, Any] = {
            "status": st,
            "count": body.get("count") if isinstance(body, dict) else None,
            "top_keys": _keys(body),
            "item_keys": _keys(stock_item),
            "variant_id_type": type(_rel_id(stock_item.get("variant"))).__name__ if stock_item else None,
            "sample_quantities": {k: stock_item.get(k) for k in ("quantity", "quantityReserved", "quantityAvailable")} if stock_item else None,
        }
        sample_vid = int(_rel_id(stock_item["variant"])) if stock_item else None
        sample_office = int(_rel_id(stock_item["office"])) if stock_item else None
        if sample_vid and sample_office:
            st, b = p.get(API, "/v1/stocks.json", {"variantid": sample_vid})
            stock_info["filter_variantid"] = {"status": st, "count": (b or {}).get("count"), "items": len((b or {}).get("items") or []),
                                              "all_match": all(int(_rel_id(i.get("variant"))) == sample_vid for i in (b or {}).get("items") or [])}
            st, b = p.get(API, "/v1/stocks.json", {"officeid": sample_office, "limit": 1})
            stock_info["filter_officeid"] = {"status": st, "count": (b or {}).get("count"),
                                             "first_matches": bool(_first(b)) and int(_rel_id(_first(b).get("office"))) == sample_office}
            st, b = p.get(API, "/v1/stocks.json", {"variantid": sample_vid, "officeid": sample_office})
            items = (b or {}).get("items") or []
            stock_info["filter_variantid_officeid"] = {"status": st, "count": (b or {}).get("count"), "items": len(items),
                                                       "all_match": all(int(_rel_id(i.get("variant"))) == sample_vid and int(_rel_id(i.get("office"))) == sample_office for i in items)}
        for vid in KNOWN_VARIANTS.get(company_id, []):
            st, b = p.get(API, "/v1/stocks.json", {"variantid": vid})
            items = (b or {}).get("items") or []
            stock_info.setdefault("known_variant_stock", {})[str(vid)] = {
                "status": st, "count": (b or {}).get("count"),
                "offices": [_rel_id(i.get("office")) for i in items][:10],
                "quantities": [i.get("quantity") for i in items][:10],
            }
        t["stocks"] = stock_info

        # P6 precios
        st, body = p.get(API, "/v1/price_lists.json", {"limit": 10})
        lists = (body or {}).get("items") or []
        price_info: dict[str, Any] = {"status": st, "lists_count": (body or {}).get("count"),
                                      "lists": [{"id": l.get("id"), "state": l.get("state")} for l in lists]}
        pl = next((l for l in lists if l.get("state") == 0), lists[0] if lists else None)
        pl_id = pl.get("id") if pl else None
        price_vid = None
        if pl_id:
            st, b = p.get(API, f"/v1/price_lists/{pl_id}/details.json", {"limit": 2})
            d = _first(b)
            price_info["details"] = {"price_list_id": pl_id, "status": st, "count": (b or {}).get("count"), "has_next": bool((b or {}).get("next")),
                                     "item_keys": _keys(d), "detail_id": d.get("id") if d else None,
                                     "variant_id": _rel_id(d.get("variant")) if d else None,
                                     "variantValue": d.get("variantValue") if d else None,
                                     "variantValueWithTaxes": d.get("variantValueWithTaxes") if d else None}
            price_vid = int(_rel_id(d["variant"])) if d else None
            if price_vid:
                st, b = p.get(API, f"/v1/price_lists/{pl_id}/details.json", {"variantid": price_vid})
                items = (b or {}).get("items") or []
                price_info["point_variantid"] = {"status": st, "count": (b or {}).get("count"), "items": len(items),
                                                 "all_match": all(int(_rel_id(i.get("variant"))) == price_vid for i in items)}
        t["prices"] = price_info

        # P7 costos (pocas variantes)
        costs: dict[str, Any] = {}
        for vid in [v for v in [sample_vid, *KNOWN_VARIANTS.get(company_id, [])] if v][:3]:
            st, b = p.get(API, f"/v1/variants/{vid}/costs.json")
            hist = (b or {}).get("history") if isinstance(b, dict) else None
            costs[str(vid)] = {
                "status": st,
                "top_keys": _keys(b),
                "averageCost_type": type((b or {}).get("averageCost")).__name__ if isinstance(b, dict) else None,
                "history_len": len(hist) if isinstance(hist, list) else None,
                "history_item_keys": _keys(hist[0]) if isinstance(hist, list) and hist else [],
                "history_has_pagination_keys": any(k in (b or {}) for k in ("count", "next", "limit", "offset")),
                "error": (b or {}).get("error") if isinstance(b, dict) else None,
            }
        t["costs"] = costs

        # P11 clientes
        cl: dict[str, Any] = {}
        for lbl, params in (("state0", {"state": 0, "limit": 1}), ("state1", {"state": 1, "limit": 1}), ("nostate", {"limit": 1})):
            st, b = p.get(API, "/v1/clients.json", params)
            it = _first(b)
            cl[lbl] = {"status": st, "count": (b or {}).get("count"), "item_state": it.get("state") if it else None,
                       "item_keys": _keys(it),
                       "date_like_keys": [k for k in _keys(it) if any(s in k.lower() for s in ("date", "update", "creat", "modif"))]}
        t["clients"] = cl

        # P12 rutas webhook v2 vs v1
        v2: dict[str, Any] = {}
        ref_vid = sample_vid
        if ref_vid:
            st1, b1 = p.get(API, f"/v1/variants/{ref_vid}.json")
            st2, b2 = p.get(API, f"/v2/variants/{ref_vid}.json")
            v2["variant"] = {"v1_status": st1, "v2_status": st2, "v1_keys": _keys(b1), "v2_keys": _keys(b2),
                             "same_id": isinstance(b1, dict) and isinstance(b2, dict) and str(b1.get("id")) == str(b2.get("id")),
                             "same_state": isinstance(b1, dict) and isinstance(b2, dict) and b1.get("state") == b2.get("state")}
            pid = _rel_id((b1 or {}).get("product")) if isinstance(b1, dict) else None
            if pid:
                st1, b1p = p.get(API, f"/v1/products/{pid}.json")
                st2, b2p = p.get(API, f"/v2/products/{pid}.json")
                v2["product"] = {"v1_status": st1, "v2_status": st2, "v1_keys": _keys(b1p), "v2_keys": _keys(b2p),
                                 "same_id": isinstance(b1p, dict) and isinstance(b2p, dict) and str(b1p.get("id")) == str(b2p.get("id"))}
            if sample_office:
                st2, b2s = p.get(API, "/v2/stocks.json", {"variant": ref_vid, "office": sample_office})
                items = (b2s or {}).get("items") or [] if isinstance(b2s, dict) else []
                v2["stock"] = {"v2_status": st2, "count": (b2s or {}).get("count") if isinstance(b2s, dict) else None, "items": len(items),
                               "top_keys": _keys(b2s), "item_keys": _keys(items[0]) if items else [],
                               "all_match": all(int(_rel_id(i.get("variant")) or -1) == ref_vid and int(_rel_id(i.get("office")) or -1) == sample_office for i in items)}
        if pl_id and price_vid:
            st2, b2d = p.get(API, f"/v2/price_lists/{pl_id}/details.json", {"variant": price_vid})
            items = (b2d or {}).get("items") or [] if isinstance(b2d, dict) else []
            v2["price"] = {"v2_status": st2, "count": (b2d or {}).get("count") if isinstance(b2d, dict) else None, "items": len(items),
                           "item_keys": _keys(items[0]) if items else [],
                           "all_match": all(int(_rel_id(i.get("variant")) or -1) == price_vid for i in items)}
        t["webhook_paths"] = v2

        if company_id == 3:
            t.update(probe_documents(p))
    except CompanyAborted as exc:
        out["aborted"] = str(exc)
    out["requests"] = p.requests
    out["request_log"] = p.log
    out["header_names"] = sorted(p.header_names)
    out["header_values_of_interest"] = {k: sorted(v) for k, v in p.header_values.items()}
    return out


def _doc_meta(d: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": d.get("id"),
        "document_type_id": _rel_id(d.get("document_type")),
        "office_id": _rel_id(d.get("office")),
        "client_id": _rel_id(d.get("client")),
        "emissionDate": d.get("emissionDate"),
        "generationDate": d.get("generationDate"),
        "state": d.get("state"),
        "commercialState_present": "commercialState" in d,
        "details_href": bool((d.get("details") or {}).get("href")) if isinstance(d.get("details"), dict) else None,
        "sellers_href": bool((d.get("sellers") or {}).get("href")) if isinstance(d.get("sellers"), dict) else None,
        "references_href": bool((d.get("references") or {}).get("href")) if isinstance(d.get("references"), dict) else None,
    }


def probe_documents(p: Probe) -> dict[str, Any]:
    res: dict[str, Any] = {}
    now = datetime.now(timezone.utc)
    t1 = int(now.timestamp())
    t0 = int((now - timedelta(hours=6)).timestamp())

    # P8 generationdaterange
    st_all, b_all = p.get(API, "/v1/documents.json", {"limit": 1})
    st_a, b_a = p.get(API, "/v1/documents.json", {"generationdaterange": f"[{t0},{t1}]", "limit": 50})
    items_a = (b_a or {}).get("items") or [] if isinstance(b_a, dict) else []
    gen_dates = [i.get("generationDate") for i in items_a if i.get("generationDate") is not None]
    in_range = [g for g in gen_dates if t0 <= int(g) <= t1]
    day0 = int(datetime(now.year, now.month, now.day, tzinfo=timezone.utc).timestamp()) - 86400
    st_e, b_e = p.get(API, "/v1/documents.json", {"emissiondaterange": f"[{day0},{t1}]", "limit": 1})
    count_all = (b_all or {}).get("count") if isinstance(b_all, dict) else None
    count_a = (b_a or {}).get("count") if isinstance(b_a, dict) else None
    if st_a is None or st_a >= 400:
        verdict = "REJECTED"
    elif count_a is not None and count_all is not None and count_a == count_all:
        verdict = "ACCEPTED_BUT_IGNORED"
    elif gen_dates and len(in_range) == len(gen_dates) and count_a is not None and count_all is not None and count_a < count_all:
        verdict = "SUPPORTED_AND_FILTERS"
    else:
        verdict = "INCONCLUSIVE"
    res["generationdaterange"] = {
        "window": [t0, t1],
        "count_unfiltered": count_all,
        "count_generationdaterange": count_a,
        "count_emissiondaterange_2d": (b_e or {}).get("count") if isinstance(b_e, dict) else None,
        "returned": len(items_a),
        "with_generationDate": len(gen_dates),
        "generationDate_in_window": len(in_range),
        "min_max_generationDate": [min(gen_dates), max(gen_dates)] if gen_dates else None,
        "verdict": verdict,
    }

    # P9 OC tipo 33 recientes
    st, b = p.get(API, "/v1/documents.json", {"documenttypeid": 33, "emissiondaterange": f"[{day0},{t1}]", "limit": 5})
    items = (b or {}).get("items") or [] if isinstance(b, dict) else []
    res["oc33"] = {"status": st, "count": (b or {}).get("count") if isinstance(b, dict) else None,
                   "doc_keys": _keys(items[0]) if items else [],
                   "docs": [_doc_meta(d) for d in items],
                   "stock_like_keys": [k for k in _keys(items[0]) if "stock" in k.lower()] if items else []}

    # P10 expand details vs details.json paginado (hasta 3 OC, elige la de más líneas)
    best: dict[str, Any] | None = None
    for d in items[:3]:
        did = d.get("id")
        st, bd = p.get(API, f"/v1/documents/{did}/details.json", {"limit": 50})
        cnt = (bd or {}).get("count") if isinstance(bd, dict) else None
        if cnt is not None and (best is None or cnt > best["details_count"]):
            best = {"document_id": did, "details_count": cnt, "first_page_items": len((bd or {}).get("items") or [])}
    if best:
        did = best["document_id"]
        if best["details_count"] > 50:
            offset, total = 50, best["first_page_items"]
            while offset < best["details_count"] and offset < 300:
                st, bd = p.get(API, f"/v1/documents/{did}/details.json", {"limit": 50, "offset": offset})
                total += len((bd or {}).get("items") or [])
                offset += 50
            best["paginated_items_total"] = total
        else:
            best["paginated_items_total"] = best["first_page_items"]
        st, bx = p.get(API, f"/v1/documents/{did}.json", {"expand": "[details]"})
        det = (bx or {}).get("details") if isinstance(bx, dict) else None
        exp_items = len(det.get("items") or []) if isinstance(det, dict) else None
        best["expand_status"] = st
        best["expand_details_keys"] = _keys(det)
        best["expand_details_count_field"] = det.get("count") if isinstance(det, dict) else None
        best["expand_items"] = exp_items
        if exp_items is None:
            best["verdict"] = "INCONCLUSIVE"
        elif exp_items == best["paginated_items_total"] and best["details_count"] > 25:
            best["verdict"] = "EXPAND_COMPLETE"
        elif exp_items < best["paginated_items_total"]:
            best["verdict"] = "EXPAND_TRUNCATED"
        else:
            best["verdict"] = "INCONCLUSIVE (<=25 líneas, no prueba truncamiento)"
        st_u, bu = p.get(API, f"/documents/{did}.json")
        res["webhook_document_path"] = {"path": "/documents/{id}.json", "status": st_u,
                                        "same_id": isinstance(bu, dict) and str(bu.get("id")) == str(did),
                                        "keys": _keys(bu)}
    res["expand_details"] = best
    return res


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", required=True, help="ruta del JSON de resultados (fuera del repo)")
    parser.add_argument("--companies", default="1,2,3")
    args = parser.parse_args()
    try:
        from dotenv import load_dotenv

        load_dotenv()
    except ImportError:
        pass
    wanted = {int(x) for x in args.companies.split(",") if x.strip()}
    results = []
    for cid, name, env in COMPANIES:
        if cid not in wanted:
            continue
        token = (os.getenv(env) or "").strip()
        if not token:
            results.append({"company_id": cid, "local_name": name, "skipped": f"{env} ausente"})
            print(f"[company {cid}] SKIP: {env} ausente")
            continue
        print(f"[company {cid}] inicio")
        r = probe_company(cid, name, token)
        print(f"[company {cid}] fin requests={r['requests']} aborted={r.get('aborted')}")
        results.append(r)
    with open(args.out, "w", encoding="utf-8") as fh:
        json.dump({"generated_at": datetime.now(timezone.utc).isoformat(), "results": results}, fh, ensure_ascii=False, indent=2, default=str)
    print(f"resultados: {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
