"""Probe read-only de recepciones (``backend/debug/bsale_receptions_probe.py``). Sin red: Bsale falso."""

from __future__ import annotations

import copy
import json
import re

import pytest

from backend.debug import bsale_receptions_probe as rp
from backend.debug.bsale_raw_live_probe import CompanyAborted, Probe

TOKEN = "tok-SECRETO-123"
API = "https://api.bsale.io/v1"
BASE_DAY = 1_780_000_000 - 1_780_000_000 % 86400
DOCS = ["FACTURA", "GUÍA DE DESPACHO ELECTRÓNICA", "NOTA DE CRÉDITO ELECTRÓNICA T", "SIN DOCUMENTO", None]
RECEPTION_RE = re.compile(r"^/v1/stocks/receptions/(\d+)\.json$")
DETAILS_RE = re.compile(r"^/v1/stocks/receptions/(\d+)/details\.json$")
COSTS_RE = re.compile(r"^/v1/variants/(\d+)/costs\.json$")


def reception(i: int) -> dict:
    rid = 1000 + i
    return {
        "href": f"{API}/stocks/receptions/{rid}.json", "id": rid, "admissionDate": BASE_DAY + (i // 10) * 86400,
        "document": DOCS[i % len(DOCS)], "documentNumber": str(5000 + i) if i % 7 else "",
        "note": f"SECRETO_NOTA_{i} Juan Pérez", "internalDispatchId": 0,
        "office": {"href": f"{API}/offices/{1 + 2 * (i % 2)}.json", "id": str(1 + 2 * (i % 2))},
        "user": {"href": f"{API}/users/7.json", "id": "7"},
        "details": {"href": f"{API}/stocks/receptions/{rid}/details.json"},
    }


def detail(rid: int, n: int) -> dict:
    return {"href": f"{API}/stocks/receptions/{rid}/details/{rid * 100 + n}.json", "id": rid * 100 + n,
            "quantity": 2.0 + n, "cost": 100.0 + n, "variantStock": 10, "serialNumber": "",
            "variant": {"href": f"{API}/variants/{30000 + (rid + n) % 50}.json", "id": str(30000 + (rid + n) % 50)}}


class FakeBsale:
    def __init__(self, n=120, *, range_supported=False, expand="complete", cost="match", big=None, fail_429_after=None):
        self.receptions = [reception(i) for i in range(n)]
        self.lines = {r["id"]: [detail(r["id"], k) for k in range(1 + i % 4)] for i, r in enumerate(self.receptions)}
        if big:
            self.lines[big] = [detail(big, k) for k in range(60)]
        self.range_supported = range_supported
        self.expand = expand
        self.cost = cost
        self.fail_429_after = fail_429_after
        self.calls = 0

    def node(self, rid):
        items = copy.deepcopy(self.lines[rid])
        if self.expand == "truncated":
            return {"href": "x", "count": len(items), "limit": 2, "offset": 0, "items": items[:2]}
        if self.expand == "mismatch":
            items = [{**it, "quantity": it["quantity"] + 1} for it in items]
        return {"href": "x", "count": len(items), "limit": 25, "offset": 0, "items": items}

    def decorate(self, rec, params):
        rec = copy.deepcopy(rec)
        if "details" in str(params.get("expand", "")) and self.expand != "ignored":
            rec["details"] = self.node(rec["id"])
        return rec

    def handle(self, path, params):
        self.calls += 1
        if self.fail_429_after is not None and self.calls > self.fail_429_after:
            return 429, {"error": "rate"}
        params = {k: str(v) for k, v in (params or {}).items()}
        offset, limit = int(params.get("offset", 0)), int(params.get("limit", 25))
        if path == "/v1/stocks/receptions.json":
            rows = self.receptions
            if "admissiondate" in params:
                rows = [r for r in rows if r["admissionDate"] == int(params["admissiondate"])]
            if "admissiondaterange" in params and self.range_supported:
                a, b = (int(x) for x in params["admissiondaterange"].strip("[]").split(","))
                rows = [r for r in rows if a <= r["admissionDate"] <= b]
            if "officeid" in params:
                rows = [r for r in rows if r["office"]["id"] == params["officeid"]]
            if "documentnumber" in params:
                rows = [r for r in rows if r["documentNumber"] == params["documentnumber"]]
            page = [self.decorate(r, params) for r in rows[offset:offset + limit]]
            return 200, {"href": "x", "count": len(rows), "limit": limit, "offset": offset, "items": page}
        if m := RECEPTION_RE.match(path):
            rec = next(r for r in self.receptions if r["id"] == int(m.group(1)))
            return 200, self.decorate(rec, params)
        if m := DETAILS_RE.match(path):
            lines = self.lines[int(m.group(1))]
            return 200, {"href": "x", "count": len(lines), "limit": limit, "offset": offset,
                         "items": copy.deepcopy(lines[offset:offset + limit])}
        if m := COSTS_RE.match(path):
            vid = int(m.group(1))
            history = []
            for r in self.receptions:
                for d in self.lines[r["id"]]:
                    if int(d["variant"]["id"]) == vid and self.cost != "missing":
                        cost = d["cost"] if self.cost == "match" else d["cost"] / d["quantity"]
                        history.append({"reception_detail": {"href": "x", "id": str(d["id"])},
                                        "admissionDate": r["admissionDate"], "cost": cost, "availableFifo": 1})
            return 200, {"averageCost": "105.0", "totalCost": "1", "history": history}
        return 404, {"error": "not found"}


class FakeProbe:
    """Misma interfaz que ``Probe``: ``get`` / ``requests`` / ``log``; tope y 429 como el real."""

    def __init__(self, bsale):
        self.bsale = bsale
        self.requests = 0
        self.log: list = []
        self.paths: list = []

    def get(self, base, path, params=None, *, label=None):
        assert base == "https://api.bsale.io"
        if self.requests >= rp.MAX_REQUESTS:
            raise CompanyAborted("tope")
        self.requests += 1
        self.paths.append((path, dict(params or {})))
        status, body = self.bsale.handle(path, params)
        self.log.append({"endpoint": path, "status": status})
        if status == 429:
            raise CompanyAborted("429 recibido; se detiene la empresa")
        return status, body


def run(bsale=None, **kw):
    bsale = bsale or FakeBsale()
    holder = {}

    def make(company_id, token):
        assert company_id == 3 and token == TOKEN
        holder["p"] = FakeProbe(bsale)
        return holder["p"]

    return rp.probe(TOKEN, make_probe=make, **kw), holder["p"]


def test_full_run_discovers_shape_order_filters_expand_and_cost():
    out, p = run()
    v = out["verdicts"]
    assert out["requests"] <= rp.MAX_REQUESTS and p.requests == out["requests"]
    assert out["listing"]["count"] == 120 and out["listing"]["second_page_overlap_ids"] == 0
    assert v["listing_order_by_id"] == "ASC" and v["listing_order_by_date"] == "ASC"
    assert v["admissiondate_exact"] == "WORKS" and v["admissiondate_granularity"] == "EXACT_TIMESTAMP"
    assert v["admissiondaterange"] == "IGNORED"  # 200 con todo el listado: no es un rango
    assert v["unknown_filter"] == "UNKNOWN_FILTERS_IGNORED"
    assert v["officeid"] == "WORKS" and v["documentnumber"] == "WORKS"
    assert v["expand_details"] == "EXPAND_COMPLETE"
    assert v["cost_link"] == ["MATCH"] and v["cost_semantics"].startswith("UNIT_COST")
    assert out["dates"]["all_midnight_utc"] is True
    check = out["header_field_check"]
    for field in ("admissionDate", "office", "document", "documentNumber", "user", "note", "internalDispatchId"):
        assert check[field]["present"] == check[field]["of"] > 0
    assert out["detail_field_check"]["present"]["variantStock"] == out["detail_field_check"]["lines"]
    assert out["header_fields"]["fields"]["note"]["non_empty"] > 0
    assert "GUÍA DE DESPACHO ELECTRÓNICA" in out["document_categories"]
    assert all(l["same_admission_date"] for l in out["cost_links"])


def test_output_never_contains_note_text_or_token():
    out, _ = run()
    text = json.dumps(out, ensure_ascii=False, default=str)
    assert "SECRETO_NOTA" not in text and "Juan Pérez" not in text and TOKEN not in text


def test_only_get_requests_within_limit_and_no_writes():
    _, p = run()
    assert p.requests <= 40
    source = open(rp.__file__, encoding="utf-8").read()
    assert not re.search(r"\.(post|put|patch|delete)\(", source)
    assert not re.search(r"\b(INSERT|UPDATE|DELETE|psycopg2|get_connection)\b", source)


def test_range_filter_detected_when_supported():
    out, _ = run(FakeBsale(range_supported=True))
    assert out["verdicts"]["admissiondaterange"] == "WORKS"
    assert "admissiondaterange" in out["strategy"]["historical"]


@pytest.mark.parametrize("mode,expected", [
    ("ignored", "EXPAND_IGNORED"),
    ("truncated", "EXPAND_PARTIAL"),
    ("mismatch", "EXPAND_UNRELIABLE"),
])
def test_expand_is_not_accepted_just_because_of_http_200(mode, expected):
    out, _ = run(FakeBsale(expand=mode))
    assert out["verdicts"]["expand_details"].startswith(expected)
    if mode in ("ignored", "mismatch"):
        assert out["strategy"]["details"].startswith("details.json")


def test_details_pagination_over_50_lines():
    out, p = run(FakeBsale(big=1005), extra=[1005])
    d = out["details"]["1005"]
    assert d["count"] == 60 and d["complete"] is True and len(d["pages"]) == 2
    assert d["stats"]["lines"] == 60 and d["stats"]["duplicate_ids"] == 0
    assert ("/v1/stocks/receptions/1005/details.json", {"limit": 50, "offset": 50}) in p.paths


def test_reference_day_argument_is_used():
    out, p = run(day_ts=BASE_DAY + 3 * 86400)
    assert out["reference_day"]["date"] == rp._day(BASE_DAY + 3 * 86400)
    assert out["filters"]["admissiondate_exact"]["count"] == 10


@pytest.mark.parametrize("cost,link,semantics", [
    ("total", "COST_DIFFERS", "LINE_TOTAL"),
    ("missing", "NOT_IN_HISTORY", "INCONCLUSIVE"),
])
def test_cost_semantics(cost, link, semantics):
    out, _ = run(FakeBsale(cost=cost))
    assert out["verdicts"]["cost_link"] == [link]
    assert out["verdicts"]["cost_semantics"].startswith(semantics)


def test_http_429_stops_everything():
    out, p = run(FakeBsale(fail_429_after=4))
    assert "429" in out["aborted"] and p.requests == 5
    assert out["verdicts"]["expand_details"] == "INCONCLUSIVE"


def test_limited_probe_caps_at_40(monkeypatch):
    monkeypatch.setattr(Probe, "get", lambda self, base, path, params=None, *, label=None: (
        setattr(self, "requests", self.requests + 1) or (200, {})))
    lp = rp.LimitedProbe(3, TOKEN)
    for _ in range(rp.MAX_REQUESTS):
        lp.get(rp.API, "/v1/stocks/receptions.json")
    with pytest.raises(CompanyAborted):
        lp.get(rp.API, "/v1/stocks/receptions.json")
    assert rp.MAX_REQUESTS == 40


def test_pure_helpers():
    assert rp.order_of([1, 2, 2, 3]) == "ASC" and rp.order_of([3, 1]) == "DESC" and rp.order_of([1, 3, 2]) == "MIXED"
    assert rp.label_category("GUÍA DE DESPACHO ELECTRÓNICA 26079 INGRESO") == "GUÍA DE DESPACHO ELECTRÓNICA # INGRESO"
    assert rp.label_category(None) == "<NULL>"
    assert rp.node_problem({"href": "x"}) == "NOT_EXPANDED"
    assert rp.node_problem({"count": 3, "items": [{}]}) == "TRUNCATED"
    assert rp.node_problem({"count": 1, "items": [{}], "next": "y"}) == "TRUNCATED"
    assert rp.node_problem({"count": 1, "items": [{}]}) is None
    ok = rp.filter_result(200, {"count": 2, "items": [{"a": 1}]}, 10, lambda i: True)
    assert ok["verdict"] == "WORKS"
    assert rp.filter_result(400, {"error": "x"}, 10, lambda i: True)["verdict"] == "REJECTED_OR_FAILED"
    assert rp.filter_result(200, {"count": 10, "items": [{"a": 1}]}, 10, lambda i: False)["verdict"] == "IGNORED"


def test_out_path_inside_repo_is_rejected(capsys):
    assert rp.main(["--out", str(rp.REPO_ROOT / "receptions_probe.json")]) == 64


def test_main_without_token_skips(monkeypatch, tmp_path):
    monkeypatch.delenv("BSALE_TOKEN_SPA", raising=False)
    monkeypatch.setitem(__import__("sys").modules, "dotenv", None)
    assert rp.main(["--out", str(tmp_path / "r.json")]) == 1
    assert not (tmp_path / "r.json").exists()
