"""Prueba READ-ONLY de recepciones de stock Bsale (company 3) para diseñar RAW recepciones.

Sólo GET, ~1 request/segundo, tope ``MAX_REQUESTS`` (40), sin reintentos y corte ante 429 (reutiliza
``Probe`` de ``bsale_raw_live_probe``: hosts permitidos, token nunca impreso). El JSON de salida
lleva sólo ids, fechas, conteos, claves, tipos, estados HTTP y etiquetas ``document`` normalizadas
(dígitos → ``#``). ``note`` nunca se guarda: sólo si viene y si viene vacía.

    python -m backend.debug.bsale_receptions_probe --out /tmp/receptions_probe.json \
        [--day 2026-07-31] [--reception 91234 ...]

Investiga:

1. listado ``/v1/stocks/receptions.json``: ``count``, paginación (páginas 1, 2 y última), orden real
   por ``id`` y ``admissionDate``, fechas mínima / máxima observadas, campos de cabecera;
2. ``/v1/stocks/receptions/{id}/details.json`` de una muestra: líneas, variante, ``quantity``,
   ``cost``, ``id``, paginación, ``variantStock``;
3. ``expand=[details]`` en el listado (día de referencia) y en la recepción individual, comparado
   ítem por ítem contra el endpoint de detalles (un 200 no basta);
4. filtros: ``admissiondate`` exacto, el mismo día a mediodía, ``admissiondaterange`` (no
   documentado), un filtro inventado (¿Bsale ignora filtros desconocidos?), ``officeid`` y
   ``documentnumber``;
5. presencia de ``admissionDate``, ``office``, ``document``, ``documentNumber``, ``user``, ``note``,
   ``internalDispatchId`` y ``variantStock``;
6. ``/v1/variants/{id}/costs.json``: ¿``history[].reception_detail.id`` apunta a la línea recibida y
   con el mismo ``cost``? (evidencia para ``cost`` = costo unitario).
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from backend.debug.bsale_raw_live_probe import API, CompanyAborted, Probe

COMPANY_ID = 3
TOKEN_ENV = "BSALE_TOKEN_SPA"
MAX_REQUESTS = 40
PAGE = 50
DAY = 86400
MAX_DAY_SAMPLE = 3
MAX_DETAIL_SAMPLE = 5
MAX_EXTRA = 2
MAX_COST_LINKS = 3
REPO_ROOT = Path(__file__).resolve().parents[2]

LIST = "/v1/stocks/receptions.json"
HEADER_FIELDS = ("admissionDate", "office", "document", "documentNumber", "user", "note", "internalDispatchId")
DETAIL_FIELDS = ("id", "quantity", "cost", "variant", "variantStock", "serialNumber")


class LimitedProbe(Probe):
    """``Probe`` con el tope de esta fase (40 requests) por encima del tope genérico."""

    def get(self, base: str, path: str, params: dict[str, Any] | None = None, *, label: str | None = None):
        if self.requests >= MAX_REQUESTS:
            raise CompanyAborted(f"tope de {MAX_REQUESTS} requests alcanzado")
        return super().get(base, path, params, label=label)


# --- utilidades puras (sin valores sensibles en la salida) -------------------------------------------


def _int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip().lstrip("-").isdigit():
        return int(value.strip())
    return None


def _num(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def _rel(node: Any) -> int | None:
    return _int(node.get("id")) if isinstance(node, dict) else None


def _items(body: Any) -> list[dict[str, Any]]:
    items = body.get("items") if isinstance(body, dict) else None
    return [i for i in items if isinstance(i, dict)] if isinstance(items, list) else []


def _count(body: Any) -> int | None:
    return _int(body.get("count")) if isinstance(body, dict) else None


def _day(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).date().isoformat()


def label_category(document: Any) -> str:
    """Etiqueta ``document`` agrupable: mayúsculas, dígitos → ``#``, máximo 60 caracteres."""
    if document is None:
        return "<NULL>"
    text = re.sub(r"\d+", "#", str(document).strip().upper())
    return (text or "<VACÍO>")[:60]


def order_of(values: list[Any]) -> str:
    seq = [v for v in values if v is not None]
    if len(seq) < 2:
        return "UNKNOWN"
    if all(a <= b for a, b in zip(seq, seq[1:])):
        return "FLAT" if seq[0] == seq[-1] else "ASC"
    if all(a >= b for a, b in zip(seq, seq[1:])):
        return "DESC"
    return "MIXED"


def _type(value: Any) -> str:
    return "null" if value is None else type(value).__name__


def fields_profile(items: list[dict[str, Any]], *, secret_fields: tuple[str, ...] = ("note",)) -> dict[str, Any]:
    """Por clave: presencia, nulos, tipos y claves de nodos. Nunca valores."""
    out: dict[str, Any] = {}
    for item in items:
        for key, value in item.items():
            f = out.setdefault(key, {"present": 0, "null": 0, "types": set(), "node_keys": set()})
            f["present"] += 1
            f["null"] += value is None
            f["types"].add(_type(value))
            if isinstance(value, dict):
                f["node_keys"].update(value.keys())
            if key in secret_fields:
                f["non_empty"] = f.get("non_empty", 0) + bool(str(value or "").strip())
    for f in out.values():
        f["types"] = sorted(f["types"])
        f["node_keys"] = sorted(f["node_keys"])
    return {"sampled": len(items), "fields": dict(sorted(out.items()))}


def field_check(profile: dict[str, Any], names: tuple[str, ...]) -> dict[str, Any]:
    fields = profile.get("fields", {})
    return {
        n: {"present": fields.get(n, {}).get("present", 0), "of": profile.get("sampled", 0),
            "types": fields.get(n, {}).get("types", [])}
        for n in names
    }


def detail_tuple(item: dict[str, Any]) -> tuple[Any, ...]:
    return (_int(item.get("id")), _rel(item.get("variant")), _num(item.get("quantity")), _num(item.get("cost")))


def detail_stats(items: list[dict[str, Any]]) -> dict[str, Any]:
    tuples = [detail_tuple(i) for i in items]
    ids = [t[0] for t in tuples]
    variants = [t[1] for t in tuples if t[1] is not None]
    qty = [t[2] for t in tuples if t[2] is not None]
    cost = [t[3] for t in tuples if t[3] is not None]
    return {
        "lines": len(items),
        "without_id": sum(1 for i in ids if i is None),
        "duplicate_ids": len(ids) - len(set(ids)),
        "without_variant": len(tuples) - len(variants),
        "repeated_variants": len(variants) - len(set(variants)),
        "quantity": {"min": min(qty, default=None), "max": max(qty, default=None),
                     "non_integer": sum(1 for q in qty if q != int(q)), "zero_or_negative": sum(1 for q in qty if q <= 0)},
        "cost": {"min": min(cost, default=None), "max": max(cost, default=None),
                 "zero": sum(1 for c in cost if c == 0), "null_or_invalid": len(tuples) - len(cost)},
        "variant_stock_present": sum(1 for i in items if "variantStock" in i),
    }


def node_problem(node: Any) -> str | None:
    """``None`` = relación expandida completa; si no, el motivo (``NOT_EXPANDED`` / ``TRUNCATED``)."""
    if not isinstance(node, dict) or not isinstance(node.get("items"), list):
        return "NOT_EXPANDED"
    count = _int(node.get("count"))
    if node.get("next") or _int(node.get("offset")) not in (None, 0):
        return "TRUNCATED"
    if count is not None and count != len(node["items"]):
        return "TRUNCATED"
    return None


def compare_details(node: Any, reference: dict[str, Any] | None) -> str:
    if not reference or reference.get("status") != 200 or reference.get("complete") is not True:
        return "REFERENCE_FAILED"
    problem = node_problem(node)
    if problem:
        return problem
    expanded = [detail_tuple(i) for i in node["items"] if isinstance(i, dict)]
    return "MATCH" if expanded == reference["tuples"] else "MISMATCH"


def expand_verdict(comparisons: dict[str, str]) -> str:
    verdicts = set(comparisons.values())
    if not comparisons or "REFERENCE_FAILED" in verdicts:
        return "INCONCLUSIVE"
    if verdicts == {"NOT_EXPANDED"}:
        return "EXPAND_IGNORED"
    if "MISMATCH" in verdicts or "NOT_EXPANDED" in verdicts:
        return f"EXPAND_UNRELIABLE {sorted(verdicts)}"
    if "TRUNCATED" in verdicts:
        return "EXPAND_PARTIAL (relaciones truncadas: exige respaldo por details.json)"
    if len(comparisons) < 2:
        return "INCONCLUSIVE (menos de 2 recepciones comparadas)"
    return "EXPAND_COMPLETE"


def filter_result(
    status: int | None, body: Any, baseline: int | None, predicate: Callable[[dict[str, Any]], bool]
) -> dict[str, Any]:
    """Un filtro sólo ``WORKS`` si reduce el total y TODOS los ítems devueltos lo cumplen."""
    items = _items(body)
    count = _count(body)
    out: dict[str, Any] = {"status": status, "count": count, "items": len(items)}
    if status != 200 or count is None:
        out["verdict"] = "REJECTED_OR_FAILED"
    elif any(not predicate(i) for i in items):
        out["verdict"] = "IGNORED" if baseline is not None and count == baseline else "INCONSISTENT"
    elif count == 0:
        out["verdict"] = "EMPTY"
    elif baseline is not None and count >= baseline:
        out["verdict"] = "IGNORED_OR_NOT_SELECTIVE"
    else:
        out["verdict"] = "WORKS"
    return out


def cost_link(detail: tuple[Any, ...], admission: int | None, status: int | None, body: Any) -> dict[str, Any]:
    """¿El historial de costos de la variante referencia esta línea por ``reception_detail.id``?"""
    detail_id, variant_id, quantity, cost = detail
    out: dict[str, Any] = {"detail_id": detail_id, "variant_id": variant_id, "quantity": quantity, "status": status}
    history = body.get("history") if isinstance(body, dict) else None
    if status != 200 or not isinstance(history, list):
        out["verdict"] = "REFERENCE_FAILED"
        return out
    out["history_entries"] = len(history)
    matches = [h for h in history if isinstance(h, dict) and _rel(h.get("reception_detail")) == detail_id]
    if not matches:
        out["verdict"] = "NOT_IN_HISTORY"
        return out
    entry = matches[0]
    h_cost = _num(entry.get("cost"))
    out.update(
        history_matches=len(matches),
        same_admission_date=_int(entry.get("admissionDate")) == admission if admission is not None else None,
        has_available_fifo="availableFifo" in entry,
    )
    avg = _num(body.get("averageCost"))
    if avg and cost is not None:
        out["cost_over_average"] = round(cost / avg, 4)
    if h_cost is not None and cost is not None and math.isclose(h_cost, cost, rel_tol=1e-9, abs_tol=1e-6):
        out["verdict"] = "MATCH"
        out["cost_semantics"] = "UNIT" if quantity not in (None, 0, 1) else "UNDETERMINED (quantity 1)"
    elif h_cost is not None and cost is not None and quantity and math.isclose(h_cost * quantity, cost, rel_tol=1e-6):
        out["verdict"] = "COST_DIFFERS"
        out["cost_semantics"] = "TOTAL"
    else:
        out["verdict"] = "COST_DIFFERS"
    return out


def unit_cost_verdict(links: list[dict[str, Any]]) -> str:
    semantics = {l.get("cost_semantics") for l in links if l.get("verdict") in ("MATCH", "COST_DIFFERS")}
    if "UNIT" in semantics and "TOTAL" not in semantics:
        return "UNIT_COST (igual al costo del historial FIFO con quantity ≠ 1; neto vs bruto: verificación manual)"
    if "TOTAL" in semantics and "UNIT" not in semantics:
        return "LINE_TOTAL (cost = costo historial × quantity)"
    return "INCONCLUSIVE"


# --- orquestación (≤ MAX_REQUESTS GET) ----------------------------------------------------------------


def _listing(p: Probe, params: dict[str, Any]) -> tuple[int | None, Any]:
    return p.get(API, LIST, params)


def _details(p: Probe, reception_id: int) -> dict[str, Any]:
    pages: list[dict[str, Any]] = []
    status, body = p.get(API, f"/v1/stocks/receptions/{reception_id}/details.json", {"limit": PAGE, "offset": 0})
    items = _items(body)
    count = _count(body)
    pages.append({"status": status, "count": count, "items": len(items), "has_next": bool((body or {}).get("next"))
                  if isinstance(body, dict) else False})
    if status == 200 and count is not None and count > PAGE:
        status2, body2 = p.get(API, f"/v1/stocks/receptions/{reception_id}/details.json", {"limit": PAGE, "offset": PAGE})
        items += _items(body2)
        pages.append({"status": status2, "count": _count(body2), "items": len(_items(body2))})
        status = status if status2 == 200 else status2
    complete = status == 200 and count is not None and len(items) == count
    return {
        "status": status, "count": count, "pages": pages, "complete": complete, "stats": detail_stats(items),
        "profile": fields_profile(items), "tuples": [detail_tuple(i) for i in items],
    }


def _pick_samples(day_items: list[dict[str, Any]], all_items: list[dict[str, Any]], extra: list[int]) -> list[int]:
    chosen: list[int] = []
    for it in day_items[:MAX_DAY_SAMPLE]:
        rid = _int(it.get("id"))
        if rid is not None and rid not in chosen:
            chosen.append(rid)
    seen = {label_category(it.get("document")) for it in day_items[:MAX_DAY_SAMPLE]}
    for it in reversed(all_items):  # recientes primero (si el listado es ASC)
        if len(chosen) >= MAX_DETAIL_SAMPLE:
            break
        cat = label_category(it.get("document"))
        rid = _int(it.get("id"))
        if cat not in seen and rid is not None and rid not in chosen:
            chosen.append(rid)
            seen.add(cat)
    return chosen + [r for r in extra[:MAX_EXTRA] if r not in chosen]


def _run(p: Probe, out: dict[str, Any], day_ts: int | None, extra: list[int]) -> None:
    status, first = _listing(p, {"limit": PAGE, "offset": 0})
    total = _count(first)
    out["listing"] = {"status": status, "count": total, "page_items": len(_items(first)),
                      "limit": first.get("limit") if isinstance(first, dict) else None,
                      "has_next": bool(first.get("next")) if isinstance(first, dict) else None}
    if status != 200 or total is None:
        return
    pages = {0: _items(first)}
    if total > PAGE:
        _, second = _listing(p, {"limit": PAGE, "offset": PAGE})
        pages[PAGE] = _items(second)
        out["listing"]["second_page_overlap_ids"] = len(
            {_int(i.get("id")) for i in pages[0]} & {_int(i.get("id")) for i in pages[PAGE]}
        )
    last_offset = max(total - PAGE, 0)
    if last_offset > PAGE:
        _, last = _listing(p, {"limit": PAGE, "offset": last_offset})
        pages[last_offset] = _items(last)
    all_items = [it for off in sorted(pages) for it in pages[off]]
    ids = {off: [_int(i.get("id")) for i in its] for off, its in pages.items()}
    dates = [_int(i.get("admissionDate")) for i in all_items]
    first_ids, last_ids = ids[0], ids[max(pages)]
    out["order"] = {
        "pages_read": sorted(pages),
        "ids_within_first_page": order_of(first_ids),
        "dates_within_first_page": order_of([_int(i.get("admissionDate")) for i in pages[0]]),
        "ids_across_pages": order_of([min(filter(None, ids[o]), default=None) for o in sorted(pages)]),
        "dates_across_pages": order_of([min(filter(None, [_int(i.get("admissionDate")) for i in pages[o]]), default=None)
                                        for o in sorted(pages)]),
        "first_page_id_range": [min(filter(None, first_ids), default=None), max(filter(None, first_ids), default=None)],
        "last_page_id_range": [min(filter(None, last_ids), default=None), max(filter(None, last_ids), default=None)],
    }
    known = [d for d in dates if d is not None]
    out["dates"] = {"observed_min": _day(min(known)) if known else None, "observed_max": _day(max(known)) if known else None,
                    "all_midnight_utc": all(d % DAY == 0 for d in known) if known else None,
                    "note": "observadas en las páginas leídas, no en todo el historial"}
    header = fields_profile(all_items)
    out["header_fields"] = header
    out["header_field_check"] = field_check(header, HEADER_FIELDS)
    out["document_categories"] = dict(Counter(label_category(i.get("document")) for i in all_items).most_common())

    # --- filtros --------------------------------------------------------------------------------------
    if day_ts is None:
        day_ts = max(known) if known else None
    filters: dict[str, Any] = {}
    out["filters"] = filters
    day_items: list[dict[str, Any]] = []
    if day_ts is not None:
        out["reference_day"] = {"ts": day_ts, "date": _day(day_ts)}
        st, body = _listing(p, {"limit": PAGE, "offset": 0, "admissiondate": day_ts})
        filters["admissiondate_exact"] = filter_result(st, body, total, lambda i: _int(i.get("admissionDate")) == day_ts)
        day_items = _items(body)
        st, body = _listing(p, {"limit": PAGE, "offset": 0, "admissiondate": day_ts + DAY // 2})
        midday = filter_result(st, body, total, lambda i: day_ts <= (_int(i.get("admissionDate")) or -1) < day_ts + DAY)
        exact_count = filters["admissiondate_exact"].get("count")
        midday["granularity"] = (
            "EXACT_TIMESTAMP" if midday.get("count") == 0 and exact_count
            else "DAY" if midday.get("verdict") == "WORKS" and midday.get("count") == exact_count else "UNKNOWN"
        )
        filters["admissiondate_midday"] = midday
        start = day_ts - 6 * DAY
        st, body = _listing(p, {"limit": PAGE, "offset": 0, "admissiondaterange": f"[{start},{day_ts}]"})
        filters["admissiondaterange_7d"] = filter_result(
            st, body, total, lambda i: start <= (_int(i.get("admissionDate")) or -1) <= day_ts
        )
    st, body = _listing(p, {"limit": 1, "offset": 0, "zzprobeunknownfilter": 1})
    filters["unknown_filter"] = {"status": st, "count": _count(body),
                                 "verdict": "UNKNOWN_FILTERS_IGNORED" if st == 200 and _count(body) == total
                                 else "REJECTED_OR_FILTERED"}
    pool = day_items or all_items
    office = next((_rel(i.get("office")) for i in pool if _rel(i.get("office")) is not None), None)
    if office is not None:
        st, body = _listing(p, {"limit": PAGE, "offset": 0, "officeid": office})
        filters["officeid"] = filter_result(st, body, total, lambda i: _rel(i.get("office")) == office)
    number = next((str(i["documentNumber"]).strip() for i in pool if str(i.get("documentNumber") or "").strip()), None)
    if number is not None:
        st, body = _listing(p, {"limit": PAGE, "offset": 0, "documentnumber": number})
        filters["documentnumber"] = filter_result(st, body, total, lambda i: str(i.get("documentNumber") or "").strip() == number)

    # --- recepción individual (sin y con expand) -----------------------------------------------------
    sample_ids = _pick_samples(day_items, all_items, extra)
    out["sample_reception_ids"] = sample_ids
    individual_node = None
    if sample_ids:
        rid = sample_ids[0]
        st, body = p.get(API, f"/v1/stocks/receptions/{rid}.json")
        out["individual"] = {"reception_id": rid, "status": st, "profile": fields_profile([body] if isinstance(body, dict) else [])}
        st, body = p.get(API, f"/v1/stocks/receptions/{rid}.json", {"expand": "[office,user,details]"})
        profile = fields_profile([body] if isinstance(body, dict) else [])
        out["individual_expanded"] = {"reception_id": rid, "status": st, "profile": profile,
                                      "field_check": field_check(profile, HEADER_FIELDS)}
        individual_node = body.get("details") if isinstance(body, dict) else None

    # --- detalles por endpoint (referencia) ----------------------------------------------------------
    references: dict[int, dict[str, Any]] = {}
    out["details"] = {}
    for rid in sample_ids:
        ref = _details(p, rid)
        references[rid] = ref
        out["details"][str(rid)] = {k: v for k, v in ref.items() if k != "tuples"} | {"sample_lines": ref["tuples"][:3]}
    detail_profiles = [r["profile"] for r in references.values() if r["profile"]["sampled"]]
    if detail_profiles:
        merged = {f: sum(p_["fields"].get(f, {}).get("present", 0) for p_ in detail_profiles) for f in DETAIL_FIELDS}
        out["detail_field_check"] = {"lines": sum(p_["sampled"] for p_ in detail_profiles), "present": merged}

    # --- expand=[details] ------------------------------------------------------------------------------
    comparisons: dict[str, str] = {}
    if sample_ids:
        comparisons[f"individual:{sample_ids[0]}"] = compare_details(individual_node, references.get(sample_ids[0]))
    if day_ts is not None:
        st, body = _listing(p, {"limit": PAGE, "offset": 0, "admissiondate": day_ts, "expand": "[details]"})
        expanded_items = _items(body)
        out["expand_listing"] = {"status": st, "count": _count(body),
                                 "same_count_as_plain": _count(body) == filters.get("admissiondate_exact", {}).get("count")}
        for it in expanded_items:
            rid = _int(it.get("id"))
            if rid in references:
                comparisons[f"listing:{rid}"] = compare_details(it.get("details"), references[rid])
    out["expand_comparisons"] = comparisons

    # --- relación costo ↔ línea recibida -------------------------------------------------------------
    admissions = {_int(i.get("id")): _int(i.get("admissionDate")) for i in all_items + day_items}
    candidates: list[tuple[int, tuple[Any, ...]]] = []
    used_variants: set[int] = set()
    for rid in sample_ids:
        for t in references.get(rid, {}).get("tuples", []):
            if len(candidates) >= MAX_COST_LINKS:
                break
            if t[0] and t[1] and t[1] not in used_variants and t[2] not in (None, 0, 1) and (t[3] or 0) > 0:
                candidates.append((rid, t))
                used_variants.add(t[1])
    links = []
    for rid, t in candidates:
        st, body = p.get(API, f"/v1/variants/{t[1]}/costs.json")
        links.append({"reception_id": rid, **cost_link(t, admissions.get(rid), st, body)})
    out["cost_links"] = links


def verdicts(out: dict[str, Any]) -> dict[str, Any]:
    f = out.get("filters") or {}
    order = out.get("order") or {}
    return {
        "aborted": out.get("aborted"),
        "listing_order_by_id": order.get("ids_across_pages"),
        "listing_order_by_date": order.get("dates_across_pages"),
        "admissiondate_exact": (f.get("admissiondate_exact") or {}).get("verdict"),
        "admissiondate_granularity": (f.get("admissiondate_midday") or {}).get("granularity"),
        "admissiondaterange": (f.get("admissiondaterange_7d") or {}).get("verdict"),
        "unknown_filter": (f.get("unknown_filter") or {}).get("verdict"),
        "officeid": (f.get("officeid") or {}).get("verdict"),
        "documentnumber": (f.get("documentnumber") or {}).get("verdict"),
        "expand_details": expand_verdict(out.get("expand_comparisons") or {}),
        "cost_link": sorted({l.get("verdict") for l in out.get("cost_links") or []}) or None,
        "cost_semantics": unit_cost_verdict(out.get("cost_links") or []),
    }


def strategy(out: dict[str, Any]) -> dict[str, Any]:
    """Recomendación derivada de la evidencia (no ejecuta nada)."""
    v = out.get("verdicts") or verdicts(out)
    total = (out.get("listing") or {}).get("count") or 0
    expand_ok = str(v.get("expand_details")).startswith(("EXPAND_COMPLETE", "EXPAND_PARTIAL"))
    rec: dict[str, Any] = {"total_receptions": total, "listing_pages": math.ceil(total / PAGE) if total else 0}
    if v.get("admissiondaterange") == "WORKS":
        rec["historical"] = "admissiondaterange por ventanas (p. ej. 7–31 días) con count estricto por ventana"
    elif v.get("listing_order_by_id") == "ASC":
        rec["historical"] = ("listado sin filtro paginado por offset (orden ascendente por id), snapshot estricto por "
                             "tramo con cursor; o admissiondate día por día si se quiere acotar por fecha")
    elif v.get("admissiondate_exact") == "WORKS":
        rec["historical"] = "admissiondate día por día (1 request por día aunque esté vacío) con cursor"
    else:
        rec["historical"] = "SIN ESTRATEGIA SEGURA: revisar evidencia"
    rec["details"] = ("expand=[details] en el listado + details.json sólo para relaciones truncadas o inválidas"
                      if expand_ok else "details.json por recepción (paginado completo)")
    rec["incremental"] = (
        ("cola del listado por id (últimas páginas) para recepciones nuevas aunque tengan fecha retroactiva + "
         if v.get("listing_order_by_id") == "ASC" else "")
        + "admissiondate de hoy y ayer + reconciliación de 7 días nocturna y barrida mensual (sin fecha de modificación)"
    )
    rec["estimated_requests_history"] = (
        rec["listing_pages"] if expand_ok else rec["listing_pages"] + total
    )
    return rec


def probe(token: str, *, day_ts: int | None = None, extra: list[int] | None = None,
          make_probe: Callable[[int, str], Probe] = LimitedProbe) -> dict[str, Any]:
    p = make_probe(COMPANY_ID, token)
    out: dict[str, Any] = {"company_id": COMPANY_ID, "max_requests": MAX_REQUESTS}
    try:
        _run(p, out, day_ts, list(extra or []))
    except CompanyAborted as exc:
        out["aborted"] = str(exc)
    out["requests"] = p.requests
    out["request_log"] = p.log
    out["verdicts"] = verdicts(out)
    out["strategy"] = strategy(out)
    return out


def _safe_out_path(raw: str) -> Path:
    path = Path(raw).resolve()
    if path == REPO_ROOT or REPO_ROOT in path.parents:
        raise ValueError("--out debe quedar FUERA del repositorio (p. ej. /tmp/receptions_probe.json)")
    return path


def _day_arg(raw: str | None) -> int | None:
    if not raw:
        return None
    d = datetime.strptime(raw, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(d.timestamp())


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out", required=True, help="ruta del JSON de resultados (fuera del repo)")
    parser.add_argument("--day", help="día de referencia YYYY-MM-DD (UTC 00:00; default: el más reciente observado)")
    parser.add_argument("--reception", type=int, action="append", default=[],
                        help=f"reception_id adicional para details (máx. {MAX_EXTRA})")
    args = parser.parse_args(argv)
    try:
        out_path = _safe_out_path(args.out)
        day_ts = _day_arg(args.day)
    except ValueError as exc:
        print(f"uso inválido: {exc}", file=sys.stderr)
        return 64
    try:
        from dotenv import load_dotenv

        load_dotenv()
    except ImportError:
        pass
    token = (os.getenv(TOKEN_ENV) or "").strip()
    if not token:
        print(f"SKIP: {TOKEN_ENV} ausente")
        return 1
    result = probe(token, day_ts=day_ts, extra=[r for r in args.reception if r > 0])
    payload = json.dumps({"generated_at": datetime.now(timezone.utc).isoformat(), **result},
                         ensure_ascii=False, indent=2, default=str)
    if token in payload:
        print("ABORT: el resultado contendría el token; no se escribe", file=sys.stderr)
        return 1
    out_path.write_text(payload, encoding="utf-8")
    print(f"requests={result['requests']} aborted={result.get('aborted')}")
    for key, value in result["verdicts"].items():
        print(f"{key}={value}")
    print(f"resultados: {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
