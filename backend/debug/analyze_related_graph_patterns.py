"""
FASE 7.9 — Análisis masivo del grafo ``relateddetailid`` sobre OCs reales (solo lectura).

Rango por defecto: emisión UTC 2026-05-13 y 2026-05-14 (inclusive).

Uso:
  python -m backend.debug.analyze_related_graph_patterns
  python -m backend.debug.analyze_related_graph_patterns --date-from 2026-05-13 --date-to 2026-05-14
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections import Counter, defaultdict, deque
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

import pandas as pd

from backend.db import get_connection
from backend.debug.debug_related_graph_oc import (
    COMPANY_ID,
    DOC_TYPE_OC,
    OFFICE_ID,
    TERMINAL_TYPES,
    run_graph,
)
from backend.services.distribuidora.bsale_client import BsaleClient
from backend.utils.bsale_token_env import load_dotenv_if_available, read_bsale_token_from_env

DEFAULT_DATE_FROM = date(2026, 5, 13)
DEFAULT_DATE_TO = date(2026, 5, 14)
JSON_OUT = _REPO / "exports" / "related_graph_analysis_2026_05_13_14.json"
XLSX_OUT = _REPO / "exports" / "related_graph_analysis_2026_05_13_14.xlsx"
MD_OUT = _REPO / "RELATED_GRAPH_PATTERN_REPORT.md"

BETWEEN_OCS_SLEEP = float(__import__("os").getenv("RELATED_GRAPH_ANALYSIS_DELAY_SEC", "0.15"))


def _utc_day_start(d: date) -> datetime:
    return datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=timezone.utc)


def _fetch_oc_document_ids(cur, d0: date, d1: date) -> list[tuple[int, int | None, Any]]:
    """``(document_id, number, emission_date)`` orden estable."""
    start = _utc_day_start(d0)
    end_excl = _utc_day_start(d1) + timedelta(days=1)
    cur.execute(
        """
        SELECT document_id, number, emission_date
        FROM distribuidora.documents
        WHERE company_id = %s
          AND office_id = %s
          AND document_type_id = %s
          AND emission_date IS NOT NULL
          AND emission_date >= %s
          AND emission_date < %s
        ORDER BY number NULLS LAST, document_id
        """,
        (COMPANY_ID, OFFICE_ID, DOC_TYPE_OC, start, end_excl),
    )
    rows: list[tuple[int, int | None, Any]] = []
    for r in cur.fetchall():
        rows.append((int(r[0]), int(r[1]) if r[1] is not None else None, r[2]))
    return rows


def _classify_graph(g: dict[str, Any]) -> tuple[list[str], str]:
    """
    Etiquetas múltiples + ``primary_bucket`` para filas Excel.

    Etiquetas: NO_RELATIONS, ONLY_TYPE_33, ENDS_IN_1_6_9, MULTI_LEVEL_33_CHAIN,
    LOOP_DETECTED, UNRESOLVED_BRANCH, MIXED_BRANCHES
    """
    edges: list[dict[str, Any]] = g.get("edges") or []
    terms: list[dict[str, Any]] = g.get("terminal_documents") or []
    loops: list[dict[str, Any]] = g.get("loops_detected") or []
    unres: list[dict[str, Any]] = g.get("unresolved_branches") or []
    summary = g.get("summary") or {}
    oc33 = int(summary.get("oc_nodes_type_33") or 0)

    types_to = {e.get("to_document_type_id") for e in edges if e.get("to_document_type_id") is not None}

    flags: list[str] = []
    if loops:
        flags.append("LOOP_DETECTED")
    if not edges:
        flags.append("NO_RELATIONS")
    if terms:
        flags.append("ENDS_IN_1_6_9")
    if oc33 >= 2:
        flags.append("MULTI_LEVEL_33_CHAIN")
    if unres:
        flags.append("UNRESOLVED_BRANCH")
    if edges and not terms and types_to and types_to <= {DOC_TYPE_OC}:
        flags.append("ONLY_TYPE_33")
    if terms and unres:
        flags.append("MIXED_BRANCHES")

    # primary (una celda estable)
    if "LOOP_DETECTED" in flags:
        primary = "LOOP_DETECTED"
    elif "NO_RELATIONS" in flags:
        primary = "NO_RELATIONS"
    elif "MIXED_BRANCHES" in flags:
        primary = "MIXED_BRANCHES"
    elif "UNRESOLVED_BRANCH" in flags and "ENDS_IN_1_6_9" not in flags:
        primary = "UNRESOLVED_BRANCH"
    elif "ENDS_IN_1_6_9" in flags:
        primary = "ENDS_IN_1_6_9"
    elif "ONLY_TYPE_33" in flags:
        primary = "ONLY_TYPE_33"
    elif "MULTI_LEVEL_33_CHAIN" in flags:
        primary = "MULTI_LEVEL_33_CHAIN"
    else:
        primary = "OTHER"

    return flags, primary


def _max_edge_depth(edges: list[dict[str, Any]]) -> int:
    if not edges:
        return 0
    return max(int(e.get("depth") or 0) for e in edges)


def _sample_path_string(g: dict[str, Any], root_id: int) -> str | None:
    """Primer camino raíz → terminal (BFS), texto corto."""
    edges = g.get("edges") or []
    nodes = g.get("nodes") or {}
    by_from: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for e in edges:
        by_from[int(e["from_document_id"])].append(e)

    q: deque[tuple[int, list[str]]] = deque()
    q.append((root_id, [f"OC#{nodes.get(str(root_id), {}).get('number', root_id)}(id={root_id})"]))
    seen = {root_id}
    while q:
        doc_id, path = q.popleft()
        for e in by_from.get(doc_id, []):
            to_id = int(e["to_document_id"])
            tt = e.get("to_document_type_id")
            n = nodes.get(str(to_id), {})
            num = n.get("number")
            lbl = {1: "Boleta", 6: "Factura", 9: "NC", 33: "OC"}.get(tt, f"type{tt}")
            step = f"{lbl}#{num}(id={to_id})"
            if tt in TERMINAL_TYPES:
                return " → ".join(path + [step])
            if tt == DOC_TYPE_OC and to_id not in seen:
                seen.add(to_id)
                q.append((to_id, path + [step]))
    return None


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Análisis masivo grafo relateddetailid (OC 33).")
    p.add_argument("--date-from", type=str, default=None, help="YYYY-MM-DD UTC (default 2026-05-13)")
    p.add_argument("--date-to", type=str, default=None, help="YYYY-MM-DD UTC inclusive (default 2026-05-14)")
    p.add_argument(
        "--full-graph",
        action="store_true",
        help="Incluir nodes/edges completos por OC en el JSON (archivo grande).",
    )
    return p


def _parse_day(s: str | None, default: date) -> date:
    if not s:
        return default
    return datetime.strptime(s.strip(), "%Y-%m-%d").date()


def _write_markdown(
    path: Path,
    *,
    d0: date,
    d1: date,
    class_counts: Counter,
    stats: dict[str, Any],
    questions: dict[str, Any],
) -> None:
    lines = [
        "# RELATED_GRAPH_PATTERN_REPORT — FASE 7.9",
        "",
        "Generado automáticamente por `backend/debug/analyze_related_graph_patterns.py`.",
        "",
        f"**Ventana:** `{d0.isoformat()}` → `{d1.isoformat()}` (UTC, días calendario inclusivos).",
        f"**Filtro:** `company_id={COMPANY_ID}`, `office_id={OFFICE_ID}`, `document_type_id={DOC_TYPE_OC}` (OC).",
        "",
        "---",
        "",
        "## Sección 1 — Resumen ejecutivo",
        "",
        f"- **OCs analizadas:** {stats['total_oc']}",
        f"- **Con al menos una arista `relateddetailid` desde la raíz:** {stats['total_with_related']}",
        f"- **Sin aristas (solo raíz / sin matches API):** {stats['total_without_related']}",
        f"- **Clasificación primaria `ENDS_IN_1_6_9`:** {class_counts.get('ENDS_IN_1_6_9', 0)}",
        f"- **Solo cadenas hacia otras OC (33) sin terminal:** {stats['total_only_33']}",
        f"- **Loops detectados (reentrada OC 33):** {stats['total_loops']}",
        f"- **Ramas no resueltas (API/parse/sin detalle/profundidad):** {stats['total_unresolved']}",
        f"- **Cadenas multi-nivel 33 (≥2 nodos OC en el grafo):** {stats['total_multi_level_33']}",
        f"- **Aristas y nodos agregados (suma por OC):** edges={stats['total_edges']}, nodes={stats['total_nodes']}",
        f"- **Profundidad máxima de arista observada:** {stats['max_depth_found']}",
        f"- **Profundidad media (por OC, máx. prof. arista):** {round(stats['average_depth'], 4)}",
        "",
        "Distribución por **bucket primario** (`primary_bucket`):",
        "",
    ]
    for k, v in stats.get("primary_bucket_counts", {}).items():
        lines.append(f"- `{k}`: **{v}**")
    lines.extend(
        [
            "",
            "---",
            "",
            "## Sección 2 — Patrones detectados",
            "",
            "Conteo de **flags** (una OC puede tener varias etiquetas):",
            "",
        ],
    )
    for k, v in class_counts.most_common():
        lines.append(f"- `{k}`: **{v}**")
    lines.extend(
        [
            "",
            "---",
            "",
            "## Sección 3 — Semántica probable de `document_type_id = 33`",
            "",
            "En los grafos construidos solo con `relateddetailid`, un vínculo hacia otro documento 33 suele interpretarse como **continuidad operacional** (reemisión, modificación sustitutiva o nueva versión de OC), no como facturación.",
            "Los documentos **1 / 6 / 9** aparecen como **hojas de venta** cuando Bsale indexa la relación por línea (`relateddetailid`) hacia esos tipos.",
            "",
            "---",
            "",
            "## Sección 4 — ¿`relateddetailid` parece suficiente?",
            "",
            f"- OCs que **llegan a terminal 1/6/9** (flag `ENDS_IN_1_6_9`) en este rango: **{questions['ends_terminal_count']}** de {stats['total_oc']} ({questions['pct_ends_terminal']} %).",
            f"- OCs **sin ninguna arista** `relateddetailid` desde sus líneas: **{questions['no_rel_count']}** ({questions['pct_no_rel']} %).",
            "",
            "Si el porcentaje de terminales es alto y estable en el tiempo, `relateddetailid` (más eventual cadena 33) puede modelar buena parte de la **facturación por línea**.",
            "Si muchas OCs “facturadas” en negocio caen en `NO_RELATIONS` o solo `ONLY_TYPE_33`, el modelo **no** bastará sin otra fuente (p. ej. `references`).",
            "",
            "---",
            "",
            "## Sección 5 — ¿Cuándo sería necesario `references`?",
            "",
            "- Cuando la factura exista en Bsale pero **no** aparezca en ninguna respuesta `documents.json?relateddetailid=` desde las líneas de la OC.",
            "- Cuando solo existan vínculos **documento↔documento** tributarios o de nota de referencia.",
            "- Cuando el negocio requiera **paridad con DTE/XML** más que con despacho por línea.",
            "",
            "---",
            "",
            "## Sección 6 — Recomendación técnica final (heurística)",
            "",
        ],
    )
    rec = questions.get("recommendation", "C")
    rec_text = {
        "A": "**A) `relateddetailid` puro** — La mayoría de OCs llega a 1/6/9 sin ambigüedad; cadena 33 rara.",
        "B": "**B) `relateddetailid` + resolución recursiva 33** — Muchos terminales tras una o más OC 33; conviene seguir cadenas 33 acotadas antes de fallback.",
        "C": "**C) `relateddetailid` + `references` híbrido** — Mezcla fuerte de `NO_RELATIONS` / solo 33 y terminales; usar referencias para cobertura.",
        "D": "**D) `references` obligatorio** — `relateddetailid` casi no discrimina facturación en este rango (no recomendado sin evidencia adicional).",
    }
    lines.append(rec_text.get(rec, rec_text["C"]))
    lines.append("")
    lines.append(f"_Heurística aplicada en este run: **{rec}** (`{questions.get('recommendation_rationale', '')}`)._")
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## Anexos")
    lines.append("")
    lines.append(f"- JSON: `{JSON_OUT.relative_to(_REPO)}`")
    lines.append(f"- Excel: `{XLSX_OUT.relative_to(_REPO)}`")
    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


RECOMMENDATION_STDOUT: dict[str, str] = {
    "A": "A) relateddetailid puro",
    "B": "B) relateddetailid + recursión 33",
    "C": "C) relateddetailid + references híbrido",
    "D": "D) references obligatorio",
}


def _terminal_types_str(g: dict[str, Any]) -> str:
    terms = g.get("terminal_documents") or []
    ids = sorted(
        {int(t["document_type_id"]) for t in terms if t.get("document_type_id") is not None},
    )
    return ",".join(str(x) for x in ids) if ids else "-"


def _print_stdout_summary(
    *,
    d0: date,
    d1: date,
    stats: dict[str, Any],
    class_counter: Counter[str],
    recommendation: str,
    rationale: str,
    per_oc: list[dict[str, Any]],
) -> None:
    cc = class_counter
    sep = "=" * 60
    print()
    print(sep)
    print("RELATED GRAPH ANALYSIS — SUMMARY")
    print(sep)
    print(f"date_range_utc: {d0.isoformat()} .. {d1.isoformat()} (inclusive)")
    print()
    print(f"total_oc:                 {stats['total_oc']}")
    print(f"total_with_related:       {stats['total_with_related']}")
    print(f"total_without_related:    {stats['total_without_related']}")
    print()
    print(f"NO_RELATIONS:             {cc.get('NO_RELATIONS', 0)}")
    print(f"ONLY_TYPE_33:             {cc.get('ONLY_TYPE_33', 0)}")
    print(f"ENDS_IN_1_6_9:            {cc.get('ENDS_IN_1_6_9', 0)}")
    print(f"MULTI_LEVEL_33_CHAIN:     {cc.get('MULTI_LEVEL_33_CHAIN', 0)}")
    print(f"LOOP_DETECTED:            {cc.get('LOOP_DETECTED', 0)}")
    print(f"UNRESOLVED_BRANCH:      {cc.get('UNRESOLVED_BRANCH', 0)}")
    print(f"MIXED_BRANCHES:           {cc.get('MIXED_BRANCHES', 0)}")
    print()
    print(f"total_terminal_1 (edges): {stats.get('total_terminal_1', 0)}")
    print(f"total_terminal_6 (edges): {stats.get('total_terminal_6', 0)}")
    print(f"total_terminal_9 (edges): {stats.get('total_terminal_9', 0)}")
    print()
    print(f"max_depth_found:          {stats['max_depth_found']}")
    print(f"average_depth:            {round(stats['average_depth'], 6)}")
    print()
    print(f"total_nodes:              {stats['total_nodes']}")
    print(f"total_edges:              {stats['total_edges']}")
    print(f"total_api_calls:          {stats.get('total_api_calls', 0)}")
    print()
    rec_line = RECOMMENDATION_STDOUT.get(recommendation, f"{recommendation} (desconocido)")
    print("recommendation:")
    print(f"  {rec_line}")
    print(f"  ({rationale})")
    print(sep)

    def _top(flag: str, title: str) -> None:
        print()
        print(f"TOP 10 — {title}")
        rows = [r for r in per_oc if flag in (r.get("classifications") or [])][:10]
        if not rows:
            print("  (ninguno)")
            return
        for r in rows:
            print(
                f"  OC {r.get('number')} / doc_id {r.get('document_id')} / "
                f"depth {r.get('max_edge_depth')} / terminal_types {r.get('terminal_types', '-')}",
            )

    _top("ONLY_TYPE_33", "ONLY_TYPE_33")
    _top("ENDS_IN_1_6_9", "ENDS_IN_1_6_9")
    _top("UNRESOLVED_BRANCH", "UNRESOLVED_BRANCH")
    print()
    print(sep)
    print()


def _excel_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    if "classifications" in out.columns:
        out["classifications"] = out["classifications"].apply(
            lambda x: ",".join(x) if isinstance(x, list) else str(x),
        )
    return out


def main() -> int:
    load_dotenv_if_available()
    args = _build_arg_parser().parse_args()
    d0 = _parse_day(args.date_from, DEFAULT_DATE_FROM)
    d1 = _parse_day(args.date_to, DEFAULT_DATE_TO)
    if d1 < d0:
        d0, d1 = d1, d0

    token = read_bsale_token_from_env()
    if not token:
        print("Defina BSALE_TOKEN o BSALE_TOKEN_SPA.", file=sys.stderr)
        return 2

    conn = get_connection()
    cur = conn.cursor()
    oc_rows: list[tuple[int, int | None, Any]]
    try:
        oc_rows = _fetch_oc_document_ids(cur, d0, d1)
    finally:
        cur.close()
        conn.close()

    total_oc = len(oc_rows)
    print(f"OCs a analizar: {total_oc} (UTC {d0} .. {d1})")

    client = BsaleClient(token)
    per_oc: list[dict[str, Any]] = []
    class_counter: Counter[str] = Counter()
    primary_counter: Counter[str] = Counter()

    sum_edges = 0
    sum_nodes = 0
    sum_api = 0
    depths: list[int] = []
    total_terminal_1 = total_terminal_6 = total_terminal_9 = 0
    total_with_related = 0
    total_without_related = 0
    total_only_33 = 0
    total_loops = 0
    total_unresolved_oc = 0
    total_multi_level = 0
    sample_paths: list[dict[str, Any]] = []
    unresolved_cases: list[dict[str, Any]] = []

    for i, (doc_id, number, emission_date) in enumerate(oc_rows):
        if i and i % 10 == 0:
            print(f"  … {i}/{total_oc}")
        g = run_graph(client, doc_id, number, root_source="db")
        flags, primary = _classify_graph(g)
        for f in flags:
            class_counter[f] += 1
        primary_counter[primary] += 1

        edges = g.get("edges") or []
        sum_edges += len(edges)
        sum_nodes += int((g.get("summary") or {}).get("total_nodes") or 0)
        sum_api += int(g.get("api_calls") or 0)
        md = _max_edge_depth(edges)
        depths.append(md)

        if edges:
            total_with_related += 1
        else:
            total_without_related += 1
        if "ONLY_TYPE_33" in flags:
            total_only_33 += 1
        if "LOOP_DETECTED" in flags:
            total_loops += 1
        if "UNRESOLVED_BRANCH" in flags:
            total_unresolved_oc += 1
        if "MULTI_LEVEL_33_CHAIN" in flags:
            total_multi_level += 1

        for t in g.get("terminal_documents") or []:
            tid = t.get("document_type_id")
            if tid == 1:
                total_terminal_1 += 1
            elif tid == 6:
                total_terminal_6 += 1
            elif tid == 9:
                total_terminal_9 += 1

        entry: dict[str, Any] = {
            "document_id": doc_id,
            "number": number,
            "emission_date": str(emission_date) if emission_date is not None else None,
            "classifications": flags,
            "primary_bucket": primary,
            "api_calls": g.get("api_calls"),
            "total_nodes": (g.get("summary") or {}).get("total_nodes"),
            "total_edges": (g.get("summary") or {}).get("total_edges"),
            "oc_nodes_type_33": (g.get("summary") or {}).get("oc_nodes_type_33"),
            "terminal_count": len(g.get("terminal_documents") or []),
            "loops_count": len(g.get("loops_detected") or []),
            "unresolved_count": len(g.get("unresolved_branches") or []),
            "max_edge_depth": md,
            "graph_conclusion": (g.get("summary") or {}).get("conclusion"),
            "terminal_types": _terminal_types_str(g),
        }
        if args.full_graph:
            entry["nodes"] = g.get("nodes")
            entry["edges"] = g.get("edges")
            entry["terminal_documents"] = g.get("terminal_documents")
            entry["loops_detected"] = g.get("loops_detected")
            entry["unresolved_branches"] = g.get("unresolved_branches")

        path_str = _sample_path_string(g, doc_id)
        entry["sample_path"] = path_str
        per_oc.append(entry)

        if path_str and len(sample_paths) < 25 and "ENDS_IN_1_6_9" in flags:
            sample_paths.append(
                {
                    "document_id": doc_id,
                    "oc_number": number,
                    "primary_bucket": primary,
                    "path": path_str,
                },
            )
        if "UNRESOLVED_BRANCH" in flags and len(unresolved_cases) < 80:
            unresolved_cases.append(
                {
                    "document_id": doc_id,
                    "oc_number": number,
                    "primary_bucket": primary,
                    "unresolved": (g.get("unresolved_branches") or [])[:12],
                },
            )

        if BETWEEN_OCS_SLEEP > 0:
            time.sleep(BETWEEN_OCS_SLEEP)

    avg_depth = sum(depths) / len(depths) if depths else 0.0
    max_depth_found = max(depths) if depths else 0

    ends_terminal_oc_count = int(class_counter.get("ENDS_IN_1_6_9", 0))
    pct_ends = round(100.0 * ends_terminal_oc_count / total_oc, 2) if total_oc else 0.0
    pct_no_rel = round(100.0 * total_without_related / total_oc, 2) if total_oc else 0.0

    # Heurística recomendación A/B/C/D
    pct_terminalish = (class_counter.get("ENDS_IN_1_6_9", 0) / total_oc) if total_oc else 0
    pct_only33 = (class_counter.get("ONLY_TYPE_33", 0) / total_oc) if total_oc else 0
    pct_no_rel_class = (class_counter.get("NO_RELATIONS", 0) / total_oc) if total_oc else 0
    if pct_terminalish >= 0.55 and pct_only33 < 0.25 and total_loops <= max(1, total_oc // 50):
        recommendation, rationale = "A", "Alta tasa de terminales, pocas solo-33, pocos loops"
    elif pct_terminalish >= 0.35 or class_counter.get("MULTI_LEVEL_33_CHAIN", 0) >= max(3, total_oc // 20):
        recommendation, rationale = "B", "Terminales frecuentes o cadenas 33 relevantes"
    elif pct_no_rel_class >= 0.35 or pct_only33 >= 0.30:
        recommendation, rationale = "C", "Muchas sin related o solo 33; conviene híbrido con references"
    elif pct_terminalish < 0.10 and pct_no_rel_class > 0.45:
        recommendation, rationale = "D", "Muy pocas OCs con terminal 1/6/9 y muchas sin aristas relateddetailid"
    else:
        recommendation, rationale = "C", "Distribución mixta por defecto"

    stats = {
        "total_oc": total_oc,
        "ends_terminal_oc_count": ends_terminal_oc_count,
        "total_with_related": total_with_related,
        "total_without_related": total_without_related,
        "total_only_33": total_only_33,
        "total_terminal_1": total_terminal_1,
        "total_terminal_6": total_terminal_6,
        "total_terminal_9": total_terminal_9,
        "total_unresolved": total_unresolved_oc,
        "total_loops": total_loops,
        "total_multi_level_33": total_multi_level,
        "max_depth_found": max_depth_found,
        "average_depth": avg_depth,
        "total_edges": sum_edges,
        "total_nodes": sum_nodes,
        "total_api_calls": sum_api,
        "primary_bucket_counts": dict(primary_counter),
        "classification_flag_counts": dict(class_counter),
    }

    questions = {
        "ends_terminal_count": ends_terminal_oc_count,
        "pct_ends_terminal": pct_ends,
        "no_rel_count": total_without_related,
        "pct_no_rel": pct_no_rel,
        "recommendation": recommendation,
        "recommendation_rationale": rationale,
    }

    out_json = {
        "metadata": {
            "date_from_utc": d0.isoformat(),
            "date_to_utc_inclusive": d1.isoformat(),
            "company_id": COMPANY_ID,
            "office_id": OFFICE_ID,
            "document_type_oc": DOC_TYPE_OC,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        },
        "summary": stats,
        "classifications": dict(class_counter),
        "per_oc_analysis": per_oc,
        "statistics": stats,
        "sample_paths": sample_paths,
        "unresolved_cases": unresolved_cases,
    }

    JSON_OUT.parent.mkdir(parents=True, exist_ok=True)
    JSON_OUT.write_text(json.dumps(out_json, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(f"JSON: {JSON_OUT}")

    df_all = pd.DataFrame(per_oc)
    df_summary = pd.DataFrame([stats])
    df_only33 = df_all[df_all["classifications"].apply(lambda x: "ONLY_TYPE_33" in x)]
    df_ends = df_all[df_all["classifications"].apply(lambda x: "ENDS_IN_1_6_9" in x)]
    df_loops = df_all[df_all["classifications"].apply(lambda x: "LOOP_DETECTED" in x)]
    df_unres = df_all[df_all["classifications"].apply(lambda x: "UNRESOLVED_BRANCH" in x)]
    df_multi = df_all[df_all["classifications"].apply(lambda x: "MULTI_LEVEL_33_CHAIN" in x)]
    df_paths = pd.DataFrame(sample_paths)

    with pd.ExcelWriter(XLSX_OUT, engine="openpyxl") as xw:
        _excel_df(df_summary).to_excel(xw, sheet_name="summary", index=False)
        _excel_df(df_all).to_excel(xw, sheet_name="all_oc", index=False)
        _excel_df(df_only33).to_excel(xw, sheet_name="only_33", index=False)
        _excel_df(df_ends).to_excel(xw, sheet_name="ends_in_terminal", index=False)
        _excel_df(df_loops).to_excel(xw, sheet_name="loops", index=False)
        _excel_df(df_unres).to_excel(xw, sheet_name="unresolved", index=False)
        _excel_df(df_multi).to_excel(xw, sheet_name="multi_level_chains", index=False)
        _excel_df(df_paths).to_excel(xw, sheet_name="sample_paths", index=False)

    print(f"Excel: {XLSX_OUT}")

    _write_markdown(MD_OUT, d0=d0, d1=d1, class_counts=class_counter, stats=stats, questions=questions)
    print(f"Markdown: {MD_OUT}")

    _print_stdout_summary(
        d0=d0,
        d1=d1,
        stats=stats,
        class_counter=class_counter,
        recommendation=recommendation,
        rationale=rationale,
        per_oc=per_oc,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
