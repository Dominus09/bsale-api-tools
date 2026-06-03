"""Instrumentación y EXPLAIN para GET dispatch-prep/planning-rows."""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

logger = logging.getLogger(__name__)

LOG_TAG = "[PLANNING_ROWS_DEBUG]"

# Temporal: EXPLAIN ANALYZE en cada request si PLANNING_ROWS_EXPLAIN=1 (o audit_planning_rows.py).
EXPLAIN_ENV = "PLANNING_ROWS_EXPLAIN"

SLOW_NODE_TYPES = frozenset(
    {
        "Seq Scan",
        "Nested Loop",
        "Hash Join",
        "Merge Join",
        "Subquery Scan",
        "Function Scan",
        "Materialize",
        "CTE Scan",
    }
)


def planning_rows_debug_enabled() -> bool:
    return os.environ.get("PLANNING_ROWS_DEBUG", "true").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def explain_analyze_enabled() -> bool:
    return os.environ.get(EXPLAIN_ENV, "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def log_planning_rows(
    message: str,
    *,
    plan_id: int | None = None,
    **fields: Any,
) -> None:
    if not planning_rows_debug_enabled():
        return
    parts = [LOG_TAG, message]
    if plan_id is not None:
        parts.append(f"plan={plan_id}")
    for k, v in fields.items():
        if v is None:
            continue
        parts.append(f"{k}={v}")
    logger.info(" ".join(parts))


class PlanningRowsTimer:
    """Acumula tiempos por fase (ms desde inicio)."""

    def __init__(self) -> None:
        self._t0 = time.perf_counter()
        self.phases: dict[str, float] = {}

    def mark(self, name: str) -> None:
        self.phases[name] = round((time.perf_counter() - self._t0) * 1000.0, 2)

    def lap_ms(self) -> float:
        return round((time.perf_counter() - self._t0) * 1000.0, 2)

    def total_ms(self) -> float:
        return round((time.perf_counter() - self._t0) * 1000.0, 2)


def run_explain_analyze(
    cur,
    sql: str,
    params: tuple[Any, ...],
    *,
    label: str,
) -> dict[str, Any]:
    """EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) + ranking y alertas."""
    cur.execute(
        f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {sql}",
        params,
    )
    raw = cur.fetchone()[0]
    if isinstance(raw, str):
        plan = json.loads(raw)
    else:
        plan = raw
    nodes = _summarize_plan_nodes(plan if isinstance(plan, list) else [plan])
    ranked = rank_explain_nodes(nodes)
    issues = detect_explain_issues(nodes)
    return {
        "label": label,
        "top_nodes": ranked[:12],
        "issues": issues,
        "node_count": len(nodes),
    }


def _summarize_plan_nodes(
    nodes: list[dict[str, Any]],
    *,
    depth: int = 0,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for node in nodes:
        if not isinstance(node, dict):
            continue
        plan = node.get("Plan") if "Plan" in node else node
        if not isinstance(plan, dict):
            continue
        rt = plan.get("Actual Total Time") or plan.get("Total Cost")
        rows = plan.get("Actual Rows") or plan.get("Plan Rows")
        rel = plan.get("Relation Name") or plan.get("Alias")
        node_type = plan.get("Node Type")
        out.append(
            {
                "node_type": node_type,
                "relation": rel,
                "actual_time_ms": round(float(rt), 2) if rt is not None else None,
                "rows": rows,
                "index": plan.get("Index Name"),
                "filter": (plan.get("Filter") or "")[:160] or None,
                "depth": depth,
                "parent_relationship": plan.get("Parent Relationship"),
            }
        )
        sub = plan.get("Plans")
        if isinstance(sub, list):
            out.extend(_summarize_plan_nodes(sub, depth=depth + 1))
    return out


def rank_explain_nodes(nodes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    scored = [
        n
        for n in nodes
        if n.get("actual_time_ms") is not None and n.get("node_type")
    ]
    scored.sort(key=lambda x: float(x.get("actual_time_ms") or 0), reverse=True)
    return scored[:20]


def detect_explain_issues(nodes: list[dict[str, Any]]) -> dict[str, Any]:
    """Seq Scan, Nested Loop, LATERAL (SubPlan), vistas materializadas."""
    seq_scans: list[str] = []
    nested_loops = 0
    laterals = 0
    materialize = 0
    for n in nodes:
        nt = n.get("node_type") or ""
        rel = n.get("relation") or n.get("index") or "?"
        pr = n.get("parent_relationship") or ""
        if nt == "Seq Scan" and rel:
            seq_scans.append(str(rel))
        if nt == "Nested Loop":
            nested_loops += 1
        if pr in ("Outer", "Inner") and "SubPlan" in str(n.get("filter") or ""):
            laterals += 1
        if pr == "SubPlan" or nt == "SubPlan":
            laterals += 1
        if nt in ("Materialize", "CTE Scan"):
            materialize += 1
    return {
        "seq_scan_tables": sorted(set(seq_scans))[:15],
        "nested_loop_count": nested_loops,
        "lateral_subplan_count": laterals,
        "materialize_cte_count": materialize,
        "has_expensive_patterns": bool(seq_scans or nested_loops > 5 or laterals > 0),
    }


def build_phase_ranking(
    *,
    sql_ids_ms: float,
    sql_enrich_ms: float,
    serialize_ms: float,
    json_ms: float,
    total_ms: float,
) -> list[dict[str, Any]]:
    """
    Ranking de fases del endpoint planning-rows.

    observaciones / resumen comunas / KPI: no aplican (otros endpoints o frontend local).
    """
    phases = [
        ("sql_ids", sql_ids_ms, "Fase 1: paginar document_id por fecha"),
        ("sql_enrich", sql_enrich_ms, "Fase 2: status + cliente + probables (batch)"),
        ("serialize_rows", serialize_ms, "Python: filas → dict"),
        ("json_payload", json_ms, "JSON serialización respuesta"),
    ]
    ranked = sorted(phases, key=lambda x: x[1], reverse=True)
    return [
        {
            "phase": name,
            "ms": ms,
            "pct_of_total": round(100.0 * ms / total_ms, 1) if total_ms > 0 else 0,
            "description": desc,
        }
        for name, ms, desc in ranked
    ]


def attach_perf_debug(
    payload: dict[str, Any],
    *,
    timer: PlanningRowsTimer,
    sql_ids_ms: float,
    sql_enrich_ms: float,
    serialize_ms: float,
    json_ms: float,
    explains: list[dict[str, Any]] | None = None,
) -> None:
    total_ms = timer.total_ms()
    payload["_perf"] = {
        "endpoint": "planning-rows",
        "total_ms": total_ms,
        "phase_ranking": build_phase_ranking(
            sql_ids_ms=sql_ids_ms,
            sql_enrich_ms=sql_enrich_ms,
            serialize_ms=serialize_ms,
            json_ms=json_ms,
            total_ms=total_ms,
        ),
        "phases_ms": {
            **timer.phases,
            "sql_ids": sql_ids_ms,
            "sql_enrich": sql_enrich_ms,
            "serialize_rows": serialize_ms,
            "json_payload": json_ms,
        },
        "not_in_endpoint": {
            "observaciones": "GET .../observaciones (paralelo en frontend)",
            "resumen_comunas": "agregado local en frontend",
            "kpi_counts": "filtro local Pendientes/Probables/Facturadas",
        },
        "explain": explains or [],
    }
