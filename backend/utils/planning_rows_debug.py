"""Instrumentación y EXPLAIN para GET dispatch-prep/planning-rows."""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

logger = logging.getLogger(__name__)

LOG_TAG = "[PLANNING_ROWS_DEBUG]"


def planning_rows_debug_enabled() -> bool:
    return os.environ.get("PLANNING_ROWS_DEBUG", "true").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def explain_analyze_enabled() -> bool:
    return os.environ.get("PLANNING_ROWS_EXPLAIN", "").strip().lower() in (
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
    """Acumula tiempos por fase."""

    def __init__(self) -> None:
        self._t0 = time.perf_counter()
        self.phases: dict[str, float] = {}

    def mark(self, name: str) -> None:
        self.phases[name] = round((time.perf_counter() - self._t0) * 1000.0, 2)

    def total_ms(self) -> float:
        return round((time.perf_counter() - self._t0) * 1000.0, 2)


def run_explain_analyze(cur, sql: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
    """Ejecuta EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) y resume nodos."""
    cur.execute(
        f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {sql}",
        params,
    )
    raw = cur.fetchone()[0]
    if isinstance(raw, str):
        plan = json.loads(raw)
    else:
        plan = raw
    return _summarize_plan_nodes(plan if isinstance(plan, list) else [plan])


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
        out.append(
            {
                "node_type": plan.get("Node Type"),
                "relation": plan.get("Relation Name") or plan.get("Alias"),
                "actual_time_ms": round(float(rt), 2) if rt is not None else None,
                "rows": rows,
                "index": plan.get("Index Name"),
                "filter": (plan.get("Filter") or "")[:120] or None,
                "depth": depth,
            }
        )
        sub = plan.get("Plans")
        if isinstance(sub, list):
            out.extend(_summarize_plan_nodes(sub, depth=depth + 1))
    return out


def rank_explain_nodes(nodes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Ranking: nodos con mayor actual_time_ms."""
    scored = [
        n
        for n in nodes
        if n.get("actual_time_ms") is not None and n.get("node_type")
    ]
    scored.sort(key=lambda x: float(x.get("actual_time_ms") or 0), reverse=True)
    return scored[:15]
