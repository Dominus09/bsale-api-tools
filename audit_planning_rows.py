#!/usr/bin/env python3
"""
Auditoría planning-rows: fases + EXPLAIN ANALYZE (dos consultas).

Uso:
  set PLANNING_ROWS_EXPLAIN=true
  python audit_planning_rows.py --from 2026-06-02 --to 2026-06-02 --limit 500

Salida: /tmp/planning_rows_audit.txt
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

os.environ.setdefault("PLANNING_ROWS_DEBUG", "true")
os.environ["PLANNING_ROWS_EXPLAIN"] = "true"


def main() -> int:
    parser = argparse.ArgumentParser(description="Auditoría planning-rows")
    parser.add_argument("--from", dest="d0", required=True, help="YYYY-MM-DD")
    parser.add_argument("--to", dest="d1", required=True, help="YYYY-MM-DD")
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--only-not-invoiced", action="store_true", default=True)
    parser.add_argument("--out", default="/tmp/planning_rows_audit.txt")
    args = parser.parse_args()

    d0 = date.fromisoformat(args.d0)
    d1 = date.fromisoformat(args.d1)

    from backend.services.distribuidora.orders_service import (  # noqa: PLC0415
        list_dispatch_prep_planning_rows,
    )

    lines: list[str] = []
    lines.append(f"=== planning-rows audit {d0} .. {d1} limit={args.limit} ===\n")

    t0 = time.perf_counter()
    payload = list_dispatch_prep_planning_rows(
        emission_date_from=d0,
        emission_date_to=d1,
        only_not_invoiced=args.only_not_invoiced,
        limit=args.limit,
        offset=0,
    )
    wall_s = time.perf_counter() - t0
    lines.append(f"wall_seconds={wall_s:.2f}")
    lines.append(f"rows_returned={len(payload.get('items') or [])}\n")

    stage_profile = payload.get("_stage_profile") or {}
    if stage_profile.get("stages"):
        lines.append("--- [PLANNING_ROWS_STAGE] ranking ---\n")
        for i, rec in enumerate(stage_profile["stages"], start=1):
            lines.append(
                f"  {i}. {rec.get('stage')}: {rec.get('elapsed_ms')} ms "
                f"(rows={rec.get('rows_count')})"
            )
        lines.append(f"  total: {stage_profile.get('total_ms')} ms\n")

    perf = payload.get("_perf") or {}
    lines.append("--- Ranking fases (solo endpoint planning-rows) ---\n")
    for row in perf.get("phase_ranking") or []:
        lines.append(
            f"  {row.get('phase'):<16} {row.get('ms'):>8} ms  "
            f"({row.get('pct_of_total')}%)  {row.get('description')}"
        )
    lines.append("\n--- No medidos en este endpoint (frontend) ---")
    for k, v in (perf.get("not_in_endpoint") or {}).items():
        lines.append(f"  {k}: {v}")

    lines.append("\n--- EXPLAIN ANALYZE por consulta ---\n")
    for block in perf.get("explain") or []:
        label = block.get("label", "?")
        lines.append(f"### {label}")
        issues = block.get("issues") or {}
        lines.append(f"  seq_scan: {issues.get('seq_scan_tables')}")
        lines.append(f"  nested_loops: {issues.get('nested_loop_count')}")
        lines.append(f"  lateral/subplan: {issues.get('lateral_subplan_count')}")
        lines.append(f"  materialize/cte: {issues.get('materialize_cte_count')}")
        lines.append(
            f"{'Nodo':<40} | {'ms':>8} | {'rows':>8} | tipo / índice"
        )
        lines.append("-" * 80)
        for n in block.get("top_nodes") or []:
            rel = n.get("relation") or n.get("node_type") or "?"
            idx = n.get("index") or n.get("node_type") or "—"
            lines.append(
                f"{str(rel)[:40]:<40} | {n.get('actual_time_ms')!s:>8} | "
                f"{n.get('rows')!s:>8} | {idx}"
            )
        lines.append("")

    lines.append("--- Recomendaciones ---")
    lines.append("1. Migraciones 028 + 029 en BD de producción.")
    lines.append("2. Fase 2 usa batch DISTINCT ON (sin LATERAL x500).")
    lines.append("3. Sin v_orders_purchase_status / v_documents_latest en planning-rows.")
    lines.append("4. KPI / comunas / observaciones: otros endpoints o cliente local.")

    out_path = Path(args.out)
    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Written {out_path} ({wall_s:.2f}s)")
    if perf:
        print(json.dumps(perf.get("phase_ranking"), indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
