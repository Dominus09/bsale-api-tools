#!/usr/bin/env python3
"""
Auditoría planning-rows: EXPLAIN ANALYZE + ranking de nodos.

Uso (en servidor o local con DATABASE_URL / PG_*):
  set PLANNING_ROWS_EXPLAIN=true
  python audit_planning_rows.py --from 2026-05-20 --to 2026-05-22

Salida: /tmp/planning_rows_audit.txt
"""

from __future__ import annotations

import argparse
import os
import sys
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
    parser.add_argument("--limit", type=int, default=401)
    parser.add_argument("--only-not-invoiced", action="store_true", default=True)
    parser.add_argument("--out", default="/tmp/planning_rows_audit.txt")
    args = parser.parse_args()

    d0 = date.fromisoformat(args.d0)
    d1 = date.fromisoformat(args.d1)

    from backend.services.distribuidora.orders_service import (  # noqa: PLC0415
        list_dispatch_prep_planning_rows,
    )

    lines: list[str] = []
    lines.append(f"=== planning-rows audit {d0} .. {d1} ===\n")

    payload = list_dispatch_prep_planning_rows(
        emission_date_from=d0,
        emission_date_to=d1,
        only_not_invoiced=args.only_not_invoiced,
        limit=args.limit,
        offset=0,
    )
    lines.append(f"rows_returned={len(payload.get('items') or [])}")
    dbg = payload.get("_debug") or {}
    timing = dbg.get("timing_ms") or {}
    lines.append(f"timing_ms={timing}\n")

    lines.append("--- EXPLAIN ranking (top nodes by actual time) ---\n")
    lines.append(
        f"{'Consulta / nodo':<42} | {'Tiempo ms':>10} | {'Filas':>8} | Índice / tipo"
    )
    lines.append("-" * 95)
    for n in dbg.get("explain_top") or []:
        rel = n.get("relation") or n.get("node_type") or "?"
        idx = n.get("index") or n.get("node_type") or "—"
        t = n.get("actual_time_ms")
        rows = n.get("rows")
        lines.append(f"{str(rel)[:42]:<42} | {t!s:>10} | {rows!s:>8} | {idx}")

    lines.append("\n--- Recomendaciones ---")
    lines.append("1. Aplicar migración 028_planning_rows_indexes.sql en producción.")
    lines.append("2. Confirmar que el deploy usa SQL sin v_purchase_document_status.")
    lines.append("3. Filtros Pendientes/Probables/Facturadas: solo en frontend (sin re-fetch).")

    out_path = Path(args.out)
    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Written {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
