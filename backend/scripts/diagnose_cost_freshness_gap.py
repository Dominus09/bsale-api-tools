#!/usr/bin/env python3
"""Diagnóstico READ-ONLY del desfase analytics vs RAW (company 3 por defecto).

No escribe. No requiere SELECT en stock_receptions: usa sync_state/sync_runs +
analytics.cost_reception_history / cost_sync_state.

Uso:
  python -m backend.scripts.diagnose_cost_freshness_gap --company-id 3
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime
from typing import Any

from backend.db import get_connection
from backend.services.analytics_v2.cost_freshness_diagnosis import (
    FreshnessSignals,
    classify_cost_freshness,
    windows_cover_after,
)


def _fetch(cur, sql: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
    cur.execute(sql, params)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def collect_signals(cur, company_id: int) -> tuple[FreshnessSignals, dict[str, Any]]:
    analytics_months = _fetch(
        cur,
        """
        SELECT
            date_trunc('month', admission_date)::date AS mes,
            count(*)::bigint AS lineas,
            count(DISTINCT reception_id)::bigint AS recepciones,
            max(admission_date) AS max_adm
        FROM analytics.cost_reception_history
        WHERE company_id = %s
          AND admission_date >= DATE '2026-01-01'
        GROUP BY 1
        ORDER BY 1
        """,
        (company_id,),
    )
    sync_state = _fetch(
        cur,
        """
        SELECT *
        FROM analytics.cost_sync_state
        WHERE company_id = %s
        """,
        (company_id,),
    )
    raw_state = _fetch(
        cur,
        """
        SELECT resource, scope, status, last_success_at, rows_received, last_sync_run_id
        FROM bsale_raw.sync_state
        WHERE company_id = %s AND resource = 'stock_receptions'
        ORDER BY scope
        """,
        (company_id,),
    )
    windows = _fetch(
        cur,
        """
        SELECT
            id,
            started_at,
            summary->>'scope' AS scope,
            (summary->>'rows_received')::int AS rows_received,
            (summary->'point'->>'lines')::int AS lines,
            (summary->'point'->>'created')::int AS created
        FROM bsale_raw.sync_runs
        WHERE (summary->>'resource') = 'stock_receptions'
          AND (summary->>'company_id') = %s
          AND mode = 'WINDOW_RECONCILE'
          AND started_at >= TIMESTAMPTZ '2026-10-01'
        ORDER BY started_at DESC
        LIMIT 40
        """,
        (str(company_id),),
    )

    max_adm: date | None = None
    for row in analytics_months:
        mad = row.get("max_adm")
        if mad is None:
            continue
        d = mad.date() if isinstance(mad, datetime) else mad
        if max_adm is None or d > max_adm:
            max_adm = d

    css = sync_state[0] if sync_state else {}
    admission_scope = next(
        (r for r in raw_state if r.get("scope") == "admission_date"),
        raw_state[0] if raw_state else {},
    )
    scopes = [str(w.get("scope") or "") for w in windows]
    cover = windows_cover_after(scopes, after=max_adm) if max_adm else bool(scopes)

    signals = FreshnessSignals(
        analytics_max_admission=max_adm,
        analytics_last_run_at=css.get("last_run_at"),
        analytics_last_message=css.get("last_message"),
        raw_receptions_last_success_at=admission_scope.get("last_success_at"),
        raw_receptions_status=admission_scope.get("status"),
        raw_window_covers_after_analytics=cover,
        sync_cost_receptions_is_manual=True,
    )
    extras = {
        "analytics_months_2026": [
            {
                "mes": str(r["mes"]),
                "lineas": int(r["lineas"]),
                "recepciones": int(r["recepciones"]),
            }
            for r in analytics_months
        ],
        "cost_sync_state": {
            k: (v.isoformat() if isinstance(v, datetime) else v)
            for k, v in css.items()
        },
        "raw_sync_state": [
            {
                k: (v.isoformat() if isinstance(v, datetime) else v)
                for k, v in r.items()
            }
            for r in raw_state
        ],
        "recent_raw_windows": [
            {
                "id": w["id"],
                "started_at": w["started_at"].isoformat()
                if isinstance(w["started_at"], datetime)
                else w["started_at"],
                "scope": w["scope"],
                "rows_received": w["rows_received"],
                "lines": w["lines"],
                "created": w["created"],
            }
            for w in windows[:12]
        ],
    }
    return signals, extras


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--company-id", type=int, default=3)
    args = parser.parse_args(argv)

    conn = get_connection()
    try:
        cur = conn.cursor()
        signals, extras = collect_signals(cur, args.company_id)
        diagnosis = classify_cost_freshness(signals)
        payload = {
            "ok": True,
            "company_id": args.company_id,
            "diagnosis": diagnosis,
            **extras,
            "catch_up_hint": (
                "Dry-run: python -m backend.jobs.sync_cost_receptions "
                f"--company-id {args.company_id} "
                "--date-from YYYY-MM-DD --date-to YYYY-MM-DD "
                "--history-only --dry-run"
            ),
        }
        print(json.dumps(payload, default=str, ensure_ascii=False, indent=2))
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
