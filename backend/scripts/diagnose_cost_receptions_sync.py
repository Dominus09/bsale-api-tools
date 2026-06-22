#!/usr/bin/env python3
"""
Diagnóstico sync_cost_receptions — NO modifica datos.

Uso (con variables PG_* y tokens Bsale en entorno):
  python -m backend.scripts.diagnose_cost_receptions_sync
  python -m backend.scripts.diagnose_cost_receptions_sync --company-id 1 --lookback-days 15
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone

from backend.db import get_connection
from backend.repositories import cost_analytics_repo as repo
from backend.services.distribuidora.bsale_client import BsaleClient
from backend.services.sync_cost_receptions import INITIAL_LOOKBACK_DAYS, LIMIT

HISTORY = "analytics.cost_reception_history"
SYNC_STATE = "analytics.cost_sync_state"


def _ts_fmt(ts: int | None) -> str:
    if not ts:
        return "(null)"
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _resolve_since_ts(
    state: dict | None,
    *,
    lookback_days: int | None,
) -> tuple[int, str]:
    """Replica exacta de sync_cost_receptions (sin modificar)."""
    if state and state.get("last_admission_ts"):
        since = int(state["last_admission_ts"]) - 86400
        reason = (
            f"INCREMENTAL: last_admission_ts={state['last_admission_ts']} "
            f"({_ts_fmt(state['last_admission_ts'])}) - 86400s overlap"
        )
        return since, reason
    days = lookback_days if lookback_days is not None else INITIAL_LOOKBACK_DAYS
    since = int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp())
    reason = f"LOOKBACK: {days} días (lookback_days={lookback_days}, default={INITIAL_LOOKBACK_DAYS})"
    return since, reason


def _load_companies(cur, company_id: int | None) -> list[dict]:
    q = """
        SELECT company_id, name, bsale_token
        FROM bsale.companies
        WHERE active = TRUE
    """
    params: list = []
    if company_id is not None:
        q += " AND company_id = %s"
        params.append(company_id)
    q += " ORDER BY company_id"
    cur.execute(q, params)
    out = []
    for cid, name, token_key in cur.fetchall():
        token = os.getenv(str(token_key or "").strip())
        out.append(
            {
                "company_id": int(cid),
                "name": name,
                "token_key": token_key,
                "token_ok": bool(token),
            }
        )
    return out


def _scan_bsale(
    client: BsaleClient,
    *,
    since_ts: int,
    window_days: int,
) -> dict:
    window_start = int(
        (datetime.now(timezone.utc) - timedelta(days=window_days)).timestamp()
    )
    now_ts = int(datetime.now(timezone.utc).timestamp())

    stats = {
        "pages_fetched": 0,
        "total_items_seen": 0,
        "in_since_window": 0,
        "in_last_n_days": 0,
        "discarded_below_since": 0,
        "stop_paging_triggered": False,
        "first_page_admission_dates": [],
        "sample_in_window": [],
        "sample_discarded": [],
        "order_hint": None,
    }

    offset = 0
    stop_paging = False

    while not stop_paging and stats["pages_fetched"] < 5:
        data = client.get(
            "/stocks/receptions.json",
            {"limit": LIMIT, "offset": offset, "expand": "[office]"},
        )
        items = data.get("items") or []
        stats["pages_fetched"] += 1
        if not items:
            break

        page_ts = [int(r.get("admissionDate") or 0) for r in items]
        if stats["pages_fetched"] == 1:
            stats["first_page_admission_dates"] = [
                {"id": r.get("id"), "admissionDate": _ts_fmt(int(r.get("admissionDate") or 0))}
                for r in items[:5]
            ]
            if len(page_ts) >= 2:
                if page_ts[0] > page_ts[-1]:
                    stats["order_hint"] = "DESC (más reciente primero)"
                elif page_ts[0] < page_ts[-1]:
                    stats["order_hint"] = "ASC (más antigua primero) — RIESGO con stop_paging"
                else:
                    stats["order_hint"] = "indeterminado"

        for rec in items:
            adm_ts = int(rec.get("admissionDate") or 0)
            stats["total_items_seen"] += 1

            if window_start <= adm_ts <= now_ts:
                stats["in_last_n_days"] += 1

            if adm_ts < since_ts:
                stats["discarded_below_since"] += 1
                if len(stats["sample_discarded"]) < 3:
                    stats["sample_discarded"].append(
                        {
                            "id": rec.get("id"),
                            "admissionDate": _ts_fmt(adm_ts),
                            "reason": f"admissionDate < since_ts ({_ts_fmt(since_ts)})",
                        }
                    )
                stop_paging = True
                stats["stop_paging_triggered"] = True
                continue

            stats["in_since_window"] += 1
            if len(stats["sample_in_window"]) < 5:
                stats["sample_in_window"].append(
                    {
                        "id": rec.get("id"),
                        "admissionDate": _ts_fmt(adm_ts),
                        "document": rec.get("document"),
                        "office": (rec.get("office") or {}).get("name"),
                    }
                )

        offset += LIMIT
        if len(items) < LIMIT:
            break
        if stop_paging:
            break

    return stats


def _history_counts(cur, company_id: int) -> dict:
    cur.execute(
        f"""
        SELECT
            COUNT(*)::int AS lines,
            COUNT(DISTINCT reception_id)::int AS receptions,
            MIN(admission_date) AS min_admission,
            MAX(admission_date) AS max_admission
        FROM {HISTORY}
        WHERE company_id = %s
        """,
        (company_id,),
    )
    row = cur.fetchone()
    cols = [d[0] for d in cur.description]
    d = dict(zip(cols, row))
    for k in ("min_admission", "max_admission"):
        if d.get(k):
            d[k] = d[k].isoformat()
    return d


def _dry_run_line_skips(
    cur,
    client: BsaleClient,
    *,
    company_id: int,
    since_ts: int,
    max_receptions: int = 3,
) -> list[dict]:
    """Muestra por qué líneas no se insertarían (sin escribir)."""
    reports: list[dict] = []
    offset = 0
    checked = 0

    while checked < max_receptions:
        data = client.get(
            "/stocks/receptions.json",
            {"limit": LIMIT, "offset": offset, "expand": "[office]"},
        )
        items = data.get("items") or []
        if not items:
            break

        for rec in items:
            adm_ts = int(rec.get("admissionDate") or 0)
            if adm_ts < since_ts:
                return reports
            rec_id = int(rec["id"])
            det = client.get(
                f"/stocks/receptions/{rec_id}/details.json",
                {"limit": 10, "offset": 0, "expand": "[variant]"},
            )
            det_items = det.get("items") or []
            line_stats = {
                "already_in_db": 0,
                "no_variant_id": 0,
                "would_insert": 0,
            }
            for line in det_items:
                detail_id = int(line["id"])
                if repo.line_exists(cur, company_id, detail_id):
                    line_stats["already_in_db"] += 1
                    continue
                variant_node = line.get("variant") or {}
                if not int(variant_node.get("id") or 0):
                    line_stats["no_variant_id"] += 1
                    continue
                line_stats["would_insert"] += 1

            reports.append(
                {
                    "reception_id": rec_id,
                    "admissionDate": _ts_fmt(adm_ts),
                    "detail_lines_sampled": len(det_items),
                    "line_stats": line_stats,
                }
            )
            checked += 1
            if checked >= max_receptions:
                break

        offset += LIMIT
        if len(items) < LIMIT:
            break

    return reports


def main() -> int:
    parser = argparse.ArgumentParser(description="Diagnóstico sync cost receptions")
    parser.add_argument("--company-id", type=int, default=None)
    parser.add_argument("--lookback-days", type=int, default=15)
    parser.add_argument("--window-days", type=int, default=15, help="Ventana Bsale a contar")
    args = parser.parse_args()

    print("=" * 72)
    print("DIAGNÓSTICO sync_cost_receptions")
    print(f"Fecha diagnóstico: {datetime.now(timezone.utc).isoformat()}")
    print("=" * 72)

    conn = get_connection()
    cur = conn.cursor()

    # 1. cost_sync_state global
    print("\n## 1. analytics.cost_sync_state (todas las empresas)")
    try:
        cur.execute(
            f"""
            SELECT company_id, last_admission_ts, last_run_at, last_status,
                   last_message, receptions_inserted, lines_inserted, total_lines_processed
            FROM {SYNC_STATE}
            ORDER BY company_id
            """
        )
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        if not rows:
            print("  (vacío — primera sync usaría lookback)")
        for row in rows:
            d = dict(zip(cols, row))
            d["last_admission_ts_human"] = _ts_fmt(d.get("last_admission_ts"))
            if d.get("last_run_at"):
                d["last_run_at"] = d["last_run_at"].isoformat()
            print(json.dumps(d, ensure_ascii=False, default=str, indent=2))
    except Exception as exc:
        print(f"  ERROR leyendo {SYNC_STATE}: {exc}")
        print("  ¿Se aplicó 038_cost_analytics_receptions.sql?")

    companies = _load_companies(cur, args.company_id)
    print(f"\n## Empresas activas: {len(companies)}")
    for co in companies:
        print(f"  - {co['company_id']} {co['name']} token_key={co['token_key']} ok={co['token_ok']}")

    for co in companies:
        cid = co["company_id"]
        if not co["token_ok"]:
            print(f"\n## Empresa {cid}: SKIP (token no en entorno)")
            continue

        print("\n" + "-" * 72)
        print(f"## Empresa {cid}: {co['name']}")
        print("-" * 72)

        state = repo.get_sync_state(cur, cid)
        print("\n### 2. Estado sync esta empresa")
        if state:
            state_print = dict(state)
            state_print["last_admission_ts_human"] = _ts_fmt(state.get("last_admission_ts"))
            if state.get("last_run_at"):
                state_print["last_run_at"] = state["last_run_at"].isoformat()
            print(json.dumps(state_print, ensure_ascii=False, default=str, indent=2))
        else:
            print("  (sin fila en cost_sync_state)")

        since_ts, since_reason = _resolve_since_ts(state, lookback_days=args.lookback_days)
        print(f"\n### 3. since_ts que usaría el sync AHORA")
        print(f"  since_ts = {since_ts}")
        print(f"  since_ts humano = {_ts_fmt(since_ts)}")
        print(f"  origen = {since_reason}")

        lookback_ignored = bool(state and state.get("last_admission_ts"))
        print(f"\n### 4. lookback_days={args.lookback_days} ¿aplicado?")
        if lookback_ignored:
            print(
                "  NO — existe last_admission_ts en cost_sync_state; "
                "lookback_days del request se IGNORA (solo overlap 24h)."
            )
        else:
            print(f"  SÍ — no hay watermark; se usaría lookback de {args.lookback_days} días.")

        hist = _history_counts(cur, cid)
        print(f"\n### 5. analytics.cost_reception_history (empresa {cid})")
        print(json.dumps(hist, ensure_ascii=False, indent=2))

        wm = state.get("last_admission_ts") if state else None
        hist_lines = hist.get("lines") or 0
        if wm and hist_lines == 0:
            print(
                "\n### ⚠ INCONSISTENCIA: last_admission_ts definido pero history vacío"
            )
            print(
                "  Causa conocida (código anterior): watermark avanzaba al examinar "
                "recepciones aunque lines_inserted=0."
            )
            print(
                "  Acción: NULL last_admission_ts en cost_sync_state y re-ejecutar sync "
                "con lookback_days, o deploy fix que no avanza watermark sin inserts."
            )

        client = BsaleClient(os.getenv(str(co["token_key"]).strip()))
        print(f"\n### 6. Muestra Bsale /stocks/receptions.json (ventana {args.window_days} días)")
        bsale_stats = _scan_bsale(
            client, since_ts=since_ts, window_days=args.window_days
        )
        print(json.dumps(bsale_stats, ensure_ascii=False, indent=2))

        print("\n### 7. Dry-run líneas (primeras recepciones en ventana)")
        try:
            dry = _dry_run_line_skips(cur, client, company_id=cid, since_ts=since_ts)
            print(json.dumps(dry, ensure_ascii=False, indent=2))
        except Exception as exc:
            print(f"  ERROR dry-run: {exc}")

    cur.close()
    conn.close()

    print("\n" + "=" * 72)
    print("FIN diagnóstico")
    print("=" * 72)
    return 0


if __name__ == "__main__":
    sys.exit(main())
