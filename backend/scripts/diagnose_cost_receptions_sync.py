#!/usr/bin/env python3
"""
Diagnóstico sync_cost_receptions — NO modifica datos.
Nunca escribe en DB ni consulta/actualiza variant costs.

Uso:
  python -m backend.scripts.diagnose_cost_receptions_sync \\
    --company-id 3 --lookback-days 45 --window-days 45

  python -m backend.scripts.diagnose_cost_receptions_sync \\
    --company-id 3 --lookback-days 45 --window-days 45 \\
    --scan-all-details --max-receptions 5000
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

from backend.db import get_connection
from backend.repositories import cost_analytics_repo as repo
from backend.services.cost_receptions_fetch import LIST_LIMIT, iter_receptions_for_sync
from backend.services.distribuidora.bsale_client import BsaleClient
from backend.services.sync_cost_receptions import INITIAL_LOOKBACK_DAYS

HISTORY = "analytics.cost_reception_history"
SYNC_STATE = "analytics.cost_sync_state"

DEFAULT_DETAIL_SAMPLE_LIMIT = 100
MAX_DETAIL_SAMPLE_LIMIT = 500
DEFAULT_MAX_RECEPTIONS = 5000
DETAIL_PAGE_LIMIT = 50


def clamp_detail_sample_limit(value: int | None) -> int:
    raw = DEFAULT_DETAIL_SAMPLE_LIMIT if value is None else int(value)
    if raw < 1:
        raise ValueError("--detail-sample-limit mínimo es 1")
    if raw > MAX_DETAIL_SAMPLE_LIMIT:
        raise ValueError(
            f"--detail-sample-limit máximo es {MAX_DETAIL_SAMPLE_LIMIT}"
        )
    return raw


def clamp_max_receptions(value: int | None) -> int:
    raw = DEFAULT_MAX_RECEPTIONS if value is None else int(value)
    if raw < 1:
        raise ValueError("--max-receptions mínimo es 1")
    return raw


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


def fetch_receptions_window(
    client: BsaleClient,
    *,
    since_ts: int,
    window_days: int,
    until_ts: int | None = None,
) -> tuple[list[dict], dict]:
    """Misma estrategia de listado que el sync; no pide detalles ni costs."""
    now_ts = until_ts or int(datetime.now(timezone.utc).timestamp())
    window_start = int(
        (datetime.now(timezone.utc) - timedelta(days=window_days)).timestamp()
    )
    effective_since = max(since_ts, window_start)
    receptions, meta = iter_receptions_for_sync(
        client, since_ts=effective_since, until_ts=now_ts, limit=LIST_LIMIT
    )
    receptions = sorted(
        receptions,
        key=lambda r: (int(r.get("admissionDate") or 0), int(r.get("id") or 0)),
    )
    return receptions, meta


def _iter_reception_details(
    client: BsaleClient,
    reception_id: int,
) -> tuple[list[dict], list[str]]:
    """Página todos los detalles de una recepción. Nunca llama /costs.json."""
    items: list[dict] = []
    failures: list[str] = []
    offset = 0
    while True:
        try:
            det = client.get(
                f"/stocks/receptions/{reception_id}/details.json",
                {
                    "limit": DETAIL_PAGE_LIMIT,
                    "offset": offset,
                    "expand": "[variant]",
                },
            )
        except Exception as exc:
            failures.append(f"reception_id={reception_id} details offset={offset}: {exc}")
            break
        page = det.get("items") or []
        if not page:
            break
        items.extend(page)
        offset += DETAIL_PAGE_LIMIT
        if len(page) < DETAIL_PAGE_LIMIT:
            break
    return items, failures


def classify_detail_lines(
    cur,
    *,
    company_id: int,
    detail_items: list[dict],
) -> dict[str, Any]:
    already = 0
    would_insert = 0
    no_variant = 0
    detail_ids: list[int] = []
    for line in detail_items:
        detail_id = int(line.get("id") or 0)
        if not detail_id:
            continue
        detail_ids.append(detail_id)
        if repo.line_exists(cur, company_id, detail_id):
            already += 1
            continue
        variant_node = line.get("variant") or {}
        if not int(variant_node.get("id") or 0):
            no_variant += 1
            continue
        would_insert += 1
    id_counts = Counter(detail_ids)
    duplicate_detail_ids = sorted(i for i, n in id_counts.items() if n > 1)
    return {
        "details_fetched": len(detail_items),
        "unique_detail_ids": len(id_counts),
        "already_in_history": already,
        "would_insert": would_insert,
        "no_variant_id": no_variant,
        "duplicate_detail_ids": duplicate_detail_ids,
        "detail_ids": detail_ids,
    }


def analyze_details_for_receptions(
    cur,
    client: BsaleClient,
    *,
    company_id: int,
    receptions: list[dict],
    progress_every: int = 50,
) -> dict[str, Any]:
    """
    Inspecciona detalles de las recepciones dadas (read-only).
    No escribe DB, no consulta variant costs.
    """
    office_breakdown: dict[str, dict[str, int]] = defaultdict(
        lambda: {
            "receptions": 0,
            "details_fetched": 0,
            "already_in_history": 0,
            "would_insert": 0,
        }
    )
    date_breakdown: dict[str, dict[str, int]] = defaultdict(
        lambda: {
            "receptions": 0,
            "details_fetched": 0,
            "already_in_history": 0,
            "would_insert": 0,
        }
    )

    total_details = 0
    already = 0
    would_insert = 0
    no_variant = 0
    failed_reception_details: list[str] = []
    all_detail_ids: list[int] = []
    duplicate_in_api: list[dict[str, Any]] = []
    potential_collisions: list[dict[str, Any]] = []
    min_adm: int | None = None
    max_adm: int | None = None

    for idx, rec in enumerate(receptions, start=1):
        adm_ts = int(rec.get("admissionDate") or 0)
        if adm_ts:
            min_adm = adm_ts if min_adm is None else min(min_adm, adm_ts)
            max_adm = adm_ts if max_adm is None else max(max_adm, adm_ts)
        office = rec.get("office") or {}
        office_key = str(office.get("id") or "null")
        day_key = (
            datetime.fromtimestamp(adm_ts, tz=timezone.utc).strftime("%Y-%m-%d")
            if adm_ts
            else "unknown"
        )
        office_breakdown[office_key]["receptions"] += 1
        date_breakdown[day_key]["receptions"] += 1

        rec_id = int(rec.get("id") or 0)
        if not rec_id:
            failed_reception_details.append("reception sin id")
            continue

        items, failures = _iter_reception_details(client, rec_id)
        failed_reception_details.extend(failures)
        classified = classify_detail_lines(
            cur, company_id=company_id, detail_items=items
        )
        total_details += classified["details_fetched"]
        already += classified["already_in_history"]
        would_insert += classified["would_insert"]
        no_variant += classified["no_variant_id"]
        all_detail_ids.extend(classified["detail_ids"])

        office_breakdown[office_key]["details_fetched"] += classified["details_fetched"]
        office_breakdown[office_key]["already_in_history"] += classified[
            "already_in_history"
        ]
        office_breakdown[office_key]["would_insert"] += classified["would_insert"]
        date_breakdown[day_key]["details_fetched"] += classified["details_fetched"]
        date_breakdown[day_key]["already_in_history"] += classified["already_in_history"]
        date_breakdown[day_key]["would_insert"] += classified["would_insert"]

        if classified["duplicate_detail_ids"]:
            duplicate_in_api.append(
                {
                    "reception_id": rec_id,
                    "duplicate_detail_ids": classified["duplicate_detail_ids"],
                }
            )

        if progress_every and idx % progress_every == 0:
            print(
                f"  … progreso detalles {idx}/{len(receptions)} "
                f"details={total_details} would_insert={would_insert}",
                flush=True,
            )

    id_counts = Counter(all_detail_ids)
    for detail_id, n in sorted(id_counts.items()):
        if n > 1:
            potential_collisions.append(
                {
                    "company_id": company_id,
                    "reception_detail_id": detail_id,
                    "occurrences_in_api_sample": n,
                    "unique_key": f"{company_id}_{detail_id}",
                }
            )

    return {
        "receptions_sampled_for_details": len(receptions),
        "details_fetched": total_details,
        "unique_detail_ids": len(id_counts),
        "already_in_history": already,
        "would_insert": would_insert,
        "no_variant_id": no_variant,
        "failed_reception_details": failed_reception_details,
        "duplicate_detail_ids_in_api_sample": duplicate_in_api,
        "potential_company_detail_collisions": potential_collisions,
        "min_admission_date": _ts_fmt(min_adm),
        "max_admission_date": _ts_fmt(max_adm),
        "by_office_id": dict(sorted(office_breakdown.items())),
        "by_admission_date": dict(sorted(date_breakdown.items())),
        "note": (
            "Conteos de details corresponden SOLO a recepciones inspeccionadas. "
            "No extrapolar faltantes totales desde una muestra parcial."
        ),
    }


def build_detail_report(
    cur,
    client: BsaleClient,
    *,
    company_id: int,
    receptions: list[dict],
    detail_sample_limit: int,
    scan_all_details: bool,
    max_receptions: int,
) -> dict[str, Any]:
    capped = receptions[:max_receptions]
    truncated = len(receptions) > max_receptions
    if scan_all_details:
        target = capped
        mode = "scan_all_details"
    else:
        target = capped[:detail_sample_limit]
        mode = "sample"

    analysis = analyze_details_for_receptions(
        cur,
        client,
        company_id=company_id,
        receptions=target,
    )
    out: dict[str, Any] = {
        "mode": mode,
        "receptions_fetched_total": len(receptions),
        "receptions_after_max_receptions_cap": len(capped),
        "max_receptions_cap": max_receptions,
        "truncated_by_max_receptions": truncated,
        "detail_sample_limit": detail_sample_limit if mode == "sample" else None,
        "receptions_sampled_for_details": analysis["receptions_sampled_for_details"],
        "no_variant_id": analysis["no_variant_id"],
        "unique_detail_ids": analysis["unique_detail_ids"],
        "failed_reception_details": analysis["failed_reception_details"],
        "duplicate_detail_ids_in_api_sample": analysis[
            "duplicate_detail_ids_in_api_sample"
        ],
        "potential_company_detail_collisions": analysis[
            "potential_company_detail_collisions"
        ],
        "min_admission_date": analysis["min_admission_date"],
        "max_admission_date": analysis["max_admission_date"],
        "by_office_id": analysis["by_office_id"],
        "by_admission_date": analysis["by_admission_date"],
        "note": analysis["note"],
        "extrapolation_warning": (
            None
            if mode == "scan_all_details" and not truncated
            else (
                "SAMPLE/CAP: no extrapolar total de líneas faltantes "
                "desde esta muestra."
            )
        ),
    }
    if mode == "sample":
        out["details_fetched_sample"] = analysis["details_fetched"]
        out["details_already_in_history_sample"] = analysis["already_in_history"]
        out["details_would_insert_sample"] = analysis["would_insert"]
    else:
        out["details_fetched_total"] = analysis["details_fetched"]
        out["already_in_history"] = analysis["already_in_history"]
        out["would_insert"] = analysis["would_insert"]
        out["missing_reception_details"] = analysis["would_insert"]
        out["failures"] = analysis["failed_reception_details"]
    return out


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Diagnóstico sync cost receptions")
    parser.add_argument("--company-id", type=int, default=None)
    parser.add_argument("--lookback-days", type=int, default=15)
    parser.add_argument(
        "--window-days", type=int, default=15, help="Ventana Bsale a contar"
    )
    parser.add_argument(
        "--detail-sample-limit",
        type=int,
        default=DEFAULT_DETAIL_SAMPLE_LIMIT,
        help=f"Recepciones a inspeccionar en muestra (default {DEFAULT_DETAIL_SAMPLE_LIMIT}, max {MAX_DETAIL_SAMPLE_LIMIT})",
    )
    parser.add_argument(
        "--scan-all-details",
        action="store_true",
        help="Consultar detalles de todas las recepciones de la ventana (read-only)",
    )
    parser.add_argument(
        "--max-receptions",
        type=int,
        default=DEFAULT_MAX_RECEPTIONS,
        help=f"Tope de recepciones a inspeccionar (default {DEFAULT_MAX_RECEPTIONS})",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    try:
        detail_sample_limit = clamp_detail_sample_limit(args.detail_sample_limit)
        max_receptions = clamp_max_receptions(args.max_receptions)
    except ValueError as exc:
        print(f"ERROR: {exc}")
        return 2

    print("=" * 72)
    print("DIAGNÓSTICO sync_cost_receptions (READ-ONLY)")
    print(f"Fecha diagnóstico: {datetime.now(timezone.utc).isoformat()}")
    print(f"detail_sample_limit={detail_sample_limit}")
    print(f"scan_all_details={bool(args.scan_all_details)}")
    print(f"max_receptions={max_receptions}")
    print("=" * 72)

    conn = get_connection()
    cur = conn.cursor()

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

        client = BsaleClient(os.getenv(str(co["token_key"]).strip()))
        print(f"\n### 6. Listado Bsale /stocks/receptions.json (ventana {args.window_days} días)")
        receptions, bsale_meta = fetch_receptions_window(
            client, since_ts=since_ts, window_days=args.window_days
        )
        list_summary = {
            "strategy": bsale_meta.get("strategy"),
            "pages_fetched": bsale_meta.get("pages_read"),
            "api_total_count": bsale_meta.get("api_total_count"),
            "order_hint": bsale_meta.get("page_order_hint"),
            "min_admission": _ts_fmt(bsale_meta.get("min_admission_ts")),
            "max_admission": _ts_fmt(bsale_meta.get("max_admission_ts")),
            "in_since_window": bsale_meta.get("receptions_in_window"),
            "receptions_fetched_total": len(receptions),
            "sample_first_5": [
                {
                    "id": r.get("id"),
                    "admissionDate": _ts_fmt(int(r.get("admissionDate") or 0)),
                    "document": r.get("document"),
                    "office": (r.get("office") or {}).get("name"),
                    "office_id": (r.get("office") or {}).get("id"),
                }
                for r in receptions[:5]
            ],
        }
        print(json.dumps(list_summary, ensure_ascii=False, indent=2))

        print("\n### 7. Inspección de detalles (READ-ONLY, sin variant_cost)")
        try:
            detail_report = build_detail_report(
                cur,
                client,
                company_id=cid,
                receptions=receptions,
                detail_sample_limit=detail_sample_limit,
                scan_all_details=bool(args.scan_all_details),
                max_receptions=max_receptions,
            )
            print(json.dumps(detail_report, ensure_ascii=False, indent=2))
        except Exception as exc:
            print(f"  ERROR inspección detalles: {exc}")

    cur.close()
    conn.close()

    print("\n" + "=" * 72)
    print("FIN diagnóstico (sin escrituras)")
    print("=" * 72)
    return 0


if __name__ == "__main__":
    sys.exit(main())
