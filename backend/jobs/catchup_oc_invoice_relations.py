"""
Catchup histórico OC → boleta/factura con dry-run real (default).

Por rango (default histórico)::

    python -m backend.jobs.catchup_oc_invoice_relations \\
      --company-id 3 \\
      --office-id 1 \\
      --start-date 2026-07-11 \\
      --end-date 2026-08-25 \\
      --dry-run

Por folio exacto (canario; ignora emission_date)::

    python -m backend.jobs.catchup_oc_invoice_relations \\
      --company-id 3 \\
      --office-id 1 \\
      --oc-number 69087 \\
      --dry-run

Apply (requiere confirmación doble; NO ejecutar desde Cursor sin autorización)::

    python -m backend.jobs.catchup_oc_invoice_relations \\
      --company-id 3 \\
      --office-id 1 \\
      --start-date 2026-07-11 \\
      --end-date 2026-08-25 \\
      --apply \\
      --i-understand-writes
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timezone

from backend.services.distribuidora.catchup_oc_invoice_relations_service import (
    run_catchup_oc_invoice_relations,
    run_catchup_oc_invoice_relations_by_oc_number,
)
from backend.utils.bsale_token_env import load_dotenv_if_available


def _parse_date(value: str) -> date:
    return date.fromisoformat(value.strip())


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Catchup OC→boleta/factura vía Bsale relateddetailid (dry-run default)",
    )
    p.add_argument("--company-id", type=int, default=3)
    p.add_argument("--office-id", type=int, default=1)
    p.add_argument(
        "--oc-number",
        type=int,
        default=None,
        help="Canario: folio OC exacto (salta selección por emission_date).",
    )
    p.add_argument(
        "--start-date",
        type=_parse_date,
        default=None,
        help="Inicio inclusive UTC (requerido si no hay --oc-number).",
    )
    p.add_argument(
        "--end-date",
        type=_parse_date,
        default=None,
        help="Fin inclusive UTC (requerido si no hay --oc-number).",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="Consulta Bsale y reporta; no escribe (default).",
    )
    p.add_argument(
        "--apply",
        action="store_true",
        help="Inserta relaciones verificadas (requiere --i-understand-writes).",
    )
    p.add_argument(
        "--i-understand-writes",
        action="store_true",
        help="Confirmación explícita para --apply.",
    )
    p.add_argument(
        "--throttle-sec",
        type=float,
        default=None,
        help="Pausa entre llamadas Bsale (default: DISTRIBUIDORA_RELATED_API_DELAY_SEC).",
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    load_dotenv_if_available()
    args = _parse_args(argv)

    dry_run = True
    if args.apply:
        if not args.i_understand_writes:
            print(
                "ERROR: --apply requiere --i-understand-writes. Abortado.",
                file=sys.stderr,
            )
            return 2
        dry_run = False
    elif args.dry_run:
        dry_run = True

    if args.oc_number is None:
        if args.start_date is None or args.end_date is None:
            print(
                "ERROR: indique --oc-number o bien --start-date y --end-date.",
                file=sys.stderr,
            )
            return 2

    started = datetime.now(timezone.utc)
    if args.oc_number is not None:
        print(
            f"[catchup_oc_invoice_relations] start utc={started.isoformat()} "
            f"company={args.company_id} office={args.office_id} "
            f"mode=by_oc_number oc_number={args.oc_number} dry_run={dry_run}",
            flush=True,
        )
        try:
            report = run_catchup_oc_invoice_relations_by_oc_number(
                oc_number=int(args.oc_number),
                company_id=args.company_id,
                office_id=args.office_id,
                dry_run=dry_run,
                throttle=args.throttle_sec,
            )
        except Exception as exc:
            print(
                f"[catchup_oc_invoice_relations] ERROR: {exc}",
                file=sys.stderr,
                flush=True,
            )
            return 1
    else:
        print(
            f"[catchup_oc_invoice_relations] start utc={started.isoformat()} "
            f"company={args.company_id} office={args.office_id} "
            f"mode=by_range "
            f"range={args.start_date.isoformat()}..{args.end_date.isoformat()} "
            f"dry_run={dry_run}",
            flush=True,
        )
        try:
            report = run_catchup_oc_invoice_relations(
                start_date=args.start_date,
                end_date=args.end_date,
                company_id=args.company_id,
                office_id=args.office_id,
                dry_run=dry_run,
                throttle=args.throttle_sec,
            )
        except Exception as exc:
            print(
                f"[catchup_oc_invoice_relations] ERROR: {exc}",
                file=sys.stderr,
                flush=True,
            )
            return 1

    payload = report.as_dict()
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str), flush=True)
    if report.mode == "by_oc_number" and report.canary:
        c = report.canary
        print(
            "[catchup_oc_invoice_relations] canary_summary "
            f"oc_number={c.get('oc_number')} "
            f"oc_document_id={c.get('oc_document_id')} "
            f"emission_date={c.get('emission_date')} "
            f"generation_date={c.get('generation_date')} "
            f"state={c.get('state')} "
            f"details_scanned={c.get('details_scanned')} "
            f"confirmed_before={c.get('confirmed_before')} "
            f"status={c.get('status')} "
            f"would_confirm={c.get('would_confirm')} "
            f"related_number={c.get('related_number')} "
            f"related_type={c.get('related_type')} "
            f"related_generation_date={c.get('related_generation_date')} "
            f"unique_docs_count={c.get('unique_docs_count')} "
            f"edges_count={c.get('edges_count')}",
            flush=True,
        )
    print(
        "[catchup_oc_invoice_relations] summary "
        f"mode={report.mode} dry_run={report.dry_run} oc_scanned={report.oc_scanned} "
        f"ocs_completed={report.ocs_completed} "
        f"ocs_with_relation={report.ocs_with_relation} "
        f"ocs_without_relation={report.ocs_without_relation} "
        f"ocs_rate_limited={report.ocs_rate_limited} "
        f"requests_total={report.requests_total} "
        f"rate_limit_events={report.rate_limit_events} "
        f"retry_count={report.retry_count} "
        f"wait_seconds_total={report.wait_seconds_total:.1f} "
        f"would_insert_inv={report.invoice_links_would_insert} "
        f"would_insert_boleta={report.receipt_links_would_insert} "
        f"existing={report.relations_existing} api_errors={report.api_errors} "
        f"plan_ocs={len(report.plan_oc_results)}",
        flush=True,
    )
    return 1 if report.errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
