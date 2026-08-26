"""
Recuperación puntual de headers referenciados en ``document_related`` sin fila en ``documents``.

Dry-run (default) — consulta Bsale por ``related_document_id``, NO escribe PG::

    python -m backend.jobs.sync_missing_related_documents \\
      --company-id 3 \\
      --office-id 1 \\
      --dry-run

Filtrar un id concreto (canario)::

    python -m backend.jobs.sync_missing_related_documents \\
      --company-id 3 \\
      --office-id 1 \\
      --related-id 3853417 \\
      --dry-run

Apply (requiere confirmación doble; NO ejecutar desde Cursor sin autorización)::

    python -m backend.jobs.sync_missing_related_documents \\
      --company-id 3 \\
      --office-id 1 \\
      --apply \\
      --i-understand-writes
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone

from backend.services.distribuidora.sync_missing_related_documents_service import (
    run_sync_missing_related_documents,
)
from backend.utils.bsale_token_env import load_dotenv_if_available


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Sync headers faltantes para related huérfanos (fuente: GET /documents/{id}.json)"
        ),
    )
    p.add_argument("--company-id", type=int, default=3)
    p.add_argument("--office-id", type=int, default=1)
    p.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="Consulta Bsale y reporta; no escribe (default).",
    )
    p.add_argument(
        "--apply",
        action="store_true",
        help="Upsert headers verificados (requiere --i-understand-writes).",
    )
    p.add_argument(
        "--i-understand-writes",
        action="store_true",
        help="Confirmación explícita para --apply.",
    )
    p.add_argument(
        "--limit",
        type=int,
        default=500,
        help="Máximo de candidatos huérfanos por ejecución (paginación).",
    )
    p.add_argument(
        "--offset",
        type=int,
        default=0,
        help="Offset para paginación de candidatos.",
    )
    p.add_argument(
        "--related-id",
        type=int,
        action="append",
        dest="related_ids",
        help="Limitar a uno o más related_document_id (repetible).",
    )
    p.add_argument(
        "--throttle-sec",
        type=float,
        default=None,
        help="Pausa extra entre paginaciones relateddetailid (opcional).",
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

    started = datetime.now(timezone.utc)
    print(
        f"[sync_missing_related_documents] start utc={started.isoformat()} "
        f"company={args.company_id} office={args.office_id} "
        f"dry_run={dry_run} limit={args.limit} offset={args.offset} "
        f"related_ids={args.related_ids or 'all'}",
        flush=True,
    )

    try:
        report = run_sync_missing_related_documents(
            company_id=args.company_id,
            office_id=args.office_id,
            dry_run=dry_run,
            limit=args.limit,
            offset=args.offset,
            related_document_ids=args.related_ids,
            throttle=args.throttle_sec,
        )
    except Exception as exc:
        print(f"[sync_missing_related_documents] ERROR: {exc}", file=sys.stderr, flush=True)
        return 1

    payload = {
        "dry_run": report.dry_run,
        "candidates": report.candidates,
        "found_in_bsale": report.found_in_bsale,
        "would_insert": report.would_insert,
        "already_present": report.already_present,
        "not_found": report.not_found,
        "api_errors": report.api_errors,
        "rate_limited": report.rate_limited,
        "samples": report.samples,
        "headers_inserted": report.headers_inserted,
        "cn_links_would_materialize": report.cn_links_would_materialize,
        "cn_links_inserted": report.cn_links_inserted,
        "derived_nc_candidates": report.derived_nc_candidates,
        "rate_stats": report.rate_stats,
        "errors": report.errors,
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str), flush=True)
    print(
        "[sync_missing_related_documents] summary "
        f"dry_run={report.dry_run} candidates={report.candidates} "
        f"found={report.found_in_bsale} would_insert={report.would_insert} "
        f"already_present={report.already_present} not_found={report.not_found} "
        f"api_errors={report.api_errors} rate_limited={report.rate_limited}",
        flush=True,
    )
    return 1 if report.errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
