"""
Catchup controlado de relaciones documentales (OC↔factura / factura↔NC).

Por defecto: ``--dry-run`` (no escribe).

    python -m backend.jobs.sync_document_relations \\
      --company-id 3 --office-id 1 --recent-days 45 --dry-run

Apply (requiere confirmación explícita; NO ejecutar desde Cursor sin pedido):

    python -m backend.jobs.sync_document_relations ... --apply --i-understand-writes
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone

from backend.db import get_connection
from backend.services.distribuidora.document_relation_sync_service import (
    run_relation_sync_audit,
)
from backend.utils.bsale_token_env import load_dotenv_if_available


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Sync/audit document relations (dry-run default)")
    p.add_argument("--company-id", type=int, default=3)
    p.add_argument("--office-id", type=int, default=1)
    p.add_argument("--recent-days", type=int, default=45)
    p.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="No escribe (default).",
    )
    p.add_argument(
        "--apply",
        action="store_true",
        help="Materializa aristas NC en document_related (requiere --i-understand-writes).",
    )
    p.add_argument(
        "--i-understand-writes",
        action="store_true",
        help="Confirmación explícita para --apply.",
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
    # --dry-run explícito gana si ambos (seguridad)
    if args.dry_run and not args.apply:
        dry_run = True

    started = datetime.now(timezone.utc)
    print(
        f"[sync_document_relations] start utc={started.isoformat()} "
        f"company={args.company_id} office={args.office_id} "
        f"days={args.recent_days} dry_run={dry_run}",
        flush=True,
    )

    conn = get_connection()
    try:
        cur = conn.cursor()
        report = run_relation_sync_audit(
            cur,
            company_id=args.company_id,
            office_id=args.office_id,
            recent_days=args.recent_days,
            dry_run=dry_run,
        )
        if not dry_run:
            conn.commit()
        else:
            conn.rollback()
        cur.close()
    except Exception as exc:
        print(f"[sync_document_relations] ERROR: {exc}", file=sys.stderr, flush=True)
        try:
            conn.rollback()
        except Exception:
            pass
        return 1
    finally:
        conn.close()

    payload = report.as_dict()
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str), flush=True)
    print(
        "[sync_document_relations] summary "
        f"scanned={report.documents_scanned} "
        f"cn_new={report.credit_note_links_new} "
        f"cn_existing={report.credit_note_links_existing} "
        f"unresolved={report.unresolved} "
        f"errors={len(report.errors)} dry_run={report.dry_run}",
        flush=True,
    )
    return 1 if report.errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
