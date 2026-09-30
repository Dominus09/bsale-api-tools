"""
Cron Coolify (cada 5 min): sync live documentos Bsale → distribuidora.documents.

    python -m backend.jobs.live_sync_documents

Cron sugerido: ``*/5 * * * *``

Canario por folio OC (read-only: GET Bsale + SELECT; no escribe)::

    python -m backend.jobs.live_sync_documents --company-id 3 --office-id 1 \\
      --oc-number 69882 --dry-run

Reparación de un folio (pipeline normal ``reconcile_one_oc``; NO ejecutar sin autorización)::

    python -m backend.jobs.live_sync_documents --company-id 3 --office-id 1 \\
      --oc-number 69882 --apply --i-understand-writes

Reconciliación liviana de OCs recientes (dry-run default; cron sugerido ``*/30 * * * *``
con ``--apply --i-understand-writes``)::

    python -m backend.jobs.live_sync_documents --company-id 3 --office-id 1 \\
      --reconcile-recent --days 3 --dry-run
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from typing import Any

from backend.services.distribuidora.live_sync_service import (
    _print_summary,
    live_sync_documents,
)
from backend.utils.bsale_token_env import load_dotenv_if_available

_LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"


def _configure_logging() -> None:
    level = getattr(logging, os.getenv("LOG_LEVEL", "INFO").strip().upper(), logging.INFO)
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(level)
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(logging.Formatter(_LOG_FORMAT))
    root.addHandler(h)


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Live sync documentos Bsale + canario OC")
    p.add_argument("--company-id", type=int, default=3)
    p.add_argument("--office-id", type=int, default=1)
    p.add_argument("--oc-number", type=int, default=None, help="Canario/reparación por folio OC")
    p.add_argument(
        "--reconcile-recent",
        action="store_true",
        help="Reconciliar OCs con emisión en los últimos --days días completos (Bsale vs PostgreSQL)",
    )
    p.add_argument("--days", type=int, default=3)
    p.add_argument("--max-pages", type=int, default=60)
    p.add_argument("--max-repairs", type=int, default=25)
    p.add_argument("--dry-run", action="store_true", help="Read-only (default en canario)")
    p.add_argument("--apply", action="store_true", help="Escribe (requiere --i-understand-writes)")
    p.add_argument("--i-understand-writes", action="store_true")
    return p.parse_args(argv)


def _wants_writes(args: argparse.Namespace) -> bool:
    if args.apply and not args.i_understand_writes:
        raise SystemExit("--apply requiere --i-understand-writes")
    return bool(args.apply and args.i_understand_writes and not args.dry_run)


def _readonly_connection():
    from backend.db import get_connection

    conn = get_connection()
    conn.set_session(readonly=True, autocommit=False)
    return conn


def _run_canary(args: argparse.Namespace, token: str) -> dict[str, Any]:
    from backend.services.distribuidora.bsale_client import BsaleClient
    from backend.services.distribuidora.recent_documents_reconcile_service import (
        load_local_oc_matches,
        run_oc_folio_canary,
    )

    conn = _readonly_connection()
    try:
        cur = conn.cursor()

        def _loader(folio: int, ids: list[int]) -> list[dict[str, Any]]:
            return load_local_oc_matches(cur, folio=folio, bsale_ids=ids)

        report = run_oc_folio_canary(
            BsaleClient(token),
            folio=int(args.oc_number),
            company_id=int(args.company_id),
            office_id=int(args.office_id),
            local_loader=_loader,
        )
        conn.rollback()
        return report
    finally:
        conn.close()


def _run_repair(args: argparse.Namespace, token: str) -> dict[str, Any]:
    from backend.services.distribuidora.bsale_client import BsaleClient
    from backend.services.distribuidora.oc_reconciliation_service import reconcile_one_oc

    before = _run_canary(args, token)
    if before.get("found_locally"):
        return {"status": "already_present", "canary": before, "wrote": False}
    if not before.get("found_in_bsale") or not (before.get("diagnosis") or {}).get(
        "bsale_source_document_id"
    ):
        return {"status": "not_repairable", "canary": before, "wrote": False}
    result = reconcile_one_oc(
        BsaleClient(token),
        folio=int(args.oc_number),
        dry_run=False,
        company_id=int(args.company_id),
        office_id=int(args.office_id),
    )
    after = _run_canary(args, token)
    return {
        "status": result.get("status"),
        "wrote": result.get("wrote"),
        "local_document_id": result.get("local_document_id"),
        "details_replaced": result.get("details_replaced"),
        "related_rows_inserted": result.get("related_rows_inserted"),
        "found_locally_after": after.get("found_locally"),
        "canary_before": {k: v for k, v in before.items() if k != "diagnosis"},
    }


def _run_reconcile_recent(args: argparse.Namespace, token: str, *, apply: bool) -> dict[str, Any]:
    from backend.services.distribuidora.bsale_client import BsaleClient
    from backend.services.distribuidora.oc_reconciliation_service import reconcile_one_oc
    from backend.services.distribuidora.recent_documents_reconcile_service import (
        load_local_oc_folios_db,
        reconcile_recent_oc_documents,
    )

    client = BsaleClient(token)
    conn = _readonly_connection()
    try:
        cur = conn.cursor()

        def _load(folios):
            out = load_local_oc_folios_db(
                cur, company_id=args.company_id, office_id=args.office_id, folios=folios
            )
            conn.rollback()
            return out

        def _repair(folio: int, active: dict[str, Any]) -> dict[str, Any]:
            return reconcile_one_oc(
                client,
                folio=folio,
                dry_run=False,
                active_document=active,
                company_id=int(args.company_id),
                office_id=int(args.office_id),
            )

        return reconcile_recent_oc_documents(
            client,
            company_id=int(args.company_id),
            office_id=int(args.office_id),
            days=int(args.days),
            max_pages=int(args.max_pages),
            max_repairs=int(args.max_repairs),
            apply=apply,
            load_local_folios=_load,
            repair_one=_repair if apply else None,
        )
    finally:
        conn.close()


def main(argv: list[str] | None = None) -> int:
    load_dotenv_if_available()
    _configure_logging()
    args = _parse_args(argv)

    if args.oc_number is not None or args.reconcile_recent:
        from backend.utils.bsale_token_env import require_bsale_token

        writes = _wants_writes(args)
        token = require_bsale_token(label="live_sync_documents")
        try:
            if args.oc_number is not None:
                out = _run_repair(args, token) if writes else _run_canary(args, token)
            else:
                out = _run_reconcile_recent(args, token, apply=writes)
        except Exception as e:
            logging.getLogger(__name__).exception("live_sync_documents canary/reconcile")
            print(f"[live_sync_documents] ERROR: {e}", file=sys.stderr, flush=True)
            return 1
        print(json.dumps(out, ensure_ascii=False, indent=2, default=str), flush=True)
        return 1 if int(out.get("errors") or 0) > 0 else 0

    print("[live_sync_documents] INICIO", flush=True)
    try:
        stats = live_sync_documents(strict_token=True)
    except ValueError as e:
        print(f"[live_sync_documents] ERROR: {e}", file=sys.stderr, flush=True)
        return 1
    except Exception as e:
        logging.getLogger(__name__).exception("live_sync_documents")
        print(f"[live_sync_documents] ERROR: {e}", file=sys.stderr, flush=True)
        return 1

    _print_summary("LIVE SYNC DOCUMENTS — SUMMARY", stats)
    if stats.get("omitido_concurrencia"):
        return int(os.getenv("LIVE_SYNC_EXIT_CODE_ON_LOCK", "0"))
    if stats.get("skipped") or stats.get("errors"):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
