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

Rango de folios OC (mismo canario/reparación que ``--oc-number``, folio por folio)::

    python -m backend.jobs.live_sync_documents --company-id 3 --office-id 1 \\
      --oc-from 69906 --oc-to 69924 --dry-run

Reconciliación de OCs recientes (herramienta manual, no cron; dry-run default)::

    python -m backend.jobs.live_sync_documents --company-id 3 --office-id 1 \\
      --reconcile-recent --days 3 --dry-run
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from typing import Any, Callable

from backend.services.distribuidora.live_sync_service import (
    _print_summary,
    live_sync_documents,
)
from backend.utils.bsale_token_env import load_dotenv_if_available

_LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"
MAX_OC_RANGE = 200


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
    p.add_argument("--oc-from", type=int, default=None, help="Rango de folios OC (inclusive)")
    p.add_argument("--oc-to", type=int, default=None, help="Rango de folios OC (inclusive)")
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


def _canary_eligible(report: dict[str, Any], *, office_id: int) -> bool:
    return bool(
        report.get("found_in_bsale")
        and report.get("active_source_selected")
        and report.get("document_type_id") == 33
        and report.get("bsale_office_id") == int(office_id)
    )


def run_oc_range(
    folios: list[int],
    *,
    office_id: int,
    canary: Callable[[int], dict[str, Any]],
    repair: Callable[[int], dict[str, Any]] | None,
    apply: bool,
    max_repairs: int,
) -> dict[str, Any]:
    """
    Folio por folio con el canario de ``--oc-number``; con ``apply`` repara solo los
    elegibles ausentes vía la reparación puntual. Idempotente: lo ya local queda en
    ``already_exists``.
    """
    items: list[dict[str, Any]] = []
    summary = {
        "folios_requested": len(folios),
        "found_in_bsale": 0,
        "already_local": 0,
        "would_repair": 0,
        "repaired": 0,
        "not_found": 0,
        "not_eligible": 0,
        "skipped": 0,
        "errors": 0,
    }
    attempts = 0
    for folio in folios:
        item: dict[str, Any] = {"folio": folio}
        try:
            rep = canary(folio)
        except Exception as exc:
            summary["errors"] += 1
            items.append({**item, "action": "error", "error": str(exc)[:500]})
            continue
        found = bool(rep.get("found_in_bsale"))
        local = bool(rep.get("found_locally"))
        eligible = _canary_eligible(rep, office_id=office_id)
        item.update(
            {
                "found_in_bsale": found,
                "found_locally": local,
                "eligible": eligible,
                "bsale_document_id": rep.get("bsale_document_id"),
                "bsale_office_id": rep.get("bsale_office_id"),
                "state": rep.get("state"),
                "total": rep.get("total"),
                "local_document_id": rep.get("local_document_id"),
                "primary_cause": (rep.get("diagnosis") or {}).get("primary_cause"),
            }
        )
        if found:
            summary["found_in_bsale"] += 1
        if local:
            summary["already_local"] += 1
            item["action"] = "already_exists"
        elif not found:
            summary["not_found"] += 1
            item["action"] = "not_found"
        elif not eligible:
            summary["not_eligible"] += 1
            item["action"] = "not_eligible"
        elif not apply or repair is None:
            summary["would_repair"] += 1
            item["action"] = "would_repair"
        elif attempts >= max_repairs:
            summary["skipped"] += 1
            item["action"] = "skip"
            item["reason"] = "max_repairs"
        else:
            attempts += 1
            try:
                out = repair(folio)
            except Exception as exc:
                summary["errors"] += 1
                item["action"] = "error"
                item["error"] = str(exc)[:500]
            else:
                item["repair_status"] = out.get("status")
                item["local_document_id"] = out.get("local_document_id")
                item["details_replaced"] = out.get("details_replaced")
                if out.get("wrote") and out.get("found_locally_after", True):
                    summary["repaired"] += 1
                    item["action"] = "repaired"
                elif out.get("status") == "already_present":
                    summary["already_local"] += 1
                    item["action"] = "already_exists"
                else:
                    summary["errors"] += 1
                    item["action"] = "error"
        items.append(item)
    return {"mode": "apply" if apply else "dry_run", **summary, "items": items}


def _run_oc_range(args: argparse.Namespace, token: str, *, apply: bool) -> dict[str, Any]:
    folios = list(range(int(args.oc_from), int(args.oc_to) + 1))

    def _args_for(folio: int) -> argparse.Namespace:
        return argparse.Namespace(**{**vars(args), "oc_number": folio})

    return run_oc_range(
        folios,
        office_id=int(args.office_id),
        canary=lambda f: _run_canary(_args_for(f), token),
        repair=(lambda f: _run_repair(_args_for(f), token)) if apply else None,
        apply=apply,
        max_repairs=int(args.max_repairs),
    )


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


def _main_tools(argv: list[str]) -> int:
    load_dotenv_if_available()
    _configure_logging()
    args = _parse_args(argv)
    use_range = args.oc_from is not None or args.oc_to is not None
    modes = sum((args.oc_number is not None, bool(args.reconcile_recent), use_range))
    if modes != 1:
        raise SystemExit(
            "Use uno de: --oc-number, --oc-from/--oc-to, --reconcile-recent; "
            "sin argumentos corre el live normal"
        )
    if use_range:
        if args.oc_from is None or args.oc_to is None:
            raise SystemExit("--oc-from y --oc-to van juntos")
        if not 0 < args.oc_from <= args.oc_to or args.oc_to - args.oc_from >= MAX_OC_RANGE:
            raise SystemExit(f"Rango inválido (1 <= from <= to, máximo {MAX_OC_RANGE} folios)")

    from backend.utils.bsale_token_env import require_bsale_token

    writes = _wants_writes(args)
    token = require_bsale_token(label="live_sync_documents")
    try:
        if use_range:
            out = _run_oc_range(args, token, apply=writes)
        elif args.oc_number is not None:
            out = _run_repair(args, token) if writes else _run_canary(args, token)
        else:
            out = _run_reconcile_recent(args, token, apply=writes)
    except Exception as e:
        logging.getLogger(__name__).exception("live_sync_documents canary/reconcile")
        print(f"[live_sync_documents] ERROR: {e}", file=sys.stderr, flush=True)
        return 1
    print(json.dumps(out, ensure_ascii=False, indent=2, default=str), flush=True)
    return 1 if int(out.get("errors") or 0) > 0 else 0


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    if argv:
        return _main_tools(argv)
    return _main_live()


def _main_live() -> int:
    load_dotenv_if_available()
    _configure_logging()
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
