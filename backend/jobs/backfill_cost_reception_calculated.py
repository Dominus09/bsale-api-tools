"""Job Costos V2: dry-run, apply canario 1 fila, o apply-scope lote acotado.

Dry-run::

    python -m backend.jobs.backfill_cost_reception_calculated \\
      --company-id 3 --office-id 3 \\
      --date-from 2026-03-25 --date-to 2026-06-22 \\
      --dry-run

Apply canario (1 history_id)::

    python -m backend.jobs.backfill_cost_reception_calculated \\
      --company-id 3 --office-id 3 \\
      --date-from 2026-03-25 --date-to 2026-06-22 \\
      --history-id 23190 --confirm-history-id 23190 --apply

Apply scope (lote confirmado, máx. 100)::

    python -m backend.jobs.backfill_cost_reception_calculated \\
      --company-id 3 --office-id 3 \\
      --date-from 2026-03-25 --date-to 2026-06-22 \\
      --barcode 7803473005960 \\
      --apply --apply-scope --confirm-row-count 14 --max-apply-rows 100

No backfill masivo. No ejecutar apply desde Cursor contra producción.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date
from typing import Any

from backend.db import get_connection
from backend.repositories.cost_v2_backfill_repo import CostV2BackfillRepository
from backend.services.analytics.cost_v2_backfill import (
    clamp_backfill_args,
    run_cost_v2_backfill_dry_run,
    run_cost_v2_canary_apply,
    run_cost_v2_scope_apply,
)
from backend.services.analytics.cost_v2_calculator import CALCULATION_VERSION
from backend.services.analytics.validate_distribuidora_source import (
    AnalyticsValidationError,
    make_psycopg_executor,
    make_psycopg_rw_executor,
    open_readonly_connection,
    open_readwrite_connection,
)
from backend.utils.bsale_token_env import load_dotenv_if_available

logger = logging.getLogger(__name__)


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Backfill Costos V2: dry-run / apply canario / apply-scope"
    )
    p.add_argument("--company-id", type=int, required=True)
    p.add_argument("--office-id", type=int, default=None)
    p.add_argument("--date-from", type=_parse_date, required=True)
    p.add_argument("--date-to", type=_parse_date, required=True)
    p.add_argument("--dry-run", action="store_true", default=False)
    p.add_argument("--apply", action="store_true", default=False)
    p.add_argument("--apply-scope", action="store_true", default=False)
    p.add_argument("--confirm-row-count", type=int, default=None)
    p.add_argument("--max-apply-rows", type=int, default=100)
    p.add_argument("--batch-size", type=int, default=500)
    p.add_argument("--sample-limit", type=int, default=20)
    p.add_argument("--statement-timeout-seconds", type=int, default=20)
    p.add_argument("--calculation-version", type=str, default=CALCULATION_VERSION)
    p.add_argument("--history-id", type=int, default=None)
    p.add_argument("--confirm-history-id", type=int, default=None)
    p.add_argument("--variant-id", type=int, default=None)
    p.add_argument("--barcode", type=str, default=None)
    p.add_argument("--document-number", type=int, default=None)
    return p


def run_job(argv: list[str] | None = None) -> tuple[int, dict[str, Any]]:
    ns = _parser().parse_args(argv)
    try:
        if ns.apply and ns.dry_run:
            raise AnalyticsValidationError(
                "--dry-run y --apply no pueden usarse juntos",
                error_type="apply_dry_run_conflict",
            )
        if ns.apply:
            dry_run = False
            apply = True
            timeout = min(int(ns.statement_timeout_seconds or 30), 30)
        else:
            dry_run = True
            apply = False
            timeout = ns.statement_timeout_seconds

        args = clamp_backfill_args(
            company_id=ns.company_id,
            office_id=ns.office_id,
            date_from=ns.date_from,
            date_to=ns.date_to,
            dry_run=dry_run,
            batch_size=ns.batch_size,
            sample_limit=ns.sample_limit,
            statement_timeout_seconds=timeout,
            calculation_version=ns.calculation_version,
            history_id=ns.history_id,
            variant_id=ns.variant_id,
            barcode=ns.barcode,
            document_number=ns.document_number,
            apply=apply,
            confirm_history_id=ns.confirm_history_id,
            apply_scope=bool(ns.apply_scope),
            confirm_row_count=ns.confirm_row_count,
            max_apply_rows=ns.max_apply_rows,
        )
    except AnalyticsValidationError as exc:
        mode = "rejected"
        if ns.apply and ns.apply_scope:
            mode = "apply-scope-canary"
        elif ns.apply:
            mode = "apply-canary"
        return 1, {
            "ok": False,
            "committed": False,
            "mode": mode,
            "error_type": exc.error_type,
            "error": str(exc),
            "details": getattr(exc, "details", {}) or {},
        }

    conn = None
    try:
        if args.apply:
            conn = open_readwrite_connection(get_connection)
            timeout_s = min(int(args.statement_timeout_seconds), 30)
            rw = make_psycopg_rw_executor(
                conn,
                statement_timeout_seconds=timeout_s,
                lock_timeout="10s",
                sql_log=[],
            )
            repo = CostV2BackfillRepository(executor=rw, write_executor=rw)
            if args.apply_scope:
                report = run_cost_v2_scope_apply(
                    args=args,
                    repository=repo,
                    commit_fn=conn.commit,
                    rollback_fn=conn.rollback,
                )
            else:
                report = run_cost_v2_canary_apply(
                    args=args,
                    repository=repo,
                    commit_fn=conn.commit,
                    rollback_fn=conn.rollback,
                )
            return 0, report

        conn = open_readonly_connection(get_connection)
        executor = make_psycopg_executor(
            conn,
            statement_timeout_seconds=args.statement_timeout_seconds,
            lock_timeout="3s",
            sql_log=[],
        )
        repo = CostV2BackfillRepository(executor)
        report = run_cost_v2_backfill_dry_run(args=args, repository=repo)
        return 0, report
    except AnalyticsValidationError as exc:
        if conn is not None and args.apply:
            try:
                conn.rollback()
            except Exception:
                logger.exception("rollback_failed")
        mode = (
            "apply-scope-canary"
            if args.apply_scope
            else ("apply-canary" if args.apply else "dry-run")
        )
        return 1, {
            "ok": False,
            "committed": False,
            "mode": mode,
            "error_type": exc.error_type,
            "error": str(exc),
            "details": getattr(exc, "details", {}) or {},
            "company_id": ns.company_id,
            "office_id": ns.office_id,
        }
    except Exception as exc:
        logger.exception("backfill_cost_reception_calculated failed")
        if conn is not None and args.apply:
            try:
                conn.rollback()
            except Exception:
                logger.exception("rollback_failed")
        msg = str(exc).lower()
        error_type = type(exc).__name__
        if "statement timeout" in msg or "canceling statement" in msg:
            error_type = "statement_timeout"
        elif "does not exist" in msg or "undefinedcolumn" in msg.replace(" ", ""):
            error_type = "schema_mismatch"
        mode = (
            "apply-scope-canary"
            if args.apply_scope
            else ("apply-canary" if args.apply else "dry-run")
        )
        return 1, {
            "ok": False,
            "committed": False,
            "mode": mode,
            "error_type": error_type,
            "error": str(exc),
            "company_id": ns.company_id,
            "office_id": ns.office_id,
        }
    finally:
        if conn is not None:
            if not args.apply:
                try:
                    conn.rollback()
                except Exception:
                    logger.exception("rollback_failed")
            try:
                conn.close()
            except Exception:
                logger.exception("connection_close_failed")


def main(argv: list[str] | None = None) -> int:
    load_dotenv_if_available()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    code, payload = run_job(argv)
    print(json.dumps(payload, ensure_ascii=False, default=str))
    return code


if __name__ == "__main__":
    sys.exit(main())
