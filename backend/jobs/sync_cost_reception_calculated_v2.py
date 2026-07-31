"""Job sync incremental / catchup Costos V2.

Dry-run (default)::

    python -m backend.jobs.sync_cost_reception_calculated_v2 \\
      --mode catchup --company-id 3 --office-id 3 \\
      --date-from 2026-06-23 --date-to 2026-07-31

Apply piloto (1 lote)::

    python -m backend.jobs.sync_cost_reception_calculated_v2 \\
      --mode catchup --company-id 3 --office-id 3 \\
      --date-from 2026-06-23 --date-to 2026-07-31 \\
      --apply --confirm-candidate-count N \\
      --commit-batch-size 250 --max-batches 1

Incremental programado::

    python -m backend.jobs.sync_cost_reception_calculated_v2 \\
      --mode incremental --company-id 3 --office-id 3 \\
      --lookback-days 45 --commit-batch-size 250 \\
      --max-candidates 2000 --apply

Orden Coolify: 1) sync recepciones  2) este job  3) API read-only.
No ejecutar apply desde Cursor contra producción.
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
from backend.services.analytics.cost_v2_calculator import CALCULATION_VERSION
from backend.services.analytics.cost_v2_sync import (
    clamp_sync_args,
    run_cost_v2_sync,
)
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
    p = argparse.ArgumentParser(description="Sync Costos V2 catchup/incremental")
    p.add_argument("--mode", required=True, choices=["catchup", "incremental"])
    p.add_argument("--company-id", type=int, required=True)
    p.add_argument("--office-id", type=int, required=True)
    p.add_argument("--date-from", type=_parse_date, default=None)
    p.add_argument("--date-to", type=_parse_date, default=None)
    p.add_argument("--confirm-candidate-count", type=int, default=None)
    p.add_argument("--lookback-days", type=int, default=45)
    p.add_argument("--commit-batch-size", type=int, default=250)
    p.add_argument("--max-batches", type=int, default=None)
    p.add_argument("--max-candidates", type=int, default=2000)
    p.add_argument("--start-after-history-id", type=int, default=0)
    p.add_argument("--dry-run", action="store_true", default=False)
    p.add_argument("--apply", action="store_true", default=False)
    p.add_argument("--statement-timeout-seconds", type=int, default=30)
    p.add_argument("--calculation-version", type=str, default=CALCULATION_VERSION)
    return p


def run_job(argv: list[str] | None = None) -> tuple[int, dict[str, Any]]:
    ns = _parser().parse_args(argv)
    try:
        if ns.apply and ns.dry_run:
            raise AnalyticsValidationError(
                "--dry-run y --apply no pueden usarse juntos",
                error_type="apply_dry_run_conflict",
            )
        # Sin --apply ⇒ dry-run implícito
        apply = bool(ns.apply)
        dry_run = not apply
        args = clamp_sync_args(
            mode=ns.mode,
            company_id=ns.company_id,
            office_id=ns.office_id,
            dry_run=dry_run,
            apply=apply,
            calculation_version=ns.calculation_version,
            commit_batch_size=ns.commit_batch_size,
            statement_timeout_seconds=ns.statement_timeout_seconds,
            date_from=ns.date_from,
            date_to=ns.date_to,
            confirm_candidate_count=ns.confirm_candidate_count,
            lookback_days=ns.lookback_days,
            max_candidates=ns.max_candidates,
            max_batches=ns.max_batches,
            start_after_history_id=ns.start_after_history_id,
        )
    except AnalyticsValidationError as exc:
        return 1, {
            "ok": False,
            "committed": False,
            "mode": getattr(ns, "mode", "rejected"),
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

            def _emit(event: dict[str, Any]) -> None:
                print(json.dumps(event, ensure_ascii=False, default=str), flush=True)

            report = run_cost_v2_sync(
                args=args,
                repository=repo,
                commit_fn=conn.commit,
                rollback_fn=conn.rollback,
                emit_fn=_emit,
            )
            return (0 if report.get("ok") else 1), report

        conn = open_readonly_connection(get_connection)
        executor = make_psycopg_executor(
            conn,
            statement_timeout_seconds=args.statement_timeout_seconds,
            lock_timeout="3s",
            sql_log=[],
        )
        repo = CostV2BackfillRepository(executor)
        report = run_cost_v2_sync(args=args, repository=repo)
        return 0, report
    except AnalyticsValidationError as exc:
        if conn is not None and args.apply:
            try:
                conn.rollback()
            except Exception:
                logger.exception("rollback_failed")
        return 1, {
            "ok": False,
            "committed": False,
            "mode": args.mode,
            "error_type": exc.error_type,
            "error": str(exc),
            "details": getattr(exc, "details", {}) or {},
        }
    except Exception as exc:
        logger.exception("sync_cost_reception_calculated_v2 failed")
        if conn is not None and args.apply:
            try:
                conn.rollback()
            except Exception:
                logger.exception("rollback_failed")
        return 1, {
            "ok": False,
            "committed": False,
            "mode": args.mode,
            "error_type": type(exc).__name__,
            "error": str(exc),
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
