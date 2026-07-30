"""Job dry-run Costos V2: calcula sin escribir.

Uso (Coolify / contenedor backend)::

    python -m backend.jobs.backfill_cost_reception_calculated \\
      --company-id 3 --office-id 3 \\
      --date-from 2026-03-25 --date-to 2026-06-22 \\
      --dry-run --batch-size 500 --sample-limit 30 \\
      --statement-timeout-seconds 30

Etapa D: solo dry-run. --apply se rechaza.
No ejecuta migración 047. No escribe datos.
No ejecutar desde Cursor contra producción.
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
)
from backend.services.analytics.cost_v2_calculator import CALCULATION_VERSION
from backend.services.analytics.validate_distribuidora_source import (
    AnalyticsValidationError,
    make_psycopg_executor,
    open_readonly_connection,
)
from backend.utils.bsale_token_env import load_dotenv_if_available

logger = logging.getLogger(__name__)


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Backfill dry-run Costos V2 (read-only; sin escrituras)"
    )
    p.add_argument("--company-id", type=int, required=True)
    p.add_argument("--office-id", type=int, default=None)
    p.add_argument("--date-from", type=_parse_date, required=True)
    p.add_argument("--date-to", type=_parse_date, required=True)
    p.add_argument("--dry-run", action="store_true", default=False)
    p.add_argument("--apply", action="store_true", default=False)
    p.add_argument("--batch-size", type=int, default=500)
    p.add_argument("--sample-limit", type=int, default=20)
    p.add_argument("--statement-timeout-seconds", type=int, default=20)
    p.add_argument("--calculation-version", type=str, default=CALCULATION_VERSION)
    p.add_argument("--history-id", type=int, default=None)
    p.add_argument("--variant-id", type=int, default=None)
    p.add_argument("--barcode", type=str, default=None)
    p.add_argument("--document-number", type=int, default=None)
    return p


def run_job(argv: list[str] | None = None) -> tuple[int, dict[str, Any]]:
    ns = _parser().parse_args(argv)
    try:
        # dry-run por defecto obligatorio: si no viene flag, forzar True
        dry_run = True if not ns.apply else bool(ns.dry_run)
        if not ns.dry_run and not ns.apply:
            dry_run = True
        args = clamp_backfill_args(
            company_id=ns.company_id,
            office_id=ns.office_id,
            date_from=ns.date_from,
            date_to=ns.date_to,
            dry_run=dry_run or True,
            batch_size=ns.batch_size,
            sample_limit=ns.sample_limit,
            statement_timeout_seconds=ns.statement_timeout_seconds,
            calculation_version=ns.calculation_version,
            history_id=ns.history_id,
            variant_id=ns.variant_id,
            barcode=ns.barcode,
            document_number=ns.document_number,
            apply=bool(ns.apply),
        )
    except AnalyticsValidationError as exc:
        return 1, {
            "ok": False,
            "read_only": True,
            "mode": "rejected",
            "error_type": exc.error_type,
            "error": str(exc),
            "details": exc.details,
        }

    conn = None
    try:
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
        return 1, {
            "ok": False,
            "read_only": True,
            "mode": "dry-run",
            "error_type": exc.error_type,
            "error": str(exc),
            "details": exc.details,
            "company_id": ns.company_id,
            "office_id": ns.office_id,
        }
    except Exception as exc:
        logger.exception("backfill_cost_reception_calculated dry-run failed")
        msg = str(exc).lower()
        error_type = type(exc).__name__
        if "statement timeout" in msg or "canceling statement" in msg:
            error_type = "statement_timeout"
        elif "does not exist" in msg or "undefinedcolumn" in msg.replace(" ", ""):
            error_type = "schema_mismatch"
        return 1, {
            "ok": False,
            "read_only": True,
            "mode": "dry-run",
            "error_type": error_type,
            "error": str(exc),
            "company_id": ns.company_id,
            "office_id": ns.office_id,
        }
    finally:
        if conn is not None:
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
