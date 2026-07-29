"""Job diagnóstico read-only: calidad de costos de recepción / variant_cost.

Uso (Coolify / contenedor backend)::

    python -m backend.jobs.audit_cost_data_quality \\
      --company-id 3 --office-id 1 --days 90 --limit 500 --sample-limit 20

No ejecuta DDL/DML. Siempre hace rollback al terminar.
No ejecutar desde Cursor contra producción.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from typing import Any

from backend.db import get_connection
from backend.repositories.cost_data_audit_repo import CostDataAuditRepository
from backend.services.analytics.cost_audit_models import clamp_cost_audit_args
from backend.services.analytics.cost_data_audit import (
    AnalyticsValidationError,
    make_psycopg_executor,
    open_readonly_connection,
    run_cost_data_audit,
)
from backend.utils.bsale_token_env import load_dotenv_if_available

logger = logging.getLogger(__name__)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Auditoría read-only de calidad de costos (history + variant_cost + taxes)"
    )
    parser.add_argument("--company-id", type=int, required=True)
    parser.add_argument("--office-id", type=int, default=None)
    parser.add_argument("--days", type=int, default=90)
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--sample-limit", type=int, default=20)
    parser.add_argument("--statement-timeout-seconds", type=int, default=20)
    parser.add_argument("--variant-id", type=int, default=None)
    parser.add_argument("--barcode", type=str, default=None)
    parser.add_argument("--source-document-id", type=int, default=None)
    return parser


def run_job(argv: list[str] | None = None) -> tuple[int, dict[str, Any]]:
    args_ns = _parser().parse_args(argv)
    try:
        args = clamp_cost_audit_args(
            company_id=args_ns.company_id,
            office_id=args_ns.office_id,
            days=args_ns.days,
            limit=args_ns.limit,
            sample_limit=args_ns.sample_limit,
            statement_timeout_seconds=args_ns.statement_timeout_seconds,
            variant_id=args_ns.variant_id,
            barcode=args_ns.barcode,
            source_document_id=args_ns.source_document_id,
        )
    except AnalyticsValidationError as exc:
        return 1, {
            "ok": False,
            "read_only": True,
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
            lock_timeout=args.lock_timeout,
            sql_log=[],
        )
        repo = CostDataAuditRepository(executor)
        report = run_cost_data_audit(args=args, repository=repo)
        return 0, report
    except AnalyticsValidationError as exc:
        return 1, {
            "ok": False,
            "read_only": True,
            "error_type": exc.error_type,
            "error": str(exc),
            "details": exc.details,
            "company_id": args.company_id,
            "office_id": args.office_id,
        }
    except Exception as exc:
        logger.exception("audit_cost_data_quality failed")
        msg = str(exc).lower()
        error_type = type(exc).__name__
        if "statement timeout" in msg or "canceling statement" in msg:
            error_type = "statement_timeout"
        elif "does not exist" in msg or "undefinedcolumn" in msg.replace(" ", ""):
            error_type = "schema_mismatch"
        return 1, {
            "ok": False,
            "read_only": True,
            "error_type": error_type,
            "error": str(exc),
            "company_id": args.company_id,
            "office_id": args.office_id,
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
