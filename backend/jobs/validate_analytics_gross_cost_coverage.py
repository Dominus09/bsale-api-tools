"""Job diagnóstico read-only: cobertura costo bruto comercial (Etapa 3B).

Uso (Coolify / contenedor backend)::

    python -m backend.jobs.validate_analytics_gross_cost_coverage \\
      --company-id 3 --office-id 1 --days 7 --document-limit 200

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
from backend.services.analytics.cost_repository import CostCandidateRepository
from backend.services.analytics.distribuidora_source import DistribuidoraDocumentSource
from backend.services.analytics.validate_gross_cost_coverage import (
    AnalyticsValidationError,
    clamp_gross_validate_args,
    make_psycopg_executor,
    open_readonly_connection,
    run_gross_cost_validation,
)
from backend.utils.bsale_token_env import load_dotenv_if_available

logger = logging.getLogger(__name__)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validación read-only cobertura costo bruto analytics"
    )
    parser.add_argument("--company-id", type=int, required=True)
    parser.add_argument("--office-id", type=int, required=True)
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--document-limit", type=int, default=200)
    parser.add_argument("--statement-timeout-seconds", type=int, default=20)
    parser.add_argument("--sample-limit", type=int, default=10)
    return parser


def run_job(argv: list[str] | None = None) -> tuple[int, dict[str, Any]]:
    args_ns = _parser().parse_args(argv)
    try:
        args = clamp_gross_validate_args(
            company_id=args_ns.company_id,
            office_id=args_ns.office_id,
            days=args_ns.days,
            document_limit=args_ns.document_limit,
            statement_timeout_seconds=args_ns.statement_timeout_seconds,
            sample_limit=args_ns.sample_limit,
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
            sql_log=[],
        )
        doc_source = DistribuidoraDocumentSource(executor)
        cost_repo = CostCandidateRepository(executor)
        report = run_gross_cost_validation(
            args=args,
            document_source=doc_source,
            cost_repository=cost_repo,
        )
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
        logger.exception("validate_analytics_gross_cost_coverage failed")
        return 1, {
            "ok": False,
            "read_only": True,
            "error_type": type(exc).__name__,
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
    raise SystemExit(main())
