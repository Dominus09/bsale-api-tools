"""Job diagnóstico read-only: validar adaptador analítico Distribuidora (Etapa 2B).

Uso (Coolify / contenedor backend)::

    python -m backend.jobs.validate_analytics_distribuidora_source \\
      --company-id 3 --office-id 1 --days 2 --limit 20

No ejecuta DDL/DML. Siempre hace rollback al terminar.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from typing import Any

from backend.db import get_connection
from backend.services.analytics.distribuidora_source import DistribuidoraDocumentSource
from backend.services.analytics.validate_distribuidora_source import (
    AnalyticsValidationError,
    clamp_validate_args,
    make_psycopg_executor,
    open_readonly_connection,
    run_validation,
)
from backend.utils.bsale_token_env import load_dotenv_if_available

logger = logging.getLogger(__name__)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validación read-only del adaptador analytics Distribuidora"
    )
    parser.add_argument("--company-id", type=int, required=True)
    parser.add_argument("--office-id", type=int, required=True)
    parser.add_argument("--days", type=int, default=2)
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--page-size", type=int, default=20)
    parser.add_argument(
        "--document-types",
        type=str,
        default="1,6,9",
        help="Lista separada por comas (default 1,6,9)",
    )
    parser.add_argument("--statement-timeout-seconds", type=int, default=15)
    return parser


def _parse_types(raw: str) -> tuple[int, ...]:
    parts = [p.strip() for p in str(raw).split(",") if p.strip()]
    return tuple(int(p) for p in parts)


def run_job(argv: list[str] | None = None) -> tuple[int, dict[str, Any]]:
    args_ns = _parser().parse_args(argv)
    try:
        args = clamp_validate_args(
            company_id=args_ns.company_id,
            office_id=args_ns.office_id,
            days=args_ns.days,
            limit=args_ns.limit,
            page_size=args_ns.page_size,
            document_types=_parse_types(args_ns.document_types),
            statement_timeout_seconds=args_ns.statement_timeout_seconds,
        )
    except AnalyticsValidationError as exc:
        payload = {
            "ok": False,
            "read_only": True,
            "error_type": exc.error_type,
            "error": str(exc),
            "details": exc.details,
        }
        return 1, payload

    conn = None
    sql_log: list[str] = []
    try:
        conn = open_readonly_connection(get_connection)
        executor = make_psycopg_executor(
            conn,
            statement_timeout_seconds=args.statement_timeout_seconds,
            sql_log=sql_log,
        )
        source = DistribuidoraDocumentSource(executor)
        report = run_validation(args=args, source=source)
        return 0, report
    except AnalyticsValidationError as exc:
        payload = {
            "ok": False,
            "read_only": True,
            "error_type": exc.error_type,
            "error": str(exc),
            "details": exc.details,
            "company_id": args.company_id,
            "office_id": args.office_id,
        }
        return 1, payload
    except Exception as exc:
        logger.exception("validate_analytics_distribuidora_source failed")
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
