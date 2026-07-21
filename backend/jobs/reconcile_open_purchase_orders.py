"""Scheduled Task: reconciliar un lote acotado de OCs abiertas/no facturadas."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys

from backend.services.distribuidora.bsale_client import BsaleClient
from backend.services.distribuidora.oc_reconciliation_service import (
    reconcile_open_purchase_orders_batch,
)
from backend.utils.bsale_token_env import load_dotenv_if_available, require_bsale_token


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Reconciliar por lotes OCs abiertas y sin factura definitiva"
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Persiste cambios y mueve last_reconciliation_at",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fuerza diagnóstico read-only, aun si también se pasó --execute",
    )
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--recent-days", type=int, default=30)
    parser.add_argument("--company-id", type=int, default=3)
    parser.add_argument("--office-id", type=int, default=1)
    return parser


def main(argv: list[str] | None = None) -> int:
    load_dotenv_if_available()
    logging.basicConfig(
        level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    args = _parser().parse_args(argv)
    if args.limit <= 0:
        raise SystemExit("--limit debe ser mayor que cero")
    if args.recent_days <= 0:
        raise SystemExit("--recent-days debe ser mayor que cero")
    if args.company_id <= 0 or args.office_id <= 0:
        raise SystemExit("--company-id y --office-id deben ser positivos")

    execute = bool(args.execute and not args.dry_run)
    token = require_bsale_token(label="reconcile_open_purchase_orders")
    try:
        result = reconcile_open_purchase_orders_batch(
            BsaleClient(token),
            execute=execute,
            limit=args.limit,
            recent_days=args.recent_days,
            company_id=args.company_id,
            office_id=args.office_id,
        )
    except Exception as exc:
        logging.getLogger(__name__).exception(
            "reconciliation_cycle_failed error=%s",
            exc,
        )
        return 2

    print(json.dumps(result, ensure_ascii=False, default=str))
    return 1 if int(result.get("errors") or 0) > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
