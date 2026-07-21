"""Reconciliación Bsale: carril reciente + cobertura rotativa de OCs abiertas."""

from __future__ import annotations

import json
import logging
import os

from backend.services.distribuidora.bsale_client import BsaleClient
from backend.services.distribuidora.oc_reconciliation_service import reconcile_recent_ocs
from backend.utils.bsale_token_env import load_dotenv_if_available, require_bsale_token


def main() -> int:
    load_dotenv_if_available()
    logging.basicConfig(
        level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    token = require_bsale_token(label="reconcile_bsale_ocs")
    try:
        configured = int(os.getenv("OC_RECONCILIATION_WINDOW_DAYS", "30"))
    except ValueError:
        configured = 30
    try:
        full_batch_size = int(
            os.getenv("OC_RECONCILIATION_FULL_BATCH_SIZE", "100")
        )
    except ValueError:
        full_batch_size = 100
    stats = reconcile_recent_ocs(
        BsaleClient(token),
        window_days=max(30, configured),
        full_coverage_limit=max(1, full_batch_size),
        dry_run=False,
    )
    print(json.dumps(stats, ensure_ascii=False, default=str))
    return 1 if stats.get("errors") else 0


if __name__ == "__main__":
    raise SystemExit(main())
