"""Job manual: sincronizar recepciones Bsale → analytics.cost_reception_history."""

from __future__ import annotations

import argparse
import logging

from backend.services.sync_cost_receptions import sync_cost_receptions

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Sync recepciones Bsale → analytics.cost_reception_history"
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=None,
        help="Días hacia atrás si no hay watermark (default: COST_SYNC_INITIAL_DAYS o 90)",
    )
    parser.add_argument(
        "--company-id",
        type=int,
        default=None,
        help="Sincronizar solo esta empresa",
    )
    args = parser.parse_args()
    result = sync_cost_receptions(
        company_id=args.company_id,
        lookback_days=args.lookback_days,
    )
    logger.info("SYNC_COST_RECEPTIONS %s", result)


if __name__ == "__main__":
    main()
