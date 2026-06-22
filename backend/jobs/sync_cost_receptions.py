"""Job manual: sincronizar recepciones Bsale → analytics.cost_reception_history."""

from __future__ import annotations

import logging

from backend.services.sync_cost_receptions import sync_cost_receptions

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


def main() -> None:
    result = sync_cost_receptions()
    logger.info("SYNC_COST_RECEPTIONS %s", result)


if __name__ == "__main__":
    main()
