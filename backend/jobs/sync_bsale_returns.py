"""Job legado — redirige al incremental. Use sync_bsale_returns_history para bootstrap."""

from __future__ import annotations

import logging
import sys

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


def main() -> None:
    logger.warning(
        "sync_bsale_returns está deprecado. "
        "Use: python -m backend.jobs.sync_bsale_returns_history (bootstrap) "
        "o python -m backend.jobs.sync_bsale_returns_incremental (cron).",
    )
    from backend.jobs.sync_bsale_returns_incremental import main as incremental_main

    incremental_main()
    sys.exit(0)


if __name__ == "__main__":
    main()
