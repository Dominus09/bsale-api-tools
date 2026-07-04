"""Job: sincronización incremental de devoluciones Bsale (post-bootstrap)."""

from __future__ import annotations

import logging

from backend.services.sync_bsale_returns import sync_bsale_returns_incremental

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


def main() -> None:
    result = sync_bsale_returns_incremental()
    logger.info("SYNC_BSALE_RETURNS_INCREMENTAL %s", result)
    if not result.get("ok"):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
