"""
Job manual: backfill units_per_box desde (SEC N) en variants.description.

Uso:
  python -m backend.jobs.backfill_units_per_box
  python -m backend.jobs.backfill_units_per_box --dry-run

Logs esperados:
  [SEC_BACKFILL] dry_run=false variants_total=... variants_con_sec=...
  variants_actualizadas=... products_master_actualizados=... duration_ms=...
"""

from __future__ import annotations

import argparse
import logging
import sys

from backend.services.bsale.catalog_sync_service import SEC_LOG_PREFIX, run_sec_backfill

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Backfill units_per_box desde patrón (SEC N) en bsale.variants",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Solo contar registros que se modificarían; no escribe en BD",
    )
    args = parser.parse_args(argv)

    result = run_sec_backfill(dry_run=args.dry_run)
    if not result.get("ok"):
        logger.error("%s error=%s", SEC_LOG_PREFIX, result.get("error"))
        return 1

    print(
        f"{SEC_LOG_PREFIX}",
        f"dry_run={str(args.dry_run).lower()}",
        f"variants_total={result.get('variants_total', 0)}",
        f"variants_con_sec={result.get('variants_con_sec', 0)}",
        f"variants_actualizadas={result.get('variants_actualizadas', 0)}",
        f"products_master_actualizados={result.get('products_master_actualizados', 0)}",
        f"duration_ms={result.get('duration_ms', 0)}",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
