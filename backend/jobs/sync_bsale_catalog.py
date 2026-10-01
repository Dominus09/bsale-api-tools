"""
Job Coolify: sincronización completa de catálogo Bsale y refresh de products_master.

Secuencia (en proceso, bajo advisory lock de sesión):
  1. catálogo por empresa (ex sync_catalog.py)
  2. costos + precios por empresa (ex sync_prices_costs.py)
  3. stock por empresa (ex sync_stock.py)
  4. backfill_units_per_box_from_sec
  5. refresh_products_master
  6. refresh_product_master_variants

Exit codes: 0 success · 1 failed · 2 partial · 3 otra ejecución en curso (lock).

Uso:
  python -m backend.jobs.sync_bsale_catalog
"""

from __future__ import annotations

import logging
from typing import Any

from backend.services.bsale.catalog_job import EXIT_SUCCESS, run_locked
from backend.services.bsale.catalog_sync_service import (
    LOG_PREFIX,
    count_new_bsale_products_since_pm,
)

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


def sync_bsale_catalog() -> dict[str, Any]:
    try:
        before = count_new_bsale_products_since_pm()
    except Exception:
        logger.exception("%s no se pudo estimar productos nuevos", LOG_PREFIX)
        before = None
    logger.info("%s productos_nuevos_estimados_antes=%s", LOG_PREFIX, before)

    exit_code, stats = run_locked()
    stats["exit_code"] = exit_code
    stats["ok"] = exit_code == EXIT_SUCCESS
    stats["productos_nuevos_estimados_antes"] = before

    if stats.get("status") != "locked":
        try:
            stats["productos_nuevos_restantes"] = count_new_bsale_products_since_pm()
        except Exception:
            logger.exception("%s no se pudo estimar productos restantes", LOG_PREFIX)
    logger.info(
        "%s status=%s exit_code=%s errores=%s",
        LOG_PREFIX,
        stats.get("status"),
        exit_code,
        stats.get("errors") or "ninguno",
    )
    return stats


def main() -> int:
    return int(sync_bsale_catalog()["exit_code"])


if __name__ == "__main__":
    raise SystemExit(main())
