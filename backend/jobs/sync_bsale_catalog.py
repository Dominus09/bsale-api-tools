"""
Job Coolify: sincronización completa de catálogo Bsale y refresh de products_master.

Secuencia:
  1. sync_catalog.py
  2. sync_prices_costs.py
  3. sync_stock.py
  4. backfill_units_per_box_from_sec
  5. refresh_products_master

Uso:
  python -m backend.jobs.sync_bsale_catalog
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

from backend.services.bsale.catalog_sync_service import (
    LOG_PREFIX,
    backfill_units_per_box_from_sec,
    count_new_bsale_products_since_pm,
    refresh_products_master,
)

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

_REPO_ROOT = Path(__file__).resolve().parents[2]


def _run_root_script(script_name: str) -> None:
    path = _REPO_ROOT / script_name
    if not path.is_file():
        raise FileNotFoundError(f"No se encontró {path}")
    logger.info("%s iniciando %s", LOG_PREFIX, script_name)
    subprocess.run(
        [sys.executable, str(path)],
        cwd=str(_REPO_ROOT),
        env=os.environ.copy(),
        check=True,
    )
    logger.info("%s completado %s", LOG_PREFIX, script_name)


def sync_bsale_catalog() -> dict[str, Any]:
    stats: dict[str, Any] = {
        "ok": True,
        "productos_nuevos_estimados_antes": 0,
        "units_per_box_actualizados": 0,
        "products_master_insertados": 0,
        "products_master_actualizados": 0,
        "errores": [],
    }
    try:
        stats["productos_nuevos_estimados_antes"] = count_new_bsale_products_since_pm()
        logger.info(
            "%s productos_nuevos_estimados_antes=%s",
            LOG_PREFIX,
            stats["productos_nuevos_estimados_antes"],
        )

        _run_root_script("sync_catalog.py")
        _run_root_script("sync_prices_costs.py")
        _run_root_script("sync_stock.py")

        bf = backfill_units_per_box_from_sec()
        stats["units_per_box_actualizados"] = bf.get("units_per_box_actualizados", 0)
        if not bf.get("ok"):
            stats["errores"].append(bf.get("error") or "backfill_units_per_box_from_sec")

        rf = refresh_products_master()
        stats["products_master_insertados"] = rf.get("products_master_insertados", 0)
        stats["products_master_actualizados"] = rf.get("products_master_actualizados", 0)
        if not rf.get("ok"):
            stats["errores"].append(rf.get("error") or "refresh_products_master")

        nuevos_despues = count_new_bsale_products_since_pm()
        stats["productos_nuevos_restantes"] = nuevos_despues
        logger.info(
            "%s productos_nuevos=%s productos_actualizados=%s "
            "units_per_box_actualizados=%s products_master_insertados=%s "
            "products_master_actualizados=%s errores=%s",
            LOG_PREFIX,
            stats["productos_nuevos_estimados_antes"],
            "—",
            stats["units_per_box_actualizados"],
            stats["products_master_insertados"],
            stats["products_master_actualizados"],
            stats["errores"] or "ninguno",
        )
        if stats["errores"]:
            stats["ok"] = False
    except subprocess.CalledProcessError as exc:
        stats["ok"] = False
        stats["errores"].append(f"subprocess: {exc}")
        logger.exception("%s falló subprocess", LOG_PREFIX)
    except Exception as exc:
        stats["ok"] = False
        stats["errores"].append(str(exc))
        logger.exception("%s falló", LOG_PREFIX)

    return stats


def main() -> int:
    result = sync_bsale_catalog()
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
