"""Job: bootstrap histórico devoluciones Bsale (2026-01-01 → 2026-06-30)."""

from __future__ import annotations

import argparse
import logging

from backend.services.sync_bsale_returns import (
    diagnose_bsale_returns_api,
    sync_bsale_returns_history,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Bootstrap histórico returns Bsale — Company 3 / Office 1, H1 2026",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Reanudar carga histórica incompleta desde la última página guardada",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Repetir bootstrap aunque exista uno completed con datos",
    )
    parser.add_argument(
        "--diagnose",
        action="store_true",
        help="Ejecutar pruebas A–E contra la API Bsale (sin sincronizar)",
    )
    args = parser.parse_args()

    if args.diagnose:
        logger.info("[RETURNS_API_DIAG] Modo diagnóstico — sin sincronización")
        result = diagnose_bsale_returns_api()
        logger.info("RETURNS_API_DIAGNOSTIC %s", result)
        if not result.get("ok"):
            raise SystemExit(1)
        return

    logger.info(
        "[RETURNS_SYNC_DEBUG] Iniciando sync_bsale_returns_history resume=%s force=%s",
        args.resume,
        args.force,
    )
    result = sync_bsale_returns_history(resume=args.resume, force=args.force)
    logger.info("SYNC_BSALE_RETURNS_HISTORY %s", result)
    if not result.get("ok"):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
