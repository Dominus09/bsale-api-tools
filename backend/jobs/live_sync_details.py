"""
Cron Coolify (cada 15 min): detalles live para documentos recientes.

    python -m backend.jobs.live_sync_details

Cron sugerido: ``*/15 * * * *``
"""

from __future__ import annotations

import logging
import os
import sys

from backend.services.distribuidora.live_sync_service import (
    _print_summary,
    live_sync_details,
)
from backend.utils.bsale_token_env import load_dotenv_if_available

_LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"


def _configure_logging() -> None:
    level = getattr(logging, os.getenv("LOG_LEVEL", "INFO").strip().upper(), logging.INFO)
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(level)
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(logging.Formatter(_LOG_FORMAT))
    root.addHandler(h)


def main() -> int:
    load_dotenv_if_available()
    _configure_logging()
    print("[live_sync_details] INICIO", flush=True)
    try:
        stats = live_sync_details(strict_token=True)
    except ValueError as e:
        print(f"[live_sync_details] ERROR: {e}", file=sys.stderr, flush=True)
        return 1
    except Exception as e:
        logging.getLogger(__name__).exception("live_sync_details")
        print(f"[live_sync_details] ERROR: {e}", file=sys.stderr, flush=True)
        return 1

    _print_summary("LIVE SYNC DETAILS — SUMMARY", stats)
    if stats.get("omitido_concurrencia"):
        return int(os.getenv("LIVE_SYNC_EXIT_CODE_ON_LOCK", "0"))
    if stats.get("skipped") or stats.get("errors"):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
