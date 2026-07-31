"""Job manual: sincronizar recepciones Bsale → analytics.cost_reception_history.

Legacy (escribe, puede tocar variant_cost)::

    python -m backend.jobs.sync_cost_receptions --company-id 3

Piloto dry-run (default con fechas; NO escribe)::

    python -m backend.jobs.sync_cost_receptions \\
      --company-id 3 \\
      --date-from 2026-06-23 --date-to 2026-07-31 \\
      --history-only --max-receptions 25 --dry-run

Piloto apply (preparado; no ejecutar desde Cursor contra prod)::

    python -m backend.jobs.sync_cost_receptions \\
      --company-id 3 \\
      --date-from 2026-06-23 --date-to 2026-07-31 \\
      --history-only --max-receptions 25 \\
      --confirm-reception-count N --apply
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date
from typing import Any

from backend.services.sync_cost_receptions import (
    CostReceptionSyncError,
    sync_cost_receptions,
)

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def build_parser() -> argparse.ArgumentParser:
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
    parser.add_argument("--date-from", type=_parse_date, default=None)
    parser.add_argument("--date-to", type=_parse_date, default=None)
    parser.add_argument(
        "--confirm-reception-count",
        type=int,
        default=None,
        help="Debe coincidir exactamente con recepciones Bsale del rango (obligatorio con --apply + fechas)",
    )
    parser.add_argument(
        "--max-receptions",
        type=int,
        default=None,
        help="Tope de recepciones a procesar (piloto parcial)",
    )
    parser.add_argument(
        "--history-only",
        action="store_true",
        help="Inserta history + sync_state; no consulta ni escribe variant_cost",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="No escribe (implícito con fechas si no hay --apply)",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Permite escritura (requerido con rango de fechas)",
    )
    return parser


def run_job(argv: list[str] | None = None) -> tuple[int, dict[str, Any]]:
    ns = build_parser().parse_args(argv)
    try:
        result = sync_cost_receptions(
            company_id=ns.company_id,
            lookback_days=ns.lookback_days,
            date_from=ns.date_from,
            date_to=ns.date_to,
            confirm_reception_count=ns.confirm_reception_count,
            max_receptions=ns.max_receptions,
            history_only=bool(ns.history_only),
            dry_run=bool(ns.dry_run),
            apply=bool(ns.apply),
        )
    except CostReceptionSyncError as exc:
        payload = {
            "ok": False,
            "error_type": exc.error_type,
            "error": str(exc),
            "details": exc.details,
        }
        logger.error("SYNC_COST_RECEPTIONS %s", json.dumps(payload, default=str))
        return 2, payload
    except ValueError as exc:
        payload = {"ok": False, "error": str(exc)}
        logger.error("SYNC_COST_RECEPTIONS %s", json.dumps(payload, default=str))
        return 2, payload

    logger.info("SYNC_COST_RECEPTIONS %s", json.dumps(result, default=str))
    return (0 if result.get("ok") else 1), result


def main(argv: list[str] | None = None) -> None:
    code, _ = run_job(argv)
    sys.exit(code)


if __name__ == "__main__":
    main()
