"""
Reparación de OC 68199 descubriendo dinámicamente el source Bsale activo.

Uso::

    python -m backend.jobs.repair_oc_68199_details_from_bsale_source
    python -m backend.jobs.repair_oc_68199_details_from_bsale_source --execute
    python -m backend.jobs.repair_oc_68199_details_from_bsale_source --execute --recalculate-weight
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from backend.services.distribuidora.bsale_client import BsaleClient
from backend.services.distribuidora.oc_reconciliation_service import (
    reconcile_one_oc,
)
from backend.services.order_weight_service import recalculate_order_weight
from backend.utils.bsale_token_env import load_dotenv_if_available, read_bsale_token_from_env

logger = logging.getLogger("repair_oc_68199")

def execute_repair(
    *,
    dry_run: bool,
    recalculate_weight: bool = False,
) -> dict:
    token = read_bsale_token_from_env()
    if not token:
        raise SystemExit("BSALE_TOKEN / BSALE_TOKEN_SPA no configuradas")
    client = BsaleClient(token)
    report = reconcile_one_oc(
        client,
        folio=68199,
        local_document_id=3832233,
        dry_run=dry_run,
    )
    if (
        recalculate_weight
        and not dry_run
        and report.get("status") == "already_in_sync"
    ):
        local_id = int(report["local_document_id"])
        weight = recalculate_order_weight(
            document_id=local_id,
            company_id=3,
            office_id=1,
            persist=True,
        )
        report.update(
            {
                "recalculated_weight": True,
                "peso_despues_kg": weight.get("peso_total_kg"),
                "cobertura_despues": weight.get("porcentaje_cobertura"),
            }
        )
    return report


def main(argv: list[str] | None = None) -> int:
    load_dotenv_if_available()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    p = argparse.ArgumentParser(description="Reparar details OC 68199 desde Bsale source id")
    p.add_argument(
        "--execute",
        action="store_true",
        help="Aplica replace solo si hay diff (sin esto solo dry-run)",
    )
    p.add_argument(
        "--no-write",
        action="store_true",
        help="Fuerza dry-run aunque se pase --execute",
    )
    p.add_argument(
        "--recalculate-weight",
        action="store_true",
        help="Si ya está in sync, recalcula peso sin replace de detalles",
    )
    args = p.parse_args(argv)
    dry_run = (not args.execute) or args.no_write
    try:
        report = execute_repair(
            dry_run=dry_run,
            recalculate_weight=bool(args.recalculate_weight),
        )
    except SystemExit as e:
        print(str(e.args[0] if e.args else e), file=sys.stderr)
        return 1
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
