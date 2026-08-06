"""
Reparación controlada de encabezado OC (día OBSERVACIONES + vínculo facturación).

Uso (no ejecutar apply desde Cursor contra producción sin confirmación)::

    python -m backend.jobs.repair_oc_header_from_bsale \\
        --order-number 68513 --dry-run

    python -m backend.jobs.repair_oc_header_from_bsale \\
        --order-number 68513 --confirm-order-number 68513 --apply
"""

from __future__ import annotations

import argparse
import json
import logging
import sys

from backend.services.distribuidora.bsale_client import BsaleClient
from backend.services.distribuidora.oc_reconciliation_service import reconcile_one_oc
from backend.utils.bsale_token_env import load_dotenv_if_available, read_bsale_token_from_env

logger = logging.getLogger("repair_oc_header")


def execute_repair(
    *,
    order_number: int,
    dry_run: bool,
    company_id: int = 3,
    office_id: int = 1,
) -> dict:
    token = read_bsale_token_from_env()
    if not token:
        raise SystemExit("BSALE_TOKEN / BSALE_TOKEN_SPA no configuradas")
    client = BsaleClient(token)
    return reconcile_one_oc(
        client,
        folio=int(order_number),
        dry_run=dry_run,
        company_id=company_id,
        office_id=office_id,
    )


def main(argv: list[str] | None = None) -> int:
    load_dotenv_if_available()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    p = argparse.ArgumentParser(
        description=(
            "Repara día (OBSERVACIONES) y vínculo de factura de una OC "
            "desde el source Bsale vigente"
        )
    )
    p.add_argument("--order-number", type=int, required=True, help="Folio OC")
    p.add_argument(
        "--confirm-order-number",
        type=int,
        default=None,
        help="Obligatorio con --apply; debe coincidir con --order-number",
    )
    p.add_argument(
        "--apply",
        action="store_true",
        help="Persiste cambios (requiere --confirm-order-number)",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Solo diagnóstico (default si no hay --apply)",
    )
    p.add_argument("--company-id", type=int, default=3)
    p.add_argument("--office-id", type=int, default=1)
    args = p.parse_args(argv)

    apply = bool(args.apply)
    if apply:
        if args.confirm_order_number is None:
            p.error("--apply requiere --confirm-order-number")
        if int(args.confirm_order_number) != int(args.order_number):
            p.error("--confirm-order-number debe coincidir con --order-number")
        dry_run = False
    else:
        dry_run = True

    try:
        report = execute_repair(
            order_number=int(args.order_number),
            dry_run=dry_run,
            company_id=int(args.company_id),
            office_id=int(args.office_id),
        )
    except SystemExit as e:
        print(str(e.args[0] if e.args else e), file=sys.stderr)
        return 1

    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
