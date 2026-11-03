"""Recuperación selectiva de OC Bsale ausentes en PostgreSQL.

Por defecto opera en dry-run (sin escrituras). Requiere ``--execute`` para persistir.

Ejemplos::

    python -m backend.jobs.reconcile_bsale_ocs --window-days 7 --missing-only

    python -m backend.jobs.reconcile_bsale_ocs \\
        --execute --window-days 7 --missing-only --max-repairs 10
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys

from backend.services.distribuidora.bsale_client import BsaleClient
from backend.services.distribuidora.oc_reconciliation_service import (
    recover_missing_purchase_orders,
    reconcile_recent_ocs,
)
from backend.utils.bsale_token_env import load_dotenv_if_available


def _require_spa_token(*, label: str = "reconcile_bsale_ocs") -> str:
    """Prefiere ``BSALE_TOKEN_SPA`` (La Quillotana); fallback a ``BSALE_TOKEN``."""
    load_dotenv_if_available()
    spa = (os.getenv("BSALE_TOKEN_SPA") or "").strip()
    generic = (os.getenv("BSALE_TOKEN") or "").strip()
    token = spa or generic
    if not token or token in {"PEGAR_TOKEN_AQUI", "PEGAR_TOKEN"}:
        print(
            f"[{label}] ERROR: defina BSALE_TOKEN_SPA (preferido) o BSALE_TOKEN "
            "en el entorno. No se imprimirá el valor.",
            file=sys.stderr,
        )
        raise SystemExit(1)
    source = "BSALE_TOKEN_SPA" if spa else "BSALE_TOKEN"
    logging.getLogger(__name__).info("bsale_token_source=%s", source)
    return token


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Detecta OC tipo 33 vigentes en Bsale ausentes en PostgreSQL "
            "y las recupera de forma selectiva vía reconcile_one_oc"
        )
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Persiste recuperaciones (obligatorio para escribir)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Solo diagnóstico (default si no se pasa --execute)",
    )
    parser.add_argument(
        "--window-days",
        type=int,
        default=7,
        help="Ventana de emissiondaterange en días (default: 7)",
    )
    parser.add_argument(
        "--missing-only",
        action="store_true",
        default=True,
        help="Solo recuperar OC ausentes (default: activo)",
    )
    parser.add_argument(
        "--no-missing-only",
        action="store_true",
        help="Usa el carril legacy (re-reconcilia todo el listado Bsale)",
    )
    parser.add_argument(
        "--fast-lane-only",
        action="store_true",
        default=True,
        help="En modo legacy, omite el carril local de open_purchase_orders",
    )
    parser.add_argument(
        "--no-fast-lane-only",
        action="store_true",
        help="En modo legacy, también ejecuta cobertura local",
    )
    parser.add_argument(
        "--max-repairs",
        type=int,
        default=10,
        help="Máximo de OC ausentes a reconciliar por corrida (default: 10)",
    )
    parser.add_argument(
        "--full-coverage-limit",
        type=int,
        default=100,
        help="Límite del carril local legacy (ignorado con --missing-only)",
    )
    parser.add_argument("--company-id", type=int, default=3)
    parser.add_argument("--office-id", type=int, default=1)
    return parser


def main(argv: list[str] | None = None) -> int:
    load_dotenv_if_available()
    logging.basicConfig(
        level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    args = _parser().parse_args(argv)

    if args.execute and args.dry_run:
        print(
            "ERROR: no combine --execute con --dry-run",
            file=sys.stderr,
        )
        return 2
    if args.window_days <= 0:
        print("ERROR: --window-days debe ser > 0", file=sys.stderr)
        return 2
    if args.max_repairs < 0:
        print("ERROR: --max-repairs debe ser >= 0", file=sys.stderr)
        return 2
    if args.company_id <= 0 or args.office_id <= 0:
        print("ERROR: company-id y office-id deben ser positivos", file=sys.stderr)
        return 2

    dry_run = not bool(args.execute)
    missing_only = not bool(args.no_missing_only)
    fast_lane_only = not bool(args.no_fast_lane_only)

    token = _require_spa_token()
    # Evitar que el token quede referenciado en locals útiles al debuggear dumps.
    client = BsaleClient(token)
    del token

    try:
        if missing_only:
            stats = recover_missing_purchase_orders(
                client,
                window_days=args.window_days,
                dry_run=dry_run,
                max_repairs=args.max_repairs,
                company_id=args.company_id,
                office_id=args.office_id,
            )
        else:
            stats = reconcile_recent_ocs(
                client,
                window_days=args.window_days,
                full_coverage_limit=args.full_coverage_limit,
                dry_run=dry_run,
                company_id=args.company_id,
                office_id=args.office_id,
                fast_lane_only=fast_lane_only,
                missing_only=False,
                max_repairs=args.max_repairs,
            )
    except Exception as exc:
        logging.getLogger(__name__).exception(
            "reconcile_bsale_ocs_failed error=%s", exc
        )
        print(
            json.dumps(
                {"status": "failed", "error": str(exc), "dry_run": dry_run},
                ensure_ascii=False,
            )
        )
        return 1

    print(json.dumps(stats, ensure_ascii=False, default=str))
    status = str(stats.get("status") or "")
    if status == "failed":
        return 1
    if int(stats.get("errors") or 0) > 0 and not dry_run:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
