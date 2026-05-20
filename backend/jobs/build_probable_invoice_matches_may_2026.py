"""
Job: heurística «probable facturada» mayo 2026 (solo escritura en ``document_probable_matches``).

Requisitos: documents + document_details mayo ya en BD. No muta API Bsale ni ``document_related``.

Ejecución (raíz del repo)::

    python -m backend.jobs.build_probable_invoice_matches_may_2026

Validación opcional OC 66697 → boleta 2616098::

    python -m backend.jobs.build_probable_invoice_matches_may_2026 --validate-oc 66697 --validate-boleta 2616098
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

from backend.services.distribuidora.probable_invoice_service import (
    build_probable_invoice_matches_may_2026,
    validate_oc_probable_match,
)
from backend.utils.bsale_token_env import load_dotenv_if_available

_LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"
_LOG_DATEFMT = "%Y-%m-%dT%H:%M:%S"


def _configure_logging() -> None:
    level_name = os.getenv("LOG_LEVEL", "INFO").strip().upper()
    level = getattr(logging, level_name, logging.INFO)
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(level)
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(logging.Formatter(_LOG_FORMAT, datefmt=_LOG_DATEFMT))
    root.addHandler(h)


def main() -> int:
    load_dotenv_if_available()
    _configure_logging()

    parser = argparse.ArgumentParser(
        description="Construye document_probable_matches (mayo 2026 UTC)."
    )
    parser.add_argument(
        "--validate-oc",
        type=int,
        default=None,
        help="Tras el job, validar score de esta OC (número folio).",
    )
    parser.add_argument(
        "--validate-boleta",
        type=int,
        default=None,
        help="Boleta/factura esperada para --validate-oc.",
    )
    args = parser.parse_args()

    print(
        "[build_probable_invoice_matches_may_2026] INICIO — capa analítica, mayo 2026",
        flush=True,
    )

    try:
        stats = build_probable_invoice_matches_may_2026()
    except Exception as e:
        logging.getLogger(__name__).exception("build_probable_invoice_matches_may_2026")
        print(f"[build_probable_invoice_matches_may_2026] ERROR: {e}", file=sys.stderr, flush=True)
        return 1

    print("", flush=True)
    print("=" * 60, flush=True)
    print("PROBABLE INVOICE MATCHES — MAYO 2026 — RESUMEN", flush=True)
    print("=" * 60, flush=True)
    for key in (
        "emission_from",
        "emission_to",
        "ocs_total",
        "ocs_processed",
        "candidates_evaluated",
        "rows_upserted",
        "high_tier",
        "medium_tier",
        "low_tier",
        "rows_deleted_below_min",
    ):
        if key in stats:
            print(f"  {key}: {stats[key]}", flush=True)
    if stats.get("errors"):
        print(f"  errors: {stats['errors']}", flush=True)
    print("=" * 60, flush=True)

    if args.validate_oc is not None and args.validate_boleta is not None:
        v = validate_oc_probable_match(args.validate_oc, args.validate_boleta)
        print("", flush=True)
        print("VALIDACIÓN", flush=True)
        print(f"  OC {args.validate_oc} → boleta {args.validate_boleta}", flush=True)
        print(f"  ok: {v.get('ok')}", flush=True)
        print(f"  computed: {v.get('computed')}", flush=True)
        print(f"  persisted: {v.get('persisted')}", flush=True)
        if not v.get("ok"):
            return 1

    return 1 if stats.get("errors") else 0


if __name__ == "__main__":
    raise SystemExit(main())
