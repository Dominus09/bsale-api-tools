"""
Dry-run (default) / apply controlado para OCs con header pero sin líneas.

Usa la reconciliación real (`reconcile_one_oc`). No ejecutar --apply desde Cursor
contra producción sin confirmación explícita.

Uso::

    python -m backend.jobs.repair_oc_missing_details_folios --dry-run

    python -m backend.jobs.repair_oc_missing_details_folios \\
        --folios 68701,68700,68697,68696,68695,68694 --dry-run

    python -m backend.jobs.repair_oc_missing_details_folios \\
        --folios 68701 --confirm-folios 68701 --apply
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from typing import Any

from backend.services.distribuidora.bsale_client import BsaleClient
from backend.services.distribuidora.oc_reconciliation_service import reconcile_one_oc
from backend.utils.bsale_token_env import load_dotenv_if_available, read_bsale_token_from_env

logger = logging.getLogger("repair_oc_missing_details")

DEFAULT_FOLIOS = (68701, 68700, 68697, 68696, 68695, 68694)


def _parse_folios(raw: str | None) -> list[int]:
    if not raw or not str(raw).strip():
        return list(DEFAULT_FOLIOS)
    out: list[int] = []
    for part in str(raw).split(","):
        part = part.strip()
        if not part:
            continue
        out.append(int(part))
    if not out:
        raise SystemExit("Lista de folios vacía")
    return out


def _summarize_dry_run(report: dict[str, Any]) -> dict[str, Any]:
    weight = report.get("weight") or {}
    pg_details = report.get("postgresql_details") or []
    bsale_details = report.get("bsale_details") or []
    status = weight.get("status")
    if status is None:
        kg = weight.get("peso_total_kg")
        missing = weight.get("productos_sin_peso")
        total = weight.get("productos_totales")
        if kg is None and not bsale_details:
            status = "unavailable"
        elif missing and total and int(missing) > 0:
            status = "partial"
        elif kg is not None:
            status = "calculated"
        else:
            status = "unavailable"
    return {
        "folio": report.get("folio"),
        "status": report.get("status"),
        "local_document_id": report.get("local_document_id"),
        "source_document_id": report.get("current_bsale_source_document_id"),
        "source_changed": report.get("source_changed"),
        "needs_detail_sync": report.get("needs_detail_sync"),
        "source_hash_matches": report.get("source_hash_matches"),
        "lines_source": len(bsale_details),
        "lines_local_before": len(pg_details),
        "lines_projected_after": len(bsale_details),
        "peso_projected_kg": weight.get("peso_total_kg"),
        "weight_status_projected": status,
        "diff_matches": (report.get("diff") or {}).get("matches"),
    }


def run_folios(
    *,
    folios: list[int],
    dry_run: bool,
    company_id: int = 3,
    office_id: int = 1,
) -> dict[str, Any]:
    token = read_bsale_token_from_env()
    if not token:
        raise SystemExit("BSALE_TOKEN / BSALE_TOKEN_SPA no configuradas")
    client = BsaleClient(token)
    summaries: list[dict[str, Any]] = []
    reports: list[dict[str, Any]] = []
    for folio in folios:
        logger.info("processing folio=%s dry_run=%s", folio, dry_run)
        report = reconcile_one_oc(
            client,
            folio=int(folio),
            dry_run=dry_run,
            company_id=company_id,
            office_id=office_id,
        )
        reports.append(report)
        summaries.append(_summarize_dry_run(report))
    return {
        "dry_run": dry_run,
        "folios": folios,
        "summaries": summaries,
        "reports": reports,
    }


def main(argv: list[str] | None = None) -> int:
    load_dotenv_if_available()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    p = argparse.ArgumentParser(
        description="Resync controlado de OCs sin líneas (dry-run por defecto)"
    )
    p.add_argument(
        "--folios",
        type=str,
        default=",".join(str(f) for f in DEFAULT_FOLIOS),
        help="Folios separados por coma",
    )
    p.add_argument(
        "--confirm-folios",
        type=str,
        default=None,
        help="Obligatorio con --apply; debe coincidir con --folios",
    )
    p.add_argument("--apply", action="store_true", help="Persiste (peligroso en prod)")
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Solo proyección (default si no hay --apply)",
    )
    p.add_argument("--company-id", type=int, default=3)
    p.add_argument("--office-id", type=int, default=1)
    args = p.parse_args(argv)

    folios = _parse_folios(args.folios)
    apply = bool(args.apply)
    dry_run = (not apply) or bool(args.dry_run)
    if apply:
        confirm = _parse_folios(args.confirm_folios)
        if confirm != folios:
            raise SystemExit(
                "--confirm-folios debe coincidir exactamente con --folios para --apply"
            )
        dry_run = False

    result = run_folios(
        folios=folios,
        dry_run=dry_run,
        company_id=int(args.company_id),
        office_id=int(args.office_id),
    )
    print(json.dumps({"summaries": result["summaries"], "dry_run": dry_run}, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
