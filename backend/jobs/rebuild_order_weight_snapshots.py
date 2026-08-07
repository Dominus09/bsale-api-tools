"""
Rebuild de snapshots de peso para OCs que ya tienen document_details.

No toca Bsale ni details. Usa ``calculate_order_weight`` / persistencia real
en ``distribuidora.order_weight_snapshots``.

Uso::

    python -m backend.jobs.rebuild_order_weight_snapshots \\
        --folios 68701,68700,68697,68696,68695,68694 --dry-run

    python -m backend.jobs.rebuild_order_weight_snapshots \\
        --folios 68701,68700,68697,68696,68695,68694 \\
        --confirm-folios 68701,68700,68697,68696,68695,68694 \\
        --apply

No ejecutar --apply desde Cursor contra producción sin confirmación explícita.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from typing import Any

from backend.db import get_connection
from backend.services.order_weight_service import (
    build_weight_payload,
    calculate_order_weight,
)
from backend.utils.bsale_token_env import load_dotenv_if_available

logger = logging.getLogger("rebuild_order_weight_snapshots")

DEFAULT_FOLIOS = (68701, 68700, 68697, 68696, 68695, 68694)
COMPANY_ID = 3
OFFICE_ID = 1


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


def _load_docs_by_folios(
    folios: list[int],
    *,
    company_id: int,
    office_id: int,
) -> list[dict[str, Any]]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT d.document_id, d.number,
                   (SELECT COUNT(*)::int
                      FROM distribuidora.document_details dd
                     WHERE dd.document_id = d.document_id) AS line_count,
                   ows.peso_total_kg AS old_peso_kg,
                   ows.porcentaje_cobertura AS old_coverage,
                   ows.productos_sin_peso AS old_missing
            FROM distribuidora.documents d
            LEFT JOIN distribuidora.order_weight_snapshots ows
              ON ows.document_id = d.document_id
            WHERE d.company_id = %s
              AND d.office_id = %s
              AND d.document_type_id = 33
              AND d.number = ANY(%s::bigint[])
            ORDER BY d.number DESC
            """,
            (int(company_id), int(office_id), list(folios)),
        )
        rows = cur.fetchall() or []
        cols = [c[0] for c in cur.description]
        cur.close()
        return [dict(zip(cols, row)) for row in rows]
    finally:
        conn.close()


def _status_from_calc(result: dict[str, Any]) -> str:
    payload = result.get("weight") if isinstance(result.get("weight"), dict) else None
    if payload and payload.get("status"):
        return str(payload["status"])
    summary = {
        "productos_totales": result.get("productos_totales"),
        "productos_sin_peso": result.get("productos_sin_peso"),
        "peso_total_kg": result.get("peso_total_kg"),
    }
    return build_weight_payload(summary, lines=result.get("lines") or []).get(
        "status", "unavailable"
    )


def rebuild_folios(
    *,
    folios: list[int],
    dry_run: bool,
    company_id: int = COMPANY_ID,
    office_id: int = OFFICE_ID,
) -> dict[str, Any]:
    docs = _load_docs_by_folios(folios, company_id=company_id, office_id=office_id)
    found = {int(d["number"]) for d in docs if d.get("number") is not None}
    missing_folios = [f for f in folios if f not in found]
    summaries: list[dict[str, Any]] = []

    for doc in docs:
        document_id = int(doc["document_id"])
        folio = int(doc["number"])
        lines = int(doc.get("line_count") or 0)
        old_snapshot = None
        if doc.get("old_peso_kg") is not None:
            old_snapshot = {
                "peso_total_kg": float(doc["old_peso_kg"]),
                "porcentaje_cobertura": (
                    float(doc["old_coverage"])
                    if doc.get("old_coverage") is not None
                    else None
                ),
                "productos_sin_peso": (
                    int(doc["old_missing"]) if doc.get("old_missing") is not None else None
                ),
            }

        if lines <= 0:
            summaries.append(
                {
                    "folio": folio,
                    "document_id": document_id,
                    "lines": 0,
                    "old_snapshot": old_snapshot,
                    "projected_weight": None,
                    "coverage": None,
                    "missing_products": None,
                    "projected_status": "unavailable",
                    "wrote": False,
                    "reason": "no_local_details",
                }
            )
            continue

        result = calculate_order_weight(
            document_id,
            company_id=company_id,
            office_id=office_id,
            persist_cache=not dry_run,
        )
        status = _status_from_calc(result)
        summaries.append(
            {
                "folio": folio,
                "document_id": document_id,
                "lines": lines,
                "old_snapshot": old_snapshot,
                "projected_weight": result.get("peso_total_kg"),
                "coverage": result.get("porcentaje_cobertura"),
                "missing_products": result.get("productos_sin_peso"),
                "projected_status": status,
                "wrote": not dry_run and bool(result),
                "dry_run": dry_run,
            }
        )
        logger.info(
            "rebuild_snapshot folio=%s document_id=%s dry_run=%s "
            "projected=%s status=%s coverage=%s",
            folio,
            document_id,
            dry_run,
            result.get("peso_total_kg"),
            status,
            result.get("porcentaje_cobertura"),
        )

    return {
        "dry_run": dry_run,
        "folios": folios,
        "missing_folios": missing_folios,
        "summaries": summaries,
    }


def main(argv: list[str] | None = None) -> int:
    load_dotenv_if_available()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    p = argparse.ArgumentParser(
        description="Rebuild order_weight_snapshots para folios explícitos"
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
    p.add_argument("--apply", action="store_true", help="Persiste snapshots")
    p.add_argument("--dry-run", action="store_true", help="Solo proyección")
    p.add_argument("--company-id", type=int, default=COMPANY_ID)
    p.add_argument("--office-id", type=int, default=OFFICE_ID)
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

    result = rebuild_folios(
        folios=folios,
        dry_run=dry_run,
        company_id=int(args.company_id),
        office_id=int(args.office_id),
    )
    print(
        json.dumps(
            {
                "dry_run": dry_run,
                "missing_folios": result["missing_folios"],
                "summaries": result["summaries"],
            },
            indent=2,
            default=str,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
