"""
Reparación SOLO para OC 68199 / document_id PG 3832233.

Problema confirmado (2026-07-20):
  - documents.raw_data.id = 3832384 (Bsale vivo, total 219800, folio 68199)
  - documents.document_id  = 3832233 (PK histórica por ON CONFLICT folio)
  - document_details se refrescaron desde GET /documents/3832233/details.json
    → quantity=1 (fantasma) → peso modal 15 kg en vez de 300 kg

Uso (NO ejecutar writes sin --execute)::

    python -m backend.jobs.repair_oc_68199_details_from_bsale_source
    python -m backend.jobs.repair_oc_68199_details_from_bsale_source --execute
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from typing import Any

from backend.db import get_connection
from backend.repositories.distribuidora.details_repo import replace_document_details
from backend.services.distribuidora.bsale_client import BsaleClient
from backend.services.order_weight_service import (
    invalidate_order_weight_cache,
    recalculate_order_weight,
)
from backend.utils.bsale_document_ids import resolve_bsale_source_document_id
from backend.utils.bsale_token_env import load_dotenv_if_available, read_bsale_token_from_env

logger = logging.getLogger("repair_oc_68199")

PG_DOCUMENT_ID = 3832233
EXPECTED_FOLIO = 68199


def _fetch_details(client: BsaleClient, bsale_document_id: int) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    offset = 0
    limit = 50
    while True:
        data = client.get(
            f"/documents/{bsale_document_id}/details.json",
            {"limit": limit, "offset": offset},
        )
        page = data.get("items") if isinstance(data, dict) else data
        if not isinstance(page, list):
            break
        items.extend(page)
        if len(page) < limit:
            break
        offset += len(page)
        if offset > 5000:
            break
    return items


def plan_repair() -> dict[str, Any]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT document_id, number, total_amount,
                   raw_data->>'id' AS raw_bsale_id,
                   raw_data->>'totalAmount' AS raw_total
            FROM distribuidora.documents
            WHERE document_id = %s
            """,
            (PG_DOCUMENT_ID,),
        )
        doc = cur.fetchone()
        cur.execute(
            """
            SELECT detail_id, variant_id, quantity, total_amount
            FROM distribuidora.document_details
            WHERE document_id = %s
            ORDER BY detail_id
            """,
            (PG_DOCUMENT_ID,),
        )
        details = cur.fetchall()
        cur.close()
    finally:
        conn.close()

    if not doc:
        raise SystemExit(f"PG document_id={PG_DOCUMENT_ID} no existe")

    raw_bsale_id = doc[3]
    source_id = resolve_bsale_source_document_id(
        local_document_id=PG_DOCUMENT_ID,
        raw_data_id=raw_bsale_id,
    )

    return {
        "pg_document_id": PG_DOCUMENT_ID,
        "expected_folio": EXPECTED_FOLIO,
        "bsale_source_document_id": source_id,
        "pg_header": {
            "document_id": doc[0],
            "number": doc[1],
            "total_amount": float(doc[2]) if doc[2] is not None else None,
            "raw_bsale_id": raw_bsale_id,
            "raw_total": doc[4],
        },
        "pg_details_before": [
            {
                "detail_id": r[0],
                "variant_id": r[1],
                "quantity": float(r[2]) if r[2] is not None else None,
                "total_amount": float(r[3]) if r[3] is not None else None,
            }
            for r in details
        ],
        "expected_diff": {
            "quantity": {"before": 1.0, "after": 20.0},
            "line_total": {"before": 10990.0, "after": 219800.0},
            "peso_total_kg": {"before": 15.0, "after": 300.0, "unit_manual_kg": 15.0},
        },
        "steps": [
            f"GET /documents/{source_id}.json — validar number={EXPECTED_FOLIO}",
            f"GET /documents/{source_id}/details.json — esperar quantity≈20",
            f"replace_document_details(pg={PG_DOCUMENT_ID}, items=…)",
            f"invalidate_order_weight_cache({PG_DOCUMENT_ID})",
            f"recalculate_order_weight({PG_DOCUMENT_ID}) → esperado ~300 kg (20×15)",
            "No borrar products_master.weight_box_kg=15 (peso manual/unitario)",
        ],
    }


def execute_repair(*, dry_run: bool) -> dict[str, Any]:
    plan = plan_repair()
    token = read_bsale_token_from_env()
    if not token:
        raise SystemExit("BSALE_TOKEN / BSALE_TOKEN_SPA no configuradas")

    client = BsaleClient(token)
    src = int(plan["bsale_source_document_id"])
    header = client.get(f"/documents/{src}.json")
    if not isinstance(header, dict):
        raise SystemExit("Respuesta documento Bsale inválida")
    number = header.get("number")
    try:
        if int(number) != EXPECTED_FOLIO:
            raise SystemExit(
                f"ABORT: Bsale id={src} number={number!r} ≠ {EXPECTED_FOLIO}"
            )
    except (TypeError, ValueError) as e:
        raise SystemExit(f"ABORT: number Bsale inválido: {number!r}") from e

    items = _fetch_details(client, src)
    qty_sum = sum(float(it.get("quantity") or 0) for it in items)
    plan["bsale_live"] = {
        "id": header.get("id"),
        "number": number,
        "totalAmount": header.get("totalAmount"),
        "detail_count": len(items),
        "quantity_sum": qty_sum,
        "lines": [
            {
                "id": it.get("id"),
                "quantity": it.get("quantity"),
                "totalAmount": it.get("totalAmount"),
                "variant_id": (it.get("variant") or {}).get("id"),
            }
            for it in items
        ],
    }
    plan["diff_preview"] = {
        "local_document_id": PG_DOCUMENT_ID,
        "bsale_source_document_id": src,
        "ids_differ": PG_DOCUMENT_ID != src,
        "pg_quantity_before": (
            plan["pg_details_before"][0]["quantity"] if plan["pg_details_before"] else None
        ),
        "bsale_quantity_live": qty_sum,
        "pg_header_total": plan["pg_header"]["total_amount"],
        "bsale_total_live": header.get("totalAmount"),
    }

    if dry_run:
        plan["executed"] = False
        plan["note"] = "Dry-run: no se escribió en PostgreSQL"
        return plan

    conn = get_connection()
    try:
        cur = conn.cursor()
        n = replace_document_details(cur, PG_DOCUMENT_ID, items)
        conn.commit()
        cur.close()
    finally:
        conn.close()

    invalidate_order_weight_cache(PG_DOCUMENT_ID)
    weight = recalculate_order_weight(
        PG_DOCUMENT_ID, company_id=3, office_id=1, persist=True
    )
    plan["executed"] = True
    plan["details_rows_written"] = n
    plan["peso_despues_kg"] = weight.get("peso_total_kg")
    plan["cobertura_despues"] = weight.get("porcentaje_cobertura")
    return plan


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
        help="Aplica replace/invalidate/recalc (sin esto solo dry-run)",
    )
    p.add_argument(
        "--no-write",
        action="store_true",
        help="Fuerza dry-run aunque se pase --execute",
    )
    args = p.parse_args(argv)
    dry_run = (not args.execute) or args.no_write
    try:
        report = execute_repair(dry_run=dry_run)
    except SystemExit as e:
        print(str(e.args[0] if e.args else e), file=sys.stderr)
        return 1
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
