"""
Diagnóstico SOLO LECTURA: compara una OC Bsale vs PostgreSQL.

Pensado para ejecutarse en el contenedor backend (donde existe BSALE_TOKEN)::

    python -m backend.jobs.diagnose_oc_bsale_vs_pg --folio 68199 --company-id 3 --office-id 1 --no-write

Opciones:
  --folio N          número/folio de la OC (document_type 33)
  --document-id ID   alternativa si se conoce el id Bsale/PG
  --company-id       default 3
  --office-id        default 1
  --no-write         obligatorio en esta versión (solo lectura; falla si se omite
                     en entornos que lo exijan vía DIAGNOSE_REQUIRE_NO_WRITE=1)

No imprime tokens ni secretos. No escribe en PostgreSQL ni en Bsale.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import sys
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from backend.db import get_connection
from backend.services.distribuidora.bsale_client import BsaleClient
from backend.utils.bsale_token_env import load_dotenv_if_available, read_bsale_token_from_env

logger = logging.getLogger("diagnose_oc")

DOC_TYPE_OC = 33
HEADER_KEYS = (
    "totalAmount",
    "netAmount",
    "taxAmount",
    "state",
    "commercialState",
    "emissionDate",
    "expirationDate",
    "generationDate",
    "modificationDate",
    "number",
    "informedSii",
)


def _json_safe(v: Any) -> Any:
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, datetime):
        return v.isoformat()
    return v


def _num(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _epoch_iso(raw: Any) -> str | None:
    if raw is None:
        return None
    try:
        return datetime.fromtimestamp(int(raw), tz=timezone.utc).isoformat()
    except (TypeError, ValueError, OSError):
        return str(raw)


def _details_hash(items: list[dict[str, Any]]) -> str:
    norm = []
    for it in items:
        variant = it.get("variant") or {}
        norm.append(
            {
                "id": it.get("id"),
                "lineNumber": it.get("lineNumber"),
                "variant_id": variant.get("id"),
                "quantity": it.get("quantity"),
                "netUnitValue": it.get("netUnitValue"),
                "totalUnitValue": it.get("totalUnitValue"),
                "netAmount": it.get("netAmount"),
                "totalAmount": it.get("totalAmount"),
                "netDiscount": it.get("netDiscount"),
                "totalDiscount": it.get("totalDiscount"),
                "discountPercentage": it.get("discountPercentage"),
            }
        )
    norm.sort(key=lambda x: (x.get("lineNumber") is None, x.get("lineNumber"), x.get("id")))
    blob = json.dumps(norm, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:16]


def _fetch_bsale_details(client: BsaleClient, document_id: int) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    offset = 0
    limit = 50
    while True:
        data = client.get(
            f"/documents/{document_id}/details.json",
            {"limit": limit, "offset": offset},
        )
        page = data.get("items") or []
        if not isinstance(page, list):
            break
        items.extend(page)
        if len(page) < limit:
            break
        offset += len(page)
        if offset > 5000:
            break
    return items


def _load_pg_oc(
    *,
    folio: int | None,
    document_id: int | None,
    company_id: int,
    office_id: int,
) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        if document_id is not None:
            cur.execute(
                """
                SELECT document_id, number, company_id, office_id, document_type_id,
                       client_id, emission_date, expiration_date, generation_date,
                       total_amount, net_amount, tax_amount, state, commercial_state,
                       informed_sii, updated_at, created_at, raw_data
                FROM distribuidora.documents
                WHERE document_id = %s
                LIMIT 1
                """,
                (document_id,),
            )
        else:
            cur.execute(
                """
                SELECT document_id, number, company_id, office_id, document_type_id,
                       client_id, emission_date, expiration_date, generation_date,
                       total_amount, net_amount, tax_amount, state, commercial_state,
                       informed_sii, updated_at, created_at, raw_data
                FROM distribuidora.documents
                WHERE company_id = %s AND office_id = %s
                  AND document_type_id = %s AND number = %s
                ORDER BY document_id DESC
                LIMIT 1
                """,
                (company_id, office_id, DOC_TYPE_OC, folio),
            )
        row = cur.fetchone()
        if not row:
            return None, []
        cols = [d[0] for d in cur.description]
        doc = {c: _json_safe(v) for c, v in zip(cols, row)}
        did = int(doc["document_id"])
        cur.execute(
            """
            SELECT detail_id, line_number, variant_id, quantity,
                   net_unit_value, total_unit_value, net_amount, tax_amount,
                   total_amount, net_discount, total_discount, discount_percentage,
                   variant_code, variant_description, updated_at, raw_data
            FROM distribuidora.document_details
            WHERE document_id = %s
            ORDER BY line_number NULLS LAST, detail_id
            """,
            (did,),
        )
        dcols = [d[0] for d in cur.description]
        details = [{c: _json_safe(v) for c, v in zip(dcols, r)} for r in cur.fetchall()]
        cur.close()
        return doc, details
    finally:
        conn.close()


def _header_diffs(bsale_doc: dict[str, Any], pg_doc: dict[str, Any]) -> list[dict[str, Any]]:
    raw = pg_doc.get("raw_data") or {}
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except Exception:
            raw = {}

    mapping = [
        ("total_amount", "totalAmount", pg_doc.get("total_amount"), "monto / camión"),
        ("net_amount", "netAmount", pg_doc.get("net_amount"), "margen / reportes"),
        ("tax_amount", "taxAmount", pg_doc.get("tax_amount"), "impuestos"),
        ("state", "state", pg_doc.get("state"), "estado operativo"),
        ("commercial_state", "commercialState", pg_doc.get("commercial_state"), "estado comercial"),
        ("number", "number", pg_doc.get("number"), "folio"),
        ("generationDate", "generationDate", (raw or {}).get("generationDate"), "auditoría sync"),
        ("modificationDate", "modificationDate", (raw or {}).get("modificationDate"), "frescura"),
    ]
    out: list[dict[str, Any]] = []
    for field, bkey, pg_val, impact in mapping:
        b_val = bsale_doc.get(bkey)
        # comparar numéricos con tolerancia
        bn, pn = _num(b_val), _num(pg_val)
        if bn is not None and pn is not None:
            match = abs(bn - pn) < 0.01
        else:
            match = str(b_val) == str(pg_val) if b_val is not None or pg_val is not None else True
        out.append(
            {
                "campo": field,
                "bsale_actual": b_val,
                "bsale_iso": _epoch_iso(b_val) if field.endswith("Date") else None,
                "postgresql": pg_val,
                "raw_data": (raw or {}).get(bkey),
                "coincide": match,
                "impacto": impact,
            }
        )
    return out


def _detail_diffs(
    bsale_items: list[dict[str, Any]],
    pg_details: list[dict[str, Any]],
) -> dict[str, Any]:
    by_bsale_id = {int(it["id"]): it for it in bsale_items if it.get("id") is not None}
    by_pg_id = {int(d["detail_id"]): d for d in pg_details if d.get("detail_id") is not None}

    only_bsale = sorted(set(by_bsale_id) - set(by_pg_id))
    only_pg = sorted(set(by_pg_id) - set(by_bsale_id))
    common = sorted(set(by_bsale_id) & set(by_pg_id))

    line_diffs: list[dict[str, Any]] = []
    for did in common:
        b = by_bsale_id[did]
        p = by_pg_id[did]
        variant = b.get("variant") or {}
        pairs = [
            ("quantity", b.get("quantity"), p.get("quantity"), "peso × qty / picking"),
            ("total_amount", b.get("totalAmount"), p.get("total_amount"), "totales OC"),
            ("net_amount", b.get("netAmount"), p.get("net_amount"), "neto"),
            ("total_unit_value", b.get("totalUnitValue"), p.get("total_unit_value"), "precio"),
            ("net_unit_value", b.get("netUnitValue"), p.get("net_unit_value"), "precio neto"),
            ("total_discount", b.get("totalDiscount"), p.get("total_discount"), "descuentos"),
            ("discount_percentage", b.get("discountPercentage"), p.get("discount_percentage"), "descuentos"),
            ("variant_id", variant.get("id"), p.get("variant_id"), "peso / catálogo"),
        ]
        for campo, bv, pv, impact in pairs:
            bn, pn = _num(bv), _num(pv)
            if bn is not None and pn is not None:
                match = abs(bn - pn) < 0.0001
            else:
                match = str(bv) == str(pv)
            if not match:
                line_diffs.append(
                    {
                        "detail_id": did,
                        "campo": campo,
                        "bsale_actual": bv,
                        "postgresql": pv,
                        "coincide": False,
                        "impacto": impact,
                    }
                )

    return {
        "bsale_line_count": len(bsale_items),
        "pg_line_count": len(pg_details),
        "only_in_bsale_detail_ids": only_bsale,
        "only_in_pg_detail_ids": only_pg,
        "field_mismatches": line_diffs,
        "bsale_details_hash": _details_hash(bsale_items),
        "pg_details_hash": _details_hash(
            [
                {
                    "id": d.get("detail_id"),
                    "lineNumber": d.get("line_number"),
                    "variant": {"id": d.get("variant_id")},
                    "quantity": d.get("quantity"),
                    "netUnitValue": d.get("net_unit_value"),
                    "totalUnitValue": d.get("total_unit_value"),
                    "netAmount": d.get("net_amount"),
                    "totalAmount": d.get("total_amount"),
                    "netDiscount": d.get("net_discount"),
                    "totalDiscount": d.get("total_discount"),
                    "discountPercentage": d.get("discount_percentage"),
                }
                for d in pg_details
            ]
        ),
    }


def _print_report(report: dict[str, Any]) -> None:
    print("=" * 72)
    print("DIAGNÓSTICO OC Bsale ↔ PostgreSQL (SOLO LECTURA)")
    print("=" * 72)
    meta = report["meta"]
    print(
        f"folio={meta.get('folio')} document_id={meta.get('document_id')} "
        f"company={meta.get('company_id')} office={meta.get('office_id')} "
        f"no_write={meta.get('no_write')}"
    )
    print(f"bsale_fetched_at={meta.get('bsale_fetched_at')}")
    print()
    print("--- Encabezado ---")
    for row in report["header_diff"]:
        flag = "OK" if row["coincide"] else "DIFF"
        print(
            f"[{flag}] {row['campo']}: bsale={row['bsale_actual']!r} "
            f"pg={row['postgresql']!r} impacto={row['impacto']}"
        )
    print()
    d = report["details_diff"]
    print("--- Detalles ---")
    print(
        f"líneas bsale={d['bsale_line_count']} pg={d['pg_line_count']} "
        f"hash_bsale={d['bsale_details_hash']} hash_pg={d['pg_details_hash']}"
    )
    if d["only_in_bsale_detail_ids"]:
        print(f"solo en Bsale detail_ids={d['only_in_bsale_detail_ids']}")
    if d["only_in_pg_detail_ids"]:
        print(f"solo en PG detail_ids={d['only_in_pg_detail_ids']}")
    for row in d["field_mismatches"]:
        print(
            f"[DIFF] detail_id={row['detail_id']} {row['campo']}: "
            f"bsale={row['bsale_actual']!r} pg={row['postgresql']!r} "
            f"impacto={row['impacto']}"
        )
    if not d["field_mismatches"] and not d["only_in_bsale_detail_ids"] and not d["only_in_pg_detail_ids"]:
        print("[OK] Todas las líneas coinciden en ids y campos comparados")
    print()
    print("--- Veredicto ---")
    print(report["verdict"])
    print("=" * 72)


def run(
    *,
    folio: int | None,
    document_id: int | None,
    company_id: int,
    office_id: int,
    no_write: bool,
) -> dict[str, Any]:
    if not no_write:
        raise SystemExit(
            "Este diagnóstico es solo lectura. Pase --no-write explícitamente."
        )
    if os.getenv("DIAGNOSE_REQUIRE_NO_WRITE", "").strip() in ("1", "true", "yes") and not no_write:
        raise SystemExit("--no-write requerido")

    token = read_bsale_token_from_env()
    if not token:
        raise SystemExit(
            "BSALE_TOKEN / BSALE_TOKEN_SPA no configuradas (no se imprime el valor)."
        )

    pg_doc, pg_details = _load_pg_oc(
        folio=folio,
        document_id=document_id,
        company_id=company_id,
        office_id=office_id,
    )
    if not pg_doc:
        raise SystemExit(
            f"OC no encontrada en PostgreSQL (folio={folio} document_id={document_id})"
        )

    did = int(pg_doc["document_id"])
    client = BsaleClient(token)
    # No loguear headers ni token
    logger.info("GET Bsale /documents/%s.json (token_present=yes len=%s)", did, len(token))
    bsale_doc = client.get(f"/documents/{did}.json")
    if not isinstance(bsale_doc, dict):
        raise SystemExit("Respuesta Bsale de documento inválida")
    bsale_details = _fetch_bsale_details(client, did)

    header_diff = _header_diffs(bsale_doc, pg_doc)
    details_diff = _detail_diffs(bsale_details, pg_details)

    header_ok = all(r["coincide"] for r in header_diff if r["campo"] != "modificationDate")
    # modificationDate null en PG raw vs valor en Bsale cuenta como drift de frescura
    mod_row = next((r for r in header_diff if r["campo"] == "modificationDate"), None)
    details_ok = (
        not details_diff["only_in_bsale_detail_ids"]
        and not details_diff["only_in_pg_detail_ids"]
        and not details_diff["field_mismatches"]
        and details_diff["bsale_details_hash"] == details_diff["pg_details_hash"]
    )

    if header_ok and details_ok:
        verdict = (
            "FRESCO: PostgreSQL coincide con Bsale actual en montos/líneas. "
            "Si el síntoma persiste, es catálogo/peso (variant_id vs barcode), no frescura."
        )
    elif not details_ok:
        verdict = (
            "STALE_DETAILS: Bsale tiene líneas/cantidades/precios distintos a PG. "
            "La OC necesita resync de details (replace) + invalidar peso."
        )
    else:
        verdict = (
            "STALE_HEADER: montos/estado del encabezado difieren; details alineados. "
            "Falta upsert del documento desde Bsale."
        )

    if mod_row and not mod_row["coincide"] and mod_row["bsale_actual"] is not None:
        verdict += (
            f" modificationDate Bsale={mod_row['bsale_actual']} "
            f"(iso={mod_row.get('bsale_iso')}) no está en raw_data PG."
        )

    report = {
        "meta": {
            "folio": pg_doc.get("number") or folio,
            "document_id": did,
            "company_id": company_id,
            "office_id": office_id,
            "no_write": True,
            "bsale_fetched_at": datetime.now(timezone.utc).isoformat(),
            "pg_updated_at": pg_doc.get("updated_at"),
        },
        "bsale_header_subset": {k: bsale_doc.get(k) for k in HEADER_KEYS},
        "header_diff": header_diff,
        "details_diff": details_diff,
        "verdict": verdict,
    }
    return report


def main(argv: list[str] | None = None) -> int:
    load_dotenv_if_available()
    logging.basicConfig(
        level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    p = argparse.ArgumentParser(description="Diagnóstico OC Bsale vs PG (solo lectura)")
    p.add_argument("--folio", type=int, default=None)
    p.add_argument("--document-id", type=int, default=None)
    p.add_argument("--company-id", type=int, default=3)
    p.add_argument("--office-id", type=int, default=1)
    p.add_argument(
        "--no-write",
        action="store_true",
        help="Obligatorio: garantiza que no se escribe en BD/API",
    )
    p.add_argument(
        "--json-out",
        type=str,
        default="",
        help="Ruta opcional para volcar el reporte JSON (sin secretos)",
    )
    args = p.parse_args(argv)
    if args.folio is None and args.document_id is None:
        p.error("Indique --folio o --document-id")

    try:
        report = run(
            folio=args.folio,
            document_id=args.document_id,
            company_id=args.company_id,
            office_id=args.office_id,
            no_write=args.no_write,
        )
    except SystemExit as e:
        print(str(e) or "error", file=sys.stderr)
        return 1
    except Exception as e:
        logger.exception("diagnose_oc failed")
        # Nunca incluir token en el mensaje
        msg = str(e)
        if "access_token" in msg.lower() or "bsale_token" in msg.lower():
            msg = "error de autenticación/red Bsale (detalle omitido)"
        print(f"ERROR: {msg}", file=sys.stderr)
        return 1

    _print_report(report)
    if args.json_out:
        path = args.json_out
        with open(path, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2, default=str)
        print(f"JSON escrito en {path} (sin secretos)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
