"""
Diagnóstico SOLO LECTURA: compara una OC Bsale vs PostgreSQL.

Pensado para ejecutarse en el contenedor backend (donde existe BSALE_TOKEN)::

    python -m backend.jobs.diagnose_oc_bsale_vs_pg --folio 68199 --company-id 3 --office-id 1 --no-write

Opciones:
  --folio N          número/folio de la OC (document_type 33)
  --document-id ID   alternativa si se conoce el id Bsale/PG
  --company-id       default 3
  --office-id        default 1
  --no-write         obligatorio (solo lectura)

No imprime tokens ni secretos. No escribe en PostgreSQL ni en Bsale.
"""

from __future__ import annotations

import argparse
import copy
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
    "id",
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

# Valores que Bsale a veces entrega en documentos “fantasma”; el CLI no los inventa.
_SUSPICIOUS_NUMBER = 0
_SUSPICIOUS_STATE = 8888


def _json_safe(v: Any) -> Any:
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, datetime):
        return v.isoformat()
    if isinstance(v, dict):
        return {k: _json_safe(x) for k, x in v.items()}
    if isinstance(v, list):
        return [_json_safe(x) for x in v]
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


def _field_present(obj: dict[str, Any] | None, key: str) -> bool:
    return isinstance(obj, dict) and key in obj


def _get_raw_field(
    obj: dict[str, Any] | None,
    key: str,
    *,
    warnings: list[str],
    context: str,
) -> Any:
    """
    Lee un campo sin inventar defaults.
    Si falta la clave → None + warning de parsing (nunca 0/8888 sintéticos).
    """
    if not isinstance(obj, dict):
        warnings.append(f"{context}: objeto no es dict; campo {key!r} → null")
        return None
    if key not in obj:
        warnings.append(f"{context}: falta campo {key!r} → null (sin default)")
        return None
    return obj.get(key)


def _variant_id_from_bsale_detail(item: dict[str, Any]) -> Any:
    variant = item.get("variant")
    if isinstance(variant, dict) and "id" in variant:
        return variant.get("id")
    if "variant_id" in item:
        return item.get("variant_id")
    return None


def _line_summary_bsale(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "detail_id": item.get("id") if "id" in item else None,
        "variant_id": _variant_id_from_bsale_detail(item),
        "quantity": item.get("quantity") if "quantity" in item else None,
        "total_amount": item.get("totalAmount") if "totalAmount" in item else None,
    }


def _line_summary_pg(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "detail_id": row.get("detail_id") if "detail_id" in row else None,
        "variant_id": row.get("variant_id") if "variant_id" in row else None,
        "quantity": row.get("quantity") if "quantity" in row else None,
        "total_amount": row.get("total_amount") if "total_amount" in row else None,
    }


def _fetch_bsale_document(client: BsaleClient, document_id: int) -> dict[str, Any]:
    """GET /documents/{document_id}.json — copia profunda independiente."""
    path = f"/documents/{document_id}.json"
    logger.info("GET Bsale %s (token_present=yes)", path)
    raw = client.get(path)
    if not isinstance(raw, dict):
        raise SystemExit(
            f"Respuesta Bsale de documento inválida (tipo={type(raw).__name__}) "
            f"para {path}"
        )
    # Copia propia: el fetch de details no puede mutar este dict.
    return copy.deepcopy(raw)


def _fetch_bsale_details(client: BsaleClient, document_id: int) -> list[dict[str, Any]]:
    """GET /documents/{document_id}/details.json paginado — lista independiente."""
    items: list[dict[str, Any]] = []
    offset = 0
    limit = 50
    path = f"/documents/{document_id}/details.json"
    while True:
        logger.info(
            "GET Bsale %s limit=%s offset=%s (token_present=yes)",
            path,
            limit,
            offset,
        )
        data = client.get(path, {"limit": limit, "offset": offset})
        if isinstance(data, list):
            page = data
        elif isinstance(data, dict):
            page = data.get("items")
            if page is None:
                page = []
            if not isinstance(page, list):
                raise SystemExit(
                    f"Bsale details: 'items' no es lista (tipo={type(page).__name__})"
                )
        else:
            raise SystemExit(
                f"Respuesta Bsale details inválida (tipo={type(data).__name__})"
            )
        items.extend(copy.deepcopy(page))
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


def _values_match(bv: Any, pv: Any, *, tol: float = 0.01) -> bool:
    bn, pn = _num(bv), _num(pv)
    if bn is not None and pn is not None:
        return abs(bn - pn) < tol
    if bv is None and pv is None:
        return True
    return str(bv) == str(pv)


def _header_diffs(
    bsale_document: dict[str, Any],
    pg_document: dict[str, Any],
    *,
    warnings: list[str],
) -> list[dict[str, Any]]:
    raw = pg_document.get("raw_data") or {}
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except Exception:
            raw = {}
    if not isinstance(raw, dict):
        raw = {}

    mapping = [
        ("total_amount", "totalAmount", pg_document.get("total_amount"), "monto / camión"),
        ("net_amount", "netAmount", pg_document.get("net_amount"), "margen / reportes"),
        ("tax_amount", "taxAmount", pg_document.get("tax_amount"), "impuestos"),
        ("state", "state", pg_document.get("state"), "estado operativo"),
        ("commercial_state", "commercialState", pg_document.get("commercial_state"), "estado comercial"),
        ("number", "number", pg_document.get("number"), "folio"),
        ("generationDate", "generationDate", raw.get("generationDate"), "auditoría sync"),
        ("modificationDate", "modificationDate", raw.get("modificationDate"), "frescura"),
    ]
    out: list[dict[str, Any]] = []
    for field, bkey, pg_val, impact in mapping:
        b_val = _get_raw_field(
            bsale_document, bkey, warnings=warnings, context="bsale_document"
        )
        if field == "state" and b_val == _SUSPICIOUS_STATE:
            warnings.append(
                f"bsale_document.state={_SUSPICIOUS_STATE} (sentinel típico de documento "
                "fantasma en Bsale; valor crudo, no inventado por el CLI)"
            )
        if field == "number" and b_val == _SUSPICIOUS_NUMBER:
            warnings.append(
                f"bsale_document.number={_SUSPICIOUS_NUMBER} (sospechoso como folio; "
                "valor crudo de Bsale, no default del CLI)"
            )
        present = _field_present(bsale_document, bkey)
        match = _values_match(b_val, pg_val) if present or pg_val is not None else True
        if not present and pg_val is not None:
            match = False
        out.append(
            {
                "campo": field,
                "bsale_actual": b_val,
                "bsale_campo_presente": present,
                "bsale_iso": _epoch_iso(b_val) if field.endswith("Date") else None,
                "postgresql": pg_val,
                "raw_data": raw.get(bkey) if bkey in raw else None,
                "coincide": match,
                "impacto": impact,
            }
        )
    return out


def _detail_diffs(
    bsale_details: list[dict[str, Any]],
    pg_details: list[dict[str, Any]],
) -> dict[str, Any]:
    by_bsale_id = {
        int(it["id"]): it for it in bsale_details if it.get("id") is not None
    }
    by_pg_id = {
        int(d["detail_id"]): d for d in pg_details if d.get("detail_id") is not None
    }

    only_bsale = sorted(set(by_bsale_id) - set(by_pg_id))
    only_pg = sorted(set(by_pg_id) - set(by_bsale_id))
    common = sorted(set(by_bsale_id) & set(by_pg_id))

    line_diffs: list[dict[str, Any]] = []
    line_comparisons: list[dict[str, Any]] = []
    for did in common:
        b = by_bsale_id[did]
        p = by_pg_id[did]
        b_summary = _line_summary_bsale(b)
        p_summary = _line_summary_pg(p)
        pairs = [
            ("quantity", b_summary["quantity"], p_summary["quantity"], "peso × qty / picking"),
            ("total_amount", b_summary["total_amount"], p_summary["total_amount"], "totales OC"),
            (
                "net_amount",
                b.get("netAmount") if "netAmount" in b else None,
                p.get("net_amount"),
                "neto",
            ),
            (
                "total_unit_value",
                b.get("totalUnitValue") if "totalUnitValue" in b else None,
                p.get("total_unit_value"),
                "precio",
            ),
            (
                "net_unit_value",
                b.get("netUnitValue") if "netUnitValue" in b else None,
                p.get("net_unit_value"),
                "precio neto",
            ),
            (
                "total_discount",
                b.get("totalDiscount") if "totalDiscount" in b else None,
                p.get("total_discount"),
                "descuentos",
            ),
            (
                "discount_percentage",
                b.get("discountPercentage") if "discountPercentage" in b else None,
                p.get("discount_percentage"),
                "descuentos",
            ),
            ("variant_id", b_summary["variant_id"], p_summary["variant_id"], "peso / catálogo"),
        ]
        line_ok = True
        for campo, bv, pv, impact in pairs:
            match = _values_match(bv, pv, tol=0.0001)
            if not match:
                line_ok = False
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
        line_comparisons.append(
            {
                "detail_id": did,
                "bsale": b_summary,
                "postgresql": p_summary,
                "coincide": line_ok,
            }
        )

    lines_match = (
        not only_bsale
        and not only_pg
        and not line_diffs
        and len(bsale_details) == len(pg_details)
    )

    return {
        "bsale_line_count": len(bsale_details),
        "pg_line_count": len(pg_details),
        "only_in_bsale_detail_ids": only_bsale,
        "only_in_pg_detail_ids": only_pg,
        "field_mismatches": line_diffs,
        "line_comparisons": line_comparisons,
        "bsale_lines": [_line_summary_bsale(it) for it in bsale_details],
        "pg_lines": [_line_summary_pg(d) for d in pg_details],
        "lines_match": lines_match,
    }


def _lookup_bsale_ids_by_folio(
    client: BsaleClient,
    *,
    folio: int,
    office_id: int,
) -> list[dict[str, Any]]:
    """Solo lectura: lista corta por folio para enriquecer mensajes de abort."""
    data = client.get(
        "/documents.json",
        {"number": folio, "officeid": office_id, "limit": 10, "offset": 0},
    )
    items = data.get("items") if isinstance(data, dict) else None
    if not isinstance(items, list):
        return []
    out: list[dict[str, Any]] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        out.append(
            {
                "id": it.get("id") if "id" in it else None,
                "number": it.get("number") if "number" in it else None,
                "state": it.get("state") if "state" in it else None,
                "totalAmount": it.get("totalAmount") if "totalAmount" in it else None,
                "generationDate": it.get("generationDate") if "generationDate" in it else None,
            }
        )
    return out


def _assert_bsale_folio(
    *,
    bsale_document: dict[str, Any],
    document_id: int,
    expected_folio: int,
    warnings: list[str],
    client: BsaleClient | None = None,
    office_id: int | None = None,
) -> None:
    """Aborta si el folio Bsale no es el esperado (p. ej. number=0 fantasma)."""
    bsale_id = _get_raw_field(
        bsale_document, "id", warnings=warnings, context="bsale_document"
    )
    bsale_number = _get_raw_field(
        bsale_document, "number", warnings=warnings, context="bsale_document"
    )
    bsale_state = _get_raw_field(
        bsale_document, "state", warnings=warnings, context="bsale_document"
    )
    bsale_total = _get_raw_field(
        bsale_document, "totalAmount", warnings=warnings, context="bsale_document"
    )

    if bsale_id is not None:
        try:
            if int(bsale_id) != int(document_id):
                raise SystemExit(
                    f"ABORT: GET /documents/{document_id}.json devolvió id={bsale_id!r} "
                    f"(no coincide con document_id solicitado)."
                )
        except (TypeError, ValueError):
            raise SystemExit(
                f"ABORT: id Bsale no numérico: {bsale_id!r}"
            ) from None

    folio_ok = False
    try:
        folio_ok = bsale_number is not None and int(bsale_number) == int(expected_folio)
    except (TypeError, ValueError):
        folio_ok = False

    if not folio_ok:
        hint = ""
        if client is not None and office_id is not None:
            try:
                by_folio = _lookup_bsale_ids_by_folio(
                    client, folio=expected_folio, office_id=office_id
                )
                hint = f"\n  cross-check GET /documents.json?number={expected_folio}&officeid={office_id}: {by_folio!r}"
            except Exception as e:
                hint = (
                    f"\n  cross-check por folio falló "
                    f"(sin secretos): {type(e).__name__}"
                )
        raise SystemExit(
            "ABORT: folio Bsale inválido para este diagnóstico.\n"
            f"  request: GET /documents/{document_id}.json\n"
            f"  esperado number={expected_folio}\n"
            f"  recibido id={bsale_id!r} number={bsale_number!r} "
            f"state={bsale_state!r} totalAmount={bsale_total!r}\n"
            "  El CLI no sustituye number=0 ni state=8888. "
            "Si number es 0/null, el document_id de PG puede apuntar a un "
            "documento fantasma en Bsale."
            f"{hint}"
        )


def _build_verdict(
    *,
    header_diff: list[dict[str, Any]],
    details_diff: dict[str, Any],
) -> str:
    """Veredicto solo por diferencias campo a campo (sin hash)."""
    header_mismatches = [r for r in header_diff if not r["coincide"]]
    details_ok = bool(details_diff.get("lines_match"))

    if not header_mismatches and details_ok:
        return (
            "FRESCO: PostgreSQL coincide con Bsale en encabezado y líneas "
            "comparadas campo a campo."
        )
    if not details_ok:
        return (
            "STALE_DETAILS: hay diferencias reales en líneas "
            "(ids distintos, cantidad/precio/variant_id, o conteo). "
            "No se declara STALE_DETAILS si todas las líneas coinciden."
        )
    return (
        "STALE_HEADER: montos/estado/fechas del encabezado difieren; "
        "líneas alineadas campo a campo."
    )


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
    print(f"bsale_request_document={meta.get('bsale_request_document')}")
    print(f"bsale_request_details={meta.get('bsale_request_details')}")
    print(f"bsale_fetched_at={meta.get('bsale_fetched_at')}")
    print()

    warns = report.get("parsing_warnings") or []
    if warns:
        print("--- Advertencias de parsing ---")
        for w in warns:
            print(f"[WARN] {w}")
        print()

    print("--- Objetos (separados) ---")
    bd = report["bsale_document"]
    print(
        "bsale_document: "
        f"id={bd.get('id')!r} number={bd.get('number')!r} state={bd.get('state')!r} "
        f"totalAmount={bd.get('totalAmount')!r} netAmount={bd.get('netAmount')!r} "
        f"taxAmount={bd.get('taxAmount')!r} generationDate={bd.get('generationDate')!r}"
    )
    pd = report["pg_document"]
    print(
        "pg_document: "
        f"document_id={pd.get('document_id')!r} number={pd.get('number')!r} "
        f"state={pd.get('state')!r} total_amount={pd.get('total_amount')!r} "
        f"net_amount={pd.get('net_amount')!r} tax_amount={pd.get('tax_amount')!r}"
    )
    print()

    print("--- Encabezado (campo a campo) ---")
    for row in report["header_diff"]:
        flag = "OK" if row["coincide"] else "DIFF"
        print(
            f"[{flag}] {row['campo']}: bsale={row['bsale_actual']!r} "
            f"pg={row['postgresql']!r} presente_bsale={row['bsale_campo_presente']} "
            f"impacto={row['impacto']}"
        )
    print()

    d = report["details_diff"]
    print("--- Líneas (quantity / total / variant_id) ---")
    print(f"bsale_details ({d['bsale_line_count']}):")
    for line in d["bsale_lines"]:
        print(
            f"  detail_id={line['detail_id']!r} variant_id={line['variant_id']!r} "
            f"quantity={line['quantity']!r} total_amount={line['total_amount']!r}"
        )
    print(f"pg_details ({d['pg_line_count']}):")
    for line in d["pg_lines"]:
        print(
            f"  detail_id={line['detail_id']!r} variant_id={line['variant_id']!r} "
            f"quantity={line['quantity']!r} total_amount={line['total_amount']!r}"
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
    if d["lines_match"]:
        print("[OK] Todas las líneas coinciden campo a campo")
    else:
        print("[DIFF] Las líneas NO coinciden (ver mismatches / ids)")
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

    pg_document, pg_details = _load_pg_oc(
        folio=folio,
        document_id=document_id,
        company_id=company_id,
        office_id=office_id,
    )
    if not pg_document:
        raise SystemExit(
            f"OC no encontrada en PostgreSQL (folio={folio} document_id={document_id})"
        )

    # Copia independiente del lado PG.
    pg_document = copy.deepcopy(pg_document)
    pg_details = copy.deepcopy(pg_details)

    did = int(pg_document["document_id"])
    expected_folio = folio if folio is not None else pg_document.get("number")
    if expected_folio is None:
        raise SystemExit(
            "No hay folio esperado (--folio ni pg_document.number); abortando."
        )
    try:
        expected_folio = int(expected_folio)
    except (TypeError, ValueError) as e:
        raise SystemExit(f"Folio esperado no numérico: {expected_folio!r}") from e

    from backend.utils.bsale_document_ids import (
        ids_differ,
        resolve_bsale_source_document_id,
    )

    raw = pg_document.get("raw_data") or {}
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except Exception:
            raw = {}
    if not isinstance(raw, dict):
        raw = {}
    bsale_source_id = resolve_bsale_source_document_id(
        local_document_id=did,
        raw_document=raw if raw.get("id") is not None else None,
        raw_data_id=raw.get("id"),
    )
    if ids_differ(did, bsale_source_id):
        parsing_warnings_pre = [
            f"local_document_id={did} ≠ bsale_source_document_id={bsale_source_id}; "
            f"GET Bsale usará {bsale_source_id}"
        ]
    else:
        parsing_warnings_pre = []

    client = BsaleClient(token)
    parsing_warnings: list[str] = list(parsing_warnings_pre)

    # 1) Documento Bsale por source id (no por PK local tras reemisión).
    bsale_document = _fetch_bsale_document(client, bsale_source_id)
    bsale_document_id_after_doc = id(bsale_document)

    _assert_bsale_folio(
        bsale_document=bsale_document,
        document_id=bsale_source_id,
        expected_folio=expected_folio,
        warnings=parsing_warnings,
        client=client,
        office_id=office_id,
    )

    # 2) Details desde el mismo source id; persistencia/comparación vs PG local.
    bsale_details = _fetch_bsale_details(client, bsale_source_id)
    if id(bsale_document) != bsale_document_id_after_doc:
        raise SystemExit(
            "ABORT interno: bsale_document fue reemplazado tras consultar details."
        )
    # Defensa extra: details no debe ser el mismo objeto que el documento.
    if bsale_details is bsale_document:  # type: ignore[comparison-overlap]
        raise SystemExit(
            "ABORT interno: bsale_details alias de bsale_document."
        )

    header_diff = _header_diffs(
        bsale_document, pg_document, warnings=parsing_warnings
    )
    details_diff = _detail_diffs(bsale_details, pg_details)
    verdict = _build_verdict(header_diff=header_diff, details_diff=details_diff)

    # Subset de encabezado Bsale solo con claves presentes (sin inventar).
    bsale_header_subset: dict[str, Any] = {}
    for k in HEADER_KEYS:
        if k in bsale_document:
            bsale_header_subset[k] = bsale_document[k]
        else:
            bsale_header_subset[k] = None
            parsing_warnings.append(
                f"bsale_document: falta campo {k!r} en subset → null"
            )

    report = {
        "meta": {
            "folio": expected_folio,
            "document_id": did,
            "local_document_id": did,
            "bsale_source_document_id": bsale_source_id,
            "ids_differ": ids_differ(did, bsale_source_id),
            "company_id": company_id,
            "office_id": office_id,
            "no_write": True,
            "bsale_request_document": f"/documents/{bsale_source_id}.json",
            "bsale_request_details": f"/documents/{bsale_source_id}/details.json",
            "bsale_fetched_at": datetime.now(timezone.utc).isoformat(),
            "pg_updated_at": pg_document.get("updated_at"),
        },
        "parsing_warnings": parsing_warnings,
        "bsale_document": {k: bsale_document.get(k) for k in HEADER_KEYS},
        "bsale_details": [_line_summary_bsale(it) for it in bsale_details],
        "pg_document": {
            "document_id": pg_document.get("document_id"),
            "number": pg_document.get("number"),
            "total_amount": pg_document.get("total_amount"),
            "net_amount": pg_document.get("net_amount"),
            "tax_amount": pg_document.get("tax_amount"),
            "state": pg_document.get("state"),
            "commercial_state": pg_document.get("commercial_state"),
            "generation_date": pg_document.get("generation_date"),
            "updated_at": pg_document.get("updated_at"),
        },
        "pg_details": [_line_summary_pg(d) for d in pg_details],
        "bsale_header_subset": bsale_header_subset,
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
        code = e.code if isinstance(e.code, int) else 1
        msg = e.args[0] if e.args else (str(e) if e.code is not None else "error")
        if isinstance(msg, int):
            return msg
        print(str(msg) or "error", file=sys.stderr)
        return code if isinstance(code, int) and code != 0 else 1
    except Exception as e:
        logger.exception("diagnose_oc failed")
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
