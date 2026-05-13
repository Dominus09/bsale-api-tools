"""
Consolida las últimas OC (tipo 33) en un Excel para análisis: API Bsale + cruce con BD.

Ejecución:
  python -m backend.debug.export_oc_analysis_to_excel

Requiere PG_* y ``BSALE_TOKEN`` o ``BSALE_TOKEN_SPA`` (ver ``_bsale_token``).

Salida: ``oc_analysis.xlsx`` en el directorio de trabajo.
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

import pandas as pd

if __package__ is None or __package__ == "":
    # Permite ejecutar: python backend/debug/export_oc_analysis_to_excel.py
    sys.path.append(str(Path(__file__).resolve().parents[2]))

from backend.db import get_connection
from backend.services.distribuidora.bsale_client import BsaleClient
from backend.services.distribuidora.sync_related_service import OFFICE_ID

BSALE_TOKEN = os.getenv("BSALE_TOKEN")

if not BSALE_TOKEN:
    raise Exception("Falta BSALE_TOKEN en variables de entorno")

DOC_TYPE_OC = 33
OC_LIMIT = 50
DETAILS_PAGE = 50
MAX_DETAIL_IDS_FOR_RELATED = 3
RELATED_PAGE = 50
API_SLEEP_SEC = 0.1
OUT_XLSX = "oc_analysis.xlsx"


def _log(msg: str) -> None:
    print(msg, flush=True)


def _safe_int(v: Any) -> int | None:
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _get(client: BsaleClient, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    r = client.get(path, params)
    time.sleep(API_SLEEP_SEC)
    return r


def _item_doc_type_id(it: dict[str, Any]) -> int | None:
    nested = it.get("document")
    if isinstance(nested, dict):
        dt = nested.get("documentType") or nested.get("document_type")
        tid = _safe_int(dt.get("id")) if isinstance(dt, dict) else _safe_int(dt)
        if tid is not None:
            return tid
    dt = it.get("documentType") or it.get("document_type")
    if isinstance(dt, dict):
        return _safe_int(dt.get("id"))
    return _safe_int(dt)


def _to_decimal(v: Any) -> Decimal | None:
    if v is None:
        return None
    if isinstance(v, Decimal):
        return v
    try:
        return Decimal(str(v))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _count_all_details(client: BsaleClient, document_id: int) -> int:
    n = 0
    offset = 0
    while True:
        data = _get(
            client,
            f"/documents/{document_id}/details.json",
            {"limit": DETAILS_PAGE, "offset": offset},
        )
        items = data.get("items") or []
        if not items:
            break
        n += len(items)
        if len(items) < DETAILS_PAGE:
            break
        offset += len(items)
    return n


def _first_detail_ids(client: BsaleClient, document_id: int, max_ids: int) -> list[int]:
    ids: list[int] = []
    offset = 0
    while len(ids) < max_ids:
        data = _get(
            client,
            f"/documents/{document_id}/details.json",
            {"limit": DETAILS_PAGE, "offset": offset},
        )
        items = data.get("items") or []
        if not items:
            break
        for it in items:
            if len(ids) >= max_ids:
                break
            if isinstance(it, dict):
                did = _safe_int(it.get("id"))
                if did is not None:
                    ids.append(did)
        if len(items) < DETAILS_PAGE:
            break
        offset += len(items)
    return ids


def _related_types_and_items(
    client: BsaleClient, detail_id: int
) -> tuple[set[int], int]:
    types: set[int] = set()
    item_count = 0
    offset = 0
    while True:
        data = _get(
            client,
            "/documents.json",
            {
                "relateddetailid": detail_id,
                "limit": RELATED_PAGE,
                "offset": offset,
                "officeId": OFFICE_ID,
            },
        )
        items = data.get("items") or []
        if not items:
            break
        item_count += len(items)
        for it in items:
            if isinstance(it, dict):
                tid = _item_doc_type_id(it)
                if tid is not None:
                    types.add(tid)
        if len(items) < RELATED_PAGE:
            break
        offset += len(items)
    return types, item_count


def _references_count(client: BsaleClient, document_id: int) -> int:
    data = _get(client, f"/documents/{document_id}/references.json")
    items = data.get("items")
    if items is None:
        items = data.get("references") or []
    if not isinstance(items, list):
        return 0
    return len(items)


def _client_id_from_doc(doc: dict[str, Any]) -> int | None:
    c = doc.get("client")
    if isinstance(c, dict):
        return _safe_int(c.get("id"))
    return None


def _tiene_venta_bd(
    cur, *, client_id: int | None, oc_total: Decimal | None, oc_date: date | None
) -> bool:
    if client_id is None or oc_total is None or oc_date is None:
        return False
    cur.execute(
        """
        SELECT document_id
        FROM distribuidora.documents
        WHERE document_type_id IN (1, 6)
          AND client_id = %s
          AND total_amount IS NOT NULL
          AND ABS(total_amount - %s) <= GREATEST(ABS(%s) * 0.01, 50)
          AND emission_date::date BETWEEN %s::date
            AND (%s::date + INTERVAL '3 days')::date
        LIMIT 1
        """,
        (client_id, oc_total, oc_total, oc_date, oc_date),
    )
    return cur.fetchone() is not None


def run_export() -> int:
    print(f"🚀 Iniciando export de {OC_LIMIT} OC...")
    print(f"Token cargado: {'SI' if BSALE_TOKEN else 'NO'}")

    conn = get_connection()
    cur0 = conn.cursor()
    cur0.execute(
        """
        SELECT document_id, number, client_id, total_amount, emission_date
        FROM distribuidora.documents
        WHERE document_type_id = %s
        ORDER BY emission_date DESC NULLS LAST, document_id DESC
        LIMIT %s
        """,
        (DOC_TYPE_OC, OC_LIMIT),
    )
    ocs = cur0.fetchall()
    cur0.close()

    if not ocs:
        conn.close()
        _log("No hay OC (tipo 33) en distribuidora.documents.")
        return 0

    def _as_date(e: Any) -> date | None:
        if e is None:
            return None
        if isinstance(e, datetime):
            return e.date()
        if isinstance(e, date):
            return e
        return None

    ocs_parsed: list[tuple[int, Any, int | None, Any, date | None]] = []
    for r in ocs:
        e = r[4] if len(r) > 4 else None
        ocs_parsed.append(
            (int(r[0]), r[1], r[2] if r[2] is not None else None, r[3], _as_date(e))
        )

    client = BsaleClient(BSALE_TOKEN)
    rows: list[dict[str, Any]] = []
    qcur = conn.cursor()
    for document_id, number, client_id_db, total_db, emission_date in ocs_parsed:
        oc_num: Any = None
        cl_id: int | None = client_id_db
        total_oc: float | int | str | None = None
        details_count = 0
        rel_types: set[int] = set()
        rel_items = 0
        ref_c = 0
        has_sale = False

        try:
            doc = _get(client, f"/documents/{document_id}.json")
            oc_num = doc.get("number", number)
            cl_id = _client_id_from_doc(doc) or client_id_db
            ta = _to_decimal(doc.get("totalAmount") or doc.get("total_amount"))
            if ta is not None:
                total_oc = float(ta)
            elif total_db is not None:
                try:
                    total_oc = float(total_db)
                except (TypeError, ValueError, OverflowError):
                    total_oc = None

            details_count = _count_all_details(client, document_id)
            d_ids = _first_detail_ids(
                client, document_id, MAX_DETAIL_IDS_FOR_RELATED
            )
            for d_id in d_ids:
                ts, n_it = _related_types_and_items(client, d_id)
                rel_types |= ts
                rel_items += n_it
            ref_c = _references_count(client, document_id)
            oc_for_sql = _to_decimal(doc.get("totalAmount") or doc.get("total_amount") or total_db)
            has_sale = _tiene_venta_bd(
                qcur,
                client_id=cl_id,
                oc_total=oc_for_sql,
                oc_date=emission_date,
            )
        except Exception as e:
            print(f"❌ Error en OC {number}: {str(e)}")
            oc_num = number
            if total_db is not None:
                try:
                    total_oc = float(total_db)
                except (TypeError, ValueError, OverflowError):
                    total_oc = None
            has_sale = _tiene_venta_bd(
                qcur,
                client_id=client_id_db,
                oc_total=_to_decimal(total_db),
                oc_date=emission_date,
            )

        rows.append(
            {
                "OC_number": oc_num,
                "document_id": document_id,
                "client_id": cl_id,
                "total_oc": total_oc,
                "details_count": details_count,
                "related_types": ",".join(
                    str(x) for x in sorted(rel_types) if x is not None
                )
                if rel_types
                else "",
                "related_items_count": rel_items,
                "references_count": ref_c,
                "tiene_venta_bd": has_sale,
            }
        )

    qcur.close()
    conn.close()

    df = pd.DataFrame(rows)
    output_file = "oc_analysis.xlsx"
    df.to_excel(output_file, index=False)
    print(f"✅ Excel generado en: {output_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(run_export())
