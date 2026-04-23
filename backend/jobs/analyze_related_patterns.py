"""
Analiza ``relateddetailid`` en las últimas 50 OC (tipo 33) para ver patrones de facturación.

Uso:
  python -m backend.jobs.analyze_related_patterns

Requiere BD (PG_*) y ``BSALE_TOKEN`` o ``BSALE_TOKEN_SPA``.
"""

from __future__ import annotations

import sys
import time
from decimal import Decimal
from typing import Any

from backend.db import get_connection
from backend.services.distribuidora.bsale_client import BsaleClient
from backend.services.distribuidora.sync_related_service import OFFICE_ID
from backend.services.distribuidora.sync_service import _bsale_token

DETAILS_LIMIT = 50
RELATED_LIMIT = 50
DOC_TYPE_OC = 33


def _log(msg: str) -> None:
    print(msg, flush=True)


def _safe_int(v: Any) -> int | None:
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


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


def _fetch_detail_ids(client: BsaleClient, document_id: int) -> list[int]:
    ids: list[int] = []
    offset = 0
    while True:
        data = client.get(
            f"/documents/{document_id}/details.json",
            {"limit": DETAILS_LIMIT, "offset": offset},
        )
        items = data.get("items") or []
        if not isinstance(items, list):
            break
        for it in items:
            if isinstance(it, dict):
                did = _safe_int(it.get("id"))
                if did is not None:
                    ids.append(did)
        if len(items) < DETAILS_LIMIT:
            break
        offset += len(items)
        time.sleep(0.12)
    return list(dict.fromkeys(ids))


def _fetch_related_types(client: BsaleClient, detail_id: int) -> set[int]:
    types: set[int] = set()
    offset = 0
    while True:
        data = client.get(
            "/documents.json",
            {
                "relateddetailid": detail_id,
                "limit": RELATED_LIMIT,
                "offset": offset,
                "officeId": OFFICE_ID,
            },
        )
        items = data.get("items") or []
        if not items:
            break
        for it in items:
            if isinstance(it, dict):
                tid = _item_doc_type_id(it)
                if tid is not None:
                    types.add(tid)
        if len(items) < RELATED_LIMIT:
            break
        offset += len(items)
        time.sleep(0.12)
    return types


def _classify(types: set[int]) -> str:
    if not types:
        return "NO FACTURADA"
    if types & {1, 6}:
        return "FACTURADA"
    if DOC_TYPE_OC in types:
        return "SOLO_33"
    return "OTRO"


def _bd_tipo_33_con_venta(
    cur,
    *,
    client_id: int | None,
    document_id: int,
    oc_total: Decimal | None,
) -> bool:
    if client_id is None or oc_total is None:
        return False
    try:
        oc_f = float(oc_total)
    except Exception:
        return False
    tol = max(50.0, abs(oc_f) * 0.01)
    cur.execute(
        """
        SELECT 1
        FROM distribuidora.documents d
        WHERE d.client_id = %s
          AND d.document_type_id IN (1, 6)
          AND d.document_id <> %s
          AND d.total_amount IS NOT NULL
          AND ABS(d.total_amount - %s) <= %s
        LIMIT 1
        """,
        (client_id, document_id, oc_total, Decimal(str(tol))),
    )
    return cur.fetchone() is not None


def main() -> int:
    token = _bsale_token()
    if not token:
        _log("[ERROR] Sin token: defina BSALE_TOKEN o BSALE_TOKEN_SPA")
        return 1

    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT document_id, number, client_id, total_amount
        FROM distribuidora.documents
        WHERE document_type_id = %s
        ORDER BY emission_date DESC NULLS LAST, document_id DESC
        LIMIT 50
        """,
        (DOC_TYPE_OC,),
    )
    rows = cur.fetchall()
    if not rows:
        cur.close()
        conn.close()
        _log("No hay órdenes tipo 33 en BD.")
        return 0

    client = BsaleClient(token)

    _log("")
    _log("OC      | tipos (relateddetailid) | clasificación | tipo_33→venta_BD")
    _log("--------+-------------------------+-----------------+------------------")

    n_con_venta_related = 0
    n_con_33_related = 0
    n_sin_relaciones = 0
    n_solo_33_con_match_bd = 0
    n_solo_33_sin_match_bd = 0
    n_label_facturada = 0
    n_label_no_facturada = 0
    n_label_solo_33 = 0
    n_label_otro = 0

    for document_id, number, client_id, total_amount in rows:
        did = int(document_id)
        num = _safe_int(number)
        if num is None:
            num = did
        types_acc: set[int] = set()
        try:
            for detail_id in _fetch_detail_ids(client, did):
                types_acc |= _fetch_related_types(client, detail_id)
                time.sleep(0.1)
        except Exception as e:
            _log(f"{num:<7} | (error API: {e}) | — | —")
            time.sleep(0.3)
            continue

        tipos_str = str(sorted(types_acc)) if types_acc else "[]"
        label = _classify(types_acc)
        if label == "FACTURADA":
            n_label_facturada += 1
        elif label == "NO FACTURADA":
            n_label_no_facturada += 1
        elif label == "SOLO_33":
            n_label_solo_33 += 1
        else:
            n_label_otro += 1

        if types_acc & {1, 6}:
            n_con_venta_related += 1
        if DOC_TYPE_OC in types_acc:
            n_con_33_related += 1
        if not types_acc:
            n_sin_relaciones += 1

        match_bd = "-"
        if DOC_TYPE_OC in types_acc:
            ok = _bd_tipo_33_con_venta(
                cur,
                client_id=_safe_int(client_id),
                document_id=did,
                oc_total=total_amount if total_amount is not None else None,
            )
            match_bd = "true" if ok else "false"
            if label == "SOLO_33":
                if ok:
                    n_solo_33_con_match_bd += 1
                else:
                    n_solo_33_sin_match_bd += 1

        _log(f"{num:<7} | {tipos_str:<23} | {label:<15} | {match_bd}")
        time.sleep(0.2)

    cur.close()
    conn.close()

    _log("")
    _log("========== RESUMEN ==========")
    _log(f"OC analizadas: {len(rows)}")
    _log(f"Con tipo 1 o 6 en relateddetailid (alguna línea): {n_con_venta_related}")
    _log(f"Con tipo 33 en relateddetailid: {n_con_33_related}")
    _log(f"Sin relaciones (sin ítems / sin tipos parseados): {n_sin_relaciones}")
    _log("")
    _log("Por clasificación en tabla:")
    _log(f"  FACTURADA: {n_label_facturada}")
    _log(f"  NO FACTURADA: {n_label_no_facturada}")
    _log(f"  SOLO_33: {n_label_solo_33}")
    _log(f"  OTRO: {n_label_otro}")
    _log("")
    _log("SOLO_33 en API + venta 1/6 en BD (mismo cliente, monto ~1% o mín. 50):")
    _log(f"  con match BD: {n_solo_33_con_match_bd}")
    _log(f"  sin match BD: {n_solo_33_sin_match_bd}")
    _log("")
    _log("--- Lectura ---")
    _log(
        "Si muchas FACTURADA con [1]/[6] en relateddetailid, ese endpoint refleja facturación.",
    )
    _log(
        "Si predominan SOLO_33 pero match_bd=true, el 33 suele ser eco de la OC y la venta "
        "no viene en relateddetailid (revisar references.json u otros flujos).",
    )
    _log(
        "Si SOLO_33 y match_bd=false, puede ser OC sin facturar o relación no expuesta por Bsale.",
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
