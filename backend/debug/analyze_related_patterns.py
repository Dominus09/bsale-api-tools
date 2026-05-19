"""
Analiza ``relateddetailid`` en las últimas 500 OC (tipo 33), con throttling y muestra acotada.

Uso:
  python -m backend.debug.analyze_related_patterns

Requiere BD (PG_*) y ``BSALE_TOKEN`` o ``BSALE_TOKEN_SPA``.
"""

from __future__ import annotations

import time
from decimal import Decimal
from typing import Any

from backend.db import get_connection
from backend.services.distribuidora.bsale_client import BsaleClient
from backend.services.distribuidora.bsale_params import merge_bsale_office_query
from backend.services.distribuidora.sync_related_service import OFFICE_ID
from backend.services.distribuidora.sync_service import _bsale_token

DETAILS_LIMIT = 50
RELATED_LIMIT = 50
DOC_TYPE_OC = 33
OC_SAMPLE_LIMIT = 500
DETAILS_MAX_PER_OC = 3
API_SLEEP_SEC = 0.1
DEBUG_EJEMPLOS = 10


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
    """GET Bsale + pausa fija para no saturar la API."""
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


def _fetch_first_detail_ids(client: BsaleClient, document_id: int, max_ids: int) -> list[int]:
    """Solo las primeras ``max_ids`` líneas (orden API: primera página, luego siguiente si hace falta)."""
    ids: list[int] = []
    offset = 0
    while len(ids) < max_ids:
        data = _get(
            client,
            f"/documents/{document_id}/details.json",
            {"limit": DETAILS_LIMIT, "offset": offset},
        )
        items = data.get("items") or []
        if not isinstance(items, list) or not items:
            break
        for it in items:
            if len(ids) >= max_ids:
                break
            if isinstance(it, dict):
                did = _safe_int(it.get("id"))
                if did is not None:
                    ids.append(did)
        if len(items) < DETAILS_LIMIT:
            break
        offset += len(items)
    return ids


def _fetch_related_types(
    client: BsaleClient,
    detail_id: int,
    *,
    stop_on_sale: bool,
) -> set[int]:
    types: set[int] = set()
    offset = 0
    while True:
        data = _get(
            client,
            "/documents.json",
            merge_bsale_office_query(
                {
                    "relateddetailid": detail_id,
                    "limit": RELATED_LIMIT,
                    "offset": offset,
                },
                OFFICE_ID,
            ),
        )
        items = data.get("items") or []
        if not items:
            break
        for it in items:
            if isinstance(it, dict):
                tid = _item_doc_type_id(it)
                if tid is not None:
                    types.add(tid)
        if stop_on_sale and types & {1, 6}:
            break
        if len(items) < RELATED_LIMIT:
            break
        offset += len(items)
    return types


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
        LIMIT %s
        """,
        (DOC_TYPE_OC, OC_SAMPLE_LIMIT),
    )
    rows = cur.fetchall()
    if not rows:
        cur.close()
        conn.close()
        _log("No hay órdenes tipo 33 en BD.")
        return 0

    client = BsaleClient(token)

    total_oc = 0
    facturadas_directo = 0
    solo_tipo_33 = 0
    sin_relacion = 0
    otras_relaciones = 0
    tipo_33_con_venta = 0
    tipo_33_sin_venta = 0

    ej_33_con_venta: list[tuple[int, int, list[int]]] = []
    ej_33_sin_venta: list[tuple[int, int, list[int]]] = []

    for document_id, number, client_id, total_amount in rows:
        did = int(document_id)
        num = _safe_int(number)
        if num is None:
            num = did
        total_oc += 1

        types_acc: set[int] = set()
        try:
            detail_ids = _fetch_first_detail_ids(client, did, DETAILS_MAX_PER_OC)
            for detail_id in detail_ids:
                types_acc |= _fetch_related_types(
                    client,
                    detail_id,
                    stop_on_sale=True,
                )
                if types_acc & {1, 6}:
                    break
        except Exception:
            types_acc = set()

        if types_acc & {1, 6}:
            facturadas_directo += 1
        elif DOC_TYPE_OC in types_acc:
            solo_tipo_33 += 1
        elif not types_acc:
            sin_relacion += 1
        else:
            otras_relaciones += 1

        if DOC_TYPE_OC in types_acc:
            ok = _bd_tipo_33_con_venta(
                cur,
                client_id=_safe_int(client_id),
                document_id=did,
                oc_total=total_amount if total_amount is not None else None,
            )
            if ok:
                tipo_33_con_venta += 1
                if len(ej_33_con_venta) < DEBUG_EJEMPLOS:
                    ej_33_con_venta.append((num, did, sorted(types_acc)))
            else:
                tipo_33_sin_venta += 1
                if len(ej_33_sin_venta) < DEBUG_EJEMPLOS:
                    ej_33_sin_venta.append((num, did, sorted(types_acc)))

    cur.close()
    conn.close()

    _log("")
    _log("======== RESULTADO ========")
    _log("")
    _log(f"Total OC analizadas: {total_oc}")
    _log("")
    _log(f"Facturadas (tipo 1/6): {facturadas_directo}")
    _log(f"Solo tipo 33: {solo_tipo_33}")
    _log(f"Sin relación: {sin_relacion}")
    if otras_relaciones:
        _log(f"Otras relaciones (sin 1/6/33): {otras_relaciones}")
    _log("")
    _log(f"Tipo 33 con venta: {tipo_33_con_venta}")
    _log(f"Tipo 33 sin venta: {tipo_33_sin_venta}")
    _log("")
    _log("--- Ejemplos (tipo 33 en API, hasta 10 c/u) ---")
    _log("OC con tipo 33 y venta en BD (cliente + monto ~1% o mín. 50):")
    if not ej_33_con_venta:
        _log("  (ninguno)")
    else:
        for n, did, tipos in ej_33_con_venta:
            _log(f"  number={n} document_id={did} tipos={tipos}")
    _log("OC con tipo 33 sin venta en BD:")
    if not ej_33_sin_venta:
        _log("  (ninguno)")
    else:
        for n, did, tipos in ej_33_sin_venta:
            _log(f"  number={n} document_id={did} tipos={tipos}")
    _log("")
    _log(
        "Lectura: si ``Facturadas (1/6)`` es bajo y ``Tipo 33 con venta`` es alto, "
        "la facturación suele estar en BD pero no en ``relateddetailid``.",
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
