"""
Diagnóstico: todas las fuentes posibles de relación OC ↔ ventas en Bsale.

Uso:
  python -m backend.jobs.debug_full_bsalse_relationships 66080

Requiere BD (PG_*) y ``BSALE_TOKEN`` o ``BSALE_TOKEN_SPA``.
"""

from __future__ import annotations

import json
import sys
import time
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from backend.db import get_connection
from backend.services.distribuidora.bsale_client import BsaleClient
from backend.services.distribuidora.sync_related_service import OFFICE_ID
from backend.services.distribuidora.sync_service import _bsale_token, _utc_day_timestamp_bounds

DETAILS_LIMIT = 50
RELATED_LIMIT = 50
SALES_TYPES = frozenset({1, 6, 9})


def _log(msg: str) -> None:
    print(msg, flush=True)


def _safe_int(v: Any) -> int | None:
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _doc_type_label(raw: Any) -> str:
    if raw is None:
        return "?"
    if isinstance(raw, dict):
        i = raw.get("id")
        n = raw.get("name") or raw.get("code")
        return f"{i} ({n})" if n else str(i)
    return str(raw)


def _client_blob(doc: dict[str, Any]) -> Any:
    return doc.get("client")


def _client_id(doc: dict[str, Any]) -> int | None:
    c = doc.get("client")
    if isinstance(c, dict) and c.get("id") is not None:
        return _safe_int(c.get("id"))
    return _safe_int(doc.get("clientId"))


def _total_float(doc: dict[str, Any]) -> float | None:
    for k in ("totalAmount", "netAmount", "total"):
        v = doc.get(k)
        if v is None:
            continue
        try:
            return float(Decimal(str(v)))
        except Exception:
            continue
    return None


def _references_list(doc: dict[str, Any]) -> list[Any]:
    raw = doc.get("references")
    if raw is None:
        return []
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict):
        inner = raw.get("items")
        if isinstance(inner, list):
            return inner
    return []


def _item_doc_type_id(it: dict[str, Any]) -> int | None:
    nested = it.get("document")
    if isinstance(nested, dict):
        dt = nested.get("documentType") or nested.get("document_type")
        tid = _safe_int(dt.get("id")) if isinstance(dt, dict) else _coerce_type_scalar(dt)
        if tid is not None:
            return tid
    dt = it.get("documentType") or it.get("document_type")
    if isinstance(dt, dict):
        return _safe_int(dt.get("id"))
    return _coerce_type_scalar(dt)


def _coerce_type_scalar(dt: Any) -> int | None:
    return _safe_int(dt)


def _emission_utc_date_from_doc(doc: dict[str, Any]) -> date | None:
    raw = doc.get("emissionDate")
    if raw is None:
        return None
    try:
        return datetime.fromtimestamp(int(raw), tz=timezone.utc).date()
    except Exception:
        return None


def _fetch_all_details(client: BsaleClient, document_id: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
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
                rows.append(it)
        if len(items) < DETAILS_LIMIT:
            break
        offset += len(items)
        time.sleep(0.15)
    return rows


def _fetch_relateddetail_all(client: BsaleClient, detail_id: int) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
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
                out.append(it)
        if len(items) < RELATED_LIMIT:
            break
        offset += len(items)
        time.sleep(0.15)
    return out


def _related_item_log_fields(it: dict[str, Any]) -> dict[str, Any]:
    nested = it.get("document") if isinstance(it.get("document"), dict) else None
    src = nested if nested and nested.get("id") is not None else it
    return {
        "id": _safe_int(src.get("id")),
        "number": src.get("number"),
        "document_type": _doc_type_label(
            src.get("documentType") or src.get("document_type"),
        ),
        "total": _total_float(src) if isinstance(src, dict) else _total_float(it),
        "client": src.get("client") if isinstance(src, dict) else it.get("client"),
    }


def _ref_entry_ids_types(ref: dict[str, Any]) -> tuple[int | None, str]:
    nested = ref.get("document")
    if isinstance(nested, dict) and nested.get("id"):
        rid = _safe_int(nested.get("id"))
        dt = nested.get("documentType") or nested.get("document_type")
        return rid, _doc_type_label(dt)
    rid = _safe_int(ref.get("id") or ref.get("documentId"))
    dt = ref.get("documentType") or ref.get("document_type")
    return rid, _doc_type_label(dt)


def main() -> int:
    if len(sys.argv) < 2:
        _log("Uso: python -m backend.jobs.debug_full_bsalse_relationships <document_number>")
        return 2
    try:
        document_number = int(sys.argv[1])
    except ValueError:
        _log("document_number debe ser entero (ej. 66080)")
        return 2

    token = _bsale_token()
    if not token:
        _log("[ERROR] Sin token: defina BSALE_TOKEN o BSALE_TOKEN_SPA")
        return 1

    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT document_id FROM distribuidora.documents WHERE number = %s",
        (document_number,),
    )
    row = cur.fetchone()
    if not row or row[0] is None:
        cur.close()
        conn.close()
        _log(f"[ERROR] No hay documento en BD local con number={document_number}")
        return 1
    document_id = int(row[0])

    cur.execute(
        """
        SELECT emission_date, client_id, total_amount
        FROM distribuidora.documents
        WHERE document_id = %s
        """,
        (document_id,),
    )
    db_meta = cur.fetchone()
    cur.close()
    conn.close()

    client = BsaleClient(token)

    summary = {
        "details_total": 0,
        "relateddetail_items_total": 0,
        "references_total": 0,
        "heuristic_matches": 0,
    }

    # --- STEP A ---
    _log("[STEP A] GET /v1/documents/{document_id}.json")
    try:
        base = client.get(f"/documents/{document_id}.json")
    except Exception as e:
        _log(f"[STEP A] Error: {e}")
        return 1
    if not isinstance(base, dict):
        _log(f"[STEP A] Respuesta inesperada: {type(base)}")
        return 1

    _log(f"  number: {base.get('number')}")
    _log(f"  document_type: {_doc_type_label(base.get('documentType'))}")
    _log(f"  client: {json.dumps(_client_blob(base), ensure_ascii=False, default=str)}")
    _log(f"  total (totalAmount/netAmount): totalAmount={base.get('totalAmount')} netAmount={base.get('netAmount')}")
    refs_raw = base.get("references")
    _log(f"  references (completo): {json.dumps(refs_raw, ensure_ascii=False, default=str)}")

    oc_client_id = _client_id(base)
    oc_total = _total_float(base)
    em_day = _emission_utc_date_from_doc(base)
    if em_day is None and db_meta and db_meta[0]:
        try:
            em_day = db_meta[0].astimezone(timezone.utc).date()
            _log(f"  (emissionDate ausente en API; usando emission_date UTC desde BD: {em_day})")
        except Exception:
            em_day = None

    # --- STEP B ---
    _log("[STEP B] GET /v1/documents/{document_id}/details.json")
    try:
        detail_rows = _fetch_all_details(client, document_id)
    except Exception as e:
        _log(f"[STEP B] Error: {e}")
        detail_rows = []
    summary["details_total"] = len(detail_rows)
    _log(f"  cantidad de líneas: {len(detail_rows)}")
    detail_ids: list[int] = []
    for it in detail_rows:
        did = _safe_int(it.get("id"))
        if did is not None:
            detail_ids.append(did)
            _log(f"  detail_id={did}")
    detail_ids = list(dict.fromkeys(detail_ids))

    # --- STEP C ---
    _log("[STEP C] GET /v1/documents.json?relateddetailid=… (por cada detail_id)")
    related_accum: list[dict[str, Any]] = []
    for did in detail_ids:
        _log(f"  [relateddetailid={did}]")
        try:
            rel_items = _fetch_relateddetail_all(client, did)
        except Exception as e:
            _log(f"    error: {e}")
            continue
        _log(f"    cantidad de items (todas las páginas): {len(rel_items)}")
        summary["relateddetail_items_total"] += len(rel_items)
        for it in rel_items:
            f = _related_item_log_fields(it)
            _log(
                "    item: "
                + json.dumps(f, ensure_ascii=False, default=str),
            )
            related_accum.append(it)

    # --- STEP D (mismo documento base; listado explícito) ---
    _log('[STEP D] references desde documento base (item["references"])')
    ref_list = _references_list(base)
    summary["references_total"] = len(ref_list)
    _log(f"  cantidad: {len(ref_list)}")
    for ref in ref_list:
        if not isinstance(ref, dict):
            _log(f"  (entrada no dict): {ref!r}")
            continue
        rid, dt_label = _ref_entry_ids_types(ref)
        _log(f"  id={rid} document_type={dt_label}")

    # --- STEP E ---
    _log("[STEP E] GET /v1/documents.json?limit=50&emissiondaterange=… (heurística ventas)")
    matches: list[dict[str, Any]] = []
    if em_day is None:
        _log("  omitido: sin fecha de emisión (API ni BD)")
    elif oc_client_id is None or oc_total is None:
        _log(
            f"  omitido: falta client_id={oc_client_id} o total comparable={oc_total}",
        )
    else:
        end_day = em_day + timedelta(days=3)
        desde_ts, _ = _utc_day_timestamp_bounds(em_day)
        _, hasta_ts = _utc_day_timestamp_bounds(end_day)
        rng = f"[{desde_ts},{hasta_ts}]"
        _log(f"  emissiondaterange (UTC): {rng}  (desde {em_day} hasta +3 días)")
        try:
            data = client.get(
                "/documents.json",
                {
                    "limit": 50,
                    "offset": 0,
                    "emissiondaterange": rng,
                    "officeId": OFFICE_ID,
                },
            )
        except Exception as e:
            _log(f"  error: {e}")
            data = {}
        items = data.get("items") or []
        _log(f"  documentos devueltos por API (primer página): {len(items)}")
        tol = max(50.0, abs(float(oc_total)) * 0.01)
        for it in items:
            if not isinstance(it, dict):
                continue
            tid = _item_doc_type_id(it)
            if tid not in SALES_TYPES:
                continue
            if _safe_int(it.get("id")) == document_id:
                continue
            if _client_id(it) != oc_client_id:
                continue
            t = _total_float(it)
            if t is None:
                continue
            if abs(t - float(oc_total)) > tol:
                continue
            matches.append(
                {
                    "id": it.get("id"),
                    "number": it.get("number"),
                    "document_type": _doc_type_label(
                        it.get("documentType") or it.get("document_type"),
                    ),
                    "totalAmount": it.get("totalAmount"),
                    "client_id": _client_id(it),
                },
            )
        summary["heuristic_matches"] = len(matches)
        _log(f"  posibles matches (cliente + monto ~1% o mín. 50): {len(matches)}")
        for m in matches:
            _log("    " + json.dumps(m, ensure_ascii=False, default=str))

    # --- Resumen ---
    _log("")
    _log("========== RESUMEN ==========")
    _log(f"OC (number): {document_number}  document_id (BD/API): {document_id}")
    _log("")
    _log("DETAILS:")
    _log(f"  total: {summary['details_total']}")
    _log("")
    _log("RELATEDDETAIL:")
    _log(f"  total ítems encontrados (suma por líneas): {summary['relateddetail_items_total']}")
    _log("")
    _log("REFERENCES:")
    _log(f"  total encontrados: {summary['references_total']}")
    _log("")
    _log("MATCHES POR BÚSQUEDA:")
    _log(f"  total: {summary['heuristic_matches']}")
    _log("")
    _log("--- Interpretación rápida ---")
    has_refs = summary["references_total"] > 0
    has_rel = summary["relateddetail_items_total"] > 0
    if has_refs and has_rel:
        _log("Relaciones explícitas: references Y relateddetailid devolvieron datos.")
    elif has_refs:
        _log("Relaciones explícitas: principalmente en references (documento base).")
    elif has_rel:
        _log("Relaciones explícitas: principalmente en relateddetailid (por línea).")
    else:
        _log("Sin ítems en references ni en relateddetailid (en esta corrida).")
    if summary["heuristic_matches"] > 0 and not (has_refs or has_rel):
        _log("Solo la búsqueda heurística sugiere candidatos (inferencia, no enlace oficial).")
    elif summary["heuristic_matches"] > 0:
        _log("La búsqueda heurística añade posibles ventas aunque haya enlaces en API.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
