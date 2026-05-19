#!/usr/bin/env python3
"""
Análisis de relaciones reales OC (tipo 33) ↔ documentos vía ``relateddetailid``.

Solo usa: ``/documents.json`` (listado + relateddetailid), ``/documents/{id}.json``,
``/documents/{id}/details.json``. Sin PostgreSQL ni FastAPI.
"""

from __future__ import annotations

import sys
import time
from collections.abc import Iterator
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from backend.services.distribuidora.bsale_params import merge_bsale_office_query
from backend.utils.bsale_token_env import require_bsale_token

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------

OFFICE_ID = 1

DATE_FROM = "2026-05-01"
DATE_TO = "2026-05-11"

OC_DOCUMENT_TYPE_ID = 33

BASE_BSALE = "https://api.bsale.io/v1"
LIST_LIMIT = 50
MAX_LIST_PAGES_PER_DAY = 120
DETAILS_LIMIT = 50
MAX_DETAIL_PAGES = 40
RELATED_LIST_LIMIT = 50
MAX_RELATED_PAGES = 25
TIMEOUT = (10, 30)
SLEEP_SEC = 0.15

EXPORT_XLSX = "exports/purchase_orders_relationships.xlsx"


def _die(msg: str, code: int = 1) -> None:
    print(f"[analyze_purchase_orders_relationships] ERROR: {msg}", file=sys.stderr)
    raise SystemExit(code)


def _token() -> str:
    return require_bsale_token(label="analyze_purchase_orders_relationships")


def _parse_date(s: str, label: str) -> date:
    try:
        return datetime.strptime(s.strip(), "%Y-%m-%d").date()
    except ValueError as e:
        raise SystemExit(
            f"[analyze_purchase_orders_relationships] {label} inválida: {s!r}"
        ) from e


def _utc_day_epoch_bounds(d: date) -> tuple[int, int]:
    start = datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=timezone.utc)
    end_ts = int((start + timedelta(days=1)).timestamp()) - 1
    return int(start.timestamp()), end_ts


def _iter_dates(d0: date, d1: date) -> Iterator[date]:
    d = d0
    while d <= d1:
        yield d
        d += timedelta(days=1)


def _safe_int(v: Any) -> int | None:
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _document_type_id(doc: dict[str, Any]) -> int | None:
    dt = doc.get("document_type") or doc.get("documentType")
    if not isinstance(dt, dict):
        return None
    return _safe_int(dt.get("id"))


def _document_root(payload: dict[str, Any]) -> dict[str, Any]:
    d = payload.get("document")
    if isinstance(d, dict) and d.get("id") is not None:
        return d
    return payload


def _get_json(
    session: requests.Session,
    token: str,
    path: str,
    params: dict[str, Any] | None = None,
    *,
    allow_404: bool = False,
) -> tuple[int, Any]:
    url = path if path.startswith("http") else f"{BASE_BSALE}{path}"
    r = session.get(
        url,
        headers={"access_token": token},
        params=params or {},
        timeout=TIMEOUT,
    )
    if r.status_code == 401:
        _die("401 Unauthorized: token inválido o expirado.")
    if r.status_code == 404 and allow_404:
        return 404, None
    if not (200 <= r.status_code < 300):
        return r.status_code, {"_http_error": True, "status": r.status_code}
    try:
        return r.status_code, r.json()
    except ValueError:
        return r.status_code, {"_parse_error": True}


def _fetch_oc_document(
    session: requests.Session, token: str, doc_id: int
) -> dict[str, Any] | None:
    st, data = _get_json(session, token, f"/documents/{doc_id}.json", None)
    time.sleep(SLEEP_SEC)
    if st != 200 or not isinstance(data, dict):
        return None
    return _document_root(data)


def _fetch_all_detail_lines(
    session: requests.Session,
    token: str,
    doc_id: int,
) -> list[dict[str, Any]]:
    lines: list[dict[str, Any]] = []
    offset = 0
    for _ in range(MAX_DETAIL_PAGES):
        st, body = _get_json(
            session,
            token,
            f"/documents/{doc_id}/details.json",
            {"limit": DETAILS_LIMIT, "offset": offset},
            allow_404=True,
        )
        time.sleep(SLEEP_SEC)
        if st == 404 or not isinstance(body, dict):
            break
        items = body.get("items") or []
        if not items:
            break
        for it in items:
            if isinstance(it, dict):
                lines.append(it)
        offset += len(items)
        if len(items) < DETAILS_LIMIT:
            break
    return lines


def _fetch_related_by_detail_id(
    session: requests.Session,
    token: str,
    detail_id: int,
) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    offset = 0
    for _ in range(MAX_RELATED_PAGES):
        st, data = _get_json(
            session,
            token,
            "/documents.json",
            merge_bsale_office_query(
                {
                    "relateddetailid": detail_id,
                    "limit": RELATED_LIST_LIMIT,
                    "offset": offset,
                },
                OFFICE_ID,
            ),
        )
        time.sleep(SLEEP_SEC)
        if st != 200 or not isinstance(data, dict):
            break
        items = data.get("items") or []
        for it in items:
            if isinstance(it, dict):
                merged.append(it)
        if len(items) < RELATED_LIST_LIMIT:
            break
        offset += len(items)
    return merged


def _client_id(doc: dict[str, Any]) -> int | None:
    c = doc.get("client") or doc.get("cliente")
    if isinstance(c, dict):
        return _safe_int(c.get("id"))
    return None


def _detail_unlinked_fields(line: dict[str, Any]) -> tuple[str, Any, Any]:
    variant = line.get("variant")
    vdesc = ""
    if isinstance(variant, dict):
        vdesc = str(variant.get("description") or "")
    desc = line.get("description") or line.get("name") or vdesc or ""
    qty = line.get("quantity")
    total = line.get("totalAmount")
    if total is None:
        total = line.get("netAmount")
    return (str(desc)[:800], qty, total)


def _classify_oc(
    details_count: int,
    related_detail_lines: int,
) -> str:
    if details_count == 0:
        return "NOT_RELATED"
    if related_detail_lines == 0:
        return "NOT_RELATED"
    if related_detail_lines == details_count:
        return "FULLY_RELATED"
    return "PARTIALLY_RELATED"


def _list_ocs_type_33(
    session: requests.Session,
    token: str,
    d0: date,
    d1: date,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for day in _iter_dates(d0, d1):
        start_ts, end_ts = _utc_day_epoch_bounds(day)
        offset = 0
        for _ in range(MAX_LIST_PAGES_PER_DAY):
            st, data = _get_json(
                session,
                token,
                "/documents.json",
                merge_bsale_office_query(
                    {
                        "limit": LIST_LIMIT,
                        "offset": offset,
                        "emissiondaterange": f"[{start_ts},{end_ts}]",
                    },
                    OFFICE_ID,
                ),
            )
            time.sleep(SLEEP_SEC)
            if st != 200 or not isinstance(data, dict):
                break
            items = data.get("items") or []
            if not items:
                break
            for doc in items:
                if not isinstance(doc, dict):
                    continue
                oid = _safe_int((doc.get("office") or {}).get("id"))
                if oid != OFFICE_ID:
                    continue
                if _document_type_id(doc) == OC_DOCUMENT_TYPE_ID:
                    out.append(doc)
            offset += len(items)
            if len(items) < LIST_LIMIT:
                break
    return out


def main() -> None:
    token = _token()
    d0 = _parse_date(DATE_FROM, "DATE_FROM")
    d1 = _parse_date(DATE_TO, "DATE_TO")
    if d1 < d0:
        _die("DATE_TO debe ser >= DATE_FROM")

    repo_root = Path(__file__).resolve().parents[2]
    out_xlsx = repo_root / EXPORT_XLSX

    session = requests.Session()

    rows_po: list[dict[str, Any]] = []
    rows_rel: list[dict[str, Any]] = []
    rows_unlinked: list[dict[str, Any]] = []

    stubs = _list_ocs_type_33(session, token, d0, d1)

    for stub in stubs:
        oc_id = _safe_int(stub.get("id"))
        if oc_id is None:
            continue
        oc_num = stub.get("number")
        root = _fetch_oc_document(session, token, oc_id)
        if root is None:
            print(
                f"[analyze_oc] oc_id={oc_id} skip (no documento)",
                flush=True,
            )
            continue
        em = root.get("emissionDate") or stub.get("emissionDate")
        tam = root.get("totalAmount")
        if tam is None:
            tam = stub.get("totalAmount")

        lines = _fetch_all_detail_lines(session, token, oc_id)
        lines_with_id = [ln for ln in lines if _safe_int(ln.get("id")) is not None]
        n_det = len(lines_with_id)
        detail_has_related: dict[int, bool] = {}
        total_match_rows = 0

        for line in lines_with_id:
            did = _safe_int(line.get("id"))
            if did is None:
                continue
            raw_rel = _fetch_related_by_detail_id(session, token, did)
            filtered = [
                x
                for x in raw_rel
                if isinstance(x, dict) and _safe_int(x.get("id")) != oc_id
            ]
            has = len(filtered) > 0
            detail_has_related[did] = has
            total_match_rows += len(filtered)
            for rel in filtered:
                rid = _safe_int(rel.get("id"))
                rows_rel.append(
                    {
                        "oc_document_id": oc_id,
                        "oc_number": oc_num,
                        "detail_id": did,
                        "related_document_id": rid,
                        "related_document_type_id": _document_type_id(rel),
                        "related_document_number": rel.get("number"),
                        "related_totalAmount": rel.get("totalAmount"),
                        "related_client_id": _client_id(rel),
                        "related_emissionDate": rel.get("emissionDate")
                        or rel.get("emission_date"),
                    }
                )
            if not has:
                desc, qty, lt = _detail_unlinked_fields(line)
                rows_unlinked.append(
                    {
                        "oc_document_id": oc_id,
                        "oc_number": oc_num,
                        "detail_id": did,
                        "product_description": desc,
                        "quantity": qty,
                        "totalAmount": lt,
                    }
                )

        n_with = sum(1 for v in detail_has_related.values() if v)
        status = _classify_oc(n_det, n_with)

        rows_po.append(
            {
                "oc_document_id": oc_id,
                "oc_number": oc_num,
                "emissionDate": em,
                "totalAmount": tam,
                "details_count": n_det,
                "related_details_count": n_with,
                "relationship_status": status,
            }
        )

        print(
            f"[analyze_oc] oc_id={oc_id} number={oc_num} details={n_det} "
            f"related_lines={n_with} match_rows={total_match_rows} status={status}",
            flush=True,
        )

    # --- Summary ---
    fully = sum(1 for r in rows_po if r["relationship_status"] == "FULLY_RELATED")
    partial = sum(1 for r in rows_po if r["relationship_status"] == "PARTIALLY_RELATED")
    notrel = sum(1 for r in rows_po if r["relationship_status"] == "NOT_RELATED")
    total_details = sum(int(r["details_count"] or 0) for r in rows_po)
    related_detail_lines = sum(int(r["related_details_count"] or 0) for r in rows_po)
    unlinked_count = len(rows_unlinked)
    sum_oc_amt = 0.0
    for r in rows_po:
        v = r.get("totalAmount")
        if v is not None:
            try:
                sum_oc_amt += float(v)
            except (TypeError, ValueError):
                pass

    sum_rel_unique: dict[int, float] = {}
    for r in rows_rel:
        rid = r.get("related_document_id")
        if rid is None:
            continue
        ta = r.get("related_totalAmount")
        if rid not in sum_rel_unique and ta is not None:
            try:
                sum_rel_unique[int(rid)] = float(ta)
            except (TypeError, ValueError):
                sum_rel_unique[int(rid)] = 0.0
    sum_related_amount = sum(sum_rel_unique.values())

    summary_rows = [
        {"metric": "cantidad_OCs", "value": len(rows_po)},
        {"metric": "fully_related", "value": fully},
        {"metric": "partially_related", "value": partial},
        {"metric": "not_related", "value": notrel},
        {"metric": "cantidad_total_details", "value": total_details},
        {"metric": "cantidad_detail_lines_con_match", "value": related_detail_lines},
        {"metric": "cantidad_unlinked_detail_lines", "value": unlinked_count},
        {"metric": "totalAmount_OCs_sum", "value": round(sum_oc_amt, 2)},
        {
            "metric": "totalAmount_relacionado_docs_unicos",
            "value": round(sum_related_amount, 2),
        },
    ]

    df_po = pd.DataFrame(rows_po)
    df_rel = pd.DataFrame(rows_rel)
    df_unl = pd.DataFrame(rows_unlinked)
    df_sum = pd.DataFrame(summary_rows)

    out_xlsx.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
        df_po.to_excel(writer, sheet_name="purchase_orders", index=False)
        df_rel.to_excel(writer, sheet_name="related_documents", index=False)
        df_unl.to_excel(writer, sheet_name="unlinked_details", index=False)
        df_sum.to_excel(writer, sheet_name="summary", index=False)

    print(f"[analyze_oc] Excel: {out_xlsx}", flush=True)


if __name__ == "__main__":
    main()
