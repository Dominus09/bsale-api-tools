"""Referencias ``distribuidora.document_references`` (delete + insert por documento fuente)."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from psycopg2.extras import Json, execute_values


def _ts(raw: Any) -> Any:
    if raw is None:
        return None
    try:
        return datetime.fromtimestamp(int(raw), tz=timezone.utc)
    except (TypeError, ValueError, OSError):
        return None


def rows_from_references_json(source_document_id: int, payload: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    items = payload.get("items")
    if items is None:
        items = payload.get("references") or []
    if not isinstance(items, list):
        return out
    for it in items:
        if not isinstance(it, dict):
            continue
        num = it.get("number") or it.get("referenceNumber") or it.get("documentNumber")
        dt = it.get("document_type") or it.get("documentType") or {}
        reason = it.get("reason") or it.get("referenceReason") or it.get("description")
        ref_date = it.get("referenceDate") or it.get("emissionDate") or it.get("date")
        try:
            ref_num = int(num) if num is not None else None
        except (TypeError, ValueError):
            ref_num = None
        try:
            rdt = int(dt["id"]) if isinstance(dt, dict) and dt.get("id") is not None else None
        except (TypeError, ValueError):
            rdt = None
        out.append(
            {
                "source_document_id": source_document_id,
                "reference_number": ref_num,
                "reference_document_type_id": rdt,
                "reference_date": _ts(ref_date),
                "reference_reason": None if reason is None else str(reason),
                "raw_data": Json(it),
            }
        )
    return out


def replace_document_references(cur, source_document_id: int, payload: dict[str, Any]) -> int:
    cur.execute(
        "DELETE FROM distribuidora.document_references WHERE source_document_id = %s",
        (source_document_id,),
    )
    rows = rows_from_references_json(source_document_id, payload)
    if not rows:
        return 0
    cols = [
        "source_document_id",
        "reference_number",
        "reference_document_type_id",
        "reference_date",
        "reference_reason",
        "raw_data",
    ]
    sql = f"""
        INSERT INTO distribuidora.document_references ({", ".join(cols)}, created_at)
        VALUES %s
    """
    template = "(" + ",".join(["%s"] * len(cols)) + ",NOW())"
    values = [tuple(r[c] for c in cols) for r in rows]
    execute_values(cur, sql, values, template=template, page_size=len(values))
    return len(rows)
