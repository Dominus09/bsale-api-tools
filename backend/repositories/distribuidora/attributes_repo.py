"""Atributos ``distribuidora.document_attributes`` (delete + insert por documento)."""

from __future__ import annotations

from typing import Any

from psycopg2.extras import Json, execute_values


def rows_from_attributes_json(document_id: int, payload: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    items = payload.get("items") or []
    if not isinstance(items, list):
        return out
    for it in items:
        if not isinstance(it, dict):
            continue
        name = it.get("name") or it.get("attributeName") or it.get("label")
        if name is None or str(name).strip() == "":
            continue
        val = it.get("value") or it.get("text") or it.get("content")
        aid = it.get("id")
        out.append(
            {
                "document_id": document_id,
                "attribute_id": int(aid) if aid is not None else None,
                "attribute_name": str(name).strip(),
                "attribute_value": None if val is None else str(val),
                "raw_data": Json(it),
            }
        )
    return out


def replace_document_attributes(cur, document_id: int, payload: dict[str, Any]) -> int:
    cur.execute(
        "DELETE FROM distribuidora.document_attributes WHERE document_id = %s",
        (document_id,),
    )
    rows = rows_from_attributes_json(document_id, payload)
    if not rows:
        return 0
    cols = ["document_id", "attribute_id", "attribute_name", "attribute_value", "raw_data"]
    sql = f"""
        INSERT INTO distribuidora.document_attributes ({", ".join(cols)}, created_at)
        VALUES %s
    """
    template = "(" + ",".join(["%s"] * len(cols)) + ",NOW())"
    values = [tuple(r[c] for c in cols) for r in rows]
    execute_values(cur, sql, values, template=template, page_size=len(values))
    return len(rows)
