"""F3-D3: comparación y adopción controlada de versiones de picking en cuadratura.

No altera el motor financiero: solo diferencia alcance adoptado vs picking vigente.
"""

from __future__ import annotations

from typing import Any

from backend.utils.dispatch_plan_cuadratura_snapshot import (
    build_documents_from_picking_clients,
)


def document_identity(row: dict[str, Any]) -> dict[str, Any]:
    """Identidad documental real (preferir related_document_id sobre folio visible)."""
    related_id = row.get("related_document_id")
    try:
        related_id_i = int(related_id) if related_id is not None else None
    except (TypeError, ValueError):
        related_id_i = None
    doc_num = row.get("document_number")
    try:
        doc_num_i = int(doc_num) if doc_num is not None else None
    except (TypeError, ValueError):
        doc_num_i = None
    oc_id = row.get("oc_document_id")
    try:
        oc_id_i = int(oc_id) if oc_id is not None else None
    except (TypeError, ValueError):
        oc_id_i = None
    return {
        "related_document_id": related_id_i,
        "document_number": doc_num_i,
        "oc_document_id": oc_id_i,
        "key": document_key(row),
    }


def document_key(row: dict[str, Any]) -> str:
    """Clave estable: id tributario real; folio solo como fallback legacy."""
    if row.get("related_document_id") is not None:
        try:
            return f"id:{int(row['related_document_id'])}"
        except (TypeError, ValueError):
            pass
    if row.get("document_number") is not None:
        return f"num:{row['document_number']}"
    return ""


def doc_has_financial_work(row: dict[str, Any]) -> bool:
    medio = str(row.get("medio_pago") or "pendiente").strip().lower() or "pendiente"
    obs = str(row.get("observacion") or "").strip()
    if obs:
        return True
    return medio not in {"", "pendiente"}


def _monto(row: dict[str, Any]) -> int:
    try:
        return int(round(float(row.get("monto_clp") or 0)))
    except (TypeError, ValueError):
        return 0


def index_documents(docs: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in docs or []:
        key = document_key(row)
        if key:
            out[key] = row
    return out


def compare_document_scopes(
    adopted_docs: list[dict[str, Any]],
    proposed_docs: list[dict[str, Any]],
) -> dict[str, Any]:
    """Diff adoptado vs propuesto (picking vigente). Montos de documentos tributarios."""
    adopted_idx = index_documents(adopted_docs)
    proposed_idx = index_documents(proposed_docs)
    adopted_keys = set(adopted_idx)
    proposed_keys = set(proposed_idx)

    added_keys = sorted(proposed_keys - adopted_keys)
    removed_keys = sorted(adopted_keys - proposed_keys)
    common = adopted_keys & proposed_keys

    added = []
    for k in added_keys:
        row = proposed_idx[k]
        added.append(
            {
                **document_identity(row),
                "client_name": row.get("client_name"),
                "monto_clp": _monto(row),
                "medio_pago": row.get("medio_pago"),
            }
        )

    removed = []
    removed_with_work = []
    for k in removed_keys:
        row = adopted_idx[k]
        item = {
            **document_identity(row),
            "client_name": row.get("client_name"),
            "monto_clp": _monto(row),
            "medio_pago": row.get("medio_pago"),
            "observacion": row.get("observacion"),
            "has_financial_work": doc_has_financial_work(row),
        }
        removed.append(item)
        if item["has_financial_work"]:
            removed_with_work.append(item)

    modified = []
    for k in sorted(common):
        a = adopted_idx[k]
        p = proposed_idx[k]
        a_m = _monto(a)
        p_m = _monto(p)
        if a_m != p_m:
            modified.append(
                {
                    **document_identity(p),
                    "client_name": p.get("client_name") or a.get("client_name"),
                    "monto_adopted_clp": a_m,
                    "monto_proposed_clp": p_m,
                    "delta_clp": p_m - a_m,
                    "has_financial_work": doc_has_financial_work(a),
                    "medio_pago": a.get("medio_pago"),
                    "observacion": a.get("observacion"),
                }
            )

    venta_adopted = sum(_monto(r) for r in adopted_docs)
    venta_proposed = sum(_monto(r) for r in proposed_docs)
    return {
        "documents_added": added,
        "documents_removed": removed,
        "documents_modified": modified,
        "documents_removed_with_financial_work": removed_with_work,
        "added_count": len(added),
        "removed_count": len(removed),
        "modified_count": len(modified),
        "venta_adopted_clp": venta_adopted,
        "venta_proposed_clp": venta_proposed,
        "venta_delta_clp": venta_proposed - venta_adopted,
        "has_changes": bool(added or removed or modified),
        "blocks_auto_drop": bool(removed_with_work),
    }


def build_proposed_documents(
    picking_clients: list[dict[str, Any]],
    *,
    saved_documents: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Propuesta de alcance desde picking vigente, conservando medios de docs conocidos."""
    return build_documents_from_picking_clients(
        picking_clients,
        saved_documents=saved_documents,
    )


def merge_adopted_documents(
    *,
    adopted_docs: list[dict[str, Any]],
    proposed_docs: list[dict[str, Any]],
    allow_drop_without_work: bool = True,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Fusiona propuesta sobre adoptado. Exige resolución si se retiran docs con pagos."""
    diff = compare_document_scopes(adopted_docs, proposed_docs)
    if diff["blocks_auto_drop"]:
        raise ValueError(
            "Hay documentos retirados del picking con pagos u observaciones. "
            "Resuelva explícitamente antes de adoptar "
            f"({len(diff['documents_removed_with_financial_work'])} documento(s))."
        )
    if not allow_drop_without_work and diff["removed_count"]:
        raise ValueError(
            "La adopción retiraría documentos del alcance; confirme resolución."
        )
    # Propuesta ya trae medios preservados vía saved_documents.
    return proposed_docs, diff
