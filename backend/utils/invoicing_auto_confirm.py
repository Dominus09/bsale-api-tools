"""Clasificación operacional de facturación OC (auto-confirmación por score)."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

AUTO_CONFIRM_MIN_SCORE = 75.0
PROBABLE_MIN_SCORE = 60.0

RELATION_SOURCE_AUTO = "auto_match"
RELATION_SOURCE_RELATED = "relateddetailid"
RELATION_SOURCE_PROBABLE = "probable_match"

STATUS_CONFIRMED = "confirmed"
STATUS_PROBABLE = "probable"
STATUS_MISSING = "missing"


def _score_value(row: dict[str, Any]) -> float | None:
    raw = row.get("probable_score")
    if raw is None:
        return None
    try:
        val = float(raw)
    except (TypeError, ValueError):
        return None
    return val if val == val else None  # NaN guard


def _bsale_confirmed(row: dict[str, Any]) -> bool:
    if row.get("is_invoiced_confirmed") is True:
        return True
    if row.get("relation_source") == RELATION_SOURCE_RELATED:
        return True
    return False


def apply_operational_invoicing_row(row: dict[str, Any]) -> dict[str, Any]:
    """
    Ajusta status / relation_source / related_* sin borrar probable_* (trazabilidad).

    - Bsale confirmada (document_related): confirmed / relateddetailid
    - score >= 75: confirmed / auto_match (usa candidato como documento operativo)
    - score 60-74: probable / probable_match
    - resto: missing
    """
    out = dict(row)
    score = _score_value(out)

    if _bsale_confirmed(out):
        out["status"] = STATUS_CONFIRMED
        if not out.get("relation_source"):
            out["relation_source"] = RELATION_SOURCE_RELATED
        out["is_invoiced_confirmed"] = True
        out["is_auto_confirmed"] = False
        return out

    if score is not None and score >= AUTO_CONFIRM_MIN_SCORE:
        out["status"] = STATUS_CONFIRMED
        out["relation_source"] = RELATION_SOURCE_AUTO
        out["is_invoiced_confirmed"] = True
        out["is_auto_confirmed"] = True
        if out.get("related_document_id") is None:
            out["related_document_id"] = out.get("probable_document_id")
        if out.get("related_document_number") is None:
            out["related_document_number"] = out.get("probable_document_number")
        if out.get("related_document_type_id") is None:
            out["related_document_type_id"] = out.get("probable_document_type_id")
        if out.get("related_document_type_label") is None:
            out["related_document_type_label"] = out.get("probable_document_type_label")
        return out

    if score is not None and score >= PROBABLE_MIN_SCORE:
        out["status"] = STATUS_PROBABLE
        out["relation_source"] = RELATION_SOURCE_PROBABLE
        out["is_invoiced_confirmed"] = False
        out["is_auto_confirmed"] = False
        return out

    out["status"] = STATUS_MISSING
    out["relation_source"] = None
    out["is_invoiced_confirmed"] = False
    out["is_auto_confirmed"] = False
    return out


def apply_operational_invoicing_rows(
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    return [apply_operational_invoicing_row(dict(r)) for r in rows]


def invoicing_summary_counts(items: list[dict[str, Any]]) -> dict[str, int]:
    confirmed = 0
    auto_confirmed = 0
    probable = 0
    missing = 0
    for x in items:
        st = x.get("status")
        if st == STATUS_CONFIRMED:
            confirmed += 1
            if x.get("relation_source") == RELATION_SOURCE_AUTO or x.get(
                "is_auto_confirmed"
            ):
                auto_confirmed += 1
        elif st == STATUS_PROBABLE:
            probable += 1
        else:
            missing += 1
    return {
        "confirmed": confirmed,
        "auto_confirmed": auto_confirmed,
        "probable": probable,
        "missing": missing,
        "total": len(items),
    }


def decimal_from_row_amount(val: Any) -> Decimal:
    if val is None:
        return Decimal(0)
    try:
        return Decimal(str(val))
    except Exception:
        return Decimal(0)
