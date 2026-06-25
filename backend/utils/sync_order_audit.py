"""Auditoría temporal de sync OC (log [ORDER_SYNC_AUDIT])."""

from __future__ import annotations

import logging
import os
from typing import Any

logger = logging.getLogger(__name__)


def _audit_enabled() -> bool:
    return os.getenv("ORDER_SYNC_AUDIT", "").strip().lower() in ("1", "true", "yes", "on")


def _audit_numbers() -> set[int]:
    raw = os.getenv("ORDER_SYNC_AUDIT_NUMBERS", "").strip()
    if not raw:
        return set()
    out: set[int] = set()
    for part in raw.split(","):
        part = part.strip()
        if part.isdigit():
            out.add(int(part))
    return out


def _observations_from_bsale(d: dict[str, Any]) -> str | None:
    comments = d.get("comments")
    if comments is not None and str(comments).strip():
        return str(comments).strip()
    attrs = d.get("attributes")
    if isinstance(attrs, dict):
        items = attrs.get("items") or []
        if isinstance(items, list):
            for it in items:
                if not isinstance(it, dict):
                    continue
                name = str(it.get("name") or it.get("attributeName") or "").strip().upper()
                if name == "OBSERVACIONES":
                    val = it.get("value") or it.get("text") or it.get("content")
                    if val is not None and str(val).strip():
                        return str(val).strip()
    return None


def log_order_sync_audit(
    bsale_doc: dict[str, Any],
    *,
    phase: str,
    persisted_document_id: int | None = None,
    attributes_count: int | None = None,
) -> None:
    """
    Log estructurado para validar campos Bsale vs persistencia.

    Activar con ``ORDER_SYNC_AUDIT=1`` o filtrar OC con ``ORDER_SYNC_AUDIT_NUMBERS=67562``.
    """
    if not _audit_enabled() and not _audit_numbers():
        return

    try:
        oc_number = int(bsale_doc.get("number") or bsale_doc.get("documentNumber") or 0)
    except (TypeError, ValueError):
        oc_number = 0

    nums = _audit_numbers()
    if nums and oc_number not in nums:
        return

    logger.info(
        "[ORDER_SYNC_AUDIT] phase=%s document_id=%s documentNumber=%s "
        "generationDate=%s modificationDate=%s updatedAt=%s observations=%r "
        "persisted_document_id=%s attributes_count=%s",
        phase,
        bsale_doc.get("id"),
        bsale_doc.get("number") or bsale_doc.get("documentNumber"),
        bsale_doc.get("generationDate"),
        bsale_doc.get("modificationDate"),
        bsale_doc.get("updatedAt"),
        _observations_from_bsale(bsale_doc),
        persisted_document_id,
        attributes_count,
    )
