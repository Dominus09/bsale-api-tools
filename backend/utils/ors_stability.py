"""Logs y helpers de estabilidad para ORS / planificación."""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


def log_debug(
    endpoint: str,
    *,
    planning_id: int | None = None,
    rows: int | None = None,
    extra: dict[str, Any] | None = None,
) -> None:
    parts = [f"endpoint={endpoint}"]
    if planning_id is not None:
        parts.append(f"planning_id={planning_id}")
    if rows is not None:
        parts.append(f"rows={rows}")
    if extra:
        for k, v in extra.items():
            parts.append(f"{k}={v}")
    logger.info("[ORS_STABILITY_DEBUG] %s", " ".join(parts))


def log_error(
    endpoint: str,
    exc: BaseException,
    *,
    planning_id: int | None = None,
    extra: dict[str, Any] | None = None,
) -> None:
    parts = [f"endpoint={endpoint}", f"error={exc!r}"]
    if planning_id is not None:
        parts.append(f"planning_id={planning_id}")
    if extra:
        for k, v in extra.items():
            parts.append(f"{k}={v}")
    logger.exception("[ORS_STABILITY_DEBUG] %s", " ".join(parts))


def empty_invoicing_payload(plan_id: int, orders: list[dict[str, Any]]) -> dict[str, Any]:
    """Dashboard/facturación sin vista v_dispatch_plan_invoiced_documents."""
    items = [
        {
            "dispatch_plan_id": plan_id,
            "oc_document_id": int(o.get("oc_document_id") or 0),
            "oc_number": o.get("oc_number"),
            "route_order": o.get("route_order"),
            "status": "missing",
            "relation_source": None,
        }
        for o in orders
        if o.get("oc_document_id") is not None
    ]
    n = len(items)
    return {
        "dispatch_plan_id": plan_id,
        "items": items,
        "summary": {
            "confirmed": 0,
            "probable": 0,
            "missing": n,
            "total": n,
        },
        "warnings": [
            {
                "oc_document_id": x["oc_document_id"],
                "oc_number": x.get("oc_number"),
                "message": "Facturación no disponible (vista o migración pendiente)",
            }
            for x in items
        ],
        "probable_notes": [],
        "ready_for_picking": False,
    }
