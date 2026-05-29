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


def empty_invoicing_payload(
    plan_id: int,
    orders: list[dict[str, Any]],
    *,
    invoicing_error: str | None = None,
) -> dict[str, Any]:
    """Dashboard/facturación cuando fallan vista completa y consulta lite."""
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
    msg = (
        invoicing_error
        or "No se pudo consultar facturación (timeout, vista o migración pendiente)"
    )
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
                "message": msg,
            }
            for x in items
        ],
        "probable_notes": [],
        "ready_for_picking": False,
        "invoicing_unavailable": True,
        "invoicing_error": msg,
        "invoicing_degraded": True,
        "invoicing_source": "unavailable",
    }


def empty_plan_dashboard(
    plan_id: int,
    plan: dict[str, Any] | None,
    *,
    degraded_message: str | None = None,
) -> dict[str, Any]:
    """Dashboard seguro cuando falla facturación, margen o vistas SQL."""
    warnings: list[dict[str, Any]] = []
    if degraded_message:
        warnings.append(
            {
                "oc_document_id": 0,
                "message": degraded_message,
            }
        )
    base_plan = plan if plan else {"id": plan_id}
    return {
        "plan": base_plan,
        "invoicing": {
            "total_orders": 0,
            "total_oc_amount_clp": 0,
            "confirmed": {"count": 0, "amount_clp": 0},
            "probable": {"count": 0, "amount_clp": 0},
            "pending": {"count": 0, "amount_clp": 0},
        },
        "invoiced_items": [],
        "warnings": warnings,
        "probable_notes": [],
        "margin": None,
        "picking": {
            "client_endpoint": f"/distribuidora/dispatch-plans/{plan_id}/picking-cliente",
            "product_endpoint": f"/distribuidora/dispatch-plans/{plan_id}/picking-producto",
            "ready": False,
            "reason": degraded_message
            or "No se pudo cargar facturación para evaluar picking.",
        },
        "degraded": True,
    }


def empty_invoiced_documents_response(plan_id: int) -> dict[str, Any]:
    return {
        "dispatch_plan_id": plan_id,
        "items": [],
        "summary": {"confirmed": 0, "probable": 0, "missing": 0, "total": 0},
        "warnings": [],
        "probable_notes": [],
        "ready_for_picking": False,
        "degraded": True,
    }


def empty_picking_by_client_response(
    plan_id: int,
    *,
    reason: str | None = None,
    ready: bool = False,
) -> dict[str, Any]:
    return {
        "dispatch_plan_id": plan_id,
        "ready": ready,
        "reason": reason,
        "clients": [],
        "validation": None,
        "degraded": True,
    }


def empty_picking_by_product_response(
    plan_id: int,
    *,
    reason: str | None = None,
    ready: bool = False,
) -> dict[str, Any]:
    return {
        "dispatch_plan_id": plan_id,
        "ready": ready,
        "reason": reason,
        "items": [],
        "degraded": True,
    }
