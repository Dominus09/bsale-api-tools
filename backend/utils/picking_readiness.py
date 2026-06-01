"""Estado operativo de picking según facturación del plan."""

from __future__ import annotations

from typing import Any

PICKING_WAIT_MESSAGE = (
    "Los pickings estarán disponibles una vez existan documentos "
    "facturados o relacionados."
)

PICKING_NO_CONFIRMED_MESSAGE = (
    "No hay documentos facturados confirmados para este plan."
)


def evaluate_picking_readiness(inv: dict[str, Any]) -> dict[str, Any]:
    """
    Devuelve ready + reason humano según payload de get_invoiced_documents / dashboard.
    """
    summary = inv.get("summary") if isinstance(inv.get("summary"), dict) else {}
    confirmed = int(summary.get("confirmed") or 0)
    probable = int(summary.get("probable") or 0)
    missing = int(summary.get("missing") or 0)
    total = int(summary.get("total") or 0)

    if inv.get("invoicing_unavailable"):
        err = inv.get("invoicing_error") or "vista de facturación no disponible"
        return {
            "ready": False,
            "reason": (
                f"No se pudo consultar la facturación del plan ({err}). "
                "Revise migraciones 015/022 o sincronice document_related."
            ),
        }

    if inv.get("invoicing_source") == "lite" and inv.get("invoicing_degraded"):
        if confirmed == 0 and total > 0:
            return {
                "ready": False,
                "reason": PICKING_NO_CONFIRMED_MESSAGE,
            }

    if total == 0:
        return {
            "ready": False,
            "reason": "El plan no tiene órdenes de compra asociadas.",
        }

    if confirmed == 0:
        if probable > 0:
            return {
                "ready": False,
                "reason": (
                    "Hay coincidencias probables (60–74) pero ninguna facturación "
                    "confirmada ni auto-confirmada (≥75)."
                ),
            }
        return {
            "ready": False,
            "reason": PICKING_NO_CONFIRMED_MESSAGE,
        }

    if missing > 0:
        return {
            "ready": False,
            "reason": (
                f"Faltan {missing} OC sin facturación confirmada "
                f"({confirmed}/{total} confirmadas)."
            ),
        }

    if probable > 0:
        return {
            "ready": False,
            "reason": (
                f"Hay {probable} OC con coincidencia probable (score 60–74); "
                "confirme en Bsale o espere auto-confirmación ≥75."
            ),
        }

    if not inv.get("ready_for_picking", False):
        return {
            "ready": False,
            "reason": PICKING_WAIT_MESSAGE,
        }

    return {"ready": True, "reason": None}


def picking_block_from_invoicing(
    plan_id: int,
    inv: dict[str, Any],
) -> dict[str, Any]:
    readiness = evaluate_picking_readiness(inv)
    return {
        "client_endpoint": f"/distribuidora/dispatch-plans/{plan_id}/picking-cliente",
        "product_endpoint": f"/distribuidora/dispatch-plans/{plan_id}/picking-producto",
        "ready": readiness["ready"],
        "reason": readiness["reason"],
    }


def picking_not_ready_payload(
    plan_id: int,
    inv: dict[str, Any],
    *,
    kind: str,
) -> dict[str, Any]:
    readiness = evaluate_picking_readiness(inv)
    base: dict[str, Any] = {
        "dispatch_plan_id": plan_id,
        "ready": False,
        "reason": readiness["reason"] or PICKING_WAIT_MESSAGE,
        "degraded": False,
        "summary": inv.get("summary"),
    }
    if kind == "client":
        base["clients"] = []
        base["validation"] = inv
    else:
        base["items"] = []
    return base
