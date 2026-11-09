"""PWA operativa — lectura de despachos del chofer autenticado.

No exige app ERP. Ownership estricto por ``dispatch_plan.driver_user_id``.
P1: solo lectura (lista + ficha resumen). Sin mapa/GPS/fotos/escrituras.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query

from backend.auth.multiapp import require_pwa_profiles
from backend.services.distribuidora import dispatch_plan_driver_service as driver_svc
from backend.utils.json_safe import serialize_value

router = APIRouter(
    prefix="/distribuidora/pwa/driver",
    tags=["PWA chofer"],
    dependencies=[Depends(require_pwa_profiles("chofer"))],
)


def _uid(user: dict) -> int:
    raw = user.get("id") if user.get("id") is not None else user.get("uid")
    try:
        return int(raw)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=401, detail="Usuario sin id") from exc


@router.get("/despachos")
def list_my_despachos(
    limit: int = Query(50, ge=1, le=200),
    user: dict = Depends(require_pwa_profiles("chofer")),
):
    """Mis despachos: solo filas con ``driver_user_id`` = usuario JWT."""
    uid = _uid(user)
    items = driver_svc.list_plans_for_driver_user(uid, limit=limit)
    return serialize_value(
        {
            "items": items,
            "driver_user_id": uid,
            "count": len(items),
        }
    )


@router.get("/despachos/{plan_id}")
def get_my_despacho(
    plan_id: int,
    user: dict = Depends(require_pwa_profiles("chofer")),
):
    """Ficha read-only. Plan ajeno → 404 (sin filtrar existencia)."""
    uid = _uid(user)
    row = driver_svc.get_plan_summary_for_driver(uid, int(plan_id))
    if row is None:
        raise HTTPException(status_code=404, detail="Despacho no encontrado")
    return serialize_value(
        {
            "plan": row,
            "driver_user_id": uid,
            "read_only": True,
            "nav_placeholders": {
                "mapa": False,
                "clientes": False,
                "note_es": "Mapa y fichas de cliente en fases posteriores.",
            },
        }
    )
