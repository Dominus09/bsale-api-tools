"""PWA operativa — lectura de despachos del chofer autenticado.

No exige app ERP. Ownership estricto por ``dispatch_plan.driver_user_id``.
P2: clientes, ficha, documentos/productos y mapa inicial (solo lectura).
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query

from backend.auth.multiapp import require_pwa_profiles
from backend.services.distribuidora import dispatch_plan_driver_service as driver_svc
from backend.services.distribuidora import dispatch_plan_pwa_driver_read_service as read_svc
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
    """Mis despachos: solo filas con ``driver_user_id`` = usuario JWT + SPA vigente."""
    uid = _uid(user)
    items = driver_svc.list_plans_for_driver_user(uid, limit=limit)
    visible: list[dict] = []
    for row in items:
        try:
            read_svc.enforce_driver_plan_read_access(user, int(row["id"]))
            visible.append(row)
        except HTTPException as exc:
            if exc.status_code in (403, 503):
                continue
            if exc.status_code == 404:
                continue
            raise
    return serialize_value(
        {
            "items": visible,
            "driver_user_id": uid,
            "count": len(visible),
        }
    )


@router.get("/despachos/{plan_id}")
def get_my_despacho(
    plan_id: int,
    user: dict = Depends(require_pwa_profiles("chofer")),
):
    """Ficha read-only. Plan ajeno / oficina revocada → 404/403."""
    uid = read_svc.enforce_driver_plan_read_access(user, int(plan_id))
    row = driver_svc.get_plan_summary_for_driver(uid, int(plan_id))
    if row is None:
        raise HTTPException(status_code=404, detail="Despacho no encontrado")
    return serialize_value(
        {
            "plan": row,
            "driver_user_id": uid,
            "read_only": True,
            "nav": {
                "mapa": True,
                "clientes": True,
                "ficha": True,
                "note_es": "Mapa, clientes y ficha de entrega (solo lectura).",
            },
            "future_contracts": read_svc.FUTURE_CONTRACTS,
        }
    )


@router.get("/despachos/{plan_id}/clientes")
def list_my_clientes(
    plan_id: int,
    q: str | None = Query(None, description="Buscar por cliente o folio"),
    user: dict = Depends(require_pwa_profiles("chofer")),
):
    read_svc.enforce_driver_plan_read_access(user, int(plan_id))
    return read_svc.list_clients_for_driver(int(plan_id), q=q)


@router.get("/despachos/{plan_id}/clientes/{customer_key:path}")
def get_my_cliente(
    plan_id: int,
    customer_key: str,
    user: dict = Depends(require_pwa_profiles("chofer")),
):
    read_svc.enforce_driver_plan_read_access(user, int(plan_id))
    return read_svc.get_client_ficha_for_driver(int(plan_id), customer_key)


@router.get("/despachos/{plan_id}/mapa")
def get_my_mapa(
    plan_id: int,
    origin_lat: float | None = Query(None),
    origin_lng: float | None = Query(None),
    user: dict = Depends(require_pwa_profiles("chofer")),
):
    read_svc.enforce_driver_plan_read_access(user, int(plan_id))
    lat, lng = read_svc.validate_origin_coords(origin_lat, origin_lng)
    return read_svc.get_map_for_driver(
        int(plan_id),
        origin_lat=lat,
        origin_lng=lng,
    )


@router.get("/futuro/contratos")
def get_future_contracts(
    user: dict = Depends(require_pwa_profiles("chofer")),
):
    _uid(user)
    return read_svc.get_future_contracts()
