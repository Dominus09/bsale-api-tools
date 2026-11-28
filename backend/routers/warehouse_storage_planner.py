"""Management-only virtual planning API."""
from uuid import UUID
from fastapi import APIRouter, Depends, HTTPException

from backend.auth.permissions import require_management_access
from backend.services.warehouse.errors import WarehouseError, WarehouseConflictError, WarehouseNotFoundError
from backend.services.warehouse.pallets import PalletSchemaUnavailable
from backend.services.warehouse.storage_planner import PlanSave, default_storage_planner

router = APIRouter(prefix="/warehouse/storage-plans", tags=["inteligencia-bodega"])


def call(method, *args):
    try:
        return method(*args)
    except WarehouseError as exc:
        status = 503 if isinstance(exc, PalletSchemaUnavailable) else 409 if isinstance(exc, WarehouseConflictError) else 404 if isinstance(exc, WarehouseNotFoundError) else 400
        raise HTTPException(status_code=status, detail={"message":str(exc),"code":exc.code}) from exc


@router.get("")
def list_plans(_user: dict = Depends(require_management_access)):
    return call(default_storage_planner().list_plans)


@router.get("/{plan_id}")
def get_plan(plan_id: UUID, _user: dict = Depends(require_management_access)):
    return call(default_storage_planner().get_plan, plan_id)


@router.post("")
def save_plan(body: PlanSave, user: dict = Depends(require_management_access)):
    actor = str(user.get("email") or user.get("name") or user.get("id") or "ERP")
    return call(default_storage_planner().save_plan, body, actor)
