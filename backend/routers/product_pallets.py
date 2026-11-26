"""Separate management-only pallet overlay; leaves legacy logistics endpoints untouched."""
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from backend.auth.permissions import require_management_access
from backend.services import product_master_admin_service as admin

router = APIRouter(prefix="/products-master", tags=["product-pallets"])


class PalletPatch(BaseModel):
    boxes_per_pallet: int | None = Field(..., gt=0, strict=True)


@router.get("/pallets")
def list_pallet_data(ids: str = Query(..., max_length=1500), user: dict = Depends(require_management_access)):
    admin.actor_id(user)
    try:
        parts = ids.split(",")
        if not 1 <= len(parts) <= 100 or any(not part.isdecimal() for part in parts):
            raise ValueError()
        values = list(dict.fromkeys(int(part) for part in parts))
        if any(value <= 0 or value > 2147483647 for value in values):
            raise ValueError()
    except ValueError:
        raise HTTPException(422, "Selecciona entre 1 y 100 productos válidos.") from None
    return admin.pallet_data(values)


@router.get("/pallets/stats")
def get_pallet_stats(user: dict = Depends(require_management_access)):
    admin.actor_id(user)
    return admin.pallet_stats()


@router.patch("/id/{row_id}/pallet")
def patch_pallet(row_id: int, body: PalletPatch, user: dict = Depends(require_management_access)):
    return admin.set_pallet_boxes(row_id, body.boxes_per_pallet, admin.actor_id(user))
