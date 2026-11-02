from fastapi import APIRouter, Depends

from backend.auth.multiapp import assert_company, get_scope
from backend.database import noco_get
from backend.utils.auth_staff import require_staff_user

router = APIRouter(prefix="/dashboard", tags=["dashboard"])

TABLE_ANALYTICS = "m777i9qvqgbvpuk"


@router.get("/{company_id}")
def dashboard(company_id: int, user: dict = Depends(require_staff_user)):
    """Dashboard legacy por empresa — valida alcance company_ids si está emitido."""
    assert_company(get_scope(user), company_id)

    rows = noco_get(TABLE_ANALYTICS, params={"where": f"(company_id,eq,{company_id})"})

    low = 0
    high = 0
    ultra = 0

    for r in rows:
        if r["status"] == "LOW":
            low += 1
        elif r["status"] == "HIGH":
            high += 1
        elif r["status"] == "ULTRA_HIGH":
            ultra += 1

    return {
        "low_margin": low,
        "high_margin": high,
        "ultra_high_margin": ultra,
        "total_problems": low + high + ultra,
    }
