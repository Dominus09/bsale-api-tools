from fastapi import APIRouter
from backend.database import noco_get

router = APIRouter(
    prefix="/dashboard",
    tags=["dashboard"]
)

TABLE_ANALYTICS = "m777i9qvqgbvpuk"


@router.get("/{company_id}")
def dashboard(company_id: int):

    rows = noco_get(
        TABLE_ANALYTICS,
        params={"where": f"(company_id,eq,{company_id})"}
    )

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
        "total_problems": low + high + ultra

    }
