from fastapi import APIRouter
from backend.database import noco_get

router = APIRouter(
    prefix="/margin",
    tags=["margin"]
)

TABLE_ANALYTICS = "m777i9qvqgbvpuk"

@router.get("/")
def margin(company_id: int):

    rows = noco_get(
        TABLE_ANALYTICS,
        params={"where": f"(company_id,eq,{company_id})"}
    )

    return rows
