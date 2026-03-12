from fastapi import APIRouter
from backend.database import noco_get

router = APIRouter(
    prefix="/companies",
    tags=["companies"]
)

TABLE_COMPANIES = "m27za58sg6ustui"

@router.get("/")
def get_companies():

    rows = noco_get(TABLE_COMPANIES)

    result = []

    for r in rows:
        if r.get("active"):
            result.append({
                "company_id": r["company_id"],
                "name": r["name"]
            })

    return result
