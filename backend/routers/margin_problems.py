from fastapi import APIRouter
from backend.database import noco_get

router = APIRouter(
    prefix="/margin/problems",
    tags=["margin"]
)

TABLE_ANALYTICS = "m777i9qvqgbvpuk"


@router.get("/")
def margin_problems(company_id: int):

    rows = noco_get(
        TABLE_ANALYTICS,
        params={
            "where": f"(company_id,eq,{company_id})~and(status,neq,OK)~and(status,neq,NO_PRICE)"
        }
    )

    result = []

    for r in rows:

        result.append({

            "variant_id": r["variant_id"],
            "product_name": r["product_name"],
            "variant_name": r["variant_name"],
            "price_list_name": r["price_list_name"],

            "price_gross": r["price_gross"],
            "cost_gross": r["cost_gross"],

            "margin_percent": r["margin_percent"],
            "status": r["status"]

        })

    return result
