from fastapi import APIRouter
from fastapi.responses import StreamingResponse
from backend.database import noco_get
import csv
import io

router = APIRouter(
    prefix="/margin",
    tags=["margin"]
)

TABLE_ANALYTICS = "m777i9qvqgbvpuk"


@router.get("/export")
def export_margin(company_id:int):

    rows = noco_get(
        TABLE_ANALYTICS,
        params={
            "where": f"(company_id,eq,{company_id})~and(status,neq,OK)"
        }
    )

    output = io.StringIO()
    writer = csv.writer(output)

    writer.writerow([
        "variant_id",
        "price_list_id",
        "variant_name",
        "price_gross",
        "cost_gross",
        "margin_percent",
        "status"
    ])

    for r in rows:

        writer.writerow([
            r["variant_id"],
            r["price_list_id"],
            r["variant_name"],
            r["price_gross"],
            r["cost_gross"],
            r["margin_percent"],
            r["status"]
        ])

    output.seek(0)

    return StreamingResponse(
        output,
        media_type="text/csv",
        headers={
            "Content-Disposition":
            "attachment; filename=margin_problems.csv"
        }
    )
