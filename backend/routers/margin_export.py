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
        "product_type",
        "product",
        "variant",
        "price_list",
        "price",
        "cost",
        "margin_percent",
        "status"
    ])

    for r in rows:

        writer.writerow([
            r.get("product_type_name"),
            r.get("product_name"),
            r.get("variant_name"),
            r.get("price_list_name"),
            r.get("price_gross"),
            r.get("cost_gross"),
            r.get("margin_percent"),
            r.get("status")
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
