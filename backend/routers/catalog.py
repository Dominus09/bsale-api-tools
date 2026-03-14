from fastapi import APIRouter
from db import get_connection

router = APIRouter()

@router.get("/catalog")

def get_catalog():

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""

        SELECT
        variant_id,
        product_type,
        product,
        variant,
        bar_code,
        stock,
        price_13,
        price_14,
        price_16,
        image_url
        FROM bsale.catalog_view
        ORDER BY product

    """)

    rows = cur.fetchall()

    data = []

    for r in rows:

        data.append({

            "variant_id": r[0],
            "product_type": r[1],
            "product": r[2],
            "variant": r[3],
            "barcode": r[4],
            "stock": r[5],

            "prices": {
                "13": r[6],
                "14": r[7],
                "16": r[8]
            },

            "default_price": r[6],

            "image": r[9]

        })

    cur.close()
    conn.close()

    return data
