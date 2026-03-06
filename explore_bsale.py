import requests
import os
print("START SYNC")
BSALE_TOKEN = os.getenv("BSALE_TOKEN_Mini")

NOCODB_TOKEN = "R3EhSD8si-WSVdsPxlQVGAfiHRRcDR9cHGHJdBJL"
NOCODB_URL = "https://db.quillotana.cl"

headers_bsale = {
    "access_token": BSALE_TOKEN
}

headers_noco = {
    "xc-token": NOCODB_TOKEN,
    "Content-Type": "application/json"
}

BASE_BSALE = "https://api.bsale.io/v1"

limit = 50


TABLE_PRODUCTS = "products"
TABLE_VARIANTS = "variants"
TABLE_COSTS = "variant_costs"
TABLE_PRICES = "variant_prices"
TABLE_STOCKS = "stocks"


def fetch_all(endpoint):

    offset = 0
    results = []

    while True:

        url = f"{BASE_BSALE}/{endpoint}?limit={limit}&offset={offset}"

        r = requests.get(url, headers=headers_bsale)
        data = r.json()

        items = data.get("items", [])

        if not items:
            break

        results.extend(items)

        offset += limit

    return results


def insert_noco(table, payload):

    url = f"{NOCODB_URL}/api/v2/tables/{table}/records"

    r = requests.post(url, json=payload, headers=headers_noco)

    if r.status_code not in [200,201]:

        print("ERROR:", r.text)


# -------------------------
# PRODUCTS
# -------------------------

print("SYNC PRODUCTS")

products = fetch_all("products.json")

for p in products:

    insert_noco(TABLE_PRODUCTS,{
        "bsale_id": p["id"],
        "name": p.get("name"),
        "classification": str(p.get("classification")),
        "brand": str(p.get("brand"))
    })


# -------------------------
# VARIANTS
# -------------------------

print("SYNC VARIANTS")

variants = fetch_all("variants.json")

for v in variants:

    insert_noco(TABLE_VARIANTS,{
        "bsale_id": v["id"],
        "product_id": v["product"]["id"],
        "code": v.get("code"),
        "bar_code": v.get("barCode"),
        "description": v.get("description")
    })


# -------------------------
# COSTS
# -------------------------

print("SYNC COSTS")

for v in variants:

    variant_id = v["id"]

    for cost in v.get("costs",[]):

        insert_noco(TABLE_COSTS,{
            "variant_id": variant_id,
            "office_id": cost["office"]["id"],
            "cost": cost["cost"]
        })


# -------------------------
# PRICES
# -------------------------

print("SYNC PRICES")

for v in variants:

    variant_id = v["id"]

    for price in v.get("prices",[]):

        insert_noco(TABLE_PRICES,{
            "variant_id": variant_id,
            "price_list_id": price["priceList"]["id"],
            "price": price["price"]
        })


# -------------------------
# STOCKS
# -------------------------

print("SYNC STOCKS")

stocks = fetch_all("stocks.json")

for s in stocks:

    insert_noco(TABLE_STOCKS,{
        "variant_id": s["variant"]["id"],
        "office_id": s["office"]["id"],
        "quantity_available": s["quantityAvailable"],
        "quantity_reserved": s["quantityReserved"]
    })


print("SYNC COMPLETADO")
Exit()
