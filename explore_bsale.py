import requests
import os
import sys

print("START SYNC")

# ----------------------------
# TOKENS
# ----------------------------

BSALE_TOKEN = os.getenv("BSALE_TOKEN_Mini")
NOCODB_TOKEN = "R3EhSD8si-WSVdsPxlQVGAfiHRRcDR9cHGHJdBJL"

NOCODB_URL = "https://db.quillotana.cl"
BASE_BSALE = "https://api.bsale.io/v1"

LIMIT = 50

headers_bsale = {
    "access_token": BSALE_TOKEN
}

headers_noco = {
    "xc-token": NOCODB_TOKEN,
    "Content-Type": "application/json"
}

# ----------------------------
# TABLE IDS (NocoDB)
# ----------------------------

TABLE_PRODUCTS = "meke3fsng90uspe"
TABLE_VARIANTS = "msd4vvijzk9pre9"
TABLE_COSTS = "mdjjvdlwev2o76u"
TABLE_PRICES = "mcby3npgc3ig042"
TABLE_STOCKS = "mxs2lyz86cnxd23"


# ----------------------------
# FETCH PAGINADO BSALE
# ----------------------------

def fetch_all(endpoint):

    offset = 0
    results = []

    while True:

        url = f"{BASE_BSALE}/{endpoint}?limit={LIMIT}&offset={offset}"

        r = requests.get(url, headers=headers_bsale)
        data = r.json()

        items = data.get("items", [])

        if not items:
            break

        results.extend(items)

        offset += LIMIT

    return results


# ----------------------------
# INSERT NOCO
# ----------------------------

def insert_noco(table_id, payload):

    url = f"{NOCODB_URL}/api/v2/tables/{table_id}/records"

    r = requests.post(url, json=payload, headers=headers_noco)

    if r.status_code not in [200, 201]:
        print("ERROR:", r.text)


# ----------------------------
# PRODUCTS
# ----------------------------

print("SYNC PRODUCTS")

products = fetch_all("products.json")

for p in products:

    insert_noco(TABLE_PRODUCTS, {
        "bsale_id": p.get("id"),
        "name": p.get("name"),
        "classification": str(p.get("classification")),
        "brand": str(p.get("brand"))
    })


# ----------------------------
# VARIANTS
# ----------------------------

print("SYNC VARIANTS")

variants = fetch_all("variants.json")

for v in variants:

    product = v.get("product")

    product_id = None

    if isinstance(product, dict):
        product_id = product.get("id")

    insert_noco(TABLE_VARIANTS, {
        "bsale_id": v.get("id"),
        "product_id": product_id,
        "code": v.get("code"),
        "bar_code": v.get("barCode"),
        "description": v.get("description")
    })


# ----------------------------
# COSTS
# ----------------------------

print("SYNC COSTS")

for v in variants:

    variant_id = v.get("id")

    for cost in v.get("costs", []):

        office = cost.get("office")

        office_id = None

        if isinstance(office, dict):
            office_id = office.get("id")

        insert_noco(TABLE_COSTS, {
            "variant_id": variant_id,
            "office_id": office_id,
            "cost": cost.get("cost")
        })


# ----------------------------
# PRICES
# ----------------------------

print("SYNC PRICES")

for v in variants:

    variant_id = v.get("id")

    for price in v.get("prices", []):

        price_list = price.get("priceList")

        price_list_id = None

        if isinstance(price_list, dict):
            price_list_id = price_list.get("id")

        insert_noco(TABLE_PRICES, {
            "variant_id": variant_id,
            "price_list_id": price_list_id,
            "price": price.get("price")
        })


# ----------------------------
# STOCKS
# ----------------------------

print("SYNC STOCKS")

stocks = fetch_all("stocks.json")

for s in stocks:

    variant = s.get("variant")
    office = s.get("office")

    variant_id = None
    office_id = None

    if isinstance(variant, dict):
        variant_id = variant.get("id")

    if isinstance(office, dict):
        office_id = office.get("id")

    insert_noco(TABLE_STOCKS, {
        "variant_id": variant_id,
        "office_id": office_id,
        "quantity_available": s.get("quantityAvailable"),
        "quantity_reserved": s.get("quantityReserved")
    })


print("SYNC COMPLETADO")

sys.exit(0)
