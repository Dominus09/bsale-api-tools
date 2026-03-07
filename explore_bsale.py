import requests
import os
import json

print("SYNC PRODUCT TYPES + TAXES")

BSALE_TOKEN = os.getenv("BSALE_TOKEN_Mini")
NOCODB_TOKEN = "R3EhSD8si-WSVdsPxlQVGAfiHRRcDR9cHGHJdBJL"
NOCODB_URL = "https://db.quillotana.cl"
BASE_BSALE = "https://api.bsale.io/v1"

headers_bsale = {
    "access_token": BSALE_TOKEN
}

headers_noco = {
    "xc-token": NOCODB_TOKEN,
    "Content-Type": "application/json"
}

TABLE_PRODUCTS = "meke3fsng90uspe"
TABLE_PRODUCT_TYPES = "mcir9ile6id3813"
TABLE_TAXES = "mary3rk9y5rwviu"

LIMIT = 50


def bsale_get(url, params=None):
    r = requests.get(url, headers=headers_bsale, params=params)
    r.raise_for_status()
    return r.json()


def fetch_all(endpoint):
    offset = 0
    results = []

    while True:

        data = bsale_get(
            f"{BASE_BSALE}/{endpoint}",
            {"limit": LIMIT, "offset": offset}
        )

        items = data.get("items", [])

        if not items:
            break

        results.extend(items)
        offset += LIMIT

    return results


def insert_noco(table_id, payload):

    url = f"{NOCODB_URL}/api/v2/tables/{table_id}/records"

    r = requests.post(url, json=payload, headers=headers_noco)

    if r.status_code not in [200, 201]:
        print("INSERT ERROR", r.text)


# -------------------------
# LOAD TAXES
# -------------------------

print("LOADING TAXES")

taxes = fetch_all("taxes.json")

tax_map = {}

for t in taxes:

    tax_map[t["id"]] = {
        "name": t["name"],
        "percentage": t["percentage"]
    }

print("TAXES:", len(tax_map))


# -------------------------
# PRODUCT TYPES
# -------------------------

print("SYNC PRODUCT TYPES")

product_types = fetch_all("product_types.json")

for pt in product_types:

    insert_noco(TABLE_PRODUCT_TYPES, {
        "bsale_id": pt["id"],
        "name": pt["name"],
        "state": pt["state"]
    })

print("PRODUCT TYPES:", len(product_types))


# -------------------------
# PRODUCTS
# -------------------------

print("SYNC PRODUCTS")

products = fetch_all("products.json")

for p in products:

    product_id = p["id"]

    # product type
    product_type_id = None

    if p.get("product_type"):
        product_type_id = p["product_type"]["id"]

    # taxes
    tax_ids = []
    tax_names = []
    tax_factor = 1

    for pt in p.get("product_taxes", []):

        tax_id = pt["tax"]["id"]

        tax_ids.append(tax_id)

        if tax_id in tax_map:

            tax_names.append(tax_map[tax_id]["name"])

            tax_factor *= 1 + (tax_map[tax_id]["percentage"] / 100)

    insert_noco(TABLE_PRODUCTS, {

        "bsale_id": product_id,
        "name": p["name"],
        "product_type_id": product_type_id,
        "tax_ids_json": json.dumps(tax_ids),
        "tax_names_json": json.dumps(tax_names),
        "tax_factor": tax_factor

    })


print("PRODUCTS:", len(products))

print("SYNC COMPLETADO")
