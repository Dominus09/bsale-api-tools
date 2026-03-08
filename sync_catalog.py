import requests
import os
import json

print("SYNC CATALOG")

BASE = "https://api.bsale.io/v1"
NOCODB = "https://db.quillotana.cl"

BSALE_TOKEN = os.getenv("BSALE_TOKEN_Mini")
NOCODB_TOKEN = os.getenv("NocoDB_token")

HEAD_BSALE = {"access_token": BSALE_TOKEN}

HEAD_NOCO = {
    "xc-token": NOCODB_TOKEN,
    "Content-Type": "application/json"
}

TABLE_PRODUCTS = "meke3fsng90uspe"
TABLE_PRODUCT_TYPES = "mcir9ile6id3813"
TABLE_TAXES = "mary3rk9y5rwviu"

LIMIT = 50


def bsale_get(url, params=None):

    r = requests.get(url, headers=HEAD_BSALE, params=params)

    r.raise_for_status()

    return r.json()


def insert(table, payload):

    url = f"{NOCODB}/api/v2/tables/{table}/records"

    r = requests.post(url, headers=HEAD_NOCO, json=payload)

    if r.status_code not in [200, 201]:
        print("INSERT ERROR", r.text)


# -----------------
# TAXES
# -----------------

print("LOAD TAXES")

taxes = bsale_get(f"{BASE}/taxes.json")

tax_map = {}

for t in taxes["items"]:

    tax_map[int(t["id"])] = {
        "name": t["name"],
        "percentage": float(t["percentage"])
    }

    insert(TABLE_TAXES, {
        "bsale_id": t["id"],
        "name": t["name"],
        "percentage": t["percentage"]
    })


# -----------------
# PRODUCT TYPES
# -----------------

print("LOAD PRODUCT TYPES")

types = bsale_get(f"{BASE}/product_types.json")

for pt in types["items"]:

    insert(TABLE_PRODUCT_TYPES, {
        "bsale_id": pt["id"],
        "name": pt["name"],
        "state": pt["state"]
    })


# -----------------
# PRODUCTS
# -----------------

print("LOAD PRODUCTS")

offset = 0

while True:

    data = bsale_get(
        f"{BASE}/products.json",
        {"limit": LIMIT, "offset": offset}
    )

    items = data.get("items", [])

    if not items:
        break

    for p in items:

        tax_ids = []
        tax_names = []
        tax_total = 0

        if "product_taxes" in p and "href" in p["product_taxes"]:

            tax_data = bsale_get(p["product_taxes"]["href"])

            for t in tax_data["items"]:

                tax_id = int(t["tax"]["id"])

                tax_ids.append(tax_id)

                tax_names.append(tax_map[tax_id]["name"])

                tax_total += tax_map[tax_id]["percentage"]

        tax_factor = 1 + (tax_total / 100)

        insert(TABLE_PRODUCTS, {
            "bsale_id": p["id"],
            "name": p["name"],
            "product_type_id": p["product_type"]["id"] if p.get("product_type") else None,
            "tax_ids_json": json.dumps(tax_ids),
            "tax_names_json": json.dumps(tax_names),
            "tax_factor": round(tax_factor, 3)
        })

    offset += LIMIT

print("CATALOG DONE")
