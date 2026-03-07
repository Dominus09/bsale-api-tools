import requests
import os
import json
import time

print("SYNC CATALOG")

BSALE_TOKEN = os.getenv("BSALE_TOKEN_Mini")
NOCODB_TOKEN = os.getenv("NocoDB_token")

BASE_BSALE = "https://api.bsale.io/v1"
NOCODB = "https://db.quillotana.cl"

LIMIT = 50

headers_bsale = {"access_token": BSALE_TOKEN}

headers_noco = {
    "xc-token": NOCODB_TOKEN,
    "Content-Type": "application/json"
}

TABLE_PRODUCTS = "meke3fsng90uspe"
TABLE_VARIANTS = "msd4vvijzk9pre9"
TABLE_TAXES = "mary3rk9y5rwviu"
TABLE_OFFICES = "m878eot7j6fi5v7"
TABLE_PRICELIST = "m8zibme0z28jls6"

# -------------------

def bsale_get(url, params=None):

    while True:

        r = requests.get(url, headers=headers_bsale, params=params)

        if r.status_code == 429:

            retry = int(r.json().get("retry_after",60))
            print("RATE LIMIT WAIT", retry)
            time.sleep(retry)
            continue

        r.raise_for_status()
        time.sleep(0.2)

        return r.json()

# -------------------

def fetch_all(endpoint):

    offset = 0
    results = []

    while True:

        data = bsale_get(
            f"{BASE_BSALE}/{endpoint}",
            {"limit":LIMIT,"offset":offset}
        )

        items = data.get("items",[])

        if not items:
            break

        results.extend(items)
        offset += LIMIT

    return results

# -------------------

def insert(table,data):

    url=f"{NOCODB}/api/v2/tables/{table}/records"

    requests.post(url,json=data,headers=headers_noco)

# -------------------
# TAXES
# -------------------

print("TAXES")

taxes=fetch_all("taxes.json")

for t in taxes:

    insert(TABLE_TAXES,{
        "bsale_id":t["id"],
        "name":t["name"],
        "percentage":t["percentage"]
    })

# -------------------
# PRODUCTS
# -------------------

print("PRODUCTS")

products=fetch_all("products.json")

for p in products:

    insert(TABLE_PRODUCTS,{
        "bsale_id":p["id"],
        "name":p["name"]
    })

# -------------------
# VARIANTS
# -------------------

print("VARIANTS")

variants=fetch_all("variants.json")

for v in variants:

    insert(TABLE_VARIANTS,{
        "bsale_id":v["id"],
        "product_id":v["product"]["id"],
        "description":v["description"]
    })

print("CATALOG DONE")
