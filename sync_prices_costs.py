import requests
import os
import time
from datetime import datetime

print("SYNC COSTS + PRICES")

BASE = "https://api.bsale.io/v1"
NOCODB = "https://db.quillotana.cl"

BSALE_TOKEN = os.getenv("BSALE_TOKEN_Mini")
NOCODB_TOKEN = os.getenv("NocoDB_token")

HEAD_BSALE = {"access_token": BSALE_TOKEN}
HEAD_NOCO = {"xc-token": NOCODB_TOKEN, "Content-Type": "application/json"}

BSALE_LIMIT = 50
NOCO_LIMIT = 200

TABLE_VARIANTS = "msd4vvijzk9pre9"
TABLE_PRODUCTS = "meke3fsng90uspe"
TABLE_COSTS = "mdjjvdlwev2o76u"
TABLE_PRICES = "mcby3npgc3ig042"


# -----------------------------------------------------
# SAFE REQUEST BSALE
# -----------------------------------------------------

def bsale_get(url, params=None):

    while True:

        r = requests.get(url, headers=HEAD_BSALE, params=params)

        if r.status_code == 429:

            wait = 60

            try:
                wait = int(r.json().get("retry_after", 60))
            except:
                pass

            print("RATE LIMIT WAIT", wait)
            time.sleep(wait)
            continue

        if r.status_code != 200:

            print("BSALE ERROR", r.text)
            time.sleep(5)
            continue

        return r.json()


# -----------------------------------------------------
# NOCO GET ALL
# -----------------------------------------------------

def noco_get_all(table):

    url = f"{NOCODB}/api/v2/tables/{table}/records"

    offset = 0
    rows = []

    while True:

        r = requests.get(
            url,
            headers=HEAD_NOCO,
            params={"limit": NOCO_LIMIT, "offset": offset}
        )

        data = r.json()

        batch = data.get("list", [])

        if not batch:
            break

        rows.extend(batch)

        offset += NOCO_LIMIT

    return rows


# -----------------------------------------------------
# INSERT
# -----------------------------------------------------

def insert(table, payload):

    url = f"{NOCODB}/api/v2/tables/{table}/records"

    r = requests.post(url, headers=HEAD_NOCO, json=payload)

    if r.status_code not in [200, 201]:
        print("INSERT ERROR", r.text)


# -----------------------------------------------------
# UPDATE
# -----------------------------------------------------

def update(table, row_id, payload):

    url = f"{NOCODB}/api/v2/tables/{table}/records"

    payload["Id"] = row_id

    r = requests.patch(url, headers=HEAD_NOCO, json=payload)

    if r.status_code not in [200, 201]:
        print("UPDATE ERROR", r.text)


# -----------------------------------------------------
# LOAD PRODUCTS (solo tax_factor)
# -----------------------------------------------------

print("LOAD TAX FACTOR")

products = noco_get_all(TABLE_PRODUCTS)

tax_map = {}

for p in products:
    tax_map[p["bsale_id"]] = p.get("tax_factor", 1)


# -----------------------------------------------------
# LOAD VARIANTS (desde NOCO)
# -----------------------------------------------------

print("LOAD VARIANTS")

variants = noco_get_all(TABLE_VARIANTS)

print("VARIANTS:", len(variants))


# -----------------------------------------------------
# LOAD EXISTING COSTS
# -----------------------------------------------------

print("LOAD EXISTING COSTS")

cost_rows = noco_get_all(TABLE_COSTS)

cost_map = {}

for r in cost_rows:
    cost_map[r["variant_id"]] = r["Id"]


# -----------------------------------------------------
# LOAD EXISTING PRICES
# -----------------------------------------------------

print("LOAD EXISTING PRICES")

price_rows = noco_get_all(TABLE_PRICES)

price_map = {}

for r in price_rows:

    key = f"{r['variant_id']}_{r['price_list_id']}"

    price_map[key] = r["Id"]


# -----------------------------------------------------
# SYNC COSTS
# -----------------------------------------------------

print("SYNC COSTS")

for v in variants:

    variant_id = v["bsale_id"]
    product_id = v["product_id"]

    data = bsale_get(f"{BASE}/variants/{variant_id}/costs.json")

    avg_cost = data.get("averageCost")

    tax_factor = tax_map.get(product_id, 1)

    cost_gross = None

    if avg_cost:
        cost_gross = avg_cost * tax_factor

    payload = {
        "variant_id": variant_id,
        "average_cost_net": avg_cost,
        "average_cost_gross": cost_gross,
        "tax_factor": tax_factor,
        "last_update": datetime.utcnow().isoformat()
    }

    if variant_id in cost_map:
        update(TABLE_COSTS, cost_map[variant_id], payload)
    else:
        insert(TABLE_COSTS, payload)


print("COSTS DONE")


# -----------------------------------------------------
# SYNC PRICES
# -----------------------------------------------------

print("SYNC PRICES")

data = bsale_get(f"{BASE}/price_lists.json")

price_lists = data.get("items", [])

for pl in price_lists:

    price_list_id = pl["id"]

    offset = 0

    while True:

        data = bsale_get(
            f"{BASE}/price_lists/{price_list_id}/details.json",
            {"limit": BSALE_LIMIT, "offset": offset}
        )

        items = data.get("items", [])

        if not items:
            break

        for d in items:

            variant_id = d["variant"]["id"]

            key = f"{variant_id}_{price_list_id}"

            payload = {
                "variant_id": variant_id,
                "price_list_id": price_list_id,
                "price_net": d["variantValue"],
                "price_gross": d["variantValueWithTaxes"]
            }

            if key in price_map:
                update(TABLE_PRICES, price_map[key], payload)
            else:
                insert(TABLE_PRICES, payload)

        offset += BSALE_LIMIT


print("PRICES DONE")
print("SYNC COMPLETED")
