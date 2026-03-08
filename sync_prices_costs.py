import requests
import os
import time

print("SYNC PRICES + COSTS")

BASE = "https://api.bsale.io/v1"
NOCODB = "https://db.quillotana.cl"

BSALE_TOKEN = os.getenv("BSALE_TOKEN_Mini")
NOCODB_TOKEN = os.getenv("NocoDB_token")

HEAD_BSALE = {"access_token": BSALE_TOKEN}

HEAD_NOCO = {
    "xc-token": NOCODB_TOKEN,
    "Content-Type": "application/json"
}

TABLE_PRICES = "mcby3npgc3ig042"
TABLE_COSTS = "mdjjvdlwev2o76u"
TABLE_COST_HISTORY = "mdfyfwrrrwffg43"

LIMIT = 50


# -------------------------
# HELPERS
# -------------------------

def bsale_get(url, params=None):

    while True:

        r = requests.get(url, headers=HEAD_BSALE, params=params)

        if r.status_code == 429:
            retry = int(r.json().get("retry_after", 60))
            print("RATE LIMIT", retry)
            time.sleep(retry)
            continue

        r.raise_for_status()
        return r.json()


def noco_get(url, params=None):

    r = requests.get(url, headers=HEAD_NOCO, params=params)

    r.raise_for_status()

    return r.json()


def insert(table, payload):

    url = f"{NOCODB}/api/v2/tables/{table}/records"

    r = requests.post(url, headers=HEAD_NOCO, json=payload)

    if r.status_code not in [200, 201]:
        print("INSERT ERROR", r.text)


def update(table, row_id, payload):

    url = f"{NOCODB}/api/v2/tables/{table}/records"

    payload["Id"] = row_id

    r = requests.patch(url, headers=HEAD_NOCO, json=payload)

    if r.status_code not in [200, 201]:
        print("UPDATE ERROR", r.text)


# -------------------------
# UPSERT
# -------------------------

def upsert(table, filters, payload):

    url = f"{NOCODB}/api/v2/tables/{table}/records"

    data = noco_get(url, filters)

    rows = data.get("list", [])

    if rows:

        row_id = rows[0]["Id"]

        update(table, row_id, payload)

    else:

        insert(table, payload)


# -------------------------
# VARIANTS
# -------------------------

print("LOAD VARIANTS")

offset = 0
variants = []

while True:

    data = bsale_get(
        f"{BASE}/variants.json",
        {"limit": LIMIT, "offset": offset}
    )

    items = data.get("items", [])

    if not items:
        break

    variants.extend(items)

    offset += LIMIT

print("VARIANTS:", len(variants))


# -------------------------
# COSTS
# -------------------------

print("SYNC COSTS")

for v in variants:

    variant_id = v["id"]

    cost = bsale_get(
        f"{BASE}/variants/{variant_id}/costs.json"
    )

    payload = {
        "variant_id": variant_id,
        "average_cost_net": cost.get("averageCost"),
        "total_cost_net": cost.get("totalCost")
    }

    upsert(
        TABLE_COSTS,
        {"where": f"(variant_id,eq,{variant_id})"},
        payload
    )

    history = cost.get("history", [])

    for h in history:

        payload_hist = {
            "variant_id": variant_id,
            "cost_net": h["cost"],
            "fifo_quantity": h["availableFifo"],
            "entry_date": h["admissionDate"]
        }

        insert(TABLE_COST_HISTORY, payload_hist)

print("COSTS DONE")


# -------------------------
# PRICES
# -------------------------

print("SYNC PRICES")

lists = bsale_get("https://api.bsale.io/v1/price_lists.json")

for pl in lists["items"]:

    price_list_id = pl["id"]

    offset = 0

    while True:

        data = bsale_get(
            f"{BASE}/price_lists/{price_list_id}/details.json",
            {"limit": LIMIT, "offset": offset}
        )

        items = data.get("items", [])

        if not items:
            break

        for p in items:

            variant_id = int(p["variant"]["id"])

            payload = {
                "variant_id": variant_id,
                "price_list_id": price_list_id,
                "price_net": p["variantValue"],
                "price_gross": p["variantValueWithTaxes"]
            }

            upsert(
                TABLE_PRICES,
                {
                    "where":
                    f"(variant_id,eq,{variant_id})~and(price_list_id,eq,{price_list_id})"
                },
                payload
            )

        offset += LIMIT

print("PRICES DONE")
