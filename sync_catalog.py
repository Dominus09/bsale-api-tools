import requests
import os
import time

print("SYNC CATALOG START")

BASE = "https://api.bsale.io/v1"
NOCODB = "https://db.quillotana.cl"

BSALE_TOKEN = os.getenv("BSALE_TOKEN_Mini")
NOCODB_TOKEN = os.getenv("NocoDB_token")

LIMIT = 50

HEAD_BSALE = {"access_token": BSALE_TOKEN}

HEAD_NOCO = {
    "xc-token": NOCODB_TOKEN,
    "Content-Type": "application/json"
}

TABLE_PRODUCTS = "meke3fsng90uspe"
TABLE_VARIANTS = "msd4vvijzk9pre9"
TABLE_TAXES = "mary3rk9y5rwviu"
TABLE_OFFICES = "m878eot7j6fi5v7"
TABLE_PRICELIST = "m8zibme0z28jls6"


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


def fetch(endpoint):

    offset = 0
    results = []

    while True:

        data = bsale_get(
            f"{BASE}/{endpoint}",
            {"limit": LIMIT, "offset": offset}
        )

        items = data.get("items", [])

        if not items:
            break

        results.extend(items)
        offset += LIMIT

    return results


def upsert(table, field, value, payload):

    url = f"{NOCODB}/api/v2/tables/{table}/records"

    r = requests.get(
        url,
        headers=HEAD_NOCO,
        params={"where": f"({field},eq,{value})"}
    )

    data = r.json()

    if data["list"]:

        row_id = data["list"][0]["Id"]

        requests.patch(
            f"{url}/{row_id}",
            json=payload,
            headers=HEAD_NOCO
        )

    else:

        requests.post(
            url,
            json=payload,
            headers=HEAD_NOCO
        )


print("SYNC TAXES")

for t in fetch("taxes.json"):

    upsert(
        TABLE_TAXES,
        "bsale_id",
        t["id"],
        {
            "bsale_id": t["id"],
            "name": t["name"],
            "percentage": t["percentage"]
        }
    )


print("SYNC OFFICES")

for o in fetch("offices.json"):

    upsert(
        TABLE_OFFICES,
        "bsale_id",
        o["id"],
        {
            "bsale_id": o["id"],
            "name": o["name"]
        }
    )


print("SYNC PRICE LIST")

for p in fetch("price_lists.json"):

    upsert(
        TABLE_PRICELIST,
        "bsale_id",
        p["id"],
        {
            "bsale_id": p["id"],
            "name": p["name"]
        }
    )


print("SYNC PRODUCTS")

products = fetch("products.json")

for p in products:

    upsert(
        TABLE_PRODUCTS,
        "bsale_id",
        p["id"],
        {
            "bsale_id": p["id"],
            "name": p["name"]
        }
    )


print("SYNC VARIANTS")

variants = fetch("variants.json")

for v in variants:

    upsert(
        TABLE_VARIANTS,
        "bsale_id",
        v["id"],
        {
            "bsale_id": v["id"],
            "product_id": v["product"]["id"],
            "description": v.get("description"),
            "code": v.get("code"),
            "bar_code": v.get("barCode")
        }
    )

print("SYNC CATALOG DONE")
