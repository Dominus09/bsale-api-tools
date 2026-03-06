print("START SYNC")
import requests
import os

# TOKENS

BSALE_TOKEN = os.getenv("BSALE_TOKEN_Mini")

NOCODB_TOKEN = "R3EhSD8si-WSVdsPxlQVGAfiHRRcDR9cHGHJdBJL"
NOCODB_URL = "https://db.quillotana.cl"

# TABLE IDs

TABLE_OFFICES = "m878eot7j6fi5v7"
TABLE_PRICELIST = "m8zibme0z28jls6"
TABLE_TAXES = "mary3rk9y5rwviu"

headers_bsale = {
    "access_token": BSALE_TOKEN
}

headers_noco = {
    "xc-token": NOCODB_TOKEN,
    "Content-Type": "application/json"
}

BASE_BSALE = "https://api.bsale.io/v1"

limit = 50


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


def upsert_noco(table_id, bsale_id, payload):

    search_url = f"{NOCODB_URL}/api/v2/tables/{table_id}/records?where=(bsale_id,eq,{bsale_id})"

    r = requests.get(search_url, headers=headers_noco)
    data = r.json()

    if data["list"]:

        record_id = data["list"][0]["Id"]

        payload["Id"] = record_id

        update_url = f"{NOCODB_URL}/api/v2/tables/{table_id}/records"

        requests.patch(update_url, json=payload, headers=headers_noco)

    else:

        insert_url = f"{NOCODB_URL}/api/v2/tables/{table_id}/records"

        requests.post(insert_url, json=payload, headers=headers_noco)


# OFFICES

print("SYNC OFFICES")

for o in fetch_all("offices.json"):

    upsert_noco(TABLE_OFFICES, o["id"], {

        "bsale_id": o["id"],
        "name": o.get("name")

    })


# TAXES

print("SYNC TAXES")

for t in fetch_all("taxes.json"):

    upsert_noco(TABLE_TAXES, t["id"], {

        "bsale_id": t["id"],
        "name": t.get("name"),
        "percentage": t.get("percentage")

    })


# PRICE LIST

print("SYNC PRICE LIST")

for p in fetch_all("price_lists.json"):

    upsert_noco(TABLE_PRICELIST, p["id"], {

        "bsale_id": p["id"],
        "name": p.get("name"),
        "description": p.get("description")

    })


print("SYNC COMPLETADO")
exit()
