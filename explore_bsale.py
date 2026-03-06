import requests
import os

# ----------------------
# TOKENS
# ----------------------

BSALE_TOKEN = os.getenv("BSALE_TOKEN_Mini")

NOCODB_TOKEN = "R3EhSD8si-WSVdsPxlQVGAfiHRRcDR9cHGHJdBJL"
NOCODB_URL = "https://db.quillotana.cl"

# ----------------------
# TABLE IDS
# ----------------------

TABLE_OFFICES = "m878eot7j6fi5v7"
TABLE_PRICELIST = "m8zibme0z28jls6"
TABLE_TAXES = "mary3rk9y5rwviu"

# ----------------------
# HEADERS
# ----------------------

headers_bsale = {
    "access_token": BSALE_TOKEN
}

headers_noco = {
    "xc-token": NOCODB_TOKEN,
    "Content-Type": "application/json"
}

BASE_BSALE = "https://api.bsale.io/v1"

limit = 50


# ----------------------
# PAGINACION BSALE
# ----------------------

def fetch_all(endpoint):

    offset = 0
    items_all = []

    while True:

        url = f"{BASE_BSALE}/{endpoint}?limit={limit}&offset={offset}"

        r = requests.get(url, headers=headers_bsale)
        data = r.json()

        items = data.get("items", [])

        if not items:
            break

        items_all.extend(items)
        offset += limit

    return items_all


# ----------------------
# INSERTAR EN NOCO
# ----------------------

def insert_noco(table_id, payload):

    url = f"{NOCODB_URL}/api/v2/tables/{table_id}/records"

    r = requests.post(url, json=payload, headers=headers_noco)

    if r.status_code not in [200, 201]:
        print("ERROR:", r.text)


# ----------------------
# OFFICES
# ----------------------

print("SYNC OFFICES")

for o in fetch_all("offices.json"):

    insert_noco(TABLE_OFFICES, {
        "id": o["id"],
        "name": o.get("name"),
        "state": o.get("state")
    })


# ----------------------
# TAXES
# ----------------------

print("SYNC TAXES")

for t in fetch_all("taxes.json"):

    insert_noco(TABLE_TAXES, {
        "id": t["id"],
        "name": t.get("name"),
        "percentage": t.get("percentage"),
        "state": t.get("state")
    })


# ----------------------
# PRICE LISTS
# ----------------------

print("SYNC PRICE LISTS")

for p in fetch_all("price_lists.json"):

    insert_noco(TABLE_PRICELIST, {
        "id": p["id"],
        "name": p.get("name"),
        "description": p.get("description"),
        "state": p.get("state")
    })


print("SYNC COMPLETADO")
