import requests
import os

# -----------------------------
# TOKENS
# -----------------------------

BSALE_TOKEN = os.getenv("BSALE_TOKEN_Mini")

NOCODB_TOKEN = "R3EhSD8si-WSVdsPxlQVGAfiHRRcDR9cHGHJdBJL"
NOCODB_URL = "https://db.quillotana.cl"

# -----------------------------
# TABLE IDs
# -----------------------------

TABLE_OFFICES = "vw2fhxbbf0iy2w3y"
TABLE_PRICELISTS = "vw0prq57ai2ov1b4"
TABLE_TAXES = "vwtcuh3t7gdl1r1x"

# -----------------------------
# HEADERS
# -----------------------------

headers_bsale = {
    "access_token": BSALE_TOKEN
}

headers_noco = {
    "xc-token": NOCODB_TOKEN,
    "Content-Type": "application/json"
}

BASE_BSALE = "https://api.bsale.io/v1"

limit = 50


# -----------------------------
# FUNCION PAGINACION BSALE
# -----------------------------

def fetch_all(endpoint):

    offset = 0
    all_items = []

    while True:

        url = f"{BASE_BSALE}/{endpoint}?limit={limit}&offset={offset}"

        r = requests.get(url, headers=headers_bsale)
        data = r.json()

        items = data.get("items", [])

        if len(items) == 0:
            break

        all_items.extend(items)

        offset += limit

    return all_items


# -----------------------------
# INSERTAR EN NOCO
# -----------------------------

def insert_noco(table_id, payload):

    url = f"{NOCODB_URL}/api/v2/tables/{table_id}/records"

    r = requests.post(url, json=payload, headers=headers_noco)

    if r.status_code not in [200, 201]:
        print("ERROR:", r.text)


# -----------------------------
# SYNC OFFICES
# -----------------------------

print("\nSYNC OFFICES")

offices = fetch_all("offices.json")

for o in offices:

    payload = {
        "id": o["id"],
        "name": o.get("name"),
        "state": o.get("state")
    }

    insert_noco(TABLE_OFFICES, payload)


# -----------------------------
# SYNC TAXES
# -----------------------------

print("\nSYNC TAXES")

taxes = fetch_all("taxes.json")

for t in taxes:

    payload = {
        "id": t["id"],
        "name": t.get("name"),
        "percentage": t.get("percentage"),
        "state": t.get("state")
    }

    insert_noco(TABLE_TAXES, payload)


# -----------------------------
# SYNC PRICE LISTS
# -----------------------------

print("\nSYNC PRICE LISTS")

price_lists = fetch_all("price_lists.json")

for p in price_lists:

    payload = {
        "id": p["id"],
        "name": p.get("name"),
        "description": p.get("description"),
        "state": p.get("state")
    }

    insert_noco(TABLE_PRICELISTS, payload)


print("\nSYNC COMPLETADO")
