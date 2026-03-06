import requests
import os

BSALE_TOKEN = os.getenv("BSALE_TOKEN_Mini")

NOCODB_URL = "https://db.quillotana.cl"
NOCODB_TOKEN = "R3EhSD8si-WSVdsPxlQVGAfiHRRcDR9cHGHJdBJL"

BASE_ID = "pbgqx7nu11fz6wz"

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


def insert_noco(table, payload):

    url = f"{NOCODB_URL}/api/v2/tables/{table}/records"

    r = requests.post(url, json=payload, headers=headers_noco)

    if r.status_code != 200:
        print("ERROR:", r.text)


print("\nSYNC OFFICES")

offices = fetch_all("offices.json")

for o in offices:

    payload = {
        "id": o["id"],
        "name": o.get("name"),
        "state": o.get("state")
    }

    insert_noco("offices", payload)


print("\nSYNC TAXES")

taxes = fetch_all("taxes.json")

for t in taxes:

    payload = {
        "id": t["id"],
        "name": t.get("name"),
        "percentage": t.get("percentage"),
        "state": t.get("state")
    }

    insert_noco("taxes", payload)


print("\nSYNC PRICE LISTS")

price_lists = fetch_all("price_lists.json")

for p in price_lists:

    payload = {
        "id": p["id"],
        "name": p.get("name"),
        "description": p.get("description"),
        "state": p.get("state")
    }

    insert_noco("price_lists", payload)


print("\nSYNC COMPLETADO")
