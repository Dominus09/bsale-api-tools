import requests
import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

print("SYNC CLIENTS")

BASE = "https://api.bsale.io/v1"
NOCODB = "https://db.quillotana.cl"

BSALE_TOKEN = os.getenv("BSALE_TOKEN_SPA")
NOCODB_TOKEN = os.getenv("NocoDB_token")

TABLE_CLIENTS = "mmauyzswrd2hi1b"

HEAD_BSALE = {
    "access_token": BSALE_TOKEN,
    "Content-Type": "application/json"
}

HEAD_NOCO = {
    "xc-token": NOCODB_TOKEN,
    "Content-Type": "application/json"
}

# -------- UPSERT --------

def upsert_client(row):

    bsale_id = row["bsale_id"]

    url = f"{NOCODB}/api/v2/tables/{TABLE_CLIENTS}/records"

    payload = {
        "where": f"(bsale_id,eq,{bsale_id})",
        "records": [row]
    }

    r = requests.patch(url, headers=HEAD_NOCO, json=payload)

    if r.status_code not in [200,201]:
        print("NOCODB ERROR")
        print(r.text)

# -------- ATTRIBUTES --------

def get_attributes(bsale_id):

    r = requests.get(
        f"{BASE}/clients/{bsale_id}/attributes.json",
        headers=HEAD_BSALE
    )

    attr_data = r.json()

    attrs = {
        "dia_atencion": None,
        "nombre_fantasia": None,
        "vendedor": None
    }

    for attr in attr_data.get("items", []):

        name = attr.get("name","").strip()
        value = attr.get("value")

        if name == "Dia Atencion":
            attrs["dia_atencion"] = value

        elif name == "NOMBRE DE FANTASÍA":
            attrs["nombre_fantasia"] = value

        elif name == "Vendedor":
            attrs["vendedor"] = value

    return attrs

# -------- PROCESS CLIENT --------

def process_client(client):

    bsale_id = client["id"]

    created_raw = client.get("createdAt")
    updated_raw = client.get("updatedAt")

    created = datetime.fromtimestamp(int(created_raw)).strftime("%Y-%m-%d %H:%M:%S") if created_raw else None
    updated = datetime.fromtimestamp(int(updated_raw)).strftime("%Y-%m-%d %H:%M:%S") if updated_raw else None

    attrs = get_attributes(bsale_id)

    row = {
        "bsale_id": bsale_id,
        "first_name": client.get("firstName"),
        "last_name": client.get("lastName"),
        "code": client.get("code"),
        "phone": client.get("phone"),
        "company": client.get("company"),
        "facebook": client.get("facebook"),
        "city": client.get("city"),
        "municipality": client.get("municipality"),
        "address": client.get("address"),
        "created": created,
        "updated": updated,
        "dia_atencion": attrs["dia_atencion"],
        "nombre_fantasia": attrs["nombre_fantasia"],
        "vendedor": attrs["vendedor"]
    }

    upsert_client(row)

# -------- MAIN --------

limit = 50
offset = 0

while True:

    params = {
        "limit": limit,
        "offset": offset
    }

    r = requests.get(
        f"{BASE}/clients.json",
        headers=HEAD_BSALE,
        params=params
    )

    data = r.json()

    clients = data.get("items", [])

    if not clients:
        break

    print("CLIENTS:", len(clients))

    with ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(process_client, clients)

    offset += limit

print("SYNC CLIENTS DONE")
