import requests
import os
import time
from datetime import datetime

print("SYNC CLIENTS")

# -------- CONFIG --------

BASE = "https://api.bsale.io/v1"
NOCODB = "https://db.quillotana.cl"

BSALE_TOKEN = os.getenv("Bsale_token")
NOCODB_TOKEN = os.getenv("NocoDB_token")

TABLE_CLIENTS = "mmauyzswrd2hi1b"


# -------- HEADERS --------

HEAD_BSALE = {
    "access_token": BSALE_TOKEN,
    "Content-Type": "application/json"
}

HEAD_NOCO = {
    "xc-token": NOCODB_TOKEN,
    "Content-Type": "application/json"
}


# -------- UPSERT NOCODB --------

def upsert_client(row):

    url = f"{NOCODB}/api/v2/tables/{TABLE_CLIENTS}/records"

    payload = {
        "records": [row]
    }

    r = requests.post(url, headers=HEAD_NOCO, json=payload)

    if r.status_code not in [200, 201]:
        print("NOCODB ERROR")
        print(r.text)


# -------- SYNC CLIENTS --------

limit = 50
offset = 0

while True:

    params = {
        "limit": limit,
        "offset": offset
    }

    r = requests.get(f"{BASE}/clients.json", headers=HEAD_BSALE, params=params)

    data = r.json()

    clients = data.get("items", [])

    if not clients:
        break

    print("CLIENTS:", len(clients))

    for client in clients:

        bsale_id = client["id"]

        created_raw = client.get("createdAt")
        updated_raw = client.get("updatedAt")

        created = datetime.fromtimestamp(int(created_raw)).strftime("%Y-%m-%d %H:%M:%S") if created_raw else None
        updated = datetime.fromtimestamp(int(updated_raw)).strftime("%Y-%m-%d %H:%M:%S") if updated_raw else None

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
            "dia_atencion": None,
            "nombre_fantasia": None,
            "vendedor": None
        }

        # -------- ATRIBUTOS --------

        r_attr = requests.get(
            f"{BASE}/clients/{bsale_id}/attributes.json",
            headers=HEAD_BSALE
        )

        attr_data = r_attr.json()

        for attr in attr_data.get("items", []):

            name = attr.get("name", "").strip()
            value = attr.get("value")

            if name == "Dia Atencion":
                row["dia_atencion"] = value

            elif name == "NOMBRE DE FANTASÍA":
                row["nombre_fantasia"] = value

            elif name == "Vendedor":
                row["vendedor"] = value

        upsert_client(row)

        time.sleep(0.15)

    offset += limit

print("SYNC CLIENTS DONE")
