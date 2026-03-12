import requests
import time
from datetime import datetime

# -------- BSALE --------

BSALE_TOKEN = "TU_TOKEN"

BSALE_HEADERS = {
    "access_token": BSALE_TOKEN,
    "Content-Type": "application/json"
}

BSALE_URL = "https://api.bsale.cl/v1/clients.json"


# -------- NOCODB --------

NOCODB = "https://db.quillotana.cl"
NOCODB_TOKEN = "TU_TOKEN"
TABLE = "TABLE_ID_CLIENTS"

NOCODB_HEADERS = {
    "xc-token": NOCODB_TOKEN,
    "Content-Type": "application/json"
}


def upsert_client(row):

    url = f"{NOCODB}/api/v2/tables/{TABLE}/records"

    payload = {
        "records": [row]
    }

    r = requests.post(url, headers=NOCODB_HEADERS, json=payload)

    if r.status_code not in [200, 201]:
        print("ERROR NOCODB")
        print(r.text)


limit = 50
offset = 0

while True:

    params = {
        "limit": limit,
        "offset": offset
    }

    r = requests.get(BSALE_URL, headers=BSALE_HEADERS, params=params)
    data = r.json()

    clients = data.get("items", [])

    if not clients:
        break

    for client in clients:

        bsale_id = client["id"]

        created_raw = client.get("createdAt")
        updated_raw = client.get("updatedAt")

        created = datetime.fromtimestamp(int(created_raw)).strftime("%Y-%m-%d %H:%M:%S") if created_raw else None
        updated = datetime.fromtimestamp(int(updated_raw)).strftime("%Y-%m-%d %H:%M:%S") if updated_raw else None

        client_data = {
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

        attr_url = f"https://api.bsale.cl/v1/clients/{bsale_id}/attributes.json"

        r_attr = requests.get(attr_url, headers=BSALE_HEADERS)
        attr_data = r_attr.json()

        for attr in attr_data.get("items", []):

            name = attr.get("name", "").strip()
            value = attr.get("value")

            if name == "Dia Atencion":
                client_data["dia_atencion"] = value

            elif name == "NOMBRE DE FANTASÍA":
                client_data["nombre_fantasia"] = value

            elif name == "Vendedor":
                client_data["vendedor"] = value

        upsert_client(client_data)

        time.sleep(0.15)

    offset += limit
