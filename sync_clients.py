import requests
import os
import time
from datetime import datetime

print("SYNC CLIENTS START")

BASE = "https://api.bsale.io/v1"
NOCODB = "https://db.quillotana.cl"

NOCODB_TOKEN = os.getenv("NocoDB_token")
BSALE_TOKEN = os.getenv("BSALE_TOKEN_SPA")

TABLE_CLIENTS = "mmauyzswrd2hi1b"

LIMIT_BSALE = 50
LIMIT_NOCO = 200
BATCH = 100

HEAD_NOCO = {
    "xc-token": NOCODB_TOKEN,
    "Content-Type": "application/json"
}

HEAD_BSALE = {
    "access_token": BSALE_TOKEN
}

# -------------------------------------------------
# NOCO GET EXISTING
# -------------------------------------------------

def noco_get_all():

    url = f"{NOCODB}/api/v2/tables/{TABLE_CLIENTS}/records"

    offset = 0
    existing = {}

    while True:

        r = requests.get(
            url,
            headers=HEAD_NOCO,
            params={
                "limit": LIMIT_NOCO,
                "offset": offset
            }
        )

        data = r.json()
        rows = data.get("list", [])

        if not rows:
            break

        for row in rows:

            existing[row["bsale_id"]] = row

        offset += LIMIT_NOCO

    return existing


# -------------------------------------------------
# INSERT / UPDATE
# -------------------------------------------------

def batch_insert(rows):

    url = f"{NOCODB}/api/v2/tables/{TABLE_CLIENTS}/records"

    requests.post(url, headers=HEAD_NOCO, json=rows)


def batch_update(rows):

    url = f"{NOCODB}/api/v2/tables/{TABLE_CLIENTS}/records"

    requests.patch(url, headers=HEAD_NOCO, json=rows)


# -------------------------------------------------
# MAIN
# -------------------------------------------------

existing = noco_get_all()

insert_rows = []
update_rows = []

offset = 0

while True:

    r = requests.get(
        f"{BASE}/clients.json",
        headers=HEAD_BSALE,
        params={"limit": LIMIT_BSALE, "offset": offset}
    )

    data = r.json()

    items = data["items"]

    if not items:
        break

    for client in items:

        bsale_id = client["id"]

        created_raw = client.get("createdAt")
        updated_raw = client.get("updatedAt")

        created = datetime.fromtimestamp(int(created_raw)).strftime("%Y-%m-%d %H:%M:%S") if created_raw else None
        updated = datetime.fromtimestamp(int(updated_raw)).strftime("%Y-%m-%d %H:%M:%S") if updated_raw else None

        # attributes
        r_attr = requests.get(
            f"{BASE}/clients/{bsale_id}/attributes.json",
            headers=HEAD_BSALE
        )

        attr_data = r_attr.json()

        dia_atencion = None
        nombre_fantasia = None
        vendedor = None

        for attr in attr_data.get("items", []):

            name = attr.get("name", "").strip()
            value = attr.get("value")

            if name == "Dia Atencion":
                dia_atencion = value

            elif name == "NOMBRE DE FANTASÍA":
                nombre_fantasia = value

            elif name == "Vendedor":
                vendedor = value

        payload = {

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
            "dia_atencion": dia_atencion,
            "nombre_fantasia": nombre_fantasia,
            "vendedor": vendedor

        }

        if bsale_id in existing:

            row = existing[bsale_id]

            payload["Id"] = row["Id"]

            update_rows.append(payload)

        else:

            insert_rows.append(payload)

        if len(insert_rows) >= BATCH:

            batch_insert(insert_rows)
            insert_rows = []

        if len(update_rows) >= BATCH:

            batch_update(update_rows)
            update_rows = []

    offset += LIMIT_BSALE

if insert_rows:
    batch_insert(insert_rows)

if update_rows:
    batch_update(update_rows)

print("SYNC CLIENTS COMPLETE")
