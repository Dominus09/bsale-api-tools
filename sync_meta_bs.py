import requests
import os

print("SYNC META BSALE START")

BASE = "https://api.bsale.io/v1"
NOCODB = "https://db.quillotana.cl"

NOCODB_TOKEN = os.getenv("NocoDB_token")
BSALE_TOKEN = os.getenv("BSALE_TOKEN_SPA")

TABLE_DOC_TYPES = "msj3xk5f1yqpfzk"
TABLE_USERS = "mpqkni6mwrxie44"

HEAD_NOCO = {
    "xc-token": NOCODB_TOKEN,
    "Content-Type": "application/json"
}

HEAD_BSALE = {
    "access_token": BSALE_TOKEN
}

# -------------------------------------------------
# GENERIC NOCO GET
# -------------------------------------------------

def noco_get_all(table_id):

    url = f"{NOCODB}/api/v2/tables/{table_id}/records"

    r = requests.get(url, headers=HEAD_NOCO, params={"limit":200})

    data = r.json()

    existing = {}

    for row in data.get("list", []):
        existing[row["bsale_id"]] = row

    return existing


# -------------------------------------------------
# GENERIC INSERT
# -------------------------------------------------

def batch_insert(table_id, rows):

    if not rows:
        return

    url = f"{NOCODB}/api/v2/tables/{table_id}/records"

    requests.post(url, headers=HEAD_NOCO, json=rows)


# -------------------------------------------------
# GENERIC UPDATE
# -------------------------------------------------

def batch_update(table_id, rows):

    if not rows:
        return

    url = f"{NOCODB}/api/v2/tables/{table_id}/records"

    requests.patch(url, headers=HEAD_NOCO, json=rows)


# -------------------------------------------------
# SYNC DOCUMENT TYPES
# -------------------------------------------------

def sync_document_types():

    print("SYNC DOCUMENT TYPES")

    existing = noco_get_all(TABLE_DOC_TYPES)

    insert_rows = []
    update_rows = []

    r = requests.get(
        f"{BASE}/document_types.json",
        headers=HEAD_BSALE
    )

    data = r.json()

    for d in data.get("items", []):

        bsale_id = d["id"]

        payload = {

            "bsale_id": bsale_id,
            "name": d.get("name"),
            "code_sii": d.get("codeSii")

        }

        if bsale_id in existing:

            payload["Id"] = existing[bsale_id]["Id"]
            update_rows.append(payload)

        else:

            insert_rows.append(payload)

    batch_insert(TABLE_DOC_TYPES, insert_rows)
    batch_update(TABLE_DOC_TYPES, update_rows)

    print("DOCUMENT TYPES DONE")


# -------------------------------------------------
# SYNC USERS
# -------------------------------------------------

def sync_users():

    print("SYNC USERS")

    existing = noco_get_all(TABLE_USERS)

    insert_rows = []
    update_rows = []

    r = requests.get(
        f"{BASE}/users.json",
        headers=HEAD_BSALE
    )

    data = r.json()

    for u in data.get("items", []):

        bsale_id = u["id"]

        payload = {

            "bsale_id": bsale_id,
            "first_name": u.get("firstName"),
            "last_name": u.get("lastName"),
            "email": u.get("email"),
            "active": u.get("active")

        }

        if bsale_id in existing:

            payload["Id"] = existing[bsale_id]["Id"]
            update_rows.append(payload)

        else:

            insert_rows.append(payload)

    batch_insert(TABLE_USERS, insert_rows)
    batch_update(TABLE_USERS, update_rows)

    print("USERS DONE")


# -------------------------------------------------
# MAIN
# -------------------------------------------------

sync_document_types()
sync_users()

print("SYNC META BSALE COMPLETE")
