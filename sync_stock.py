import requests
import os
import time

print("FAST STOCK SYNC")

# -----------------------------
# CONFIG
# -----------------------------

BASE = "https://api.bsale.io/v1"
NOCODB = "https://db.quillotana.cl"

BSALE_TOKEN = os.getenv("BSALE_TOKEN_Mini")
NOCODB_TOKEN = os.getenv("NocoDB_token")

TABLE = "mxs2lyz86cnxd23"

LIMIT = 50
BATCH = 100

HEAD_BSALE = {
    "access_token": BSALE_TOKEN
}

HEAD_NOCO = {
    "xc-token": NOCODB_TOKEN,
    "Content-Type": "application/json"
}

# -----------------------------
# API HELPER
# -----------------------------

def bsale_get(url, params=None):

    while True:

        r = requests.get(url, headers=HEAD_BSALE, params=params)

        if r.status_code == 429:
            retry = int(r.json().get("retry_after", 60))
            print("RATE LIMIT HIT, WAIT", retry)
            time.sleep(retry)
            continue

        r.raise_for_status()
        return r.json()


# -----------------------------
# CLEAR TABLE (bulk delete)
# -----------------------------

def clear_table():

    print("CLEAR STOCK TABLE")

    url = f"{NOCODB}/api/v2/tables/{TABLE}/records"

    total_deleted = 0

    while True:

        r = requests.get(
            url,
            headers=HEAD_NOCO,
            params={"limit": 200}
        )

        rows = r.json().get("list", [])

        if not rows:
            break

        ids = [row["Id"] for row in rows]

        delete_url = f"{url}/bulk"

        r = requests.delete(
            delete_url,
            headers=HEAD_NOCO,
            json={"ids": ids}
        )

        if r.status_code not in [200, 201]:
            print("DELETE ERROR", r.text)
            break

        total_deleted += len(ids)
        print("DELETED", total_deleted)

    print("TABLE CLEARED")


# -----------------------------
# INSERT BATCH
# -----------------------------

def insert_batch(rows):

    url = f"{NOCODB}/api/v2/tables/{TABLE}/records"

    r = requests.post(
        url,
        headers=HEAD_NOCO,
        json=rows
    )

    if r.status_code not in [200, 201]:
        print("INSERT ERROR", r.text)


# -----------------------------
# MAIN
# -----------------------------

clear_table()

offset = 0
buffer = []
total_inserted = 0

while True:

    data = bsale_get(
        f"{BASE}/stocks.json",
        {"limit": LIMIT, "offset": offset}
    )

    items = data.get("items", [])

    if not items:
        break

    for s in items:

        buffer.append({
            "variant_id": s["variant"]["id"],
            "office_id": s["office"]["id"],
            "quantity_available": s["quantityAvailable"],
            "quantity_reserved": s["quantityReserved"]
        })

        if len(buffer) >= BATCH:

            insert_batch(buffer)

            total_inserted += len(buffer)
            print("INSERTED", total_inserted)

            buffer = []

    offset += LIMIT


# insertar resto

if buffer:

    insert_batch(buffer)

    total_inserted += len(buffer)


print("TOTAL STOCK:", total_inserted)
print("STOCK SYNC DONE")
