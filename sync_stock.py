import requests
import os
import time

print("STOCK UPSERT FAST")

# -----------------------------
# CONFIG
# -----------------------------

BASE = "https://api.bsale.io/v1"
NOCODB = "https://db.quillotana.cl"

BSALE_TOKEN = os.getenv("BSALE_TOKEN_Mini")
NOCODB_TOKEN = os.getenv("NocoDB_token")

TABLE = "mxs2lyz86cnxd23"

LIMIT_BSALE = 50
LIMIT_NOCO = 200
BATCH_INSERT = 100

HEAD_BSALE = {
    "access_token": BSALE_TOKEN
}

HEAD_NOCO = {
    "xc-token": NOCODB_TOKEN,
    "Content-Type": "application/json"
}

# -----------------------------
# HELPERS
# -----------------------------

def bsale_get(url, params=None):
    while True:
        r = requests.get(url, headers=HEAD_BSALE, params=params, timeout=90)

        if r.status_code == 429:
            retry = int(r.json().get("retry_after", 60))
            print("RATE LIMIT HIT, WAIT", retry)
            time.sleep(retry)
            continue

        r.raise_for_status()
        return r.json()


def noco_get(url, params=None):
    r = requests.get(url, headers=HEAD_NOCO, params=params, timeout=90)
    r.raise_for_status()
    return r.json()


def noco_post(url, payload):
    r = requests.post(url, headers=HEAD_NOCO, json=payload, timeout=90)
    if r.status_code not in [200, 201]:
        print("POST ERROR", r.status_code, r.text)
    return r


def noco_patch(url, payload):
    r = requests.patch(url, headers=HEAD_NOCO, json=payload, timeout=90)
    if r.status_code not in [200, 201]:
        print("PATCH ERROR", r.status_code, r.text)
    return r


# -----------------------------
# CARGAR STOCK EXISTENTE DE NOCO UNA SOLA VEZ
# -----------------------------

def load_existing_stock():
    print("LOADING EXISTING STOCK FROM NOCO")

    url = f"{NOCODB}/api/v2/tables/{TABLE}/records"
    offset = 0
    existing = {}

    while True:
        data = noco_get(
            url,
            params={"limit": LIMIT_NOCO, "offset": offset}
        )

        rows = data.get("list", [])

        if not rows:
            break

        for row in rows:
            variant_id = row.get("variant_id")
            office_id = row.get("office_id")
            row_id = row.get("Id")

            if variant_id is not None and office_id is not None and row_id is not None:
                key = f"{variant_id}-{office_id}"
                existing[key] = {
                    "Id": row_id,
                    "quantity_available": row.get("quantity_available"),
                    "quantity_reserved": row.get("quantity_reserved")
                }

        offset += LIMIT_NOCO

    print("EXISTING STOCK ROWS:", len(existing))
    return existing


# -----------------------------
# INSERT BATCH
# -----------------------------

def insert_batch(rows):
    if not rows:
        return

    url = f"{NOCODB}/api/v2/tables/{TABLE}/records"
    noco_post(url, rows)


# -----------------------------
# MAIN
# -----------------------------

existing_map = load_existing_stock()

offset = 0
insert_buffer = []
inserted = 0
updated = 0
processed = 0

while True:
    data = bsale_get(
        f"{BASE}/stocks.json",
        {"limit": LIMIT_BSALE, "offset": offset}
    )

    items = data.get("items", [])

    if not items:
        break

    for s in items:
        variant_id = s["variant"]["id"]
        office_id = s["office"]["id"]
        quantity_available = s["quantityAvailable"]
        quantity_reserved = s["quantityReserved"]

        key = f"{variant_id}-{office_id}"

        payload = {
            "variant_id": variant_id,
            "office_id": office_id,
            "quantity_available": quantity_available,
            "quantity_reserved": quantity_reserved
        }

        if key in existing_map:
            current = existing_map[key]

            # solo actualiza si cambió algo
            if (
                current.get("quantity_available") != quantity_available
                or current.get("quantity_reserved") != quantity_reserved
            ):
                row_id = current["Id"]
                noco_patch(
                    f"{NOCODB}/api/v2/tables/{TABLE}/records/{row_id}",
                    payload
                )
                updated += 1
        else:
            insert_buffer.append(payload)

            if len(insert_buffer) >= BATCH_INSERT:
                insert_batch(insert_buffer)
                inserted += len(insert_buffer)
                print("INSERTED", inserted, "| UPDATED", updated)
                insert_buffer = []

        processed += 1

    offset += LIMIT_BSALE
    print("PROCESSED", processed, "| INSERTED", inserted, "| UPDATED", updated)

# insertar remanente
if insert_buffer:
    insert_batch(insert_buffer)
    inserted += len(insert_buffer)

print("FINAL PROCESSED:", processed)
print("FINAL INSERTED:", inserted)
print("FINAL UPDATED:", updated)
print("STOCK UPSERT DONE")
