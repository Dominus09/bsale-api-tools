import requests
import os
import time

print("STOCK UPSERT FAST")

BASE = "https://api.bsale.io/v1"
NOCODB = "https://db.quillotana.cl"

NOCODB_TOKEN = os.getenv("NocoDB_token")

TABLE_STOCK = "mxs2lyz86cnxd23"
TABLE_COMPANIES = "m27za58sg6ustui"

LIMIT_BSALE = 50
LIMIT_NOCO = 200
BATCH_INSERT = 100

HEAD_NOCO = {
    "xc-token": NOCODB_TOKEN,
    "Content-Type": "application/json"
}


# -----------------------------
# GET COMPANIES
# -----------------------------

def get_companies():

    url = f"{NOCODB}/api/v2/tables/{TABLE_COMPANIES}/records"

    r = requests.get(url, headers=HEAD_NOCO, params={"limit":100})

    data = r.json()

    companies = []

    for row in data.get("list", []):

        if row.get("active"):

            token = os.getenv(row["bsale_token"])

            if not token:
                print("TOKEN NOT FOUND:", row["bsale_token"])
                continue

            companies.append({
                "company_id": row["company_id"],
                "name": row["name"],
                "token": token.strip()
            })

    return companies


# -----------------------------
# BSALE API
# -----------------------------

def bsale_get(url, headers, params=None):

    while True:

        r = requests.get(url, headers=headers, params=params)

        if r.status_code == 429:

            retry = int(r.json().get("retry_after",60))
            print("RATE LIMIT WAIT",retry)
            time.sleep(retry)
            continue

        r.raise_for_status()
        return r.json()


# -----------------------------
# NOCO HELPERS
# -----------------------------

def noco_get(url, params=None):

    r = requests.get(url, headers=HEAD_NOCO, params=params)

    r.raise_for_status()

    return r.json()


def noco_insert(rows):

    url = f"{NOCODB}/api/v2/tables/{TABLE_STOCK}/records"

    r = requests.post(url, headers=HEAD_NOCO, json=rows)

    if r.status_code not in [200,201]:
        print("INSERT ERROR", r.text)


def noco_update(row_id, payload):

    url = f"{NOCODB}/api/v2/tables/{TABLE_STOCK}/records"

    payload["Id"] = row_id

    r = requests.patch(url, headers=HEAD_NOCO, json=payload)

    if r.status_code not in [200,201]:
        print("UPDATE ERROR", r.text)


# -----------------------------
# LOAD EXISTING STOCK
# -----------------------------

def load_existing_stock(company_id):

    print("LOADING EXISTING STOCK FROM NOCO")

    url = f"{NOCODB}/api/v2/tables/{TABLE_STOCK}/records"

    offset = 0
    existing = {}
    total_rows = None

    while True:

        data = noco_get(
            url,
            params={
                "limit": LIMIT_NOCO,
                "offset": offset,
                "where": f"(company_id,eq,{company_id})"
            }
        )

        rows = data.get("list", [])

        if total_rows is None:
            total_rows = data.get("pageInfo", {}).get("totalRows", 0)

        for row in rows:

            variant_id = row.get("variant_id")
            office_id = row.get("office_id")

            row_id = row.get("Id")

            key = f"{variant_id}-{office_id}"

            existing[key] = {
                "Id": row_id,
                "quantity_available": row.get("quantity_available"),
                "quantity_reserved": row.get("quantity_reserved")
            }

        offset += LIMIT_NOCO

        if offset >= total_rows:
            break

    print("EXISTING STOCK ROWS:", len(existing))

    return existing


# -----------------------------
# MAIN
# -----------------------------

companies = get_companies()

for company in companies:

    company_id = company["company_id"]
    token = company["token"]

    print("\n======================")
    print("SYNC COMPANY:", company["name"])
    print("======================")

    HEAD_BSALE = {
        "access_token": token
    }

    existing_map = load_existing_stock(company_id)

    offset = 0
    insert_buffer = []

    inserted = 0
    updated = 0
    processed = 0

    while True:

        data = bsale_get(
            f"{BASE}/stocks.json",
            HEAD_BSALE,
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
                "company_id": company_id,
                "variant_id": variant_id,
                "office_id": office_id,
                "quantity_available": quantity_available,
                "quantity_reserved": quantity_reserved
            }

            if key in existing_map:

                current = existing_map[key]

                if (
                    current["quantity_available"] != quantity_available
                    or current["quantity_reserved"] != quantity_reserved
                ):

                    noco_update(current["Id"], payload)

                    updated += 1

            else:

                insert_buffer.append(payload)

                if len(insert_buffer) >= BATCH_INSERT:

                    noco_insert(insert_buffer)

                    inserted += len(insert_buffer)

                    print("INSERTED", inserted, "| UPDATED", updated)

                    insert_buffer = []

            processed += 1

        offset += LIMIT_BSALE

        print("PROCESSED", processed, "| INSERTED", inserted, "| UPDATED", updated)

    if insert_buffer:

        noco_insert(insert_buffer)

        inserted += len(insert_buffer)

    print("FINAL PROCESSED:", processed)
    print("FINAL INSERTED:", inserted)
    print("FINAL UPDATED:", updated)

print("STOCK UPSERT DONE")
