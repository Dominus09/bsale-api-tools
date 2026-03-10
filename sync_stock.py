import requests
import os
import time

print("SYNC STOCK START")

BASE = "https://api.bsale.io/v1"
NOCODB = "https://db.quillotana.cl"

NOCODB_TOKEN = os.getenv("NocoDB_token")

TABLE_STOCK = "mxs2lyz86cnxd23"
TABLE_COMPANIES = "m27za58sg6ustui"

LIMIT_BSALE = 50
LIMIT_NOCO = 200
BATCH = 100

HEAD_NOCO = {
    "xc-token": NOCODB_TOKEN,
    "Content-Type": "application/json"
}


# -------------------------------------------------
# SAFE GET BSALE
# -------------------------------------------------

def bsale_get(url, headers, params=None):

    while True:

        r = requests.get(url, headers=headers, params=params, timeout=30)

        if r.status_code == 429:

            wait = int(r.json().get("retry_after",60))

            print("RATE LIMIT WAIT",wait)

            time.sleep(wait)

            continue

        r.raise_for_status()

        return r.json()


# -------------------------------------------------
# GET COMPANIES
# -------------------------------------------------

def get_companies():

    url = f"{NOCODB}/api/v2/tables/{TABLE_COMPANIES}/records"

    r = requests.get(url,headers=HEAD_NOCO,params={"limit":100})

    data = r.json()

    companies = []

    for row in data["list"]:

        if not row["active"]:
            continue

        token = os.getenv(row["bsale_token"])

        companies.append({
            "company_id": row["company_id"],
            "name": row["name"],
            "token": token
        })

    return companies


# -------------------------------------------------
# NOCO HELPERS
# -------------------------------------------------

def noco_get_all(company_id):

    url = f"{NOCODB}/api/v2/tables/{TABLE_STOCK}/records"

    offset = 0
    existing = {}

    while True:

        r = requests.get(

            url,
            headers=HEAD_NOCO,
            params={
                "limit": LIMIT_NOCO,
                "offset": offset,
                "where": f"(company_id,eq,{company_id})"
            }

        )

        data = r.json()

        rows = data.get("list",[])

        if not rows:
            break

        for row in rows:

            key = f"{row['variant_id']}_{row['office_id']}"

            existing[key] = row

        offset += LIMIT_NOCO

    return existing


def batch_insert(rows):

    url = f"{NOCODB}/api/v2/tables/{TABLE_STOCK}/records"

    requests.post(url,headers=HEAD_NOCO,json=rows)


def batch_update(rows):

    url = f"{NOCODB}/api/v2/tables/{TABLE_STOCK}/records"

    requests.patch(url,headers=HEAD_NOCO,json=rows)


# -------------------------------------------------
# MAIN
# -------------------------------------------------

companies = get_companies()

for company in companies:

    print("\nSYNC COMPANY:",company["name"])

    company_id = company["company_id"]

    HEAD_BSALE = {"access_token":company["token"]}

    existing = noco_get_all(company_id)

    insert_rows = []
    update_rows = []

    offset = 0

    while True:

        data = bsale_get(

            f"{BASE}/stocks.json",
            HEAD_BSALE,
            {"limit":LIMIT_BSALE,"offset":offset}

        )

        items = data["items"]

        if not items:
            break

        for s in items:

            variant_id = s["variant"]["id"]
            office_id = s["office"]["id"]

            quantity_available = s["quantityAvailable"]
            quantity_reserved = s["quantityReserved"]

            key = f"{variant_id}_{office_id}"

            payload = {

                "company_id":company_id,
                "variant_id":variant_id,
                "office_id":office_id,
                "quantity_available":quantity_available,
                "quantity_reserved":quantity_reserved

            }

            if key in existing:

                row = existing[key]

                if (

                    row["quantity_available"] != quantity_available
                    or row["quantity_reserved"] != quantity_reserved

                ):

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

print("SYNC STOCK COMPLETE")
