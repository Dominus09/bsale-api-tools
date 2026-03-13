import requests
import os
import time
from datetime import datetime

print("SYNC DOCUMENTS START")

BASE = "https://api.bsale.io/v1"
NOCODB = "https://db.quillotana.cl"

NOCODB_TOKEN = os.getenv("NocoDB_token")
BSALE_TOKEN = os.getenv("BSALE_TOKEN_SPA")

TABLE_DOCUMENTS = "mc73age2tnhq3dd"

LIMIT_BSALE = 50
LIMIT_NOCO = 200
BATCH = 100

START_DATE = 1767225600   # 01-01-2026
END_DATE = int(time.time())

HEAD_NOCO = {
    "xc-token": NOCODB_TOKEN,
    "Content-Type": "application/json"
}

HEAD_BSALE = {
    "access_token": BSALE_TOKEN
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
# GET EXISTING DOCUMENTS
# -------------------------------------------------

def noco_get_all():

    url = f"{NOCODB}/api/v2/tables/{TABLE_DOCUMENTS}/records"

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

        rows = data.get("list",[])

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

    url = f"{NOCODB}/api/v2/tables/{TABLE_DOCUMENTS}/records"

    requests.post(url,headers=HEAD_NOCO,json=rows)


def batch_update(rows):

    url = f"{NOCODB}/api/v2/tables/{TABLE_DOCUMENTS}/records"

    requests.patch(url,headers=HEAD_NOCO,json=rows)


# -------------------------------------------------
# MAIN
# -------------------------------------------------

existing = noco_get_all()

insert_rows = []
update_rows = []

offset = 0

while True:

    data = bsale_get(

        f"{BASE}/documents.json",
        HEAD_BSALE,
        {
            "limit": LIMIT_BSALE,
            "offset": offset,
            "emissiondaterange": f"[{START_DATE},{END_DATE}]"
        }

    )

    items = data["items"]

    if not items:
        break

    print("DOCUMENTS:",len(items))

    for d in items:

        bsale_id = d["id"]

        emission_raw = d.get("emissionDate")

        emission_date = None

        if emission_raw:

            emission_date = datetime.fromtimestamp(
                int(emission_raw)
            ).strftime("%Y-%m-%d %H:%M:%S")

        payload = {

            "bsale_id": bsale_id,
            "number": d.get("number"),
            "emission_date": emission_date,
            "document_type_id": d.get("document_type",{}).get("id"),
            "client_id": d.get("client",{}).get("id"),
            "office_id": d.get("office",{}).get("id"),
            "user_id": d.get("user",{}).get("id"),
            "total_amount": d.get("totalAmount"),
            "state": d.get("state"),
            "url_public": d.get("urlPublicView"),
            "url_pdf": d.get("urlPdf"),
            "token": d.get("token")

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

print("SYNC DOCUMENTS COMPLETE")
