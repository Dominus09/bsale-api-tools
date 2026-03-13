import requests
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

print("SYNC DOCUMENT DETAILS FAST START")

BASE = "https://api.bsale.io/v1"
NOCODB = "https://db.quillotana.cl"

NOCODB_TOKEN = os.getenv("NocoDB_token")
BSALE_TOKEN = os.getenv("BSALE_TOKEN_SPA")

TABLE_DOCUMENTS = "mc73age2tnhq3dd"
TABLE_DETAILS = "mox0eode1i5flz5"

LIMIT_NOCO = 200
BATCH = 200
WORKERS = 8

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

def bsale_get(url):

    while True:

        try:

            r = requests.get(
                url,
                headers=HEAD_BSALE,
                timeout=30
            )

            if r.status_code == 429:

                wait = int(r.json().get("retry_after",60))
                print("RATE LIMIT WAIT",wait)
                time.sleep(wait)
                continue

            if r.status_code in [500,502,503,504]:

                print("BSALE ERROR",r.status_code)
                time.sleep(2)
                continue

            r.raise_for_status()

            return r.json()

        except requests.exceptions.RequestException:

            time.sleep(2)


# -------------------------------------------------
# GET DOCUMENTS FROM NOCO
# -------------------------------------------------

def get_documents():

    print("LOADING DOCUMENTS")

    url = f"{NOCODB}/api/v2/tables/{TABLE_DOCUMENTS}/records"

    offset = 0
    docs = []

    while True:

        r = requests.get(
            url,
            headers=HEAD_NOCO,
            params={"limit":LIMIT_NOCO,"offset":offset}
        )

        data = r.json()
        rows = data.get("list",[])

        if not rows:
            break

        for row in rows:
            docs.append(row["bsale_id"])

        offset += LIMIT_NOCO

    print("DOCUMENTS FOUND",len(docs))

    return docs


# -------------------------------------------------
# GET EXISTING DETAILS
# -------------------------------------------------

def get_existing_details():

    print("LOADING EXISTING DETAILS")

    url = f"{NOCODB}/api/v2/tables/{TABLE_DETAILS}/records"

    offset = 0
    existing = set()

    while True:

        r = requests.get(
            url,
            headers=HEAD_NOCO,
            params={"limit":LIMIT_NOCO,"offset":offset}
        )

        data = r.json()

        rows = data.get("list",[])

        if not rows:
            break

        for row in rows:
            existing.add(row["bsale_detail_id"])

        offset += LIMIT_NOCO

    print("DETAILS FOUND",len(existing))

    return existing


# -------------------------------------------------
# FETCH DETAILS FOR ONE DOCUMENT
# -------------------------------------------------

def fetch_details(doc_id):

    data = bsale_get(f"{BASE}/documents/{doc_id}/details.json")

    return doc_id, data.get("items",[])


# -------------------------------------------------
# BATCH INSERT
# -------------------------------------------------

def batch_insert(rows):

    if not rows:
        return

    url = f"{NOCODB}/api/v2/tables/{TABLE_DETAILS}/records"

    requests.post(url,headers=HEAD_NOCO,json=rows)


# -------------------------------------------------
# MAIN
# -------------------------------------------------

documents = get_documents()

existing_details = get_existing_details()

insert_rows = []

count = 0

with ThreadPoolExecutor(max_workers=WORKERS) as executor:

    futures = [executor.submit(fetch_details, doc) for doc in documents]

    for future in as_completed(futures):

        doc_id, items = future.result()

        for d in items:

            detail_id = d["id"]

            if detail_id in existing_details:
                continue

            payload = {

                "bsale_detail_id": detail_id,
                "document_id": doc_id,
                "line_number": d.get("lineNumber"),
                "variant_id": d.get("variant",{}).get("id"),
                "variant_code": d.get("variant",{}).get("code"),
                "variant_description": d.get("variant",{}).get("description"),
                "quantity": d.get("quantity"),
                "net_unit_value": d.get("netUnitValue"),
                "total_unit_value": d.get("totalUnitValue"),
                "net_amount": d.get("netAmount"),
                "tax_amount": d.get("taxAmount"),
                "total_amount": d.get("totalAmount"),
                "net_discount": d.get("netDiscount"),
                "discount_percentage": d.get("discountPercentage")

            }

            insert_rows.append(payload)

            count += 1

            if len(insert_rows) >= BATCH:

                batch_insert(insert_rows)

                print("INSERTED",count)

                insert_rows = []

if insert_rows:
    batch_insert(insert_rows)

print("SYNC DOCUMENT DETAILS COMPLETE")
