import requests
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

print("SYNC DOCUMENT DETAILS FAST V2 START")

BASE = "https://api.bsale.io/v1"
NOCODB = "https://db.quillotana.cl"

NOCODB_TOKEN = os.getenv("NocoDB_token")
BSALE_TOKEN = os.getenv("BSALE_TOKEN_SPA")

TABLE_DOCUMENTS = "mc73age2tnhq3dd"
TABLE_DETAILS = "mox0eode1i5flz5"

LIMIT_NOCO = 500
BATCH = 800
WORKERS = 6

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

            r = requests.get(url, headers=HEAD_BSALE, timeout=30)

            if r.status_code == 429:
                wait = int(r.json().get("retry_after",60))
                print("RATE LIMIT WAIT",wait)
                time.sleep(wait)
                continue

            if r.status_code in [500,502,503,504]:
                time.sleep(2)
                continue

            r.raise_for_status()

            return r.json()

        except:
            time.sleep(2)

# -------------------------------------------------
# GET DOCUMENTS FAST
# -------------------------------------------------

def get_documents():

    print("LOADING DOCUMENTS FAST")

    url = f"{NOCODB}/api/v2/tables/{TABLE_DOCUMENTS}/records"

    offset = 0
    docs = []

    while True:

        r = requests.get(
            url,
            headers=HEAD_NOCO,
            params={"limit": LIMIT_NOCO, "offset": offset}
        )

        data = r.json()
        rows = data.get("list", [])

        if not rows:
            break

        docs.extend([row["bsale_id"] for row in rows])

        offset += LIMIT_NOCO

    print("DOCUMENTS FOUND:", len(docs))

    return docs

# -------------------------------------------------
# FETCH DETAILS
# -------------------------------------------------

def fetch_details(doc_id):

    data = bsale_get(f"{BASE}/documents/{doc_id}/details.json")

    return doc_id, data.get("items", [])

# -------------------------------------------------
# INSERT BATCH
# -------------------------------------------------

def batch_insert(rows):

    url = f"{NOCODB}/api/v2/tables/{TABLE_DETAILS}/records"

    try:
        requests.post(url, headers=HEAD_NOCO, json=rows)
    except:
        pass

# -------------------------------------------------
# MAIN
# -------------------------------------------------

documents = get_documents()

insert_rows = []
count = 0

with ThreadPoolExecutor(max_workers=WORKERS) as executor:

    futures = [executor.submit(fetch_details, doc) for doc in documents]

    for future in as_completed(futures):

        doc_id, items = future.result()

        for d in items:

            payload = {

                "bsale_detail_id": d["id"],
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

            print("INSERTED:", count)

            insert_rows = []

if insert_rows:
    batch_insert(insert_rows)

print("SYNC DOCUMENT DETAILS COMPLETE")
