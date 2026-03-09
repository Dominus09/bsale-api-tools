import requests
import os
import json
import time

print("SYNC CATALOG START")

BASE = "https://api.bsale.io/v1"
NOCODB = "https://db.quillotana.cl"

NOCODB_TOKEN = os.getenv("NocoDB_token")

HEAD_NOCO = {
    "xc-token": NOCODB_TOKEN,
    "Content-Type": "application/json"
}

LIMIT = 50

TABLE_PRODUCTS = "meke3fsng90uspe"
TABLE_PRODUCT_TYPES = "mcir9ile6id3813"
TABLE_TAXES = "mary3rk9y5rwviu"
TABLE_VARIANTS = "msd4vvijzk9pre9"
TABLE_COMPANIES = "companies"


# ----------------------------------------------------
# GET COMPANIES FROM NOCO
# ----------------------------------------------------

def get_companies():

    url = f"{NOCODB}/api/v2/tables/{TABLE_COMPANIES}/records"

    r = requests.get(url, headers=HEAD_NOCO, params={"limit": 100})

    data = r.json()

    companies = []

    for row in data.get("list", []):

        if row["active"]:

            token = os.getenv(row["bsale_token"])

            companies.append({
                "company_id": row["company_id"],
                "token": token,
                "name": row["name"]
            })

    return companies


# ----------------------------------------------------
# BSALE REQUEST
# ----------------------------------------------------

def bsale_get(url, headers, params=None):

    while True:

        r = requests.get(url, headers=headers, params=params)

        if r.status_code == 429:
            retry = int(r.json().get("retry_after", 60))
            print("RATE LIMIT WAIT", retry)
            time.sleep(retry)
            continue

        r.raise_for_status()
        return r.json()


# ----------------------------------------------------
# NOCO HELPERS
# ----------------------------------------------------

def noco_get_all(table):

    url = f"{NOCODB}/api/v2/tables/{table}/records"

    offset = 0
    mapping = {}

    while True:

        r = requests.get(
            url,
            headers=HEAD_NOCO,
            params={"limit": 200, "offset": offset}
        )

        data = r.json()

        rows = data.get("list", [])

        if not rows:
            break

        for row in rows:
            mapping[row["bsale_id"]] = row["Id"]

        offset += 200

    return mapping


def insert(table, payload):

    url = f"{NOCODB}/api/v2/tables/{table}/records"

    r = requests.post(url, headers=HEAD_NOCO, json=payload)

    if r.status_code not in [200, 201]:
        print("INSERT ERROR", r.text)


def update(table, row_id, payload):

    url = f"{NOCODB}/api/v2/tables/{table}/records"

    payload["Id"] = row_id

    r = requests.patch(url, headers=HEAD_NOCO, json=payload)

    if r.status_code not in [200, 201]:
        print("UPDATE ERROR", r.text)


# ----------------------------------------------------
# MAIN LOOP FOR COMPANIES
# ----------------------------------------------------

companies = get_companies()

for company in companies:

    company_id = company["company_id"]
    token = company["token"]

    print("SYNC COMPANY:", company["name"])

    HEAD_BSALE = {
        "access_token": token
    }

    # ----------------------------------------------------
    # TAXES
    # ----------------------------------------------------

    print("LOAD TAXES")

    taxes = bsale_get(f"{BASE}/taxes.json", HEAD_BSALE)["items"]

    existing_taxes = noco_get_all(TABLE_TAXES)

    tax_map = {}

    for t in taxes:

        tax_id = int(t["id"])

        tax_map[tax_id] = {
            "name": t["name"],
            "percentage": float(t["percentage"])
        }

        payload = {
            "company_id": company_id,
            "bsale_id": tax_id,
            "name": t["name"],
            "percentage": t["percentage"]
        }

        if tax_id in existing_taxes:
            update(TABLE_TAXES, existing_taxes[tax_id], payload)
        else:
            insert(TABLE_TAXES, payload)

    print("TAXES DONE")

    # ----------------------------------------------------
    # PRODUCT TYPES
    # ----------------------------------------------------

    print("LOAD PRODUCT TYPES")

    existing_types = noco_get_all(TABLE_PRODUCT_TYPES)

    offset = 0

    while True:

        data = bsale_get(
            f"{BASE}/product_types.json",
            HEAD_BSALE,
            {"limit": LIMIT, "offset": offset}
        )

        items = data.get("items", [])

        if not items:
            break

        for pt in items:

            pt_id = int(pt["id"])

            payload = {
                "company_id": company_id,
                "bsale_id": pt_id,
                "name": pt["name"],
                "state": pt["state"]
            }

            if pt_id in existing_types:
                update(TABLE_PRODUCT_TYPES, existing_types[pt_id], payload)
            else:
                insert(TABLE_PRODUCT_TYPES, payload)

        offset += LIMIT

    print("PRODUCT TYPES DONE")

    # ----------------------------------------------------
    # PRODUCTS
    # ----------------------------------------------------

    print("LOAD PRODUCTS")

    existing_products = noco_get_all(TABLE_PRODUCTS)

    offset = 0

    while True:

        data = bsale_get(
            f"{BASE}/products.json",
            HEAD_BSALE,
            {"limit": LIMIT, "offset": offset}
        )

        items = data.get("items", [])

        if not items:
            break

        for p in items:

            product_id = int(p["id"])

            tax_ids = []
            tax_names = []
            tax_total = 0

            if "product_taxes" in p and "href" in p["product_taxes"]:

                tax_data = bsale_get(p["product_taxes"]["href"], HEAD_BSALE)

                for t in tax_data.get("items", []):

                    tax_id = int(t["tax"]["id"])

                    tax_ids.append(tax_id)

                    tax_names.append(tax_map[tax_id]["name"])

                    tax_total += tax_map[tax_id]["percentage"]

            tax_factor = 1 + (tax_total / 100)

            payload = {
                "company_id": company_id,
                "bsale_id": product_id,
                "name": p["name"],
                "product_type_id": p["product_type"]["id"] if p.get("product_type") else None,
                "tax_ids_json": json.dumps(tax_ids),
                "tax_names_json": json.dumps(tax_names),
                "tax_factor": round(tax_factor, 3)
            }

            if product_id in existing_products:
                update(TABLE_PRODUCTS, existing_products[product_id], payload)
            else:
                insert(TABLE_PRODUCTS, payload)

        offset += LIMIT

    print("PRODUCTS DONE")

    # ----------------------------------------------------
    # VARIANTS
    # ----------------------------------------------------

    print("LOAD VARIANTS")

    existing_variants = noco_get_all(TABLE_VARIANTS)

    offset = 0

    while True:

        data = bsale_get(
            f"{BASE}/variants.json",
            HEAD_BSALE,
            {"limit": LIMIT, "offset": offset}
        )

        items = data.get("items", [])

        if not items:
            break

        for v in items:

            variant_id = int(v["id"])

            payload = {
                "company_id": company_id,
                "bsale_id": variant_id,
                "product_id": int(v["product"]["id"]),
                "code": v.get("code"),
                "bar_code": v.get("barCode"),
                "description": v.get("description")
            }

            if variant_id in existing_variants:
                update(TABLE_VARIANTS, existing_variants[variant_id], payload)
            else:
                insert(TABLE_VARIANTS, payload)

        offset += LIMIT

    print("VARIANTS DONE")


print("SYNC CATALOG COMPLETE")
