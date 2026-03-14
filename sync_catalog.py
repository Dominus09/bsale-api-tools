import requests
import os
import json
import time
import psycopg2
from psycopg2.extras import execute_batch

print("SYNC CATALOG START")

BASE = "https://api.bsale.io/v1"
LIMIT = 50


# ----------------------------------
# POSTGRES CONNECTION
# ----------------------------------

conn = psycopg2.connect(
    host=os.getenv("PG_HOST"),
    database=os.getenv("PG_DB"),
    user=os.getenv("PG_USER"),
    password=os.getenv("PG_PASSWORD")
)

cur = conn.cursor()


# ----------------------------------
# BSALE REQUEST
# ----------------------------------

def bsale_get(url, headers, params=None):

    while True:

        r = requests.get(url, headers=headers, params=params)

        if r.status_code == 429:

            retry = int(r.json().get("retry_after",60))
            print("RATE LIMIT WAIT", retry)
            time.sleep(retry)
            continue

        r.raise_for_status()

        return r.json()


# ----------------------------------
# GET COMPANIES
# ----------------------------------

def get_companies():

    cur.execute("""
        SELECT company_id,name,bsale_token
        FROM bsale.companies
        WHERE active = true
    """)

    rows = cur.fetchall()

    companies = []

    for r in rows:

        token = os.getenv(r[2])

        if not token:
            print("TOKEN NOT FOUND:", r[2])
            continue

        companies.append({
            "company_id": r[0],
            "name": r[1],
            "token": token
        })

    return companies


# ----------------------------------
# UPSERT FUNCTION
# ----------------------------------

def upsert(query, data):

    execute_batch(cur, query, data)
    conn.commit()


# ----------------------------------
# MAIN
# ----------------------------------

companies = get_companies()

for company in companies:

    company_id = company["company_id"]
    token = company["token"]

    print("\n==============================")
    print("SYNC COMPANY:",company["name"])
    print("==============================")

    HEAD_BSALE = {
        "access_token": token
    }

    # ----------------------------------
    # TAXES
    # ----------------------------------

    print("LOAD TAXES")

    taxes = bsale_get(f"{BASE}/taxes.json",HEAD_BSALE)["items"]

    rows = []

    tax_map = {}

    for t in taxes:

        tax_id = int(t["id"])

        tax_map[tax_id] = {
            "name": t["name"],
            "percentage": float(t["percentage"])
        }

        rows.append((
            company_id,
            tax_id,
            t["name"],
            float(t["percentage"])
        ))

    upsert("""

        INSERT INTO bsale.taxes
        (company_id, bsale_id, name, percentage)

        VALUES (%s,%s,%s,%s)

        ON CONFLICT (company_id, bsale_id)
        DO UPDATE SET
        name = EXCLUDED.name,
        percentage = EXCLUDED.percentage

    """, rows)

    print("TAXES DONE")


    # ----------------------------------
    # PRODUCT TYPES
    # ----------------------------------

    print("LOAD PRODUCT TYPES")

    rows = []

    offset = 0

    while True:

        data = bsale_get(
            f"{BASE}/product_types.json",
            HEAD_BSALE,
            {"limit":LIMIT,"offset":offset}
        )

        items = data.get("items",[])

        if not items:
            break

        for pt in items:

            rows.append((
                company_id,
                int(pt["id"]),
                pt["name"],
                pt["state"]
            ))

        offset += LIMIT

    upsert("""

        INSERT INTO bsale.product_types
        (company_id, bsale_id, name, state)

        VALUES (%s,%s,%s,%s)

        ON CONFLICT (company_id, bsale_id)
        DO UPDATE SET
        name = EXCLUDED.name,
        state = EXCLUDED.state

    """, rows)

    print("PRODUCT TYPES DONE")


    # ----------------------------------
    # PRICE LISTS
    # ----------------------------------

    print("LOAD PRICE LISTS")

    lists = bsale_get(f"{BASE}/price_lists.json",HEAD_BSALE)["items"]

    rows = []

    for pl in lists:

        rows.append((
            company_id,
            int(pl["id"]),
            pl["name"],
            pl["state"]
        ))

    upsert("""

        INSERT INTO bsale.price_lists
        (company_id, bsale_id, name, state)

        VALUES (%s,%s,%s,%s)

        ON CONFLICT (company_id, bsale_id)
        DO UPDATE SET
        name = EXCLUDED.name,
        state = EXCLUDED.state

    """, rows)

    print("PRICE LISTS DONE")


    # ----------------------------------
    # OFFICES
    # ----------------------------------

    print("LOAD OFFICES")

    offices = bsale_get(f"{BASE}/offices.json",HEAD_BSALE)["items"]

    rows = []

    for o in offices:

        rows.append((
            company_id,
            int(o["id"]),
            o["name"],
            o["state"]
        ))

    upsert("""

        INSERT INTO bsale.offices
        (company_id, bsale_id, name, state)

        VALUES (%s,%s,%s,%s)

        ON CONFLICT (company_id, bsale_id)
        DO UPDATE SET
        name = EXCLUDED.name,
        state = EXCLUDED.state

    """, rows)

    print("OFFICES DONE")


    # ----------------------------------
    # PRODUCTS
    # ----------------------------------

    print("LOAD PRODUCTS")

    rows = []

    offset = 0

    while True:

        data = bsale_get(
            f"{BASE}/products.json",
            HEAD_BSALE,
            {"limit":LIMIT,"offset":offset}
        )

        items = data.get("items",[])

        if not items:
            break

        for p in items:

            product_id = int(p["id"])

            tax_ids = []
            tax_names = []
            tax_total = 0

            if "product_taxes" in p and "href" in p["product_taxes"]:

                tax_data = bsale_get(p["product_taxes"]["href"],HEAD_BSALE)

                for t in tax_data.get("items",[]):

                    tax_id = int(t["tax"]["id"])

                    tax_ids.append(tax_id)
                    tax_names.append(tax_map[tax_id]["name"])

                    tax_total += tax_map[tax_id]["percentage"]

            tax_factor = 1 + (tax_total/100)

            rows.append((
                company_id,
                product_id,
                p["name"],
                p["product_type"]["id"] if p.get("product_type") else None,
                json.dumps(tax_ids),
                json.dumps(tax_names),
                round(tax_factor,3)
            ))

        offset += LIMIT

    upsert("""

        INSERT INTO bsale.products
        (company_id, bsale_id, name, product_type_id, tax_ids_json, tax_names_json, tax_factor)

        VALUES (%s,%s,%s,%s,%s,%s,%s)

        ON CONFLICT (company_id, bsale_id)
        DO UPDATE SET
        name = EXCLUDED.name,
        product_type_id = EXCLUDED.product_type_id,
        tax_ids_json = EXCLUDED.tax_ids_json,
        tax_names_json = EXCLUDED.tax_names_json,
        tax_factor = EXCLUDED.tax_factor

    """, rows)

    print("PRODUCTS DONE")


    # ----------------------------------
    # VARIANTS
    # ----------------------------------

    print("LOAD VARIANTS")

    rows = []

    offset = 0

    while True:

        data = bsale_get(
            f"{BASE}/variants.json",
            HEAD_BSALE,
            {"limit":LIMIT,"offset":offset}
        )

        items = data.get("items",[])

        if not items:
            break

        for v in items:

            rows.append((
                company_id,
                int(v["id"]),
                int(v["product"]["id"]),
                v.get("code"),
                v.get("barCode"),
                v.get("description")
            ))

        offset += LIMIT

    upsert("""

        INSERT INTO bsale.variants
        (company_id, bsale_id, product_id, code, bar_code, description)

        VALUES (%s,%s,%s,%s,%s,%s)

        ON CONFLICT (company_id, bsale_id)
        DO UPDATE SET
        product_id = EXCLUDED.product_id,
        code = EXCLUDED.code,
        bar_code = EXCLUDED.bar_code,
        description = EXCLUDED.description

    """, rows)

    print("VARIANTS DONE")


print("\nSYNC CATALOG COMPLETE")
