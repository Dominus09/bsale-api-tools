import requests
import os
import time
import psycopg2
from psycopg2.extras import execute_batch

print("SYNC STOCK START")

BASE = "https://api.bsale.io/v1"

LIMIT = 50
BATCH = 500


# ----------------------------
# POSTGRES CONNECTION
# ----------------------------

conn = psycopg2.connect(
    host=os.getenv("PG_HOST"),
    database=os.getenv("PG_DB"),
    user=os.getenv("PG_USER"),
    password=os.getenv("PG_PASSWORD")
)

cur = conn.cursor()


# ----------------------------
# SAFE BSALE REQUEST
# ----------------------------

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


# ----------------------------
# GET COMPANIES
# ----------------------------

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


# ----------------------------
# UPSERT
# ----------------------------

def upsert(rows):

    execute_batch(cur, """

        INSERT INTO bsale.stocks
        (company_id, variant_id, office_id, quantity_available, quantity_reserved)

        VALUES (%s,%s,%s,%s,%s)

        ON CONFLICT (company_id, variant_id, office_id)
        DO UPDATE SET

        quantity_available = EXCLUDED.quantity_available,
        quantity_reserved = EXCLUDED.quantity_reserved

    """, rows)

    conn.commit()


# ----------------------------
# MAIN
# ----------------------------

companies = get_companies()

for company in companies:

    print("\nSYNC COMPANY:",company["name"])

    company_id = company["company_id"]

    HEAD_BSALE = {"access_token":company["token"]}

    offset = 0
    rows = []

    while True:

        data = bsale_get(

            f"{BASE}/stocks.json",
            HEAD_BSALE,
            {"limit":LIMIT,"offset":offset}

        )

        items = data["items"]

        if not items:
            break

        for s in items:

            rows.append((
                company_id,
                s["variant"]["id"],
                s["office"]["id"],
                s["quantityAvailable"],
                s["quantityReserved"]
            ))

        if len(rows) >= BATCH:

            upsert(rows)
            rows = []

        offset += LIMIT

    if rows:
        upsert(rows)

print("SYNC STOCK COMPLETE")
