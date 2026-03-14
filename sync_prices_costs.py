import requests
import os
import time
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_batch

print("SYNC COSTS + PRICES START")

BASE = "https://api.bsale.io/v1"

LIMIT = 50
BATCH = 500


# ---------------------------------
# POSTGRES CONNECTION
# ---------------------------------

conn = psycopg2.connect(
    host=os.getenv("PG_HOST"),
    database=os.getenv("PG_DB"),
    user=os.getenv("PG_USER"),
    password=os.getenv("PG_PASSWORD")
)

cur = conn.cursor()


# ---------------------------------
# SAFE BSALE REQUEST
# ---------------------------------

def bsale_get(url, headers, params=None):

    while True:

        r = requests.get(url,headers=headers,params=params,timeout=30)

        if r.status_code == 429:

            wait = int(r.json().get("retry_after",60))
            print("RATE LIMIT WAIT",wait)
            time.sleep(wait)
            continue

        r.raise_for_status()

        return r.json()


# ---------------------------------
# GET COMPANIES
# ---------------------------------

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


# ---------------------------------
# UPSERT COSTS
# ---------------------------------

def upsert_costs(rows):

    execute_batch(cur, """

        INSERT INTO bsale.variant_cost
        (company_id, variant_id, average_cost_net, last_update)

        VALUES (%s,%s,%s,%s)

        ON CONFLICT (company_id, variant_id)
        DO UPDATE SET

        average_cost_net = EXCLUDED.average_cost_net,
        last_update = EXCLUDED.last_update

    """, rows)

    conn.commit()


# ---------------------------------
# UPSERT PRICES
# ---------------------------------

def upsert_prices(rows):

    execute_batch(cur, """

        INSERT INTO bsale.variant_prices
        (company_id, variant_id, price_list_id, price_net, price_gross)

        VALUES (%s,%s,%s,%s,%s)

        ON CONFLICT (company_id, variant_id, price_list_id)
        DO UPDATE SET

        price_net = EXCLUDED.price_net,
        price_gross = EXCLUDED.price_gross

    """, rows)

    conn.commit()


# ---------------------------------
# MAIN
# ---------------------------------

companies = get_companies()

for company in companies:

    company_id = company["company_id"]

    print("\nSYNC COMPANY:",company["name"])

    HEAD_BSALE = {"access_token":company["token"]}


    # -----------------------------
    # COSTS
    # -----------------------------

    cur.execute("""

        SELECT bsale_id
        FROM bsale.variants
        WHERE company_id = %s

    """,(company_id,))

    variants = cur.fetchall()

    rows = []

    for v in variants:

        variant_id = v[0]

        cost_data = bsale_get(

            f"{BASE}/variants/{variant_id}/costs.json",
            HEAD_BSALE

        )

        avg_cost = cost_data.get("averageCost")

        rows.append((
            company_id,
            variant_id,
            avg_cost,
            datetime.utcnow()
        ))

        if len(rows) >= BATCH:

            upsert_costs(rows)
            rows = []

    if rows:
        upsert_costs(rows)


    # -----------------------------
    # PRICES
    # -----------------------------

    price_lists = bsale_get(

        f"{BASE}/price_lists.json",
        HEAD_BSALE

    )["items"]

    rows = []

    for pl in price_lists:

        price_list_id = pl["id"]

        offset = 0

        while True:

            data = bsale_get(

                f"{BASE}/price_lists/{price_list_id}/details.json",
                HEAD_BSALE,
                {"limit":LIMIT,"offset":offset}

            )

            items = data["items"]

            if not items:
                break

            for d in items:

                rows.append((
                    company_id,
                    d["variant"]["id"],
                    price_list_id,
                    d["variantValue"],
                    d["variantValueWithTaxes"]
                ))

                if len(rows) >= BATCH:

                    upsert_prices(rows)
                    rows = []

            offset += LIMIT

    if rows:
        upsert_prices(rows)

print("SYNC COSTS + PRICES COMPLETE")
