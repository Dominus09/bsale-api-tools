import requests
import os
import time
from datetime import datetime

print("SYNC COSTS + PRICES")

BASE = "https://api.bsale.io/v1"
NOCODB = "https://db.quillotana.cl"

NOCODB_TOKEN = os.getenv("NocoDB_token")

HEAD_NOCO = {
    "xc-token": NOCODB_TOKEN,
    "Content-Type": "application/json"
}

TABLE_VARIANTS = "msd4vvijzk9pre9"
TABLE_PRODUCTS = "meke3fsng90uspe"
TABLE_COSTS = "mdjjvdlwev2o76u"
TABLE_PRICES = "mcby3npgc3ig042"
TABLE_COMPANIES = "m27za58sg6ustui"

BSALE_LIMIT = 50
NOCO_LIMIT = 200
BATCH = 100


# -------------------------------------------------
# SAFE REQUEST
# -------------------------------------------------

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


# -------------------------------------------------
# NOCO HELPERS
# -------------------------------------------------

def noco_get_all(table, company_id):

    url = f"{NOCODB}/api/v2/tables/{table}/records"

    offset = 0
    rows = []

    while True:

        r = requests.get(

            url,
            headers=HEAD_NOCO,
            params={
                "limit":NOCO_LIMIT,
                "offset":offset,
                "where":f"(company_id,eq,{company_id})"
            }

        )

        data = r.json()

        batch = data.get("list",[])

        if not batch:
            break

        rows.extend(batch)

        offset += NOCO_LIMIT

    return rows


def batch_insert(table,rows):

    url = f"{NOCODB}/api/v2/tables/{table}/records"

    requests.post(url,headers=HEAD_NOCO,json=rows)


def batch_update(table,rows):

    url = f"{NOCODB}/api/v2/tables/{table}/records"

    requests.patch(url,headers=HEAD_NOCO,json=rows)


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

            "company_id":row["company_id"],
            "name":row["name"],
            "token":token

        })

    return companies


# -------------------------------------------------
# MAIN
# -------------------------------------------------

companies = get_companies()

for company in companies:

    company_id = company["company_id"]

    print("\nSYNC COMPANY:",company["name"])

    HEAD_BSALE = {"access_token":company["token"]}

    variants = noco_get_all(TABLE_VARIANTS,company_id)

    costs_existing = {

        r["variant_id"]:r

        for r in noco_get_all(TABLE_COSTS,company_id)

    }

    prices_existing = {

        f"{r['variant_id']}_{r['price_list_id']}":r

        for r in noco_get_all(TABLE_PRICES,company_id)

    }

    insert_costs = []
    update_costs = []

    for v in variants:

        variant_id = v["bsale_id"]

        cost_data = bsale_get(

            f"{BASE}/variants/{variant_id}/costs.json",
            HEAD_BSALE

        )

        avg_cost = cost_data.get("averageCost")

        payload = {

            "company_id":company_id,
            "variant_id":variant_id,
            "average_cost_net":avg_cost,
            "last_update":datetime.utcnow().isoformat()

        }

        if variant_id in costs_existing:

            payload["Id"] = costs_existing[variant_id]["Id"]

            update_costs.append(payload)

        else:

            insert_costs.append(payload)

    batch_insert(TABLE_COSTS,insert_costs)
    batch_update(TABLE_COSTS,update_costs)


    # ---------------------------
    # PRICES
    # ---------------------------

    price_lists = bsale_get(

        f"{BASE}/price_lists.json",
        HEAD_BSALE

    )["items"]

    insert_prices = []
    update_prices = []

    for pl in price_lists:

        price_list_id = pl["id"]

        offset = 0

        while True:

            data = bsale_get(

                f"{BASE}/price_lists/{price_list_id}/details.json",
                HEAD_BSALE,
                {"limit":BSALE_LIMIT,"offset":offset}

            )

            items = data["items"]

            if not items:
                break

            for d in items:

                variant_id = d["variant"]["id"]

                key = f"{variant_id}_{price_list_id}"

                payload = {

                    "company_id":company_id,
                    "variant_id":variant_id,
                    "price_list_id":price_list_id,
                    "price_net":d["variantValue"],
                    "price_gross":d["variantValueWithTaxes"]

                }

                if key in prices_existing:

                    payload["Id"] = prices_existing[key]["Id"]

                    update_prices.append(payload)

                else:

                    insert_prices.append(payload)

            offset += BSALE_LIMIT

    batch_insert(TABLE_PRICES,insert_prices)
    batch_update(TABLE_PRICES,update_prices)

print("SYNC COSTS + PRICES COMPLETE")
