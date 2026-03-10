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


# ---------------------------------------------------
# GET COMPANIES
# ---------------------------------------------------

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


# ---------------------------------------------------
# SAFE BSALE REQUEST
# ---------------------------------------------------

def bsale_get(url, headers, params=None):

    while True:

        r = requests.get(url, headers=headers, params=params)

        if r.status_code == 429:

            wait = 60

            try:
                wait = int(r.json().get("retry_after",60))
            except:
                pass

            print("RATE LIMIT WAIT",wait)
            time.sleep(wait)
            continue

        r.raise_for_status()

        return r.json()


# ---------------------------------------------------
# NOCO GET ALL
# ---------------------------------------------------

def noco_get_all(table):

    url = f"{NOCODB}/api/v2/tables/{table}/records"

    offset = 0
    rows = []

    while True:

        r = requests.get(
            url,
            headers=HEAD_NOCO,
            params={"limit":NOCO_LIMIT,"offset":offset}
        )

        data = r.json()

        batch = data.get("list",[])

        if not batch:
            break

        rows.extend(batch)

        offset += NOCO_LIMIT

    return rows


# ---------------------------------------------------
# INSERT / UPDATE
# ---------------------------------------------------

def batch_insert(table, rows):

    if not rows:
        return

    url = f"{NOCODB}/api/v2/tables/{table}/records"

    for i in range(0,len(rows),NOCO_LIMIT):

        chunk = rows[i:i+NOCO_LIMIT]

        r = requests.post(url, headers=HEAD_NOCO, json=chunk)

        if r.status_code not in [200,201]:
            print("INSERT ERROR", r.text)


def batch_update(table, rows):

    if not rows:
        return

    url = f"{NOCODB}/api/v2/tables/{table}/records"

    for i in range(0,len(rows),NOCO_LIMIT):

        chunk = rows[i:i+NOCO_LIMIT]

        r = requests.patch(url, headers=HEAD_NOCO, json=chunk)

        if r.status_code not in [200,201]:
            print("UPDATE ERROR", r.text)


# ---------------------------------------------------
# MAIN
# ---------------------------------------------------

companies = get_companies()

for company in companies:

    company_id = company["company_id"]
    token = company["token"]

    print("\nSYNC COMPANY:",company["name"])

    HEAD_BSALE = {"access_token":token}

    products = noco_get_all(TABLE_PRODUCTS)

    tax_map = {}

    for p in products:

        if p["company_id"] == company_id:
            tax_map[p["bsale_id"]] = p.get("tax_factor",1)

    variants = noco_get_all(TABLE_VARIANTS)

    cost_rows = noco_get_all(TABLE_COSTS)

    cost_map = {}

    for r in cost_rows:

        if r["company_id"] == company_id:
            cost_map[r["variant_id"]] = r["Id"]

    price_rows = noco_get_all(TABLE_PRICES)

    price_map = {}

    for r in price_rows:

        if r["company_id"] == company_id:

            key = f"{r['variant_id']}_{r['price_list_id']}"

            price_map[key] = r["Id"]


    # ---------------------------------------------------
    # COSTS
    # ---------------------------------------------------

    insert_costs = []
    update_costs = []

    for v in variants:

        if v["company_id"] != company_id:
            continue

        variant_id = v["bsale_id"]
        product_id = v["product_id"]

        avg_cost = v.get("averageCost")

        if avg_cost is None:

            cost_data = bsale_get(
                f"{BASE}/variants/{variant_id}/costs.json",
                HEAD_BSALE
            )

            avg_cost = cost_data.get("averageCost")

            time.sleep(0.05)

        tax_factor = tax_map.get(product_id,1)

        cost_gross = None

        if avg_cost:
            cost_gross = avg_cost * tax_factor

        payload = {

            "company_id":company_id,
            "variant_id":variant_id,
            "average_cost_net":avg_cost,
            "average_cost_gross":cost_gross,
            "tax_factor":tax_factor,
            "last_update":datetime.utcnow().isoformat()

        }

        if variant_id in cost_map:

            payload["Id"] = cost_map[variant_id]

            update_costs.append(payload)

        else:

            insert_costs.append(payload)

    batch_insert(TABLE_COSTS,insert_costs)
    batch_update(TABLE_COSTS,update_costs)

    print("COSTS DONE")


    # ---------------------------------------------------
    # PRICES
    # ---------------------------------------------------

    price_lists = bsale_get(f"{BASE}/price_lists.json",HEAD_BSALE).get("items",[])

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

            items = data.get("items",[])

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

                if key in price_map:

                    payload["Id"] = price_map[key]

                    update_prices.append(payload)

                else:

                    insert_prices.append(payload)

            offset += BSALE_LIMIT

    batch_insert(TABLE_PRICES,insert_prices)
    batch_update(TABLE_PRICES,update_prices)

    print("PRICES DONE")

print("SYNC COMPLETED")
