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
# SAFE REQUEST
# ---------------------------------------------------

def safe_get(url, headers, params=None):

    while True:

        try:

            r = requests.get(
                url,
                headers=headers,
                params=params,
                timeout=30
            )

        except requests.exceptions.Timeout:

            print("TIMEOUT RETRY")
            time.sleep(5)
            continue

        except requests.exceptions.ConnectionError:

            print("CONNECTION ERROR RETRY")
            time.sleep(5)
            continue

        if r.status_code == 429:

            wait = 60

            try:
                wait = int(r.json().get("retry_after",60))
            except:
                pass

            print("RATE LIMIT WAIT",wait)
            time.sleep(wait)
            continue

        if r.status_code != 200:

            print("BSALE ERROR",r.text)
            time.sleep(5)
            continue

        return r.json()


# ---------------------------------------------------
# GET COMPANIES
# ---------------------------------------------------

def get_companies():

    url=f"{NOCODB}/api/v2/tables/{TABLE_COMPANIES}/records"

    r=requests.get(url,headers=HEAD_NOCO,params={"limit":100})

    data=r.json()

    companies=[]

    for row in data.get("list",[]):

        if row.get("active"):

            token=os.getenv(row["bsale_token"])

            if not token:
                continue

            companies.append({

                "company_id":row["company_id"],
                "name":row["name"],
                "token":token.strip()

            })

    return companies


# ---------------------------------------------------
# NOCO HELPERS
# ---------------------------------------------------

def noco_insert(table,rows):

    if not rows:
        return

    url=f"{NOCODB}/api/v2/tables/{table}/records"

    r=requests.post(url,headers=HEAD_NOCO,json=rows)

    if r.status_code not in [200,201]:

        print("INSERT ERROR",r.text)


def noco_update(table,rows):

    if not rows:
        return

    url=f"{NOCODB}/api/v2/tables/{table}/records"

    r=requests.patch(url,headers=HEAD_NOCO,json=rows)

    if r.status_code not in [200,201]:

        print("UPDATE ERROR",r.text)


# ---------------------------------------------------
# LOAD TABLE
# ---------------------------------------------------

def noco_get_all(table):

    url=f"{NOCODB}/api/v2/tables/{table}/records"

    offset=0
    rows=[]

    while True:

        r=requests.get(

            url,
            headers=HEAD_NOCO,
            params={"limit":NOCO_LIMIT,"offset":offset}

        )

        data=r.json()

        batch=data.get("list",[])

        if not batch:
            break

        rows.extend(batch)

        offset+=NOCO_LIMIT

    return rows


# ---------------------------------------------------
# MAIN
# ---------------------------------------------------

companies=get_companies()

for company in companies:

    company_id=company["company_id"]

    token=company["token"]

    print("\nSYNC COMPANY:",company["name"])

    HEAD_BSALE={"access_token":token}

    products=noco_get_all(TABLE_PRODUCTS)

    tax_map={}

    for p in products:

        if p["company_id"]==company_id:

            tax_map[p["bsale_id"]]=p.get("tax_factor",1)


    variants=noco_get_all(TABLE_VARIANTS)

    print("VARIANTS LOADED:",len(variants))


    insert_costs=[]
    update_costs=[]

    processed=0


    for v in variants:

        if v["company_id"]!=company_id:
            continue

        variant_id=v["bsale_id"]
        product_id=v["product_id"]

        try:

            cost_data=safe_get(

                f"{BASE}/variants/{variant_id}/costs.json",
                HEAD_BSALE

            )

        except Exception as e:

            print("VARIANT COST ERROR",variant_id,e)
            continue

        avg_cost=cost_data.get("averageCost")

        tax_factor=tax_map.get(product_id,1)

        cost_gross=None

        if avg_cost:

            cost_gross=avg_cost*tax_factor


        insert_costs.append({

            "company_id":company_id,
            "variant_id":variant_id,
            "average_cost_net":avg_cost,
            "average_cost_gross":cost_gross,
            "tax_factor":tax_factor,
            "last_update":datetime.utcnow().isoformat()

        })


        if len(insert_costs)>=100:

            noco_insert(TABLE_COSTS,insert_costs)

            print("COST INSERT:",len(insert_costs))

            insert_costs=[]


        processed+=1

        if processed%200==0:

            print("PROCESSED VARIANTS:",processed)


        time.sleep(0.03)


    if insert_costs:

        noco_insert(TABLE_COSTS,insert_costs)


    print("COST SYNC DONE")


    # ---------------------------------------------------
    # PRICES
    # ---------------------------------------------------

    price_lists=safe_get(

        f"{BASE}/price_lists.json",
        HEAD_BSALE

    ).get("items",[])

    for pl in price_lists:

        price_list_id=pl["id"]

        print("SYNC PRICE LIST:",price_list_id)

        offset=0

        while True:

            data=safe_get(

                f"{BASE}/price_lists/{price_list_id}/details.json",
                HEAD_BSALE,
                {"limit":BSALE_LIMIT,"offset":offset}

            )

            items=data.get("items",[])

            if not items:
                break

            rows=[]

            for d in items:

                rows.append({

                    "company_id":company_id,
                    "variant_id":d["variant"]["id"],
                    "price_list_id":price_list_id,
                    "price_net":d["variantValue"],
                    "price_gross":d["variantValueWithTaxes"]

                })


            noco_insert(TABLE_PRICES,rows)

            print("PRICE ROWS:",len(rows),"OFFSET:",offset)

            offset+=BSALE_LIMIT

            time.sleep(0.1)


print("SYNC COMPLETED")
