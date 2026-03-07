import requests
import os
import time
import json

print("SYNC CATALOG")

BASE="https://api.bsale.io/v1"
NOCODB="https://db.quillotana.cl"

TOKEN=os.getenv("BSALE_TOKEN_Mini")
NOCOTOKEN=os.getenv("NocoDB_token")

LIMIT=50

HEAD={"access_token":TOKEN}

HEADNOCO={
"xc-token":NOCOTOKEN,
"Content-Type":"application/json"
}

TABLE_PRODUCTS="meke3fsng90uspe"
TABLE_VARIANTS="msd4vvijzk9pre9"
TABLE_TAXES="mary3rk9y5rwviu"
TABLE_OFFICES="m878eot7j6fi5v7"
TABLE_PRICELIST="m8zibme0z28jls6"

# -----------------------
# BSALE REQUEST
# -----------------------

def api(url,params=None):

    while True:

        r=requests.get(url,headers=HEAD,params=params)

        if r.status_code==429:

            retry=int(r.json().get("retry_after",60))
            print("RATE LIMIT WAIT",retry)

            time.sleep(retry)
            continue

        r.raise_for_status()
        time.sleep(0.15)

        return r.json()

# -----------------------

def fetch(endpoint):

    offset=0
    data=[]

    while True:

        j=api(
            f"{BASE}/{endpoint}",
            {"limit":LIMIT,"offset":offset}
        )

        items=j.get("items",[])

        if not items:
            break

        data.extend(items)
        offset+=LIMIT

    return data

# -----------------------
# UPSERT NOCO
# -----------------------

def upsert(table,field,value,payload):

    url=f"{NOCODB}/api/v2/tables/{table}/records"

    r=requests.get(
        url,
        headers=HEADNOCO,
        params={"where":f"({field},eq,{value})"}
    )

    data=r.json()

    if data["list"]:

        row_id=data["list"][0]["Id"]

        requests.patch(
            f"{url}/{row_id}",
            json=payload,
            headers=HEADNOCO
        )

    else:

        requests.post(
            url,
            json=payload,
            headers=HEADNOCO
        )

# -----------------------
# TAXES
# -----------------------

print("SYNC TAXES")

taxes=fetch("taxes.json")

for t in taxes:

    upsert(
        TABLE_TAXES,
        "bsale_id",
        t["id"],
        {
        "bsale_id":t["id"],
        "name":t["name"],
        "percentage":t["percentage"]
        }
    )

# -----------------------
# OFFICES
# -----------------------

print("SYNC OFFICES")

for o in fetch("offices.json"):

    upsert(
        TABLE_OFFICES,
        "bsale_id",
        o["id"],
        {
        "bsale_id":o["id"],
        "name":o["name"]
        }
    )

# -----------------------
# PRICE LIST
# -----------------------

print("SYNC PRICE LIST")

for p in fetch("price_lists.json"):

    upsert(
        TABLE_PRICELIST,
        "bsale_id",
        p["id"],
        {
        "bsale_id":p["id"],
        "name":p["name"]
        }
    )

# -----------------------
# PRODUCTS
# -----------------------

print("SYNC PRODUCTS")

products=fetch("products.json")

for p in products:

    upsert(
        TABLE_PRODUCTS,
        "bsale_id",
        p["id"],
        {
        "bsale_id":p["id"],
        "name":p["name"]
        }
    )

# -----------------------
# VARIANTS
# -----------------------

print("SYNC VARIANTS")

variants=fetch("variants.json")

for v in variants:

    upsert(
        TABLE_VARIANTS,
        "bsale_id",
        v["id"],
        {
        "bsale_id":v["id"],
        "product_id":v["product"]["id"],
        "description":v.get("description"),
        "code":v.get("code"),
        "bar_code":v.get("barCode")
        }
    )

print("CATALOG DONE")
