import requests
import os
from datetime import datetime

print("SYNC FAST COST + PRICES")

BASE = "https://api.bsale.io/v1"
NOCODB = "https://db.quillotana.cl"

BSALE_TOKEN = os.getenv("BSALE_TOKEN_Mini")
NOCODB_TOKEN = os.getenv("NocoDB_token")

HEAD_BSALE = {"access_token": BSALE_TOKEN}
HEAD_NOCO = {"xc-token": NOCODB_TOKEN,"Content-Type":"application/json"}

LIMIT = 50

TABLE_VARIANTS="msd4vvijzk9pre9"
TABLE_PRODUCTS="meke3fsng90uspe"
TABLE_COSTS="mdjjvdlwev2o76u"
TABLE_PRICES="mcby3npgc3ig042"

# --------------------------------------------------

def bsale_get(url,params=None):

    r=requests.get(url,headers=HEAD_BSALE,params=params)

    if r.status_code==429:
        print("RATE LIMIT")
        return None

    r.raise_for_status()
    return r.json()

# --------------------------------------------------

def noco_get_all(table):

    url=f"{NOCODB}/api/v2/tables/{table}/records"

    offset=0
    rows=[]

    while True:

        r=requests.get(url,headers=HEAD_NOCO,params={"limit":200,"offset":offset})
        data=r.json()

        batch=data.get("list",[])

        if not batch:
            break

        rows.extend(batch)

        offset+=200

    return rows

# --------------------------------------------------

def insert(table,payload):

    url=f"{NOCODB}/api/v2/tables/{table}/records"

    r=requests.post(url,headers=HEAD_NOCO,json=payload)

    if r.status_code not in [200,201]:
        print("INSERT ERROR",r.text)

# --------------------------------------------------

def update(table,row_id,payload):

    url=f"{NOCODB}/api/v2/tables/{table}/records"

    payload["Id"]=row_id

    r=requests.patch(url,headers=HEAD_NOCO,json=payload)

    if r.status_code not in [200,201]:
        print("UPDATE ERROR",r.text)

# --------------------------------------------------
# LOAD PRODUCTS TAX FACTOR
# --------------------------------------------------

print("LOAD PRODUCTS")

products=noco_get_all(TABLE_PRODUCTS)

tax_map={}

for p in products:

    tax_map[p["bsale_id"]]=p.get("tax_factor",1)

# --------------------------------------------------
# LOAD EXISTING COSTS
# --------------------------------------------------

print("LOAD EXISTING COSTS")

cost_rows=noco_get_all(TABLE_COSTS)

cost_map={}

for r in cost_rows:

    cost_map[r["variant_id"]]=r["Id"]

# --------------------------------------------------
# LOAD VARIANTS
# --------------------------------------------------

print("LOAD VARIANTS")

variants=[]

offset=0

while True:

    data=bsale_get(f"{BASE}/variants.json",{"limit":LIMIT,"offset":offset})

    items=data.get("items",[])

    if not items:
        break

    variants.extend(items)

    offset+=LIMIT

print("VARIANTS",len(variants))

# --------------------------------------------------
# COST AVERAGE
# --------------------------------------------------

for v in variants:

    variant_id=int(v["id"])

    cost_data=bsale_get(f"{BASE}/variants/{variant_id}/costs.json")

    if not cost_data:
        continue

    avg_cost=cost_data.get("averageCost")

    product_id=int(v["product"]["id"])

    tax_factor=tax_map.get(product_id,1)

    cost_gross=None

    if avg_cost:
        cost_gross=avg_cost*tax_factor

    payload={
        "variant_id":variant_id,
        "average_cost_net":avg_cost,
        "average_cost_gross":cost_gross,
        "tax_factor":tax_factor,
        "last_update":datetime.utcnow().isoformat()
    }

    if variant_id in cost_map:

        update(TABLE_COSTS,cost_map[variant_id],payload)

    else:

        insert(TABLE_COSTS,payload)

print("COSTS DONE")

# --------------------------------------------------
# PRICES
# --------------------------------------------------

print("SYNC PRICES")

price_lists=bsale_get(f"{BASE}/price_lists.json")["items"]

for pl in price_lists:

    pl_id=pl["id"]

    offset=0

    while True:

        data=bsale_get(f"{BASE}/price_lists/{pl_id}/details.json",{"limit":LIMIT,"offset":offset})

        items=data.get("items",[])

        if not items:
            break

        for d in items:

            insert(TABLE_PRICES,{
                "variant_id":d["variant"]["id"],
                "price_list_id":pl_id,
                "price_net":d["variantValue"],
                "price_gross":d["variantValueWithTaxes"]
            })

        offset+=LIMIT

print("PRICES DONE")

print("SYNC COMPLETE")
