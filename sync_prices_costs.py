import requests
import os
import time

print("SYNC PRICES + COSTS")

BASE="https://api.bsale.io/v1"
NOCODB="https://db.quillotana.cl"

BSALE_TOKEN=os.getenv("BSALE_TOKEN_Mini")
NOCODB_TOKEN=os.getenv("NocoDB_token")

HEAD_BSALE={"access_token":BSALE_TOKEN}

HEAD_NOCO={
    "xc-token":NOCODB_TOKEN,
    "Content-Type":"application/json"
}

TABLE_PRICES="mcby3npgc3ig042"
TABLE_COSTS="mdjjvdlwev2o76u"

LIMIT=50


# ------------------
# HELPERS
# ------------------

def bsale_get(url,params=None):

    while True:

        r=requests.get(url,headers=HEAD_BSALE,params=params)

        if r.status_code==429:
            retry=int(r.json().get("retry_after",60))
            print("RATE LIMIT",retry)
            time.sleep(retry)
            continue

        r.raise_for_status()
        return r.json()


def noco_load_map(table,key_fields):

    url=f"{NOCODB}/api/v2/tables/{table}/records"

    offset=0
    mapping={}

    while True:

        r=requests.get(
            url,
            headers=HEAD_NOCO,
            params={"limit":200,"offset":offset}
        )

        data=r.json()
        rows=data.get("list",[])

        if not rows:
            break

        for row in rows:

            key=tuple(row[k] for k in key_fields)

            mapping[key]=row["Id"]

        offset+=200

    return mapping


def insert(table,payload):

    url=f"{NOCODB}/api/v2/tables/{table}/records"

    requests.post(url,headers=HEAD_NOCO,json=payload)


def update(table,row_id,payload):

    url=f"{NOCODB}/api/v2/tables/{table}/records"

    payload["Id"]=row_id

    requests.patch(url,headers=HEAD_NOCO,json=payload)


# ------------------
# VARIANTS
# ------------------

print("LOAD VARIANTS")

variants=[]
offset=0

while True:

    data=bsale_get(
        f"{BASE}/variants.json",
        {"limit":LIMIT,"offset":offset}
    )

    items=data.get("items",[])

    if not items:
        break

    variants.extend(items)

    offset+=LIMIT

print("VARIANTS",len(variants))


# ------------------
# EXISTING MAPS
# ------------------

print("LOAD NOCO MAPS")

price_map=noco_load_map(TABLE_PRICES,["variant_id","price_list_id"])
cost_map=noco_load_map(TABLE_COSTS,["variant_id"])


# ------------------
# COSTS
# ------------------

print("SYNC COSTS")

for v in variants:

    variant_id=int(v["id"])

    cost=bsale_get(f"{BASE}/variants/{variant_id}/costs.json")

    payload={
        "variant_id":variant_id,
        "average_cost_net":cost.get("averageCost"),
        "total_cost_net":cost.get("totalCost")
    }

    key=(variant_id,)

    if key in cost_map:
        update(TABLE_COSTS,cost_map[key],payload)
    else:
        insert(TABLE_COSTS,payload)


# ------------------
# PRICES
# ------------------

print("SYNC PRICES")

lists=bsale_get(f"{BASE}/price_lists.json")["items"]

for pl in lists:

    price_list_id=int(pl["id"])

    offset=0

    while True:

        data=bsale_get(
            f"{BASE}/price_lists/{price_list_id}/details.json",
            {"limit":LIMIT,"offset":offset}
        )

        items=data.get("items",[])

        if not items:
            break

        for p in items:

            variant_id=int(p["variant"]["id"])

            payload={
                "variant_id":variant_id,
                "price_list_id":price_list_id,
                "price_net":p["variantValue"],
                "price_gross":p["variantValueWithTaxes"]
            }

            key=(variant_id,price_list_id)

            if key in price_map:
                update(TABLE_PRICES,price_map[key],payload)
            else:
                insert(TABLE_PRICES,payload)

        offset+=LIMIT

print("PRICES DONE")
