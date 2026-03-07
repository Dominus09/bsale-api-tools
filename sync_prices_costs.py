import requests
import os
import time

print("SYNC PRICES + COSTS")

BASE="https://api.bsale.io/v1"
NOCODB="https://db.quillotana.cl"

TOKEN=os.getenv("BSALE_TOKEN_Mini")
NOCOTOKEN=os.getenv("NocoDB_token")

HEAD={"access_token":TOKEN}

HEADNOCO={
"xc-token":NOCOTOKEN,
"Content-Type":"application/json"
}

LIMIT=50

TABLE_PRICES="mcby3npgc3ig042"
TABLE_COSTS="mdjjvdlwev2o76u"
TABLE_COST_HISTORY="mdfyfwrrrwffg43"


def api(url,params=None):

    while True:

        r=requests.get(url,headers=HEAD,params=params)

        if r.status_code==429:
            retry=int(r.json().get("retry_after",60))
            time.sleep(retry)
            continue

        r.raise_for_status()
        return r.json()


def upsert(table,where,payload):

    url=f"{NOCODB}/api/v2/tables/{table}/records"

    r=requests.get(url,headers=HEADNOCO,params={"where":where})

    data=r.json()

    if data["list"]:

        row=data["list"][0]["Id"]

        requests.patch(
        f"{url}/{row}",
        json=payload,
        headers=HEADNOCO)

    else:

        requests.post(
        url,
        json=payload,
        headers=HEADNOCO)


print("SYNC PRICES")

price_lists=api(f"{BASE}/price_lists.json")["items"]

for pl in price_lists:

    lid=pl["id"]

    offset=0

    while True:

        j=api(
        f"{BASE}/price_lists/{lid}/details.json",
        {"limit":LIMIT,"offset":offset}
        )

        items=j.get("items",[])

        if not items:
            break

        for d in items:

            variant_id=d["variant"]["id"]

            upsert(
            TABLE_PRICES,
            f"(variant_id,eq,{variant_id})~and(price_list_id,eq,{lid})",
            {
            "variant_id":variant_id,
            "price_list_id":lid,
            "price_net":d["variantValue"],
            "price_gross":d["variantValueWithTaxes"]
            })

        offset+=LIMIT


print("SYNC COSTS")

variants=api(f"{BASE}/variants.json",{"limit":LIMIT})["items"]

for v in variants:

    vid=v["id"]

    cost=api(f"{BASE}/variants/{vid}/costs.json")

    avg=cost.get("averageCost")

    upsert(
    TABLE_COSTS,
    f"(variant_id,eq,{vid})",
    {
    "variant_id":vid,
    "average_cost_net":avg
    })

    history=cost.get("history",[])

    for h in history:

        entry=h["admissionDate"]

        upsert(
        TABLE_COST_HISTORY,
        f"(variant_id,eq,{vid})~and(entry_date,eq,{entry})",
        {
        "variant_id":vid,
        "cost_net":h["cost"],
        "fifo_quantity":h["availableFifo"],
        "entry_date":entry
        })


print("SYNC PRICES + COSTS DONE")
