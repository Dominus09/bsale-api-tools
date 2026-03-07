
import requests
import os
import time
import json

print("SYNC CATALOG START")

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

TABLE_TAXES="mary3rk9y5rwviu"
TABLE_PRODUCT_TYPES="mcir9ile6id3813"
TABLE_PRODUCTS="meke3fsng90uspe"
TABLE_VARIANTS="msd4vvijzk9pre9"
TABLE_OFFICES="m878eot7j6fi5v7"
TABLE_PRICELIST="m8zibme0z28jls6"


def api(url,params=None):

    while True:

        r=requests.get(url,headers=HEAD,params=params)

        if r.status_code==429:
            retry=int(r.json().get("retry_after",60))
            print("RATE LIMIT",retry)
            time.sleep(retry)
            continue

        r.raise_for_status()
        return r.json()


def fetch(endpoint):

    offset=0
    results=[]

    while True:

        j=api(
        f"{BASE}/{endpoint}",
        {"limit":LIMIT,"offset":offset}
        )

        items=j.get("items",[])

        if not items:
            break

        results.extend(items)
        offset+=LIMIT

    return results


def upsert(table,field,value,payload):

    url=f"{NOCODB}/api/v2/tables/{table}/records"

    r=requests.get(
        url,
        headers=HEADNOCO,
        params={"where":f"({field},eq,{value})"}
    )

    data=r.json()

    if data["list"]:

        row=data["list"][0]["Id"]

        requests.patch(
            f"{url}/{row}",
            json=payload,
            headers=HEADNOCO
        )

    else:

        requests.post(
            url,
            json=payload,
            headers=HEADNOCO
        )


print("SYNC TAXES")

taxes=fetch("taxes.json")

tax_map={}

for t in taxes:

    tax_id=int(t["id"])
    pct=float(t["percentage"])

    tax_map[tax_id]={
        "name":t["name"],
        "percentage":pct
    }

    upsert(
        TABLE_TAXES,
        "bsale_id",
        tax_id,
        {
            "bsale_id":tax_id,
            "name":t["name"],
            "percentage":pct
        }
    )


print("SYNC PRODUCT TYPES")

for pt in fetch("product_types.json"):

    upsert(
        TABLE_PRODUCT_TYPES,
        "bsale_id",
        pt["id"],
        {
            "bsale_id":pt["id"],
            "name":pt["name"],
            "state":pt["state"]
        }
    )


print("SYNC PRODUCTS")

products=fetch("products.json")

for p in products:

    product_id=p["id"]

    product_type_id=None
    if p.get("product_type"):
        product_type_id=p["product_type"]["id"]

    brand=None
    if isinstance(p.get("brand"),dict):
        brand=p["brand"].get("name")

    classification=p.get("classification")

    tax_ids=[]
    tax_names=[]
    tax_total=0

    if p.get("product_taxes") and "href" in p["product_taxes"]:

        tax_data=api(p["product_taxes"]["href"])

        for item in tax_data.get("items",[]):

            tax_id=int(item["tax"]["id"])

            tax_ids.append(tax_id)

            if tax_id in tax_map:

                tax_names.append(tax_map[tax_id]["name"])
                tax_total+=tax_map[tax_id]["percentage"]

    tax_factor=1+(tax_total/100)

    upsert(
        TABLE_PRODUCTS,
        "bsale_id",
        product_id,
        {
            "bsale_id":product_id,
            "name":p["name"],
            "classification":classification,
            "brand":brand,
            "product_type_id":product_type_id,
            "tax_ids_json":json.dumps(tax_ids),
            "tax_names_json":json.dumps(tax_names),
            "tax_factor":round(tax_factor,3)
        }
    )


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
            "bar_code":v.get("barCode"),
            "state":v.get("state")
        }
    )


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


print("SYNC PRICE LISTS")

for p in fetch("price_lists.json"):

    upsert(
        TABLE_PRICELIST,
        "bsale_id",
        p["id"],
        {
            "bsale_id":p["id"],
            "name":p["name"],
            "description":p.get("description")
        }
    )


print("SYNC CATALOG DONE")
