import requests
import os
import json

print("SYNC CATALOG")

BASE = "https://api.bsale.io/v1"
NOCODB = "https://db.quillotana.cl"

BSALE_TOKEN = os.getenv("BSALE_TOKEN_Mini")
NOCODB_TOKEN = os.getenv("NocoDB_token")

HEAD_BSALE = {"access_token": BSALE_TOKEN}

HEAD_NOCO = {
    "xc-token": NOCODB_TOKEN,
    "Content-Type": "application/json"
}

TABLE_PRODUCTS = "meke3fsng90uspe"
TABLE_PRODUCT_TYPES = "mcir9ile6id3813"
TABLE_TAXES = "mary3rk9y5rwviu"

LIMIT = 50


# --------------------------
# HELPERS
# --------------------------

def bsale_get(url, params=None):

    r = requests.get(url, headers=HEAD_BSALE, params=params)
    r.raise_for_status()
    return r.json()


def noco_get_all(table):

    url = f"{NOCODB}/api/v2/tables/{table}/records"

    offset = 0
    existing = {}

    while True:

        r = requests.get(
            url,
            headers=HEAD_NOCO,
            params={"limit":200,"offset":offset}
        )

        data = r.json()
        rows = data.get("list",[])

        if not rows:
            break

        for row in rows:
            existing[row["bsale_id"]] = row["Id"]

        offset += 200

    return existing


def insert(table,payload):

    url = f"{NOCODB}/api/v2/tables/{table}/records"

    requests.post(url,headers=HEAD_NOCO,json=payload)


def update(table,row_id,payload):

    url = f"{NOCODB}/api/v2/tables/{table}/records"

    payload["Id"]=row_id

    requests.patch(url,headers=HEAD_NOCO,json=payload)


# --------------------------
# TAXES
# --------------------------

print("LOAD TAXES")

taxes = bsale_get(f"{BASE}/taxes.json")["items"]

tax_map = {}

existing_taxes = noco_get_all(TABLE_TAXES)

for t in taxes:

    tax_id = int(t["id"])

    tax_map[tax_id] = {
        "name":t["name"],
        "percentage":float(t["percentage"])
    }

    payload={
        "bsale_id":tax_id,
        "name":t["name"],
        "percentage":t["percentage"]
    }

    if tax_id in existing_taxes:
        update(TABLE_TAXES,existing_taxes[tax_id],payload)
    else:
        insert(TABLE_TAXES,payload)


# --------------------------
# PRODUCT TYPES
# --------------------------

print("LOAD PRODUCT TYPES")

types = bsale_get(f"{BASE}/product_types.json")["items"]

existing_types = noco_get_all(TABLE_PRODUCT_TYPES)

for pt in types:

    pt_id = int(pt["id"])

    payload={
        "bsale_id":pt_id,
        "name":pt["name"],
        "state":pt["state"]
    }

    if pt_id in existing_types:
        update(TABLE_PRODUCT_TYPES,existing_types[pt_id],payload)
    else:
        insert(TABLE_PRODUCT_TYPES,payload)


# --------------------------
# PRODUCTS
# --------------------------

print("LOAD PRODUCTS")

existing_products = noco_get_all(TABLE_PRODUCTS)

offset=0

while True:

    data=bsale_get(
        f"{BASE}/products.json",
        {"limit":LIMIT,"offset":offset}
    )

    items=data.get("items",[])

    if not items:
        break

    for p in items:

        product_id=int(p["id"])

        tax_ids=[]
        tax_names=[]
        tax_total=0

        if "product_taxes" in p and "href" in p["product_taxes"]:

            tax_data=bsale_get(p["product_taxes"]["href"])

            for t in tax_data["items"]:

                tax_id=int(t["tax"]["id"])

                tax_ids.append(tax_id)

                tax_names.append(tax_map[tax_id]["name"])

                tax_total+=tax_map[tax_id]["percentage"]

        tax_factor=1+(tax_total/100)

        payload={
            "bsale_id":product_id,
            "name":p["name"],
            "product_type_id":p["product_type"]["id"] if p.get("product_type") else None,
            "tax_ids_json":json.dumps(tax_ids),
            "tax_names_json":json.dumps(tax_names),
            "tax_factor":round(tax_factor,3)
        }

        if product_id in existing_products:
            update(TABLE_PRODUCTS,existing_products[product_id],payload)
        else:
            insert(TABLE_PRODUCTS,payload)

    offset+=LIMIT

print("CATALOG DONE")
