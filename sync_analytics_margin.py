import requests
import os
from datetime import datetime, timezone

print("SYNC ANALYTICS MARGIN")

NOCODB = "https://db.quillotana.cl"
TOKEN = "R3EhSD8si-WSVdsPxlQVGAfiHRRcDR9cHGHJdBJL"

HEAD = {
    "xc-token": TOKEN,
    "Content-Type": "application/json"
}

# TABLAS

TABLE_PRICES = "mcby3npgc3ig042"
TABLE_COSTS = "mdjjvdlwev2o76u"
TABLE_VARIANTS = "msd4vvijzk9pre9"
TABLE_PRODUCTS = "meke3fsng90uspe"
TABLE_PRODUCT_TYPES = "mcir9ile6id3813"
TABLE_PRICE_LIST = "m8zibme0z28jls6"

TABLE_RULES = "mznxcna8g1cclfh"
TABLE_ANALYTICS = "m777i9qvqgbvpuk"

LIMIT = 200


# ---------------------------------------------------
# GET ALL RECORDS
# ---------------------------------------------------

def get_all(table):

    url = f"{NOCODB}/api/v2/tables/{table}/records"

    rows = []
    offset = 0

    while True:

        r = requests.get(
            url,
            headers=HEAD,
            params={"limit": LIMIT, "offset": offset}
        )

        data = r.json()

        batch = data.get("list", [])

        if not batch:
            break

        rows.extend(batch)

        offset += LIMIT

    return rows


# ---------------------------------------------------
# INSERT BATCH
# ---------------------------------------------------

def insert_batch(table, rows):

    url = f"{NOCODB}/api/v2/tables/{table}/records"

    for i in range(0, len(rows), 200):

        chunk = rows[i:i+200]

        r = requests.post(url, headers=HEAD, json=chunk)

        if r.status_code not in [200,201]:
            print("INSERT ERROR:", r.text)


# ---------------------------------------------------
# LOAD DATA
# ---------------------------------------------------

print("LOAD TABLES")

prices = get_all(TABLE_PRICES)
costs = get_all(TABLE_COSTS)
variants = get_all(TABLE_VARIANTS)
products = get_all(TABLE_PRODUCTS)
product_types = get_all(TABLE_PRODUCT_TYPES)
price_lists = get_all(TABLE_PRICE_LIST)
rules = get_all(TABLE_RULES)

print("prices:", len(prices))
print("costs:", len(costs))
print("variants:", len(variants))
print("products:", len(products))
print("rules:", len(rules))

rules = [r for r in rules if r.get("active")]

print("rules active:", len(rules))


# ---------------------------------------------------
# MAPS (MULTI EMPRESA)
# ---------------------------------------------------

variant_map = {
    f"{v['company_id']}_{v['bsale_id']}": {
        "product_id": v["product_id"],
        "variant_name": v.get("description")
    }
    for v in variants
}

product_map = {
    f"{p['company_id']}_{p['bsale_id']}": {
        "product_name": p.get("name"),
        "product_type_id": p.get("product_type_id"),
        "tax_factor": float(p.get("tax_factor") or 1)
    }
    for p in products
}

product_type_map = {
    f"{t['company_id']}_{t['bsale_id']}": t.get("name")
    for t in product_types
}

price_list_map = {
    f"{p['company_id']}_{p['bsale_id']}": p.get("name")
    for p in price_lists
}

cost_map = {
    f"{c['company_id']}_{c['variant_id']}": c
    for c in costs
}

rule_map = {
    f"{r['company_id']}_{r['price_list_id']}_{r['product_type_id']}": r
    for r in rules
}


# ---------------------------------------------------
# BUILD ANALYTICS
# ---------------------------------------------------

rows = []

for p in prices:

    company_id = p["company_id"]
    variant_id = p["variant_id"]
    price_list_id = p["price_list_id"]

    price = float(p.get("price_gross") or 0)

    variant_key = f"{company_id}_{variant_id}"

    if variant_key not in variant_map:
        continue

    variant = variant_map[variant_key]

    product_id = variant["product_id"]
    variant_name = variant["variant_name"]

    product_key = f"{company_id}_{product_id}"

    product = product_map.get(product_key)

    if not product:
        continue

    product_name = product["product_name"]
    product_type_id = product["product_type_id"]

    product_type_name = product_type_map.get(
        f"{company_id}_{product_type_id}"
    )

    price_list_name = price_list_map.get(
        f"{company_id}_{price_list_id}"
    )

    rule_key = f"{company_id}_{price_list_id}_{product_type_id}"

    if rule_key not in rule_map:
        continue

    cost_key = f"{company_id}_{variant_id}"

    cost_row = cost_map.get(cost_key)

    if not cost_row:
        continue

    cost_net = float(cost_row.get("average_cost_net") or 0)
    cost_gross = cost_row.get("average_cost_gross")

    tax_factor = float(product.get("tax_factor") or 1)

    # COSTO BRUTO

    if cost_gross:
        cost = float(cost_gross)
    else:
        cost = cost_net * tax_factor

    if cost <= 0:
        continue

    # PRECIO NO EXISTENTE

    if price <= 1:

        rows.append({

            "company_id": company_id,

            "variant_id": variant_id,
            "variant_name": variant_name,

            "product_id": product_id,
            "product_name": product_name,

            "product_type_id": product_type_id,
            "product_type_name": product_type_name,

            "price_list_id": price_list_id,
            "price_list_name": price_list_name,

            "price_gross": round(price,2),
            "cost_gross": round(cost,2),

            "margin": 0,
            "margin_percent": 0,

            "min_margin": None,
            "max_margin": None,

            "status": "NO_PRICE",

            "last_update": datetime.now(timezone.utc).isoformat()

        })

        continue

    # MARGEN

    margin = price - cost
    margin_percent = (margin / price) * 100

    rule = rule_map[rule_key]

    status = "OK"

    if margin_percent < rule["min_margin"]:
        status = "LOW"

    elif margin_percent > 40:
        status = "ULTRA_HIGH"

    elif margin_percent > rule["max_margin"]:
        status = "HIGH"

    rows.append({

        "company_id": company_id,

        "variant_id": variant_id,
        "variant_name": variant_name,

        "product_id": product_id,
        "product_name": product_name,

        "product_type_id": product_type_id,
        "product_type_name": product_type_name,

        "price_list_id": price_list_id,
        "price_list_name": price_list_name,

        "price_gross": round(price,2),
        "cost_gross": round(cost,2),

        "margin": round(margin,2),
        "margin_percent": round(margin_percent,2),

        "min_margin": rule["min_margin"],
        "max_margin": rule["max_margin"],

        "status": status,

        "last_update": datetime.now(timezone.utc).isoformat()

    })


print("analytics rows:", len(rows))


# ---------------------------------------------------
# INSERT DATA
# ---------------------------------------------------

insert_batch(TABLE_ANALYTICS, rows)

print("ANALYTICS SYNC DONE")
