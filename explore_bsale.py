import requests
import os
import sys
import json
from datetime import datetime, timezone

print("START SYNC")

# ----------------------------
# CONFIG
# ----------------------------

BSALE_TOKEN = os.getenv("BSALE_TOKEN_Mini")
NOCODB_TOKEN = "R3EhSD8si-WSVdsPxlQVGAfiHRRcDR9cHGHJdBJL"
NOCODB_URL = "https://db.quillotana.cl"
BASE_BSALE = "https://api.bsale.io/v1"
LIMIT = 50

headers_bsale = {
    "access_token": BSALE_TOKEN
}

headers_noco = {
    "xc-token": NOCODB_TOKEN,
    "Content-Type": "application/json"
}

# ----------------------------
# TABLE IDS NOCO
# ----------------------------

TABLE_PRODUCTS = "meke3fsng90uspe"
TABLE_VARIANTS = "msd4vvijzk9pre9"
TABLE_COSTS = "mdjjvdlwev2o76u"
TABLE_PRICES = "mcby3npgc3ig042"
TABLE_STOCKS = "mxs2lyz86cnxd23"
TABLE_OFFICES = "m878eot7j6fi5v7"
TABLE_PRICELIST = "m8zibme0z28jls6"
TABLE_TAXES = "mary3rk9y5rwviu"
TABLE_COST_HISTORY = "mdfyfwrrrwffg43"

TABLES_TO_REFRESH = [
    TABLE_COST_HISTORY,
    TABLE_COSTS,
    TABLE_PRICES,
    TABLE_STOCKS,
    TABLE_VARIANTS,
    TABLE_PRODUCTS,
    TABLE_OFFICES,
    TABLE_PRICELIST,
    TABLE_TAXES,
]

# ----------------------------
# HELPERS
# ----------------------------

def bsale_get(url, params=None):
    r = requests.get(url, headers=headers_bsale, params=params, timeout=90)
    r.raise_for_status()
    return r.json()

def fetch_all_collection(endpoint, extra_params=None):
    offset = 0
    results = []
    extra_params = extra_params or {}

    while True:
        url = f"{BASE_BSALE}/{endpoint}"
        params = {"limit": LIMIT, "offset": offset}
        params.update(extra_params)

        data = bsale_get(url, params=params)
        items = data.get("items", [])

        if not items:
            break

        results.extend(items)
        offset += LIMIT

    return results

def fetch_all_full_url(url):
    offset = 0
    results = []

    while True:
        params = {"limit": LIMIT, "offset": offset}
        data = bsale_get(url, params=params)
        items = data.get("items", [])

        if not items:
            break

        results.extend(items)
        offset += LIMIT

    return results

def noco_get_records(table_id, limit=200, offset=0):
    url = f"{NOCODB_URL}/api/v2/tables/{table_id}/records"
    r = requests.get(
        url,
        headers=headers_noco,
        params={"limit": limit, "offset": offset},
        timeout=90
    )
    r.raise_for_status()
    return r.json()

def noco_delete_record(table_id, row_id):
    url = f"{NOCODB_URL}/api/v2/tables/{table_id}/records/{row_id}"
    r = requests.delete(url, headers=headers_noco, timeout=90)
    if r.status_code not in [200, 201]:
        print("DELETE ERROR:", table_id, row_id, r.status_code, r.text)

def clear_table(table_id):
    print(f"CLEAR TABLE {table_id}")
    offset = 0
    total_deleted = 0

    while True:
        data = noco_get_records(table_id, limit=200, offset=0)
        rows = data.get("list", [])

        if not rows:
            break

        for row in rows:
            row_id = row.get("Id")
            if row_id is not None:
                noco_delete_record(table_id, row_id)
                total_deleted += 1

    print(f"CLEARED {table_id}: {total_deleted}")

def insert_noco(table_id, payload):
    url = f"{NOCODB_URL}/api/v2/tables/{table_id}/records"
    r = requests.post(url, json=payload, headers=headers_noco, timeout=90)

    if r.status_code not in [200, 201]:
        print("INSERT ERROR:", table_id, r.status_code, r.text)

def as_iso_date(unix_ts):
    if unix_ts in [None, "", 0]:
        return None
    try:
        return datetime.fromtimestamp(int(unix_ts), tz=timezone.utc).strftime("%Y-%m-%d")
    except Exception:
        return None

def extract_id(value):
    if isinstance(value, dict):
        return int(value["id"]) if str(value.get("id", "")).isdigit() else value.get("id")
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip().isdigit():
        return int(value.strip())
    return None

def extract_number(value):
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        txt = value.strip().replace(",", ".")
        try:
            return float(txt)
        except ValueError:
            return None
    return None

def extract_tax_ids(product_taxes):
    tax_ids = []

    if isinstance(product_taxes, list):
        for item in product_taxes:
            tax_id = extract_id(item)
            if tax_id is not None:
                tax_ids.append(tax_id)

    elif isinstance(product_taxes, dict):
        if isinstance(product_taxes.get("items"), list):
            for item in product_taxes["items"]:
                tax_id = extract_id(item)
                if tax_id is not None:
                    tax_ids.append(tax_id)

    return tax_ids

def calc_tax_factor(tax_ids, taxes_map):
    factor = 1.0
    for tax_id in tax_ids:
        pct = taxes_map.get(tax_id)
        if pct is not None:
            factor *= (1 + (pct / 100.0))
    return factor

# ----------------------------
# FULL REFRESH
# ----------------------------

print("FULL REFRESH TABLES")
for table_id in TABLES_TO_REFRESH:
    clear_table(table_id)

# ----------------------------
# TAXES
# ----------------------------

print("SYNC TAXES")
taxes = fetch_all_collection("taxes.json")
taxes_map = {}

for t in taxes:
    tax_id = extract_id(t.get("id"))
    pct = extract_number(t.get("percentage"))
    taxes_map[tax_id] = pct if pct is not None else 0.0

    insert_noco(TABLE_TAXES, {
        "bsale_id": tax_id,
        "name": t.get("name"),
        "percentage": pct
    })

print(f"TAXES OK: {len(taxes)}")

# ----------------------------
# OFFICES
# ----------------------------

print("SYNC OFFICES")
offices = fetch_all_collection("offices.json")

for o in offices:
    insert_noco(TABLE_OFFICES, {
        "bsale_id": extract_id(o.get("id")),
        "name": o.get("name")
    })

print(f"OFFICES OK: {len(offices)}")

# ----------------------------
# PRICE LISTS
# ----------------------------

print("SYNC PRICE LISTS")
price_lists = fetch_all_collection("price_lists.json")

for p in price_lists:
    insert_noco(TABLE_PRICELIST, {
        "bsale_id": extract_id(p.get("id")),
        "name": p.get("name"),
        "description": p.get("description")
    })

print(f"PRICE LISTS OK: {len(price_lists)}")

# ----------------------------
# PRODUCTS
# Intentamos expandir product_taxes
# ----------------------------

print("SYNC PRODUCTS")
products = fetch_all_collection("products.json", extra_params={"expand": "[product_taxes]"})

product_tax_factor_map = {}

for p in products:
    product_id = extract_id(p.get("id"))
    tax_ids = extract_tax_ids(p.get("product_taxes"))
    tax_names = []
    for tax_id in tax_ids:
        for t in taxes:
            if extract_id(t.get("id")) == tax_id:
                tax_names.append(t.get("name"))

    tax_factor = calc_tax_factor(tax_ids, taxes_map)
    product_tax_factor_map[product_id] = tax_factor

    insert_noco(TABLE_PRODUCTS, {
        "bsale_id": product_id,
        "name": p.get("name"),
        "classification": json.dumps(p.get("classification"), ensure_ascii=False) if p.get("classification") is not None else None,
        "brand": json.dumps(p.get("brand"), ensure_ascii=False) if p.get("brand") is not None else None,
        "tax_ids_json": json.dumps(tax_ids, ensure_ascii=False),
        "tax_names_json": json.dumps(tax_names, ensure_ascii=False),
        "tax_factor": tax_factor
    })

print(f"PRODUCTS OK: {len(products)}")

# ----------------------------
# VARIANTS
# ----------------------------

print("SYNC VARIANTS")
variants = fetch_all_collection("variants.json")

variant_product_map = {}

for v in variants:
    variant_id = extract_id(v.get("id"))
    product_id = extract_id(v.get("product"))
    variant_product_map[variant_id] = product_id

    insert_noco(TABLE_VARIANTS, {
        "bsale_id": variant_id,
        "product_id": product_id,
        "code": v.get("code"),
        "bar_code": v.get("barCode"),
        "description": v.get("description")
    })

print(f"VARIANTS OK: {len(variants)}")

# ----------------------------
# STOCKS
# ----------------------------

print("SYNC STOCKS")
stocks = fetch_all_collection("stocks.json")

for s in stocks:
    insert_noco(TABLE_STOCKS, {
        "variant_id": extract_id(s.get("variant")),
        "office_id": extract_id(s.get("office")),
        "quantity_available": extract_number(s.get("quantityAvailable")),
        "quantity_reserved": extract_number(s.get("quantityReserved"))
    })

print(f"STOCKS OK: {len(stocks)}")

# ----------------------------
# COSTS
# variants/{id}/costs.json
# ----------------------------

print("SYNC COSTS")
cost_rows = 0
cost_hist_rows = 0

for idx, v in enumerate(variants, start=1):
    variant_id = extract_id(v.get("id"))
    product_id = variant_product_map.get(variant_id)
    tax_factor = product_tax_factor_map.get(product_id, 1.0)

    costs_url = f"{BASE_BSALE}/variants/{variant_id}/costs.json"
    cost_data = bsale_get(costs_url)

    average_cost_net = extract_number(cost_data.get("averageCost"))
    total_cost_net = extract_number(cost_data.get("totalCost"))
    history = cost_data.get("history", []) if isinstance(cost_data.get("history"), list) else []

    last_cost_net = None
    last_cost_gross = None
    last_entry_date = None

    if history:
        sorted_hist = sorted(
            history,
            key=lambda x: int(x.get("admissionDate", 0)) if str(x.get("admissionDate", "")).isdigit() else 0
        )
        last_item = sorted_hist[-1]
        last_cost_net = extract_number(last_item.get("cost"))
        last_cost_gross = (last_cost_net * tax_factor) if last_cost_net is not None else None
        last_entry_date = as_iso_date(last_item.get("admissionDate"))

    average_cost_gross = (average_cost_net * tax_factor) if average_cost_net is not None else None

    insert_noco(TABLE_COSTS, {
        "variant_id": variant_id,
        "average_cost_net": average_cost_net,
        "average_cost_gross": average_cost_gross,
        "total_cost_net": total_cost_net,
        "last_cost_net": last_cost_net,
        "last_cost_gross": last_cost_gross,
        "last_entry_date": last_entry_date,
        "tax_factor": tax_factor
    })
    cost_rows += 1

    for h in history:
        cost_net = extract_number(h.get("cost"))
        cost_gross = (cost_net * tax_factor) if cost_net is not None else None

        insert_noco(TABLE_COST_HISTORY, {
            "variant_id": variant_id,
            "cost_net": cost_net,
            "cost_gross": cost_gross,
            "fifo_quantity": extract_number(h.get("availableFifo")),
            "entry_date": as_iso_date(h.get("admissionDate")),
            "tax_factor": tax_factor
        })
        cost_hist_rows += 1

    if idx % 200 == 0:
        print(f"COSTS PROGRESS: {idx}/{len(variants)}")

print(f"COSTS OK: {cost_rows}")
print(f"COST HISTORY OK: {cost_hist_rows}")

# ----------------------------
# PRICES
# price_lists/{id}/details.json
# ----------------------------

print("SYNC PRICES")
price_rows = 0

for p in price_lists:
    price_list_id = extract_id(p.get("id"))
    details_url = f"{BASE_BSALE}/price_lists/{price_list_id}/details.json"
    details = fetch_all_full_url(details_url)

    for d in details:
        insert_noco(TABLE_PRICES, {
            "variant_id": extract_id(d.get("variant")),
            "price_list_id": price_list_id,
            "price_net": extract_number(d.get("variantValue")),
            "price_gross": extract_number(d.get("variantValueWithTaxes"))
        })
        price_rows += 1

    print(f"PRICES LIST {price_list_id} OK: {len(details)}")

print(f"PRICES OK: {price_rows}")

print("SYNC COMPLETADO")
sys.exit(0)
