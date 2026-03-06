import requests
import os
import sys
import json

print("START SYNC")

# ----------------------------
# TOKENS
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
# TABLE IDS
# ----------------------------

TABLE_PRODUCTS = "meke3fsng90uspe"
TABLE_VARIANTS = "msd4vvijzk9pre9"
TABLE_COSTS = "mdjjvdlwev2o76u"
TABLE_PRICES = "mcby3npgc3ig042"
TABLE_STOCKS = "mxs2lyz86cnxd23"

# ----------------------------
# HELPERS
# ----------------------------

def fetch_all(endpoint):
    offset = 0
    results = []

    while True:
        url = f"{BASE_BSALE}/{endpoint}?limit={LIMIT}&offset={offset}"
        r = requests.get(url, headers=headers_bsale, timeout=60)
        r.raise_for_status()

        data = r.json()
        items = data.get("items", [])

        if not items:
            break

        results.extend(items)
        offset += LIMIT

    return results


def insert_noco(table_id, payload):
    url = f"{NOCODB_URL}/api/v2/tables/{table_id}/records"
    r = requests.post(url, json=payload, headers=headers_noco, timeout=60)

    if r.status_code not in [200, 201]:
        print("ERROR INSERT:", table_id, r.status_code, r.text)


def as_dict(value):
    return value if isinstance(value, dict) else None


def extract_id(value):
    """
    Devuelve id si value viene como:
    - {"id": 123}
    - 123
    - "123"
    Si no, devuelve None
    """
    if isinstance(value, dict):
        return value.get("id")

    if isinstance(value, int):
        return value

    if isinstance(value, str):
        value = value.strip()
        if value.isdigit():
            return int(value)

    return None


def extract_number(value):
    """
    Convierte a número si viene como int/float/"123"/"123.45"
    """
    if isinstance(value, (int, float)):
        return value

    if isinstance(value, str):
        value = value.strip().replace(",", ".")
        try:
            return float(value)
        except ValueError:
            return None

    return None


# ----------------------------
# PRODUCTS
# ----------------------------

print("SYNC PRODUCTS")

products = fetch_all("products.json")

for p in products:
    payload = {
        "bsale_id": p.get("id"),
        "name": p.get("name"),
        "classification": json.dumps(p.get("classification"), ensure_ascii=False) if p.get("classification") is not None else None,
        "brand": json.dumps(p.get("brand"), ensure_ascii=False) if p.get("brand") is not None else None
    }
    insert_noco(TABLE_PRODUCTS, payload)

print(f"PRODUCTS OK: {len(products)}")


# ----------------------------
# VARIANTS
# ----------------------------

print("SYNC VARIANTS")

variants = fetch_all("variants.json")

for v in variants:
    product_id = extract_id(v.get("product"))

    payload = {
        "bsale_id": v.get("id"),
        "product_id": product_id,
        "code": v.get("code"),
        "bar_code": v.get("barCode"),
        "description": v.get("description")
    }
    insert_noco(TABLE_VARIANTS, payload)

print(f"VARIANTS OK: {len(variants)}")


# ----------------------------
# COSTS
# ----------------------------

print("SYNC COSTS")

cost_rows = 0
cost_skipped = 0
cost_debug_printed = 0

for v in variants:
    variant_id = v.get("id")
    costs = v.get("costs", [])

    if not isinstance(costs, list):
        if cost_debug_printed < 10:
            print("COSTS NO ES LISTA:", variant_id, type(costs), costs)
            cost_debug_printed += 1
        continue

    for cost in costs:
        if not isinstance(cost, dict):
            cost_skipped += 1
            if cost_debug_printed < 10:
                print("COST ITEM RARO:", variant_id, type(cost), cost)
                cost_debug_printed += 1
            continue

        office_id = extract_id(cost.get("office"))
        cost_value = extract_number(cost.get("cost"))

        payload = {
            "variant_id": variant_id,
            "office_id": office_id,
            "cost": cost_value
        }

        insert_noco(TABLE_COSTS, payload)
        cost_rows += 1

print(f"COSTS OK: {cost_rows}")
print(f"COSTS SKIPPED: {cost_skipped}")


# ----------------------------
# PRICES
# ----------------------------

print("SYNC PRICES")

price_rows = 0
price_skipped = 0
price_debug_printed = 0

for v in variants:
    variant_id = v.get("id")
    prices = v.get("prices", [])

    if not isinstance(prices, list):
        if price_debug_printed < 10:
            print("PRICES NO ES LISTA:", variant_id, type(prices), prices)
            price_debug_printed += 1
        continue

    for price in prices:
        if not isinstance(price, dict):
            price_skipped += 1
            if price_debug_printed < 10:
                print("PRICE ITEM RARO:", variant_id, type(price), price)
                price_debug_printed += 1
            continue

        price_list_id = extract_id(price.get("priceList"))
        price_value = extract_number(price.get("price"))

        payload = {
            "variant_id": variant_id,
            "price_list_id": price_list_id,
            "price": price_value
        }

        insert_noco(TABLE_PRICES, payload)
        price_rows += 1

print(f"PRICES OK: {price_rows}")
print(f"PRICES SKIPPED: {price_skipped}")


# ----------------------------
# STOCKS
# ----------------------------

print("SYNC STOCKS")

stocks = fetch_all("stocks.json")

stock_rows = 0
stock_skipped = 0
stock_debug_printed = 0

for s in stocks:
    variant_id = extract_id(s.get("variant"))
    office_id = extract_id(s.get("office"))

    quantity_available = extract_number(s.get("quantityAvailable"))
    quantity_reserved = extract_number(s.get("quantityReserved"))

    payload = {
        "variant_id": variant_id,
        "office_id": office_id,
        "quantity_available": quantity_available,
        "quantity_reserved": quantity_reserved
    }

    insert_noco(TABLE_STOCKS, payload)
    stock_rows += 1

print(f"STOCKS OK: {stock_rows}")
print(f"STOCKS SKIPPED: {stock_skipped}")

print("SYNC COMPLETADO")
sys.exit(0)
