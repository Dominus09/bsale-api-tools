import requests
import os
import json

print("TEST PRODUCT TAXES")

# ----------------------------
# CONFIG
# ----------------------------

BSALE_TOKEN = os.getenv("BSALE_TOKEN_Mini")

NOCODB_TOKEN = os.getenv("NocoDB_token")
NOCODB_URL = "https://db.quillotana.cl"

BASE_BSALE = "https://api.bsale.io/v1"

TABLE_TEST = "m2u5fw9fhgu2pj8"

headers_bsale = {
    "access_token": BSALE_TOKEN
}

headers_noco = {
    "xc-token": NOCODB_TOKEN,
    "Content-Type": "application/json"
}

LIMIT = 50


# ----------------------------
# HELPERS
# ----------------------------

def bsale_get(url, params=None):

    r = requests.get(url, headers=headers_bsale, params=params)

    if r.status_code != 200:
        print("BSALE ERROR:", r.text)

    r.raise_for_status()

    return r.json()


def insert_test(payload):

    url = f"{NOCODB_URL}/api/v2/tables/{TABLE_TEST}/records"

    r = requests.post(url, json=payload, headers=headers_noco)

    if r.status_code not in [200,201]:
        print("INSERT ERROR:", r.text)


# ----------------------------
# GET PRODUCTS
# ----------------------------

print("LOADING PRODUCTS")

products = bsale_get(
    f"{BASE_BSALE}/products.json",
    {"limit": 10, "offset": 0}
)

products = products.get("items", [])

print("PRODUCTS FOUND:", len(products))


# ----------------------------
# TEST TAXES
# ----------------------------

for p in products:

    product_id = p["id"]

    print("PRODUCT:", product_id)

    product_taxes = p.get("product_taxes")

    if isinstance(product_taxes, dict) and "href" in product_taxes:

        tax_url = product_taxes["href"]

        print("TAX URL:", tax_url)

        try:

            tax_data = bsale_get(tax_url)

            print("TAX RESPONSE:", tax_data)

            insert_test({

                "product_id": product_id,
                "tax_url": tax_url,
                "tax_response_json": json.dumps(tax_data)

            })

        except Exception as e:

            print("TAX ERROR:", e)

    else:

        print("NO TAX LINK")

print("TEST COMPLETADO")
