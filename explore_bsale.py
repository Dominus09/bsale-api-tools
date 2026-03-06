import requests
import os
import json

TOKEN = os.getenv("BSALE_TOKEN_Mini")

headers = {
    "access_token": TOKEN
}

BASE = "https://api.bsale.io/v1"

limit = 50


def fetch_all(endpoint):

    offset = 0
    all_items = []

    while True:

        url = f"{BASE}/{endpoint}?limit={limit}&offset={offset}"

        r = requests.get(url, headers=headers)
        data = r.json()

        items = data.get("items", [])

        if len(items) == 0:
            break

        all_items.extend(items)

        offset += limit

    return all_items


print("\n==============================")
print("OFFICES")
print("==============================")

offices = fetch_all("offices.json")

print("TOTAL:", len(offices))

for o in offices:
    print(o)


print("\n==============================")
print("TAXES")
print("==============================")

taxes = fetch_all("taxes.json")

print("TOTAL:", len(taxes))

for t in taxes:
    print(t)


print("\n==============================")
print("PRICE LISTS")
print("==============================")

price_lists = fetch_all("price_lists.json")

print("TOTAL:", len(price_lists))

for p in price_lists:
    print(p)


print("\n==============================")
print("VARIANTS (EJEMPLO DETALLADO)")
print("==============================")

variants = fetch_all("variants.json")

print("TOTAL VARIANTS:", len(variants))

example = variants[0]

print("\nVARIANT EJEMPLO:\n")

print(json.dumps(example, indent=2))


print("\nCOSTOS DETECTADOS\n")

for c in example.get("costs", []):
    print(c)


print("\nPRECIOS DETECTADOS\n")

for p in example.get("prices", []):
    print(p)
