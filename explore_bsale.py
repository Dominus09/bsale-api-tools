import requests
import os

TOKEN = os.getenv("BSALE_TOKEN_Mini")

headers = {
    "access_token": TOKEN
}

BASE = "https://api.bsale.io/v1"

endpoints = [
    "products.json",
    "variants.json",
    "price_lists.json",
    "stocks.json"
]

limit = 50


for endpoint in endpoints:

    print("\n==============================")
    print("ENDPOINT:", endpoint)
    print("==============================")

    offset = 0
    total = 0
    fields = set()

    while True:

        url = f"{BASE}/{endpoint}?limit={limit}&offset={offset}"

        r = requests.get(url, headers=headers)
        data = r.json()

        items = data.get("items", [])

        if len(items) == 0:
            break

        total += len(items)

        for item in items:
            for key in item.keys():
                fields.add(key)

        offset += limit

    print("TOTAL REGISTROS:", total)

    print("\nCAMPOS DETECTADOS:")

    for f in sorted(fields):
        print("-", f)
