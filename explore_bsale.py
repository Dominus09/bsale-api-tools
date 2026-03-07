import requests
import os
import json

BSALE_TOKEN = os.getenv("BSALE_TOKEN_Mini")

headers = {
    "access_token": BSALE_TOKEN
}

url = "https://api.bsale.io/v1/variants.json?limit=5"

r = requests.get(url, headers=headers)

data = r.json()

for v in data["items"]:

    print("\nVARIANT ID:", v["id"])

    print("\nFULL DATA:")
    print(json.dumps(v, indent=2))
