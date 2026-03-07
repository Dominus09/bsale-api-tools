import requests
import os
import json

BSALE_TOKEN = os.getenv("BSALE_TOKEN_Mini")

headers = {
    "access_token": BSALE_TOKEN
}

variant_id = 5547

print("TEST COSTS")

costs_url = f"https://api.bsale.io/v1/variants/{variant_id}/costs.json"

r = requests.get(costs_url, headers=headers)

print(json.dumps(r.json(), indent=2))


print("\nTEST PRICES")

prices_url = f"https://api.bsale.io/v1/price_lists/details.json?variantid={variant_id}"

r = requests.get(prices_url, headers=headers)

print(json.dumps(r.json(), indent=2))
