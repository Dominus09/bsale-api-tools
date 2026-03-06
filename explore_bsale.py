import requests
import json

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
offset = 0

for endpoint in endpoints:

```
print("\n=======================")
print("ENDPOINT:", endpoint)
print("=======================")

url = f"{BASE}/{endpoint}?limit={limit}&offset={offset}"

r = requests.get(url, headers=headers)

data = r.json()

items = data.get("items", [])

if len(items) == 0:
    print("No data")
    continue

item = items[0]

print("\nCampos detectados:\n")

for key in item.keys():
    print(key)

print("\nEjemplo registro:\n")

print(json.dumps(item, indent=2))
```
