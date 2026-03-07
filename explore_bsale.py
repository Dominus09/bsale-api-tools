import requests, os, json

headers = {
 "access_token": os.getenv("BSALE_TOKEN_Mini")
}

url = "https://api.bsale.io/v1/price_lists.json?expand=[details]&limit=50"

r = requests.get(url, headers=headers)

print(json.dumps(r.json(), indent=2))
