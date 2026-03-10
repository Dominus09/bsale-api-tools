import requests
import os
import json

BASE = "https://api.bsale.io/v1"
TOKEN = os.getenv("BSALE_TOKEN_Mini")

headers = {"access_token": TOKEN}

variant_id = 5545  # cambia por una variante que sepas que tiene costos

r = requests.get(f"{BASE}/variants/{variant_id}/costs.json", headers=headers)
print("STATUS:", r.status_code)
print(json.dumps(r.json(), indent=2, ensure_ascii=False))
