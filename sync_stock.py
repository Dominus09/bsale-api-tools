import requests
import os
import time

print("SYNC STOCK")

BASE="https://api.bsale.io/v1"

TOKEN=os.getenv("BSALE_TOKEN_Mini")
NOCOTOKEN=os.getenv("NocoDB_token")

HEAD={"access_token":TOKEN}

HEADNOCO={
"xc-token":NOCOTOKEN,
"Content-Type":"application/json"
}

NOCODB="https://db.quillotana.cl"

LIMIT=50

TABLE_STOCK="mxs2lyz86cnxd23"

def api(url,params=None):

 while True:

  r=requests.get(url,headers=HEAD,params=params)

  if r.status_code==429:

   retry=int(r.json().get("retry_after",60))
   print("RATE LIMIT",retry)
   time.sleep(retry)
   continue

  r.raise_for_status()
  time.sleep(0.2)

  return r.json()

offset=0

while True:

 j=api(f"{BASE}/stocks.json",{"limit":LIMIT,"offset":offset})

 items=j.get("items",[])

 if not items:
  break

 for s in items:

  requests.post(
  f"{NOCODB}/api/v2/tables/{TABLE_STOCK}/records",
  json={
  "variant_id":s["variant"]["id"],
  "office_id":s["office"]["id"],
  "quantity_available":s["quantityAvailable"]
  },
  headers=HEADNOCO
  )

 offset+=LIMIT

print("STOCK DONE")
