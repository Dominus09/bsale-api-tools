import requests
import os
import time

print("SYNC PRICES COSTS")

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

TABLE_COSTS="mdjjvdlwev2o76u"
TABLE_PRICES="mcby3npgc3ig042"

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

def fetch(endpoint):

 offset=0
 data=[]

 while True:

  j=api(f"{BASE}/{endpoint}",{"limit":LIMIT,"offset":offset})

  items=j.get("items",[])

  if not items:
   break

  data.extend(items)
  offset+=LIMIT

 return data

def insert(table,data):

 url=f"{NOCODB}/api/v2/tables/{table}/records"

 requests.post(url,json=data,headers=HEADNOCO)

variants=fetch("variants.json")

print("COSTS")

for v in variants:

 vid=v["id"]

 j=api(f"{BASE}/variants/{vid}/costs.json")

 insert(TABLE_COSTS,{
 "variant_id":vid,
 "average_cost":j.get("averageCost"),
 "total_cost":j.get("totalCost")
 })

print("PRICES")

lists=fetch("price_lists.json")

for pl in lists:

 lid=pl["id"]

 offset=0

 while True:

  j=api(f"{BASE}/price_lists/{lid}/details.json",{"limit":LIMIT,"offset":offset})

  items=j.get("items",[])

  if not items:
   break

  for d in items:

   insert(TABLE_PRICES,{
   "variant_id":d["variant"]["id"],
   "price_list_id":lid,
   "price_net":d["variantValue"],
   "price_gross":d["variantValueWithTaxes"]
   })

  offset+=LIMIT

print("PRICES COSTS DONE")
