import requests
import os
from datetime import datetime

print("SYNC COST HISTORY")

BASE="https://api.bsale.io/v1"
NOCODB="https://db.quillotana.cl"

BSALE_TOKEN=os.getenv("BSALE_TOKEN_Mini")
NOCODB_TOKEN=os.getenv("NocoDB_token")

HEAD_BSALE={"access_token":BSALE_TOKEN}
HEAD_NOCO={"xc-token":NOCODB_TOKEN,"Content-Type":"application/json"}

TABLE_HISTORY="mdfyfwrrrwffg43"

LIMIT=50

# --------------------------------------------------

def bsale_get(url,params=None):

    r=requests.get(url,headers=HEAD_BSALE,params=params)

    if r.status_code==429:
        print("RATE LIMIT")
        return None

    r.raise_for_status()
    return r.json()

# --------------------------------------------------

def insert(payload):

    url=f"{NOCODB}/api/v2/tables/{TABLE_HISTORY}/records"

    r=requests.post(url,headers=HEAD_NOCO,json=payload)

    if r.status_code not in [200,201]:
        print("INSERT ERROR",r.text)

# --------------------------------------------------
# LOAD VARIANTS
# --------------------------------------------------

variants=[]

offset=0

while True:

    data=bsale_get(f"{BASE}/variants.json",{"limit":LIMIT,"offset":offset})

    items=data.get("items",[])

    if not items:
        break

    variants.extend(items)

    offset+=LIMIT

print("VARIANTS",len(variants))

# --------------------------------------------------
# HISTORY
# --------------------------------------------------

for v in variants:

    variant_id=int(v["id"])

    data=bsale_get(f"{BASE}/variants/{variant_id}/costs.json")

    if not data:
        continue

    history=data.get("history",[])

    for h in history:

        insert({

            "variant_id":variant_id,

            "cost_net":h.get("cost"),

            "cost_gross":None,

            "fifo_quantity":h.get("availableFifo"),

            "entry_date":datetime.utcfromtimestamp(
                int(h.get("admissionDate"))
            ).strftime("%Y-%m-%d")

        })

print("COST HISTORY DONE")
