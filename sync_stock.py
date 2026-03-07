import requests
import os
import time

print("FAST STOCK SYNC TEST")

BASE="https://api.bsale.io/v1"
NOCODB="https://db.quillotana.cl"

TOKEN=os.getenv("BSALE_TOKEN_Mini")
NOCOTOKEN=os.getenv("NocoDB_token")

HEAD={"access_token":TOKEN}

HEADNOCO={
"xc-token":NOCOTOKEN,
"Content-Type":"application/json"
}

TABLE="mxs2lyz86cnxd23"

LIMIT=50
BATCH=100


def api(url,params=None):

    while True:

        r=requests.get(url,headers=HEAD,params=params)

        if r.status_code==429:

            retry=int(r.json().get("retry_after",60))
            print("RATE LIMIT",retry)
            time.sleep(retry)
            continue

        r.raise_for_status()
        return r.json()


def insert_batch(rows):

    url=f"{NOCODB}/api/v2/tables/{TABLE}/records"

    r=requests.post(
        url,
        json=rows,
        headers=HEADNOCO
    )

    if r.status_code not in [200,201]:
        print("INSERT ERROR",r.text)


offset=0
buffer=[]
total=0


while True:

    j=api(
        f"{BASE}/stocks.json",
        {"limit":LIMIT,"offset":offset}
    )

    items=j.get("items",[])

    if not items:
        break

    for s in items:

        buffer.append({
            "variant_id":s["variant"]["id"],
            "office_id":s["office"]["id"],
            "quantity_available":s["quantityAvailable"],
            "quantity_reserved":s["quantityReserved"]
        })

        if len(buffer)>=BATCH:

            insert_batch(buffer)

            total+=len(buffer)

            print("INSERTED",total)

            buffer=[]

    offset+=LIMIT


if buffer:
    insert_batch(buffer)
    total+=len(buffer)

print("TOTAL INSERTED:",total)
print("STOCK SYNC DONE")
