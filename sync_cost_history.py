import requests
import os
import time
from datetime import datetime, timedelta

print("SYNC COST HISTORY")

BASE="https://api.bsale.io/v1"
NOCODB="https://db.quillotana.cl"

NOCODB_TOKEN=os.getenv("NocoDB_token")

HEAD_NOCO={
    "xc-token":NOCODB_TOKEN,
    "Content-Type":"application/json"
}

TABLE_HISTORY="mdfyfwrrrwffg43"
TABLE_COMPANIES="m27za58sg6ustui"

LIMIT=50
NOCO_LIMIT=200

cut_date=datetime.utcnow()-timedelta(days=90)


# --------------------------------------------------
# GET COMPANIES
# --------------------------------------------------

def get_companies():

    url=f"{NOCODB}/api/v2/tables/{TABLE_COMPANIES}/records"

    r=requests.get(url,headers=HEAD_NOCO,params={"limit":100})

    data=r.json()

    companies=[]

    for row in data.get("list",[]):

        if row.get("active"):

            token=os.getenv(row["bsale_token"])

            if not token:
                print("TOKEN NOT FOUND",row["bsale_token"])
                continue

            companies.append({
                "company_id":row["company_id"],
                "name":row["name"],
                "token":token.strip()
            })

    return companies


# --------------------------------------------------
# SAFE BSALE REQUEST
# --------------------------------------------------

def bsale_get(url,headers,params=None):

    while True:

        r=requests.get(url,headers=headers,params=params)

        if r.status_code==429:

            print("RATE LIMIT WAIT")
            time.sleep(60)
            continue

        r.raise_for_status()

        return r.json()


# --------------------------------------------------
# NOCO GET EXISTING
# --------------------------------------------------

def load_existing(company_id):

    url=f"{NOCODB}/api/v2/tables/{TABLE_HISTORY}/records"

    offset=0
    keys=set()

    while True:

        r=requests.get(
            url,
            headers=HEAD_NOCO,
            params={
                "limit":NOCO_LIMIT,
                "offset":offset,
                "where":f"(company_id,eq,{company_id})"
            }
        )

        data=r.json()

        rows=data.get("list",[])

        if not rows:
            break

        for row in rows:

            key=f"{row['variant_id']}_{row['entry_date']}"

            keys.add(key)

        offset+=NOCO_LIMIT

    return keys


# --------------------------------------------------
# BATCH INSERT
# --------------------------------------------------

def batch_insert(rows):

    url=f"{NOCODB}/api/v2/tables/{TABLE_HISTORY}/records"

    for i in range(0,len(rows),NOCO_LIMIT):

        chunk=rows[i:i+NOCO_LIMIT]

        r=requests.post(url,headers=HEAD_NOCO,json=chunk)

        if r.status_code not in [200,201]:

            print("INSERT ERROR",r.text)


# --------------------------------------------------
# MAIN
# --------------------------------------------------

companies=get_companies()

for company in companies:

    company_id=company["company_id"]
    token=company["token"]

    print("\nSYNC COMPANY",company["name"])

    HEAD_BSALE={"access_token":token}

    existing=load_existing(company_id)

    insert_rows=[]

    # -------------------------------
    # LOAD VARIANTS
    # -------------------------------

    offset=0

    while True:

        data=bsale_get(
            f"{BASE}/variants.json",
            HEAD_BSALE,
            {"limit":LIMIT,"offset":offset}
        )

        items=data.get("items",[])

        if not items:
            break

        for v in items:

            variant_id=v["id"]

            history=bsale_get(
                f"{BASE}/variants/{variant_id}/costs.json",
                HEAD_BSALE
            ).get("history",[])

            for h in history:

                entry_date=datetime.utcfromtimestamp(
                    int(h.get("admissionDate"))
                )

                if entry_date<cut_date:
                    continue

                entry_date_str=entry_date.strftime("%Y-%m-%d")

                key=f"{variant_id}_{entry_date_str}"

                if key in existing:
                    continue

                insert_rows.append({

                    "company_id":company_id,
                    "variant_id":variant_id,
                    "cost_net":h.get("cost"),
                    "cost_gross":None,
                    "fifo_quantity":h.get("availableFifo"),
                    "entry_date":entry_date_str

                })

        offset+=LIMIT


    batch_insert(insert_rows)

    print("INSERTED",len(insert_rows))


print("COST HISTORY DONE")
