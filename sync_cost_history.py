import requests
import os
import time
from datetime import datetime,timedelta

print("SYNC COST HISTORY")

BASE="https://api.bsale.io/v1"
NOCODB="https://db.quillotana.cl"

NOCODB_TOKEN=os.getenv("NocoDB_token")

HEAD_NOCO={
    "xc-token":NOCODB_TOKEN,
    "Content-Type":"application/json"
}

TABLE_HISTORY="mdfyfwrrrwffg43"
TABLE_PRODUCTS="meke3fsng90uspe"
TABLE_COMPANIES="m27za58sg6ustui"

LIMIT=50
NOCO_LIMIT=200

CUT_DATE=datetime.utcnow()-timedelta(days=90)


# ---------------------------------------------------
# SAFE REQUEST
# ---------------------------------------------------

def safe_get(url,headers,params=None):

    while True:

        try:

            r=requests.get(

                url,
                headers=headers,
                params=params,
                timeout=30

            )

        except requests.exceptions.Timeout:

            print("TIMEOUT RETRY")
            time.sleep(5)
            continue

        except requests.exceptions.ConnectionError:

            print("CONNECTION ERROR RETRY")
            time.sleep(5)
            continue


        if r.status_code==429:

            print("RATE LIMIT WAIT")
            time.sleep(60)
            continue

        if r.status_code!=200:

            print("BSALE ERROR",r.text)
            time.sleep(5)
            continue

        return r.json()


# ---------------------------------------------------
# GET COMPANIES
# ---------------------------------------------------

def get_companies():

    url=f"{NOCODB}/api/v2/tables/{TABLE_COMPANIES}/records"

    r=requests.get(url,headers=HEAD_NOCO,params={"limit":100})

    data=r.json()

    companies=[]

    for row in data.get("list",[]):

        if row.get("active"):

            token=os.getenv(row["bsale_token"])

            if not token:
                continue

            companies.append({

                "company_id":row["company_id"],
                "name":row["name"],
                "token":token.strip()

            })

    return companies


# ---------------------------------------------------
# LOAD PRODUCTS
# ---------------------------------------------------

def noco_get_all(table):

    url=f"{NOCODB}/api/v2/tables/{table}/records"

    offset=0
    rows=[]

    while True:

        r=requests.get(

            url,
            headers=HEAD_NOCO,
            params={"limit":NOCO_LIMIT,"offset":offset}

        )

        data=r.json()

        batch=data.get("list",[])

        if not batch:
            break

        rows.extend(batch)

        offset+=NOCO_LIMIT

    return rows


def insert(rows):

    url=f"{NOCODB}/api/v2/tables/{TABLE_HISTORY}/records"

    r=requests.post(url,headers=HEAD_NOCO,json=rows)

    if r.status_code not in [200,201]:

        print("INSERT ERROR",r.text)


companies=get_companies()

products=noco_get_all(TABLE_PRODUCTS)

tax_map={}

for p in products:

    tax_map[(p["company_id"],p["bsale_id"])]=p.get("tax_factor",1)


for company in companies:

    company_id=company["company_id"]

    token=company["token"]

    print("\nSYNC COMPANY",company["name"])

    HEAD_BSALE={"access_token":token}

    insert_rows=[]

    offset=0

    processed=0

    while True:

        data=safe_get(

            f"{BASE}/variants.json",
            HEAD_BSALE,
            {"limit":LIMIT,"offset":offset}

        )

        items=data.get("items",[])

        if not items:
            break

        for v in items:

            variant_id=v["id"]

            product_id=v["product"]["id"]

            costs=safe_get(

                f"{BASE}/variants/{variant_id}/costs.json",
                HEAD_BSALE

            )

            history=costs.get("history",[])

            tax_factor=tax_map.get((company_id,product_id),1)

            for h in history:

                entry=datetime.utcfromtimestamp(

                    int(h.get("admissionDate"))

                )

                if entry<CUT_DATE:
                    continue

                cost_net=h.get("cost")

                cost_gross=None

                if cost_net:

                    cost_gross=cost_net*tax_factor


                insert_rows.append({

                    "company_id":company_id,
                    "variant_id":variant_id,
                    "cost_net":cost_net,
                    "cost_gross":cost_gross,
                    "fifo_quantity":h.get("availableFifo"),
                    "entry_date":entry.strftime("%Y-%m-%d"),
                    "tax_factor":tax_factor

                })


                if len(insert_rows)>=200:

                    insert(insert_rows)

                    print("INSERT HISTORY:",len(insert_rows))

                    insert_rows=[]


            processed+=1

            if processed%200==0:

                print("PROCESSED VARIANTS:",processed)

            time.sleep(0.03)


        offset+=LIMIT


    if insert_rows:

        insert(insert_rows)

    print("COMPANY DONE")

print("COST HISTORY DONE")
