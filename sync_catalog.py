import requests
import os
import json
import time
import psycopg2
from psycopg2.extras import execute_batch

print("SYNC CATALOG START")

BASE = "https://api.bsale.io/v1"

LIMIT = 50

# -------------------------------

# POSTGRES CONNECTION

# -------------------------------

conn = psycopg2.connect(
host=os.getenv("PG_HOST"),
database=os.getenv("PG_DB"),
user=os.getenv("PG_USER"),
password=os.getenv("PG_PASSWORD")
)

cur = conn.cursor()

# -------------------------------

# BSALE REQUEST

# -------------------------------

def bsale_get(url, headers, params=None):

```
while True:

    r = requests.get(url, headers=headers, params=params)

    if r.status_code == 429:

        retry = int(r.json().get("retry_after",60))
        print("RATE LIMIT WAIT", retry)
        time.sleep(retry)
        continue

    r.raise_for_status()

    return r.json()
```

# -------------------------------

# GET COMPANIES FROM POSTGRES

# -------------------------------

def get_companies():

```
cur.execute("""
    SELECT company_id,name,bsale_token
    FROM bsale.companies
    WHERE active = true
""")

rows = cur.fetchall()

companies = []

for r in rows:

    token = os.getenv(r[2])

    if not token:
        print("TOKEN NOT FOUND:", r[2])
        continue

    companies.append({
        "company_id": r[0],
        "name": r[1],
        "token": token
    })

return companies
```

# -------------------------------

# UPSERT HELPERS

# -------------------------------

def upsert(query, data):

```
execute_batch(cur, query, data)
conn.commit()
```

# -------------------------------

# MAIN

# -------------------------------

companies = get_companies()

for company in companies:

```
company_id = company["company_id"]
token = company["token"]

print("\nSYNC COMPANY:",company["name"])

HEAD_BSALE = {
    "access_token": token
}

# -----------------------
# TAXES
# -----------------------

print("LOAD TAXES")

taxes = bsale_get(f"{BASE}/taxes.json",HEAD_BSALE)["items"]

tax_rows = []

tax_map = {}

for t in taxes:

    tax_id = int(t["id"])

    tax_map[tax_id] = {
        "name": t["name"],
        "percentage": float(t["percentage"])
    }

    tax_rows.append((
        company_id,
        tax_id,
        t["name"],
        float(t["percentage"])
    ))

upsert("""

INSERT INTO bsale.taxes
(company_id, bsale_id, name, percentage)

VALUES (%s,%s,%s,%s)

ON CONFLICT (company_id, bsale_id)
DO UPDATE SET

name = EXCLUDED.name,
percentage = EXCLUDED.percentage

""", tax_rows)

print("TAXES DONE")

# -----------------------
# PRODUCT TYPES
# -----------------------

print("LOAD PRODUCT TYPES")

rows = []

offset = 0

while True:

    data = bsale_get(
        f"{BASE}/product_types.json",
        HEAD_BSALE,
        {"limit":LIMIT,"offset":offset}
    )

    items = data.get("items",[])

    if not items:
        break

    for pt in items:

        rows.append((
            company_id,
            int(pt["id"]),
            pt["name"],
            pt["state"]
        ))

    offset += LIMIT

upsert("""

INSERT INTO bsale.product_types
(company_id, bsale_id, name, state)

VALUES (%s,%s,%s,%s)

ON CONFLICT (company_id, bsale_id)
DO UPDATE SET

name = EXCLUDED.name,
state = EXCLUDED.state

""", rows)

print("PRODUCT TYPES DONE")


# -----------------------
# PRICE LISTS
# -----------------------

print("LOAD PRICE LISTS")

lists = bsale_get(f"{BASE}/price_lists.json",HEAD_BSALE)["items"]

rows = []

for pl in lists:

    rows.append((
        company_id,
        int(pl["id"]),
        pl["name"],
        pl["state"]
    ))

upsert("""

INSERT INTO bsale.price_lists
(company_id, bsale_id, name, state)

VALUES (%s,%s,%s,%s)

ON CONFLICT (company_id, bsale_id)
DO UPDATE SET

name = EXCLUDED.name,
state = EXCLUDED.state

""", rows)

print("PRICE LISTS DONE")


# -----------------------
# OFFICES
# -----------------------

print("LOAD OFFICES")

offices = bsale_get(f"{BASE}/offices.json",HEAD_BSALE)["items"]

rows = []

for o in offices:

    rows.append((
        company_id,
        int(o["id"]),
        o["name"],
        o["state"]
    ))

upsert("""

INSERT INTO bsale.offices
(company_id, bsale_id, name, state)

VALUES (%s,%s,%s,%s)

ON CONFLICT (company_id, bsale_id)
DO UPDATE SET

name = EXCLUDED.name,
state = EXCLUDED.state

""", rows)

print("OFFICES DONE")
```

print("\nSYNC CATALOG COMPLETE")
