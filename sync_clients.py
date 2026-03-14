import requests
import os
import time
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_batch

print("SYNC CLIENTS START")

BASE = "https://api.bsale.io/v1"

LIMIT = 50
BATCH = 500


# -----------------------------
# POSTGRES CONNECTION
# -----------------------------

conn = psycopg2.connect(
    host=os.getenv("PG_HOST"),
    database=os.getenv("PG_DB"),
    user=os.getenv("PG_USER"),
    password=os.getenv("PG_PASSWORD")
)

cur = conn.cursor()


# -----------------------------
# SAFE BSALE REQUEST
# -----------------------------

def bsale_get(url, headers, params=None):

    while True:

        r = requests.get(url, headers=headers, params=params, timeout=30)

        if r.status_code == 429:

            wait = int(r.json().get("retry_after",60))
            print("RATE LIMIT WAIT",wait)
            time.sleep(wait)
            continue

        r.raise_for_status()

        return r.json()


# -----------------------------
# GET COMPANIES
# -----------------------------

def get_companies():

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


# -----------------------------
# UPSERT CLIENTS
# -----------------------------

def upsert(rows):

    execute_batch(cur, """

        INSERT INTO bsale.clients
        (
            company_id,
            bsale_id,
            first_name,
            last_name,
            code,
            phone,
            company,
            facebook,
            city,
            municipality,
            address,
            created,
            updated,
            dia_atencion,
            nombre_fantasia,
            vendedor
        )

        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)

        ON CONFLICT (company_id, bsale_id)
        DO UPDATE SET

        first_name = EXCLUDED.first_name,
        last_name = EXCLUDED.last_name,
        code = EXCLUDED.code,
        phone = EXCLUDED.phone,
        company = EXCLUDED.company,
        facebook = EXCLUDED.facebook,
        city = EXCLUDED.city,
        municipality = EXCLUDED.municipality,
        address = EXCLUDED.address,
        updated = EXCLUDED.updated,
        dia_atencion = EXCLUDED.dia_atencion,
        nombre_fantasia = EXCLUDED.nombre_fantasia,
        vendedor = EXCLUDED.vendedor

    """, rows)

    conn.commit()


# -----------------------------
# MAIN
# -----------------------------

companies = get_companies()

for company in companies:

    company_id = company["company_id"]

    print("\nSYNC COMPANY:",company["name"])

    HEAD_BSALE = {"access_token":company["token"]}

    offset = 0
    rows = []

    while True:

        data = bsale_get(
            f"{BASE}/clients.json",
            HEAD_BSALE,
            {"limit":LIMIT,"offset":offset}
        )

        items = data["items"]

        if not items:
            break

        for client in items:

            bsale_id = client["id"]

            created = None
            updated = None

            if client.get("createdAt"):
                created = datetime.fromtimestamp(
                    int(client["createdAt"])
                )

            if client.get("updatedAt"):
                updated = datetime.fromtimestamp(
                    int(client["updatedAt"])
                )


            # ---------------------
            # CLIENT ATTRIBUTES
            # ---------------------

            attr_data = bsale_get(
                f"{BASE}/clients/{bsale_id}/attributes.json",
                HEAD_BSALE
            )

            dia_atencion = None
            nombre_fantasia = None
            vendedor = None

            for attr in attr_data.get("items", []):

                name = attr.get("name","").strip()
                value = attr.get("value")

                if name == "Dia Atencion":
                    dia_atencion = value

                elif name == "NOMBRE DE FANTASÍA":
                    nombre_fantasia = value

                elif name == "Vendedor":
                    vendedor = value


            rows.append((
                company_id,
                bsale_id,
                client.get("firstName"),
                client.get("lastName"),
                client.get("code"),
                client.get("phone"),
                client.get("company"),
                client.get("facebook"),
                client.get("city"),
                client.get("municipality"),
                client.get("address"),
                created,
                updated,
                dia_atencion,
                nombre_fantasia,
                vendedor
            ))

            if len(rows) >= BATCH:

                upsert(rows)
                rows = []

        offset += LIMIT

    if rows:
        upsert(rows)

print("SYNC CLIENTS COMPLETE")
