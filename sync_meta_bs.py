import os
import time

import psycopg2
import requests
from psycopg2.extras import execute_batch

print("SYNC META BSALE START")

BASE = "https://api.bsale.io/v1"
LIMIT_BSALE = 50
BATCH = 500

# ---------------------------------
# POSTGRES (igual que sync_prices_costs.py)
# ---------------------------------

conn = psycopg2.connect(
    host=os.getenv("PG_HOST"),
    database=os.getenv("PG_DB"),
    user=os.getenv("PG_USER"),
    password=os.getenv("PG_PASSWORD"),
)

cur = conn.cursor()


# ---------------------------------
# SAFE BSALE REQUEST (igual criterio que sync_prices_costs.py)
# ---------------------------------


def bsale_get(url, headers, params=None):
    while True:
        r = requests.get(url, headers=headers, params=params, timeout=30)
        if r.status_code == 429:
            wait = int(r.json().get("retry_after", 60))
            print("RATE LIMIT WAIT", wait)
            time.sleep(wait)
            continue
        r.raise_for_status()
        return r.json()


# ---------------------------------
# GET COMPANIES (igual que sync_prices_costs.py)
# ---------------------------------


def get_companies():
    cur.execute(
        """
        SELECT company_id, name, bsale_token
        FROM bsale.companies
        WHERE active = true
        """
    )
    rows = cur.fetchall()
    companies = []
    for r in rows:
        token = os.getenv(r[2])
        if not token:
            print("TOKEN NOT FOUND:", r[2])
            continue
        companies.append({"company_id": r[0], "name": r[1], "token": token})
    return companies


# ---------------------------------
# UPSERT DOCUMENT TYPES
# ---------------------------------


def upsert_document_types(rows):
    if not rows:
        return
    execute_batch(
        cur,
        """
        INSERT INTO bsale.document_types
            (company_id, bsale_id, name, code_sii)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (company_id, bsale_id)
        DO UPDATE SET
            name = EXCLUDED.name,
            code_sii = EXCLUDED.code_sii
        """,
        rows,
    )
    conn.commit()


# ---------------------------------
# UPSERT BSALE USERS (API /users.json)
# ---------------------------------


def upsert_bsale_users(rows):
    if not rows:
        return
    execute_batch(
        cur,
        """
        INSERT INTO bsale.bsale_users
            (company_id, bsale_id, first_name, last_name, email, state)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (company_id, bsale_id)
        DO UPDATE SET
            first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name,
            email = EXCLUDED.email,
            state = EXCLUDED.state
        """,
        rows,
    )
    conn.commit()


def _state_text(value):
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _code_sii_to_int(value):
    """Bsale puede mandar codeSii vacío (''); PostgreSQL INTEGER no acepta ''."""
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    s = str(value).strip()
    if not s:
        return None
    try:
        return int(s)
    except ValueError:
        return None


# ---------------------------------
# SYNC DOCUMENT TYPES
# ---------------------------------


def sync_document_types(company_id, token):
    print("SYNC DOCUMENT TYPES", company_id)
    head = {"access_token": token}
    offset = 0
    rows = []

    while True:
        data = bsale_get(
            f"{BASE}/document_types.json",
            head,
            {"limit": LIMIT_BSALE, "offset": offset},
        )
        items = data.get("items", [])
        if not items:
            break

        for d in items:
            rows.append(
                (
                    company_id,
                    d["id"],
                    d.get("name"),
                    _code_sii_to_int(d.get("codeSii")),
                )
            )
            if len(rows) >= BATCH:
                upsert_document_types(rows)
                rows = []

        offset += LIMIT_BSALE

    if rows:
        upsert_document_types(rows)

    print("DOCUMENT TYPES DONE", company_id)


# ---------------------------------
# SYNC USERS (API)
# ---------------------------------


def sync_bsale_users(company_id, token):
    print("SYNC USERS", company_id)
    head = {"access_token": token}
    offset = 0
    rows = []

    while True:
        data = bsale_get(
            f"{BASE}/users.json",
            head,
            {"limit": LIMIT_BSALE, "offset": offset},
        )
        items = data.get("items", [])
        if not items:
            break

        for u in items:
            rows.append(
                (
                    company_id,
                    u["id"],
                    u.get("firstName"),
                    u.get("lastName"),
                    u.get("email"),
                    _state_text(u.get("state")),
                )
            )
            if len(rows) >= BATCH:
                upsert_bsale_users(rows)
                rows = []

        offset += LIMIT_BSALE

    if rows:
        upsert_bsale_users(rows)

    print("USERS DONE", company_id)


# ---------------------------------
# MAIN
# ---------------------------------

companies = get_companies()

for company in companies:
    cid = company["company_id"]
    print("\nSYNC COMPANY:", company["name"])
    sync_document_types(cid, company["token"])
    sync_bsale_users(cid, company["token"])

conn.close()
print("SYNC META BSALE COMPLETE")
