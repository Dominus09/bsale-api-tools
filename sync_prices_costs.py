import requests
import os
import time

print("SYNC STOCK START")

BASE = "https://api.bsale.io/v1"
NOCODB = "https://db.quillotana.cl"

TOKEN = os.getenv("BSALE_TOKEN_Mini")
NOCOTOKEN = os.getenv("NocoDB_token")

HEAD = {"access_token": TOKEN}

HEADNOCO = {
    "xc-token": NOCOTOKEN,
    "Content-Type": "application/json"
}

LIMIT = 50
TABLE = "mxs2lyz86cnxd23"


def api(url, params=None):

    while True:

        r = requests.get(url, headers=HEAD, params=params)

        if r.status_code == 429:
            retry = int(r.json().get("retry_after", 60))
            print("RATE LIMIT", retry)
            time.sleep(retry)
            continue

        r.raise_for_status()
        return r.json()


def upsert_stock(variant_id, office_id, available, reserved):

    url = f"{NOCODB}/api/v2/tables/{TABLE}/records"

    r = requests.get(
        url,
        headers=HEADNOCO,
        params={
            "where": f"(variant_id,eq,{variant_id})~and(office_id,eq,{office_id})"
        }
    )

    data = r.json()

    payload = {
        "variant_id": variant_id,
        "office_id": office_id,
        "quantity_available": available,
        "quantity_reserved": reserved
    }

    if data["list"]:

        row_id = data["list"][0]["Id"]

        requests.patch(
            f"{url}/{row_id}",
            json=payload,
            headers=HEADNOCO
        )

    else:

        requests.post(
            url,
            json=payload,
            headers=HEADNOCO
        )


offset = 0

while True:

    j = api(
        f"{BASE}/stocks.json",
        {"limit": LIMIT, "offset": offset}
    )

    items = j.get("items", [])

    if not items:
        break

    for s in items:

        upsert_stock(
            s["variant"]["id"],
            s["office"]["id"],
            s["quantityAvailable"],
            s["quantityReserved"]
        )

    offset += LIMIT

print("SYNC STOCK DONE")
