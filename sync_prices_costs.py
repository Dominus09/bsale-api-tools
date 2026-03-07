import requests
import os
import time

print("SYNC PRICES START")

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

TABLE_PRICES = "mcby3npgc3ig042"


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


def upsert_price(variant_id, price_list_id, net, gross):

    url = f"{NOCODB}/api/v2/tables/{TABLE_PRICES}/records"

    r = requests.get(
        url,
        headers=HEADNOCO,
        params={
            "where": f"(variant_id,eq,{variant_id})~and(price_list_id,eq,{price_list_id})"
        }
    )

    data = r.json()

    payload = {
        "variant_id": variant_id,
        "price_list_id": price_list_id,
        "price_net": net,
        "price_gross": gross
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


price_lists = api(f"{BASE}/price_lists.json")["items"]

for pl in price_lists:

    lid = pl["id"]

    offset = 0

    while True:

        j = api(
            f"{BASE}/price_lists/{lid}/details.json",
            {"limit": LIMIT, "offset": offset}
        )

        items = j.get("items", [])

        if not items:
            break

        for d in items:

            upsert_price(
                d["variant"]["id"],
                lid,
                d["variantValue"],
                d["variantValueWithTaxes"]
            )

        offset += LIMIT

print("SYNC PRICES DONE")
