import os
import requests

NOCODB_URL = os.getenv("NOCODB_URL", "https://db.quillotana.cl")
NOCODB_TOKEN = "R3EhSD8si-WSVdsPxlQVGAfiHRRcDR9cHGHJdBJL"

HEADERS = {
    "xc-token": NOCODB_TOKEN
}


def noco_get(table_id, params=None):

    url = f"{NOCODB_URL}/api/v2/tables/{table_id}/records"

    r = requests.get(url, headers=HEADERS, params=params)

    r.raise_for_status()

    return r.json().get("list", [])
