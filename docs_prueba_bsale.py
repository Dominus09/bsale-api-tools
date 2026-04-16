import requests
import os
import json
import time
from datetime import datetime, timedelta

# 🔑 CONFIGURA TU TOKEN
BSALE_TOKEN = "3d46d0ac6f42455660f2504d27399d5da3550e25"

HEADERS = {
    "access_token": BSALE_TOKEN,
    "Content-Type": "application/json"
}

BASE_URL = "https://api.bsale.io/v1"
OUTPUT_DIR = "bsale_dump"


# -------------------------------
# CONFIG FILTRO
# -------------------------------

OFFICE_ID = "1"

# 👉 EJEMPLO: martes 7 abril 2026
DATE_FROM = "2026-04-07"
DATE_TO = "2026-04-07"


def to_unix(date_str):
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return int(dt.timestamp())


SINCE_TS = to_unix(DATE_FROM)
UNTIL_TS = to_unix(DATE_TO) + 86400  # incluye todo el día


# -------------------------------
# UTILIDADES
# -------------------------------

def ensure_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)


def save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def get(url):
    try:
        res = requests.get(url, headers=HEADERS)
        if res.status_code == 200:
            return res.json()
        else:
            print(f"❌ Error {res.status_code}: {url}")
    except Exception as e:
        print(f"❌ Exception: {e}")
    return None


# -------------------------------
# TRAER DOCUMENTOS FILTRADOS
# -------------------------------

def get_documents_by_type(doc_type, limit=5):
    print(f"\n🔎 Tipo {doc_type} | Office {OFFICE_ID} | Fecha {DATE_FROM}")

    offset = 0
    limit_api = 50

    encontrados = []

    while len(encontrados) < limit:
        url = f"{BASE_URL}/documents.json?limit={limit_api}&offset={offset}&emissiondaterange=[{SINCE_TS},{UNTIL_TS}]"

        data = get(url)

        if not data or "items" not in data:
            break

        items = data["items"]

        if not items:
            break

        for d in items:
            # 🔥 FILTRO CLAVE
            if (
                str(d["document_type"]["id"]) == str(doc_type)
                and str(d["office"]["id"]) == OFFICE_ID
            ):
                encontrados.append(d)

                if len(encontrados) >= limit:
                    break

        offset += limit_api
        time.sleep(0.3)

    print(f"✔ Encontrados {len(encontrados)} documentos tipo {doc_type}")

    return encontrados


# -------------------------------
# ENRIQUECER DOCUMENTO
# -------------------------------

def enrich_document(doc):
    doc_id = doc["id"]

    print(f"📄 Doc {doc_id} | Nº {doc['number']}")

    enriched = {
        "document": doc,
        "details": None,
        "attributes": None,
        "references": None,
        "sellers": None
    }

    # DETAILS
    if "details" in doc and doc["details"]:
        enriched["details"] = get(doc["details"]["href"])
        time.sleep(0.2)

    # ATTRIBUTES
    attr_url = f"{BASE_URL}/documents/{doc_id}/attributes.json"
    enriched["attributes"] = get(attr_url)
    time.sleep(0.2)

    # REFERENCES
    ref_url = f"{BASE_URL}/documents/{doc_id}/references.json"
    enriched["references"] = get(ref_url)
    time.sleep(0.2)

    # SELLERS
    sellers_url = f"{BASE_URL}/documents/{doc_id}/sellers.json"
    enriched["sellers"] = get(sellers_url)
    time.sleep(0.2)

    return enriched


# -------------------------------
# MAIN
# -------------------------------

def main():
    ensure_dir(OUTPUT_DIR)

    tipos = {
        "boletas": 1,
        "facturas": 6,
        "ordenes_compra": 33
    }

    for nombre, tipo in tipos.items():
        print(f"\n==================== {nombre.upper()} ====================")

        carpeta = os.path.join(OUTPUT_DIR, nombre)
        ensure_dir(carpeta)

        docs = get_documents_by_type(tipo, 5)

        for doc in docs:
            enriched = enrich_document(doc)

            file_name = f"{doc['number']}_{doc['id']}.json"
            path = os.path.join(carpeta, file_name)

            save_json(path, enriched)

    print("\n🔥 LISTO — filtrado por office + fecha")


# -------------------------------
# RUN
# -------------------------------

if __name__ == "__main__":
    main()